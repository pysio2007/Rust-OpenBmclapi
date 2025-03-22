use anyhow::Result;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode, header};
use futures::stream::StreamExt;
use std::collections::HashSet;
use std::path::{PathBuf, Path};
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio_util::io::ReaderStream;
use colored::Colorize;

use log::info;
use crate::storage::base::{Storage, RequestHandler};
use crate::types::{FileInfo, GCCounter};
use crate::util::hash_to_filename;

pub struct FileStorage {
    cache_dir: PathBuf,
}

impl FileStorage {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }
}

#[async_trait]
impl Storage for FileStorage {
    async fn check(&self) -> Result<bool> {
        if !self.cache_dir.exists() {
            fs::create_dir_all(&self.cache_dir).await?;
        }
        
        if !self.cache_dir.is_dir() {
            return Err(anyhow::anyhow!("缓存目录不是一个目录"));
        }
        
        // 检查是否有写入权限
        let test_file = self.cache_dir.join(".writetest");
        fs::write(&test_file, b"test").await?;
        fs::remove_file(test_file).await?;
        
        Ok(true)
    }
    
    async fn write_file(&self, path: String, content: Vec<u8>, _file_info: &FileInfo) -> Result<()> {
        let file_path = self.cache_dir.join(&path);
        
        // 确保父目录存在
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        fs::write(file_path, content).await?;
        Ok(())
    }
    
    async fn exists(&self, path: &str) -> Result<bool> {
        let file_path = self.cache_dir.join(path);
        Ok(file_path.exists())
    }
    
    fn get_absolute_path(&self, path: &str) -> String {
        self.cache_dir.join(path).to_string_lossy().to_string()
    }
    
    async fn get_missing_files(&self, files: &[FileInfo]) -> Result<Vec<FileInfo>> {
        let mut missing_files = Vec::new();
        
        for file in files {
            let hash_path = self.cache_dir.join(&file.hash);
            if !hash_path.exists() {
                missing_files.push(file.clone());
            }
        }
        
        Ok(missing_files)
    }
    
    async fn gc(&self, files: &[FileInfo]) -> Result<GCCounter> {
        let mut counter = GCCounter::default();
        
        // 构建有效文件的集合
        let file_set: HashSet<String> = files.iter()
            .map(|file| file.hash.clone())
            .collect();
        
        let mut queue = vec![self.cache_dir.clone()];
        
        while let Some(dir) = queue.pop() {
            let mut entries = fs::read_dir(dir).await?;
            
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let metadata = entry.metadata().await?;
                
                if metadata.is_dir() {
                    queue.push(path);
                    continue;
                }
                
                // 检查文件是否在有效文件列表中
                let relative_path = path.strip_prefix(&self.cache_dir)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();
                    
                if !file_set.contains(&relative_path) {
                    info!("{}", format!("删除过期文件: {}", path.display()).dimmed());
                    fs::remove_file(&path).await?;
                    counter.count += 1;
                    counter.size += metadata.len();
                }
            }
        }
        
        Ok(counter)
    }
    
    async fn handle_bytes_request(&self, hash_path: &str, _req: Request<&[u8]>) -> Result<Response<Body>> {
        let file_path = self.cache_dir.join(hash_path);
        
        let file = match fs::File::open(&file_path).await {
            Ok(file) => file,
            Err(_) => {
                return Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())?);
            }
        };
        
        let metadata = file.metadata().await?;
        let stream = ReaderStream::new(file);
        let body = Body::from_stream(stream);
        
        let response = Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, metadata.len())
            .header(header::CACHE_CONTROL, "max-age=2592000") // 30天缓存
            .body(body)?;
            
        Ok(response)
    }
} 
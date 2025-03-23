use anyhow::Result;
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode, header};
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::fs;
use tokio_util::io::ReaderStream;
use colored::Colorize;
use tokio::time::{sleep, Duration};
use sha1::{Sha1, Digest};

use log::{info, warn, error, debug};
use crate::storage::base::Storage;
use crate::types::{FileInfo, GCCounter};
use crate::util::hash_to_filename;

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: u64 = 5;

pub struct FileStorage {
    cache_dir: PathBuf,
}

impl FileStorage {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { cache_dir }
    }
    
    async fn verify_file(&self, path: &str, expected_hash: &str) -> Result<bool> {
        let file_path = self.cache_dir.join(path);
        
        let content = match fs::read(&file_path).await {
            Ok(content) => content,
            Err(e) => {
                error!("读取文件失败: {} - {}", file_path.display(), e);
                return Ok(false);
            }
        };
        
        let mut hasher = Sha1::new();
        hasher.update(&content);
        let actual_hash = format!("{:x}", hasher.finalize());
        
        Ok(actual_hash.to_lowercase() == expected_hash.to_lowercase())
    }
    
    async fn retry_operation<F, Fut, T>(&self, operation: F, description: &str) -> Result<T>
    where
        F: Fn() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send + 'static,
    {
        let mut retries = 0;
        let mut last_error = None;
        
        while retries < MAX_RETRIES {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    warn!("{} 重试 {}/{}: {}", description, retries + 1, MAX_RETRIES, e);
                    last_error = Some(e);
                    retries += 1;
                    if retries < MAX_RETRIES {
                        sleep(Duration::from_secs(RETRY_DELAY)).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("最大重试次数已用完")))
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
    
    async fn write_file(&self, path: String, content: Vec<u8>, file_info: &FileInfo) -> Result<()> {
        let file_path = self.cache_dir.join(&path);
        
        // 确保父目录存在
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        // 克隆数据用于闭包
        let file_path_clone = file_path.clone();
        let content_clone = content.clone();
        
        // 写入文件
        self.retry_operation(move || {
            let file_path = file_path_clone.clone();
            let content = content_clone.clone();
            
            async move {
                Ok(fs::write(file_path, content).await?)
            }
        }, "写入文件").await?;
        
        // 验证文件完整性
        if !self.verify_file(&path, &file_info.hash).await? {
            fs::remove_file(&file_path).await?;
            return Err(anyhow::anyhow!("文件完整性验证失败"));
        }
        
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
            let relative_path = hash_to_filename(&file.hash);
            let file_path = self.cache_dir.join(&relative_path);
            
            debug!("检查文件 {} (哈希值: {})", file.path, file.hash);
            debug!("对应的本地路径: {}", file_path.display());
            
            if !file_path.exists() {
                debug!("文件不存在: {}", file_path.display());
                missing_files.push(file.clone());
                continue;
            }
            
            match self.verify_file(&relative_path, &file.hash).await {
                Ok(true) => {
                    debug!("文件验证成功: {}", file.path);
                },
                Ok(false) => {
                    warn!("文件验证失败: {} (哈希值不匹配)", file.path);
                    missing_files.push(file.clone());
                },
                Err(e) => {
                    error!("文件验证出错: {} - {}", file.path, e);
                    missing_files.push(file.clone());
                }
            }
        }
        
        if !missing_files.is_empty() {
            info!("发现 {} 个缺失或损坏的文件", missing_files.len());
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
            let mut entries = match fs::read_dir(&dir).await {
                Ok(entries) => entries,
                Err(e) => {
                    error!("读取目录失败: {} - {}", dir.display(), e);
                    continue;
                }
            };
            
            while let Some(entry) = entries.next_entry().await? {
                let path = entry.path();
                let metadata = match entry.metadata().await {
                    Ok(metadata) => metadata,
                    Err(e) => {
                        error!("读取文件元数据失败: {} - {}", path.display(), e);
                        continue;
                    }
                };
                
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
                    if let Err(e) = fs::remove_file(&path).await {
                        error!("删除文件失败: {} - {}", path.display(), e);
                        continue;
                    }
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
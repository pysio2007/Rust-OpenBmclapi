use anyhow::{anyhow, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use log::info;
use reqwest::{Client, StatusCode as ReqStatusCode};
use serde_json::Value;
use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use crate::storage::base::{Storage, RequestHandler};
use crate::types::{FileInfo, GCCounter};
use crate::util::hash_to_filename;

pub struct AlistWebdavStorage {
    client: Client,
    base_url: String,
    username: String,
    password: String,
    path: String,
    temp_dir: String,
}

impl AlistWebdavStorage {
    pub fn new(opts: Value) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        
        Self {
            client,
            base_url: opts["base_url"].as_str().unwrap_or("").to_string(),
            username: opts["username"].as_str().unwrap_or("").to_string(),
            password: opts["password"].as_str().unwrap_or("").to_string(),
            path: opts["path"].as_str().unwrap_or("/").to_string(),
            temp_dir: opts["temp_dir"].as_str().unwrap_or("temp").to_string(),
        }
    }
    
    async fn ensure_dir_exists(&self, path: &str) -> Result<()> {
        let url = format!("{}{}", self.base_url, path);
        
        // 先尝试获取目录信息，看是否存在
        let res = self.client.head(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        if res.status().is_success() {
            return Ok(());
        }
        
        // 如果目录不存在，则创建
        let res = self.client.request(reqwest::Method::from_bytes(b"MKCOL").unwrap(), &url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        if res.status().is_success() {
            Ok(())
        } else {
            Err(anyhow!("创建WebDAV目录失败: {} - {}", path, res.status()))
        }
    }
}

#[async_trait]
impl Storage for AlistWebdavStorage {
    async fn init(&self) -> Result<()> {
        // 确保根目录存在
        self.ensure_dir_exists(&self.path).await?;
        Ok(())
    }
    
    async fn check(&self) -> Result<bool> {
        let url = format!("{}{}", self.base_url, self.path);
        
        let res = self.client.head(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        if res.status().is_success() {
            Ok(true)
        } else {
            Ok(false)
        }
    }
    
    async fn write_file(&self, path: String, content: Vec<u8>, _file_info: &FileInfo) -> Result<()> {
        let file_url = format!("{}{}/{}", self.base_url, self.path, path);
        
        // 确保父目录存在
        if let Some(parent) = Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                let parent_path = format!("{}/{}", self.path, parent.to_string_lossy());
                self.ensure_dir_exists(&parent_path).await?;
            }
        }
        
        // 上传文件
        let res = self.client.put(&file_url)
            .basic_auth(&self.username, Some(&self.password))
            .body(content)
            .send()
            .await?;
            
        if res.status().is_success() {
            Ok(())
        } else {
            Err(anyhow!("上传文件失败: {} - {}", path, res.status()))
        }
    }
    
    async fn exists(&self, path: &str) -> Result<bool> {
        let file_url = format!("{}{}/{}", self.base_url, self.path, path);
        
        let res = self.client.head(&file_url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        Ok(res.status().is_success())
    }
    
    fn get_absolute_path(&self, path: &str) -> String {
        format!("{}{}/{}", self.base_url, self.path, path)
    }
    
    async fn get_missing_files(&self, files: &[FileInfo]) -> Result<Vec<FileInfo>> {
        let mut missing_files = Vec::new();
        
        for file in files.iter() {
            let exists = self.exists(&file.hash).await?;
            if !exists {
                missing_files.push(file.clone());
            }
        }
        
        Ok(missing_files)
    }
    
    async fn gc(&self, _files: &[FileInfo]) -> Result<GCCounter> {
        // 这里简化处理，返回空计数器
        Ok(GCCounter::default())
    }
    
    async fn handle_bytes_request(&self, hash_path: &str, _req: Request<&[u8]>) -> Result<Response<Body>> {
        // 对于WebDAV，我们需要先下载文件到临时目录，然后返回
        let file_url = format!("{}{}/{}", self.base_url, self.path, hash_path);
        
        // 下载文件到临时目录
        let res = self.client.get(&file_url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        if !res.status().is_success() {
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())?);
        }
        
        let bytes = res.bytes().await?;
        
        // 返回响应
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", bytes.len())
            .header("Cache-Control", "max-age=2592000")
            .body(Body::from(bytes))?)
    }
} 
use anyhow::Result;
use async_trait::async_trait;
use axum::http::Request;
use axum::response::Response;
use std::path::Path;

use crate::config::Config;
use crate::types::{FileInfo, GCCounter};
use crate::storage::file::FileStorage;
use crate::storage::alist_webdav::AlistWebdavStorage;
use log::info;

// 为handle_request方法创建新的trait
#[allow(dead_code)]
#[async_trait]
pub trait RequestHandler<B> {
    async fn handle_request(&self, hash_path: &str, req: Request<B>) -> Result<Response<axum::body::Body>>;
}

#[async_trait]
pub trait Storage: Send + Sync {
    async fn init(&self) -> Result<()> {
        Ok(())
    }
    
    async fn check(&self) -> Result<bool>;
    
    async fn write_file(&self, path: String, content: Vec<u8>, file_info: &FileInfo) -> Result<()>;
    
    async fn exists(&self, path: &str) -> Result<bool>;
    
    fn get_absolute_path(&self, path: &str) -> String;
    
    async fn get_missing_files(&self, files: &[FileInfo]) -> Result<Vec<FileInfo>>;
    
    async fn gc(&self, files: &[FileInfo]) -> Result<GCCounter>;
    
    // 一个方便的方法来实现RequestHandler
    async fn handle_bytes_request(&self, hash_path: &str, req: Request<&[u8]>) -> Result<Response<axum::body::Body>>;
}

pub fn get_storage(config: &Config) -> Box<dyn Storage> {
    match config.storage.as_str() {
        "file" => {
            let storage = FileStorage::new(Path::new("cache").to_path_buf());
            info!("使用文件存储");
            Box::new(storage)
        }
        "alist" => {
            if let Some(opts) = &config.storage_opts {
                let storage = AlistWebdavStorage::new(opts.clone());
                info!("使用Alist WebDAV存储");
                Box::new(storage)
            } else {
                panic!("使用Alist存储需要提供storage_opts");
            }
        }
        _ => panic!("未知的存储类型: {}", config.storage),
    }
} 
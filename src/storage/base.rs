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

pub fn get_storage(config: &Config, cache_dir: Option<std::path::PathBuf>) -> Box<dyn Storage> {
    match config.storage.as_str() {
        "file" => {
            let path = cache_dir.unwrap_or_else(|| Path::new("cache").to_path_buf());
            let storage = FileStorage::new(path.clone());
            info!("使用文件存储，文件目录: {:?}", path);
            Box::new(storage)
        }
        "alist" => {
            // 获取已解析好的AList配置
            let storage_opts = if let Some(opts) = config.get_alist_config() {
                opts
            } else {
                panic!("获取AList WebDAV配置失败");
            };
            
            let storage = AlistWebdavStorage::new(storage_opts);
            info!("使用Alist WebDAV存储");
            Box::new(storage)
        }
        _ => panic!("未知的存储类型: {}", config.storage),
    }
} 
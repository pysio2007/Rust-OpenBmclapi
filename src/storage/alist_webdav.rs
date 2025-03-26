use anyhow::{anyhow, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use reqwest::Client;
use serde_json::Value;
use std::path::Path;
use std::time::Duration;
use log::{debug, warn};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::storage::base::Storage;
use crate::types::{FileInfo, GCCounter};

// 重定向URL缓存结构
#[derive(Clone)]
struct RedirectCacheEntry {
    url: String,
    expires: u64,
}

/// Alist WebDAV存储配置模式
/// ```
/// {
///   "url": "http://example.com/webdav",
///   "username": "user",
///   "password": "pass",
///   "basePath": "/openbmclapi/",
///   "cacheTtl": "1h"
/// }
/// ```
pub struct AlistWebdavStorage {
    client: Client,
    base_url: String,
    username: String,
    password: String,
    path: String,
    #[allow(dead_code)]
    temp_dir: String,
    // 添加重定向URL缓存
    redirect_cache: Arc<RwLock<HashMap<String, RedirectCacheEntry>>>,
    // 存储空文件集合，避免重复请求
    empty_files: Arc<RwLock<HashSet<String>>>,
    // 文件信息缓存
    files: Arc<RwLock<HashMap<String, FileInfo>>>,
    cache_ttl: Duration,
}

impl AlistWebdavStorage {
    pub fn new(opts: Value) -> Self {
        // 打印收到的配置参数
        log::info!("AlistWebdavStorage配置参数: {}", opts);
        
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .unwrap();
            
        // 从配置中获取缓存TTL，默认为1小时
        let cache_ttl = if let Some(ttl) = opts.get("cacheTtl") {
            if let Some(ttl_str) = ttl.as_str() {
                // 解析类似 "1h" 的时间字符串
                let ttl_secs = match ttl_str {
                    "1h" => 3600,
                    "1d" => 86400,
                    _ => 3600, // 默认1小时
                };
                Duration::from_secs(ttl_secs)
            } else if let Some(ttl_num) = ttl.as_i64() {
                Duration::from_secs(ttl_num as u64)
            } else {
                Duration::from_secs(3600) // 默认1小时
            }
        } else {
            Duration::from_secs(3600) // 默认1小时
        };
        
        // 处理URL
        let url = match opts.get("url") {
            Some(val) => val.as_str().unwrap_or("").trim().to_string(),
            None => "".to_string(),
        };
        
        if url.is_empty() {
            log::error!("错误: WebDAV URL不能为空");
            panic!("错误: WebDAV URL不能为空，请检查配置");
        }
        
        let base_url = if !url.starts_with("http://") && !url.starts_with("https://") {
            format!("http://{}", url)
        } else {
            url
        };
        
        log::debug!("WebDAV基础URL: {}", base_url);
        
        // 处理路径
        let path = match opts.get("basePath") {
            Some(val) => val.as_str().unwrap_or("/").trim().to_string(),
            None => "/".to_string(),
        };
        
        let path = if !path.starts_with("/") {
            format!("/{}", path)
        } else {
            path
        };
        
        // 处理用户名和密码
        let username = match opts.get("username") {
            Some(val) => val.as_str().unwrap_or("").to_string(),
            None => "".to_string(),
        };
        
        let password = match opts.get("password") {
            Some(val) => val.as_str().unwrap_or("").to_string(),
            None => "".to_string(),
        };
        
        log::info!("Alist WebDAV配置: URL={}, 路径={}, 用户名={}, 密码长度={}", 
            base_url, path, 
            if username.is_empty() { "<未设置>" } else { &username },
            if password.is_empty() { 0 } else { password.len() });
        
        Self {
            client,
            base_url,
            username,
            password,
            path,
            temp_dir: opts.get("temp_dir").and_then(|v| v.as_str()).unwrap_or("temp").to_string(),
            redirect_cache: Arc::new(RwLock::new(HashMap::new())),
            empty_files: Arc::new(RwLock::new(HashSet::new())),
            files: Arc::new(RwLock::new(HashMap::new())),
            cache_ttl,
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
            debug!("目录已存在: {}", path);
            return Ok(());
        }
        
        // 检查是否需要创建多级目录
        // 对于"a/b/c"这样的路径，需要确保"a"和"a/b"都存在
        let parts: Vec<&str> = path.split('/')
            .filter(|p| !p.is_empty())
            .collect();
            
        if parts.len() > 1 {
            let mut current_path = String::new();
            
            // 逐层创建目录
            for (i, part) in parts.iter().enumerate() {
                // 跳过最后一个部分，只创建父目录
                if i == parts.len() - 1 {
                    break;
                }
                
                if current_path.is_empty() {
                    current_path = format!("/{}", part);
                } else {
                    current_path = format!("{}/{}", current_path, part);
                }
                
                // 确保父目录存在
                // 使用Box::pin处理递归的异步调用
                Box::pin(self.ensure_dir_exists(&current_path)).await?;
            }
        }
        
        debug!("创建WebDAV目录: {}", path);
        // 如果目录不存在，则创建
        let res = self.client.request(reqwest::Method::from_bytes(b"MKCOL").unwrap(), &url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        if res.status().is_success() {
            debug!("成功创建目录: {}", path);
            Ok(())
        } else if res.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED || 
                  res.status() == reqwest::StatusCode::CONFLICT {
            // 405表示目录已存在，409表示冲突(可能因为目录已存在)
            debug!("目录可能已存在 (状态: {}): {}", res.status(), path);
            Ok(())
        } else {
            Err(anyhow!("创建WebDAV目录失败: {} - {}", path, res.status()))
        }
    }
    
    // 获取缓存的重定向URL
    async fn get_cached_redirect(&self, hash_path: &str) -> Option<String> {
        let cache = self.redirect_cache.read().await;
        if let Some(entry) = cache.get(hash_path) {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
                
            if entry.expires > now {
                return Some(entry.url.clone());
            }
        }
        None
    }
    
    // 设置重定向URL缓存
    async fn set_redirect_cache(&self, hash_path: String, url: String) {
        let expires = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() + self.cache_ttl.as_secs();
            
        let mut cache = self.redirect_cache.write().await;
        cache.insert(hash_path, RedirectCacheEntry { url, expires });
    }
    
    // 检查文件是否为空文件
    async fn is_empty_file(&self, hash_path: &str) -> bool {
        let empty_files = self.empty_files.read().await;
        empty_files.contains(hash_path)
    }
    
    // 获取文件大小，支持范围请求
    #[allow(dead_code)]
    fn get_size(&self, size: u64, range: Option<&str>) -> u64 {
        if range.is_none() {
            return size;
        }
        
        // 简化实现，不计算精确的范围大小
        size
    }
}

#[async_trait]
impl Storage for AlistWebdavStorage {
    async fn init(&self) -> Result<()> {
        // 确保根目录存在
        // 如果路径只是根目录"/"，通常WebDAV服务器已经创建好了
        if self.path != "/" {
            debug!("初始化存储，确保路径存在: {}", self.path);
            self.ensure_dir_exists(&self.path).await?;
        } else {
            debug!("使用WebDAV根目录，跳过目录检查");
        }
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
    
    async fn write_file(&self, path: String, content: Vec<u8>, file_info: &FileInfo) -> Result<()> {
        // 如果是空文件，添加到empty_files集合中
        if content.is_empty() {
            let mut empty_files = self.empty_files.write().await;
            empty_files.insert(path);
            return Ok(());
        }
        
        let file_url = format!("{}{}/{}", self.base_url, self.path, path);
        debug!("上传文件URL: {}", file_url);
        
        // 确保父目录存在
        if let Some(parent) = Path::new(&path).parent() {
            if !parent.as_os_str().is_empty() {
                let parent_path = format!("{}{}", self.path, 
                    if parent.to_string_lossy().starts_with("/") {
                        parent.to_string_lossy().to_string()
                    } else {
                        format!("/{}", parent.to_string_lossy())
                    });
                debug!("确保父目录存在: {}", parent_path);
                self.ensure_dir_exists(&parent_path).await?;
            }
        }
        
        // 上传文件
        debug!("上传文件到WebDAV: {}", path);
        let res = self.client.put(&file_url)
            .basic_auth(&self.username, Some(&self.password))
            .body(content)
            .send()
            .await?;
            
        if res.status().is_success() {
            // 添加到文件缓存
            let mut files = self.files.write().await;
            files.insert(file_info.hash.clone(), file_info.clone());
            Ok(())
        } else {
            Err(anyhow!("上传文件失败: {} - {} (URL: {})", path, res.status(), file_url))
        }
    }
    
    async fn exists(&self, path: &str) -> Result<bool> {
        // 检查是否是空文件
        if self.is_empty_file(path).await {
            return Ok(true);
        }
        
        // 检查缓存中是否存在
        {
            let files = self.files.read().await;
            if files.contains_key(path) {
                return Ok(true);
            }
        }
        
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
        
        // 检查本地缓存
        {
            let cached_files = self.files.read().await;
            let empty_files = self.empty_files.read().await;
            
            for file in files.iter() {
                if !cached_files.contains_key(&file.hash) && !empty_files.contains(&file.hash) {
                    missing_files.push(file.clone());
                }
            }
            
            // 如果已有缓存数据，直接返回结果
            if !cached_files.is_empty() {
                return Ok(missing_files);
            }
        }
        
        // 如果没有缓存，则需要检查每个文件
        let mut result = Vec::new();
        for file in files.iter() {
            let exists = self.exists(&file.hash).await?;
            if !exists {
                result.push(file.clone());
            }
        }
        
        Ok(result)
    }
    
    async fn gc(&self, _files: &[FileInfo]) -> Result<GCCounter> {
        // 这里简化处理，返回空计数器
        Ok(GCCounter::default())
    }
    
    async fn handle_bytes_request(&self, hash_path: &str, req: Request<&[u8]>) -> Result<Response<Body>> {
        // 检查是否是空文件
        if self.is_empty_file(hash_path).await {
            debug!("处理空文件请求: {}", hash_path);
            return Ok(Response::builder()
                .status(StatusCode::OK)
                .body(Body::empty())?);
        }
        
        // 首先检查缓存的重定向URL
        if let Some(cached_url) = self.get_cached_redirect(hash_path).await {
            debug!("使用缓存的重定向URL: {}", cached_url);
            return Ok(Response::builder()
                .status(StatusCode::FOUND)
                .header("Location", cached_url)
                .header("Cache-Control", "max-age=3600")
                .body(Body::empty())?);
        }
        
        let file_url = format!("{}{}/{}", self.base_url, self.path, hash_path);
        
        // 先检查文件是否存在
        let head_res = self.client.head(&file_url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        if !head_res.status().is_success() {
            debug!("文件在WebDAV中不存在: {}", hash_path);
            return Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())?);
        }
        
        // 获取Range请求头
        let range_header = req.headers().get("range").and_then(|v| v.to_str().ok());
        
        // 使用reqwest检查重定向，但不下载整个文件
        debug!("处理文件请求: {}", hash_path);
        let mut request = self.client.get(&file_url)
            .basic_auth(&self.username, Some(&self.password));
            
        // 如果有Range头，添加到请求中
        if let Some(range) = range_header {
            request = request.header("range", range);
        }
        
        let response = request.send().await?;
        
        // 若返回重定向，则直接返回重定向响应
        if response.status().is_redirection() {
            if let Some(location) = response.headers().get(reqwest::header::LOCATION) {
                // 将 reqwest 的 HeaderValue 转换为字符串
                let location_str = location.to_str().unwrap_or_default();
                debug!("重定向到: {}", location_str);
                
                if location_str.is_empty() {
                    warn!("获取到空的重定向URL，使用原始URL");
                    return Ok(Response::builder()
                        .status(StatusCode::FOUND)
                        .header("Location", file_url)
                        .header("Cache-Control", "max-age=3600")
                        .body(Body::empty())?);
                }
                
                // 缓存重定向URL
                self.set_redirect_cache(hash_path.to_string(), location_str.to_string()).await;
                
                return Ok(Response::builder()
                    .status(StatusCode::FOUND)
                    .header("Location", location_str)
                    .header("Cache-Control", "max-age=3600")
                    .body(Body::empty())?);
            }
        }
        
        // 如果是成功的响应，直接返回内容
        if response.status().is_success() {
            let status = response.status();
            let body = response.bytes().await?;
            return Ok(Response::builder()
                .status(StatusCode::from_u16(status.as_u16())?)
                .header("Content-Type", "application/octet-stream")
                .header("Content-Length", body.len())
                .header("Cache-Control", "max-age=3600")
                .body(Body::from(body))?);
        }
        
        // 如果没有重定向，则返回一个重定向到原始URL的响应
        debug!("直接重定向到文件URL: {}", file_url);
        Ok(Response::builder()
            .status(StatusCode::FOUND)
            .header("Location", file_url)
            .header("Cache-Control", "max-age=3600")
            .body(Body::empty())?)
    }
} 
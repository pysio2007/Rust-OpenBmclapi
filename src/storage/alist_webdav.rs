use anyhow::{anyhow, Result};
use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, Response, StatusCode};
use reqwest::Client;
use serde_json::Value;
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
    // 目录存在性缓存
    existing_dirs: Arc<RwLock<HashSet<String>>>,
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
            existing_dirs: Arc::new(RwLock::new(HashSet::new())),
            cache_ttl,
        }
    }
    
    async fn ensure_dir_exists(&self, path: &str) -> Result<()> {
        // 先检查目录缓存
        {
            let dirs = self.existing_dirs.read().await;
            if dirs.contains(path) {
                debug!("目录已在缓存中: {}", path);
                return Ok(());
            }
        }
        
        let url = format!("{}{}", self.base_url, path);
        
        // 先尝试获取目录信息，看是否存在
        let res = self.client.head(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        if res.status().is_success() {
            debug!("目录已存在: {}", path);
            // 添加到目录缓存
            let mut dirs = self.existing_dirs.write().await;
            dirs.insert(path.to_string());
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
            // 添加到目录缓存
            let mut dirs = self.existing_dirs.write().await;
            dirs.insert(path.to_string());
            Ok(())
        } else if res.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED || 
                  res.status() == reqwest::StatusCode::CONFLICT {
            // 405表示目录已存在，409表示冲突(可能因为目录已存在)
            debug!("目录可能已存在 (状态: {}): {}", res.status(), path);
            // 添加到目录缓存
            let mut dirs = self.existing_dirs.write().await;
            dirs.insert(path.to_string());
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
            // 根目录默认存在，添加到缓存
            let mut dirs = self.existing_dirs.write().await;
            dirs.insert("/".to_string());
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
        
        // 确保哈希前缀目录存在
        if path.len() >= 2 {
            let prefix = &path[0..2];
            let prefix_dir = format!("{}/{}", self.path, prefix);
            
            // 检查目录是否在缓存中
            let prefix_exists = {
                let dirs = self.existing_dirs.read().await;
                dirs.contains(&prefix_dir)
            };
            
            // 如果目录不在缓存中，则创建
            if !prefix_exists {
                debug!("创建哈希前缀目录: {}", prefix_dir);
                self.ensure_dir_exists(&prefix_dir).await?;
            }
        }
        
        let file_url = format!("{}{}/{}", self.base_url, self.path, path);
        debug!("上传文件URL: {}", file_url);
        
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
        
        // 检查哈希前缀目录是否存在
        if path.len() >= 2 {
            let prefix = &path[0..2];
            let prefix_dir = format!("{}/{}", self.path, prefix);
            
            // 检查前缀目录是否在缓存中
            let prefix_dir_exists = {
                let dirs = self.existing_dirs.read().await;
                dirs.contains(&prefix_dir)
            };
            
            // 如果前缀目录不存在，直接返回false
            if !prefix_dir_exists {
                // 尝试检查前缀目录
                let prefix_url = format!("{}{}", self.base_url, prefix_dir);
                let res = self.client.head(&prefix_url)
                    .basic_auth(&self.username, Some(&self.password))
                    .send()
                    .await?;
                    
                if !res.status().is_success() {
                    // 前缀目录不存在，添加到缓存
                    debug!("哈希前缀目录不存在: {}", prefix_dir);
                    return Ok(false);
                } else {
                    // 前缀目录存在，添加到缓存
                    let mut dirs = self.existing_dirs.write().await;
                    dirs.insert(prefix_dir);
                }
            }
        }
        
        // 构建文件路径
        let file_path = format!("{}/{}", self.path, path);
        let file_url = format!("{}{}", self.base_url, file_path);
        
        // 检查文件是否存在
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
        // 检查本地缓存
        {
            let cached_files = self.files.read().await;
            let empty_files = self.empty_files.read().await;
            
            if !cached_files.is_empty() {
                // 如果已有缓存数据，检查哪些文件不在缓存中
                let mut missing_files = Vec::new();
                for file in files.iter() {
                    if !cached_files.contains_key(&file.hash) && !empty_files.contains(&file.hash) {
                        missing_files.push(file.clone());
                    }
                }
                return Ok(missing_files);
            }
        }
        
        // 首先检查根目录是否存在
        let root_url = format!("{}{}", self.base_url, self.path);
        debug!("检查WebDAV根目录是否存在: {}", root_url);
        
        let res = self.client.head(&root_url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await?;
            
        // 如果根目录不存在，则认为所有文件都缺失
        if !res.status().is_success() {
            debug!("WebDAV根目录不存在，将下载所有文件: {}", self.path);
            return Ok(files.to_vec());
        }
        
        // 按照哈希前缀分组检查文件夹是否存在
        debug!("WebDAV根目录存在，按照哈希前缀检查子目录");
        
        // 创建一个包含所有可能的哈希前缀的集合（十六进制，共256个可能）
        let mut prefix_dirs = HashMap::<String, bool>::new();
        let mut missing_files = Vec::new();
        
        // 先按哈希前缀字符（前2个字符）对文件进行分组
        let mut files_by_prefix = HashMap::<String, Vec<FileInfo>>::new();
        
        for file in files.iter() {
            if file.hash.len() < 2 {
                // 对于异常短的哈希，直接添加到缺失列表
                missing_files.push(file.clone());
                continue;
            }
            
            let prefix = file.hash[0..2].to_string();
            files_by_prefix.entry(prefix).or_insert_with(Vec::new).push(file.clone());
        }
        
        debug!("已将{}个文件按哈希前缀分组为{}个目录", files.len(), files_by_prefix.len());
        
        // 检查每个前缀目录是否存在
        for (prefix, prefix_files) in files_by_prefix.iter() {
            let prefix_dir = format!("{}/{}", self.path, prefix);
            let prefix_url = format!("{}{}", self.base_url, prefix_dir);
            
            debug!("检查前缀目录: {}", prefix_url);
            let dir_res = self.client.head(&prefix_url)
                .basic_auth(&self.username, Some(&self.password))
                .send()
                .await?;
                
            if !dir_res.status().is_success() {
                // 如果前缀目录不存在，则该前缀下的所有文件都缺失
                debug!("前缀目录不存在: {}，添加{}个文件到缺失列表", prefix, prefix_files.len());
                missing_files.extend(prefix_files.clone());
                
                // 记录目录不存在
                prefix_dirs.insert(prefix.clone(), false);
            } else {
                // 前缀目录存在，添加到目录缓存
                {
                    let mut dirs = self.existing_dirs.write().await;
                    dirs.insert(prefix_dir);
                }
                
                // 记录目录存在
                prefix_dirs.insert(prefix.clone(), true);
                
                // 如果前缀目录存在，只检查目录中的少量文件来判断是否需要逐个检查
                // 选择该前缀下的第一个文件进行检查
                if let Some(sample_file) = prefix_files.first() {
                    let exists = self.exists(&sample_file.hash).await?;
                    
                    if !exists {
                        // 如果样本文件不存在，我们假设前缀目录是空的或者所有文件都需要下载
                        debug!("前缀目录{}存在但样本文件不存在，可能是空目录，添加{}个文件到缺失列表", 
                            prefix, prefix_files.len());
                        missing_files.extend(prefix_files.clone());
                    } else {
                        // 样本文件存在，逐个检查其余文件
                        debug!("前缀目录{}存在且有文件，将逐个检查剩余文件", prefix);
                        
                        // 从第二个文件开始检查，因为第一个已经检查过了
                        for file in prefix_files.iter().skip(1) {
                            if !self.exists(&file.hash).await? {
                                missing_files.push(file.clone());
                            }
                        }
                    }
                }
            }
        }
        
        debug!("文件检查完成，共发现{}个缺失文件", missing_files.len());
        Ok(missing_files)
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
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
        
        // 添加重试逻辑
        let mut retry_count = 0;
        let max_retries = 3;
        
        loop {
            let res = self.client.request(reqwest::Method::from_bytes(b"MKCOL").unwrap(), &url)
                .basic_auth(&self.username, Some(&self.password))
                .send()
                .await?;
                
            if res.status().is_success() {
                debug!("成功创建目录: {}", path);
                // 添加到目录缓存
                let mut dirs = self.existing_dirs.write().await;
                dirs.insert(path.to_string());
                return Ok(());
            } else if res.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED || 
                      res.status() == reqwest::StatusCode::CONFLICT {
                // 405表示目录已存在，409表示冲突(可能因为目录已存在)
                debug!("目录可能已存在 (状态: {}): {}", res.status(), path);
                // 添加到目录缓存
                let mut dirs = self.existing_dirs.write().await;
                dirs.insert(path.to_string());
                return Ok(());
            } else if res.status() == reqwest::StatusCode::LOCKED && retry_count < max_retries {
                // 423表示资源被锁定，等待一段时间后重试
                retry_count += 1;
                warn!("目录被锁定 (状态: 423): {}，等待重试 ({}/{})", path, retry_count, max_retries);
                tokio::time::sleep(std::time::Duration::from_millis(500 * retry_count)).await;
                continue;
            } else {
                return Err(anyhow!("创建WebDAV目录失败: {} - {}", path, res.status()));
            }
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

impl Clone for AlistWebdavStorage {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            path: self.path.clone(),
            redirect_cache: self.redirect_cache.clone(),
            empty_files: self.empty_files.clone(),
            files: self.files.clone(),
            existing_dirs: self.existing_dirs.clone(),
            cache_ttl: self.cache_ttl,
        }
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
            empty_files.insert(path.clone());
            debug!("注册空文件: {}", path);
            return Ok(());
        }
        
        // 确保使用正确的路径格式
        let check_path = if path.contains('/') {
            // 如果路径中已经包含了前缀（如"b4/b46e2617fd37a31a751e639e4ede3b70aaaf2465"）
            path.clone()
        } else if path.len() >= 2 {
            // 如果路径没有前缀，且长度足够，添加前缀
            let prefix = &path[0..2];
            
            // 确保哈希前缀目录存在
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
            
            format!("{}/{}", prefix, path)
        } else {
            // 路径太短，直接使用原始路径
            path.clone()
        };
        
        // 目标文件URL
        let file_url = format!("{}{}/{}", self.base_url, self.path, check_path);
        debug!("直接上传文件到WebDAV最终位置: {} (原始路径: {})", file_url, path);
        
        // 直接上传到最终位置
        let res = self.client.put(&file_url)
            .basic_auth(&self.username, Some(&self.password))
            .body(content)
            .send()
            .await?;
            
        if res.status().is_success() {
            // 添加到文件缓存
            let mut files = self.files.write().await;
            files.insert(file_info.hash.clone(), file_info.clone());
            debug!("文件成功上传到最终位置: {}", file_url);
            Ok(())
        } else {
            Err(anyhow!("上传文件失败: {} - {} (URL: {})", path, res.status(), file_url))
        }
    }
    
    async fn exists(&self, path: &str) -> Result<bool> {
        // 检查是否是空文件
        if self.is_empty_file(path).await {
            debug!("文件存在检查: 空文件: {}", path);
            return Ok(true);
        }
        
        // 检查缓存中是否存在
        {
            let files = self.files.read().await;
            if files.contains_key(path) {
                debug!("文件存在检查: 缓存命中: {}", path);
                return Ok(true);
            }
        }
        
        // 确保我们使用的是正确的路径格式（检查是否有前缀/）
        let check_path = if path.contains('/') {
            // 如果路径中已经包含了前缀（如"b4/b46e2617fd37a31a751e639e4ede3b70aaaf2465"）
            // 则直接使用该路径
            path.to_string()
        } else if path.len() >= 2 {
            // 如果路径没有前缀，且长度足够，添加前缀（如"b46e2617fd37a31a751e639e4ede3b70aaaf2465"转为"b4/b46e2617fd37a31a751e639e4ede3b70aaaf2465"）
            let prefix = &path[0..2];
            format!("{}/{}", prefix, path)
        } else {
            // 路径太短，直接使用原始路径
            path.to_string()
        };
        
        // 构建文件URL
        let file_path = format!("{}/{}", self.path, check_path);
        let file_url = format!("{}{}", self.base_url, file_path);
        
        debug!("检查文件是否存在: {} (URL: {})", path, file_url);
        
        // 检查文件是否存在
        let mut retry_count = 0;
        let max_retries = 2;
        
        while retry_count <= max_retries {
            match self.client.head(&file_url)
                .basic_auth(&self.username, Some(&self.password))
                .send()
                .await {
                Ok(res) => {
                    let exists = res.status().is_success();
                    if exists {
                        debug!("文件存在检查: 文件存在: {} (URL: {})", path, file_url);
                        // 如果文件存在，添加到缓存
                        if let Some(content_length) = res.headers().get("content-length") {
                            if let Ok(size_str) = content_length.to_str() {
                                if let Ok(size) = size_str.parse::<u64>() {
                                    // 获取到文件大小后，可以添加到文件缓存
                                    debug!("获取到文件大小: {} = {} 字节", path, size);
                                }
                            }
                        }
                    } else {
                        debug!("文件存在检查: 文件不存在: {} (URL: {})", path, file_url);
                    }
                    return Ok(exists);
                },
                Err(e) => {
                    retry_count += 1;
                    if retry_count > max_retries {
                        warn!("检查文件存在失败(重试{}次): {}: {}", retry_count, file_url, e);
                        return Err(anyhow!("检查文件存在失败: {}: {}", file_url, e));
                    }
                    debug!("检查文件存在失败，尝试重试({}/{}): {}: {}", 
                          retry_count, max_retries, file_url, e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
        }
        
        // 这行代码不应该被执行到，因为前面的while循环会返回结果或抛出错误
        Err(anyhow!("检查文件存在失败: 意外的执行流程"))
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
        
        // 先检查所有前缀目录是否存在(可以并行检查)
        let client = self.client.clone();
        let username = self.username.clone();
        let password = self.password.clone();
        let base_url = self.base_url.clone();
        let path = self.path.clone();
        
        use futures::future::join_all;
        
        // 并发检查所有前缀目录
        let prefix_checks = files_by_prefix.keys().map(|prefix| {
            let prefix = prefix.clone();
            let client = client.clone();
            let username = username.clone();
            let password = password.clone();
            let base_url = base_url.clone();
            let path = path.clone();
            
            async move {
                let prefix_dir = format!("{}/{}", path, prefix);
                let prefix_url = format!("{}{}", base_url, prefix_dir);
                
                debug!("检查前缀目录: {}", prefix_url);
                let mut retry_count = 0;
                let max_retries = 2;
                
                while retry_count <= max_retries {
                    match client.head(&prefix_url)
                        .basic_auth(&username, Some(&password))
                        .send()
                        .await {
                        Ok(res) => {
                            return (prefix, prefix_dir, res.status().is_success());
                        },
                        Err(e) => {
                            retry_count += 1;
                            if retry_count > max_retries {
                                warn!("检查前缀目录失败(重试{}次): {}: {}", retry_count, prefix_dir, e);
                                return (prefix, prefix_dir, false);
                            }
                            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                        }
                    }
                }
                
                (prefix, prefix_dir, false)
            }
        }).collect::<Vec<_>>();
        
        let prefix_results = join_all(prefix_checks).await;
        
        // 处理前缀目录检查结果
        for (prefix, prefix_dir, exists) in prefix_results {
            if !exists {
                // 如果前缀目录不存在，则该前缀下的所有文件都缺失
                if let Some(files) = files_by_prefix.get(&prefix) {
                    debug!("前缀目录不存在: {}，添加{}个文件到缺失列表", prefix, files.len());
                    missing_files.extend(files.clone());
                }
                
                // 记录目录不存在
                prefix_dirs.insert(prefix, false);
            } else {
                // 前缀目录存在，添加到目录缓存
                {
                    let mut dirs = self.existing_dirs.write().await;
                    dirs.insert(prefix_dir);
                }
                
                // 记录目录存在
                prefix_dirs.insert(prefix.clone(), true);
            }
        }
        
        // 逐个检查前缀目录存在的文件
        let mut file_results = Vec::new();
        
        // 处理每个存在的前缀目录
        for (prefix, exists) in &prefix_dirs {
            if !*exists {
                continue; // 跳过不存在的目录，这些目录中的文件已经添加到缺失列表
            }
            
            // 获取此前缀下的所有文件
            if let Some(prefix_files) = files_by_prefix.get(prefix) {
                debug!("前缀目录{}存在，将逐个检查文件是否存在", prefix);
                
                // 并发检查最多10个文件
                let batch_size = 10;
                for chunk in prefix_files.chunks(batch_size) {
                    let mut batch_futures = Vec::new();
                    
                    // 为每个文件创建检查任务
                    for file in chunk {
                        let storage_self = self.clone();
                        let file_clone = file.clone();
                        
                        batch_futures.push(async move {
                            debug!("检查文件 {} 在目录 {} 中是否存在", file_clone.hash, prefix);
                            
                            // 检查文件 - 注意：这里使用原始哈希值，避免路径重复添加前缀
                            let hash = &file_clone.hash;
                            
                            // 优先从缓存检查
                            if storage_self.is_empty_file(hash).await {
                                debug!("文件 {} 在缓存中存在(空文件)", hash);
                                return (file_clone, true);
                            }
                            
                            // 检查文件存在性
                            match storage_self.exists(hash).await {
                                Ok(true) => {
                                    debug!("文件 {} 已存在于WebDAV中", hash);
                                    (file_clone, true)
                                },
                                _ => {
                                    debug!("文件 {} 不存在，添加到缺失列表", hash);
                                    (file_clone, false)
                                }
                            }
                        });
                    }
                    
                    // 并发执行一批检查
                    let batch_results = join_all(batch_futures).await;
                    file_results.extend(batch_results);
                }
            }
        }
        
        // 处理文件检查结果
        for (file, exists) in file_results {
            if !exists {
                missing_files.push(file);
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
        
        // 确保使用正确的路径格式
        let check_path = if hash_path.contains('/') {
            // 如果路径中已经包含了前缀（如"b4/b46e2617fd37a31a751e639e4ede3b70aaaf2465"）
            hash_path.to_string()
        } else if hash_path.len() >= 2 {
            // 如果路径没有前缀，且长度足够，添加前缀
            let prefix = &hash_path[0..2];
            format!("{}/{}", prefix, hash_path)
        } else {
            // 路径太短，直接使用原始路径
            hash_path.to_string()
        };
        
        // 构建文件URL
        let file_url = format!("{}{}/{}", self.base_url, self.path, check_path);
        debug!("处理文件请求: {} (URL: {})", hash_path, file_url);
        
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
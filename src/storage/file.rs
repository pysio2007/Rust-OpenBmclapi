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
use tokio::sync::Semaphore;
use std::sync::Arc;
use futures::stream::{self, StreamExt};

use log::{info, warn, error, debug};
use crate::storage::base::Storage;
use crate::types::{FileInfo, GCCounter};
use crate::util::{hash_to_filename, validate_file};

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: u64 = 5;

pub struct FileStorage {
    cache_dir: PathBuf,
    verified_hashes: Arc<tokio::sync::Mutex<HashSet<String>>>,
}

impl FileStorage {
    pub fn new(cache_dir: PathBuf) -> Self {
        Self { 
            cache_dir,
            verified_hashes: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
        }
    }
    
    async fn verify_file(&self, path: &str, expected_hash: &str) -> Result<bool> {
        let file_path = self.cache_dir.join(path);
        
        // 检查是否已验证过
        {
            let verified = self.verified_hashes.lock().await;
            if verified.contains(expected_hash) {
                return Ok(true);
            }
        }
        
        debug!("验证文件: {}, 预期哈希: {}", file_path.display(), expected_hash);
        
        // 读取文件内容
        let content = match fs::read(&file_path).await {
            Ok(content) => content,
            Err(e) => {
                error!("读取文件失败: {} - {}", file_path.display(), e);
                return Ok(false);
            }
        };
        
        // 使用公共验证函数
        let is_valid = validate_file(&content, expected_hash);
        
        if !is_valid {
            warn!("文件哈希验证失败: {}", file_path.display());
        } else {
            debug!("文件哈希验证成功: {}", file_path.display());
            // 添加到已验证集合
            let mut verified = self.verified_hashes.lock().await;
            verified.insert(expected_hash.to_string());
        }
        
        Ok(is_valid)
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
            return Err(anyhow::anyhow!("文件目录不是一个目录"));
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
        let total_files = files.len();
        info!("开始检查 {} 个文件", total_files);
        
        // 首先预扫描并建立索引，避免逐个查询文件系统
        info!("预扫描文件目录，建立索引...");
        let mut _file_index: std::collections::HashMap<String, std::path::PathBuf> = std::collections::HashMap::new();
        let mut _size_index: std::collections::HashMap<std::path::PathBuf, u64> = std::collections::HashMap::new();
        
        // 使用多线程IO扫描文件系统
        let scan_start = std::time::Instant::now();
        let cache_dir = self.cache_dir.clone();
        
        // 使用多线程扫描方式
        let file_index_result = tokio::task::spawn_blocking(move || {
            let mut file_index: std::collections::HashMap<String, std::path::PathBuf> = std::collections::HashMap::new();
            let mut size_index: std::collections::HashMap<std::path::PathBuf, u64> = std::collections::HashMap::new();
            
            fn scan_dir(dir: &std::path::Path, file_index: &mut std::collections::HashMap<String, std::path::PathBuf>, 
                       size_index: &mut std::collections::HashMap<std::path::PathBuf, u64>) -> Result<()> {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    
                    if path.is_dir() {
                        scan_dir(&path, file_index, size_index)?;
                    } else if let Some(file_name) = path.file_name() {
                        if let Some(file_name_str) = file_name.to_str() {
                            file_index.insert(file_name_str.to_lowercase(), path.clone());
                            
                            // 同时缓存文件大小信息
                            if let Ok(metadata) = std::fs::metadata(&path) {
                                size_index.insert(path, metadata.len());
                            }
                        }
                    }
                }
                Ok(())
            }
            
            // 确保目录存在
            if !cache_dir.exists() {
                let _ = std::fs::create_dir_all(&cache_dir);
                return (file_index, size_index);
            }
            
            if let Err(e) = scan_dir(&cache_dir, &mut file_index, &mut size_index) {
                eprintln!("扫描目录时出错: {}", e);
            }
            
            (file_index, size_index)
        }).await.unwrap();
        
        let (file_index, size_index) = file_index_result;
        let scan_duration = scan_start.elapsed();
        info!("目录扫描完成，耗时 {:.2}s，索引 {} 个文件", 
              scan_duration.as_secs_f64(), file_index.len());
        
        // 获取当前时间戳，用于决定是否进行随机抽检
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // 减少抽检比例，提高性能
        let verification_ratio = 0.005; // 只对0.5%的文件进行完整校验
        
        // 分批处理文件检查，每批8000个文件
        let batch_size = 8000;
        let mut missing_files = Vec::new();
        
        for (batch_index, file_batch) in files.chunks(batch_size).enumerate() {
            let batch_start = std::time::Instant::now();
            info!("处理批次 {}/{} (每批 {} 个文件)...", 
                 batch_index + 1, (total_files + batch_size - 1) / batch_size, batch_size);
            
            // 创建一个Arc引用以便在多个任务间共享
            let self_arc = Arc::new(self.clone());
            let file_index = Arc::new(file_index.clone());
            let size_index = Arc::new(size_index.clone());
            
            // 使用200个并发任务进行检查
            let semaphore = Arc::new(Semaphore::new(200));
            
            // 使用一个计数器跟踪进度
            let checked_count = Arc::new(tokio::sync::Mutex::new(0));
            let batch_size = file_batch.len();
            
            // 准备批次的文件
            let files_vec: Vec<FileInfo> = file_batch.to_vec();
            
            // 并发处理此批次
            let results = stream::iter(files_vec)
                .map(|file| {
                    let self_clone = self_arc.clone();
                    let sem_clone = semaphore.clone();
                    let counter_clone = checked_count.clone();
                    let file_index = file_index.clone();
                    let size_index = size_index.clone();
                    
                    async move {
                        // 获取信号量许可，限制并发数量
                        let _permit = sem_clone.acquire().await.unwrap();
                        
                        let hash = file.hash.to_lowercase();
                        let hash_path = hash_to_filename(&hash);
                        
                        // 更新计数器并记录进度
                        {
                            let mut count = counter_clone.lock().await;
                            *count += 1;
                            if *count % 1000 == 0 {
                                debug!("批次进度: {}/{}", *count, batch_size);
                            }
                        }
                        
                        // 使用索引快速检查文件是否存在
                        let file_parts: Vec<&str> = hash_path.split('/').collect();
                        let file_name = if file_parts.len() > 1 { file_parts[1] } else { &hash };
                        
                        if !file_index.contains_key(file_name) {
                            return (file, true); // 文件缺失
                        }
                        
                        // 获取文件路径
                        let file_path = match file_index.get(file_name) {
                            Some(path) => path,
                            None => return (file, true), // 不应该发生，但为了安全考虑
                        };
                        
                        // 使用索引快速检查文件大小
                        if let Some(size) = size_index.get(file_path) {
                            if *size != file.size {
                                debug!("文件大小不匹配: {} (期望: {}, 实际: {})", 
                                      file.path, file.size, size);
                                return (file, true); // 文件大小不符
                            }
                        } else {
                            // 如果没有在索引中找到大小，则回退到传统检查
                            match tokio::fs::metadata(file_path).await {
                                Ok(metadata) => {
                                    if metadata.len() != file.size {
                                        debug!("文件大小不匹配: {} (期望: {}, 实际: {})", 
                                              file.path, file.size, metadata.len());
                                        return (file, true);
                                    }
                                },
                                Err(_) => return (file, true),
                            }
                        }
                        
                        // 随机抽检
                        let should_verify = {
                            let first_byte = u8::from_str_radix(&hash[0..2], 16).unwrap_or(0);
                            let random_value = (first_byte as f64 / 255.0 + (timestamp % 100) as f64 / 100.0) % 1.0;
                            random_value < verification_ratio
                        };
                        
                        if should_verify {
                            debug!("对文件进行随机抽检: {}", file.path);
                            match self_clone.verify_file(&hash_path, &hash).await {
                                Ok(true) => (file, false),
                                _ => (file, true), // 文件验证失败或错误
                            }
                        } else {
                            (file, false) // 文件存在且大小匹配
                        }
                    }
                })
                .buffer_unordered(200) // 最多同时进行200个并发任务
                .collect::<Vec<(FileInfo, bool)>>()
                .await;
            
            // 处理本批次结果
            let batch_missing = results.into_iter()
                .filter_map(|(file, is_missing)| if is_missing { Some(file) } else { None })
                .collect::<Vec<_>>();
            
            let batch_duration = batch_start.elapsed();
            info!("批次 {}/{} 完成，耗时 {:.2}s，发现 {} 个缺失文件", 
                 batch_index + 1, (total_files + batch_size - 1) / batch_size, 
                 batch_duration.as_secs_f64(), batch_missing.len());
            
            // 添加到总缺失文件列表
            missing_files.extend(batch_missing);
        }
        
        if !missing_files.is_empty() {
            info!("检查完成，共发现 {} 个缺失或损坏的文件", missing_files.len());
        } else {
            info!("检查完成，所有文件完整性检查通过");
        }
        
        Ok(missing_files)
    }
    
    async fn gc(&self, files: &[FileInfo]) -> Result<GCCounter> {
        let mut counter = GCCounter::default();
        
        // 构建有效文件的集合
        let file_set: HashSet<String> = files.iter()
            .map(|file| file.hash.to_lowercase())
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
                
                // 从文件路径中提取哈希值
                if let Some(file_name) = path.file_name().and_then(|n| n.to_str()) {
                    let hash = file_name.to_lowercase();
                    if !file_set.contains(&hash) {
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

// 为了使FileStorage可以被Clone
impl Clone for FileStorage {
    fn clone(&self) -> Self {
        Self {
            cache_dir: self.cache_dir.clone(),
            verified_hashes: self.verified_hashes.clone(),
        }
    }
} 
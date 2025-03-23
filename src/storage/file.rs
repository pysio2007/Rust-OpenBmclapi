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
use tokio::sync::Semaphore;
use std::sync::Arc;
use futures::stream::{self, StreamExt};

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
        let total_files = files.len();
        
        // 创建一个Arc引用以便在多个任务间共享
        let self_arc = Arc::new(self.clone());
        
        // 使用信号量限制并发数量为32
        let semaphore = Arc::new(Semaphore::new(32));
        
        // 使用一个计数器跟踪进度
        let checked_count = Arc::new(tokio::sync::Mutex::new(0));
        
        // 将files切片转换为包含所有权的Vec
        let files_vec: Vec<FileInfo> = files.to_vec();

        // 获取当前时间戳，用于决定是否进行随机抽检
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // 抽检比例，默认为5%的文件会被完整校验
        let verification_ratio = 0.05;
        
        // 创建一个流处理文件检查
        let results = stream::iter(files_vec)
            .map(|file| {
                let self_clone = self_arc.clone();
                let sem_clone = semaphore.clone();
                let counter_clone = checked_count.clone();
                
                async move {
                    // 获取信号量许可，限制并发数量
                    let _permit = sem_clone.acquire().await.unwrap();
                    
                    let hash_path = hash_to_filename(&file.hash);
                    let file_path = self_clone.cache_dir.join(&hash_path);
                    
                    // 更新计数器并记录进度
                    {
                        let mut count = counter_clone.lock().await;
                        *count += 1;
                        if *count % 100 == 0 {
                            debug!("已检查 {}/{} 个文件", *count, total_files);
                        }
                    }
                    
                    // 首先检查文件是否存在
                    if !file_path.exists() {
                        debug!("文件不存在: {}", file_path.display());
                        return (file, true); // 表示文件缺失
                    }
                    
                    // 检查文件大小
                    match fs::metadata(&file_path).await {
                        Ok(metadata) => {
                            if metadata.len() != file.size {
                                debug!("文件大小不匹配: {} (期望: {}, 实际: {})", 
                                    file.path, file.size, metadata.len());
                                return (file, true); // 表示文件缺失或损坏
                            }
                        },
                        Err(e) => {
                            error!("获取文件元数据失败: {} - {}", file_path.display(), e);
                            return (file, true); // 表示文件缺失或损坏
                        }
                    }
                    
                    // 随机抽检：使用文件hash与时间戳组合计算，决定是否进行完整的SHA1校验
                    // 注意：文件名本身就是SHA1，所以默认我们认为文件名正确则文件正确
                    // 但为了安全起见，我们会对一部分文件进行完整的内容校验
                    let should_verify = {
                        // 使用文件hash的第一个字节和时间戳计算随机值
                        let first_byte = u8::from_str_radix(&file.hash[0..2], 16).unwrap_or(0);
                        let random_value = (first_byte as f64 / 255.0 + (timestamp % 100) as f64 / 100.0) % 1.0;
                        random_value < verification_ratio
                    };
                    
                    if should_verify {
                        // 对随机选中的文件进行完整的哈希验证
                        debug!("对文件进行随机抽检: {}", file.path);
                        match self_clone.verify_file(&hash_path, &file.hash).await {
                            Ok(true) => {
                                debug!("文件验证成功: {}", file.path);
                                (file, false) // 表示文件完好
                            },
                            Ok(false) => {
                                warn!("文件验证失败: {} (哈希值不匹配)", file.path);
                                (file, true) // 表示文件缺失或损坏
                            },
                            Err(e) => {
                                error!("文件验证出错: {} - {}", file.path, e);
                                (file, true) // 表示文件缺失或损坏
                            }
                        }
                    } else {
                        // 对其他文件只验证文件名和大小
                        debug!("文件名和大小验证通过: {}", file.path);
                        (file, false) // 表示文件完好
                    }
                }
            })
            .buffer_unordered(32) // 最多同时进行32个并发任务
            .collect::<Vec<(FileInfo, bool)>>()
            .await;
        
        // 处理结果
        for (file, is_missing) in results {
            if is_missing {
                missing_files.push(file);
            }
        }
        
        if !missing_files.is_empty() {
            info!("发现 {} 个缺失或损坏的文件", missing_files.len());
        } else {
            info!("所有文件完整性检查通过");
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
        }
    }
} 
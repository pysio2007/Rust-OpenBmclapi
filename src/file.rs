use sha1::{Digest, Sha1};
use std::path::Path;
use tokio::fs;
use anyhow::Result;

// 验证文件是否完整
pub async fn validate_file<P: AsRef<Path>>(path: P, expected_hash: &str, expected_size: u64) -> Result<bool> {
    let content = fs::read(path).await?;
    
    if content.len() as u64 != expected_size {
        return Ok(false);
    }
    
    let mut hasher = Sha1::new();
    hasher.update(&content);
    let hash = format!("{:x}", hasher.finalize());
    
    Ok(hash.to_lowercase() == expected_hash.to_lowercase())
} 
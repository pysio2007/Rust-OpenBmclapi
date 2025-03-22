use anyhow::Result;
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::fs;

// 验证文件是否完整
pub async fn validate_file<P: AsRef<Path>>(path: P, expected_hash: &str, expected_size: u64) -> Result<bool> {
    let metadata = fs::metadata(&path).await?;
    
    if metadata.len() != expected_size {
        return Ok(false);
    }
    
    let content = fs::read(&path).await?;
    let mut hasher = Sha256::new();
    hasher.update(&content);
    let hash = hex::encode(hasher.finalize());
    
    Ok(hash.to_lowercase() == expected_hash.to_lowercase())
} 
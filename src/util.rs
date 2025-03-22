use anyhow::Result;
use log::error;
use sha2::{Digest, Sha256};
use std::path::Path;

// 将哈希值转换为文件名
pub fn hash_to_filename(hash: &str) -> String {
    let hash = hash.to_lowercase();
    if hash.len() < 3 {
        return hash;
    }
    
    format!("{}/{}/{}", &hash[0..1], &hash[1..2], hash)
}

// 计算文件的SHA256哈希值
pub async fn calculate_file_hash<P: AsRef<Path>>(path: P) -> Result<String> {
    let data = tokio::fs::read(path).await?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let result = hasher.finalize();
    
    Ok(hex::encode(result))
}

// 检查签名
pub fn check_sign(data: &[u8], sign: &str, secret: &str) -> bool {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.update(secret.as_bytes());
    let result = hasher.finalize();
    let computed_sign = hex::encode(result);
    
    sign == computed_sign
} 
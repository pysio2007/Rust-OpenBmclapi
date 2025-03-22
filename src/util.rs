use anyhow::{Result, anyhow};
use sha2::{Digest, Sha256};
use std::path::Path;
use std::collections::BTreeMap;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

// 将哈希值转换为文件名
pub fn hash_to_filename(hash: &str) -> String {
    // 在hash的每两个字符之间插入/
    let mut result = String::new();
    for (i, c) in hash.chars().enumerate() {
        if i > 0 && i % 2 == 0 {
            result.push('/');
        }
        result.push(c);
    }
    result
}

// 计算文件的SHA256哈希值
pub async fn calculate_file_hash<P: AsRef<Path>>(path: P) -> Result<String> {
    let data = tokio::fs::read(path).await?;
    let mut hasher = Sha256::new();
    hasher.update(&data);
    let result = hasher.finalize();
    
    Ok(hex::encode(result))
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    data: BTreeMap<String, String>,
    exp: usize,
}

/// 验证JWT令牌
pub fn verify_token(token: &str, secret: &str) -> Result<BTreeMap<String, String>> {
    let decoding_key = DecodingKey::from_secret(secret.as_bytes());
    let validation = Validation::default();
    
    let token_data = decode::<Claims>(token, &decoding_key, &validation)
        .map_err(|e| anyhow!("Token验证失败: {}", e))?;
    
    Ok(token_data.claims.data)
}

/// 创建JWT令牌
pub fn create_token(claims: &BTreeMap<String, String>, secret: &str) -> Result<String> {
    let exp = chrono::Utc::now()
        .checked_add_signed(chrono::Duration::hours(24))
        .expect("有效的时间戳")
        .timestamp() as usize;
    
    let claims = Claims {
        data: claims.clone(),
        exp,
    };
    
    let encoding_key = EncodingKey::from_secret(secret.as_bytes());
    let token = encode(&Header::default(), &claims, &encoding_key)
        .map_err(|e| anyhow!("Token创建失败: {}", e))?;
    
    Ok(token)
}

// 创建所有父目录
pub fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }
    Ok(())
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
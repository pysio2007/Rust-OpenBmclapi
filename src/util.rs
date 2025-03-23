use anyhow::{Result, anyhow};
use sha2::{Digest as Sha2Digest, Sha256};
use std::path::Path;
use std::collections::BTreeMap;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha1::Sha1;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

// 将哈希值转换为文件名
pub fn hash_to_filename(hash: &str) -> String {
    let prefix = &hash[0..2];
    Path::new(prefix).join(hash).to_string_lossy().to_string()
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

/// 检查签名是否有效（匹配NodeJS版本的checkSign函数）
pub fn check_sign(hash: &str, secret: &str, query: &HashMap<String, String>) -> bool {
    // 获取签名和过期时间
    let sign = match query.get("s") {
        Some(s) => s,
        None => return false,
    };
    
    let expire = match query.get("e") {
        Some(e) => e,
        None => return false,
    };
    
    // 计算签名
    let mut hasher = Sha1::new();
    hasher.update(secret.as_bytes());
    hasher.update(hash.as_bytes());
    hasher.update(expire.as_bytes());
    let digest = hasher.finalize();
    
    // Base64 URL Safe 编码
    let calculated_sign = URL_SAFE_NO_PAD.encode(digest);
    
    // 检查签名是否匹配，并且是否过期
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("获取系统时间失败")
        .as_millis();
        
    let expire_time = match i64::from_str_radix(expire, 36) {
        Ok(time) => time as u128,
        Err(_) => return false,
    };
    
    sign == &calculated_sign && now < expire_time
} 
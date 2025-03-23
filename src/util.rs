use anyhow::{Result, anyhow};
use sha1::{Digest as Sha1Digest, Sha1};
use std::path::Path;
use std::collections::BTreeMap;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use std::fs;
use log;
use md5;

// 将哈希值转换为文件名
pub fn hash_to_filename(hash: &str) -> String {
    // 始终使用 / 作为分隔符，避免平台差异
    format!("{}/{}", &hash[0..2], hash)
}

// 计算文件的SHA1哈希值
pub async fn calculate_file_hash<P: AsRef<Path>>(path: P) -> Result<String> {
    let data = fs::read(path)?;
    let mut hasher = Sha1::new();
    hasher.update(&data);
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
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
pub fn check_sign(path: &str, secret: &str, query: &HashMap<String, String>) -> bool {
    // 获取签名和过期时间
    let sign = match query.get("s") {
        Some(s) => s,
        None => return false,
    };
    
    let expire = match query.get("e") {
        Some(e) => e,
        None => return false,
    };
    
    // 处理路径 - 确保与TS版本一致
    // TypeScript版本直接使用hash作为路径参数，不带前缀
    let actual_path = if path.starts_with("/download/") {
        // 如果传入的是带/download/前缀的路径，提取出hash
        path.strip_prefix("/download/").unwrap_or(path)
    } else {
        // 已经是hash或其他格式的路径
        path
    };
    
    log::debug!("计算签名: secret={}, path={}, expire={}", secret, actual_path, expire);
    
    // 计算签名
    let mut hasher = Sha1::new();
    hasher.update(secret.as_bytes());
    hasher.update(actual_path.as_bytes());
    hasher.update(expire.as_bytes());
    let digest = hasher.finalize();
    
    // Base64 URL Safe 编码
    let calculated_sign = URL_SAFE_NO_PAD.encode(digest);
    
    log::debug!("签名比较: 计算值={}, 请求值={}", calculated_sign, sign);
    
    // 检查签名是否匹配，并且是否过期
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("获取系统时间失败")
        .as_millis();
        
    let expire_time = match i64::from_str_radix(expire, 36) {
        Ok(time) => time as u128,
        Err(_) => return false,
    };
    
    let sign_match = sign == &calculated_sign;
    let not_expired = now < expire_time;
    
    log::debug!("签名验证结果: 签名匹配={}, 未过期={}, 当前时间={}, 过期时间={}", 
           sign_match, not_expired, now, expire_time);
    
    sign_match && not_expired
}

// 修改validate_file函数，添加忽略大小写和跨平台路径处理，并支持SHA1和MD5两种哈希算法
pub fn validate_file(data: &[u8], hash: &str) -> bool {
    let expected = hash.to_lowercase().trim().to_string();
    
    // 根据哈希长度判断使用哪种算法
    if expected.len() == 32 {
        // MD5哈希 (32位)
        let digest = md5::compute(data);
        let actual = format!("{:x}", digest).to_lowercase();
        
        if expected != actual {
            log::debug!("MD5哈希校验失败 - 文件大小: {} 字节, 计算哈希: {}, 期望哈希: {}", data.len(), actual, expected);
        }
        
        actual == expected
    } else {
        // SHA1哈希 (40位)
        use sha1::{Sha1, Digest};
        let mut hasher = Sha1::new();
        hasher.update(data);
        let actual = format!("{:x}", hasher.finalize()).to_lowercase();
        
        if expected != actual {
            log::debug!("SHA1哈希校验失败 - 文件大小: {} 字节, 计算哈希: {}, 期望哈希: {}", data.len(), actual, expected);
        }
        
        actual == expected
    }
} 
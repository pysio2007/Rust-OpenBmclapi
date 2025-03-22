use anyhow::Result;
use jsonwebtoken::{encode, EncodingKey, Header};
use log::info;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: u64,
    iat: u64,
}

#[derive(Debug, Clone)]
pub struct TokenManager {
    cluster_id: String,
    cluster_secret: String,
    #[allow(dead_code)]
    version: String,
    token: Arc<RwLock<Option<String>>>,
    refresh_lock: Arc<Mutex<()>>,
}

impl TokenManager {
    pub fn new(cluster_id: String, cluster_secret: String, version: String) -> Self {
        TokenManager {
            cluster_id,
            cluster_secret,
            version,
            token: Arc::new(RwLock::new(None)),
            refresh_lock: Arc::new(Mutex::new(())),
        }
    }

    pub async fn get_token(&self) -> Result<String> {
        if let Some(token) = self.token.read().unwrap().clone() {
            return Ok(token);
        }

        let _lock = self.refresh_lock.lock().await;
        
        // 在获取锁后再次检查，以避免多次生成令牌
        if let Some(token) = self.token.read().unwrap().clone() {
            return Ok(token);
        }
        
        let token = self.generate_token()?;
        *self.token.write().unwrap() = Some(token.clone());
        
        Ok(token)
    }

    fn generate_token(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("获取系统时间失败")
            .as_secs();
        
        let claims = Claims {
            sub: self.cluster_id.clone(),
            exp: now + 3600, // 令牌有效期1小时
            iat: now,
        };
        
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.cluster_secret.as_bytes()),
        )?;
        
        info!("生成新的访问令牌");
        
        Ok(token)
    }
} 
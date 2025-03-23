use anyhow::{anyhow, Result};
use hmac::{Hmac, Mac};
use log::{debug, error, trace, info};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::sleep;

// 定义一个常量用于存储User-Agent基础信息
const USER_AGENT: &str = "rust-openbmclapi-cluster";

#[derive(Debug, Serialize, Deserialize)]
struct Challenge {
    challenge: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TokenResponse {
    token: String,
    ttl: u64,
}

#[derive(Debug, Clone)]
pub struct TokenManager {
    cluster_id: String,
    cluster_secret: String,
    version: String,
    token: Arc<RwLock<Option<String>>>,
    client: Client,
    prefix_url: String,
    user_agent: String, // 添加一个字段存储完整的User-Agent字符串
}

impl TokenManager {
    pub fn new(cluster_id: String, cluster_secret: String, version: String) -> Self {
        let prefix_url = env::var("CLUSTER_BMCLAPI")
            .unwrap_or_else(|_| "https://openbmclapi.bangbang93.com".to_string());
        
        // 创建完整的User-Agent字符串
        let user_agent = format!("{}/{}", USER_AGENT, version);
        
        // 创建HTTP客户端
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(&user_agent)
            .build()
            .unwrap_or_else(|_| Client::new());

        TokenManager {
            cluster_id,
            cluster_secret,
            version: version.to_string(),
            token: Arc::new(RwLock::new(None)),
            client,
            prefix_url,
            user_agent,
        }
    }

    pub async fn get_token(&self) -> Result<String> {
        // 如果已有令牌，直接返回
        if let Some(token) = self.token.read().await.clone() {
            return Ok(token);
        }

        // 获取新令牌
        let token = self.fetch_token().await?;
        *self.token.write().await = Some(token.clone());
        Ok(token)
    }

    async fn fetch_token(&self) -> Result<String> {
        // 添加调试输出
        debug!("DEBUG: TokenManager版本号: {}", self.version);
        debug!("DEBUG: TokenManager UA: {}", self.user_agent);
        
        // 1. 请求challenge
        let challenge_url = format!("{}/openbmclapi-agent/challenge", self.prefix_url);
        debug!("DEBUG: 请求challenge URL: {}", challenge_url);
        
        let response = self.client
            .get(&challenge_url)
            .query(&[("clusterId", &self.cluster_id)])
            .header("user-agent", &self.user_agent)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("请求challenge失败: {}", response.status()));
        }

        let challenge: Challenge = response.json().await?;

        // 2. 计算签名
        let mut mac = Hmac::<Sha256>::new_from_slice(self.cluster_secret.as_bytes())
            .map_err(|_| anyhow!("HMAC初始化失败"))?;
        mac.update(challenge.challenge.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        // 3. 请求令牌
        let token_url = format!("{}/openbmclapi-agent/token", self.prefix_url);
        let response = self.client
            .post(&token_url)
            .json(&serde_json::json!({
                "clusterId": self.cluster_id,
                "challenge": challenge.challenge,
                "signature": signature,
            }))
            .header("user-agent", &self.user_agent)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("请求令牌失败: {}", response.status()));
        }

        let token_response: TokenResponse = response.json().await?;
        
        // 4. 设置定时刷新令牌
        self.schedule_refresh_token(token_response.ttl);
        
        Ok(token_response.token)
    }

    fn schedule_refresh_token(&self, ttl: u64) {
        let next_ms = std::cmp::max(ttl.saturating_sub(600_000), ttl / 2); // ttl - 10分钟或ttl/2
        info!("计划在{}ms后刷新令牌", next_ms);
        
        // 克隆需要的变量，避免使用self
        let cluster_id = self.cluster_id.clone();
        let cluster_secret = self.cluster_secret.clone();
        let version = self.version.clone();
        let token = self.token.clone();
        let client = self.client.clone();
        let prefix_url = self.prefix_url.clone();
        let user_agent = self.user_agent.clone();
        
        tokio::spawn(async move {
            sleep(Duration::from_millis(next_ms)).await;
            
            // 创建一个本地的令牌管理器实例
            let current_token = match token.read().await.clone() {
                Some(t) => t,
                None => {
                    error!("无令牌可刷新");
                    return;
                }
            };
            
            // 执行令牌刷新
            let token_url = format!("{}/openbmclapi-agent/token", prefix_url);
            let response = match client
                .post(&token_url)
                .json(&serde_json::json!({
                    "clusterId": cluster_id,
                    "token": current_token,
                }))
                .header("user-agent", &user_agent)
                .send()
                .await {
                    Ok(resp) => resp,
                    Err(err) => {
                        error!("刷新令牌请求失败: {}", err);
                        return;
                    }
                };
            
            if !response.status().is_success() {
                error!("刷新令牌失败: {}", response.status());
                return;
            }
            
            // 解析响应
            let token_response: TokenResponse = match response.json().await {
                Ok(tr) => tr,
                Err(err) => {
                    error!("解析令牌响应失败: {}", err);
                    return;
                }
            };
            
            debug!("成功刷新令牌");
            
            // 更新令牌
            *token.write().await = Some(token_response.token);
            
            // 安排下一次刷新
            let next_ms = std::cmp::max(token_response.ttl.saturating_sub(600_000), token_response.ttl / 2);
            let token_clone = token.clone();
            let cluster_id_clone = cluster_id.clone();
            let client_clone = client.clone();
            let prefix_url_clone = prefix_url.clone();
            let version_clone = version.clone();
            let cluster_secret_clone = cluster_secret.clone();
            let user_agent_clone = user_agent.clone();
            
            tokio::spawn(async move {
                sleep(Duration::from_millis(next_ms)).await;
                let token_manager = TokenManager {
                    cluster_id: cluster_id_clone,
                    cluster_secret: cluster_secret_clone,
                    version: version_clone,
                    token: token_clone,
                    client: client_clone,
                    prefix_url: prefix_url_clone,
                    user_agent: user_agent_clone,
                };
                
                if let Err(err) = token_manager.refresh_token().await {
                    error!("刷新令牌失败: {}", err);
                }
            });
        });
    }

    async fn refresh_token(&self) -> Result<()> {
        let current_token = match self.token.read().await.clone() {
            Some(token) => token,
            None => return Err(anyhow!("无令牌可刷新")),
        };

        let token_url = format!("{}/openbmclapi-agent/token", self.prefix_url);
        let response = self.client
            .post(&token_url)
            .json(&serde_json::json!({
                "clusterId": self.cluster_id,
                "token": current_token,
            }))
            .header("user-agent", &self.user_agent)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow!("刷新令牌失败: {}", response.status()));
        }

        let token_response: TokenResponse = response.json().await?;
        debug!("成功刷新令牌");
        
        *self.token.write().await = Some(token_response.token);
        self.schedule_refresh_token(token_response.ttl);
        
        Ok(())
    }
} 
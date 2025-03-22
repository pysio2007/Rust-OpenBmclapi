use anyhow::Result;
use lazy_static::lazy_static;
use log::info;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigFlavor {
    pub runtime: String,
    pub storage: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    // 基本配置
    pub cluster_id: String,
    pub cluster_secret: String,
    pub cluster_ip: Option<String>,
    pub port: u16,
    pub cluster_public_port: u16,
    pub byoc: bool,
    pub disable_access_log: bool,
    
    // 存储配置
    pub storage: String,
    pub storage_opts: Option<serde_json::Value>,
    
    // SSL配置
    pub ssl_key: Option<String>,
    pub ssl_cert: Option<String>,
    
    // 功能开关
    pub enable_nginx: bool,
    pub enable_upnp: bool,
    pub enable_metrics: bool,
    
    // 高级选项
    pub debug_log: bool,
    
    // 运行时信息
    pub flavor: ConfigFlavor,
}

impl Config {
    pub fn new() -> Result<Self> {
        let cluster_id = env::var("CLUSTER_ID").expect("CLUSTER_ID环境变量必须设置");
        let cluster_secret = env::var("CLUSTER_SECRET").expect("CLUSTER_SECRET环境变量必须设置");
        let cluster_ip = env::var("CLUSTER_IP").ok();
        
        let port_str = env::var("CLUSTER_PORT").unwrap_or_else(|_| "4000".to_string());
        let port = port_str.parse::<u16>().expect("CLUSTER_PORT必须是有效的端口号");
        
        let public_port_str = env::var("CLUSTER_PUBLIC_PORT").unwrap_or_else(|_| port_str.clone());
        let cluster_public_port = public_port_str.parse::<u16>().expect("CLUSTER_PUBLIC_PORT必须是有效的端口号");
        
        let byoc = env::var("CLUSTER_BYOC").map(|v| v == "true" || v == "1").unwrap_or(false);
        let disable_access_log = env::var("DISABLE_ACCESS_LOG").map(|v| v == "true" || v == "1").unwrap_or(false);
        
        let enable_nginx = env::var("ENABLE_NGINX").map(|v| v == "true" || v == "1").unwrap_or(false);
        let enable_upnp = env::var("ENABLE_UPNP").map(|v| v == "true" || v == "1").unwrap_or(false);
        let enable_metrics = env::var("ENABLE_METRICS").map(|v| v == "true" || v == "1").unwrap_or(false);
        
        let storage = env::var("CLUSTER_STORAGE").unwrap_or_else(|_| "file".to_string());
        let storage_opts = env::var("CLUSTER_STORAGE_OPTIONS")
            .map(|v| serde_json::from_str(&v).unwrap_or(serde_json::Value::Null))
            .ok();
        
        let ssl_key = env::var("SSL_KEY").ok();
        let ssl_cert = env::var("SSL_CERT").ok();
        
        // 高级选项
        let debug_log = env::var("DEBUG_LOG").map(|v| v == "true" || v == "1").unwrap_or(false);
        
        let flavor = ConfigFlavor {
            runtime: format!("Rust/{}", env!("CARGO_PKG_VERSION")),
            storage: storage.clone(),
        };
        
        Ok(Config {
            cluster_id,
            cluster_secret,
            cluster_ip,
            port,
            cluster_public_port,
            byoc,
            disable_access_log,
            enable_nginx,
            enable_upnp,
            enable_metrics,
            storage,
            storage_opts,
            ssl_key,
            ssl_cert,
            debug_log,
            flavor,
        })
    }
}

lazy_static! {
    pub static ref CONFIG: Arc<RwLock<Config>> = {
        let config = Config::new().expect("配置初始化失败");
        info!("配置已加载");
        Arc::new(RwLock::new(config))
    };
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenbmclapiAgentConfiguration {
    pub sync: SyncConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    pub source: String,
    pub concurrency: usize,
} 
use anyhow::Result;
use lazy_static::lazy_static;
use log::info;
use serde::{Deserialize, Serialize};
use std::env;
use std::sync::Arc;
use std::sync::RwLock;
use std::path::Path;
use std::fs;

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
    pub alist_opts: Option<AlistStorageConfig>,
    
    // SSL配置
    pub ssl_key: Option<String>,
    pub ssl_cert: Option<String>,
    
    // 功能开关
    pub enable_upnp: bool,
    pub enable_metrics: bool,
    
    // 高级选项
    pub debug_log: bool,
    
    // 运行时信息
    pub flavor: ConfigFlavor,
}

/// AList WebDAV存储配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlistStorageConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    pub base_path: String,
    pub cache_ttl: String,
}

impl Config {
    // 创建默认的.env文件模板（如果不存在）
    fn create_default_env_file() -> Result<()> {
        let env_path = ".env";
        if !Path::new(env_path).exists() {
            let env_content = 
                "# 请填写以下必要配置项\n\
                 NO_DAEMON=1\n\
                 # 请替换为你的CLUSTER_ID（必填）\n\
                 CLUSTER_ID=\n\
                 # 请替换为你的CLUSTER_SECRET（必填）\n\
                 CLUSTER_SECRET=\n\
                 # 是否启用UPnP自动端口映射功能（可选，默认关闭）\n\
                 # 如果你的路由器支持UPnP且需要自动端口映射，设置为true\n\
                 # ENABLE_UPNP=false\n\
                 # 设置你的公网IP或内网IP（可选）\n\
                 # CLUSTER_IP=\n\
                 # 设置服务监听端口（可选，默认4000）\n\
                 # CLUSTER_PORT=4000\n\
                 # 设置对外公开的端口（可选，默认与监听端口相同）\n\
                 # CLUSTER_PUBLIC_PORT=4000\n\
                 # 是否启用指标收集（可选，默认关闭）\n\
                 # ENABLE_METRICS=false\n\
                 # 存储类型（可选，默认file）\n\
                 # CLUSTER_STORAGE=file\n\
                 \n\
                 # 如果使用Alist存储，可以通过以下两种方式之一配置:\n\
                 # 1. JSON方式配置:\n\
                 # CLUSTER_STORAGE_OPTIONS={\"url\":\"http://example.com/dav\",\"username\":\"user\",\"password\":\"pass\",\"basePath\":\"/openbmclapi\"}\n\
                 # 2. 分离环境变量方式配置:\n\
                 # ALIST_URL=http://example.com/dav\n\
                 # ALIST_USERNAME=user\n\
                 # ALIST_PASSWORD=pass\n\
                 # ALIST_BASE_PATH=/openbmclapi\n";
            
            fs::write(env_path, env_content)?;
            info!("已创建.env文件模板，请填写必要的配置项");
        }
        Ok(())
    }

    pub fn new() -> Result<Self> {
        // 尝试创建默认的.env文件模板
        Self::create_default_env_file()?;
        
        // 默认为单进程模式
        env::set_var("NO_DAEMON", "1");
        
        let cluster_id = match env::var("CLUSTER_ID") {
            Ok(id) if !id.trim().is_empty() => id,
            _ => {
                return Err(anyhow::anyhow!("CLUSTER_ID环境变量未设置或为空，请在.env文件中填写必要的配置项"));
            }
        };
        
        let cluster_secret = match env::var("CLUSTER_SECRET") {
            Ok(secret) if !secret.trim().is_empty() => secret,
            _ => {
                return Err(anyhow::anyhow!("CLUSTER_SECRET环境变量未设置或为空，请在.env文件中填写必要的配置项"));
            }
        };
        
        let cluster_ip = env::var("CLUSTER_IP").ok();
        
        let port_str = env::var("CLUSTER_PORT").unwrap_or_else(|_| "4000".to_string());
        let port = port_str.parse::<u16>().expect("CLUSTER_PORT必须是有效的端口号");
        
        let public_port_str = env::var("CLUSTER_PUBLIC_PORT").unwrap_or_else(|_| port_str.clone());
        let cluster_public_port = public_port_str.parse::<u16>().expect("CLUSTER_PUBLIC_PORT必须是有效的端口号");
        
        let byoc = env::var("CLUSTER_BYOC").map(|v| v == "true" || v == "1").unwrap_or(false);
        let disable_access_log = env::var("DISABLE_ACCESS_LOG").map(|v| v == "true" || v == "1").unwrap_or(false);
        
        let enable_upnp = env::var("ENABLE_UPNP").map(|v| v == "true" || v == "1").unwrap_or(false);
        let enable_metrics = env::var("ENABLE_METRICS").map(|v| v == "true" || v == "1").unwrap_or(false);
        
        let storage = env::var("CLUSTER_STORAGE").unwrap_or_else(|_| "file".to_string());
        
        // 存储选项可以通过两种方式配置：
        // 1. JSON方式: CLUSTER_STORAGE_OPTIONS={"url":"http://example.com/dav","username":"user",...}
        // 2. 单独环境变量方式: 使用ALIST_URL、ALIST_USERNAME等变量
        let storage_opts = env::var("CLUSTER_STORAGE_OPTIONS")
            .map(|v| serde_json::from_str(&v).unwrap_or(serde_json::Value::Null))
            .ok();
            
        // 解析ALIST配置 (如果存储类型是alist)
        let alist_opts = if storage == "alist" {
            Some(Self::parse_alist_config(&storage_opts))
        } else {
            None
        };
        
        let ssl_key = env::var("SSL_KEY").ok();
        let ssl_cert = env::var("SSL_CERT").ok();
        
        // 高级选项
        let debug_log = env::var("DEBUG_LOG").map(|v| v == "true" || v == "1").unwrap_or(false);
        
        let flavor = ConfigFlavor {
            runtime: format!("Rust/{}", "1.85.1"),
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
            enable_upnp,
            enable_metrics,
            storage,
            storage_opts,
            alist_opts,
            ssl_key,
            ssl_cert,
            debug_log,
            flavor,
        })
    }
    
    /// 解析ALIST存储的配置，支持两种模式：JSON和独立环境变量
    fn parse_alist_config(storage_opts: &Option<serde_json::Value>) -> AlistStorageConfig {
        // 如果有JSON配置，优先使用
        if let Some(opts) = storage_opts {
            if let Some(obj) = opts.as_object() {
                info!("使用JSON配置Alist WebDAV存储");
                
                // 从JSON获取参数
                let url = obj.get("url")
                    .and_then(|v| v.as_str())
                    .unwrap_or("").trim().to_string();
                    
                let username = obj.get("username")
                    .and_then(|v| v.as_str())
                    .unwrap_or("").to_string();
                    
                let password = obj.get("password")
                    .and_then(|v| v.as_str())
                    .unwrap_or("").to_string();
                    
                let base_path = obj.get("basePath")
                    .and_then(|v| v.as_str())
                    .unwrap_or("/").trim().to_string();
                    
                let cache_ttl = obj.get("cacheTtl")
                    .map(|v| {
                        if let Some(s) = v.as_str() {
                            s.to_string()
                        } else if let Some(n) = v.as_i64() {
                            n.to_string()
                        } else {
                            "1h".to_string()
                        }
                    })
                    .unwrap_or_else(|| "1h".to_string());
                
                if url.is_empty() {
                    panic!("错误: WebDAV URL不能为空，请检查配置");
                }
                
                return AlistStorageConfig {
                    url,
                    username,
                    password,
                    base_path,
                    cache_ttl,
                };
            }
        }
        
        // 从环境变量获取配置
        info!("使用环境变量配置Alist WebDAV存储");
        let url = env::var("ALIST_URL")
            .or_else(|_| env::var("WEBDAV_URL"))
            .unwrap_or_else(|_| "".to_string())
            .trim().to_string();
            
        let username = env::var("ALIST_USERNAME")
            .or_else(|_| env::var("WEBDAV_USERNAME"))
            .unwrap_or_else(|_| "".to_string());
            
        let password = env::var("ALIST_PASSWORD")
            .or_else(|_| env::var("WEBDAV_PASSWORD"))
            .unwrap_or_else(|_| "".to_string());
            
        let base_path = env::var("ALIST_BASE_PATH")
            .or_else(|_| env::var("WEBDAV_BASE_PATH"))
            .unwrap_or_else(|_| "/".to_string())
            .trim().to_string();
            
        let cache_ttl = env::var("ALIST_CACHE_TTL")
            .unwrap_or_else(|_| "1h".to_string());
        
        if url.is_empty() {
            panic!("错误: WebDAV URL不能为空，请检查环境变量ALIST_URL或WEBDAV_URL");
        }
        
        AlistStorageConfig {
            url,
            username,
            password,
            base_path,
            cache_ttl,
        }
    }
    
    /// 获取Alist存储的配置对象
    pub fn get_alist_config(&self) -> Option<serde_json::Value> {
        if let Some(config) = &self.alist_opts {
            let mut map = serde_json::Map::new();
            
            // 转换url为合法URL
            let url = if !config.url.starts_with("http://") && !config.url.starts_with("https://") {
                format!("http://{}", config.url)
            } else {
                config.url.clone()
            };
            
            // 转换路径以确保有前导斜杠
            let base_path = if !config.base_path.starts_with("/") {
                format!("/{}", config.base_path)
            } else {
                config.base_path.clone()
            };
            
            map.insert("url".to_string(), serde_json::Value::String(url));
            map.insert("username".to_string(), serde_json::Value::String(config.username.clone()));
            map.insert("password".to_string(), serde_json::Value::String(config.password.clone()));
            map.insert("basePath".to_string(), serde_json::Value::String(base_path));
            map.insert("cacheTtl".to_string(), serde_json::Value::String(config.cache_ttl.clone()));
            
            Some(serde_json::Value::Object(map))
        } else {
            None
        }
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
    #[serde(default)]  // 添加 default 属性，使其可选
    pub remote_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    pub source: String,
    pub concurrency: usize,
} 
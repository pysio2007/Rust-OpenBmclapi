use anyhow::{anyhow, Result};
use axum::body::Body;
use axum::{response::Response, routing::get, Router};
use axum::extract::{Query, State, Path};
use axum::http::{StatusCode, Request};
use axum::response::IntoResponse;
use reqwest::Client;
use serde_json::json;   
use std::collections::HashMap;
use log::{debug, error, info, warn};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use futures::FutureExt;
use rust_socketio::{
    asynchronous::{Client as SocketClient, ClientBuilder},
    Payload, TransportType,
};
use indicatif;

use crate::config::CONFIG;
use crate::config::{OpenbmclapiAgentConfiguration, SyncConfig};
use crate::storage::Storage;
use crate::storage::get_storage;
use crate::token::TokenManager;
use crate::types::{Counters, FileInfo, FileList};
use crate::upnp;

const USER_AGENT: &str = "openbmclapi-cluster/1.13.1 rust-openbmclapi-cluster";

pub struct Cluster {
    client: Client,
    storage: Arc<Box<dyn Storage>>,
    token_manager: Arc<TokenManager>,
    version: String,
    host: Option<String>,
    port: u16,
    public_port: u16,
    is_enabled: Arc<RwLock<bool>>,
    want_enable: Arc<RwLock<bool>>,
    counters: Arc<RwLock<Counters>>,
    base_url: String,
    tmp_dir: PathBuf,
    cert_key_files: Arc<RwLock<Option<(PathBuf, PathBuf)>>>,
    socket: Arc<RwLock<Option<SocketClient>>>,
    user_agent: String,
    cache_dir: PathBuf,
}

impl Cluster {
    pub fn new(version: &str, token_manager: Arc<TokenManager>) -> Result<Self> {
        let config = CONFIG.read().unwrap().clone();
        
        debug!("DEBUG: Cluster创建，传入版本号: {}", version);
        
        let user_agent = format!("{}/{}", USER_AGENT, version);
        debug!("DEBUG: Cluster UA: {}", user_agent);
        
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(&user_agent)
            .build()?;
        
        let storage = Arc::new(get_storage(&config));
        
        let base_url = std::env::var("CLUSTER_BMCLAPI")
            .unwrap_or_else(|_| "https://openbmclapi.bangbang93.com".to_string());
            
        let tmp_dir = std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join(".ssl");
        std::fs::create_dir_all(&tmp_dir)?;

        // 从环境变量获取cache目录位置，如果未设置则使用默认的cache目录
        let cache_dir = std::env::var("BMCLAPI_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join("cache"));
        
        // 确保cache目录存在
        std::fs::create_dir_all(&cache_dir)?;
        info!("使用缓存目录: {:?}", cache_dir);
        
        Ok(Cluster {
            client,
            storage,
            token_manager,
            version: version.to_string(),
            host: config.cluster_ip,
            port: config.port,
            public_port: config.cluster_public_port,
            is_enabled: Arc::new(RwLock::new(false)),
            want_enable: Arc::new(RwLock::new(false)),
            counters: Arc::new(RwLock::new(Counters::default())),
            base_url,
            tmp_dir,
            cert_key_files: Arc::new(RwLock::new(None)),
            socket: Arc::new(RwLock::new(None)),
            user_agent,
            cache_dir,
        })
    }
    
    pub async fn init(&self) -> Result<()> {
        self.storage.init().await?;
        
        let config = CONFIG.read().unwrap().clone();
        if config.enable_upnp {
            match upnp::setup_upnp(config.port, config.cluster_public_port).await {
                Ok(ip) => {
                    if upnp::is_public_ip(&ip) {
                        info!("UPnP端口映射成功，外网IP: {}", ip);
                        
                        if self.host.is_none() {
                            let mut config = CONFIG.write().unwrap();
                            config.cluster_ip = Some(ip);
                        }
                    } else {
                        warn!("UPnP返回的IP不是公网IP: {}", ip);
                    }
                },
                Err(e) => {
                    warn!("UPnP端口映射失败: {}", e);
                }
            }
        }
        
        Ok(())
    }
    
    pub async fn is_enabled(&self) -> bool {
        *self.is_enabled.read().unwrap()
    }
    
    pub async fn want_enable(&self) -> bool {
        *self.want_enable.read().unwrap()
    }
    
    pub async fn request_cert(&self) -> bool {
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        let socket = match socket_opt {
            Some(socket) => socket,
            None => {
                warn!("没有可用的Socket.IO连接，请先调用connect()方法建立连接");
                return false;
            },
        };

        tokio::time::sleep(Duration::from_millis(200)).await;
        let tmp_dir = self.tmp_dir.clone();
        let cert_key_files = self.cert_key_files.clone();
        
        let ack_callback = move |message: Payload, _| {
            let tmp_dir = tmp_dir.clone();
            let cert_key_files = cert_key_files.clone();
            async move {
                info!("收到证书响应回调");
                match message {
                    Payload::Text(values) => {
                        debug!("处理证书响应的文本数据: {:?}", values);
                        if values.is_empty() {
                            error!("证书响应为空");
                            return;
                        }
                        
                        let outer_array = match values.get(0) {
                            Some(array) if array.is_array() => array.as_array().unwrap(),
                            _ => {
                                error!("证书响应格式错误: 第一元素不是数组");
                                return;
                            }
                        };
                        
                        let inner_array = match outer_array.get(0) {
                            Some(array) if array.is_array() => array.as_array().unwrap(),
                            _ => {
                                error!("证书响应格式错误: 第二层元素不是数组");
                                return;
                            }
                        };
                        
                        if inner_array.len() < 2 {
                            error!("证书响应格式错误: 内层数组长度不足，需要至少两个元素");
                            return;
                        }
                        
                        let cert_data = &inner_array[1];
                        if !cert_data.is_object() {
                            error!("证书响应格式错误: 内层数组的第二个元素不是对象，实际值: {:?}", cert_data);
                            return;
                        }
                        
                        let cert = cert_data.get("cert");
                        let key = cert_data.get("key");
                        
                        if cert.is_none() || key.is_none() {
                            error!("证书响应中缺少cert或key字段: {:?}", cert_data);
                            return;
                        }
                        
                        let cert = cert.unwrap();
                        let key = key.unwrap();
                        
                        if !cert.is_string() || !key.is_string() {
                            error!("证书或密钥不是字符串格式: cert类型={}, key类型={}", 
                                  cert.is_string(), key.is_string());
                            return;
                        }
                        
                        let cert_file = tmp_dir.join("cert.pem");
                        let key_file = tmp_dir.join("key.pem");
                        
                        debug!("准备保存证书到: {:?}", cert_file);
                        debug!("准备保存密钥到: {:?}", key_file);
                        
                        if let Some(parent) = cert_file.parent() {
                            if !parent.exists() {
                                match tokio::fs::create_dir_all(parent).await {
                                    Ok(_) => debug!("创建证书目录成功: {:?}", parent),
                                    Err(e) => {
                                        error!("创建证书目录失败: {:?}, 错误: {}", parent, e);
                                        return;
                                    }
                                }
                            }
                        }
                        
                        if cert_file.exists() {
                            if let Err(e) = tokio::fs::remove_file(&cert_file).await {
                                error!("删除现有证书文件失败: {}", e);
                            }
                        }
                        
                        if key_file.exists() {
                            if let Err(e) = tokio::fs::remove_file(&key_file).await {
                                error!("删除现有密钥文件失败: {}", e);
                            }
                        }

                        let cert_str = cert.as_str().unwrap();
                        let key_str = key.as_str().unwrap();
                        
                        match tokio::fs::write(&cert_file, cert_str).await {
                            Ok(_) => info!("成功写入证书文件"),
                            Err(e) => {
                                error!("写入证书文件失败: {}", e);
                                return;
                            }
                        }
                        
                        match tokio::fs::write(&key_file, key_str).await {
                            Ok(_) => info!("成功写入密钥文件"),
                            Err(e) => {
                                error!("写入密钥文件失败: {}", e);
                                return;
                            }
                        }
                        
                        let mut cert_files_guard = cert_key_files.write().unwrap();
                        *cert_files_guard = Some((cert_file.clone(), key_file.clone()));
                        info!("已更新证书和密钥文件路径记录");
                    },
                    _ => error!("收到非文本格式的证书响应: {:?}", message),
                }
            }.boxed()
        };

        info!("发送证书请求...");
        let res = socket
            .emit_with_ack("request-cert", "", Duration::from_secs(10), ack_callback)
            .await;
            
        tokio::time::sleep(Duration::from_secs(5)).await;
        
        let cert_files = self.cert_key_files.read().unwrap();
        let file_exists = if let Some((cert_path, key_path)) = &*cert_files {
            let cert_exists = std::path::Path::new(cert_path).exists();
            let key_exists = std::path::Path::new(key_path).exists();
            
            if !cert_exists {
                error!("证书文件不存在: {:?}", cert_path);
            }
            
            if !key_exists {
                error!("密钥文件不存在: {:?}", key_path);
            }
            
            cert_exists && key_exists
        } else {
            error!("没有记录证书和密钥文件路径");
            false
        };
        
        if res.is_err() {
            error!("请求证书失败: {:?}", res.err());
            false
        } else if !file_exists {
            error!("证书请求成功但文件未正确保存");
            false
        } else {
            info!("成功获取并保存证书和密钥");
            true
        }
    }
    
    pub async fn use_self_cert(&self) -> Result<()> {
        let config = CONFIG.read().unwrap().clone();
        
        if config.ssl_cert.is_none() || config.ssl_key.is_none() {
            return Err(anyhow!("未提供SSL证书或密钥"));
        }
        
        let ssl_cert = config.ssl_cert.unwrap();
        let ssl_key = config.ssl_key.unwrap();
        
        let cert_path = self.tmp_dir.join("cert.pem");
        let key_path = self.tmp_dir.join("key.pem");
        
        info!("使用自定义证书: {:?}", ssl_cert);
        
        if std::path::Path::new(&ssl_cert).exists() {
            tokio::fs::copy(&ssl_cert, &cert_path).await?;
        } else {
            tokio::fs::write(&cert_path, ssl_cert).await?;
        }
        
        if std::path::Path::new(&ssl_key).exists() {
            tokio::fs::copy(&ssl_key, &key_path).await?;
        } else {
            tokio::fs::write(&key_path, ssl_key).await?;
        }
        
        {
            let mut cert_files = self.cert_key_files.write().unwrap();
            *cert_files = Some((cert_path, key_path));
        }
        
        Ok(())
    }
    
    pub async fn setup_server_with_https(&self, use_https: bool) -> Result<Router> {
        let router = self.create_router();
        
        if use_https {
            let cert_files = self.cert_key_files.read().unwrap();
            if cert_files.is_none() {
                return Err(anyhow!("未找到SSL证书，无法启动HTTPS服务器"));
            }
            
            info!("已配置HTTPS服务器，证书就绪");
        }
        
        Ok(router)
    }
    
    fn create_router(&self) -> Router {
        let cluster = Arc::new(self.clone());
        
        Router::new()
            .route("/files/:hash_path", get(serve_file))
            .route("/list/directory", get(
                |State(cluster): State<Arc<Cluster>>, Query(params): axum::extract::Query<HashMap<String, String>>| async move {
                    let sign = params.get("sign");
                    let path = params.get("path").unwrap_or(&String::from("")).clone();
                    
                    if sign.is_none() {
                        return Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Body::from("Missing signature"))
                            .unwrap();
                    }
                
                    let cluster_secret = {
                        let config = CONFIG.read().unwrap();
                        config.cluster_secret.clone()
                    };
                    
                    let verify_path = format!("/list/directory?path={}", path);
                    
                    let mut query_map = HashMap::new();
                    query_map.insert("s".to_string(), sign.unwrap().to_string());
                    if let Some(e) = params.get("e") {
                        query_map.insert("e".to_string(), e.to_string());
                    }
                    
                    if !crate::util::check_sign(&verify_path, &cluster_secret, &query_map) {
                        return Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Body::from("Invalid signature"))
                            .unwrap();
                    }
                
                    let _storage = cluster.get_storage();
                    let storage_path = cluster.cache_dir.join(&path);
                    
                    match tokio::fs::read_dir(storage_path).await {
                        Ok(mut entries) => {
                            let mut files = Vec::new();
                            
                            while let Ok(Some(entry)) = entries.next_entry().await {
                                if let Ok(metadata) = entry.metadata().await {
                                    let entry_type = if metadata.is_dir() { "directory" } else { "file" };
                                    let filename = entry.file_name().to_string_lossy().to_string();
                                    let size = if metadata.is_file() { metadata.len() } else { 0 };
                                    
                                    files.push(serde_json::json!({
                                        "name": filename,
                                        "type": entry_type,
                                        "size": size,
                                    }));
                                }
                            }
                            
                            Response::builder()
                                .status(StatusCode::OK)
                                .header(axum::http::header::CONTENT_TYPE, "application/json")
                                .body(Body::from(serde_json::to_string(&serde_json::json!({
                                    "path": path,
                                    "files": files
                                })).unwrap()))
                                .unwrap()
                        },
                        Err(e) => {
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(Body::from(format!("Failed to read directory: {}", e)))
                                .unwrap()
                        }
                    }
                }
            ))
            .route("/metrics", get(
                |State(cluster): State<Arc<Cluster>>, Query(params): axum::extract::Query<HashMap<String, String>>| async move {
                    let sign = params.get("sign");
                    if sign.is_none() {
                        return Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Body::from("Missing signature"))
                            .unwrap();
                    }
                
                    let cluster_secret = {
                        let config = CONFIG.read().unwrap();
                        config.cluster_secret.clone()
                    };
                    
                    let path = "/metrics";
                    
                    let mut query_map = HashMap::new();
                    query_map.insert("s".to_string(), sign.unwrap().to_string());
                    if let Some(e) = params.get("e") {
                        query_map.insert("e".to_string(), e.to_string());
                    }
                    
                    if !crate::util::check_sign(path, &cluster_secret, &query_map) {
                        return Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Body::from("Invalid signature"))
                            .unwrap();
                    }
                
                    let counters = cluster.counters.read().unwrap().clone();
                    
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(axum::http::header::CONTENT_TYPE, "application/json")
                        .body(Body::from(serde_json::to_string(&serde_json::json!({
                            "metrics": {
                                "hits": counters.hits,
                                "bytes": counters.bytes,
                            },
                            "timestamp": chrono::Utc::now().timestamp()
                        })).unwrap()))
                        .unwrap()
                }
            ))
            .with_state(cluster)
    }
    
    pub async fn enable(&self) -> Result<()> {
        if *self.is_enabled.read().unwrap() {
            debug!("节点已经处于启用状态，无需重复启用");
            return Ok(());
        }
        
        {
            let mut want_enable = self.want_enable.write().unwrap();
            *want_enable = true;
            debug!("已设置 want_enable = true");
        }
        
        info!("开始启用集群节点...");
        
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        let socket = match socket_opt {
            Some(socket) => {
                debug!("获取到有效的Socket.IO连接");
                socket
            },
            None => {
                error!("Socket.IO连接未初始化，请先调用connect()方法建立连接");
                return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接"));
            }
        };
        
        let host = self.host.clone().unwrap_or_else(|| "".to_string());
        debug!("使用主机地址: {}", if host.is_empty() { "<空>" } else { &host });
        
        let byoc = {
            let config = CONFIG.read().unwrap();
            config.byoc
        };
        debug!("BYOC设置: {}", byoc);
        
        let flavor = {
            let config = CONFIG.read().unwrap();
            config.flavor.clone()
        };
        debug!("运行时环境: {}/{}", flavor.runtime, flavor.storage);
        
        let no_fast_enable = std::env::var("NO_FAST_ENABLE").unwrap_or_else(|_| "false".to_string()) == "true";
        debug!("快速启用模式: {}", !no_fast_enable);
        
        let payload = json!({
            "host": host,
            "port": self.public_port,
            "version": env!("CARGO_PKG_VERSION"),
            "byoc": byoc,
            "noFastEnable": no_fast_enable,
            "flavor": flavor,
        });
        
        info!("准备发送的注册数据: {}", payload);
        
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        
        let ack_callback = move |message: Payload, _| {
            let tx = tx.clone();
            async move {
                debug!("收到enable响应回调");
                match message {
                    Payload::Text(values) => {
                        debug!("处理enable响应的文本数据: {:?}", values);
                        
                        // 克隆整个values数组以避免生命周期问题
                        let values = values.to_vec();
                        
                        if values.len() < 2 {
                            let err = "启用集群响应格式错误: 数组长度不足";
                            error!("{} (期望>=2, 实际={})", err, values.len());
                            let _ = tx.send(Err(anyhow!(err))).await;
                            return;
                        }
                        
                        let err_value = values[0].clone();
                        if !err_value.is_null() {
                            if let Some(err_msg) = err_value.get("message").and_then(|v| v.as_str()) {
                                error!("主控服务器返回错误: {}", err_msg);
                                let _ = tx.send(Err(anyhow!(err_msg.to_string()))).await;
                                return;
                            } else {
                                let err = format!("主控服务器返回未知错误: {:?}", err_value);
                                error!("{}", err);
                                let _ = tx.send(Err(anyhow!(err))).await;
                                return;
                            }
                        }
                        
                        let ack_value = values[1].clone();
                        if ack_value.as_bool() != Some(true) {
                            let err = format!("节点注册失败: 服务器未返回成功确认 (返回值: {:?})", ack_value);
                            error!("{}", err);
                            let _ = tx.send(Err(anyhow!(err))).await;
                            return;
                        }
                        
                        info!("节点注册成功，等待集群启用");
                        let _ = tx.send(Ok(())).await;
                    },
                    _ => {
                        let err = format!("收到非文本格式的enable响应: {:?}", message);
                        error!("{}", err);
                        let _ = tx.send(Err(anyhow!(err))).await;
                    }
                }
            }.boxed()
        };
        
        info!("正在发送节点注册请求 (超时时间: 300秒)...");
        let res = socket
            .emit_with_ack("enable", payload, Duration::from_secs(300), ack_callback)
            .await;
            
        if let Err(e) = res {
            error!("发送注册请求失败: {:?}", e);
            return Err(anyhow!("发送注册请求失败: {}", e));
        }
        
        // 等待回调处理完成或超时
        match tokio::time::timeout(Duration::from_secs(300), rx.recv()).await {
            Ok(Some(Ok(()))) => {
                {
                    let mut is_enabled = self.is_enabled.write().unwrap();
                    *is_enabled = true;
                }
                info!("节点注册流程完成，开始工作");
                // 启动 keepalive
                // TODO: 实现 keepalive 启动
                Ok(())
            },
            Ok(Some(Err(e))) => {
                error!("节点注册失败: {}", e);
                Err(e)
            },
            Ok(None) => {
                error!("回调通道已关闭");
                Err(anyhow!("节点注册失败: 回调通道已关闭"))
            },
            Err(_) => {
                error!("节点注册超时");
                Err(anyhow!("节点注册超时"))
            }
        }
    }
    
    pub async fn disable(&self) -> Result<()> {
        if !*self.is_enabled.read().unwrap() {
            return Ok(());
        }
        
        {
            let mut want_enable = self.want_enable.write().unwrap();
            *want_enable = false;
        }
        
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        let socket = match socket_opt {
            Some(socket) => socket,
            None => return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接")),
        };
        
        let is_enabled = self.is_enabled.clone();
        
        let ack_callback = move |message: Payload, _| {
            let is_enabled = is_enabled.clone();
            async move {
                debug!("收到disable响应回调");
                match message {
                    Payload::Text(values) => {
                        debug!("处理disable响应的文本数据: {:?}", values);
                        
                        if values.len() < 2 {
                            error!("禁用集群响应格式错误: 数组长度不足");
                            return;
                        }
                        
                        if !values[0].is_null() {
                            if let Some(err_msg) = values[0].get("message").and_then(|v| v.as_str()) {
                                error!("禁用集群失败: {}", err_msg);
                                return;
                            } else {
                                error!("禁用集群失败: {:?}", values[0]);
                                return;
                            }
                        }
                        
                        if values[1].as_bool() != Some(true) {
                            error!("节点禁用失败: 未收到成功确认");
                            return;
                        }
                        
                        {
                            let mut is_enabled_guard = is_enabled.write().unwrap();
                            *is_enabled_guard = false;
                            info!("集群已成功禁用");
                        }
                    },
                    _ => error!("收到非文本格式的disable响应: {:?}", message),
                }
            }.boxed()
        };
        
        info!("发送禁用请求...");
        let res = socket
            .emit_with_ack("disable", json!(null), Duration::from_secs(30), ack_callback)
            .await;
            
        tokio::time::sleep(Duration::from_secs(3)).await;
        
        if res.is_err() {
            error!("发送禁用请求失败: {:?}", res.err());
            return Err(anyhow!("发送禁用请求失败"));
        }
        
        if *self.is_enabled.read().unwrap() {
            return Err(anyhow!("节点禁用失败或超时"));
        }
        
        Ok(())
    }
    
    pub async fn send_heartbeat(&self) -> Result<()> {
        if !*self.is_enabled.read().unwrap() {
            return Ok(());
        }
        
        let host = self.host.clone().unwrap_or_else(|| "".to_string());
        
        let payload = json!({
            "host": host,
            "port": self.public_port,
            "version": env!("CARGO_PKG_VERSION"),
        });
        
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        let socket = match socket_opt {
            Some(socket) => socket,
            None => return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接")),
        };
        
        let ack_callback = move |message: Payload, _| {
            async move {
                debug!("收到heartbeat响应回调");
                match message {
                    Payload::Text(values) => {
                        debug!("处理heartbeat响应的文本数据: {:?}", values);
                    },
                    _ => error!("收到非文本格式的heartbeat响应: {:?}", message),
                }
            }.boxed()
        };
        
        debug!("发送心跳...");
        let res = socket
            .emit_with_ack("heartbeat", payload, Duration::from_secs(5), ack_callback)
            .await;
            
        if res.is_err() {
            error!("发送心跳失败: {:?}", res.err());
            return Err(anyhow!("发送心跳失败"));
        }
        
        debug!("心跳请求已完成");
        Ok(())
    }
    
    pub async fn connect(&self) -> Result<()> {
        info!("正在建立Socket.IO持久连接到 {}...", self.base_url);
        
        let token = self.token_manager.get_token().await?;
        
        let mut socket_builder = ClientBuilder::new(&self.base_url)
            .transport_type(TransportType::Websocket)
            .auth(json!({"token": token}));
        
        socket_builder = socket_builder.on("connect", {
            let cluster = self.clone();
            move |_: Payload, _: SocketClient| {
                let cluster = cluster.clone();
                async move {
                    info!("Socket.IO连接已建立 - 收到connect事件");
                    
                    let is_enabled = *cluster.is_enabled.read().unwrap();
                    let want_enable = *cluster.want_enable.read().unwrap();
                    info!("当前集群状态 - 是否启用: {}, 是否希望启用: {}", is_enabled, want_enable);
                    
                    if *cluster.want_enable.read().unwrap() && !*cluster.is_enabled.read().unwrap() {
                        info!("检测到连接重新建立，且want_enable=true但is_enabled=false，将尝试重新启用集群");
                        let cluster_clone = cluster.clone();
                        tokio::spawn(async move {
                            info!("开始尝试重新启用集群...");
                            if let Err(e) = cluster_clone.enable().await {
                                error!("自动重新启用集群失败: {}, 错误详情: {:?}", e, e);
                            } else {
                                info!("集群已成功重新启用");
                            }
                        });
                    } else if *cluster.want_enable.read().unwrap() && *cluster.is_enabled.read().unwrap() {
                        info!("集群当前状态正常 (want_enable=true, is_enabled=true)，无需执行额外操作");
                    } else if !*cluster.want_enable.read().unwrap() {
                        info!("集群当前不希望启用 (want_enable=false)，不会尝试重新启用");
                    }
                }.boxed()
            }
        });
        
        info!("Socket.IO配置: URL={}, 传输类型=Websocket", self.base_url);
        
        match socket_builder.connect().await {
            Ok(socket) => {
                info!("Socket.IO连接已建立，准备认证");
                
                tokio::time::sleep(Duration::from_secs(1)).await;
                
                {
                    let mut socket_guard = self.socket.write().unwrap();
                    *socket_guard = Some(socket.clone());
                    debug!("已将新的Socket.IO连接保存到共享状态");
                }
                
                info!("Socket.IO连接已完成初始化并保存");
                
                let cluster = self.clone();
                tokio::spawn(async move {
                    cluster.start_keepalive_task().await;
                });
                
                Ok(())
            },
            Err(e) => {
                error!("Socket.IO连接失败: {}，错误详情: {:?}", e, e);
                
                let err_str = e.to_string();
                if err_str.contains("timeout") {
                    error!("连接超时 - 请检查网络连接和服务器状态");
                } else if err_str.contains("connection") || err_str.contains("connect") {
                    error!("连接错误 - 可能是网络问题或服务器不可用");
                } else if err_str.contains("handshake") {
                    error!("握手错误 - 认证可能失败或服务器不接受连接");
                } else if err_str.contains("auth") {
                    error!("认证错误 - 请检查令牌是否有效");
                }
                
                info!("将在5秒后尝试重新连接");
                
                let base_url = self.base_url.clone();
                let token_manager = self.token_manager.clone();
                let want_enable = Arc::clone(&self.want_enable);
                let _is_enabled = Arc::clone(&self.is_enabled);
                
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    
                    if !*want_enable.read().unwrap() {
                        debug!("集群已不再需要连接，取消重连");
                        return;
                    }
                    
                    let token = match token_manager.get_token().await {
                        Ok(t) => t,
                        Err(e) => {
                            error!("获取令牌失败，无法重连: {}", e);
                            return;
                        }
                    };
                    
                    info!("正在尝试Socket.IO重新连接到 {}...", base_url);
                    match ClientBuilder::new(&base_url)
                        .transport_type(TransportType::Websocket)
                        .auth(json!({"token": token}))
                        .connect().await 
                    {
                        Ok(_socket) => {
                            info!("Socket.IO重连成功，连接已建立");
                        },
                        Err(e) => error!("Socket.IO重新连接失败: {}，错误详情: {:?}", e, e)
                    }
                });
                
                Err(anyhow!("Socket.IO连接失败: {}", e))
            }
        }
    }
    
    async fn start_keepalive_task(&self) {
        info!("启动keep-alive定时任务 (间隔60秒)");
        let mut failed_keepalive = 0;
        
        while *self.is_enabled.read().unwrap() {
            debug!("等待60秒后发送下一次keep-alive...");
            tokio::time::sleep(Duration::from_secs(60)).await;
            debug!("keep-alive计时结束，准备发送keep-alive");
            
            if !*self.is_enabled.read().unwrap() {
                info!("集群已禁用 (is_enabled=false)，停止keep-alive定时任务");
                break;
            }
            
            let current_counter = {
                let counters = self.counters.read().unwrap();
                counters.clone()
            };
            
            let start_time = chrono::Utc::now().timestamp() * 1000;
            let payload = json!({
                "hits": current_counter.hits,
                "bytes": current_counter.bytes,
                "time": start_time
            });
            
            let socket_opt = {
                let socket_guard = self.socket.read().unwrap();
                socket_guard.clone()
            };
            
            if let Some(socket) = socket_opt {
                debug!("发送keep-alive数据: {:?} (当前时间戳: {})", payload, start_time);
                
                let counters = self.counters.clone();
                let current_hits = current_counter.hits;
                let current_bytes = current_counter.bytes;
                
                let ack_callback = move |message: Payload, _| {
                    let counters = counters.clone();
                    let current_hits = current_hits;
                    let current_bytes = current_bytes;
                    async move {
                        debug!("收到keep-alive响应回调");
                        match message {
                            Payload::Text(values) => {
                                debug!("处理keep-alive响应的文本数据: {:?}", values);
                                
                                {
                                    let mut counters_guard = counters.write().unwrap();
                                    counters_guard.hits -= current_hits;
                                    counters_guard.bytes -= current_bytes;
                                    debug!("更新计数器 - 当前计数 hits: {}, bytes: {}", 
                                         counters_guard.hits, counters_guard.bytes);
                                }
                                
                                debug!("keep-alive处理完成");
                            },
                            _ => error!("收到非文本格式的keep-alive响应: {:?}", message),
                        }
                    }.boxed()
                };
                
                match socket.emit_with_ack("keep-alive", payload, Duration::from_secs(10), ack_callback).await {
                    Ok(_) => {
                        if failed_keepalive > 0 {
                            info!("keep-alive发送成功，重置失败计数 (之前失败次数: {})", failed_keepalive);
                        } else {
                            debug!("keep-alive发送成功");
                        }
                        failed_keepalive = 0;
                    },
                    Err(e) => {
                        failed_keepalive += 1;
                        error!("发送keep-alive失败 ({}/3): {}, 错误详情: {:?}", 
                             failed_keepalive, e, e);
                        
                        if failed_keepalive >= 3 {
                            error!("连续3次keep-alive失败，将禁用集群 (当前连接可能已断开)");
                            let _cluster_id = self.token_manager.clone();
                            let want_enable = Arc::clone(&self.want_enable);
                            
                            tokio::spawn(async move {
                                {
                                    let mut want_enable_guard = want_enable.write().unwrap();
                                    let previous = *want_enable_guard;
                                    *want_enable_guard = false;
                                    info!("由于连续keep-alive失败，已设置集群为不希望启用状态 (之前状态: {})", previous);
                                }
                            });
                            break;
                        }
                    }
                }
            } else {
                error!("没有可用的Socket.IO连接，无法发送keep-alive (socket为None)");
                break;
            }
        }
        
        info!("keep-alive定时任务结束");
    }
    
    pub async fn get_file_list(&self, last_modified: Option<u64>) -> Result<FileList> {
        let mut url = format!("{}/openbmclapi/files", self.base_url);
        
        if let Some(lm) = last_modified {
            url = format!("{}?lastModified={}", url, lm);
        }
        
        debug!("DEBUG: 发送请求到: {}", url);
        debug!("DEBUG: User-Agent: {}", self.user_agent);
        debug!("DEBUG: 版本号: {}", self.version);
        
        let token = self.token_manager.get_token().await?;
        
        let response = self.client.get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("user-agent", &self.user_agent)
            .send()
            .await?;
            
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(FileList { files: vec![] });
        }
        
        if response.status().is_success() {
            let is_zstd = response.headers()
                .get("content-type")
                .map(|v| v.to_str().unwrap_or(""))
                .unwrap_or("")
                .contains("application/zstd");
            
            debug!("收到响应，内容类型: {}", 
                response.headers().get("content-type")
                    .map(|v| v.to_str().unwrap_or("未知"))
                    .unwrap_or("无内容类型头"));
                
            let bytes = response.bytes().await?;
            debug!("接收到原始数据大小: {} 字节", bytes.len());
            
            let decompressed_bytes = if is_zstd {
                debug!("收到zstd压缩的文件列表，进行解压缩处理...");
                match zstd::decode_all(bytes.as_ref()) {
                    Ok(decoded) => {
                        debug!("zstd解压缩成功，解压后大小: {}字节", decoded.len());
                        decoded
                    },
                    Err(e) => {
                        error!("zstd解压缩失败: {}", e);
                        return Err(anyhow!("zstd解压缩失败: {}", e));
                    }
                }
            } else {
                debug!("收到未压缩的文件列表");
                bytes.to_vec()
            };
            
            match Self::parse_avro_file_list(&decompressed_bytes) {
                Ok(files) => {
                    info!("成功解析文件列表，共{}个文件", files.len());
                    Ok(FileList { files })
                },
                Err(e) => {
                    error!("解析文件列表失败: {}", e);
                    
                    let preview = if decompressed_bytes.len() > 100 {
                        String::from_utf8_lossy(&decompressed_bytes[0..100]).to_string() + "..."
                    } else {
                        String::from_utf8_lossy(&decompressed_bytes).to_string()
                    };
                    
                    error!("内容预览: {}", preview);
                    Err(anyhow!("解析文件列表失败"))
                }
            }
        } else {
            let status = response.status();
            let text = response.text().await?;
            error!("获取文件列表失败: {} - {}", status, text);
            Err(anyhow!("获取文件列表失败: {} - {}", status, text))
        }
    }
    
    fn parse_avro_file_list(bytes: &[u8]) -> Result<Vec<FileInfo>> {
        if bytes.len() < 8 {
            return Err(anyhow!("数据太短，无法解析"));
        }
        
        let mut cursor = std::io::Cursor::new(bytes);
        
        if let Ok(files) = Self::try_decode_avro_array(&mut cursor) {
            if !files.is_empty() {
                return Ok(files);
            }
        }
        
        for offset in [0, 4, 8, 16, 32, 64] {
            if offset >= bytes.len() {
                continue;
            }
            
            cursor.set_position(offset as u64);
            if let Ok(files) = Self::try_decode_avro_array(&mut cursor) {
                if !files.is_empty() {
                    debug!("在偏移量{}处成功解析到文件列表", offset);
                    return Ok(files);
                }
            }
        }
        
        let mut files = Vec::new();
        cursor.set_position(0);
        
        while cursor.position() + 20 < bytes.len() as u64 {
            let start_pos = cursor.position();
            
            if let Ok(Some(file)) = Self::try_decode_avro_record(&mut cursor) {
                files.push(file);
            } else {
                cursor.set_position(start_pos + 1);
            }
            
            if files.len() > 100000 {
                break;
            }
        }
        
        if !files.is_empty() {
            debug!("成功解析出{}个文件记录", files.len());
            return Ok(files);
        }
        
        match serde_json::from_slice::<Vec<FileInfo>>(bytes) {
            Ok(json_files) => {
                debug!("成功解析JSON格式文件列表，共{}个文件", json_files.len());
                return Ok(json_files);
            },
            Err(e) => {
                debug!("JSON解析失败: {}", e);
            }
        }
        
        Err(anyhow!("无法解析文件列表数据"))
    }
    
    fn try_decode_avro_array(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Vec<FileInfo>> {
        let start_pos = cursor.position();
        
        let array_size = match Self::read_avro_zigzag_long(cursor) {
            Ok(size) if size > 0 && size < 100000 => size as usize,
            _ => {
                cursor.set_position(start_pos);
                return Err(anyhow!("无效的Avro数组大小"));
            }
        };
        
        let mut files = Vec::with_capacity(array_size);
        
        for _ in 0..array_size {
            match Self::try_decode_avro_record(cursor) {
                Ok(Some(file)) => files.push(file),
                _ => {
                    cursor.set_position(start_pos);
                    return Err(anyhow!("解析数组元素失败"));
                }
            }
        }
        
        Ok(files)
    }
    
    fn try_decode_avro_record(cursor: &mut std::io::Cursor<&[u8]>) -> Result<Option<FileInfo>> {
        let start_pos = cursor.position();
        
        let path = match Self::read_avro_string(cursor) {
            Ok(s) if !s.is_empty() => s,
            _ => {
                cursor.set_position(start_pos);
                return Ok(None);
            }
        };
        
        let hash = match Self::read_avro_string(cursor) {
            Ok(s) if !s.is_empty() && s.len() >= 32 => s,
            _ => {
                cursor.set_position(start_pos);
                return Ok(None);
            }
        };
        
        let size = match Self::read_avro_zigzag_long(cursor) {
            Ok(s) if s > 0 => s,
            _ => {
                cursor.set_position(start_pos);
                return Ok(None);
            }
        };
        
        let mtime = match Self::read_avro_zigzag_long(cursor) {
            Ok(m) if m > 0 => m,
            _ => {
                cursor.set_position(start_pos);
                return Ok(None);
            }
        };
        
        if path.contains('/') && path.len() < 500 && hash.len() < 100 {
            return Ok(Some(FileInfo {
                path,
                hash,
                size,
                mtime,
            }));
        }
        
        cursor.set_position(start_pos);
        Ok(None)
    }
    
    fn read_avro_string(cursor: &mut std::io::Cursor<&[u8]>) -> Result<String> {
        use bytes::Buf;
        
        let len = Self::read_avro_zigzag_long(cursor)? as usize;
        
        if len == 0 || len > 10000 {
            return Err(anyhow!("字符串长度不合理: {}", len));
        }
        
        let remaining = cursor.get_ref().len() as u64 - cursor.position();
        if remaining < len as u64 {
            return Err(anyhow!("数据不足，无法读取完整字符串"));
        }
        
        let mut buf = vec![0u8; len];
        cursor.copy_to_slice(&mut buf);
        
        String::from_utf8(buf).map_err(|e| anyhow!("字符串解码失败: {}", e))
    }
    
    fn read_avro_zigzag_long(cursor: &mut std::io::Cursor<&[u8]>) -> Result<u64> {
        use bytes::Buf;
        
        if !cursor.has_remaining() {
            return Err(anyhow!("没有更多数据可读"));
        }
        
        let mut value: u64 = 0;
        let mut shift: u32 = 0;
        
        loop {
            if !cursor.has_remaining() {
                return Err(anyhow!("未完成的VarInt"));
            }
            
            let b = cursor.get_u8();
            value |= ((b & 0x7F) as u64) << shift;
            
            if b & 0x80 == 0 {
                break;
            }
            
            shift += 7;
            if shift > 63 {
                return Err(anyhow!("VarInt太长"));
            }
        }
        
        let decoded = (value >> 1) ^ (-(value as i64 & 1) as u64);
        Ok(decoded as u64)
    }
    
    pub async fn get_configuration(&self) -> Result<OpenbmclapiAgentConfiguration> {
        let url = format!("{}/openbmclapi/configuration", self.base_url);
        debug!("请求配置: {}", url);
        
        let token = self.token_manager.get_token().await?;
        
        let response = self.client.get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .header("user-agent", &self.user_agent)
            .send()
            .await?;
            
        if response.status().is_success() {
            let json_value = response.json::<serde_json::Value>().await?;
            debug!("获取配置成功: {}", json_value);
            
            let sync_value = json_value.get("sync").ok_or_else(|| anyhow!("配置中缺少sync字段"))?;
            
            let source = sync_value.get("source")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("配置中缺少sync.source字段"))?
                .to_string();
                
            let concurrency = sync_value.get("concurrency")
                .and_then(|v| v.as_u64())
                .ok_or_else(|| anyhow!("配置中缺少sync.concurrency字段"))?
                as usize;
                
            let sync_config = SyncConfig {
                source,
                concurrency,
            };
            
            let config = OpenbmclapiAgentConfiguration {
                sync: sync_config,
                remote_url: "".to_string(),
            };
            
            Ok(config)
        } else {
            let status = response.status();
            let text = response.text().await?;
            error!("获取服务器配置失败: {} - {}", status, text);
            Err(anyhow!("获取服务器配置失败: {} - {}", status, text))
        }
    }
    
    pub async fn sync_files(&self, file_list: &FileList, sync_config: &OpenbmclapiAgentConfiguration) -> Result<()> {
        if !self.storage.check().await? {
            return Err(anyhow!("存储异常"));
        }
        
        info!("正在检查缺失文件");
        let missing_files = self.storage.get_missing_files(&file_list.files).await?;
        
        if missing_files.is_empty() {
            return Ok(());
        }
        
        info!("发现 {} 个文件需要同步, 开始同步", missing_files.len());
        info!("{:?} 同步策略", sync_config);
        
        let token = self.token_manager.get_token().await?;
        
        let parallel = sync_config.sync.concurrency;
        let source = &sync_config.sync.source;
        
        let total_count = missing_files.len();
        let mut success_count = 0;
        let mut total_bytes = 0u64;
        
        // 计算总字节数
        for file in &missing_files {
            total_bytes += file.size;
        }
        
        info!("总计需要下载: {} 个文件, {} 字节", total_count, total_bytes);
        
        // 创建多进度条
        let multi_progress = indicatif::MultiProgress::new();
        
        // 创建总进度条
        let total_progress = multi_progress.add(indicatif::ProgressBar::new(total_bytes));
        total_progress.set_style(indicatif::ProgressStyle::default_bar()
            .template("[总进度] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
            
        let multi_progress = Arc::new(multi_progress);
        let total_progress = Arc::new(total_progress);
        
        use futures::stream::{self, StreamExt};
        
        let results = stream::iter(missing_files)
            .map(|file| {
                let client = self.client.clone();
                let token = token.clone();
                let storage = self.storage.clone();
                let _source = source.clone();
                let _base_url = self.base_url.clone();
                let multi_progress = multi_progress.clone();
                let total_progress = total_progress.clone();
                let cache_dir = self.cache_dir.clone();
                
                async move {
                    // 为每个文件创建单独的进度条
                    let file_progress = multi_progress.add(indicatif::ProgressBar::new(file.size));
                    file_progress.set_style(indicatif::ProgressStyle::default_bar()
                        .template("[{filename}] [{bar:40.green/red}] {bytes}/{total_bytes}")
                        .unwrap()
                        .progress_chars("=>-"));
                    
                    let filename = file.path.split('/').last().unwrap_or(&file.path);
                    file_progress.set_message(filename.to_string());
                    
                    info!("开始下载文件: {} (大小: {} 字节)", file.path, file.size);
                    
                    for retry in 0..10 {
                        if retry > 0 {
                            debug!("重试下载文件 {} (第{}次)", file.path, retry);
                        }
                        
                        let path = if file.path.starts_with('/') {
                            file.path[1..].to_string()
                        } else {
                            file.path.clone()
                        };
                        
                        let openbmclapi_url = &_base_url;
                        
                        let url = if openbmclapi_url.ends_with('/') {
                            format!("{}{}", openbmclapi_url, path)
                        } else {
                            format!("{}/{}", openbmclapi_url, path)
                        };
                        
                        debug!("获取文件重定向URL: {}", url);
                        
                        match client.get(&url)
                            .header("Authorization", format!("Bearer {}", token))
                            .send()
                            .await {
                            Ok(response) => {
                                if response.status().is_success() {
                                    match response.bytes().await {
                                        Ok(bytes) => {
                                            debug!("文件 {} 下载完成，大小: {} 字节", file.path, bytes.len());
                                            
                                            let is_file_correct = validate_file(&bytes, &file.hash);
                                            if !is_file_correct {
                                                error!("文件{}校验失败", file.path);
                                                continue;
                                            }
                                            
                                            // 确保目录存在
                                            let hash_path = cache_dir.join(hash_to_filename(&file.hash));
                                            if let Some(parent) = hash_path.parent() {
                                                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                                                    error!("创建目录失败 {}: {}", parent.display(), e);
                                                    continue;
                                                }
                                            }
                                            
                                            if let Err(e) = storage.write_file(hash_path.to_str().unwrap().to_string(), bytes.to_vec(), &file).await {
                                                error!("保存文件 {} 失败: {}", file.path, e);
                                                continue;
                                            }
                                            
                                            file_progress.finish_with_message(format!("{} - 完成", filename));
                                            total_progress.inc(file.size);
                                            debug!("文件 {} 同步成功", file.path);
                                            return Ok(file.path.clone());
                                        }
                                        Err(e) => {
                                            debug!("读取文件 {} 响应体失败: {}", file.path, e);
                                        }
                                    }
                                } else if response.status().is_redirection() {
                                    if let Some(location) = response.headers().get("location") {
                                        if let Ok(redirect_url) = location.to_str() {
                                            debug!("文件 {} 获取到重定向URL: {}", file.path, redirect_url);
                                            
                                            match client.get(redirect_url)
                                                .send()
                                                .await {
                                                Ok(redirect_response) => {
                                                    if redirect_response.status().is_success() {
                                                        match redirect_response.bytes().await {
                                                            Ok(bytes) => {
                                                                debug!("文件 {} 从重定向URL下载完成，大小: {} 字节", file.path, bytes.len());
                                                                
                                                                let is_file_correct = validate_file(&bytes, &file.hash);
                                                                if !is_file_correct {
                                                                    error!("文件{}校验失败", file.path);
                                                                    continue;
                                                                }
                                                                
                                                                // 确保目录存在
                                                                let hash_path = cache_dir.join(hash_to_filename(&file.hash));
                                                                if let Some(parent) = hash_path.parent() {
                                                                    if let Err(e) = tokio::fs::create_dir_all(parent).await {
                                                                        error!("创建目录失败 {}: {}", parent.display(), e);
                                                                        continue;
                                                                    }
                                                                }
                                                                
                                                                if let Err(e) = storage.write_file(hash_path.to_str().unwrap().to_string(), bytes.to_vec(), &file).await {
                                                                    error!("保存文件 {} 失败: {}", file.path, e);
                                                                    continue;
                                                                }
                                                                
                                                                file_progress.finish_with_message(format!("{} - 完成", filename));
                                                                total_progress.inc(file.size);
                                                                debug!("文件 {} 同步成功", file.path);
                                                                return Ok(file.path.clone());
                                                            }
                                                            Err(e) => {
                                                                debug!("读取重定向文件 {} 响应体失败: {}", file.path, e);
                                                            }
                                                        }
                                                    } else {
                                                        debug!("从重定向URL下载文件 {} 失败: HTTP {}", file.path, redirect_response.status());
                                                    }
                                                }
                                                Err(e) => {
                                                    debug!("请求重定向URL {} 失败: {}", redirect_url, e);
                                                }
                                            }
                                        } else {
                                            debug!("解析重定向URL失败");
                                        }
                                    } else {
                                        debug!("重定向响应没有location头");
                                    }
                                } else {
                                    debug!("下载文件 {} 失败: HTTP {}", file.path, response.status());
                                }
                            }
                            Err(e) => {
                                debug!("下载文件 {} 失败，正在重试: {}", file.path, e);
                            }
                        }
                        
                        if retry < 9 {
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                    
                    file_progress.finish_with_message(format!("{} - 失败", filename));
                    error!("下载文件 {} 失败，已重试多次", file.path);
                    Err(anyhow!("下载文件 {} 失败", file.path))
                }
            })
            .buffer_unordered(parallel)
            .collect::<Vec<Result<String>>>()
            .await;
        
        total_progress.finish_with_message("下载完成");
        
        let mut has_error = false;
        for result in &results {
            if result.is_ok() {
                success_count += 1;
            } else {
                has_error = true;
            }
        }
        
        info!("完成 {}/{} 文件", success_count, total_count);
        
        if has_error {
            error!("同步失败");
            Err(anyhow!("同步失败"))
        } else {
            info!("同步完成");
            Ok(())
        }
    }
    
    pub async fn gc_background(&self, file_list: &FileList) -> Result<()> {
        let files = file_list.files.clone();
        let storage = self.storage.clone();
        
        tokio::spawn(async move {
            match storage.gc(&files).await {
                Ok(counter) => {
                    if counter.count > 0 {
                        info!("垃圾回收完成: {} 个文件, {} 字节", counter.count, counter.size);
                    }
                },
                Err(e) => {
                    error!("垃圾回收错误: {}", e);
                }
            }
        });
        
        Ok(())
    }
    
    pub async fn setup_server(&self) -> Router {
        self.create_router()
    }
    
    pub fn get_storage(&self) -> Arc<Box<dyn Storage>> {
        self.storage.clone()
    }

    pub async fn port_check(&self) -> Result<()> {
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        let socket = match socket_opt {
            Some(socket) => socket,
            None => return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接")),
        };
        
        let host = self.host.clone().unwrap_or_else(|| "".to_string());
        let byoc = {
            let config = CONFIG.read().unwrap();
            config.byoc
        };
        
        let flavor = {
            let config = CONFIG.read().unwrap();
            config.flavor.clone()
        };
        
        let no_fast_enable = std::env::var("NO_FAST_ENABLE").unwrap_or_else(|_| "false".to_string()) == "true";
        
        let payload = json!({
            "host": host,
            "port": self.public_port,
            "version": env!("CARGO_PKG_VERSION"),
            "byoc": byoc,
            "noFastEnable": no_fast_enable,
            "flavor": flavor,
        });
        
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        
        let ack_callback = move |message: Payload, _| {
            let tx = tx.clone();
            async move {
                debug!("收到port-check响应回调");
                match message {
                    Payload::Text(values) => {
                        debug!("处理port-check响应的文本数据: {:?}", values);
                        
                        // 克隆整个values数组
                        let values = values.to_vec();
                        
                        if values.len() < 2 {
                            let err = "端口检查响应格式错误: 数组长度不足";
                            error!("{} (期望>=2, 实际={})", err, values.len());
                            let _ = tx.send(Err(anyhow!(err))).await;
                            return;
                        }
                        
                        // 克隆第一个元素用于错误检查
                        let err_value = values[0].clone();
                        if !err_value.is_null() {
                            if let Some(err_msg) = err_value.get("message").and_then(|v| v.as_str()) {
                                let err_msg = err_msg.to_string(); // 克隆字符串
                                error!("主控服务器返回错误: {}", err_msg);
                                let _ = tx.send(Err(anyhow!(err_msg))).await;
                                return;
                            } else {
                                let err = format!("主控服务器返回未知错误: {:?}", err_value);
                                error!("{}", err);
                                let _ = tx.send(Err(anyhow!(err))).await;
                                return;
                            }
                        }
                        
                        info!("端口检查成功");
                        let _ = tx.send(Ok(())).await;
                    },
                    _ => {
                        let err = format!("收到非文本格式的port-check响应: {:?}", message);
                        error!("{}", err);
                        let _ = tx.send(Err(anyhow!(err))).await;
                    }
                }
            }.boxed()
        };
        
        info!("正在发送端口检查请求...");
        let res = socket
            .emit_with_ack("port-check", payload, Duration::from_secs(10), ack_callback)
            .await;
            
        if let Err(e) = res {
            error!("发送端口检查请求失败: {:?}", e);
            return Err(anyhow!("发送端口检查请求失败: {}", e));
        }
        
        // 等待回调处理完成或超时
        match tokio::time::timeout(Duration::from_secs(10), rx.recv()).await {
            Ok(Some(Ok(()))) => {
                info!("端口检查完成");
                Ok(())
            },
            Ok(Some(Err(e))) => {
                error!("端口检查失败: {}", e);
                Err(e)
            },
            Ok(None) => {
                error!("回调通道已关闭");
                Err(anyhow!("端口检查失败: 回调通道已关闭"))
            },
            Err(_) => {
                error!("端口检查超时");
                Err(anyhow!("端口检查超时"))
            }
        }
    }
}

impl Clone for Cluster {
    fn clone(&self) -> Self {
        Cluster {
            client: self.client.clone(),
            storage: self.storage.clone(),
            token_manager: self.token_manager.clone(),
            version: self.version.clone(),
            host: self.host.clone(),
            port: self.port,
            public_port: self.public_port,
            is_enabled: self.is_enabled.clone(),
            want_enable: self.want_enable.clone(),
            counters: self.counters.clone(),
            base_url: self.base_url.clone(),
            tmp_dir: self.tmp_dir.clone(),
            cert_key_files: self.cert_key_files.clone(),
            socket: self.socket.clone(),
            user_agent: self.user_agent.clone(),
            cache_dir: self.cache_dir.clone(),
        }
    }
}

impl std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Cluster")
            .field("version", &self.version)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("public_port", &self.public_port)
            .field("is_enabled", &self.is_enabled)
            .field("want_enable", &self.want_enable)
            .field("base_url", &self.base_url)
            .field("tmp_dir", &self.tmp_dir)
            .finish()
    }
}

async fn serve_file(
    State(cluster): State<Arc<Cluster>>,
    Path(hash_path): Path<String>,
    _req: Request<axum::body::Body>,
) -> impl IntoResponse {
    let storage = cluster.get_storage();
    
    let hash = hash_path.split('/').last().unwrap_or(&hash_path).to_string();
    
    let empty_req = Request::new(&[] as &[u8]);
    
    match storage.as_ref().handle_bytes_request(&hash_path, empty_req).await {
        Ok(mut response) => {
            let headers = response.headers_mut();
            if let Ok(header_value) = axum::http::HeaderValue::from_str(&hash) {
                headers.insert("x-bmclapi-hash", header_value);
            }
            
            if let Some(content_length) = response.headers().get(axum::http::header::CONTENT_LENGTH) {
                if let Ok(size) = content_length.to_str().unwrap_or("0").parse::<u64>() {
                    let mut counters = cluster.counters.write().unwrap();
                    counters.hits += 1;
                    counters.bytes += size;
                }
            }
            
            response
        },
        Err(e) => {
            error!("处理文件请求失败: {}", e);
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("处理请求失败: {}", e)))
                .unwrap()
        }
    }
}

#[allow(dead_code)]
async fn try_reconnect(cluster: &Cluster) -> Result<(), anyhow::Error> {
    if !*cluster.want_enable.read().unwrap() {
        return Ok(());
    }
    
    info!("尝试重新连接服务器...");
    
    match cluster.connect().await {
        Ok(_) => {
            info!("集群连接已恢复");
            
            Ok(())
        },
        Err(e) => {
            error!("重新连接失败: {}", e);
            Err(anyhow!("重新连接失败: {}", e))
        }
    }
}

fn validate_file(data: &[u8], hash: &str) -> bool {
    use sha1::{Sha1, Digest};
    let mut hasher = Sha1::new();
    hasher.update(data);
    let calculated_hash = format!("{:x}", hasher.finalize());
    calculated_hash == hash.to_lowercase()
}

fn hash_to_filename(hash: &str) -> String {
    format!("{}/{}", &hash[0..2], hash)
} 
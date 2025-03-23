use anyhow::{anyhow, Result};
use axum::body::Body;
use axum::{response::Response, routing::get, Router};
use axum::extract::{Query, State, Path};
use axum::http::{StatusCode, Request};
use axum::response::IntoResponse;
use reqwest::Client;
use serde_json::{json, Value};
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
use tokio::sync::broadcast;

use crate::config::CONFIG;
use crate::config::{OpenbmclapiAgentConfiguration, SyncConfig};
use crate::storage::Storage;
use crate::storage::get_storage;
use crate::token::TokenManager;
use crate::types::{Counters, FileInfo, FileList};
use crate::upnp;

struct SocketState {
    cert_receiver: Option<broadcast::Receiver<Vec<Value>>>,
    enable_receiver: Option<broadcast::Receiver<Vec<Value>>>,
    disable_receiver: Option<broadcast::Receiver<Vec<Value>>>,
}

impl Default for SocketState {
    fn default() -> Self {
        Self {
            cert_receiver: None,
            enable_receiver: None,
            disable_receiver: None,
        }
    }
}

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
    socket_state: Arc<RwLock<SocketState>>,
}

impl Cluster {
    pub fn new(version: &str, token_manager: Arc<TokenManager>) -> Result<Self> {
        let config = CONFIG.read().unwrap().clone();
        
        // 创建HTTP客户端
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        
        // 获取配置的存储
        let storage = Arc::new(get_storage(&config));
        
        let base_url = std::env::var("CLUSTER_BMCLAPI")
            .unwrap_or_else(|_| "https://openbmclapi.bangbang93.com".to_string());
            
        // 创建临时目录
        let tmp_dir = std::env::temp_dir().join("rust-bmclapi");
        std::fs::create_dir_all(&tmp_dir)?;
        
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
            socket_state: Arc::new(RwLock::new(SocketState::default())),
        })
    }
    
    pub async fn init(&self) -> Result<()> {
        self.storage.init().await?;
        
        // 处理UPnP端口映射
        let config = CONFIG.read().unwrap().clone();
        if config.enable_upnp {
            match upnp::setup_upnp(config.port, config.cluster_public_port).await {
                Ok(ip) => {
                    // 检查IP是否为公网IP
                    if upnp::is_public_ip(&ip) {
                        info!("UPnP端口映射成功，外网IP: {}", ip);
                        
                        // 如果未指定集群IP，则使用UPnP获取的IP
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
    
    pub async fn request_cert(&self) -> Result<()> {
        // 获取当前的socket连接
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        // 如果没有现有连接，则返回错误
        let socket = match socket_opt {
            Some(socket) => socket,
            None => return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接")),
        };
        
        // 获取证书接收器
        let mut cert_rx = {
            let state = self.socket_state.read().unwrap();
            match &state.cert_receiver {
                Some(rx) => rx.resubscribe(),
                None => return Err(anyhow!("没有可用的证书响应接收器，请先调用connect()方法建立连接")),
            }
        };
        
        // 请求证书
        info!("正在请求证书...");
        match socket.emit("request-cert", json!({})).await {
            Ok(_) => info!("证书请求已发送"),
            Err(e) => {
                error!("发送证书请求失败: {}", e);
                return Err(anyhow!("发送证书请求失败: {}", e));
            }
        };
        
        // 等待响应，最多等待10秒
        let cert_array = match tokio::time::timeout(
            Duration::from_secs(10), 
            cert_rx.recv()
        ).await {
            Ok(Ok(data)) => data,
            Ok(Err(e)) => return Err(anyhow!("接收证书响应失败: {}", e)),
            Err(_) => return Err(anyhow!("请求证书超时")),
        };
        
        // 检查数组长度
        if cert_array.len() < 2 {
            return Err(anyhow!("证书响应格式错误: 数组长度不足"));
        }
        
        // 检查第一个元素是否为错误对象
        if !cert_array[0].is_null() {
            return Err(anyhow!("请求证书失败: {:?}", cert_array[0]));
        }
        
        // 从第二个元素中提取证书和密钥
        let cert_data = &cert_array[1];
        
        // 提取证书和密钥
        let cert = cert_data.get("cert")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("服务器返回的证书格式不正确"))?;
        
        let key = cert_data.get("key")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("服务器返回的密钥格式不正确"))?;
        
        // 保存证书和密钥到临时目录
        let cert_path = self.tmp_dir.join("cert.pem");
        let key_path = self.tmp_dir.join("key.pem");
        
        tokio::fs::write(&cert_path, cert).await?;
        tokio::fs::write(&key_path, key).await?;
        
        info!("证书已保存到: {:?}", cert_path);
        
        // 更新证书路径
        {
            let mut cert_files = self.cert_key_files.write().unwrap();
            *cert_files = Some((cert_path, key_path));
        }
        
        Ok(())
    }
    
    pub async fn use_self_cert(&self) -> Result<()> {
        let config = CONFIG.read().unwrap().clone();
        
        if config.ssl_cert.is_none() || config.ssl_key.is_none() {
            return Err(anyhow!("未提供SSL证书或密钥"));
        }
        
        let ssl_cert = config.ssl_cert.unwrap();
        let ssl_key = config.ssl_key.unwrap();
        
        // 目标路径
        let cert_path = self.tmp_dir.join("cert.pem");
        let key_path = self.tmp_dir.join("key.pem");
        
        info!("使用自定义证书: {:?}", ssl_cert);
        
        // 检查是文件路径还是证书内容
        if std::path::Path::new(&ssl_cert).exists() {
            // 文件路径，复制到临时目录
            tokio::fs::copy(&ssl_cert, &cert_path).await?;
        } else {
            // 证书内容，写入临时文件
            tokio::fs::write(&cert_path, ssl_cert).await?;
        }
        
        if std::path::Path::new(&ssl_key).exists() {
            // 文件路径，复制到临时目录
            tokio::fs::copy(&ssl_key, &key_path).await?;
        } else {
            // 证书内容，写入临时文件
            tokio::fs::write(&key_path, ssl_key).await?;
        }
        
        // 更新证书路径
        {
            let mut cert_files = self.cert_key_files.write().unwrap();
            *cert_files = Some((cert_path, key_path));
        }
        
        Ok(())
    }
    
    pub async fn setup_server_with_https(&self, use_https: bool) -> Result<Router> {
        let router = self.create_router();
        
        // 如果使用HTTPS，确保证书已准备好
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
            .route("/measure/:size", get(measure_handler))
            .route("/auth", get(auth_handler))
            .route("/list/directory", get(
                |State(cluster): State<Arc<Cluster>>, Query(params): axum::extract::Query<HashMap<String, String>>| async move {
                    // 从URL中提取sign和path参数
                    let sign = params.get("sign");
                    let path = params.get("path").unwrap_or(&String::from("")).clone();
                    
                    if sign.is_none() {
                        return Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Body::from("Missing signature"))
                            .unwrap();
                    }
                
                    // 获取config的读锁
                    let cluster_secret = {
                        let config = CONFIG.read().unwrap();
                        config.cluster_secret.clone()
                    };
                    
                    // 计算验证数据
                    let verify_path = format!("/list/directory?path={}", path);
                    
                    // 构建查询参数
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
                
                    // 获取存储实例
                    let _storage = cluster.get_storage();
                    let storage_path = std::path::Path::new("cache").join(&path);
                    
                    // 异步读取目录内容
                    match tokio::fs::read_dir(storage_path).await {
                        Ok(mut entries) => {
                            let mut files = Vec::new();
                            
                            // 读取目录中的所有条目
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
                            
                            // 返回JSON响应
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
                    // 从URL中提取sign参数
                    let sign = params.get("sign");
                    if sign.is_none() {
                        return Response::builder()
                            .status(StatusCode::FORBIDDEN)
                            .body(Body::from("Missing signature"))
                            .unwrap();
                    }
                
                    // 获取config的读锁并立即释放，避免Send问题
                    let cluster_secret = {
                        let config = CONFIG.read().unwrap();
                        config.cluster_secret.clone()
                    };
                    
                    // 计算验证数据
                    let path = "/metrics";
                    
                    // 构建查询参数
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
                
                    // 获取计数器
                    let counters = cluster.counters.read().unwrap().clone();
                    
                    // 返回JSON响应
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
        // 检查是否已启用
        if *self.is_enabled.read().unwrap() {
            return Ok(());
        }
        
        // 设置状态为希望启用
        {
            let mut want_enable = self.want_enable.write().unwrap();
            *want_enable = true;
        }
        
        info!("启用集群...");
        
        // 获取当前的socket连接
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        // 如果没有现有连接，则返回错误
        let socket = match socket_opt {
            Some(socket) => socket,
            None => return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接")),
        };
        
        // 获取enable响应接收器
        let mut enable_rx = {
            let state = self.socket_state.read().unwrap();
            match &state.enable_receiver {
                Some(rx) => rx.resubscribe(),
                None => return Err(anyhow!("没有可用的enable响应接收器，请先调用connect()方法建立连接")),
            }
        };
        
        // 获取IP和端口
        let public_ip = match Self::find_public_ip().await {
            Ok(ip) => ip,
            Err(e) => {
                error!("无法获取公网IP地址: {}", e);
                return Err(anyhow!("无法获取公网IP地址: {}", e));
            }
        };
        
        info!("获取到公网IP地址: {}", public_ip);
        
        // 准备payload
        let host = self.host.clone().unwrap_or_else(|| public_ip);
        let payload = json!({
            "host": host,
            "port": self.public_port,
            "version": self.version,
        });
        
        info!("发送的payload: {}", payload);
        
        // 发送enable事件
        info!("发送启用请求...");
        match socket.emit("enable", payload).await {
            Ok(_) => info!("启用请求已发送"),
            Err(e) => {
                error!("发送启用请求失败: {}", e);
                return Err(anyhow!("发送启用请求失败: {}", e));
            }
        };
        
        // 等待响应，最多等待5分钟
        let timeout_duration = Duration::from_secs(300); // 5分钟
        let result = match tokio::time::timeout(
            timeout_duration,
            enable_rx.recv()
        ).await {
            Ok(Ok(values)) => values,
            Ok(Err(e)) => return Err(anyhow!("接收启用响应失败: {}", e)),
            Err(_) => return Err(anyhow!("节点注册超时")),
        };
        
        // 检查结果
        if result.len() < 2 {
            return Err(anyhow!("启用集群响应格式错误: 数组长度不足"));
        }
        
        // 检查错误
        if !result[0].is_null() {
            if let Some(err_msg) = result[0].get("message").and_then(|v| v.as_str()) {
                return Err(anyhow!(err_msg.to_string()));
            } else {
                return Err(anyhow!("启用集群失败: {:?}", result[0]));
            }
        }
        
        // 检查确认
        if result[1].as_bool() != Some(true) {
            return Err(anyhow!("节点注册失败"));
        }
        
        // 设置状态为已启用
        {
            let mut is_enabled = self.is_enabled.write().unwrap();
            *is_enabled = true;
        }
        
        info!("集群已成功启用");
        Ok(())
    }
    
    pub async fn disable(&self) -> Result<()> {
        // 检查是否已禁用
        if !*self.is_enabled.read().unwrap() {
            return Ok(());
        }
        
        // 设置状态为不希望启用
        {
            let mut want_enable = self.want_enable.write().unwrap();
            *want_enable = false;
        }
        
        // 获取当前的socket连接
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        // 如果没有现有连接，则返回错误
        let socket = match socket_opt {
            Some(socket) => socket,
            None => return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接")),
        };
        
        // 获取disable响应接收器
        let mut disable_rx = {
            let state = self.socket_state.read().unwrap();
            match &state.disable_receiver {
                Some(rx) => rx.resubscribe(),
                None => return Err(anyhow!("没有可用的disable响应接收器，请先调用connect()方法建立连接")),
            }
        };
        
        // 发送disable事件
        info!("发送禁用请求...");
        match socket.emit("disable", json!(null)).await {
            Ok(_) => info!("禁用请求已发送"),
            Err(e) => {
                error!("发送禁用请求失败: {}", e);
                return Err(anyhow!("发送禁用请求失败: {}", e));
            }
        };
        
        // 等待响应
        let result = match tokio::time::timeout(
            Duration::from_secs(30),
            disable_rx.recv()
        ).await {
            Ok(Ok(values)) => values,
            Ok(Err(e)) => return Err(anyhow!("接收禁用响应失败: {}", e)),
            Err(_) => return Err(anyhow!("禁用集群超时")),
        };
        
        // 检查结果
        if result.len() < 2 {
            return Err(anyhow!("禁用集群响应格式错误: 数组长度不足"));
        }
        
        // 检查错误
        if !result[0].is_null() {
            if let Some(err_msg) = result[0].get("message").and_then(|v| v.as_str()) {
                return Err(anyhow!(err_msg.to_string()));
            } else {
                return Err(anyhow!("禁用集群失败: {:?}", result[0]));
            }
        }
        
        // 检查确认
        if result[1].as_bool() != Some(true) {
            return Err(anyhow!("节点禁用失败"));
        }
        
        // 设置状态为已禁用
        {
            let mut is_enabled = self.is_enabled.write().unwrap();
            *is_enabled = false;
        }
        
        info!("集群已成功禁用");
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
            "version": self.version,
        });
        
        // 获取当前的socket连接
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        // 如果没有现有连接，则返回错误
        let socket = match socket_opt {
            Some(socket) => socket,
            None => return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接")),
        };
        
        // 发送心跳事件
        debug!("发送心跳...");
        match socket.emit("heartbeat", payload).await {
            Ok(_) => debug!("心跳已发送"),
            Err(e) => {
                error!("发送心跳失败: {}", e);
                return Err(anyhow!("发送心跳失败: {}", e));
            }
        };
        
        Ok(())
    }
    
    // 添加新的connect方法，实现持久Socket.IO连接和重连机制
    pub async fn connect(&self) -> Result<()> {
        info!("正在建立Socket.IO持久连接到 {}...", self.base_url);
        
        // 获取认证令牌
        let token = self.token_manager.get_token().await?;
        
        // 创建各种事件响应通道
        let (cert_tx, cert_rx) = tokio::sync::broadcast::channel::<Vec<Value>>(32);
        let cert_tx = Arc::new(cert_tx);
        
        let (enable_tx, enable_rx) = tokio::sync::broadcast::channel::<Vec<Value>>(32);
        let enable_tx = Arc::new(enable_tx);
        
        let (disable_tx, disable_rx) = tokio::sync::broadcast::channel::<Vec<Value>>(32);
        let disable_tx = Arc::new(disable_tx);
        
        // 创建新的Socket.IO客户端
        let mut socket_builder = ClientBuilder::new(&self.base_url)
            .transport_type(TransportType::Websocket)
            .auth(json!({"token": token}));
        
        // 注册request-cert事件监听器
        socket_builder = socket_builder.on("request-cert", {
            let cert_tx = cert_tx.clone();
            move |payload: Payload, _: SocketClient| {
                let cert_tx = cert_tx.clone();
                async move {
                    info!("收到证书响应");
                    match payload {
                        Payload::Text(values) => {
                            if let Err(e) = cert_tx.send(values) {
                                error!("发送证书响应到通道失败: {}", e);
                            } else {
                                debug!("已将证书响应发送到通道");
                            }
                        },
                        _ => error!("收到非文本格式的证书响应: {:?}", payload),
                    }
                }.boxed()
            }
        });
        
        // 注册enable事件监听器
        socket_builder = socket_builder.on("enable", {
            let enable_tx = enable_tx.clone();
            move |payload: Payload, _: SocketClient| {
                let enable_tx = enable_tx.clone();
                async move {
                    info!("收到enable响应");
                    match payload {
                        Payload::Text(values) => {
                            if let Err(e) = enable_tx.send(values) {
                                error!("发送enable响应到通道失败: {}", e);
                            } else {
                                debug!("已将enable响应发送到通道");
                            }
                        },
                        _ => error!("收到非文本格式的enable响应: {:?}", payload),
                    }
                }.boxed()
            }
        });
        
        // 注册disable事件监听器
        socket_builder = socket_builder.on("disable", {
            let disable_tx = disable_tx.clone();
            move |payload: Payload, _: SocketClient| {
                let disable_tx = disable_tx.clone();
                async move {
                    info!("收到disable响应");
                    match payload {
                        Payload::Text(values) => {
                            if let Err(e) = disable_tx.send(values) {
                                error!("发送disable响应到通道失败: {}", e);
                            } else {
                                debug!("已将disable响应发送到通道");
                            }
                        },
                        _ => error!("收到非文本格式的disable响应: {:?}", payload),
                    }
                }.boxed()
            }
        });
        
        // 连接事件
        socket_builder = socket_builder.on("connect", {
            let cluster = self.clone();
            move |_: Payload, _: SocketClient| {
                let cluster = cluster.clone();
                async move {
                    info!("Socket.IO连接已建立 - 收到connect事件");
                    
                    // 记录当前状态
                    let is_enabled = *cluster.is_enabled.read().unwrap();
                    let want_enable = *cluster.want_enable.read().unwrap();
                    info!("当前集群状态 - 是否启用: {}, 是否希望启用: {}", is_enabled, want_enable);
                    
                    // 如果之前已启用但因连接断开而停用，则自动重新启用
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
        
        // 打印连接配置信息
        info!("Socket.IO配置: URL={}, 传输类型=Websocket", self.base_url);
        
        // 尝试连接
        match socket_builder.connect().await {
            Ok(socket) => {
                info!("Socket.IO连接已建立，准备认证");
                
                // 等待连接稳定
                tokio::time::sleep(Duration::from_secs(1)).await;
                
                // 保存到共享变量
                {
                    let mut socket_guard = self.socket.write().unwrap();
                    *socket_guard = Some(socket.clone());
                    info!("已将新的Socket.IO连接保存到共享状态");
                }
                
                // 保存各种响应接收器
                {
                    let mut state = self.socket_state.write().unwrap();
                    state.cert_receiver = Some(cert_rx);
                    state.enable_receiver = Some(enable_rx);
                    state.disable_receiver = Some(disable_rx);
                    debug!("已保存所有事件响应接收器");
                }
                
                // 记录基本连接信息
                info!("Socket.IO连接已完成初始化并保存");
                
                // 启动keep-alive定时任务
                let cluster = self.clone();
                tokio::spawn(async move {
                    cluster.start_keepalive_task().await;
                });
                
                Ok(())
            },
            Err(e) => {
                error!("Socket.IO连接失败: {}，错误详情: {:?}", e, e);
                
                // 根据错误消息提供更多具体信息
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
                
                // 尝试重新连接
                info!("将在5秒后尝试重新连接");
                
                // 创建一个不依赖于self的重连逻辑
                let base_url = self.base_url.clone();
                let token_manager = self.token_manager.clone();
                let want_enable = Arc::clone(&self.want_enable);
                let _is_enabled = Arc::clone(&self.is_enabled);
                
                // 不再使用rx/tx通道，直接在tokio::spawn中尝试重连
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    
                    // 先检查是否还想要启用
                    if !*want_enable.read().unwrap() {
                        debug!("集群已不再需要连接，取消重连");
                        return;
                    }
                    
                    // 获取令牌
                    let token = match token_manager.get_token().await {
                        Ok(t) => t,
                        Err(e) => {
                            error!("获取令牌失败，无法重连: {}", e);
                            return;
                        }
                    };
                    
                    // 尝试重连
                    info!("正在尝试Socket.IO重新连接到 {}...", base_url);
                    match ClientBuilder::new(&base_url)
                        .transport_type(TransportType::Websocket)
                        .auth(json!({"token": token}))
                        .connect().await 
                    {
                        Ok(_socket) => {
                            info!("Socket.IO重连成功，连接已建立");
                            // 后续处理将由connect事件监听器完成
                        },
                        Err(e) => error!("Socket.IO重新连接失败: {}，错误详情: {:?}", e, e)
                    }
                });
                
                Err(anyhow!("Socket.IO连接失败: {}", e))
            }
        }
    }
    
    // 添加新的定时发送keep-alive的方法
    async fn start_keepalive_task(&self) {
        info!("启动keep-alive定时任务 (间隔60秒)");
        let mut failed_keepalive = 0;
        
        while *self.is_enabled.read().unwrap() {
            // 等待60秒
            debug!("等待60秒后发送下一次keep-alive...");
            tokio::time::sleep(Duration::from_secs(60)).await;
            debug!("keep-alive计时结束，准备发送keep-alive");
            
            if !*self.is_enabled.read().unwrap() {
                info!("集群已禁用 (is_enabled=false)，停止keep-alive定时任务");
                break;
            }
            
            // 获取当前计数器数据
            let current_counter = {
                let counters = self.counters.read().unwrap();
                counters.clone()
            };
            
            // 准备发送的数据
            let start_time = chrono::Utc::now().timestamp() * 1000;
            let payload = json!({
                "hits": current_counter.hits,
                "bytes": current_counter.bytes,
                "time": start_time
            });
            
            // 获取socket
            let socket_opt = {
                let socket_guard = self.socket.read().unwrap();
                socket_guard.clone()
            };
            
            if let Some(socket) = socket_opt {
                debug!("发送keep-alive数据: {:?} (当前时间戳: {})", payload, start_time);
                
                match socket.emit("keep-alive", payload).await {
                    Ok(_) => {
                        // 发送成功，重置失败计数
                        if failed_keepalive > 0 {
                            info!("keep-alive发送成功，重置失败计数 (之前失败次数: {})", failed_keepalive);
                        } else {
                            debug!("keep-alive发送成功");
                        }
                        failed_keepalive = 0;
                        
                        // 更新计数器，减去已报告的值
                        {
                            let mut counters = self.counters.write().unwrap();
                            counters.hits -= current_counter.hits;
                            counters.bytes -= current_counter.bytes;
                            debug!("更新计数器 - 当前计数 hits: {}, bytes: {}", 
                                  counters.hits, counters.bytes);
                        }
                        
                        debug!("keep-alive处理完成，等待下一个周期");
                    },
                    Err(e) => {
                        failed_keepalive += 1;
                        error!("发送keep-alive失败 ({}/3): {}, 错误详情: {:?}", 
                              failed_keepalive, e, e);
                        
                        if failed_keepalive >= 3 {
                            error!("连续3次keep-alive失败，将禁用集群 (当前连接可能已断开)");
                            // 在一个新的任务中禁用集群，避免死锁
                            let _cluster_id = self.token_manager.clone();
                            let want_enable = Arc::clone(&self.want_enable);
                            
                            tokio::spawn(async move {
                                // 设置状态为不希望启用
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
        
        let token = self.token_manager.get_token().await?;
        
        let response = self.client.get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await?;
            
        if response.status() == reqwest::StatusCode::NO_CONTENT {
            return Ok(FileList { files: vec![] });
        }
        
        if response.status().is_success() {
            let bytes = response.bytes().await?;
            // 注意：原始实现使用了zstd解压缩，这里简化处理
            let files: Vec<FileInfo> = serde_json::from_slice(&bytes)?;
            Ok(FileList { files })
        } else {
            let status = response.status();
            let text = response.text().await?;
            error!("获取文件列表失败: {} - {}", status, text);
            Err(anyhow!("获取文件列表失败: {} - {}", status, text))
        }
    }
    
    pub async fn get_configuration(&self) -> Result<OpenbmclapiAgentConfiguration> {
        let url = format!("{}/openbmclapi/configuration", self.base_url);
        let token = self.token_manager.get_token().await?;
        
        let response = self.client.get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await?;
            
        if response.status().is_success() {
            let json_value = response.json::<serde_json::Value>().await?;
            
            // 构造OpenbmclapiAgentConfiguration结构
            let remote_url = json_value["remote_url"].as_str()
                .ok_or_else(|| anyhow!("配置中缺少remote_url字段"))?
                .to_string();
                
            let source = json_value["sync"]["source"].as_str()
                .ok_or_else(|| anyhow!("配置中缺少sync.source字段"))?
                .to_string();
                
            let concurrency = json_value["sync"]["concurrency"].as_u64()
                .ok_or_else(|| anyhow!("配置中缺少sync.concurrency字段"))?
                as usize;
                
            let sync_config = SyncConfig {
                source,
                concurrency,
            };
            
            let config = OpenbmclapiAgentConfiguration {
                sync: sync_config,
                remote_url,
            };
            
            Ok(config)
        } else {
            let status = response.status();
            let text = response.text().await?;
            error!("获取配置失败: {} - {}", status, text);
            Err(anyhow!("获取配置失败: {} - {}", status, text))
        }
    }
    
    pub async fn sync_files(&self, file_list: &FileList, sync_config: &OpenbmclapiAgentConfiguration) -> Result<()> {
        // 检查存储状态
        if !self.storage.check().await? {
            return Err(anyhow!("存储检查失败"));
        }
        
        // 获取缺失的文件
        let missing_files = self.storage.get_missing_files(&file_list.files).await?;
        
        if missing_files.is_empty() {
            info!("没有需要同步的文件");
            return Ok(());
        }
        
        info!("需要同步 {} 个文件", missing_files.len());
        info!("同步策略: {:?}", sync_config);
        
        let token = self.token_manager.get_token().await?;
        
        // 并发下载文件
        let concurrency = sync_config.sync.concurrency.max(1); // 确保并发数至少为1
        let source = &sync_config.remote_url;
        
        use futures::stream::{self, StreamExt};
        
        let results = stream::iter(missing_files)
            .map(|file| {
                let client = self.client.clone();
                let token = token.clone();
                let source = source.clone();
                let storage = self.storage.clone();
                
                async move {
                    // 构建下载URL
                    let url = format!("{}/{}", source, file.path);
                    
                    // 下载文件
                    let response = client.get(&url)
                        .header("Authorization", format!("Bearer {}", token))
                        .send()
                        .await?;
                        
                    if !response.status().is_success() {
                        return Err(anyhow!("下载文件失败: {}", response.status()));
                    }
                    
                    let content = response.bytes().await?;
                    
                    // 保存文件
                    storage.write_file(file.hash.clone(), content.to_vec(), &file).await?;
                    
                    info!("同步文件完成: {}", file.path);
                    
                    Ok::<_, anyhow::Error>(())
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
            
        // 检查结果
        for result in results {
            if let Err(e) = result {
                error!("同步文件错误: {}", e);
                return Err(anyhow!("同步文件过程中发生错误"));
            }
        }
        
        info!("文件同步完成");
        Ok(())
    }
    
    pub async fn gc_background(&self, file_list: &FileList) -> Result<()> {
        // 在后台执行垃圾回收
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

    // 端口检查方法，与Node.js版本保持一致
    pub async fn port_check(&self) -> Result<()> {
        let host = self.host.clone().unwrap_or_else(|| "".to_string());
        
        // 在await之前获取CONFIG中需要的值，避免在await点跨越持有锁
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
            "version": self.version,
            "byoc": byoc,
            "noFastEnable": no_fast_enable,
            "flavor": flavor,
        });

        // 获取当前的socket连接
        let socket_opt = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };
        
        // 如果没有现有连接，则返回错误
        let socket = match socket_opt {
            Some(socket) => socket,
            None => return Err(anyhow!("没有可用的Socket.IO连接，请先调用connect()方法建立连接")),
        };
        
        // 发送port-check事件
        info!("发送端口检查请求...");
        match socket.emit("port-check", payload).await {
            Ok(_) => info!("端口检查请求已发送"),
            Err(e) => {
                error!("发送端口检查请求失败: {}", e);
                return Err(anyhow!("发送端口检查请求失败: {}", e));
            }
        };
        
        // 等待一段时间确保事件被服务器处理
        tokio::time::sleep(Duration::from_secs(2)).await;
        
        Ok(())
    }

    // 查找公网IP（未使用）
    #[allow(dead_code)]
    async fn find_public_ip() -> Result<String> {
        // 首先检查是否启用UPnP及获取端口配置
        let enable_upnp = {
            let config = CONFIG.read().unwrap();
            config.enable_upnp
        };
        
        let (port, public_port) = {
            let config = CONFIG.read().unwrap();
            (config.port, config.cluster_public_port)
        };
        
        // 尝试通过UPnP获取
        if enable_upnp {
            info!("尝试通过UPnP获取公网IP...");
            match upnp::setup_upnp(port, public_port).await {
                Ok(ip) => {
                    info!("成功通过UPnP获取公网IP: {}", ip);
                    return Ok(ip);
                },
                Err(e) => {
                    warn!("UPnP获取公网IP失败: {}", e);
                    warn!("将尝试使用在线IP查询服务获取公网IP");
                    // 继续尝试其他方法
                }
            }
        } else {
            info!("UPnP功能未启用，将尝试使用在线IP查询服务获取公网IP");
        }
        
        // 使用IP查询服务
        let ip_services = [
            "https://api.ipify.org",
            "https://ifconfig.me/ip",
            "https://icanhazip.com",
        ];
        
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        
        for service in ip_services {
            info!("尝试从 {} 获取公网IP...", service);
            match client.get(service).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        if let Ok(ip) = response.text().await {
                            let ip = ip.trim();
                            if !ip.is_empty() {
                                info!("成功从 {} 获取公网IP: {}", service, ip);
                                return Ok(ip.to_string());
                            }
                        }
                    } else {
                        warn!("从 {} 获取IP失败，HTTP状态码: {}", service, response.status());
                    }
                },
                Err(e) => {
                    warn!("从 {} 获取IP失败: {}", service, e);
                    continue;
                }
            }
        }
        
        error!("无法通过任何方式获取公网IP");
        Err(anyhow!("无法获取公网IP，请手动在配置中指定CLUSTER_IP"))
    }
}

// 克隆实现
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
            socket_state: self.socket_state.clone(),
        }
    }
}

// 为Cluster手动实现Debug
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

// 文件服务处理函数
async fn serve_file(
    State(cluster): State<Arc<Cluster>>,
    Path(hash_path): Path<String>,
    _req: Request<axum::body::Body>,
) -> impl IntoResponse {
    let storage = cluster.get_storage();
    
    // 从路径中提取哈希值
    let hash = hash_path.split('/').last().unwrap_or(&hash_path).to_string();
    
    // 创建一个空的字节请求
    let empty_req = Request::new(&[] as &[u8]);
    
    // 请求处理
    match storage.as_ref().handle_bytes_request(&hash_path, empty_req).await {
        Ok(mut response) => {
            // 添加x-bmclapi-hash响应头
            let headers = response.headers_mut();
            if let Ok(header_value) = axum::http::HeaderValue::from_str(&hash) {
                headers.insert("x-bmclapi-hash", header_value);
            }
            
            // 获取文件大小并更新计数器
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

// 测速处理函数
async fn measure_handler(
    Path(size): Path<u32>,
    State(_cluster): State<Arc<Cluster>>,
    req: Request<Body>,
) -> impl IntoResponse {
    // 检查请求是否带有合法签名
    let query_params = req.uri().query().unwrap_or("");
    let query_dict: HashMap<String, String> = query_params
        .split('&')
        .filter_map(|item| {
            let split: Vec<&str> = item.split('=').collect();
            if split.len() == 2 {
                Some((split[0].to_string(), split[1].to_string()))
            } else {
                None
            }
        })
        .collect();

    // 从URL中提取sign参数
    let sign = query_dict.get("sign");
    if sign.is_none() {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("Missing signature"))
            .unwrap();
    }

    // 获取config的读锁
    let config = CONFIG.read().unwrap();
    
    // 计算验证数据
    let path = format!("/measure/{}", size);
    
    // 构建查询参数，确保包含必要的参数
    if !crate::util::check_sign(&path, &config.cluster_secret, &query_dict) {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("Invalid signature"))
            .unwrap();
    }

    // 检查请求的大小是否合理
    if size > 200 {
        return Response::builder()
            .status(StatusCode::BAD_REQUEST)
            .body(Body::from("Size too large"))
            .unwrap();
    }

    // 生成指定大小的数据
    let buffer_size = 1024 * 1024; // 1MB
    let buffer = vec![0u8; buffer_size];

    // 创建异步数据流
    let stream = tokio_stream::iter(std::iter::repeat_with(move || {
        Ok::<_, std::io::Error>(bytes::Bytes::from(buffer.clone()))
    }).take(size as usize));

    // 创建响应
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Length", (size as usize * buffer_size).to_string())
        .body(Body::from_stream(stream))
        .unwrap()
}

// 认证处理函数
async fn auth_handler(req: Request<Body>) -> impl IntoResponse {
    // 获取原始URL
    let original_uri = req.headers().get("x-original-uri");
    if original_uri.is_none() {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("Missing original URI"))
            .unwrap();
    }
    
    let original_uri = original_uri.unwrap().to_str().unwrap_or("");
    
    // 解析URL
    let url = match url::Url::parse(&format!("http://localhost{}", original_uri)) {
        Ok(url) => url,
        Err(_) => {
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(Body::from("Invalid URI"))
                .unwrap();
        }
    };
    
    // 从路径中提取hash
    let path = url.path();
    let hash = path.split('/').last().unwrap_or("");
    
    // 从查询参数中获取sign和过期时间
    let query_params: HashMap<String, String> = url.query_pairs()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    
    // 获取配置
    let config = CONFIG.read().unwrap();
    
    // 验证签名
    if !crate::util::check_sign(hash, &config.cluster_secret, &query_params) {
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("Invalid signature"))
            .unwrap();
    }
    
    // 签名验证通过
    Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

// 尝试重新连接集群（未使用）
#[allow(dead_code)]
async fn try_reconnect(cluster: &Cluster) -> Result<(), anyhow::Error> {
    // 如果集群已经不想启用，则直接返回
    if !*cluster.want_enable.read().unwrap() {
        return Ok(());
    }
    
    info!("尝试重新连接服务器...");
    
    // 直接调用connect方法
    match cluster.connect().await {
        Ok(_) => {
            info!("集群连接已恢复");
            
            // 如果之前希望启用但现在处于禁用状态，connect中的connect事件处理器会自动处理重新启用
            Ok(())
        },
        Err(e) => {
            error!("重新连接失败: {}", e);
            Err(anyhow!("重新连接失败: {}", e))
        }
    }
} 
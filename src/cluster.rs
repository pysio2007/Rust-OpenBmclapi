use anyhow::{anyhow, Result};
use axum::body::Body;
use axum::{response::Response, routing::get, Router};
use axum::extract::{Query, State, Path, Request, connect_info::ConnectInfo};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::middleware;
use reqwest::Client;
use serde_json::json;   
use std::collections::HashMap;
use log::{debug, error, info, warn};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use futures::FutureExt;
use tokio::io::AsyncWriteExt;
use sha1::Digest;
use rust_socketio::{
    asynchronous::{Client as SocketClient, ClientBuilder},
    Payload, TransportType,
};
use indicatif;
use serde::{Deserialize, Serialize};
use bytes;
use hex;
use url::Url;

use crate::config::CONFIG;
use crate::config::{OpenbmclapiAgentConfiguration, SyncConfig};
use crate::storage::Storage;
use crate::storage::get_storage;
use crate::token::TokenManager;
use crate::types::{Counters, FileInfo, FileList};
use crate::upnp;

const USER_AGENT: &str = "openbmclapi-cluster/1.13.1 rust-openbmclapi-cluster";

// 从openapi.rs移过来的结构体定义
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenbmclapiBaseConfiguration {
    pub enabled: bool,
    pub priority: i32,
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
        info!("使用文件目录: {:?}", cache_dir);
        
        let storage = Arc::new(get_storage(&config, Some(cache_dir.clone())));
        
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
                        
                        // 处理错误情况：[Array [Array [Object {"message": String("错误信息")}]]]
                        if inner_array.len() >= 1 && inner_array[0].is_object() {
                            if let Some(err_obj) = inner_array[0].as_object() {
                                if let Some(msg) = err_obj.get("message").and_then(|m| m.as_str()) {
                                    error!("主控服务器返回证书错误: {}", msg);
                                    return;
                                }
                            }
                            
                            // 如果没有message字段但有错误对象
                            error!("主控服务器返回未知证书错误对象: {:?}", inner_array[0]);
                            return;
                        }
                        
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
            
            let (cert_path, key_path) = cert_files.as_ref().unwrap();
            
            // 读取证书和密钥
            let cert = tokio::fs::read(cert_path).await?;
            let key = tokio::fs::read(key_path).await?;
            
            // 配置 TLS
            let config = axum_server::tls_rustls::RustlsConfig::from_pem(cert, key)
                .await
                .map_err(|e| anyhow!("TLS配置错误: {}", e))?;
                
            info!("已配置HTTPS服务器，证书就绪");
            
            // 启动 HTTPS 服务器
            let addr = std::net::SocketAddr::from(([0, 0, 0, 0], self.port));
            let router_clone = router.clone();
            
            info!("正在启动HTTPS服务器，监听地址: {}", addr);
            
            // 创建一个完成信号通道，确保服务器启动成功
            let (tx, rx) = tokio::sync::oneshot::channel::<()>();
            
            tokio::spawn(async move {
                let server = axum_server::bind_rustls(addr, config);
                
                info!("HTTPS服务器绑定成功，开始监听请求");
                
                // 发送服务器成功启动的信号
                let _ = tx.send(());
                
                if let Err(e) = server.serve(router_clone.into_make_service()).await {
                    error!("HTTPS服务器错误: {}", e);
                }
            });
            
            // 等待服务器启动确认
            match tokio::time::timeout(Duration::from_secs(5), rx).await {
                Ok(_) => info!("HTTPS服务器已成功启动"),
                Err(_) => {
                    warn!("等待HTTPS服务器启动超时，但将继续执行");
                    // 这里我们不返回错误，因为服务器可能仍在启动中
                }
            }
        } else {
            info!("使用HTTP模式，HTTPS服务器不会启动");
        }
        
        Ok(router)
    }
    
    pub async fn start_server(&self, router: Router, addr: std::net::SocketAddr) -> Result<()> {
        info!("正在启动HTTP服务器，监听地址: {}", addr);
        
        // 使用手动方式创建并配置TcpListener，确保可以正确获取客户端IP
        let socket = match addr.ip() {
            std::net::IpAddr::V4(_) => tokio::net::TcpSocket::new_v4()?,
            std::net::IpAddr::V6(_) => tokio::net::TcpSocket::new_v6()?,
        };
        
        // 设置SO_REUSEADDR选项允许地址重用
        socket.set_reuseaddr(true)?;
        
        // 绑定到指定地址
        socket.bind(addr)?;
        
        // 开始监听连接
        let listener = socket.listen(1024)?;
        
        info!("成功绑定到端口 {}，启用地址重用", addr.port());
        info!("配置TCP监听器，启用IP地址传递，最大连接队列: 1024");
        
        // 配置Axum响应应用并启用连接信息收集
        let app = router.into_make_service_with_connect_info::<std::net::SocketAddr>();
        
        // 使用axum::serve启动服务
        info!("启动服务器，使用增强型连接信息收集机制");
        let server = axum::serve(
            listener,
            app
        );
        
        info!("HTTP服务器开始运行，已配置长连接支持和IP地址收集");
        server.await?;
        
        Ok(())
    }
    
    fn create_router(&self) -> Router {
        let cluster = Arc::new(self.clone());
        
        Router::new()
            .layer(axum::middleware::from_fn(|req: Request, next: middleware::Next| {
                async move {
                    // 保存所有请求信息，避免后续借用冲突
                    let method = req.method().clone();
                    let version = match req.version() {
                        axum::http::Version::HTTP_09 => "HTTP/0.9",
                        axum::http::Version::HTTP_10 => "HTTP/1.0",
                        axum::http::Version::HTTP_11 => "HTTP/1.1",
                        axum::http::Version::HTTP_2 => "HTTP/2.0",
                        axum::http::Version::HTTP_3 => "HTTP/3.0",
                        _ => "HTTP/?",
                    };
                    let uri = req.uri().clone();
                    let path = uri.path().to_string();
                    let query = uri.query().map(|q| q.to_string());
                    
                    // 获取客户端IP地址
                    // 直接输出socket连接信息，确定是否有正确连接
                    if let Some(connect_info) = req.extensions().get::<ConnectInfo<std::net::SocketAddr>>() {
                        debug!("原始socket连接信息: {}", connect_info.0);
                    } else {
                        debug!("无法获取socket连接信息!");
                    }
                    
                    // 获取真实IP地址
                    let ip_str = get_client_ip(&req);
                    
                    let user_agent = req.headers()
                        .get(axum::http::header::USER_AGENT)
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("-")
                        .to_string();
                    
                    // 获取当前时间
                    let now = chrono::Local::now();
                    let time_str = now.format("%d/%b/%Y:%H:%M:%S %z").to_string();
                    
                    // 获取URI路径和查询参数
                    let request_line = if let Some(q) = &query {
                        format!("{} {}?{} {}", method, path, q, version)
                    } else {
                        format!("{} {} {}", method, path, version)
                    };
                    
                    // 执行请求处理，首先克隆请求
                    let response = next.run(req).await;
                    
                    // 获取响应状态码和内容长度
                    let status = response.status().as_u16();
                    let content_length = response.headers()
                        .get(axum::http::header::CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0);
                    
                    // 使用Apache风格记录完整日志
                    info!("{} - - [{}] \"{}\" {} {} \"-\" \"{}\"", 
                        ip_str, time_str, request_line, status, content_length, user_agent);
                    
                    response
                }
            }))
            .route("/download/:hash", get(serve_file))
            .route("/auth", get(auth_handler))
            .route("/measure/:size", get(measure_handler))
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
        
        info!("开始启用节点节点...");
        
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
                        
                        // 处理可能的嵌套数组格式 [Array [Array [Null, Bool(true)]]]
                        if values.len() == 1 && values[0].is_array() {
                            if let Some(inner_array) = values[0].as_array() {
                                if inner_array.len() == 1 && inner_array[0].is_array() {
                                    if let Some(inner_inner_array) = inner_array[0].as_array() {
                                        // 成功情况：[Array [Array [Null, Bool(true)]]]
                                        if inner_inner_array.len() >= 2 && 
                                           inner_inner_array[0].is_null() && 
                                           inner_inner_array[1].is_boolean() && 
                                           inner_inner_array[1].as_bool().unwrap_or(false) {
                                            info!("节点注册成功，等待节点启用 ");
                                            let _ = tx.send(Ok(())).await;
                                            return;
                                        }
                                        
                                        // 错误情况：[Array [Array [Object {"message": String("错误信息")}]]]
                                        if inner_inner_array.len() >= 1 && inner_inner_array[0].is_object() {
                                            if let Some(err_obj) = inner_inner_array[0].as_object() {
                                                if let Some(msg) = err_obj.get("message").and_then(|m| m.as_str()) {
                                                    let err = format!("主控服务器返回错误: {}", msg);
                                                    error!("{}", err);
                                                    let _ = tx.send(Err(anyhow!(err))).await;
                                                    return;
                                                }
                                            }
                                            
                                            // 如果没有message字段但有错误对象
                                            let err = format!("主控服务器返回未知错误对象: {:?}", inner_inner_array[0]);
                                            error!("{}", err);
                                            let _ = tx.send(Err(anyhow!(err))).await;
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                        
                        // 克隆整个values数组以避免生命周期问题
                        let values = values.to_vec();
                        
                        if values.len() < 2 {
                            let err = "启用节点响应格式错误: 数组长度不足";
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
                        
                        info!("节点注册成功，等待节点启用");
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
                let cluster = self.clone();
                tokio::spawn(async move {
                    info!("启动 keepalive 任务");
                    
                    // 发送首次心跳
                    if let Err(e) = cluster.send_heartbeat().await {
                        error!("首次发送心跳失败: {}", e);
                    }
                    
                    let mut interval = tokio::time::interval(Duration::from_secs(60));
                    while *cluster.is_enabled.read().unwrap() {
                        interval.tick().await;
                        
                        // 发送心跳
                        match cluster.send_heartbeat().await {
                            Ok(_) => {
                                debug!("心跳发送成功，计数器已在send_heartbeat中更新");
                            }
                            Err(e) => {
                                error!("发送心跳失败: {}", e);
                            }
                        }
                    }
                    info!("keepalive 任务结束");
                });
                
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
                        
                        // 处理可能的嵌套数组格式 [Array [Array [Null, Bool(true)]]]
                        if values.len() == 1 && values[0].is_array() {
                            if let Some(inner_array) = values[0].as_array() {
                                if inner_array.len() == 1 && inner_array[0].is_array() {
                                    if let Some(inner_inner_array) = inner_array[0].as_array() {
                                        // 成功情况：[Array [Array [Null, Bool(true)]]]
                                        if inner_inner_array.len() >= 2 && 
                                           inner_inner_array[0].is_null() && 
                                           inner_inner_array[1].is_boolean() && 
                                           inner_inner_array[1].as_bool().unwrap_or(false) {
                                            info!("节点已成功禁用 ");
                                            let mut is_enabled_guard = is_enabled.write().unwrap();
                                            *is_enabled_guard = false;
                                            return;
                                        }
                                        
                                        // 错误情况：[Array [Array [Object {"message": String("错误信息")}]]]
                                        if inner_inner_array.len() >= 1 && inner_inner_array[0].is_object() {
                                            if let Some(err_obj) = inner_inner_array[0].as_object() {
                                                if let Some(msg) = err_obj.get("message").and_then(|m| m.as_str()) {
                                                    error!("主控服务器返回错误: {}", msg);
                                                    return;
                                                }
                                            }
                                            
                                            // 如果没有message字段但有错误对象
                                            error!("主控服务器返回未知错误对象: {:?}", inner_inner_array[0]);
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                        
                        if values.len() < 2 {
                            error!("禁用节点响应格式错误: 数组长度不足");
                            return;
                        }
                        
                        if !values[0].is_null() {
                            if let Some(err_msg) = values[0].get("message").and_then(|v| v.as_str()) {
                                error!("禁用节点失败: {}", err_msg);
                                return;
                            } else {
                                error!("禁用节点失败: {:?}", values[0]);
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
                            info!("节点已成功禁用");
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
        // 获取当前计数器状态
        let current_counter = {
            let counters = self.counters.read().unwrap();
            let counter_copy = counters.clone();
            info!("准备上报计数器数据 - hits: {}, bytes: {}", 
                counter_copy.hits, counter_copy.bytes);
            counter_copy
        };

        // 构建心跳请求
        let socket_result = {
            let socket_guard = self.socket.read().unwrap();
            socket_guard.clone()
        };

        let socket = match socket_result {
            Some(socket) => socket,
            None => {
                debug!("没有已连接的WebSocket，跳过心跳发送");
                return Ok(());
            }
        };

        // 心跳数据不再包含IP信息
        let payload = json!({
            "type": "heartbeat",
            "version": self.version,
            "publicPort": self.public_port,
            "metrics": {
                "hits": current_counter.hits,
                "bytes": current_counter.bytes
            }
        });

        // 发送心跳
        debug!("发送心跳: {}", payload);
        socket.emit("heartbeat", payload).await?;

        // 心跳成功发送后，重置计数器
        let mut counters = self.counters.write().unwrap();
        let old_hits = counters.hits;
        let old_bytes = counters.bytes;
        counters.hits = 0;
        counters.bytes = 0;
        info!("心跳发送成功，更新计数器 - 本次上报计数 hits: {}, bytes: {}", old_hits, old_bytes);

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
                    info!("当前节点状态 - 是否启用: {}, 是否希望启用: {}", is_enabled, want_enable);
                    
                    if *cluster.want_enable.read().unwrap() && !*cluster.is_enabled.read().unwrap() {
                        info!("检测到连接重新建立，且want_enable=true但is_enabled=false，将尝试重新启用节点");
                        let cluster_clone = cluster.clone();
                        tokio::spawn(async move {
                            info!("开始尝试重新启用节点...");
                            if let Err(e) = cluster_clone.enable().await {
                                error!("自动重新启用节点失败: {}, 错误详情: {:?}", e, e);
                            } else {
                                info!("节点已成功重新启用");
                            }
                        });
                    } else if *cluster.want_enable.read().unwrap() && *cluster.is_enabled.read().unwrap() {
                        info!("节点当前状态正常 (want_enable=true, is_enabled=true)，无需执行额外操作");
                    } else if !*cluster.want_enable.read().unwrap() {
                        info!("节点当前不希望启用 (want_enable=false)，不会尝试重新启用");
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
                
                // 在单独线程中运行keepalive逻辑
                let cluster_arc = Arc::new(self.clone());
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(Duration::from_secs(60));
                    loop {
                        interval.tick().await;
                        
                        if !*cluster_arc.is_enabled.read().unwrap() {
                            break;
                        }
                        
                        if let Err(e) = cluster_arc.send_heartbeat().await {
                            error!("keepalive心跳失败: {}", e);
                        }
                    }
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
                        debug!("节点已不再需要连接，取消重连");
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
            
            if files.len() > 5000000 {
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
            Ok(size) if size > 0 && size < 5000000 => size as usize,
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
        
        // 使用全局共享的MULTI_PROGRESS实例
        let multi_progress = crate::logger::MULTI_PROGRESS.clone();
        
        // 创建总进度条，放在底部
        let total_progress = multi_progress.add(indicatif::ProgressBar::new(total_bytes));
        total_progress.set_style(indicatif::ProgressStyle::default_bar()
            .template("[总进度] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}")
            .unwrap()
            .progress_chars("=>-"));
        
        // 设置进度条无阻塞绘制，防止被日志顶掉
        total_progress.enable_steady_tick(std::time::Duration::from_millis(100));
            
        let total_progress = Arc::new(total_progress);
        
        use futures::stream::{self, StreamExt};
        
        // 跟踪已完成的文件路径
        let mut completed_paths = Vec::new();
        // 跟踪当前要下载的文件
        let mut current_files = missing_files;
        
        // 循环直到所有文件都下载完成
        while !current_files.is_empty() {
            info!("开始下载批次，共{}个文件", current_files.len());
            
            // 保存成功和失败的文件
            let mut successful_files = Vec::new();
            let mut failed_files = Vec::new();
            
            // 使用future流同时下载多个文件
            let download_files = current_files.clone();
            let mut download_stream = stream::iter(download_files.iter().cloned())
                .map(|file_info| {
                    let client = self.client.clone();
                    let token = token.clone();
                    let _storage = self.storage.clone();
                    let _source = source.clone();
                    let _base_url = self.base_url.clone();
                    let multi_progress = multi_progress.clone();
                    let total_progress = total_progress.clone();
                    let cache_dir = self.cache_dir.clone();
                    
                    async move {
                        // 为每个文件创建单独的进度条，放置在总进度条之上
                        let file_progress = multi_progress.insert_before(&*total_progress, 
                            indicatif::ProgressBar::new(file_info.size));
                        file_progress.set_style(indicatif::ProgressStyle::default_bar()
                            .template("[{msg}] [{bar:40.green/red}] {bytes}/{total_bytes}")
                            .unwrap()
                            .progress_chars("=>-"));
                        
                        // 启用稳定的后台刷新，减少被日志覆盖的可能性
                        file_progress.enable_steady_tick(std::time::Duration::from_millis(100));
                        
                        // 初始显示的文件名为路径最后一部分，处理更多特殊情况
                        let original_filename = {
                            // 首先尝试从路径中提取文件名
                            let path_filename = file_info.path.split(|c| c == '/' || c == '\\').last().unwrap_or(&file_info.path);
                            
                            // 检查是否是纯哈希值（通常为32位MD5或40位SHA1）
                            if (path_filename.len() == 32 || path_filename.len() == 40) && 
                               path_filename.chars().all(|c| c.is_ascii_hexdigit()) {
                                
                                // 尝试从原始URL路径中提取更好的文件名
                                match file_info.path.split(|c| c == '/' || c == '\\')
                                          .filter(|&part| !part.is_empty() && 
                                                 (part.len() != 32 && part.len() != 40 || 
                                                  !part.chars().all(|c| c.is_ascii_hexdigit())))
                                          .last() {
                                    Some(better_name) => better_name,
                                    None => path_filename, 
                                }
                            } else {
                                path_filename
                            }
                        };
                        
                        // 确保文件名不为空
                        let original_filename = if original_filename.is_empty() {
                            file_info.path.clone()
                        } else {
                            original_filename.to_string()
                        };
                        
                        file_progress.set_message(original_filename.clone());
                        
                        // 用debug级别记录开始下载信息，避免过多info日志干扰进度条
                        debug!("开始下载文件: {} (大小: {} 字节)", file_info.path, file_info.size);
                        
                        // 初始化下载URL和来源标志，在后续使用中再赋值
                        let mut download_url;
                        let mut is_direct_download;
                        
                        'retry_loop: for retry in 0..10 {
                            if retry > 0 {
                                debug!("重试下载文件 {} (第{}次)", file_info.path, retry);
                            }
                            
                            let path = if file_info.path.starts_with('/') {
                                file_info.path[1..].to_string()
                            } else {
                                file_info.path.clone()
                            };
                            
                            let openbmclapi_url = &_base_url;
                            
                            let url = if openbmclapi_url.ends_with('/') {
                                format!("{}{}", openbmclapi_url, path)
                            } else {
                                format!("{}/{}", openbmclapi_url, path)
                            };
                            
                            debug!("获取文件重定向URL: {}", url);
                            
                            // 缓存主控URL，用于后续判断是否是直接从主控下载
                            download_url = url.clone();
                            
                            match client.get(&url)
                                .header("Authorization", format!("Bearer {}", token))
                                .send()
                                .await {
                                Ok(response) => {
                                    if response.status().is_success() {
                                        // 记录是直接从主控下载，没有重定向
                                        is_direct_download = true;
                                        
                                        // 使用流式下载，避免一次性加载大文件到内存
                                        let hash_path = hash_to_filename(&file_info.hash);
                                        let full_path = cache_dir.join(&hash_path);
                                        
                                        // 确保目录存在
                                        if let Some(parent) = full_path.parent() {
                                            if let Err(e) = tokio::fs::create_dir_all(parent).await {
                                                error!("创建目录失败 {}: {}", parent.display(), e);
                                                continue;
                                            }
                                        }
                                        
                                        // 创建临时文件
                                        let temp_path = full_path.with_extension("tmp");
                                        let mut temp_file = match tokio::fs::File::create(&temp_path).await {
                                            Ok(file) => file,
                                            Err(e) => {
                                                error!("创建临时文件失败 {}: {}", temp_path.display(), e);
                                                continue;
                                            }
                                        };
                                        
                                        // 流式下载并写入文件
                                        let mut stream = response.bytes_stream();
                                        let mut hasher = sha1::Sha1::new();
                                        let mut downloaded_size: u64 = 0;
                                        
                                        while let Some(chunk_result) = stream.next().await {
                                            match chunk_result {
                                                Ok(chunk) => {
                                                    // 更新哈希
                                                    hasher.update(&chunk);
                                                    downloaded_size += chunk.len() as u64;
                                                    
                                                    // 写入文件
                                                    if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut temp_file, &chunk).await {
                                                        error!("写入文件数据失败: {}", e);
                                                        let _ = tokio::fs::remove_file(&temp_path).await;
                                                        continue 'retry_loop; // 跳转到外层循环重试
                                                    }
                                                    
                                                    // 显式刷新以确保数据写入磁盘
                                                    if downloaded_size % 1024000 == 0 {
                                                        if let Err(e) = temp_file.flush().await {
                                                            error!("刷新文件数据失败: {}", e);
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("下载文件流出错: {}", e);
                                                    let _ = tokio::fs::remove_file(&temp_path).await;
                                                    continue 'retry_loop; // 跳转到外层循环重试
                                                }
                                            }
                                            
                                            // 显式释放chunk内存
                                        }
                                        
                                        // 关闭文件
                                        if let Err(e) = temp_file.flush().await {
                                            error!("刷新文件数据失败: {}", e);
                                            let _ = tokio::fs::remove_file(&temp_path).await;
                                            continue;
                                        }
                                        
                                        // 计算并校验文件哈希
                                        let hash_hex = hasher.finalize();
                                        let calculated_hash = format!("{:x}", hash_hex);
                                        
                                        // 直接比较哈希值，避免再次读取整个文件
                                        let is_valid = if file_info.hash.len() == 40 {
                                            // SHA1
                                            file_info.hash.to_lowercase() == calculated_hash
                                        } else if file_info.hash.len() == 32 {
                                            // MD5 - 需要重新计算
                                            let file_content = match tokio::fs::read(&temp_path).await {
                                                Ok(content) => content,
                                                Err(e) => {
                                                    error!("读取临时文件失败: {}", e);
                                                    let _ = tokio::fs::remove_file(&temp_path).await;
                                                    continue;
                                                }
                                            };
                                            
                                            let md5_digest = md5::compute(&file_content);
                                            let md5_hex = format!("{:x}", md5_digest);
                                            
                                            // 释放file_content内存
                                            drop(file_content);
                                            
                                            file_info.hash.to_lowercase() == md5_hex
                                        } else {
                                            // 其他长度哈希，需要使用通用验证函数
                                            let content = match tokio::fs::read(&temp_path).await {
                                                Ok(data) => data,
                                                Err(e) => {
                                                    error!("读取临时文件失败: {}", e);
                                                    let _ = tokio::fs::remove_file(&temp_path).await;
                                                    continue;
                                                }
                                            };
                                            
                                            let valid = validate_file(&content, &file_info.hash);
                                            
                                            // 释放content内存
                                            drop(content);
                                            
                                            valid
                                        };
                                        
                                        if !is_valid {
                                            error!("文件校验失败: 期望={}, 实际={}", file_info.hash, calculated_hash);
                                            let _ = tokio::fs::remove_file(&temp_path).await;
                                            continue;
                                        }
                                        
                                        // 校验文件大小
                                        if downloaded_size != file_info.size {
                                            error!("文件大小不匹配: 期望={}, 实际={}", file_info.size, downloaded_size);
                                            let _ = tokio::fs::remove_file(&temp_path).await;
                                            continue;
                                        }
                                        
                                        // 关闭临时文件句柄
                                        drop(temp_file);
                                        
                                        // 重命名临时文件为最终文件
                                        if let Err(e) = tokio::fs::rename(&temp_path, &full_path).await {
                                            error!("重命名文件失败: {} -> {}: {}", temp_path.display(), full_path.display(), e);
                                            let _ = tokio::fs::remove_file(&temp_path).await;
                                            continue;
                                        }
                                        
                                        debug!("文件 {} 下载完成，大小: {} 字节", file_info.path, downloaded_size);
                                        
                                        // 按照规则显示文件名：直接从主控下载，显示主控地址
                                        let display_name = if is_direct_download {
                                            // 从URL中提取主机部分作为显示
                                            if let Ok(parsed_url) = url::Url::parse(&download_url) {
                                                format!("{} (via {})", original_filename, parsed_url.host_str().unwrap_or("主控"))
                                            } else {
                                                format!("{} (via 主控)", original_filename)
                                            }
                                        } else {
                                            original_filename.to_string()
                                        };
                                        
                                        file_progress.finish_with_message(format!("{} - 完成", display_name));
                                        file_progress.set_style(indicatif::ProgressStyle::default_bar()
                                            .template("[{msg}] [{bar:40.bright_green/red}] {bytes}/{total_bytes} ✓")
                                            .unwrap()
                                            .progress_chars("=>-"));
                                        
                                        // 完成后移动到顶部：先从当前位置移除，然后插入到顶部
                                        let pb_clone = file_progress.clone();
                                        multi_progress.remove(&pb_clone);
                                        multi_progress.insert(0, pb_clone);
                                        
                                        total_progress.inc(file_info.size);
                                        debug!("文件 {} 同步成功", file_info.path);
                                        return (true, file_info);
                                    } else if response.status().is_redirection() {
                                        if let Some(location) = response.headers().get("location") {
                                            if let Ok(location_url) = location.to_str() {
                                                debug!("文件 {} 获取到重定向URL: {}", file_info.path, location_url);
                                                
                                                // 记录最终的重定向URL，用于显示
                                                download_url = location_url.to_string();
                                                is_direct_download = false;
                                                
                                                match client.get(location_url)
                                                    .send()
                                                    .await {
                                                    Ok(redirect_response) => {
                                                        if redirect_response.status().is_success() {
                                                            // 使用流式下载，避免一次性加载大文件到内存
                                                            let hash_path = hash_to_filename(&file_info.hash);
                                                            let full_path = cache_dir.join(&hash_path);
                                                            
                                                            // 确保目录存在
                                                            if let Some(parent) = full_path.parent() {
                                                                if let Err(e) = tokio::fs::create_dir_all(parent).await {
                                                                    error!("创建目录失败 {}: {}", parent.display(), e);
                                                                    continue;
                                                                }
                                                            }
                                                            
                                                            // 创建临时文件
                                                            let temp_path = full_path.with_extension("tmp");
                                                            let mut temp_file = match tokio::fs::File::create(&temp_path).await {
                                                                Ok(file) => file,
                                                                Err(e) => {
                                                                    error!("创建临时文件失败 {}: {}", temp_path.display(), e);
                                                                    continue;
                                                                }
                                                            };
                                                            
                                                            // 流式下载并写入文件
                                                            let mut stream = redirect_response.bytes_stream();
                                                            let mut hasher = sha1::Sha1::new();
                                                            let mut downloaded_size: u64 = 0;
                                                            
                                                            while let Some(chunk_result) = stream.next().await {
                                                                match chunk_result {
                                                                    Ok(chunk) => {
                                                                        // 更新哈希
                                                                        hasher.update(&chunk);
                                                                        downloaded_size += chunk.len() as u64;
                                                                        
                                                                        // 写入文件
                                                                        if let Err(e) = tokio::io::AsyncWriteExt::write_all(&mut temp_file, &chunk).await {
                                                                            error!("写入文件数据失败: {}", e);
                                                                            let _ = tokio::fs::remove_file(&temp_path).await;
                                                                            continue 'retry_loop; // 跳转到外层循环重试
                                                                        }
                                                                        
                                                                        // 显式刷新以确保数据写入磁盘
                                                                        if downloaded_size % 1024000 == 0 {
                                                                            if let Err(e) = temp_file.flush().await {
                                                                                error!("刷新文件数据失败: {}", e);
                                                                            }
                                                                        }
                                                                    }
                                                                    Err(e) => {
                                                                        error!("下载文件流出错: {}", e);
                                                                        let _ = tokio::fs::remove_file(&temp_path).await;
                                                                        continue 'retry_loop; // 跳转到外层循环重试
                                                                    }
                                                                }
                                                                
                                                                // 显式释放chunk内存
                                                            }
                                                            
                                                            // 关闭文件
                                                            if let Err(e) = temp_file.flush().await {
                                                                error!("刷新文件数据失败: {}", e);
                                                                let _ = tokio::fs::remove_file(&temp_path).await;
                                                                continue;
                                                            }
                                                            
                                                            // 计算并校验文件哈希
                                                            let hash_hex = hasher.finalize();
                                                            let calculated_hash = format!("{:x}", hash_hex);
                                                            
                                                            // 直接比较哈希值，避免再次读取整个文件
                                                            let is_valid = if file_info.hash.len() == 40 {
                                                                // SHA1
                                                                file_info.hash.to_lowercase() == calculated_hash
                                                            } else if file_info.hash.len() == 32 {
                                                                // MD5 - 需要重新计算
                                                                let file_content = match tokio::fs::read(&temp_path).await {
                                                                    Ok(content) => content,
                                                                    Err(e) => {
                                                                        error!("读取临时文件失败: {}", e);
                                                                        let _ = tokio::fs::remove_file(&temp_path).await;
                                                                        continue;
                                                                    }
                                                                };
                                                                
                                                                let md5_digest = md5::compute(&file_content);
                                                                let md5_hex = format!("{:x}", md5_digest);
                                                                
                                                                // 释放file_content内存
                                                                drop(file_content);
                                                                
                                                                file_info.hash.to_lowercase() == md5_hex
                                                            } else {
                                                                // 其他长度哈希，需要使用通用验证函数
                                                                let content = match tokio::fs::read(&temp_path).await {
                                                                    Ok(data) => data,
                                                                    Err(e) => {
                                                                        error!("读取临时文件失败: {}", e);
                                                                        let _ = tokio::fs::remove_file(&temp_path).await;
                                                                        continue;
                                                                    }
                                                                };
                                                                
                                                                let valid = validate_file(&content, &file_info.hash);
                                                                
                                                                // 释放content内存
                                                                drop(content);
                                                                
                                                                valid
                                                            };
                                                            
                                                            if !is_valid {
                                                                error!("文件校验失败: 期望={}, 实际={}", file_info.hash, calculated_hash);
                                                                let _ = tokio::fs::remove_file(&temp_path).await;
                                                                continue;
                                                            }
                                                            
                                                            // 校验文件大小
                                                            if downloaded_size != file_info.size {
                                                                error!("文件大小不匹配: 期望={}, 实际={}", file_info.size, downloaded_size);
                                                                let _ = tokio::fs::remove_file(&temp_path).await;
                                                                continue;
                                                            }
                                                            
                                                            // 关闭临时文件句柄
                                                            drop(temp_file);
                                                            
                                                            // 重命名临时文件为最终文件
                                                            if let Err(e) = tokio::fs::rename(&temp_path, &full_path).await {
                                                                error!("重命名文件失败: {} -> {}: {}", temp_path.display(), full_path.display(), e);
                                                                let _ = tokio::fs::remove_file(&temp_path).await;
                                                                continue;
                                                            }
                                                            
                                                            debug!("文件 {} 从重定向URL下载完成，大小: {} 字节", file_info.path, downloaded_size);
                                                            
                                                            // 按照规则显示文件名：来自重定向，显示上一部分
                                                            let display_name = if !is_direct_download {
                                                                // 从URL中提取主机部分作为显示
                                                                if let Ok(parsed_url) = url::Url::parse(&download_url) {
                                                                    let host = parsed_url.host_str().unwrap_or("unknown");
                                                                    format!("{} (via {})", original_filename, host)
                                                                } else {
                                                                    original_filename.to_string()
                                                                }
                                                            } else {
                                                                original_filename.to_string()
                                                            };
                                                            
                                                            file_progress.finish_with_message(format!("{} - 完成", display_name));
                                                            file_progress.set_style(indicatif::ProgressStyle::default_bar()
                                                                .template("[{msg}] [{bar:40.bright_green/red}] {bytes}/{total_bytes} ✓")
                                                                .unwrap()
                                                                .progress_chars("=>-"));
                                                            
                                                            // 完成后移动到顶部：先从当前位置移除，然后插入到顶部
                                                            let pb_clone = file_progress.clone();
                                                            multi_progress.remove(&pb_clone);
                                                            multi_progress.insert(0, pb_clone);
                                                            
                                                            total_progress.inc(file_info.size);
                                                            debug!("文件 {} 同步成功", file_info.path);
                                                            return (true, file_info);
                                                        } else {
                                                            info!("从重定向URL下载文件 {} 失败: HTTP {}", file_info.path, redirect_response.status());
                                                        }
                                                    }
                                                    Err(e) => {
                                                        debug!("请求重定向URL {} 失败: {}", download_url, e);
                                                    }
                                                }
                                            } else {
                                                debug!("解析重定向URL失败");
                                            }
                                        } else {
                                            debug!("重定向响应没有location头");
                                        }
                                    } else {
                                        info!("下载文件 {} 失败: HTTP {}", file_info.path, response.status());
                                    }
                                }
                                Err(e) => {
                                    info!("下载文件 {} 失败，正在重试: {}", file_info.path, e);
                                }
                            }
                            
                            if retry < 9 {
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                        }
                        
                        // 显示失败状态，但记录用于后续重试
                        file_progress.finish_with_message(format!("{} - 失败，将在1分钟后重试", original_filename));
                        file_progress.set_style(indicatif::ProgressStyle::default_bar()
                            .template("[{msg}] [{bar:40.red/red}] {bytes}/{total_bytes} ⟳")
                            .unwrap()
                            .progress_chars("=>-"));
                        
                        // 失败的任务也移动到顶部，但在成功任务之后
                        let pb_clone = file_progress.clone();
                        multi_progress.remove(&pb_clone);
                        multi_progress.insert(0, pb_clone);
                        
                        // 使用error级别记录失败，确保在日志中明显显示
                        error!("下载文件失败: {} - 已重试多次，添加到1分钟后重试队列", file_info.path);
                        return (false, file_info);
                    }
                })
                .buffer_unordered(parallel);
                
            // 同时处理下载结果，区分成功和失败
            while let Some((is_success, file_info)) = download_stream.next().await {
                if is_success {
                    // 成功下载
                    successful_files.push(file_info.path.clone());
                    success_count += 1;
                } else {
                    // 下载失败，稍后重试，先记录日志
                    error!("文件下载失败: {} - 将在1分钟后重试", file_info.path);
                    failed_files.push(file_info);
                }
            }
            
            // 记录已完成的路径
            completed_paths.extend(successful_files);
            
            // 更新剩余需要下载的文件
            current_files = failed_files;
            
            // 如果还有失败的文件，等待1分钟后重试
            if !current_files.is_empty() {
                info!("有 {} 个文件下载失败，将在1分钟后重试", current_files.len());
                tokio::time::sleep(Duration::from_secs(60)).await;
            }
        }
        
        // 完成后总进度条也更新样式
        total_progress.finish_with_message("下载完成");
        total_progress.set_style(indicatif::ProgressStyle::default_bar()
            .template("[总进度] [{bar:40.bright_green/blue}] {bytes}/{total_bytes} ✓")
            .unwrap()
            .progress_chars("=>-"));
        
        info!("完成 {}/{} 文件", success_count, total_count);
        
        if success_count < total_count {
            error!("同步失败，部分文件未能成功下载");
            Err(anyhow!("同步失败，只有{}个文件下载成功，总共{}个文件", success_count, total_count))
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
                        
                        // 处理可能的嵌套数组格式 [Array [Array [Null, Bool(true)]]]
                        if values.len() == 1 && values[0].is_array() {
                            if let Some(inner_array) = values[0].as_array() {
                                if inner_array.len() == 1 && inner_array[0].is_array() {
                                    if let Some(inner_inner_array) = inner_array[0].as_array() {
                                        // 成功情况：[Array [Array [Null, Bool(true)]]]
                                        if inner_inner_array.len() >= 2 && 
                                           inner_inner_array[0].is_null() && 
                                           inner_inner_array[1].is_boolean() && 
                                           inner_inner_array[1].as_bool().unwrap_or(false) {
                                            info!("端口检查成功 ");
                                            let _ = tx.send(Ok(())).await;
                                            return;
                                        }
                                        
                                        // 错误情况：[Array [Array [Object {"message": String("错误信息")}]]]
                                        if inner_inner_array.len() >= 1 && inner_inner_array[0].is_object() {
                                            if let Some(err_obj) = inner_inner_array[0].as_object() {
                                                if let Some(msg) = err_obj.get("message").and_then(|m| m.as_str()) {
                                                    let err_msg = format!("主控服务器返回错误: {}", msg);
                                                    error!("{}", err_msg);
                                                    let _ = tx.send(Err(anyhow!(err_msg))).await;
                                                    return;
                                                }
                                            }
                                            
                                            // 如果没有message字段但有错误对象
                                            let err_msg = format!("主控服务器返回未知错误对象: {:?}", inner_inner_array[0]);
                                            error!("{}", err_msg);
                                            let _ = tx.send(Err(anyhow!(err_msg))).await;
                                            return;
                                        }
                                    }
                                }
                            }
                        }
                        
                        // 原始格式检查
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

// 添加format_http_date函数
fn format_http_date(time: std::time::SystemTime) -> String {
    use chrono::{DateTime, Utc};
    let datetime: DateTime<Utc> = time.into();
    datetime.format("%d/%b/%Y:%H:%M:%S %z").to_string()
}

async fn serve_file(
    State(cluster): State<Arc<Cluster>>,
    Path(hash): Path<String>,
    req: Request,
) -> impl IntoResponse {
    let storage = cluster.get_storage();
    
    // 记录用户代理
    let user_agent = req.headers()
        .get(axum::http::header::USER_AGENT)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("-");
    
    // 处理查询参数，用于签名验证
    let mut query_params = HashMap::new();
    let query_str = req.uri().query().unwrap_or("");
    
    if let Some(query) = req.uri().query() {
        let query_str = format!("http://localhost/?{}", query);
        if let Ok(url) = Url::parse(&query_str) {
            for (key, value) in url.query_pairs() {
                query_params.insert(key.to_string(), value.to_string());
            }
        }
    }
    
    // 获取当前时间
    let now = chrono::Local::now();
    let time_str = now.format("%d/%b/%Y:%H:%M:%S %z").to_string();
    
    // 获取请求方法和HTTP版本
    let method = req.method().to_string();
    let version = format!("HTTP/{}", match req.version() {
        axum::http::Version::HTTP_09 => "0.9",
        axum::http::Version::HTTP_10 => "1.0",
        axum::http::Version::HTTP_11 => "1.1",
        axum::http::Version::HTTP_2 => "2.0",
        axum::http::Version::HTTP_3 => "3.0",
        _ => "?",
    });
    
    // 获取URL路径和查询字符串
    let path = req.uri().path();
    let full_url = if !query_str.is_empty() {
        format!("{}?{}", path, query_str)
    } else {
        path.to_string()
    };
    
    // 验证签名
    let cluster_secret = {
        let config = CONFIG.read().unwrap();
        config.cluster_secret.clone()
    };
    
    // 使用规范的哈希路径格式进行签名验证
    debug!("签名验证参数: path={}, params={:?}", hash, query_params);
    
    // 直接使用hash作为签名验证路径参数，保持与其他语言版本一致性
    let sign_valid = crate::util::check_sign(&hash, &cluster_secret, &query_params);
    if !sign_valid {
        // 使用Apache风格日志记录签名验证失败
        error!("{} - - [{}] \"{} {} {}\" 403 0 \"-\" \"{}\"", 
               "anonymous", time_str, method, full_url, version, user_agent);
        
        return Response::builder()
            .status(StatusCode::FORBIDDEN)
            .body(Body::from("Invalid signature"))
            .unwrap();
    }
    
    // 转换为存储中的文件名格式
    let file_path = crate::util::hash_to_filename(&hash);
    
    // 检查文件是否存在
    let exists = match storage.exists(&file_path).await {
        Ok(exists) => exists,
        Err(e) => {
            error!("检查文件存在性失败: {}", e);
            // 使用Apache风格日志记录内部错误
            error!("{} - - [{}] \"{} {} {}\" 500 0 \"-\" \"{}\"", 
                   "anonymous", time_str, method, full_url, version, user_agent);
                   
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("检查文件失败: {}", e)))
                .unwrap();
        }
    };
    
    if !exists {
        // 使用Apache风格日志记录文件不存在
        error!("{} - - [{}] \"{} {} {}\" 404 0 \"-\" \"{}\"", 
               "anonymous", time_str, method, full_url, version, user_agent);
               
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("File not found"))
            .unwrap();
    }
    
    // 获取Range请求头信息
    let range_header = req.headers().get(axum::http::header::RANGE);
    
    // 创建一个新的请求对象，包含原始请求中的Range头
    let mut storage_req = Request::new(&[] as &[u8]);
    if let Some(range) = range_header {
        storage_req.headers_mut().insert(axum::http::header::RANGE, range.clone());
    }
    
    // 处理文件请求
    match storage.as_ref().handle_bytes_request(&file_path, storage_req).await {
        Ok(mut response) => {
            // 添加哈希标头
            {
                let headers = response.headers_mut();
                if let Ok(header_value) = axum::http::HeaderValue::from_str(&hash) {
                    headers.insert("x-bmclapi-hash", header_value);
                }
            }
            
            // 获取响应的内容长度
            let content_length = response.headers()
                .get(axum::http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            
            // 使用Apache风格日志记录成功的请求
            info!("anonymous - - [{}] \"{}\" {} {} \"-\" \"{}\"",
                format_http_date(std::time::SystemTime::now()),
                format!("{} {} HTTP/1.1", 
                    req.method(),
                    req.uri()
                ),
                response.status().as_u16(),
                content_length,
                user_agent
            );
            
            // 更新计数器 - 只累计成功状态的请求
            if response.status().is_success() {
                // 获取实际返回的内容长度
                let content_length = response.headers()
                    .get(axum::http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                
                if content_length > 0 {
                    let mut counters = cluster.counters.write().unwrap();
                    counters.hits += 1;
                    counters.bytes += content_length;
                    
                    debug!("文件上传成功 - 累计计数器: hits={}, bytes={}，本次上传: {} 字节", 
                        counters.hits, counters.bytes, content_length);
                } else {
                    debug!("文件上传成功但内容长度为0，不累计计数");
                }
            }
            
            // 如果有Range请求头但Storage没有处理，我们需要在这里处理
            if range_header.is_some() && response.status() != StatusCode::PARTIAL_CONTENT {
                // 先获取内容长度，避免多重借用
                let content_length = {
                    let headers = response.headers();
                    headers.get(axum::http::header::CONTENT_LENGTH)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<u64>().ok())
                        .unwrap_or(0)
                };
                
                if content_length == 0 {
                    warn!("无法处理Range请求: 响应中未包含有效的Content-Length头");
                    return response;
                }
                
                // 处理Range请求
                if let Some(range_str) = range_header.and_then(|h| h.to_str().ok()) {
                    if let Some(bytes_range) = range_str.strip_prefix("bytes=") {
                        let ranges: Vec<&str> = bytes_range.split(',').collect();
                        
                        if ranges.len() > 1 {
                            // 多范围请求，返回适当的响应告知客户端我们仅支持单范围
                            warn!("请求了多范围 ({}), 但服务器仅支持单范围请求", range_str);
                            return Response::builder()
                                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                .header(axum::http::header::CONTENT_RANGE, format!("bytes */{}", content_length))
                                .body(Body::from("Multiple ranges are not supported"))
                                .unwrap();
                        } else if ranges.len() == 1 {
                            // 处理单一范围的情况
                            let range_parts: Vec<&str> = ranges[0].split('-').collect();
                            
                            if range_parts.len() == 2 {
                                let start_str = range_parts[0];
                                let end_str = range_parts[1];
                                
                                let start = start_str.parse::<u64>().unwrap_or(0);
                                let end = if end_str.is_empty() {
                                    content_length - 1
                                } else {
                                    end_str.parse::<u64>().unwrap_or(content_length - 1)
                                };
                                
                                // 检查范围是否有效
                                if start >= content_length {
                                    warn!("Range请求范围超出文件大小: 起始位置 {} >= 文件大小 {}", start, content_length);
                                    return Response::builder()
                                        .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                        .header(axum::http::header::CONTENT_RANGE, format!("bytes */{}", content_length))
                                        .body(Body::from("Range out of bounds"))
                                        .unwrap();
                                }
                                
                                // 修正end值，确保不超过文件大小
                                let end = std::cmp::min(end, content_length - 1);
                                
                                debug!("处理Range请求: {}-{}/{}", start, end, content_length);
                                
                                // 需要从响应体中提取指定范围的数据
                                // 首先获取完整的响应体
                                match axum::body::to_bytes(response.into_body(), usize::MAX).await {
                                    Ok(full_body) => {
                                        if (start as usize) < full_body.len() {
                                            // 提取请求的范围
                                            let end_pos = std::cmp::min(end as usize + 1, full_body.len());
                                            let partial_body = full_body.slice(start as usize..end_pos);
                                            
                                            // 构建新的部分内容响应
                                            let mut partial_resp = Response::builder()
                                                .status(StatusCode::PARTIAL_CONTENT)
                                                .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
                                                .header(axum::http::header::CONTENT_LENGTH, partial_body.len())
                                                .header(axum::http::header::CONTENT_RANGE, 
                                                       format!("bytes {}-{}/{}", start, end, content_length))
                                                .header(axum::http::header::CACHE_CONTROL, "max-age=2592000");
                                            
                                            // 添加哈希标头
                                            if let Ok(header_value) = axum::http::HeaderValue::from_str(&hash) {
                                                partial_resp = partial_resp.header("x-bmclapi-hash", header_value);
                                            }
                                            
                                            // 返回范围响应
                                            return partial_resp
                                                .body(Body::from(partial_body))
                                                .unwrap();
                                        } else {
                                            warn!("Range请求起始位置 {} 超出响应体大小 {}", start, full_body.len());
                                            return Response::builder()
                                                .status(StatusCode::RANGE_NOT_SATISFIABLE)
                                                .header(axum::http::header::CONTENT_RANGE, format!("bytes */{}", content_length))
                                                .body(Body::from("Range out of bounds"))
                                                .unwrap();
                                        }
                                    },
                                    Err(e) => {
                                        error!("读取响应体失败: {}", e);
                                        return Response::builder()
                                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                                            .body(Body::from(format!("Error reading response body: {}", e)))
                                            .unwrap();
                                    }
                                }
                            }
                        }
                    }
                }
                
                // 如果无法处理Range请求，返回原始响应
                warn!("无法解析Range请求: {:?}", range_header);
            }
            
            response
        },
        Err(e) => {
            error!("处理文件请求失败: {}", e);
            // 使用Apache风格日志记录处理失败
            error!("{} - - [{}] \"{} {} {}\" 500 0 \"-\" \"{}\"", 
                   user_agent, time_str, method, full_url, version, user_agent);
                   
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("处理请求失败: {}", e)))
                .unwrap()
        }
    }
}

/**
 * 验证文件内容与给定的哈希值是否匹配
 * 
 * 支持以下哈希算法：
 * - MD5 (32位十六进制字符串)
 * - SHA1 (40位十六进制字符串)
 * - 其他长度的哈希值会尝试用MD5和SHA1两种算法进行验证
 * 
 * # 参数
 * - `data`: 文件数据
 * - `hash`: 期望的哈希值，大小写不敏感
 * 
 * # 返回值
 * - `true`: 哈希值匹配
 * - `false`: 哈希值不匹配
 */
pub fn validate_file(data: &[u8], hash: &str) -> bool {
    let expected = hash.to_lowercase().trim().to_string();
    
    // 根据哈希长度判断使用哪种算法
    if expected.len() == 32 {
        // MD5哈希 (32位)
        let digest = md5::compute(data);
        let actual = format!("{:x}", digest).to_lowercase();
        
        if expected != actual {
            log::error!("MD5哈希校验失败: 期望={}, 实际={}", expected, actual);
            log::debug!("MD5哈希校验失败 - 文件大小: {} 字节, 计算哈希: {}, 期望哈希: {}", data.len(), actual, expected);
        }
        
        actual == expected
    } else if expected.len() == 40 {
        // SHA1哈希 (40位)
        use sha1::{Sha1, Digest};
        let mut hasher = Sha1::new();
        hasher.update(data);
        let actual = format!("{:x}", hasher.finalize()).to_lowercase();
        
        if expected != actual {
            log::error!("SHA1哈希校验失败: 期望={}, 实际={}", expected, actual);
            log::debug!("SHA1哈希校验失败 - 文件大小: {} 字节, 计算哈希: {}, 期望哈希: {}", data.len(), actual, expected);
        }
        
        actual == expected
    } else {
        // 未知长度的哈希值，尝试两种算法
        // 先尝试MD5
        let md5_digest = md5::compute(data);
        let md5_actual = format!("{:x}", md5_digest).to_lowercase();
        
        if expected == md5_actual {
            return true;
        }
        
        // 再尝试SHA1
        use sha1::{Sha1, Digest};
        let mut hasher = Sha1::new();
        hasher.update(data);
        let sha1_actual = format!("{:x}", hasher.finalize()).to_lowercase();
        
        if expected == sha1_actual {
            return true;
        }
        
        // 都不匹配，记录错误并返回失败
        log::error!("未知长度哈希校验失败: 期望={}, MD5={}, SHA1={}", expected, md5_actual, sha1_actual);
        log::warn!("未知哈希算法: 哈希长度 {}", expected.len());
        false
    }
}

pub fn hash_to_filename(hash: &str) -> String {
    format!("{}/{}", &hash[0..2], hash)
} 

async fn auth_handler(
    req: Request,
) -> impl IntoResponse {
    let config = CONFIG.read().unwrap();
    
    // 获取原始URI
    let original_uri = match req.headers().get("x-original-uri") {
        Some(uri) => uri.to_str().unwrap_or_default(),
        None => return (StatusCode::FORBIDDEN, "Invalid request").into_response(),
    };
    
    debug!("Auth handler - 原始URI: {}", original_uri);
    
    // 解析URI
    let uri = match Url::parse(&format!("http://localhost{}", original_uri)) {
        Ok(uri) => uri,
        Err(e) => {
            error!("无法解析URI '{}': {}", original_uri, e);
            return (StatusCode::FORBIDDEN, "Invalid URI").into_response();
        }
    };
    
    // 获取哈希和查询参数
    let path = uri.path();
    debug!("Auth handler - 处理路径: {}", path);
    
    // 获取最后一个路径部分作为哈希
    let hash = if let Some(last_segment) = path.split('/').last() {
        if !last_segment.is_empty() {
            last_segment
        } else {
            error!("URI路径没有有效的哈希部分: {}", path);
            return (StatusCode::FORBIDDEN, "Invalid path format").into_response();
        }
    } else {
        error!("无法从URI路径提取哈希部分: {}", path);
        return (StatusCode::FORBIDDEN, "Invalid path").into_response();
    };
    
    // 获取查询参数
    let mut query_params = HashMap::new();
    for (key, value) in uri.query_pairs() {
        query_params.insert(key.to_string(), value.to_string());
    }
    
    debug!("Auth handler - 处理签名验证: hash={}, params={:?}", hash, query_params);
    
    // 检查签名 - 直接使用哈希值进行验证
    if !crate::util::check_sign(hash, &config.cluster_secret, &query_params) {
        error!("签名验证失败 - hash: {}", hash);
        return (StatusCode::FORBIDDEN, "Invalid signature").into_response();
    }
    
    // 返回成功
    debug!("Auth handler - 签名验证成功: {}", hash);
    StatusCode::NO_CONTENT.into_response()
}

// 从openapi.rs移植过来的measure处理程序，完全基于NodeJS参考实现
async fn measure_handler(
    Path(size): Path<u32>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    debug!("Measure handler - 请求下载测量数据大小: {}MB", size);
    
    // 验证签名 - 使用规范的路径格式
    let config = CONFIG.read().unwrap();
    let path = format!("/measure/{}", size);
    
    debug!("Measure handler - 验证签名路径: {}, 参数: {:?}", path, params);
    
    if !crate::util::check_sign(&path, &config.cluster_secret, &params) {
        error!("测量请求签名验证失败 - 路径: {}", path);
        return (StatusCode::FORBIDDEN, "Invalid signature").into_response();
    }
    
    // 检查size是否有效
    if size > 200 {
        warn!("测量请求大小超出限制: {}MB > 200MB", size);
        return (StatusCode::BAD_REQUEST, "Size too large").into_response();
    }
    
    // 创建1MB的固定内容缓冲区，内容为"0066ccff"的重复（完全参考NodeJS实现）
    let mut buffer = Vec::with_capacity(1024 * 1024);
    let pattern = match hex::decode("0066ccff") {
        Ok(p) => p,
        Err(e) => {
            error!("无法解码测量模式数据: {}", e);
            return (StatusCode::INTERNAL_SERVER_ERROR, "Pattern decode error").into_response();
        }
    };
    
    while buffer.len() < 1024 * 1024 {
        buffer.extend_from_slice(&pattern);
    }
    buffer.truncate(1024 * 1024); // 确保大小为1MB
    
    // 计算总大小
    let total_size = size as usize * buffer.len();
    
    // 参考NodeJS版本，一次性创建完整响应
    let buffer_bytes = bytes::Bytes::from(buffer);
    
    info!("发送测量响应，大小 {}MB ({}字节)", size, total_size);
    
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", total_size.to_string())
        .header("Cache-Control", "no-store")
        .body(Body::from(buffer_bytes.repeat(size as usize)))
        .unwrap()
        .into_response()
}

// 获取真实客户端IP地址的工具函数 - 不再获取IP地址
fn get_client_ip(_req: &Request) -> String {
    // 不再获取IP地址，仅返回固定值
    "anonymous".to_string()
}

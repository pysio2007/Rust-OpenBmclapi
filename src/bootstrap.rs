use anyhow::{anyhow, Result};
use axum::Router;
use axum::http::StatusCode;
use colored::Colorize;
use log::{debug, error, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::{self, Instant};
use tokio::sync::mpsc;
use tokio::net::TcpListener;

use crate::cluster::Cluster;
use crate::config::CONFIG;
use crate::constants::DEFAULT_KEEPALIVE_INTERVAL;
use crate::keepalive::Keepalive;
use crate::token::TokenManager;
use crate::types::FileList;

pub async fn bootstrap(version: &str) -> Result<()> {
    // 打印启动信息
    info!("{}", format!("启动 rust-bmclapi {}", version).green());
    
    // 创建令牌管理器
    let config = CONFIG.read().unwrap().clone();
    let token_manager = Arc::new(TokenManager::new(
        config.cluster_id.clone(),
        config.cluster_secret.clone(),
        version.to_string(),
    ));
    
    // 获取令牌（初始化）
    token_manager.get_token().await?;
    
    // 创建并初始化集群
    let cluster = Cluster::new(version, token_manager.clone())?;
    cluster.init().await?;
    
    // 设置心跳
    let keepalive = Keepalive::new(*DEFAULT_KEEPALIVE_INTERVAL);
    keepalive.set_cluster(Arc::new(cluster.clone())).await;
    keepalive.start().await;
    
    // 设置HTTPS
    let use_https = true; // 默认使用HTTPS
    let mut proto = "https";
    
    if config.byoc {
        // 当BYOC但是没有提供证书时，使用http
        if config.ssl_cert.is_none() || config.ssl_key.is_none() {
            proto = "http";
            info!("未提供证书和密钥，将使用HTTP");
        } else {
            info!("使用自定义证书");
            cluster.use_self_cert().await?;
        }
    } else {
        info!("请求证书");
        cluster.request_cert().await?;
    }
    
    // 创建HTTP服务器
    let router = cluster.setup_server_with_https(proto == "https").await?;
    
    // 构建地址
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    info!("HTTP服务器监听于 {}", addr);
    
    // 启动HTTP服务器
    let listener = TcpListener::bind(&addr).await?;
    let server = axum::serve(listener, router);
    
    // 设置关闭信号
    let (tx, mut rx) = mpsc::channel(1);
    let server_with_shutdown = server.with_graceful_shutdown(async move {
        rx.recv().await;
        info!("收到关闭信号，服务器即将关闭...");
    });
    
    // 获取文件列表
    info!("获取文件列表...");
    let files = cluster.get_file_list(None).await?;
    info!("获取到 {} 个文件", files.files.len());
    
    // 获取配置
    let configuration = cluster.get_configuration().await?;
    
    // 检查存储状态
    if !cluster.get_storage().check().await? {
        return Err(anyhow!("存储异常"));
    }
    
    // 同步文件
    info!("开始同步文件...");
    cluster.sync_files(&files, &configuration).await?;
    
    // 垃圾回收
    info!("启动垃圾回收...");
    cluster.gc_background(&files).await?;
    
    // 启用集群
    info!("请求上线...");
    cluster.enable().await?;
    
    info!("{}", format!("服务完成初始化，共 {} 个文件", files.files.len()).green());
    
    // 设置文件检查定时器
    let check_file_interval = Duration::from_secs(600); // 10分钟
    let mut last_files = files;
    
    // 创建一个任务来定期检查新文件
    let cluster_clone = cluster.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(check_file_interval);
        
        loop {
            interval.tick().await;
            
            debug!("检查新文件...");
            match check_new_files(&cluster_clone, &last_files).await {
                Ok(new_files) => {
                    if !new_files.files.is_empty() {
                        last_files = new_files;
                    }
                },
                Err(e) => {
                    error!("检查新文件错误: {}", e);
                }
            }
        }
    });
    
    // 处理终止信号
    tokio::spawn(async move {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("收到终止信号，开始关闭服务...");
                
                // 禁用集群
                if let Err(e) = cluster.disable().await {
                    error!("禁用集群失败: {}", e);
                }
                
                // 通知服务器关闭
                let _ = tx.send(()).await;
            },
            Err(e) => {
                error!("无法监听Ctrl+C信号: {}", e);
            }
        }
    });
    
    // 等待服务器关闭
    if let Err(e) = server_with_shutdown.await {
        error!("服务器错误: {}", e);
        return Err(anyhow!("服务器错误: {}", e));
    }
    
    info!("服务已关闭");
    Ok(())
}

// 检查新文件的函数
async fn check_new_files(cluster: &Cluster, last_files: &FileList) -> Result<FileList> {
    // 获取最后一个文件的修改时间
    let last_modified = last_files.files.iter()
        .map(|f| f.mtime)
        .max();
    
    // 获取新文件
    let file_list = cluster.get_file_list(last_modified).await?;
    
    if file_list.files.is_empty() {
        debug!("没有新文件");
        return Ok(last_files.clone());
    }
    
    info!("发现 {} 个新文件", file_list.files.len());
    
    // 获取配置
    let configuration = cluster.get_configuration().await?;
    
    // 同步新文件
    cluster.sync_files(&file_list, &configuration).await?;
    
    // 更新文件列表（合并旧文件和新文件）
    let mut merged_files = last_files.clone();
    merged_files.files.extend(file_list.files);
    
    Ok(merged_files)
}
use anyhow::{anyhow, Result};
use colored::Colorize;
use log::{debug, error, info, warn};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::{self};

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
    
    // 添加：建立持久Socket.IO连接
    info!("建立Socket.IO持久连接...");
    cluster.connect().await?;
    
    // 设置HTTPS
    let _use_https = true; // 默认使用HTTPS
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
        if !cluster.request_cert().await {
            return Err(anyhow!("请求证书失败"));
        }
    }
    
    // 创建并设置路由
    let is_https = proto == "https";
    
    // 构建地址
    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    
    // 根据是否使用HTTPS选择不同的服务器启动方式
    let server_handle = if is_https {
        info!("使用HTTPS模式启动服务器于端口 {}", config.port);
        // 启动HTTPS服务器
        let _ = cluster.setup_server_with_https(true).await?;
        // HTTPS服务器已在setup_server_with_https方法中启动
        None 
    } else {
        info!("使用HTTP模式启动服务器于端口 {}", config.port);
        let router = cluster.setup_server().await;
        // 不再需要合并路由，直接使用cluster中的router
        let cluster_clone = cluster.clone();
        Some(tokio::spawn(async move {
            if let Err(e) = cluster_clone.start_server(router, addr).await {
                error!("HTTP服务器错误: {}", e);
            }
        }))
    };
    
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
    
    // 进行端口检查
    info!("检查端口可达性...");
    if let Err(e) = cluster.port_check().await {
        warn!("端口检查失败: {}，这可能影响集群的可用性", e);
    } else {
        info!("端口检查成功");
    }
    
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
        let mut last_check_time = chrono::Utc::now();
        
        loop {
            interval.tick().await;
            
            let now = chrono::Utc::now();
            let elapsed = now.signed_duration_since(last_check_time);
            
            debug!("开始检查新文件... (距离上次检查: {}秒)", elapsed.num_seconds());
            match check_new_files(&cluster_clone, &last_files).await {
                Ok(new_files) => {
                    if !new_files.files.is_empty() {
                        let diff_count = new_files.files.len() - last_files.files.len();
                        if diff_count > 0 {
                            info!("发现 {} 个新文件需要同步", diff_count);
                            
                            // 计算新增文件的总大小
                            let mut new_files_size = 0u64;
                            for file in &new_files.files {
                                if !last_files.files.iter().any(|f| f.hash == file.hash) {
                                    new_files_size += file.size;
                                }
                            }
                            
                            if new_files_size > 0 {
                                info!("新文件总大小: {} 字节", new_files_size);
                            }
                        }
                        last_files = new_files;
                    } else {
                        debug!("没有发现新文件");
                    }
                },
                Err(e) => {
                    error!("检查新文件错误: {}", e);
                }
            }
            
            last_check_time = now;
        }
    });
    
    // 处理终止信号
    match signal::ctrl_c().await {
        Ok(()) => {
            info!("收到终止信号，开始关闭服务...");
            
            // 禁用集群
            if let Err(e) = cluster.disable().await {
                error!("禁用集群失败: {}", e);
            }
            
            // 等待服务器优雅关闭
            tokio::time::sleep(Duration::from_secs(3)).await;
            
            // 等待服务器任务完成
            if let Some(handle) = server_handle {
                match handle.await {
                    Ok(_) => info!("HTTP服务器已成功关闭"),
                    Err(e) => error!("等待HTTP服务器关闭时发生错误: {}", e)
                }
            }
            
            info!("服务已成功关闭");
        },
        Err(e) => {
            error!("无法监听Ctrl+C信号: {}", e);
        }
    }
    
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
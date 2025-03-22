use anyhow::Result;
use dotenv::dotenv;
use log::{error, info};
use rust_bmclapi::bootstrap::bootstrap;
use rust_bmclapi::cluster_manager::{ClusterManager, is_worker, get_worker_id};
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // 加载环境变量
    dotenv().ok();
    
    // 初始化日志
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    
    // 获取版本信息
    let version = env!("CARGO_PKG_VERSION");
    
    // 检查是否是工作进程
    if is_worker() {
        let worker_id = get_worker_id().unwrap_or(0);
        info!("启动工作进程 {} - rust-bmclapi {}", worker_id, version);
        
        // 工作进程直接启动bootstrap
        if let Err(e) = bootstrap(version).await {
            error!("工作进程 {} 启动错误: {}", worker_id, e);
            std::process::exit(1);
        }
        
        return Ok(());
    }
    
    // 主进程逻辑
    info!("启动 rust-bmclapi {}", version);
    
    let no_daemon = env::var("NO_DAEMON").is_ok();
    
    if no_daemon {
        // 单进程模式
        if let Err(e) = bootstrap(version).await {
            error!("启动错误: {}", e);
            std::process::exit(1);
        }
    } else {
        // 多进程模式
        // 获取CPU核心数，决定要启动的工作进程数量
        let num_workers = match env::var("WORKER_COUNT") {
            Ok(count) => count.parse::<usize>().unwrap_or_else(|_| num_cpus::get()),
            Err(_) => num_cpus::get(),
        };
        
        info!("启动集群模式，工作进程数: {}", num_workers);
        
        // 创建并启动集群管理器
        let mut cluster_manager = ClusterManager::new(num_workers);
        if let Err(e) = cluster_manager.start().await {
            error!("集群管理器启动失败: {}", e);
            std::process::exit(1);
        }
        
        // 等待Ctrl+C信号
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("收到终止信号，开始关闭服务...");
                if let Err(e) = cluster_manager.stop().await {
                    error!("停止集群管理器失败: {}", e);
                }
            },
            Err(e) => {
                error!("无法监听Ctrl+C信号: {}", e);
            }
        }
    }
    
    Ok(())
}

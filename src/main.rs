use anyhow::Result;
use dotenv::dotenv;
use log::{error, info};
use rust_bmclapi::bootstrap::bootstrap;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // 加载环境变量
    dotenv().ok();
    
    // 强制设置单进程模式
    env::set_var("NO_DAEMON", "1");
    
    // 初始化日志
    setup_logger()?;
    
    // 获取版本信息
    let version = env!("CARGO_PKG_VERSION");
    
    // 主进程逻辑
    info!("启动 rust-bmclapi {} (单进程模式)", version);
    
    // 启动服务
    match bootstrap(version).await {
        Ok(_) => {},
        Err(e) => {
            if e.to_string().contains("CLUSTER_ID") || e.to_string().contains("CLUSTER_SECRET") {
                error!("配置错误: {}", e);
                error!("请检查.env文件并填写必要的配置项（CLUSTER_ID和CLUSTER_SECRET）");
            } else {
                error!("启动错误: {}", e);
            }
            std::process::exit(1);
        }
    }
    
    Ok(())
}

fn setup_logger() -> Result<()> {
    // 设置日志级别和初始化日志库
    if std::env::var("RUST_LOG").is_err() {
        // 默认日志级别为INFO，但可以通过RUST_LOG环境变量覆盖
        std::env::set_var("RUST_LOG", "info,rust_bmclapi=debug,axum::server=debug,axum::http=debug");
    }
    
    // 初始化logger
    env_logger::init();
    
    // 输出一条日志表明logger已经初始化
    info!("日志系统启动");
    info!("系统环境: RUST_LOG={}", std::env::var("RUST_LOG").unwrap_or_default());
    
    Ok(())
}

use anyhow::Result;
use dotenv::dotenv;
use log::{error, info};
use rust_bmclapi::bootstrap::bootstrap;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    // 加载环境变量
    dotenv().ok();
    
    // 初始化日志
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));
    
    // 获取版本信息
    let version = env!("CARGO_PKG_VERSION");
    info!("启动 rust-bmclapi {}", version);
    
    let no_daemon = env::var("NO_DAEMON").is_ok();
    
    if no_daemon {
        // 单进程模式
        if let Err(e) = bootstrap(version).await {
            error!("启动错误: {}", e);
            std::process::exit(1);
        }
    } else {
        // TODO: 实现类似Node.js的cluster模式，支持多进程
        // 暂时使用单进程模式
        if let Err(e) = bootstrap(version).await {
            error!("启动错误: {}", e);
            std::process::exit(1);
        }
    }
    
    Ok(())
}

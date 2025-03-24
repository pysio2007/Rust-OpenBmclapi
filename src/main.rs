use anyhow::Result;
use dotenv::dotenv;
use log::{error, info, warn};
use rust_bmclapi::bootstrap::bootstrap;
use rust_bmclapi::logger;
use std::env;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // 加载环境变量
    dotenv().ok();
    
    // 强制设置单进程模式
    env::set_var("NO_DAEMON", "1");
    
    // 初始化日志
    logger::init_logger()?;
    
    // 获取版本信息
    let version = env!("CARGO_PKG_VERSION");
    
    // 主进程逻辑
    info!("启动 rust-bmclapi {} (单进程模式)", version);
    
    // 实现循环重试机制
    let mut retry_count = 0;
    loop {
        // 启动服务
        match bootstrap(version).await {
            Ok(_) => {
                // 成功运行
                break; // 正常退出循环
            },
            Err(e) => {
                retry_count += 1;
                if e.to_string().contains("CLUSTER_ID") || e.to_string().contains("CLUSTER_SECRET") {
                    error!("配置错误: {}", e);
                    error!("请检查.env文件并填写必要的配置项（CLUSTER_ID和CLUSTER_SECRET）");
                    // 配置错误直接退出程序
                    std::process::exit(1);
                } else {
                    error!("启动错误: {}", e);
                    
                    // 计算等待时间 (指数退避策略)
                    let wait_seconds = if retry_count > 10 {
                        300 // 最大等待5分钟
                    } else {
                        30.min(retry_count * retry_count * 5)
                    };
                    
                    warn!("程序将在 {} 秒后自动重试 (第 {} 次失败)...", wait_seconds, retry_count);
                    tokio::time::sleep(Duration::from_secs(wait_seconds)).await;
                    info!("开始重试...");
                    
                    // 继续循环尝试
                }
            }
        }
    }
    
    Ok(())
}

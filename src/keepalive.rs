use crate::cluster::Cluster;
use log::{debug, error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use crate::error_handler;

#[derive(Debug)]
pub struct Keepalive {
    interval: Duration,
    cluster: Arc<Mutex<Option<Arc<Cluster>>>>,
}

impl Keepalive {
    pub fn new(interval: Duration) -> Self {
        Keepalive {
            interval,
            cluster: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn set_cluster(&self, cluster: Arc<Cluster>) {
        let mut guard = self.cluster.lock().await;
        *guard = Some(cluster);
    }

    pub async fn start(&self) {
        let interval = self.interval;
        let cluster_arc = self.cluster.clone();

        tokio::spawn(async move {
            let mut interval_timer = time::interval(interval);
            let mut consecutive_failure_count = 0;

            loop {
                interval_timer.tick().await;
                
                let cluster = {
                    let guard = cluster_arc.lock().await;
                    match &*guard {
                        Some(cluster) => cluster.clone(),
                        None => {
                            debug!("等待节点实例初始化...");
                            continue;
                        }
                    }
                };
                
                debug!("发送心跳");
                
                if !cluster.is_enabled().await {
                    debug!("节点未启用，跳过心跳");
                    consecutive_failure_count = 0;
                    continue;
                }
                
                if let Err(e) = cluster.send_heartbeat().await {
                    error!("心跳失败: {}", e);
                    consecutive_failure_count += 1;
                    
                    // 如果心跳失败但希望启用，尝试重新启用
                    if cluster.want_enable().await {
                        info!("尝试重新启用节点");
                        if let Err(e) = cluster.enable().await {
                            error!("重新启用失败: {}", e);
                            
                            // 当失败达到阈值（3次连续失败）或错误中包含特定字符串时触发重启
                            if consecutive_failure_count >= 3 || 
                               e.to_string().contains("EngineIO Error") ||
                               e.to_string().contains("not connected") ||
                               e.to_string().contains("connection") {
                                
                                error!("出现严重连接错误，记录并退出以触发程序重启");
                                
                                // 记录错误，但不等待结果
                                let _ = error_handler::handle_registration_failure().await;
                                
                                // 主动退出进程，让外部循环处理重启
                                std::process::exit(1);
                            }
                        } else {
                            // 重启成功，重置失败计数
                            consecutive_failure_count = 0;
                        }
                    }
                } else {
                    // 心跳成功，重置失败计数
                    consecutive_failure_count = 0;
                }
            }
        });
    }
} 
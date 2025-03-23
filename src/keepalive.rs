use crate::cluster::Cluster;
use log::{debug, error, info};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;

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
                    continue;
                }
                
                if let Err(e) = cluster.send_heartbeat().await {
                    error!("心跳失败: {}", e);
                    
                    // 如果心跳失败但希望启用，尝试重新启用
                    if cluster.want_enable().await {
                        info!("尝试重新启用节点");
                        if let Err(e) = cluster.enable().await {
                            error!("重新启用失败: {}", e);
                        }
                    }
                }
            }
        });
    }
} 
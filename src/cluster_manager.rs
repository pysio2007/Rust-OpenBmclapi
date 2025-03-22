use std::process::{Child, Command};
use anyhow::{anyhow, Result};
use log::{error, info, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use std::time::Duration;
use tokio::time::sleep;
use std::collections::HashMap;

const BACKOFF_FACTOR: u64 = 2;
const MAX_BACKOFF: u64 = 60;

#[allow(dead_code)]
pub struct Worker {
    id: usize,
    process: Child,
    ready: bool,
}

pub struct ClusterManager {
    workers: Arc<Mutex<HashMap<usize, Worker>>>,
    worker_count: usize,
    next_id: usize,
    running: Arc<Mutex<bool>>,
}

impl ClusterManager {
    pub fn new(worker_count: usize) -> Self {
        ClusterManager {
            workers: Arc::new(Mutex::new(HashMap::new())),
            worker_count,
            next_id: 1,
            running: Arc::new(Mutex::new(true)),
        }
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("启动集群管理器，计划创建 {} 个工作进程", self.worker_count);
        
        // 设置运行标志
        *self.running.lock().await = true;
        
        // 启动工作进程
        for _ in 0..self.worker_count {
            self.fork_worker().await?;
        }
        
        // 启动监控任务
        self.monitor_workers().await;
        
        Ok(())
    }

    pub async fn stop(&self) -> Result<()> {
        info!("正在停止集群管理器...");
        
        // 设置停止标志
        *self.running.lock().await = false;
        
        // 发送终止信号给所有工作进程
        let workers = self.workers.lock().await;
        for (id, worker) in workers.iter() {
            info!("正在终止工作进程 {}", id);
            
            // Windows上不能直接发送信号，所以我们尝试终止进程
            #[cfg(windows)]
            unsafe {
                use std::os::windows::io::AsRawHandle;
                let handle = worker.process.as_raw_handle();
                winapi::um::processthreadsapi::TerminateProcess(
                    handle as winapi::um::winnt::HANDLE, 
                    0
                );
            }
            
            // Unix系统上发送SIGTERM信号
            #[cfg(unix)]
            {
                let pid = worker.process.id();
                let _ = Command::new("kill")
                    .arg("-TERM")
                    .arg(&pid.to_string())
                    .spawn();
            }
        }
        
        // 等待所有工作进程退出
        let timeout = Duration::from_secs(30);
        let start = std::time::Instant::now();
        
        while !self.workers.lock().await.is_empty() && start.elapsed() < timeout {
            sleep(Duration::from_millis(100)).await;
            
            // 清理已终止的进程
            self.cleanup_dead_workers().await;
        }
        
        // 强制终止剩余的进程
        if !self.workers.lock().await.is_empty() {
            warn!("部分工作进程未能正常终止，强制终止");
            self.workers.lock().await.clear();
        }
        
        info!("集群管理器已停止");
        Ok(())
    }

    async fn fork_worker(&mut self) -> Result<()> {
        let id = self.next_id;
        self.next_id += 1;
        
        info!("创建工作进程 {}", id);
        
        // 设置环境变量，表示这是一个工作进程
        let mut cmd = Command::new(std::env::current_exe()?);
        cmd.env("WORKER_ID", id.to_string())
            .env("NO_DAEMON", "1"); // 告诉工作进程不要再创建子进程
        
        // 启动进程
        match cmd.spawn() {
            Ok(child) => {
                let worker = Worker {
                    id,
                    process: child,
                    ready: false,
                };
                
                self.workers.lock().await.insert(id, worker);
                Ok(())
            },
            Err(e) => {
                error!("启动工作进程失败: {}", e);
                Err(anyhow!("启动工作进程失败: {}", e))
            }
        }
    }

    async fn cleanup_dead_workers(&self) {
        let mut workers = self.workers.lock().await;
        let mut dead_workers = Vec::new();
        
        for (id, worker) in workers.iter_mut() {
            match worker.process.try_wait() {
                Ok(Some(status)) => {
                    info!("工作进程 {} 已退出，状态码: {:?}", id, status.code());
                    dead_workers.push(*id);
                },
                Ok(None) => {
                    // 进程仍在运行
                },
                Err(e) => {
                    error!("检查工作进程 {} 状态时出错: {}", id, e);
                    dead_workers.push(*id);
                }
            }
        }
        
        // 移除已死亡的工作进程
        for id in dead_workers {
            workers.remove(&id);
        }
    }

    async fn monitor_workers(&self) {
        let workers_clone = self.workers.clone();
        let running_clone = self.running.clone();
        
        tokio::spawn(async move {
            let mut backoff = 1;
            
            loop {
                // 如果集群管理器已停止，退出循环
                if !*running_clone.lock().await {
                    break;
                }
                
                // 清理已死亡的工作进程
                let mut dead_workers = Vec::new();
                {
                    let mut workers = workers_clone.lock().await;
                    
                    for (id, worker) in workers.iter_mut() {
                        match worker.process.try_wait() {
                            Ok(Some(status)) => {
                                warn!("工作进程 {} 异常退出，状态码: {:?}，{} 秒后重启", id, status.code(), backoff);
                                dead_workers.push(*id);
                            },
                            Ok(None) => {
                                // 进程仍在运行
                                backoff = 1; // 重置退避时间
                            },
                            Err(e) => {
                                error!("检查工作进程 {} 状态时出错: {}", id, e);
                                dead_workers.push(*id);
                            }
                        }
                    }
                    
                    // 移除已死亡的工作进程
                    for id in &dead_workers {
                        workers.remove(id);
                    }
                }
                
                // 如果有工作进程退出，且管理器仍在运行，则创建新的工作进程
                if !dead_workers.is_empty() && *running_clone.lock().await {
                    sleep(Duration::from_secs(backoff)).await;
                    
                    for _ in 0..dead_workers.len() {
                        let mut manager = ClusterManager {
                            workers: workers_clone.clone(),
                            worker_count: 1,
                            next_id: id_counter(),
                            running: running_clone.clone(),
                        };
                        
                        if let Err(e) = manager.fork_worker().await {
                            error!("重新创建工作进程失败: {}", e);
                        }
                    }
                    
                    // 增加退避时间，但不超过最大值
                    backoff = std::cmp::min(backoff * BACKOFF_FACTOR, MAX_BACKOFF);
                }
                
                // 等待一段时间再检查
                sleep(Duration::from_secs(1)).await;
            }
        });
    }
}

// 生成唯一的工作进程ID
fn id_counter() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static COUNTER: AtomicUsize = AtomicUsize::new(1);
    COUNTER.fetch_add(1, Ordering::SeqCst)
}

// 检查当前进程是否是工作进程
pub fn is_worker() -> bool {
    std::env::var("WORKER_ID").is_ok()
}

// 获取当前工作进程ID
pub fn get_worker_id() -> Option<usize> {
    std::env::var("WORKER_ID")
        .ok()
        .and_then(|id| id.parse().ok())
} 
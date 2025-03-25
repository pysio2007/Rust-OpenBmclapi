// 这个文件在Rust中可能不需要，因为我们已经使用了env_logger库
// 但为了保持与原项目相同的结构，我们仍然保留此文件

use anyhow::Result;
use chrono::Local;
use env_logger::fmt::Color;
use indicatif::MultiProgress;
use log::{Level, LevelFilter};
use std::fs::create_dir_all;
use std::io::{self, Write};
use std::path::Path;
use std::thread;
use std::time::Duration;

// 用于控制清理线程的标志
static CLEANER_STARTED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// 全局MultiProgress实例，可以被其他模块共享使用
lazy_static::lazy_static! {
    pub static ref MULTI_PROGRESS: MultiProgress = MultiProgress::new();
}

pub fn setup() -> Result<()> {
    // 如果已经通过其他方式初始化了日志系统，这个方法就是空操作
    // 这里只是为了保持结构一致
    Ok(())
}

pub fn init_logger() -> Result<()> {
    // 设置日志级别
    if std::env::var("RUST_LOG").is_err() {
        // 默认日志级别为INFO，但可以通过RUST_LOG环境变量覆盖
        std::env::set_var("RUST_LOG", "info,rust_bmclapi=info,axum::server=info,axum::http=info,rustls::common_state=error");
    } else {
        // 如果用户已设置RUST_LOG，添加rustls过滤器
        let mut log_env = std::env::var("RUST_LOG").unwrap_or_default();
        if !log_env.contains("rustls::common_state") {
            if !log_env.is_empty() {
                log_env.push_str(",");
            }
            log_env.push_str("rustls::common_state=error");
            std::env::set_var("RUST_LOG", log_env);
        }
    }

    // 确保logs目录存在
    let logs_dir = Path::new("logs");
    if !logs_dir.exists() {
        create_dir_all(logs_dir)?;
    }

    // 配置文件日志 - 选择按小时滚动，但会同时实现文件大小控制
    let file_appender = tracing_appender::rolling::hourly(logs_dir, "bmclapi.log");
    
    // 设置非阻塞写入
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    
    // 需要将guard保存在全局变量中，避免被drop
    // 使用Box::leak确保guard不会被释放
    Box::leak(Box::new(_guard));

    // 同时写入标准输出和文件的自定义Writer
    struct DualWriter {
        console: io::Stdout,
        file: Box<dyn Write + Send>,
    }

    impl Write for DualWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            // 临时隐藏所有进度条
            let multi_progress = MULTI_PROGRESS.clone();
            multi_progress.suspend(|| {
                // 在进度条被隐藏的情况下写入日志
                let console_result = self.console.write(buf);
                let _ = self.file.write(buf);
                console_result
            })
        }

        fn flush(&mut self) -> io::Result<()> {
            let _ = self.console.flush();
            let _ = self.file.flush();
            Ok(())
        }
    }

    // 创建双重输出的writer
    let dual_writer = DualWriter {
        console: io::stdout(),
        file: Box::new(non_blocking),
    };

    // 配置环境日志格式
    env_logger::Builder::new()
        .format(|buf, record| {
            // 过滤掉TLS警告
            if record.target().contains("rustls") {
                let msg = format!("{}", record.args());
                if msg.contains("TLS alert warning received") || 
                   msg.contains("UserCanceled") {
                    // 返回空字符串表示不输出此日志
                    return Ok(());
                }
            }
            
            let mut style = buf.style();
            let level_color = match record.level() {
                Level::Error => Color::Red,
                Level::Warn => Color::Yellow,
                Level::Info => Color::Green,
                Level::Debug => Color::Blue,
                Level::Trace => Color::Cyan,
            };
            
            style.set_color(level_color);
            
            let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f");
            
            let level_str = style.value(format!("{:<5}", record.level()));
            
            writeln!(
                buf,
                "[{} {} {}] {}",
                timestamp,
                level_str,
                record.target(),
                record.args()
            )
        })
        .filter(None, LevelFilter::Debug)  // 默认Debug级别
        // 应用RUST_LOG环境变量指定的过滤器
        .parse_env("RUST_LOG")
        // 使用自定义的双重输出Writer
        .target(env_logger::Target::Pipe(Box::new(dual_writer)))
        .init();
    
    // 启动一个后台线程，定期清理日志文件，保持最多5个文件
    // 使用原子变量确保清理线程只启动一次
    if !CLEANER_STARTED.swap(true, std::sync::atomic::Ordering::SeqCst) {
        thread::spawn(|| {
            loop {
                // 每10分钟检查一次
                thread::sleep(Duration::from_secs(600));
                
                match clean_old_logs() {
                    Ok(_) => log::debug!("已清理旧日志文件，保留最新的5个"),
                    Err(e) => log::error!("清理旧日志文件失败: {}", e),
                }
                
                // 检查文件大小并进行切割
                match check_and_rotate_logs(5 * 1024 * 1024) {  // 5MB
                    Ok(_) => (),
                    Err(e) => log::error!("检查日志文件大小失败: {}", e),
                }
            }
        });
    }

    // 输出一条日志表明logger已经初始化
    log::info!("日志系统启动，日志文件保存在logs目录");
    log::info!("日志配置: 每小时自动滚动，文件大小超过5MB时强制滚动，保留最新的5个文件");
    log::info!("系统环境: RUST_LOG={}", std::env::var("RUST_LOG").unwrap_or_default());
    
    Ok(())
}

/// 清理旧日志文件，保留最新的5个
fn clean_old_logs() -> Result<()> {
    let logs_dir = Path::new("logs");
    let mut log_files: Vec<(std::fs::Metadata, std::path::PathBuf)> = Vec::new();
    
    // 读取目录中的所有文件
    for entry in std::fs::read_dir(logs_dir)? {
        let entry = entry?;
        let path = entry.path();
        
        // 只处理bmclapi日志文件
        if path.is_file() && 
           path.file_name()
               .and_then(|name| name.to_str())
               .map(|name| name.starts_with("bmclapi"))
               .unwrap_or(false) {
            
            if let Ok(metadata) = std::fs::metadata(&path) {
                log_files.push((metadata, path));
            }
        }
    }
    
    // 按修改时间排序，最新的在前面
    log_files.sort_by(|a, b| {
        b.0.modified().unwrap_or_else(|_| std::time::SystemTime::now())
            .cmp(&a.0.modified().unwrap_or_else(|_| std::time::SystemTime::now()))
    });
    
    // 删除多余的文件
    if log_files.len() > 5 {
        for i in 5..log_files.len() {
            if let Err(e) = std::fs::remove_file(&log_files[i].1) {
                log::warn!("无法删除旧日志文件 {:?}: {}", log_files[i].1, e);
            } else {
                log::debug!("已删除旧日志文件: {:?}", log_files[i].1);
            }
        }
    }
    
    Ok(())
}

/// 检查日志文件大小，如果超过最大大小则重命名
fn check_and_rotate_logs(max_size: u64) -> Result<()> {
    let logs_dir = Path::new("logs");
    
    // 获取当前小时的日志文件
    let now = Local::now();
    let current_log_name = format!("bmclapi.log.{}", now.format("%Y-%m-%d-%H"));
    let current_log_path = logs_dir.join(&current_log_name);
    
    // 检查文件大小
    if current_log_path.exists() {
        let metadata = std::fs::metadata(&current_log_path)?;
        if metadata.len() > max_size {
            // 文件已经超过最大大小，需要重命名
            // 查找当前小时可用的编号
            let mut index = 1;
            loop {
                let new_name = format!("{}.{}", current_log_name, index);
                let new_path = logs_dir.join(&new_name);
                
                if !new_path.exists() {
                    // 重命名文件
                    std::fs::rename(&current_log_path, &new_path)?;
                    log::info!("日志文件大小超过{}字节，已重命名为 {}", max_size, new_name);
                    break;
                }
                
                index += 1;
                if index > 100 {  // 避免无限循环
                    return Err(anyhow::anyhow!("无法为日志文件找到可用的索引名"));
                }
            }
        }
    }
    
    Ok(())
} 
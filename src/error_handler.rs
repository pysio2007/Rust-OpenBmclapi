use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use log::{error, info, warn};
use std::fs;
use std::io::{self, Read, Write};
use std::path::Path;
use std::io::ErrorKind;

#[derive(Debug, Clone)]
pub struct ErrorRecord {
    pub restarts: Vec<DateTime<Utc>>,
}

impl ErrorRecord {
    pub fn new() -> Self {
        ErrorRecord {
            restarts: Vec::new(),
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(ErrorRecord::new());
        }

        let mut file = match fs::File::open(path) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(ErrorRecord::new()),
            Err(e) => {
                error!("无法打开错误记录文件: {}", e);
                return Ok(ErrorRecord::new());
            }
        };

        let mut content = String::new();
        if let Err(e) = file.read_to_string(&mut content) {
            error!("读取错误记录文件失败: {}", e);
            return Ok(ErrorRecord::new());
        }

        let mut restarts = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }

            match line.parse::<DateTime<Utc>>() {
                Ok(time) => restarts.push(time),
                Err(e) => error!("解析时间戳失败: {} - {}", line, e),
            }
        }

        Ok(ErrorRecord { restarts })
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        let content = self.restarts
            .iter()
            .map(|time| time.to_rfc3339())
            .collect::<Vec<_>>()
            .join("\n");

        let mut file = fs::File::create(path)?;
        file.write_all(content.as_bytes())?;
        Ok(())
    }

    pub fn filter_last_24h(&mut self) {
        let cutoff = Utc::now() - Duration::hours(24);
        self.restarts.retain(|time| *time >= cutoff);
    }

    pub fn add_restart(&mut self) {
        self.restarts.push(Utc::now());
    }

    pub fn get_restart_count(&self) -> usize {
        let cutoff = Utc::now() - Duration::hours(24);
        self.restarts.iter().filter(|time| **time >= cutoff).count()
    }
}

pub async fn handle_registration_failure() -> Result<()> {
    let error_file = Path::new(".24herrors");
    let mut error_record = ErrorRecord::load(error_file)?;
    error_record.filter_last_24h();
    let restart_count = error_record.get_restart_count();
    if restart_count >= 90 {
        warn!("警告: 24小时内已失败上线 {} 次，即将达到100次限制!", restart_count);
        warn!("如果您确定要继续，请输入 'y' 并按回车...");
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                if input.trim().to_lowercase() != "y" {
                    error!("用户取消重启，程序将退出");
                    std::process::exit(1);
                }
            },
            Err(e) => {
                error!("读取用户输入失败: {}", e);
                std::process::exit(1);
            }
        }
    }
    if restart_count >= 100 {
        error!("24小时内已失败上线达到100次限制，程序将退出");
        error!("请检查网络连接和服务器状态，或等待24小时后再次尝试");
        std::process::exit(1);
    }
    error_record.add_restart();
    error_record.save(error_file)?;
    info!("节点注册失败，已记录错误");
    
    // 不再在此函数中重启程序，只返回错误
    Err(anyhow::anyhow!("节点注册失败，需要重试"))
}
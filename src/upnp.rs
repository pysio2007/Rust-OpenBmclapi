use anyhow::{anyhow, Result};
use log::{error, info, warn};
use std::net::Ipv4Addr;
use std::time::Duration;
use std::sync::Arc;
use igd::{self, Gateway, SearchOptions};

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: u64 = 5;

// 创建Gateway的包装器，实现Clone
struct GatewayWrapper {
    inner: Arc<Gateway>,
    addr: String,
}

impl GatewayWrapper {
    fn new(gateway: Gateway) -> Self {
        let addr = gateway.addr.to_string();
        Self {
            inner: Arc::new(gateway),
            addr,
        }
    }
    
    fn add_port_with_retry(&self, protocol: igd::PortMappingProtocol, port: u16, local_addr: std::net::SocketAddrV4, lease_duration: u32, description: &str) -> igd::Result<()> {
        let mut retries = 0;
        let mut last_error = None;
        
        while retries < MAX_RETRIES {
            match self.add_port(protocol, port, local_addr, lease_duration, description) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    warn!("UPnP端口映射重试 {}/{}：{}", retries + 1, MAX_RETRIES, e);
                    last_error = Some(e);
                    retries += 1;
                    if retries < MAX_RETRIES {
                        std::thread::sleep(Duration::from_secs(RETRY_DELAY));
                    }
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| {
            igd::Error::RequestError(igd::RequestError::InvalidResponse("最大重试次数已用完".to_string()))
        }))
    }
    
    fn add_port(&self, protocol: igd::PortMappingProtocol, port: u16, local_addr: std::net::SocketAddrV4, lease_duration: u32, description: &str) -> igd::Result<()> {
        Ok(self.inner.add_port(protocol, port, local_addr, lease_duration, description)?)
    }
    
    fn get_external_ip_with_retry(&self) -> igd::Result<Ipv4Addr> {
        let mut retries = 0;
        let mut last_error = None;
        
        while retries < MAX_RETRIES {
            match self.get_external_ip() {
                Ok(ip) => return Ok(ip),
                Err(e) => {
                    warn!("获取外部IP重试 {}/{}：{}", retries + 1, MAX_RETRIES, e);
                    last_error = Some(e);
                    retries += 1;
                    if retries < MAX_RETRIES {
                        std::thread::sleep(Duration::from_secs(RETRY_DELAY));
                    }
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| {
            igd::Error::RequestError(igd::RequestError::InvalidResponse("最大重试次数已用完".to_string()))
        }))
    }
    
    fn get_external_ip(&self) -> igd::Result<Ipv4Addr> {
        Ok(self.inner.get_external_ip()?)
    }
}

impl Clone for GatewayWrapper {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            addr: self.addr.clone(),
        }
    }
}

pub async fn setup_upnp(port: u16, public_port: u16) -> Result<String> {
    info!("正在设置UPnP映射...");
    
    // 创建搜索选项，设置超时为5秒
    let search_options = SearchOptions {
        timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    };
    
    // 使用tokio运行时执行阻塞操作
    let gateway_result = tokio::task::spawn_blocking(move || {
        igd::search_gateway(search_options)
    }).await?;
    
    let gateway = match gateway_result {
        Ok(gateway) => GatewayWrapper::new(gateway),
        Err(e) => {
            error!("UPnP设备搜索失败: {}", e);
            return Err(anyhow!("UPnP设备搜索失败: {}", e));
        }
    };
    
    // 获取外部IP
    let get_ip_result = tokio::task::spawn_blocking({
        let gateway = gateway.clone();
        move || gateway.get_external_ip_with_retry()
    }).await?;
    
    let external_ip = match get_ip_result {
        Ok(ip) => ip,
        Err(e) => {
            error!("获取外部IP失败: {}", e);
            return Err(anyhow!("获取外部IP失败: {}", e));
        }
    };
    
    // 验证IP是否是私有地址
    if is_private_ip(&external_ip) {
        return Err(anyhow!("无法获取公网IP, UPNP返回的IP位于私有地址段, IP: {}", external_ip));
    }
    
    // 获取本地IP地址
    let local_ip = get_local_ip()?;
    
    // 添加端口映射
    let add_port_result = tokio::task::spawn_blocking({
        let gateway = gateway.clone();
        move || {
            gateway.add_port_with_retry(
                igd::PortMappingProtocol::TCP,
                port,
                std::net::SocketAddrV4::new(local_ip, public_port),
                0, // 租约时间，0表示永久
                "OpenBMCLAPI"
            )
        }
    }).await?;
    
    match add_port_result {
        Ok(_) => {
            info!("UPnP端口映射已添加: {}:{} -> 内部端口 {}", gateway.addr, public_port, port);
            info!("外部IP地址: {}", external_ip);
        },
        Err(e) => {
            error!("UPnP端口映射失败: {}", e);
            return Err(anyhow!("UPnP端口映射失败: {}", e));
        }
    }
    
    Ok(external_ip.to_string())
}

// 判断IP是否是私有地址
fn is_private_ip(ip: &Ipv4Addr) -> bool {
    ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_broadcast()
}

// 判断IP是否是公网IP
pub fn is_public_ip(ip: &str) -> bool {
    if let Ok(ip) = ip.parse::<Ipv4Addr>() {
        return !is_private_ip(&ip);
    }
    false
}

// 获取本地IP地址
fn get_local_ip() -> Result<Ipv4Addr> {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0")?;
    
    // 不需要真正连接，只是为了获取本地地址
    // 使用Google DNS服务器地址作为目标
    socket.connect("8.8.8.8:80")?;
    
    match socket.local_addr()? {
        std::net::SocketAddr::V4(addr) => Ok(*addr.ip()),
        _ => Err(anyhow!("无法获取本地IPv4地址")),
    }
} 
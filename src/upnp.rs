use anyhow::{anyhow, Result};
use log::{error, info, warn};
use std::net::Ipv4Addr;
use std::time::Duration;
use rupnp::ssdp::{SearchTarget, URN};
use rupnp::Device;
use rupnp::http::Uri;
use async_trait::async_trait;
use futures::TryStreamExt;

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY: u64 = 5;
const GATEWAY_SEARCH_TIMEOUT: u64 = 10; // 搜索网关的超时时间，单位为秒
const IGD_DEVICE_TYPE: URN = URN::device("schemas-upnp-org", "InternetGatewayDevice", 1);
const WAN_CONNECTION_SERVICE: URN = URN::service("schemas-upnp-org", "WANIPConnection", 1);

#[async_trait]
trait UPnPGateway {
    async fn add_port_mapping(&self, protocol: &str, internal_port: u16, external_port: u16, description: &str) -> Result<()>;
    async fn get_external_ip(&self) -> Result<Ipv4Addr>;
}

struct RupnpGateway {
    device: Device,
    service_url: String,
}

impl RupnpGateway {
    async fn new() -> Result<Self> {
        info!("开始搜索UPnP网关设备...");
        
        // 搜索IGD设备
        let search_target = SearchTarget::URN(IGD_DEVICE_TYPE);
        let devices = rupnp::discover(&search_target, Duration::from_secs(GATEWAY_SEARCH_TIMEOUT)).await?;
        futures::pin_mut!(devices);
        
        // 寻找第一个可用的IGD设备
        let device = match devices.try_next().await? {
            Some(device) => {
                info!("找到UPnP网关设备: {}", device.friendly_name());
                device
            },
            None => return Err(anyhow!("未找到UPnP网关设备")),
        };
        
        // 寻找WAN IP连接服务
        let _service = device.find_service(&WAN_CONNECTION_SERVICE)
            .ok_or_else(|| anyhow!("网关设备中未找到WAN IP连接服务"))?;
        
        let service_url = device.url().to_string();
        
        Ok(Self {
            device,
            service_url,
        })
    }
}

#[async_trait]
impl UPnPGateway for RupnpGateway {
    async fn add_port_mapping(&self, protocol: &str, internal_port: u16, external_port: u16, description: &str) -> Result<()> {
        // 获取本地IP地址
        let local_ip = get_local_ip()?;
        
        // 构建请求参数
        let args = format!(
            "<NewRemoteHost></NewRemoteHost>\
            <NewExternalPort>{}</NewExternalPort>\
            <NewProtocol>{}</NewProtocol>\
            <NewInternalPort>{}</NewInternalPort>\
            <NewInternalClient>{}</NewInternalClient>\
            <NewEnabled>1</NewEnabled>\
            <NewPortMappingDescription>{}</NewPortMappingDescription>\
            <NewLeaseDuration>0</NewLeaseDuration>",
            external_port, protocol, internal_port, local_ip, description
        );
        
        // 查找服务
        let service = self.device.find_service(&WAN_CONNECTION_SERVICE)
            .ok_or_else(|| anyhow!("网关设备中未找到WAN IP连接服务"))?;
        
        // 将service_url字符串转换为Uri
        let uri = self.service_url.parse::<Uri>()
            .map_err(|e| anyhow!("无法解析服务URL: {}", e))?;
        
        // 执行AddPortMapping操作
        service.action(&uri, "AddPortMapping", &args).await?;
        
        info!("UPnP端口映射已添加: {}:{} -> 内部端口 {}", local_ip, external_port, internal_port);
        
        Ok(())
    }
    
    async fn get_external_ip(&self) -> Result<Ipv4Addr> {
        // 查找服务
        let service = self.device.find_service(&WAN_CONNECTION_SERVICE)
            .ok_or_else(|| anyhow!("网关设备中未找到WAN IP连接服务"))?;
        
        // 将service_url字符串转换为Uri
        let uri = self.service_url.parse::<Uri>()
            .map_err(|e| anyhow!("无法解析服务URL: {}", e))?;
        
        // 执行GetExternalIPAddress操作
        let response = service.action(&uri, "GetExternalIPAddress", "").await?;
        
        // 解析返回的IP地址
        let ip_str = response.get("NewExternalIPAddress")
            .ok_or_else(|| anyhow!("获取外部IP地址失败：响应中未包含IP地址"))?;
        
        // 解析为Ipv4Addr
        let ip = ip_str.parse::<Ipv4Addr>()?;
        
        // 检查IP是否为私有地址
        if is_private_ip(&ip) {
            return Err(anyhow!("无法获取公网IP, UPNP返回的IP位于私有地址段, IP: {}", ip));
        }
        
        Ok(ip)
    }
}

pub async fn setup_upnp(port: u16, public_port: u16) -> Result<String> {
    info!("正在设置UPnP映射...");
    
    // 尝试创建网关，包含多次重试逻辑
    let mut retries = 0;
    let mut last_error = None;
    
    while retries < MAX_RETRIES {
        match RupnpGateway::new().await {
            Ok(gateway) => {
                // 获取外部IP
                let external_ip = match gateway.get_external_ip().await {
                    Ok(ip) => ip,
                    Err(e) => {
                        error!("获取外部IP失败: {}", e);
                        retries += 1;
                        last_error = Some(e);
                        if retries < MAX_RETRIES {
                            tokio::time::sleep(Duration::from_secs(RETRY_DELAY)).await;
                        }
                        continue;
                    }
                };
                
                // 添加端口映射
                if let Err(e) = gateway.add_port_mapping("TCP", port, public_port, "OpenBMCLAPI").await {
                    error!("UPnP端口映射失败: {}", e);
                    return Err(anyhow!("UPnP端口映射失败: {}", e));
                }
                
                info!("外部IP地址: {}", external_ip);
                return Ok(external_ip.to_string());
            },
            Err(e) => {
                warn!("UPnP设备搜索失败 (尝试 {}/{}): {}", retries + 1, MAX_RETRIES, e);
                last_error = Some(e);
                retries += 1;
                if retries < MAX_RETRIES {
                    tokio::time::sleep(Duration::from_secs(RETRY_DELAY)).await;
                }
            }
        }
    }
    
    Err(last_error.unwrap_or_else(|| anyhow!("UPnP设备搜索最终失败")))
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
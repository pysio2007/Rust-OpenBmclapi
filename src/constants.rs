use lazy_static::lazy_static;
use std::time::Duration;

pub const DEFAULT_BACKOFF_FACTOR: u32 = 2;
pub const MAX_BACKOFF_SECONDS: u32 = 60;
pub const DEFAULT_PORT: u16 = 4000;

lazy_static! {
    pub static ref DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    pub static ref DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
    pub static ref DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(300);
    pub static ref DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(60);
} 
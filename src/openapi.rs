use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use axum::{
    extract::{Path, Query},
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::get,
    Router,
};

use crate::config::CONFIG;
use crate::util::check_sign;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenbmclapiBaseConfiguration {
    pub enabled: bool,
    pub priority: i32,
}

// 创建并返回API路由器
pub fn create_router() -> Router {
    Router::new()
        .route("/auth", get(auth_handler))
        .route("/measure/:size", get(measure_handler))
}

// measure路由处理程序
async fn measure_handler(
    Path(size): Path<u32>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    // 验证签名
    let config = CONFIG.read().unwrap();
    let path = format!("/measure/{}", size);
    
    if !check_sign(&path, &config.cluster_secret, &params) {
        return (StatusCode::FORBIDDEN, "Invalid signature").into_response();
    }
    
    // 检查size是否有效
    if size > 200 {
        return (StatusCode::BAD_REQUEST, "Size too large").into_response();
    }
    
    // 创建响应
    let buffer = vec![0; 1024 * 1024]; // 1MB的缓冲区
    let total_size = size as usize * 1024 * 1024;
    
    // 设置流式响应
    let stream = async_stream::stream! {
        let mut sent = 0;
        while sent < total_size {
            let to_send = std::cmp::min(buffer.len(), total_size - sent);
            yield Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(&buffer[..to_send]));
            sent += to_send;
        }
    };
    
    // 返回带有Content-Length的流式响应
    axum::response::Response::builder()
        .header("content-length", total_size.to_string())
        .body(axum::body::Body::from_stream(stream))
        .unwrap()
        .into_response()
}

// auth路由处理程序
async fn auth_handler(
    req: Request<axum::body::Body>,
) -> impl IntoResponse {
    let config = CONFIG.read().unwrap();
    
    // 获取原始URI
    let original_uri = match req.headers().get("x-original-uri") {
        Some(uri) => uri.to_str().unwrap_or_default(),
        None => return (StatusCode::FORBIDDEN, "Invalid request").into_response(),
    };
    
    // 解析URI
    let uri = match url::Url::parse(&format!("http://localhost{}", original_uri)) {
        Ok(uri) => uri,
        Err(_) => return (StatusCode::FORBIDDEN, "Invalid URI").into_response(),
    };
    
    // 获取哈希和查询参数
    let path = uri.path();
    let segments: Vec<&str> = path.split('/').collect();
    let hash = segments.last().unwrap_or(&"");
    
    // 获取查询参数
    let mut query_params = HashMap::new();
    for (key, value) in uri.query_pairs() {
        query_params.insert(key.to_string(), value.to_string());
    }
    
    // 检查签名
    if !check_sign(hash, &config.cluster_secret, &query_params) {
        return (StatusCode::FORBIDDEN, "Invalid signature").into_response();
    }
    
    // 返回成功
    StatusCode::NO_CONTENT.into_response()
} 
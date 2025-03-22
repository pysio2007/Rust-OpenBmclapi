use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenbmclapiBaseConfiguration {
    pub enabled: bool,
    pub priority: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenbmclapiAgentConfiguration {
    pub base: OpenbmclapiBaseConfiguration,
    pub remote_url: String,
    pub sync_all: bool,
    pub sync_items: HashMap<String, bool>,
} 
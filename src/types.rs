use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileList {
    pub files: Vec<FileInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    pub path: String,
    pub hash: String,
    pub size: u64,
    pub mtime: u64,
}

#[derive(Debug, Clone, Default)]
pub struct GCCounter {
    pub count: usize,
    pub size: u64,
}

#[derive(Debug, Clone, Default)]
pub struct Counters {
    pub hits: u64,
    pub bytes: u64,
} 
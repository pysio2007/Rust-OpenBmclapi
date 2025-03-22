# Rust-BMCLAPI

这是一个用Rust实现的OpenBMCLAPI集群节点。OpenBMCLAPI是BMCLAPI的开放集群系统，允许用户使用自己的服务器贡献带宽，分发Minecraft相关文件。

## 特性

- 完全兼容原版OpenBMCLAPI协议
- 高性能的文件服务
- 支持多种存储后端（本地文件、WebDAV等）
- 低资源占用
- 自动UPnP端口映射
- 支持SSL加密

## 安装与使用

### 前提条件

- Rust 1.56.0+
- 访问互联网的能力（用于下载文件和注册集群）

### 从源码编译

1. 克隆仓库
```bash
git clone https://github.com/yourusername/rust-bmclapi.git
cd rust-bmclapi
```

2. 编译项目
```bash
cargo build --release
```

3. 配置环境变量
```bash
cp .env.example .env
```
然后编辑`.env`文件，设置必要的配置，主要包括：
- CLUSTER_ID：集群ID（从BMCLAPI获取）
- CLUSTER_SECRET：集群密钥（从BMCLAPI获取）

其他配置项请参考`.env.example`中的说明。

4. 运行
```bash
./target/release/rust-bmclapi
```

### 使用Docker（待实现）

```bash
docker run -d --name rust-bmclapi \
  -p 4000:4000 \
  -v /path/to/cache:/app/cache \
  -e CLUSTER_ID=your_cluster_id \
  -e CLUSTER_SECRET=your_cluster_secret \
  yourusername/rust-bmclapi
```

## 配置说明

主要配置项：

- `CLUSTER_ID`：集群ID，从BMCLAPI获取
- `CLUSTER_SECRET`：集群密钥，从BMCLAPI获取
- `CLUSTER_IP`：可选，服务器公网IP，不设置会自动获取
- `CLUSTER_PORT`：服务监听端口，默认4000
- `CLUSTER_PUBLIC_PORT`：公网访问端口，默认与CLUSTER_PORT相同
- `ENABLE_UPNP`：是否启用UPnP自动端口映射，默认false
- `CLUSTER_STORAGE`：存储类型，支持file（本地文件）和alist（WebDAV）
- `CLUSTER_STORAGE_OPTIONS`：存储配置，JSON格式

详细配置请参考`.env.example`文件。

## 贡献

欢迎贡献代码、报告问题或提出建议！请提交Issue或Pull Request。

## 许可证

本项目采用MIT许可证 - 详见LICENSE文件。

## 致谢

感谢原版OpenBMCLAPI项目以及BMCLAPI团队的支持。 
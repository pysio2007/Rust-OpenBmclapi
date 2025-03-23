# BMCLAPI

BMCLAPI是@bangbang93开发的BMCL的一部分，用于解决国内线路对Forge和Minecraft官方使用的Amazon S3速度缓慢的问题。BMCLAPI是对外开放的，所有需要Minecraft资源的启动器均可调用。

## Rust-OpenBMCLAPI

这个项目是OpenBMCLAPI的Rust实现，主要目的是辅助BMCLAPI分发文件。相比原始节点，该实现具有更高的性能和更低的资源占用。

节点要求：

1. 公网可访问（端口映射也可），可以非80端口
2. 10Mbps以上的上行速度
3. 可以长时间稳定在线
4. 支持IPv4，也支持IPv6双栈

## 安装

### Cargo安装

```bash
# 克隆仓库
git clone https://github.com/yourusername/rust-bmclapi.git
cd rust-bmclapi

# 编译项目
cargo build --release

# 运行
./target/release/rust-bmclapi
```

如果你看到了 `CLUSTER_ID环境变量必须设置` 的报错，说明一切正常，需要设置参数了。

### Docker安装

我们提供了Docker镜像，方便用户快速部署：

```bash
# 使用官方Docker镜像
docker run -d \
  --name rust-bmclapi \
  -p 4000:4000 \
  -v $(pwd)/cache:/app/cache \
  -e CLUSTER_ID=你的CLUSTER_ID \
  -e CLUSTER_SECRET=你的CLUSTER_SECRET \
  -e BMCLAPI_CACHE_DIR=/app/cache \
  pysio/rust-openbmclapi:main
```

参数说明：

- `-p 4000:4000`: 将容器的4000端口映射到主机的4000端口
- `-v $(pwd)/cache:/app/cache`: 将主机当前目录下的cache文件夹挂载到容器内的/app/cache目录
- `-e CLUSTER_ID`: 设置集群ID环境变量
- `-e CLUSTER_SECRET`: 设置集群密钥环境变量
- `-e BMCLAPI_CACHE_DIR`: 设置数据目录位置（可选，默认为 ./cache）

您也可以使用docker-compose:

```yaml
version: '3'
services:
  rust-bmclapi:
    image: pysio/rust-openbmclapi:main
    container_name: rust-bmclapi
    restart: unless-stopped
    ports:
      - "4000:4000"
    volumes:
      - ./cache:/app/cache
    environment:
      - CLUSTER_ID=你的CLUSTER_ID
      - CLUSTER_SECRET=你的CLUSTER_SECRET
      - BMCLAPI_CACHE_DIR=/app/cache
      # 可选配置
      # - CLUSTER_PORT=4000
      # - ENABLE_UPNP=false
```

将上述内容保存为`docker-compose.yml`文件，然后运行：

```bash
docker-compose up -d
```

### 设置参数

在项目根目录创建一个文件，名为 `.env`

写入如下内容：

```env
CLUSTER_ID=你的CLUSTER_ID
CLUSTER_SECRET=你的CLUSTER_SECRET
CLUSTER_PORT=对外访问端口
BMCLAPI_CACHE_DIR=数据目录路径  # 可选，默认为 ./cache
```

CLUSTER_ID 和 CLUSTER_SECRET 请联系BMCLAPI管理员获取。

如果配置无误的话，运行程序后将开始拉取文件，拉取完成后就会开始等待服务器分发请求。

### 同步数据

Rust-OpenBMCLAPI 会自行同步需要的文件，但初次同步可能速度过慢。如果您的节点是全量节点，可以通过以下命令使用rsync快速同步（以下rsync服务器相同，选择任意一台）：

- `rsync -rzvP openbmclapi@home.933.moe::openbmclapi cache`
- `rsync -avP openbmclapi@storage.yserver.ink::bmcl cache`
- `rsync -azvrhP openbmclapi@openbmclapi.home.mxd.moe::data cache`

## 配置说明

主要配置项：

- `CLUSTER_ID`：集群ID，从BMCLAPI获取
- `CLUSTER_SECRET`：集群密钥，从BMCLAPI获取
- `CLUSTER_IP`：可选，服务器公网IP，不设置会自动获取
- `CLUSTER_PORT`：服务监听端口，默认4000
- `CLUSTER_PUBLIC_PORT`：公网访问端口，默认与CLUSTER_PORT相同
- `ENABLE_UPNP`：是否启用UPnP自动端口映射，默认false
- `CLUSTER_STORAGE`：存储类型，支持file（本地文件）和webdav（WebDAV）
- `BMCLAPI_CACHE_DIR`：数据目录位置，默认为 ./cache

详细配置请参考`.env.example`文件。

## 特性

- 完全兼容原版OpenBMCLAPI协议
- 高性能的文件服务
- 支持多种存储后端（本地文件、WebDAV等）
- 低资源占用
- 自动UPnP端口映射
- 支持SSL加密
- 可自定义数据目录位置
- 实时下载进度显示

## 贡献

欢迎贡献代码、报告问题或提出建议！请提交Issue或Pull Request。

## 许可证

本项目采用AGPLv3许可证 - 详见LICENSE文件。

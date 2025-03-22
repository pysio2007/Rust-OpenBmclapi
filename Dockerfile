FROM rust:1.76-slim as builder

WORKDIR /usr/src/rust-bmclapi
COPY . .

# 安装构建依赖
RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 构建应用
RUN cargo build --release

# 生产镜像
FROM debian:bookworm-slim

# 安装运行时依赖
RUN apt-get update && \
    apt-get install -y ca-certificates libssl3 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 从构建阶段复制构建好的二进制文件
COPY --from=builder /usr/src/rust-bmclapi/target/release/rust-bmclapi /app/
# 复制示例配置文件
COPY --from=builder /usr/src/rust-bmclapi/.env.example /app/

# 创建缓存目录
RUN mkdir -p /app/cache

# 设置环境变量
ENV CLUSTER_PORT=4000
ENV CLUSTER_PUBLIC_PORT=4000
ENV RUST_LOG=info

# 暴露端口
EXPOSE 4000

# 创建并使用非root用户
RUN useradd -m bmclapi
RUN chown -R bmclapi:bmclapi /app
USER bmclapi

# 设置卷
VOLUME ["/app/cache", "/app/.env"]

# 运行应用
CMD ["/app/rust-bmclapi"] 
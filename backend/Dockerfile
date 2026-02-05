# /backend/Dockerfile

# --- Stage 1: Base Image ---
# 使用官方的 Python 3.12 slim-buster 镜像，体积更小
FROM python:3.12-slim AS build_base

# 设置环境变量，确保 Python 输出无缓冲
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# --- Stage 2: Builder ---
# 该阶段用于安装依赖
FROM build_base AS builder

# 设置工作目录
WORKDIR /app

# 安装编译依赖，用于某些 Python 包的安装
RUN apt-get update && apt-get install -y --no-install-recommends build-essential

# 复制依赖文件并安装
# 这样做可以利用 Docker 的层缓存机制，只有当 requirements.txt 变化时才会重新安装
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# --- Stage 3: Final Image ---
# 最终的运行镜像
FROM build_base AS final

# 设置工作目录
WORKDIR /app

# 从 builder 阶段复制已安装的依赖
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# 复制后端应用代码
COPY . .

# 暴露 FastAPI 应用运行的端口
EXPOSE 8000

# 容器启动命令
# 使用 uvicorn 启动 FastAPI 应用，监听所有网络接口
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

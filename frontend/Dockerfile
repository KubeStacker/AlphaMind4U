# /frontend/Dockerfile

# --- Stage 1: Build Stage ---
# 使用官方的 Node.js 24 镜像作为构建环境
FROM node:24-alpine AS build-stage

# 设置工作目录
WORKDIR /app

# 复制 package.json 和 package-lock.json (如果存在)
COPY package.json ./

# 安装项目依赖
# 使用 --no-cache 来减少镜像体积
RUN npm install

# 复制所有前端源代码
COPY . .

# 执行构建命令，生成生产环境的静态文件
# 这会生成一个 /app/dist 目录


RUN npm run build

# --- Stage 2: Production Stage ---
# 使用轻量的 Nginx 镜像作为生产环境
FROM nginx:stable-alpine

# 从构建阶段复制编译好的静态文件到 Nginx 的 web 根目录
COPY --from=build-stage /app/dist /usr/share/nginx/html

# 复制自定义的 Nginx 配置文件
# 这个配置文件包含了对 Vue Router history 模式的支持和对后端的 API 代理
COPY nginx.conf /etc/nginx/nginx.conf

# 暴露 Nginx 监听的 80 端口
EXPOSE 80

# 容器启动时，以非守护进程模式启动 Nginx
CMD ["nginx", "-g", "daemon off;"]

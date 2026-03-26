# /frontend/Dockerfile

# --- Stage 1: Build Stage ---
FROM node:24-alpine AS build-stage

WORKDIR /app

# 复制 package.json 和 package-lock.json（利用 Docker 层缓存）
COPY package.json package-lock.json ./

# 使用 npm ci 按 lock 文件精确安装，比 npm install 更快更可靠
RUN npm ci

# 复制所有前端源代码
COPY . .

RUN npm run build

# --- Stage 2: Production Stage ---
FROM nginx:stable-alpine

COPY --from=build-stage /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/nginx.conf

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]

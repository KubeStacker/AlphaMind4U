#!/bin/bash

# --- 脚本功能 ---
# 一键部署或更新 Jarvis Quant 应用
# - 停止并移除旧容器
# - 重新构建镜像 (确保使用最新代码)
# - 启动新的容器
# - 清理悬空的 Docker 镜像

# 使用颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== 开始部署 Jarvis Quant ===${NC}"

# 1. 检查 docker-compose.yml 文件是否存在
if [ ! -f "../docker-compose.yml" ]; then
    echo "错误：'../docker-compose.yml' 文件未找到！"
    echo "请确保你在 'scripts' 目录下运行此脚本。"
    exit 1
fi

# 切换到项目根目录执行 docker-compose 命令
cd ..

# 2. 停止并移除当前运行的容器
echo -e "${YELLOW}--> 步骤 1/4: 停止并移除旧容器...${NC}"
docker-compose down
echo "旧容器已停止并移除。"

# 3. 强制重新构建镜像
echo -e "${YELLOW}--> 步骤 2/4: 构建新的 Docker 镜像...${NC}"
# 使用 --no-cache 可以确保所有层都重新构建，但会慢一些
# 为了平衡，我们默认使用缓存，但 docker-compose 会检测 Dockerfile 的变化
docker-compose build --force-rm
echo "镜像构建完成。"

# 4. 以后台模式启动所有服务
echo -e "${YELLOW}--> 步骤 3/4: 启动新的容器...${NC}"
docker-compose up -d
echo "所有服务已在后台启动。"

# 5. 清理悬空的 (dangling) 镜像
echo -e "${YELLOW}--> 步骤 4/4: 清理旧的悬空镜像...${NC}"
docker image prune -f
echo "悬空镜像已清理。"

echo -e "${GREEN}=== 部署完成！ ===${NC}"
echo "你可以通过以下地址访问应用:"
echo -e "前端 (Web UI): ${GREEN}http://localhost:8080${NC}"
echo -e "后端 (API Docs): ${GREEN}http://localhost:8000/docs${NC}"

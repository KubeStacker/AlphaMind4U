#!/bin/bash

# --- 脚本功能 ---
# 初始化 Jarvis Quant 的主机环境
# - 检查并安装 Docker 和 Docker Compose
# - 创建必要的数据目录

# 使用颜色输出
GREEN='\033[0;32m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== 开始初始化基础软件环境 ===${NC}"

if false ; then
    # 设置主机名
    hostnamectl set-hostname cvm

    # 设置zsh和oh-my-zsh
    apt update
    apt install zsh
    sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"

    # 订阅clash
    git clone --branch master --depth 1 https://gh-proxy.org/https://github.com/nelvko/clash-for-linux-install.git && cd clash-for-linux-install && bash install.sh
fi 

if ! command -v gemini &> /dev/null; then
    echo "未检测到gemini，正在尝试安装..."  
    # 安装node
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
    source ~/.zshrc
    nvm install 24

    # 安装gemini-cli
    npm install -g @google/gemini-cli; 
else
    echo "gemini 已安装。"
fi

echo -e "${GREEN}=== 开始初始化主机环境 ===${NC}"

# 1. 检查 Docker 是否已安装
if ! command -v docker &> /dev/null
then
    echo "未检测到 Docker，正在尝试安装..."
    # 使用官方脚本安装 Docker (适用于大多数 Linux 发行版)
    # 注意：这需要 root 或 sudo 权限
    curl -fsSL https://get.docker.com -o get-docker.sh
    sh get-docker.sh
    rm get-docker.sh
    
    # 检查安装是否成功
    if ! command -v docker &> /dev/null
    then
        echo "Docker 安装失败，请手动安装后重试。"
        exit 1
    fi
    echo "Docker 安装成功！"
else
    echo "Docker 已安装。"
fi

# 2. 检查 Docker Compose 是否已安装
if ! command -v docker-compose &> /dev/null
then
    echo "未检测到 Docker Compose，正在尝试安装..."
    # 安装 Docker Compose V1 (V2 已集成到 docker compose)
    # 很多系统仍然依赖 V1
    sudo curl -L "https://github.com/docker/compose/releases/download/1.29.2/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose

    # 检查安装是否成功
    if ! command -v docker-compose &> /dev/null
    then
        echo "Docker Compose 安装失败，请手动安装后重试。"
        exit 1
    fi
    echo "Docker Compose 安装成功！"
else
    echo "Docker Compose 已安装。"
fi


# 3. 创建数据持久化目录
echo "正在检查并创建 'data' 目录..."
if [ ! -d "../data" ]; then
    mkdir ../data
    echo "已创建 '../data' 目录，用于持久化数据库。"
else
    echo "'../data' 目录已存在。"
fi

echo -e "${GREEN}=== 环境初始化完成！ ===${NC}"

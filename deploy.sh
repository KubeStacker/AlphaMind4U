#!/bin/bash
# 一键部署脚本

set -e

echo "=========================================="
echo "数据分析系统 - 一键部署脚本"
echo "=========================================="

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    echo "错误: Docker未安装，请先安装Docker"
    exit 1
fi

# 检查Docker Compose是否安装
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "错误: Docker Compose未安装，请先安装Docker Compose"
    exit 1
fi

# 创建数据目录
echo "创建数据目录..."
mkdir -p /data/mysql/data
mkdir -p /data/mysql/conf
mkdir -p /data/mysql/logs
chmod -R 755 /data/mysql

# 停止并删除旧容器（如果存在）
echo "清理旧容器..."
docker-compose down 2>/dev/null || true
docker stop stock-mysql stock-backend stock-frontend 2>/dev/null || true
docker rm stock-mysql stock-backend stock-frontend 2>/dev/null || true

# 构建并启动服务
echo "构建并启动服务..."
cd /root/app
docker-compose up -d --build

# 等待MySQL启动
echo "等待MySQL服务启动..."
sleep 3

# 检查服务状态
echo ""
echo "=========================================="
echo "部署完成！"
echo "=========================================="
echo ""
echo "服务状态："
docker-compose ps
echo ""
echo "访问地址："
echo "  前端: http://localhost 或 http://$(hostname -I | awk '{print $1}')"
echo "  后端API: http://localhost:8000"
echo "  API文档: http://localhost:8000/docs"
echo ""
echo "数据库信息："
echo "  主机: localhost:3306"
echo "  数据库: stock"
echo "  用户名: admin"
echo "  密码: admin"
echo "  管理员密码: password"
echo ""
echo "查看日志："
echo "  docker-compose logs -f"
echo ""

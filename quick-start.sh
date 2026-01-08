#!/bin/bash
# 快速启动脚本

echo "=========================================="
echo "股票数据分析系统 - 快速启动"
echo "=========================================="

# 检查是否已安装Docker
if ! command -v docker &> /dev/null; then
    echo "错误: 请先安装Docker"
    exit 1
fi

# 创建数据目录
echo "创建数据目录..."
mkdir -p /data/mysql/data
mkdir -p /data/mysql/conf
mkdir -p /data/mysql/logs
chmod -R 755 /data/mysql

# 启动服务
echo "启动Docker Compose服务..."
cd /root/app
docker-compose up -d --build

echo ""
echo "等待服务启动..."
sleep 10

echo ""
echo "=========================================="
echo "服务启动完成！"
echo "=========================================="
echo ""
echo "访问地址："
echo "  前端: http://$(hostname -I | awk '{print $1}')"
echo "  后端API: http://$(hostname -I | awk '{print $1}'):8000"
echo "  API文档: http://$(hostname -I | awk '{print $1}'):8000/docs"
echo ""
echo "查看服务状态: docker-compose ps"
echo "查看日志: docker-compose logs -f"
echo "停止服务: docker-compose down"
echo ""

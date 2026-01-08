#!/bin/bash
# MySQL 8 Docker部署脚本

# 创建数据目录
mkdir -p /data/mysql/data
mkdir -p /data/mysql/conf
mkdir -p /data/mysql/logs

# 设置目录权限
chmod -R 755 /data/mysql

# 停止并删除已存在的容器（如果存在）
docker stop stock-mysql 2>/dev/null || true
docker rm stock-mysql 2>/dev/null || true

# 启动MySQL 8容器
docker run -d \
  --name stock-mysql \
  --restart=always \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=password \
  -e MYSQL_DATABASE=stock \
  -e MYSQL_USER=admin \
  -e MYSQL_PASSWORD=admin \
  -v /data/mysql/data:/var/lib/mysql \
  -v /data/mysql/conf:/etc/mysql/conf.d \
  -v /data/mysql/logs:/var/log/mysql \
  mysql:8.0 \
  --character-set-server=utf8mb4 \
  --collation-server=utf8mb4_unicode_ci \
  --default-authentication-plugin=mysql_native_password

echo "MySQL 8 容器启动中，请等待30秒后数据库初始化完成..."
sleep 30

# 等待MySQL完全启动
echo "等待MySQL服务就绪..."
for i in {1..30}; do
  if docker exec stock-mysql mysqladmin ping -h localhost --silent; then
    echo "MySQL服务已就绪！"
    break
  fi
  echo "等待中... ($i/30)"
  sleep 2
done

# 执行初始化SQL
echo "执行数据库初始化脚本..."
docker cp /root/app/database/init.sql stock-mysql:/tmp/init.sql
docker exec stock-mysql mysql -uadmin -padmin stock < /tmp/init.sql 2>/dev/null || \
docker exec stock-mysql mysql -uroot -ppassword stock < /tmp/init.sql

echo "MySQL 8 部署完成！"
echo "数据库连接信息："
echo "  主机: localhost:3306"
echo "  数据库: stock"
echo "  用户名: admin"
echo "  密码: admin"
echo "  管理员密码: password"

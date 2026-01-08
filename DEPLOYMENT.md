# 部署说明

## 系统要求

- Ubuntu 18.04+ 或其他支持Docker的Linux系统
- Docker 20.10+
- Docker Compose 1.29+
- 至少 10GB 可用磁盘空间（用于MySQL数据存储）
- 网络连接（用于访问akshare数据源）

## 快速部署

### 一键部署

```bash
cd /root/app
./deploy/deploy.sh
```

或者使用快速启动脚本：

```bash
cd /root/app
./quick-start.sh
```

### 手动部署步骤

1. **准备数据目录**

```bash
mkdir -p /data/mysql/data
mkdir -p /data/mysql/conf
mkdir -p /data/mysql/logs
chmod -R 755 /data/mysql
```

2. **启动所有服务**

```bash
cd /root/app
docker-compose up -d --build
```

3. **等待服务启动**

```bash
# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f
```

## 服务说明

### MySQL数据库

- **容器名**: stock-mysql
- **端口**: 3306
- **数据目录**: /data/mysql/data
- **配置目录**: /data/mysql/conf
- **日志目录**: /data/mysql/logs

**连接信息**:
- 数据库名: stock
- 用户名: admin
- 密码: admin
- 管理员密码: password

### 后端服务

- **容器名**: stock-backend
- **端口**: 8000
- **API文档**: http://服务器IP:8000/docs
- **健康检查**: http://服务器IP:8000/

### 前端服务

- **容器名**: stock-frontend
- **端口**: 80
- **访问地址**: http://服务器IP

## 数据采集

系统会在每天18:00自动执行数据采集任务。首次部署后，建议手动触发一次数据采集：

```bash
# 进入后端容器
docker exec -it stock-backend bash

# 执行数据采集
python -c "from data_collector import DataCollector; c = DataCollector(); c.collect_stock_daily_data(); c.collect_sector_daily_data(); c.collect_hot_stocks(); c.collect_gainers(); c.clean_old_data(); c.close()"
```

**注意**: 首次数据采集可能需要较长时间（数小时），因为需要采集所有A股的数据。

## 常用命令

### 查看服务状态

```bash
docker-compose ps
```

### 查看日志

```bash
# 查看所有服务日志
docker-compose logs -f

# 查看特定服务日志
docker-compose logs -f backend
docker-compose logs -f frontend
docker-compose logs -f mysql
```

### 重启服务

```bash
# 重启所有服务
docker-compose restart

# 重启特定服务
docker-compose restart backend
```

### 停止服务

```bash
docker-compose down
```

### 停止并删除数据（谨慎使用）

```bash
docker-compose down -v
# 注意：这会删除所有数据，包括MySQL数据
```

### 重新构建

```bash
docker-compose up -d --build
```

## 数据备份

### 备份MySQL数据

```bash
# 备份数据库
docker exec stock-mysql mysqldump -uadmin -padmin stock > /root/stock_backup_$(date +%Y%m%d).sql

# 恢复数据库
docker exec -i stock-mysql mysql -uadmin -padmin stock < /root/stock_backup_20231201.sql
```

### 备份数据目录

```bash
# 备份整个数据目录
tar -czf /root/mysql_data_backup_$(date +%Y%m%d).tar.gz /data/mysql/data
```

## 故障排查

### 1. 服务无法启动

检查Docker和Docker Compose是否正常运行：

```bash
docker --version
docker-compose --version
```

### 2. MySQL连接失败

检查MySQL容器是否正常运行：

```bash
docker ps | grep mysql
docker logs stock-mysql
```

### 3. 数据采集失败

检查网络连接和akshare接口：

```bash
docker exec stock-backend python -c "import akshare as ak; print(ak.__version__)"
```

### 4. 前端无法访问后端API

检查nginx配置和网络连接：

```bash
docker exec stock-frontend cat /etc/nginx/conf.d/default.conf
docker logs stock-frontend
```

### 5. 端口冲突

如果端口被占用，修改 `docker-compose.yml` 中的端口映射：

```yaml
ports:
  - "新端口:容器端口"
```

## 性能优化

### 1. 限制数据采集频率

在 `backend/data_collector.py` 中调整 `time.sleep()` 的值，避免请求过快。

### 2. MySQL性能优化

在 `/data/mysql/conf/my.cnf` 中添加MySQL优化配置：

```ini
[mysqld]
innodb_buffer_pool_size = 1G
max_connections = 200
```

### 3. 定期清理日志

```bash
# 清理Docker日志
docker system prune -f
```

## 安全建议

1. **修改默认密码**: 部署后立即修改数据库密码
2. **防火墙配置**: 只开放必要的端口（80, 8000）
3. **定期备份**: 设置定时任务自动备份数据
4. **更新依赖**: 定期更新Docker镜像和依赖包

## 监控和维护

### 查看资源使用

```bash
docker stats
```

### 查看磁盘使用

```bash
df -h
du -sh /data/mysql/data
```

### 定时任务

可以设置cron任务定期备份数据：

```bash
# 编辑crontab
crontab -e

# 添加每日备份任务（每天凌晨2点）
0 2 * * * docker exec stock-mysql mysqldump -uadmin -padmin stock > /root/backups/stock_$(date +\%Y\%m\%d).sql
```

## 联系和支持

如遇到问题，请检查：
1. Docker和Docker Compose版本
2. 系统资源（内存、磁盘）
3. 网络连接
4. 服务日志

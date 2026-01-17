# 502 Bad Gateway 错误诊断指南

## 错误说明
502 错误表示前端（Nginx）能够接收请求，但无法从后端服务获取有效响应。

## 快速诊断步骤

### 1. 检查后端容器状态
```bash
# 查看所有容器状态
docker ps -a

# 查看后端容器状态
docker ps | grep backend

# 如果容器未运行，查看退出原因
docker logs stock-backend
```

### 2. 检查后端服务日志
```bash
# 查看实时日志
docker logs -f stock-backend

# 查看最近100行日志
docker logs --tail 100 stock-backend
```

### 3. 检查网络连接
```bash
# 检查Docker网络
docker network ls

# 检查容器网络连接
docker network inspect stock-network

# 测试前端容器能否访问后端
docker exec stock-frontend ping -c 3 backend
```

### 4. 检查后端服务是否正常启动
```bash
# 进入后端容器
docker exec -it stock-backend bash

# 检查进程
ps aux | grep uvicorn

# 检查端口监听
netstat -tlnp | grep 8000
# 或
ss -tlnp | grep 8000
```

### 5. 测试后端API直接访问
```bash
# 从宿主机测试（如果端口映射正常）
curl http://localhost:8000/api/auth/login -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"test","password":"test"}'

# 从容器内测试
docker exec stock-backend curl http://localhost:8000/docs
```

### 6. 重启服务
```bash
# 重启后端服务
docker restart stock-backend

# 或重启整个栈
docker-compose restart backend

# 完全重启
docker-compose down
docker-compose up -d
```

## 常见原因和解决方案

### 原因1: 后端容器未运行
**症状**: `docker ps` 中看不到 `stock-backend` 容器

**解决方案**:
```bash
docker-compose up -d backend
```

### 原因2: 后端服务启动失败
**症状**: 容器运行但服务崩溃，日志中有错误信息

**解决方案**:
1. 查看日志: `docker logs stock-backend`
2. 检查常见问题：
   - 数据库连接失败（检查 MySQL 容器状态）
   - Python 依赖缺失
   - 代码语法错误
   - 端口被占用

### 原因3: 数据库连接问题
**症状**: 日志中显示数据库连接错误

**解决方案**:
```bash
# 检查 MySQL 容器
docker ps | grep mysql

# 检查数据库连接
docker exec stock-backend python -c "
from db.database import get_db
try:
    with get_db() as db:
        print('数据库连接成功')
except Exception as e:
    print(f'数据库连接失败: {e}')
"
```

### 原因4: 网络配置问题
**症状**: 容器运行但无法互相通信

**解决方案**:
```bash
# 检查网络
docker network inspect stock-network

# 重新创建网络
docker-compose down
docker network prune
docker-compose up -d
```

### 原因5: 后端服务启动慢
**症状**: 服务正在启动但还未就绪

**解决方案**:
- 等待30-60秒后重试
- 检查日志确认服务已完全启动
- 考虑添加健康检查

## 预防措施

### 添加健康检查
在 `docker-compose.yml` 中为后端添加健康检查：

```yaml
backend:
  # ... 其他配置
  healthcheck:
    test: ["CMD", "curl", "-f", "http://localhost:8000/docs"]
    interval: 10s
    timeout: 5s
    retries: 5
    start_period: 30s
```

### 监控服务状态
```bash
# 设置监控脚本
watch -n 5 'docker ps | grep -E "backend|frontend|mysql"'
```

## 获取帮助

如果以上步骤都无法解决问题，请提供以下信息：
1. `docker ps -a` 的输出
2. `docker logs stock-backend` 的最后50行
3. `docker network inspect stock-network` 的输出
4. 前端浏览器控制台的完整错误信息

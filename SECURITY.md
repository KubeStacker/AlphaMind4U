# 安全加固说明

## 已实现的安全措施

### 1. 用户认证
- JWT Token认证
- 密码使用bcrypt加密存储
- Token有效期8小时

### 2. 防暴力破解
- 登录失败5次后锁定账户30分钟
- 记录所有登录尝试（成功/失败）
- 记录IP地址和User-Agent

### 3. API保护
- 所有数据API都需要认证
- 自动处理Token过期
- 401错误自动跳转登录页

### 4. 前端安全
- 路由守卫保护所有页面
- Token存储在localStorage
- 自动拦截401错误

## 默认管理员账号

- **用户名**: admin
- **密码**: admin123

**⚠️ 重要：首次部署后请立即修改默认密码！**

## 修改管理员密码

```bash
# 进入后端容器
docker exec -it stock-backend bash

# 运行初始化脚本
python init_admin.py <新密码>
```

## 生产环境安全建议

1. **修改JWT密钥**：在环境变量中设置 `JWT_SECRET_KEY`
2. **修改默认密码**：部署后立即修改管理员密码
3. **使用HTTPS**：配置SSL证书
4. **限制IP访问**：使用防火墙或Nginx限制访问IP
5. **定期审查登录日志**：检查异常登录尝试
6. **数据库安全**：使用强密码，限制数据库访问

## 查看登录日志

```sql
-- 查看最近的登录记录
SELECT * FROM login_logs ORDER BY created_at DESC LIMIT 50;

-- 查看失败的登录尝试
SELECT * FROM login_logs WHERE login_status = 'failed' ORDER BY created_at DESC;

-- 查看特定用户的登录记录
SELECT * FROM login_logs WHERE username = 'admin' ORDER BY created_at DESC;
```

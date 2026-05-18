# 核心约束

## DuckDB 数据库访问

**规则：** 使用 DuckDB 嵌入式单进程模型，通过 `/admin/db/query` API 查询。

**原因：**
- DuckDB 不支持高并发写入
- 直接 `duckdb.connect()` 会导致 `Unique file handle conflict`
- 共享连接上 `register()` 后必须及时 `unregister()`

**正确方式：**
```bash
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM stock_basic LIMIT 5"}'
```

**错误方式：**
```python
# ❌ 禁止在仓库外直接打开
import duckdb
con = duckdb.connect('/root/jarvis/data/jarvis.duckdb')
```

## Tushare Pro 数据源

**规则：** 优先使用 Tushare Pro 接口，实现前确认接口所需积分。

**当前限制：**
- Token 仅有 2000 积分
- 高积分接口（如 `stk_factor`、`daily_hf`）无权限调用
- 如遇积分不足，给出说明即可，不用强求验证

**参考：** `backend/etl/providers/` 中已有的 Tushare 接口封装

## 容器化开发

**规则：** 代码在 Docker 容器中运行，修改后通过 curl 验证。

**正确流程：**
1. 修改文件（自动同步到容器）
2. uvicorn `--reload` 自动重载
3. 使用 curl 调用 API 验证功能

**禁止操作：**
- 重启容器
- 手工 copy 文件到容器
- 在主机执行 `npm run build`

## 验证示例

```bash
# 测试数据库查询
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1"}'

# 测试市场情绪
curl "http://localhost:8000/admin/market_sentiment?days=30"

# 测试数据完整性
curl "http://localhost:8000/admin/integrity?start_date=2025-01-01&end_date=2025-12-31"
```

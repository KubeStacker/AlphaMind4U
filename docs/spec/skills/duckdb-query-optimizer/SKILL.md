# DuckDB 查询优化技能

## 描述

管理 DuckDB 连接、优化查询性能、处理批量导入和临时视图清理。

## 使用场景

- 获取数据库连接
- 执行 SQL 查询
- 批量导入数据
- 清理临时视图

## 调用方式

**获取连接：**

```python
from db.connection import get_connection

con = get_connection()
result = con.execute("SELECT * FROM stock_basic LIMIT 5").fetchdf()
```

## 连接管理

**单例模式：** 后端使用进程级共享 DuckDB 连接

**资源配置：**
- `duckdb_memory_limit` - 内存限制
- `duckdb_threads` - 线程数

## 批量导入

**原则：** 优先分批落库，避免长连接残留临时视图

```python
# 注册 DataFrame（用完必须 unregister）
con.register('temp_df', df)
con.execute("INSERT INTO target_table SELECT * FROM temp_df")
con.unregister('temp_df')
```

## 查询优化

**使用 API：** 优先通过 `/admin/db/query` 执行查询

```bash
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM daily_price WHERE trade_date = \"2025-01-01\""}'
```

## 注意事项

- 禁止从其他进程直接打开临时 DuckDB 会话
- 共享连接上 `register()` 后必须及时 `unregister()`
- 大批量导入优先分批落库

## 参考

- `backend/db/connection.py` - 数据库连接管理
- `backend/db/schema.py` - 数据库 schema
- `docs/spec/concepts/01-core-constraints.md` - 核心约束

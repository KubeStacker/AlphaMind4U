# 后端开发模式

## FastAPI 路由注册

**模式：** 在 `main.py` 中统一注册路由，使用 `/admin` 前缀

```python
from api.routes import users_router, stocks_router

app = FastAPI(title="Jarvis-Quant Backend")

app.include_router(users_router, prefix="/admin")
app.include_router(stocks_router, prefix="/admin")
```

## Pydantic 模型验证

**规则：** 对所有 API 请求/响应使用 Pydantic 模型

```python
from pydantic import BaseModel

class QueryRequest(BaseModel):
    sql: str

class QueryResponse(BaseModel):
    data: list
    columns: list
```

## 异步处理

**模式：** 对 FastAPI 端点使用 `async/await`，用 `asyncio.to_thread()` 包装同步函数

```python
import asyncio

@app.post("/admin/db/query")
async def query_db(request: QueryRequest):
    result = await asyncio.to_thread(execute_query, request.sql)
    return result
```

## ETL 任务模式

**结构：** `backend/etl/tasks/` 下创建任务类，继承基类

**注册：** 在 `sync.py` 中注册或通过 API 端点暴露

**调度：** 使用 APScheduler 进行定时任务

## 数据库连接

**模式：** 使用 `db/connection.py` 中的共享连接助手

**原则：** 避免在业务模块中直接打开新的 `duckdb.connect()`

```python
from db.connection import get_connection

def execute_query(sql: str):
    con = get_connection()
    result = con.execute(sql).fetchdf()
    return result
```

## DuckDB 资源配置

**环境变量：**
- `duckdb_memory_limit` - 内存限制
- `duckdb_threads` - 线程数

**注意：** 避免默认配置吃满内存

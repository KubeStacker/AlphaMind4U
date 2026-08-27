# 模块概览

| 模块 | 主要路径 |
|------|----------|
| 数据采集 | `backend/etl/`、`backend/api/routes/etl.py`、`backend/db/` |
| MCP 数据读取 | `ai/mcp/server.py`、`backend/api/routes/datasets.py` |

MCP 依赖 backend 的共享 DuckDB 连接，不直接打开数据库文件。项目没有
前端、用户业务、策略或推测模块。

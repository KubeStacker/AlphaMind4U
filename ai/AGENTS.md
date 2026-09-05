# ai/ — AI 智能体技能指令库

本目录只收敛与 AI 智能体相关的提示词技能（非服务代码）：

| 模块 | 说明 | 关键文件 |
|------|------|----------|
| `skills/` | AI Agent 专属技能指令库 | `spec-writer/`、`tushare-data-fetcher/`、`duckdb-query-optimizer/` |

说明：MCP 只读代理已迁至 `services/mcp-server/`（Go），通过 HTTP 反代量化后端，
不直连库文件。skills 里的 DuckDB 查询优化准则同样适用于 SQLite（点查 + LIMIT）。

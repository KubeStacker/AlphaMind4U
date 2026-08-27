# ai/ — AI 与智能体能力子系统

本目录统一收敛 Jarvis 项目所有与 AI / MCP 协议 / 智能体相关的组件：

| 模块 | 说明 | 关键文件 |
|------|------|----------|
| `mcp/` | Model Context Protocol 只读数据服务 | `server.py`, `Dockerfile` |
| `skills/` | AI Agent 专属技能指令库 | `spec-writer/`, `tushare-data-fetcher/`, `duckdb-query-optimizer/` |

## 约束
- MCP 服务通过 HTTP API 与 `backend` 通信，不直接打开 DuckDB 数据库文件。
- Skills 定义了 AI 智能体在进行 Spec 编写、数据采集及 DuckDB 查询优化时的行为准则。

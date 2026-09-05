# spec/architecture.md — 目标架构与删除清单

## 1. 目标树

```text
jarvis/
  AGENTS.md  jarvis.sh  .env.example
  spec/  shared/contracts/
  services/
    quant-backend/      # Go :8000，量化域唯一
    mcp-server/         # Go :8081，只读代理
    _template-backend/  # 新后端脚手架
  frontend/             # Vite+Vue3+TS :8080
  data/  scripts/
```

## 2. 域划分

- `quant-backend`：采集 ETL + 分析 + strategy，同进程、同库（`data/quant.db` SQLite）。
- `mcp-server`：无库，HTTP 反代 `quant-backend:/admin/datasets`。
- 未来后端（如 docreader）：独立进程、独立库文件、独立端口，与 quant 无关。

## 3. 前端路由（URI 管多后端）

映射表唯一真源见 `frontend/routing.md`（端口见 `backend/ports.md`）：
`/admin/*→quant`、`/mcp→mcp`、`/doc/*→docreader（预留）`、`/settings→本地`。

## 4. 删除清单（一步到位，不过渡）

删：`backend/`（py + venv）、`ai/mcp/`（py + wheels）、`docker-compose*.yml`、`*/Dockerfile`、
`frontend/nginx.conf` + 旧单文件 `index.html`、`data/jarvis.duckdb*`（已迁 SQLite）、
`data/jarvis.db`、`scripts/`（并入 `jarvis.sh`）、`.env.mcp`。
留：`data/quant.db`、`data/docs/`、`ai/skills/`（纯提示词）。

2026-09-16策略精简：删除S3的新任务入口与请求类型；删除前端S0候选选项，保留研究基准计算和全部历史记录。

## 5. 依赖上限

- quant：stdlib + `modernc.org/sqlite`（pure-Go）+ `robfig/cron` + tushare 直调（`net/http`）。
- mcp：stdlib only。
- 前端：`vite + vue + router + pinia + tailwind + echarts（按需）`。
- 禁 ORM、禁重 UI 框架、禁 BFF 网关。

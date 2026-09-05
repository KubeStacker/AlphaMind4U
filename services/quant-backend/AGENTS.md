# services/quant-backend/AGENTS.md — 量化后端（AI 必读）

> Go 实现，量化域唯一后端。端口 8000，独占 `data/quant.db`。

## 可读文件

- `internal/api/` — `/admin` 接口（datasets/etl）。
- `internal/engine/` — `engine.go` 装配 + `task_*.go` 任务 + `run.go` 分发。
- `internal/{db,provider,calendar,sched,config}/` — 按名即义。
- 规范：`spec/backend/quant/`、`spec/backend/ports.md`、`spec/sdd.md`。

## 禁区

- 禁直连他人库文件；禁跨服务 import（`mcpserver` 等）。
- 改接口先改 `shared/contracts`；新表走 `migrations/`（如有）+ 登记 `database.md`。
- 热路径禁逐行日志；查询强制 `LIMIT`；单文件行宽 ≤120 列。

## 本地命令（在 `services/quant-backend/` 下）

```sh
go vet ./... && go build -o bin/quant-backend ./cmd/server
QUANT_DB_PATH=/root/jarvis/data/quant.db ./bin/quant-backend
```

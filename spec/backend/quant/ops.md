# spec/backend/quant/ops — 量化后端运维片段

- 构建：`go build -o bin/quant-backend ./cmd/server`；启动经 `jarvis.sh`（见 `ops/`）。
- SQLite 参数：`journal_mode=WAL` + `synchronous=NORMAL` + 独占单连接；常驻目标 `<400MB`。
- 迁移入口：`migrations/NNNN_*.sql` 顺序执行，`schema_version` 记账（见 `database.md`）。
- 日志：`data/logs/quant.log`；慢查询（>1s）记 warn 并附 `EXPLAIN`。
- 超内存红线先查慢查询，再谈加配，不直接调大 limit。

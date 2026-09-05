# spec/ops/jarvis-sh — 唯一入口与资源红线

## 1. 命令

```sh
./jarvis.sh start|stop|restart|status|logs [quant|mcp|frontend|all]
```

- `start`：按 `quant → mcp（等 quant /health）→ frontend（vite dev，含反代）` 顺序，
  `nohup … >> data/logs/<svc>.log &`，写 `data/run/<svc>.pid`。Go 先 `go build -o bin/`。
- `stop`：读 pid kill，超 10s 未退则 `kill -9`，清 pid。
- `status`：pid 存活 + `curl /health` 双检。`logs`：`tail -f data/logs/<svc>.log`。

## 2. 环境

- Go ≥1.22、Node ≥20 由各服务 `AGENTS.md` 声明，`jarvis.sh` 只检查不安装。
- `.env` 从 `.env.example` 复制，禁提交真 token。

## 3. 资源红线（2C/3.8G）

quant `<400MB`、mcp `<100MB`、frontend preview `<150MB`；
超限先查慢查询/`EXPLAIN` 再谈加配。磁盘剩 15G：DB 备份只留最近 2 份，
`data/logs` 按 50MB 轮转。

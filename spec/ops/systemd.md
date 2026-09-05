# spec/ops/systemd — 开机自启模板

> 默认不启用；需要随机器自启时才用。`Restart=on-failure`。

```ini
[Unit]
Description=jarvis <svc>
After=network.target

[Service]
Type=simple
WorkingDirectory=/root/jarvis
ExecStart=/root/jarvis/jarvis.sh start <svc>
ExecStop=/root/jarvis/jarvis.sh stop <svc>
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

- 三服务各一 unit（`jarvis-quant/mcp/frontend`），顺序依赖用 `After=` 表达。
- 自启与手动 `jarvis.sh` 二选一，不混用，防双进程抢独占库锁。

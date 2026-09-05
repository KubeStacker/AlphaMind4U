# spec/ops/README — 运维索引

> 宿主原生进程管理，无 docker。唯一入口 `jarvis.sh`。

- `jarvis-sh.md` — 五命令、日志/PID、环境、资源红线。
- `retention.md` — 日志轮转、磁盘水位、OOM 防护（清理机制）。
- `drills.md` — 故障演练手册与 5 日连续运行计划。
- `systemd.md` — 开机自启模板（默认不启用）。
- 端口总表见 `backend/ports.md`；各服务特有运维看 `backend/<name>/ops.md`。

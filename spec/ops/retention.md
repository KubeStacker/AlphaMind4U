# spec/ops/retention — 日志清理、磁盘水位、OOM 防护

> 目标：磁盘不打满、内存不爆。清理走定时任务 + 启动自检，不靠人记。

## 1. 应用日志（代码侧规则）

- 级别：生产默认 `info`；`debug` 只本地开，禁提交开启状态。
- 热路径（查询/ETL 行循环）禁逐行打日志，要打则采样（每 1000 行 1 条）或只记汇总。
- 禁打全量数据：请求体/结果集只记行数 + 耗时 + 前 200 字错误，不记全量 rows。
- 日志只写 `data/logs/<svc>.log`（stdout 重定向），不写散文件。

## 2. 日志轮转（文件侧规则）

- 按 50MB 轮转，保留最近 10 份，压缩旧份（`.gz`），30 天外删除。
- 实现：`jarvis.sh` 内 `rotatelogs` 逻辑或 `logrotate` 配置二选一，
  以 `jarvis.sh` 为准，模板见本目录附录（实现时补）。
- `status` 命令附带检查：任一日志 >80MB 即 warn 提示。

## 3. 磁盘水位

- 水位线：`/` 或 `data/` 使用率 ≥80% 告警（`status` 输出 warn），≥90% 拒绝新 ETL
  全量任务（只允许增量 `daily_update`），并自动清最旧 DB 备份。
- DB 备份只留最近 2 份；`*.tmp`、`data/tmp/`、`__pycache__` 重启时清；
  ETL 临时 Parquet/CSV 落 `data/tmp/`，任务结束即删，重启兜底清 7 天外文件。
- `data/` 下禁放 `venv`、`node_modules`、`*.whl`（见删除清单）。

## 4. OOM 防护

- 硬顶：quant `<400MB`、mcp `<100MB`、
  frontend preview `<150MB`；`systemd` 启用时加 `MemoryMax=` 同值。
- 代码禁令：查询强制 `LIMIT`（见 `quant/api.md`）；大导出走流式写（分块 flush，
  禁一次性拼全量 slice）；ETL 批量 ≤5000 行一批，批间释放（`nil` 切片 + 显式 GC 点）。
- SQLite：单连接串行（见 `quant/etl.md`）；WAL 膨胀（>100MB）低峰执行检查点
  （停写后重开连接即合并，见 `database.md` 备份节）。
- 排查：`logs` 先查慢查询 warn；复现才开 `pprof`（默认关闭，`env` 门控，
  禁常开暴露）。

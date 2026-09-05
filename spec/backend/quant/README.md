# spec/backend/quant/README — 量化后端索引

> 量化域唯一后端：数据采集 ETL + 分析 + strategy，同进程同库。

- 端口：8000（总表见 `../ports.md`）；健康 `GET / → {status,service}`。
- 库：`data/quant.db`（SQLite），本进程独占，详见 `database.md`。
- `api.md` — 接口对齐清单（datasets + etl + tasks）。
- `etl.md` — 定时调度、任务消费、tushare 限速、单连接串行。
- `database.md` — 选型、范式、效率检查、迁移规范。
- `publish.md` — 数据发布日历（何时可取的唯一依据）。
- `ops.md` — 构建、启动参数、内存红线、日志。

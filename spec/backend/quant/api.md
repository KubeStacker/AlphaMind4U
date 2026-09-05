# spec/backend/quant/api — 接口对齐清单

> 与现 Python 行为逐项一致；改接口先改 `shared/contracts`。

## 1. 布局

```text
services/quant-backend/
  cmd/server/main.go
  internal/{api,core,db,etl/{scheduler,sync,tasks,providers}}
  migrations/NNNN_*.sql
  go.mod  AGENTS.md  bin/
```

## 1.1 单用户登录（免鉴权，唯一例外）

- `POST /admin/auth/login`：用户名、密码、共享令牌三者同时匹配
  `JARVIS_ADMIN_USER` / `JARVIS_ADMIN_PASSWORD` / `JARVIS_STATIC_AUTH_TOKEN`
  才 200 并返回共用令牌；任一 mismatch 401；服务端未配齐账号 503。
- 密码与令牌常量时间比对；单用户控制台不做会话/过期/多角色。

## 2. 数据集只读（白名单 22 表）

- `GET /admin/datasets`：字段 `{name,description,date_column}`。
- `GET /admin/datasets/{name}/schema`：`PRAGMA table_info`，剔除黑名单列。
- `POST /admin/datasets/query`：单表、参数化、`LIMIT≤1000`、`OFFSET≤1000000`、
  `ORDER BY` 白名单校验。禁 join、禁聚合、禁 `SELECT *`。

## 3. 采集任务（队列 + 串行消费 + 去重）

- `POST /admin/etl/sync`（202）：任务名 24 个（basic/calendar/price/daily/
  moneyflow/daily_basic/concepts/index/index_members/express/income/
  financials/balancesheet/balance_sheet/cashflow/fina_indicator/dividend/
  forecast/suspend/namechange/index_weight/moneyflow_hsgt/hsgt/margin/
  daily_update），入队 `etl_tasks`，后台单消费者串行执行。
- 同参数 PENDING/RUNNING 已存在则复用其 ID，不重复入队。
- 磁盘 ≥90% 时拒绝全量任务（force/years>0/basic/calendar/concepts/
  index_members），返回 503，只允许增量。
- `GET /admin/tasks/status?limit≤100`：按创建时间倒序，状态含
  PENDING/RUNNING/COMPLETED/FAILED/SKIPPED（权限不足记 SKIPPED）。
- `daily_update` 任一步骤失败则整体 FAILED，不再“失败却 COMPLETED”。

## 4. 覆盖治理

- `GET /admin/quality/coverage`：每表边界、总量、近 5 日计数 + 上市股票池，
  供质量看板与补采决策（前端 `/quality` 页直调）。

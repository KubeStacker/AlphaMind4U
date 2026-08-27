# 数据采集 (Data Collection)

## 概述

后端从 Tushare 采集原始 A 股数据并写入 DuckDB。项目无前端、无 SQL
控制台，也不生成技术指标、情绪、评分、推荐、策略或预测结果。

## API

| 端点 | 方法 | 说明 |
|------|------|------|
| `/admin/etl/sync` | POST | 将白名单采集任务加入持久化队列 |
| `/admin/tasks/status` | GET | 查看最近采集任务状态 |

任务类型：`basic`、`calendar`、`price`/`daily`、`moneyflow`、
`daily_basic`、`concepts`、`index`、`index_members`、`express`、
`income`/`financials`、`balancesheet`/`balance_sheet`、`cashflow`、
`fina_indicator`、`dividend`、`forecast`、`suspend`、`namechange`、
`index_weight`、`moneyflow_hsgt`/`hsgt`、`margin`、`daily_update`。

## 数据表

- `stock_basic`
- `daily_price`
- `stock_concepts` / `stock_concept_details`
- `stock_moneyflow`
- `market_index`
- `stock_margin`
- `stock_income`
- `stock_balance_sheet`
- `stock_cashflow`
- `stock_fina_indicator`
- `stock_daily_basic`
- `stock_index_member_all`
- `stock_express`
- `stock_suspend`
- `stock_namechange`
- `stock_dividend`
- `index_weight`
- `stock_forecast`
- `moneyflow_hsgt`
- `trade_calendar`
- `etl_tasks`（采集任务运行状态）

旧业务表可能继续存在于已有数据库文件中，但初始化不再创建、迁移或写入，
也不会在精简过程中主动 DROP。

## 核心模块

| 模块 | 说明 |
|------|------|
| `etl/providers/tushare_pro.py` | Tushare Provider 与限流重试 |
| `etl/tasks/` | 原始数据采集、规范化和落库 |
| `etl/sync.py` | 采集任务编排 |
| `etl/scheduler.py` | 定时采集 |
| `api/routes/etl.py` | 持久化任务队列 API |
| `db/connection.py` | 进程级 DuckDB 共享连接 |
| `db/schema.py` | 原始采集表与任务表 DDL |

## 约束

- 仅允许字段选择、日期转换、单位统一、去重及安全 upsert 等确定性规范化。
- `daily_price` 保存 Tushare OHLCV、成交额和 `adj_factor`，不写技术因子 JSON。
- 概念同步继续使用 staging 表与原子发布。
- DataFrame 注册到共享连接后必须在 `finally` 中注销。
- Tushare 失败采用有界重试，不绕过积分限制。

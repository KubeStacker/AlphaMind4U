# 数据同步 (Data Sync)

## 概述

位于 Settings > 数据管理 tab（仅管理员可见）。提供 ETL 数据同步、数据完整性检查、数据验证和 SQL 控制台功能。

## 前端

### 页面/组件

| 文件 | 说明 |
|------|------|
| `frontend/src/views/Settings.vue` | 数据管理 tab — 表统计、同步控件、完整性热力图、SQL 控制台 |

### 核心交互

1. **数据仪表盘**：展示所有数据表的记录数、最新日期、最早日期
2. **同步触发**：按任务类型触发 ETL（daily/basic/concepts/calendar/price/index/moneyflow 等）
3. **数据完整性**：按日期范围查看各表 FULL/PARTIAL/MISSING 状态
4. **数据验证**：对比 Tushare API 与数据库的数据一致性
5. **SQL 控制台**：执行只读 SELECT 查询

## 后端

### API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/admin/etl/sync` | POST | 统一 ETL 同步入口（支持 task/years/days/start_date/end_date） |
| `/admin/etl/sentiment` | POST | 情绪分析 ETL |
| `/admin/etl/train_kline_patterns` | POST | 训练 K 线形态 |
| `/admin/tasks/status` | GET | 任务队列状态 |
| `/admin/data/dashboard` | GET | 数据管理仪表盘（表统计、日期范围） |
| `/admin/data/day_status` | GET | 指定日期各表数据状态 |
| `/admin/data/sync_date` | POST | 触发指定日期的全量数据刷新 |
| `/admin/data_verify` | GET | 数据验证（API vs DB 对比） |
| `/admin/integrity` | GET | 数据完整性检查（含 summaries 汇总） |
| `/admin/db/query` | POST | 执行只读 SQL 查询 |
| `/admin/system/trigger_daily_sync` | GET | 触发每日数据同步 |
| `/admin/system/backfill_history` | GET | 回填历史数据 |

### 核心模块

| 模块 | 说明 |
|------|------|
| `etl/sync.py` | 同步引擎 — 统一调度所有 ETL 任务 |
| `etl/scheduler.py` | APScheduler 定时任务调度器 |
| `etl/calendar.py` | 交易日历 |
| `etl/tasks/` | 各数据同步任务 |
| `etl/providers/` | 数据源适配（Tushare Pro） |
| `etl/utils/` | 工具（kline_patterns, factors, scoring, backfill, quality） |
| `db/connection.py` | DuckDB 共享连接（单例） |
| `db/schema.py` | 数据库表 DDL |

### ETL 任务类型

`basic` / `concepts` / `calendar` / `price`(daily) / `index` / `moneyflow` / `daily_basic` / `index_members` / `express` / `financials` / `fina_indicator` / `quarterly_income` / `margin` / `factors`

### 任务队列

- 任务持久化到 `etl_tasks` 表
- 状态流转：PENDING → RUNNING → COMPLETED/FAILED
- 僵尸任务恢复：10分钟无心跳的 RUNNING 改回 PENDING
- 重跑保护：同一 KLINE_TRAIN 全局只允许一个排队/运行

### 数据库表（核心数据表）

- `stock_basic` — 股票基本信息
- `daily_price` — 日线行情
- `stock_daily_basic` — 日频基础指标
- `stock_moneyflow` — 资金流向
- `market_index` — 市场指数
- `stock_margin` — 融资融券
- `stock_concepts` / `stock_concept_details` — 概念分类
- `stock_index_member_all` — 申万行业归属
- `stock_express` — 业绩快报
- `stock_fina_indicator` / `stock_income` — 财务数据
- `stock_factor_daily` — 因子宽表
- `etl_tasks` — 任务队列

## 约束与注意事项

- DuckDB 单进程模型，**禁止**直接打开 DuckDB 文件
- Tushare API 有限流，使用 tenacity 重试
- 概念同步采用 staging + 原子发布（`__staging` 后缀表）
- 完整性检查统一使用 4000 条阈值判定交易日数据完整性
- `/admin/etl/sentiment?sync_index=true` 复用 `sync_core_market_indices`
- `/admin/system/trigger_daily_sync` 复用 `perform_daily_data_update`

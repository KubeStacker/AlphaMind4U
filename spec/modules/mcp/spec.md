# MCP 数据库读取服务

## 目标

项目收敛为“原始数据采集 + MCP 数据库读取服务”。删除前端、推测/评分/建议逻辑和不再被 MCP 使用的业务能力，同时保留现有 DuckDB 数据，不执行历史表或数据文件的破坏性清理。

## 保留范围

### 数据采集

- 保留 Tushare Provider、ETL 任务、任务队列、定时调度、数据完整性与只读 SQL 查询。
- 保留 DuckDB 单例连接、采集数据表 DDL、数据库初始化和 ETL 所需公共配置。
- 仅保留由 Tushare 等 Provider 获取并原样或仅做字段/日期/单位规范化后落库的数据。
- 保留 ETL 运行状态数据，但不将其作为行情推测结果暴露。

### MCP

MCP 不再按具体业务场景提供分析工具，只保留以下 3 个通用、只读数据库工具：

| MCP 工具 | 说明 |
|----------|------|
| `jarvis_list_datasets` | 返回允许读取的采集数据表及说明 |
| `jarvis_describe_dataset` | 返回指定采集表的字段、类型和字段说明 |
| `jarvis_query_dataset` | 对单个采集表执行字段选择、精确条件、代码、日期范围、排序和分页查询 |

MCP 继续通过静态 Bearer Token 调用后端；后端不再提供浏览器登录流程。MCP 进程不得直接打开 DuckDB 文件，必须通过 backend 的共享连接访问，以满足 DuckDB 单进程约束。

允许读取的采集数据表为：

- `stock_basic`
- `daily_price`
- `stock_concepts`
- `stock_concept_details`
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

`jarvis_query_dataset` 不接受任意 SQL，不允许跨表 JOIN、表达式、聚合、写操作或未列入白名单的表；默认限制返回行数，并设置最大上限。

## 删除范围

### MCP 工具

- 删除现有全部 13 个面向业务场景的 MCP 工具，由 3 个通用数据库读取工具取代。
- 删除技术指标、日频探针、市场建议、市场概览、实时行情、持仓、主线和策略广场等包含计算、拼接或推测的输出。

### 应用逻辑

- 删除整个 Vue 前端及其 Docker 服务。
- 删除 Watchlist、自选、持仓、实时盯盘、点位推荐及图片导入逻辑。
- 删除主线分析和策略广场逻辑。
- 删除用户管理、浏览器登录、AI 配置、提示词和 AI 个股分析逻辑。
- 删除市场情绪、市场状态、技术指标、因子评分、K 线形态、回测及推荐逻辑。
- 删除 `stock_factor_daily`、`market_sentiment`、`market_overview_snapshots` 等衍生数据的生产任务和初始化 DDL。
- 删除非数据采集、非保留 MCP 依赖的 API、脚本和测试。
- 从数据库初始化代码中移除已删除业务的表 DDL；不主动 DROP 已存在的表。

## 后端目标结构

- `main.py` 只注册 ETL、受控数据集读取和健康检查。
- 新建或保留一个精简的数据集路由，集中实现表白名单、schema 描述和受限行查询。
- `etl/`、`db/` 保持原始采集链路完整；删除因子、情绪、策略广场、盯盘训练和其他衍生计算分支。
- `docker-compose.yml` 只运行 backend；`docker-compose.mcp.yml` 继续运行 MCP Server。

## 验收

- 后端与 MCP 模块可导入，Python 编译检查通过。
- MCP 工具列表严格为上述 3 项，查询结果直接来自白名单数据表。
- 三个 MCP 工具对应的后端路由仍存在，越权表名、字段名、limit 和写入尝试均被拒绝。
- ETL 同步与调度仍可用；删除面向 SQL 控制台的 `/admin/db/query`，MCP 和后端均不暴露任意 SQL。
- 源码中不再注册 Watchlist、持仓、主线、策略广场、AI 和用户管理路由。
- 源码中不再生成市场建议、情绪、技术指标、因子分、推荐、回测等推测性结果。
- 不改写或删除现有 DuckDB 数据文件。

## 约束

- 遵守 DuckDB 单进程共享连接规则，DataFrame 临时注册后必须注销。
- 保留 Tushare 重试和 2000 积分限制下可用的采集接口。
- 实施不改写或删除现有 DuckDB 数据文件。

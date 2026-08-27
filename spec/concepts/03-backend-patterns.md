# 后端模式

## 路由边界

`backend/main.py` 只注册：

- `api.routes.etl`：原始数据采集队列。
- `api.routes.datasets`：MCP 使用的受控单表读取。

所有 `/admin` 路由统一使用静态 Bearer Token。没有浏览器登录、用户管理、
业务分析或任意 SQL 路由。

## 数据集查询

- 表名必须存在于静态白名单。
- 字段名必须来自运行时 schema，并排除历史衍生字段。
- 值条件全部参数化。
- 单次返回默认 100 行、最多 1000 行。
- 仅支持单表字段选择、精确匹配、日期范围、排序和分页。

## ETL

- 每项采集任务继承 `BaseTask` 并复用 Provider。
- `/admin/etl/sync` 只接受已声明的原始采集任务类型。
- APScheduler 仅调度采集，不调度任何计算或策略任务。
- 批量写入使用 `update_then_insert_missing` 等无 ART 冲突模式。

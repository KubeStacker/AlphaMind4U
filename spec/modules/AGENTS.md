# 模块概览

本项目按前端 tab 划分为 5 个模块，每个模块一个目录，目录下 `spec.md` 包含前后端完整视图。

| 模块 | 目录 | 前端 Tab | 路由 |
|------|------|----------|------|
| 盯盘 | [watchlist/](watchlist/spec.md) | Tab1 | `/` |
| 仪表盘 | [dashboard/](dashboard/spec.md) | Tab2 | `/dashboard` |
| 策略广场 | [strategy/](strategy/spec.md) | Tab3 | `/strategies` |
| 数据同步 | [data-sync/](data-sync/spec.md) | Settings | 数据管理 |
| 基础设置 | [basic/](basic/spec.md) | Settings | 用户/AI/K线 |

## 模块间关系

```
basic (用户/认证/AI配置)
  └── watchlist (依赖 basic 的认证 + AI分析)
  └── data-sync (依赖 basic 的认证)
  └── dashboard (依赖 data-sync 的数据)
  └── strategy (依赖 data-sync 的数据)

dashboard (情绪+主线)
  └── watchlist (dashboard 确定方向，watchlist 盯具体标的)
  └── strategy (dashboard 确定方向，strategy 出具体形态)

strategy (策略广场)
  └── watchlist (复用 K线图组件和弹窗链路)
```

## 后端模块路径映射

| 模块 | 主要后端路径 |
|------|-------------|
| watchlist | `api/routes/stocks.py`, `strategy/watchlist/`, `strategy/sentiment/live_monitor.py` |
| dashboard | `api/routes/market.py`, `strategy/sentiment/`, `strategy/mainline/` |
| strategy | `api/routes/strategy_plaza.py`, `strategy/plaza/` |
| data-sync | `api/routes/etl.py`, `api/routes/db.py`, `etl/` |
| basic | `api/routes/users.py`, `api/routes/ai.py`, `api/routes/docs.py`, `api/routes/system.py`, `api/auth.py` |

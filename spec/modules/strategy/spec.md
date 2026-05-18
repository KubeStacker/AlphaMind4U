# 策略广场 (Strategy Plaza)

## 概述

路由 `/strategies`，与 Watchlist 并列的一级入口。只展示"某策略某日期新进入观察的标的 + 3/5/10 日结果摘要 + 滚动总结"，不展示中间阶段池。

当前内置三个本地指标策略，不依赖 AI 提示词。

## 前端

### 页面/组件

| 文件 | 说明 |
|------|------|
| `frontend/src/views/Strategies.vue` | 主页面 — 策略选择器、日期选择器、观察表、摘要卡、K线弹窗 |
| `frontend/src/composables/useStrategyPlaza.js` | 策略广场数据 helpers |
| `frontend/src/composables/useKlineChart.js` | K线图（共享，复用 Watchlist 同一链路） |

### 核心交互流程

1. 选择策略 → 选择观察日期 → 查看新进入观察标的列表
2. 每只标的附带 3/5/10 日回测结果简表
3. 底部展示滚动统计摘要文本
4. 点击标的名称 → 打开 K 线弹窗（复用 `/admin/stock/{ts_code}/kline`）

## 后端

### API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/admin/strategy-plaza/strategies` | GET | 策略列表（display_order 排序） |
| `/admin/strategy-plaza/observations` | GET | 指定策略/日期的新进入观察标的 + 3/5/10日结果 |
| `/admin/strategy-plaza/summary` | GET | 滚动统计摘要；无新标的时返回 null |
| `/admin/strategy-plaza/run` | POST | 手动触发策略广场任务（支持历史日期和单策略重跑） |

### 核心模块

| 模块 | 说明 |
|------|------|
| `strategy/plaza/registry.py` | 策略注册中心 — 扫描 `builtin/` 下的策略类 |
| `strategy/plaza/base.py` | 策略基类 — 定义触发、过滤、入观察、回测锚点接口 |
| `strategy/plaza/service.py` | 策略服务 — 调度运行、写入观察、回测计算 |
| `strategy/plaza/summarizer.py` | 滚动统计与总结文本生成 |
| `strategy/plaza/builtin/head7_dragon_return.py` | 头7龙回头 |
| `strategy/plaza/builtin/single_yang_hold.py` | 单阳不破 |
| `strategy/plaza/builtin/golden_eye.py` | 大眼睛/空中加油 |

### 内置策略说明

**头7龙回头**：最后一个封死涨停板或放量启动阳线触发 → 定位该段最高点 → 最高点后 5-8 个交易日缩量回调找二波启动点。回撤保护锚定触发K。主板看半实体回撤+MA10，创业板/科创板允许回到起涨点附近+MA20。最新观察日量能需压缩到触发段 1/3 以内。

**单阳不破**：单根强势阳线后，缩量整理不破起涨点。

**大眼睛/空中加油**：放量长上影后，缩量确认支撑。

板块阈值：`301/300/688` 统一按高弹性板块处理。

### 数据库表

- `strategy_definitions` — 策略定义与启用状态
- `strategy_observations` — 每日新进入观察标的归档
- `strategy_backtest_runs` — 3/5/10 日表现与回撤结果
- `strategy_daily_summaries` — 滚动统计与总结文本

## 约束与注意事项

- 策略本体由后端代码决定，`builtin/` 下可自行定义多阶段触发、过滤、延迟入观察
- 回测从策略声明的 `entry_anchor_date + entry_price_source` 计算
- 结果表点击标的复用 Watchlist K线弹窗，不额外维护独立图表逻辑
- 若当日无新进入观察标的，summary 返回 `null`，前端不应展示历史总结

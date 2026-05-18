# 盯盘 (Watchlist)

## 概述

用户登录后的首页（路由 `/`），提供持仓跟踪和自选盯盘功能。核心目标：**对已持仓和关注标的做实时监控与交易决策辅助**。

页面拆分为"持仓跟踪"和"观察列表"两段。

## 前端

### 页面/组件

| 文件 | 说明 |
|------|------|
| `frontend/src/views/Watchlist.vue` | 主页面，含搜索栏、筛选chip、持仓区、观察区、隐私模式、专注模式、图片导入 |
| `frontend/src/composables/watchlistHoldings.js` | 持仓数据管理（CRUD、批量更新、盈亏计算） |
| `frontend/src/composables/watchlistZen.js` | 专注模式（精简视图、双列点位块） |
| `frontend/src/composables/useStockSearch.js` | 股票搜索（拼音首字母匹配） |
| `frontend/src/composables/useKlineChart.js` | ECharts K线图（共享组件） |

### 核心交互流程

1. **页面加载**：优先恢复 `sessionStorage + localStorage` 快照 → 后台并行拉取自选和持仓
2. **搜索添加**：搜索候选层（近不透明实底浮层）→ 选中 → 本地先插入卡片 → 单票补拉 compact 数据
3. **删除**：本地先移除 → 后台调用 DELETE
4. **自动排序**：观察列表按 `recommendation_score + state_bucket` 自动排序，不支持拖拽
5. **折叠优化**：折叠时自动刷新仅更新持仓，观察价格手动展开后按需刷新
6. **隐私模式**：遮罩持仓股数、成本、盈亏、总市值（localStorage 持久化）
7. **专注模式**：全屏持仓视图只保留代码、现价、动作词、主支撑/主压力/触发/失效位
8. **持仓图片导入**：识别 → 预览（不自动写入）→ 确认 → 调用 batch 写入 → 本地更新 + 后台补拉

### 状态/数据流

- **Pinia store**: `auth.js` — 用户认证状态
- **本地缓存**: `sessionStorage + localStorage` — 首屏快照持久化
- **API 调用**: 高频轮询走 `/admin/watchlist/realtime?analysis_depth=compact&sort_mode=auto`

## 后端

### API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/admin/watchlist` | GET | 当前用户自选列表 |
| `/admin/watchlist` | POST | 添加自选（仅允许 stock_basic 中存在的代码） |
| `/admin/watchlist/{ts_code}` | DELETE | 删除自选 |
| `/admin/watchlist/realtime` | GET | 实时盯盘数据（compact/full，auto/manual 排序） |
| `/admin/watchlist/{ts_code}/analysis` | GET | 单只股票深度分析（详情弹窗按需加载） |
| `/admin/watchlist/levels/backtest` | GET | 点位回测诊断（adaptive vs legacy） |
| `/admin/stock/search` | GET | 股票搜索（拼音首字母） |
| `/admin/stock/{ts_code}/kline` | GET | K线数据（日线未入库时补当日临时bar） |
| `/admin/stock/{ts_code}/mainline_analysis` | GET | 个股主线归属分析 |
| `/admin/stock/{ts_code}/indicators` | GET | 个股技术指标 |
| `/admin/users/me/holdings` | GET | 持仓数据 |
| `/admin/users/me/holdings/batch` | POST | 批量更新持仓（可选同步自选） |
| `/admin/users/me/holdings/parse-image` | POST | 持仓截图识别 |
| `/admin/users/me/holdings/{ts_code}` | PUT | 更新单条持仓 |
| `/admin/users/me/holdings/{ts_code}` | DELETE | 删除单条持仓 |

### 核心模块

| 模块 | 说明 |
|------|------|
| `strategy/watchlist/recommendation.py` | 自选推荐引擎（`build_watch_recommendation`, `sort_watch_candidates`） |
| `etl/utils/kline_patterns.py` | K线形态识别 + 专业点评（`get_professional_commentary_detailed`） |
| `strategy/sentiment/live_monitor.py` | 实时监控数据 |

### 点位引擎

支撑/压力位采用 **rolling window 多源候选 + ATR/振幅归一化去重 + 结构触线共振** 算法：
- 多源候选：MA5/10/20/60、近5/10/20/60日高低点、swing high/low、趋势线、未补缺口、近60日成交密集区
- 结构压力："三触顶" — 至少1次阳线收盘锚定 + 2次上影摸线
- 结构支撑："四承接" — 至少4次下探 + 阴线收盘/阳线开盘锚点 + 2次下影试探 + 放量小阳确认
- 主板/创业板/科创板三套波动画像自适应校准

### 数据库表

- `watchlist` — 按 `user_id + ts_code` 复合主键
- `user_holdings` — 用户持仓
- `daily_price` — 日线行情
- `stock_basic` — 股票基本信息（含 pinyin/pinyin_abbr）

## 约束与注意事项

- 自选接口按 `user_id` 隔离，需要 `Authorization: Bearer <token>`
- 颜色语义：红色=买入，绿色=卖出，白色=观望
- K线形态仅作辅助参考，主力信号来自趋势/量价/资金/因子分
- 日线数据单位：`vol`=手，`amount`=千元
- 新增自选仅允许 `stock_basic` 存在的代码，无效代码以空占位返回

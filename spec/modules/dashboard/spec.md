# 仪表盘 (Dashboard)

## 概述

路由 `/dashboard`，提供每日市场全景复盘。两段式布局：

1. **情绪驾驶舱**（顶部）— 回答"今天能不能打、打多大"
2. **主线作战板**（底部）— 回答"先看什么方向"

## 前端

### 页面/组件

| 文件 | 说明 |
|------|------|
| `frontend/src/views/Dashboard.vue` | 容器页面，组合两个面板 |
| `frontend/src/components/dashboard/SentimentPanel.vue` | 情绪仪表盘 — 环形仪表、市场状态、历史弹窗 |
| `frontend/src/components/dashboard/MainlinePanel.vue` | 主线作战板 — 主线卡片、龙头列表（点击懒加载） |

### 核心交互流程

1. **页面加载**：并行拉取市场情绪 + 主线数据
2. **自动刷新**：交易时段按 `dashboard_refresh_seconds` 自动轮询，用上证+创业板实时快照修正盘中节奏
3. **情绪区**：头部直接输出结论，不拆单独标题。下方固定四格：市场节奏、建议仓位、进攻方式、风控底线
4. **预测情绪**：按钮触发弹窗参考，不在首页固定占位
5. **主线区**：最多展示 3 条主线。顶部只保留轮动正文。龙头在左侧方向卡片内按点击懒加载展开

### 视觉规范

- 低饱和冷暖分区：情绪区偏暖色、主线区偏冷色
- **禁止**紫色/靛蓝色调
- 按钮触发内容保持固定容器，避免异步结果撑乱布局

## 后端

### API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/admin/market_sentiment` | GET | 市场情绪历史 + 自动补算最新收盘情绪；交易时段叠加实时修正 |
| `/admin/sentiment/preview` | GET | 盘中情绪预估（线性估算，建议 14:50） |
| `/admin/market/suggestion` | GET | 统一市场建议（EOD/盘中） |
| `/admin/mainline_history` | GET | 主线演变历史（近10日复盘、当前最强/连续主线、Top5 个股摘要） |
| `/admin/mainline/leaders` | GET | 主线及龙头（优先近10日持续性方向，剔除北交所） |
| `/admin/mainline/preview` | GET | 盘中主线预估 |
| `/admin/portfolio/recommendation` | GET | 组合建议（regime + theme + stock rank + position sizing） |
| `/admin/factor/diagnostics` | GET | 因子诊断（IC/RankIC/分层收益，支持行业中性开关） |

### 核心模块

| 模块 | 说明 |
|------|------|
| `strategy/sentiment/analyst.py` | 市场情绪分析核心 — 基于全市场涨跌分布、连板高度、跌停家数 |
| `strategy/sentiment/dashboard.py` | 情绪 API payload 构建（含 10Y/Pizza 宏观风险旁证） |
| `strategy/sentiment/config.py` | 情绪策略配置、回测参数 |
| `strategy/sentiment/live_monitor.py` | 实时监控 |
| `strategy/mainline/analyst.py` | 主线板块分析 — 概念共振、宽主题归并、行业锚点 |
| `strategy/mainline/config.py` | 主线策略配置 |

### 三层策略递进

1. **情绪分析** — 自适应情绪导数模型，输出 0-100 分 + 交易预案 (BUY/SELL/HOLD/WATCH)
2. **主线分析** — 概念共振算法，过细概念归并为宽主题，计算赚钱效应评分
3. **组合建议** — 情绪仓位 × 主线方向 × 横截面因子 × 相关性约束

### 数据库表

- `market_sentiment` — 市场情绪历史
- `mainline_scores` — 主线评分历史
- `market_index` — 市场指数数据
- `stock_factor_daily` — 因子宽表

## 约束与注意事项

- 收盘后优先补算最新交易日情绪并返回收盘基线
- 10Y 优先取 Tushare `us_tycr`，失败回退海外源；Pizza 解析 live 区段 DOUGHCON/spike
- 容器无法访问海外站点时宏观字段返回 unavailable，前端展示降级态
- 主线龙头评分剔除北交所标的，不按单日涨幅排队
- 通信链拆分为 `光通信 / 通信网络 / 算力基建`
- 主线太弱（score < 7）时降仓至 WATCH

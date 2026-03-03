# Sentiment 与 Mainline 逻辑总结（含接口、质检、结果检查）

## 1. 整体逻辑

### 1.1 Sentiment（市场情绪）

Sentiment 的目标是把市场状态量化成可执行建议：

- 输入：全市场日线、指数、资金流、融资融券等
- 输出：
  - 情绪分数（`0-100`）
  - 信号（`PLAN_BUY_* / PLAN_SELL_* / PLAN_HOLD / PLAN_WATCH`）
  - 执行建议（新增）：`action / target_position / tranche_count / stop_loss_pct / take_profit_pct / confidence`

核心特征（部分）：

- 涨停/跌停强度、炸板率、修复率
- 市场广度（上涨家数占比）
- 指数共振（`index_pct_chg`）
- 成交活跃度（相对历史）
- 资金流向（`net_mf_ratio`）
- 融资变化（`margin_financing_delta5`）
- 新高新低结构
- 波动率代理（`iv_proxy_z`）

风控增强（为了利润与回撤平衡）：

- 高波动 + 弱广度 + 非牛市时，倾向 `WATCH` 或降仓
- `BUY` 信号下动态降仓
- 输出明确止损止盈参数，方便执行

### 1.2 Mainline（市场主线）

Mainline 的目标是识别“当下最有资金共识”的方向：

- 将细分概念映射到大主线（如人工智能、半导体、新能源等）
- 依据涨幅、涨停、广度、强势股比例、成交额、资金流评分
- 输出主线排名与龙头股列表

新增盘中预估：

- 使用最近交易日主线归属和龙头池
- 用实时行情替换涨跌幅（`realtime_quote`）
- 给出盘中主线强弱排序（14:50 可用）

## 2. 使用到的 Tushare 接口

核心接口如下：

- `daily`：股票日线
- `index_daily`：指数日线
- `moneyflow`：个股资金流
- `margin` / `margin_detail`：融资融券
- `trade_cal`：交易日历
- `stock_basic`：股票基础信息
- `concept` / `concept_detail`：概念与成分
- `realtime_quote`：盘中实时行情（用于 preview）

## 3. 如何检查数据质量

### 3.1 质量报告

- `GET /admin/integrity?start_date=2025-01-01&end_date=2026-12-31`

### 3.2 任务运行状态

- `GET /admin/tasks/status`

### 3.3 关键表最新交易日对齐检查

推荐用：`POST /admin/db/query`

```bash
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT '\''daily_price'\'' t, MAX(trade_date) d FROM daily_price\nUNION ALL SELECT '\''market_index'\'', MAX(trade_date) FROM market_index\nUNION ALL SELECT '\''stock_moneyflow'\'', MAX(trade_date) FROM stock_moneyflow\nUNION ALL SELECT '\''market_sentiment'\'', MAX(trade_date) FROM market_sentiment;"}'
```

## 4. 如何检查建议结果（Suggestion）

### 4.1 EOD 正式建议

- `GET /admin/market/suggestion?use_preview=false`

### 4.2 盘中预估建议（建议 14:50）

- `GET /admin/market/suggestion?use_preview=true&src=dc`

重点字段：

- `action`：BUY/SELL/WATCH
- `target_position`：目标仓位
- `risk_controls.stop_loss_pct`、`take_profit_pct`、`tranche_count`
- `rationale.sentiment_score`
- `rationale.top_mainline`、`top_mainline_score`

## 5. 已提供接口清单

### 5.1 情绪相关

- `GET /admin/market_sentiment`：情绪历史
- `POST /admin/etl/sentiment`：重算情绪（可选先同步指数）
- `GET /admin/sentiment/preview`：盘中预估次日情绪（不落库）

### 5.2 主线相关

- `GET /admin/mainline_history`：主线历史
- `GET /admin/mainline/preview`：盘中主线预估（不落库）

### 5.3 统一建议

- `GET /admin/market/suggestion`
  - `use_preview=false`：EOD 模式
  - `use_preview=true`：盘中预估模式

### 5.4 同步与自动刷新

- `POST /admin/etl/sync`
  - 新增参数：
    - `refresh_sentiment`（是否同步后自动重算情绪）
    - `sentiment_days`（重算窗口）

示例：

```bash
curl -X POST "http://localhost:8000/admin/etl/sync" \
  -H "Content-Type: application/json" \
  -d '{"task":"daily","years":1,"refresh_sentiment":true,"sentiment_days":30}'
```

## 6. 推荐日常使用流程（实盘）

1. 14:50 调用 `GET /admin/market/suggestion?use_preview=true` 获取盘中建议。
2. 收盘后执行 `POST /admin/etl/sync`，并开启 `refresh_sentiment=true`。
3. 再调用 `GET /admin/market/suggestion?use_preview=false` 查看正式 EOD 建议。
4. 对比盘中预估与 EOD 偏差，持续优化阈值与权重。

## 7. 注意事项

- 盘中预估本质是“近实时代理模型”，不是最终收盘结论。
- 预估依赖 `realtime_quote` 的可用性与权限。
- 策略优化目标是“提高盈亏比 + 降低回撤”，无法承诺单日或单阶段绝对盈利。

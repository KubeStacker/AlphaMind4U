# 情绪策略与回测操作文档

## 目标

- 用统一接口重算情绪与回测结果
- 使用参数优化器在历史样本上寻找更高回测收益组合
- 明确约束：历史回测高收益不代表未来收益，无法对未来做收益保证

## 关键接口

### 1) 重算情绪（推荐）

```bash
curl -X POST "http://localhost:8000/admin/etl/sentiment?days=365&sync_index=false"
```

说明：
- `sync_index=false`：仅重算情绪（快）
- `sync_index=true`：先同步指数再重算（慢）

### 2) 查看任务状态

```bash
curl "http://localhost:8000/admin/tasks/status"
```

### 3) 查看最近情绪结果

```bash
curl "http://localhost:8000/admin/market_sentiment?days=30"
```

## 回测优化逻辑

策略在回测时会自动网格搜索以下参数：

- `leverage`
- `trend_floor_pos`（趋势底仓）
- `fee_rate`

配置位置：

- `backend/strategy/sentiment/config.py` -> `SENTIMENT_CONFIG["backtest"]["optimizer"]`

默认目标：

- `target_total_return = 1.0`（即 100%）

注意：

- 该目标仅用于历史样本参数搜索，不等价于未来收益保证。

## 结果产出

回测报告路径：

- `backend/strategy/sentiment/backtest_report.md`

报告包含：

- 总收益、年化、回撤、夏普、基准收益
- 杠杆与趋势底仓参数
- 最优参数组合（JSON）

## 调参与建议

若收益不足 100%，可按顺序调整：

1. 提高 `leverage_grid` 上限（如 3.0）
2. 提高 `trend_floor_grid` 下限（如从 0.2 起）
3. 放宽买入阈值（`buy.breakout_score`、`buy.normal_buy_min_score`）
4. 放宽趋势买入过滤（`momentum.buy_min_breadth`）

若回撤过大，可按顺序收紧：

1. 降低 `leverage_grid` 上限
2. 收紧 `sell.stop_loss` 与 `sell.trailing_pullback`
3. 降低 `trend_floor_pos`

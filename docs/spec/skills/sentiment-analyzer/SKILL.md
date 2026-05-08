# 市场情绪分析技能

## 描述

分析A股市场情绪，提供情绪历史、自动补算、实时叠加等功能。

## 使用场景

- 获取市场情绪历史数据
- 交易时段实时叠加情绪
- 提供10Y/Pizza风险旁证

## 调用方式

**API端点：** `/admin/market_sentiment`

```bash
# 获取情绪历史
curl "http://localhost:8000/admin/market_sentiment?days=30"

# 预览情绪
curl "http://localhost:8000/admin/sentiment/preview"
```

## 核心功能

**情绪历史：** 返回历史情绪数据，自动补算最新收盘情绪

**实时叠加：** 交易时段按上证/创业板实时叠加

**自动刷新：** 返回刷新提示和自动刷新间隔

**风险旁证：** 10Y收益率、Pizza指数（DOUGHCON/spike）

## 注意事项

- 宏观字段若容器无法访问海外站点，会返回 unavailable
- 前端应展示降级态而不是假定有值
- 交易时段只用上证+创业板实时快照修正盘中节奏
- 收盘后优先补算最新交易日情绪

## 参考

- `backend/strategy/sentiment/` - 情绪分析模块
- `backend/api/routes/market.py` - API路由

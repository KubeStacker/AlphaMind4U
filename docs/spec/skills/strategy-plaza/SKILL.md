# 策略广场技能

## 描述

管理策略广场的回测服务、归档与观察标的，内置三个本地指标策略。

## 使用场景

- 查看策略广场策略列表
- 获取指定策略的观察标的
- 手动触发策略广场任务

## 调用方式

**API端点：**

```bash
# 策略列表
curl "http://localhost:8000/admin/strategy-plaza/strategies"

# 观察标的
curl "http://localhost:8000/admin/strategy-plaza/observations?strategy_id=1&date=2025-01-01"

# 滚动摘要
curl "http://localhost:8000/admin/strategy-plaza/summary?strategy_id=1&date=2025-01-01"

# 手动触发
curl -X POST "http://localhost:8000/admin/strategy-plaza/run" \
  -H "Content-Type: application/json" \
  -d '{"strategy_id": 1, "date": "2025-01-01"}'
```

## 内置策略

**头7龙回头：** 最后一个封死涨停板或放量启动阳线触发后，5-8个交易日缩量回调找二波启动点

**单阳不破：** 单根强势阳线后，缩量整理不破起涨点

**大眼睛/空中加油：** 放量长上影后，缩量确认支撑

## 板块阈值

- 主板与创业板/科创板区分
- `301/300/688` 统一按高弹性板块处理

## 注意事项

- 策略本体由 `backend/strategy/plaza/builtin/` 下的后端代码决定
- 结果表点击标的名称，复用 Watchlist 同链路的K线弹窗
- 不依赖AI提示词

## 参考

- `backend/strategy/plaza/` - 策略广场模块
- `backend/strategy/plaza/builtin/` - 内置策略

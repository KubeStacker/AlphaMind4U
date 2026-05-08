# K线形态识别技能

## 描述

识别股票K线图中的各种技术形态，提供向量化检测、置信度评分和历史回测功能。

## 使用场景

- 分析个股的K线形态信号
- 生成专业的技术分析点评
- 回测形态识别的历史表现

## 调用方式

**模块路径：** `backend/etl/utils/kline_patterns.py`

**关键函数：**

```python
from backend.etl.utils.kline_patterns import detect_all_patterns, get_latest_signals

# 检测所有形态
patterns_df = detect_all_patterns(df)

# 获取最新信号
latest = get_latest_signals(df)
```

## 支持的形态

**看涨形态：** 锤子线、吞没形态、刺透形态、启明星、三白兵、上升三法、仙人指路、老鸭头、蓄势

**看跌形态：** 上吊线、流星线、看跌吞没、乌云盖顶、黄昏星、三黑鸦、下降三法、背离

**中性形态：** 十字星、极度缩量

## 生成专业点评

```python
from backend.etl.utils.kline_patterns import get_professional_commentary_detailed

commentary = get_professional_commentary_detailed(df, patterns, context=None)
# 返回：decision, trade_plan, key_levels, level_methodology, classification, observation_points
```

## 注意事项

- 数据单位：vol 为"手"，amount 为"千元"
- 前端展示"亿"应按 `amount / 1e5` 换算
- K线弹窗卡片和图例需显式标出单位

## 参考

- `backend/etl/utils/kline_patterns.py` - 核心实现
- `docs/spec/concepts/03-backend-patterns.md` - 后端模式

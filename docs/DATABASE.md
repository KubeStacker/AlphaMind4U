# 数据库表结构说明

## 核心行情表

### stock_basic - 股票基本信息
- **主键**: ts_code
- **字段**: symbol, name, area, industry, market, list_date, is_hs
- **记录数**: 5,484

### daily_price - 日线行情
- **主键**: (trade_date, ts_code)
- **字段**: open, high, low, close, vol, amount, adj_factor, factors (JSON)
- **记录数**: 1000万+

### market_index - 市场指数
- **主键**: (trade_date, ts_code)
- **字段**: 上证指数、深证成指等

---

## 财务数据表

### stock_fina_indicator - 财务指标 (推荐使用)
- **主键**: (ts_code, end_date)
- **核心字段**: 
  - eps (每股收益)
  - roe (净资产收益率)
  - gross_profit_margin (毛利率)
  - net_profit_margin (净利率)
  - total_rev (营业总收入)
  - profit (净利润)
- **记录数**: 12万+
- **说明**: 数据全，包含ROE、毛利率等核心指标，同步顺畅

### stock_income - 季度利润表
- **主键**: (ts_code, end_date, report_type)
- **核心字段**:
  - total_revenue (营业总收入)
  - revenue (营业收入)
  - operate_profit (营业利润)
  - total_profit (利润总额)
  - n_income (净利润)
  - basic_eps (基本每股收益)
- **记录数**: 同步中
- **说明**: 原始财务数据，因Tushare API限流，同步较慢

---

## 资金流向表

### stock_moneyflow - 个股资金流向
- **主键**: (ts_code, trade_date)
- **字段**: buy_sm_vol, sell_sm_vol, buy_md_vol, net_mf_vol, net_mf_amount 等
- **记录数**: 130万+

---

## 分析结果表

### strategy_recommendations - 策略推荐记录
- **主键**: (recommend_date, ts_code, strategy_name)
- **字段**: score, filters_used (JSON), p1_return, p3_return, p5_return, p10_return

### market_sentiment - 市场情绪
- **主键**: trade_date
- **字段**: score (0-100), label (Panic/Fear/Neutral/Greed/Extreme Greed), details (JSON)

### mainline_scores - 主线评分历史
- **主键**: (trade_date, mapped_name)
- **字段**: score, limit_ups, stock_count, top_stocks (JSON)

---

## 表关系图

```
stock_basic (主表)
    │
    ├── daily_price (1:N)
    │       └── factors (JSON) - 技术指标
    │
    ├── stock_income (1:N)
    │       └── 季度财务数据
    │
    ├── stock_fina_indicator (1:N)
    │       └── 财务比率指标
    │
    └── stock_moneyflow (1:N)
            └── 资金流向数据
```

# Jarvis-Quant 量化决策系统

A 股量化交易决策系统模块化架构。

## 快速启动

```bash
# 后端
cd backend
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 前端
cd frontend
npm install
npm run dev
```

## 系统架构

```
Jarvis-Quant
├── backend/          # FastAPI 后端
│   ├── api/         # API 接口
│   ├── db/          # 数据库连接与schema
│   ├── etl/         # 数据同步模块
│   └── strategy/    # 量化策略引擎
└── frontend/        # Vue 3 前端
```

## 核心模块

### 1. 数据同步 (ETL)
- 从 Tushare 采集行情、财务数据
- 支持日线、分钟线、资金流向、财务指标等

### 2. 策略引擎
- **市场情绪**: 择时系统，判定仓位水位
- **主线分析**: 识别最强热点板块
- **个股推荐**: 插件化选股策略

## 数据库表

| 表名 | 记录数 | 说明 |
|------|--------|------|
| stock_basic | 5,484 | 股票基本信息 |
| daily_price | 1000万+ | 日线行情 |
| stock_fina_indicator | 12万+ | 财务指标 |
| stock_income | 同步中 | 季度利润表 |
| stock_moneyflow | 130万+ | 资金流向 |

详见 `docs/DATABASE.md`

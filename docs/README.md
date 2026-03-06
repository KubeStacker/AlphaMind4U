# Jarvis-Quant 决策系统

FastAPI 后端与 Vue 3 前端构建。

## 快速启动

### 后端

```bash
cd backend
pip install -r requirements.txt

# 开发模式
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 生产模式（ETL/重算任务推荐单进程）
uvicorn main:app --host 0.0.0.0 --port 8000

# 或使用 Docker
docker-compose up backend
```

### 前端

```bash
cd frontend
npm install
npm run dev
```

### Docker Compose

```bash
# 启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f backend
docker-compose logs -f frontend
```

## 系统架构

```
Jarvis-Quant
├── backend/           # FastAPI 后端
│   ├── api/          # API 接口 (admin.py, auth.py)
│   ├── db/           # 数据库连接与 schema
│   ├── etl/          # 数据同步模块
│   │   ├── tasks/   # 数据任务
│   │   ├── providers/ # 数据源 (Tushare)
│   │   └── utils/   # 工具 (backfill, factors)
│   └── strategy/     # 量化策略引擎
│       ├── sentiment/ # 市场情绪分析
│       ├── mainline/  # 主线板块分析
│       └── recommend/ # 个股推荐引擎
└── frontend/         # Vue 3 前端
```

---

## 核心模块

### 1. 数据同步 (ETL)

从 Tushare 采集行情、财务数据：

- **日线/分钟线**: `daily`, `daily_hsgt`
- **财务指标**: `stock_fina_indicator` (推荐), `stock_income`
- **资金流向**: `stock_moneyflow`
- **概念板块**: `concept`, `concept_detail`
- **融资融券**: `margin`, `margin_detail`

### 2. 策略引擎 (三层递进架构)

#### 层级一：市场情绪分析

- **目标**: 解决「何时买」的问题
- **核心逻辑**: 基于全市场 5000+ 股票的涨跌分布、连板高度、跌停家数等指标，构建自适应情绪导数模型
- **输出**: 情绪分值 (0-100) 与交易预案 (BUY/SELL/HOLD/WATCH)
- **增强风控**: 高波动 + 弱广度时倾向 WATCH 或降仓，输出明确止损止盈参数

#### 层级二：主线板块分析

- **目标**: 解决「买什么方向」的问题
- **核心逻辑**: 通过概念共振算法，计算各板块的赚钱效应评分，解决「一票多概念」的归类偏向问题
- **输出**: 核心领涨主线排名与龙头股列表
- **盘中预估**: 使用 realtime_quote 给出盘中主线强弱排序 (14:50 可用)

#### 层级三：个股推荐引擎

- **目标**: 解决「具体买哪只」的问题
- **核心策略**:
  - **Alpha 动量增强**: 聚焦强势上升通道且具备业绩/动量支撑的标的
  - **回调反转策略**: 捕捉「牛回头」或「二波起涨」的机会
- **架构**: 插件化设计，可在 `backend/strategy/recommend/plugins/` 下新增策略

---

## 数据库表结构

### 核心行情表

| 表名 | 记录数 | 说明 |
|------|--------|------|
| stock_basic | 5,484 | 股票基本信息 |
| daily_price | 1000万+ | 日线行情 |
| market_index | - | 市场指数 (上证、深证等) |

### 财务数据表

| 表名 | 记录数 | 说明 |
|------|--------|------|
| stock_fina_indicator | 12万+ | 财务指标 (推荐使用) |
| stock_income | 同步中 | 季度利润表 |

### 资金流向表

| 表名 | 记录数 | 说明 |
|------|--------|------|
| stock_moneyflow | 130万+ | 个股资金流向 |

### 分析结果表

| 表名 | 说明 |
|------|------|
| strategy_recommendations | 策略推荐记录 |
| market_sentiment | 市场情绪 |
| mainline_scores | 主线评分历史 |

---

## API 接口

### 系统接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/` | GET | 健康检查 |
| `/system/status` | GET | 市场状态 (交易/休市) |
| `/system/db_check` | GET | 分析恐慌日病理 |
| `/system/trigger_daily_sync` | GET | 触发每日数据同步 |
| `/system/backfill_history` | GET | 回填历史数据 |

### 管理接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/admin/db/query` | POST | 执行 SELECT 查询 |
| `/admin/etl/sync` | POST | 统一 ETL 同步 |
| `/admin/etl/sentiment` | POST | 重算市场情绪 |
| `/admin/tasks/status` | GET | 任务队列状态 |
| `/admin/market_sentiment` | GET | 市场情绪历史 |
| `/admin/integrity` | GET | 数据完整性报告 |

### 策略接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/admin/market/suggestion` | GET | 统一建议 (EOD/盘中) |
| `/admin/mainline_history` | GET | 主线历史 |
| `/admin/mainline/preview` | GET | 盘中主线预估 |
| `/admin/sentiment/preview` | GET | 盘中情绪预估 |

---

## 常用操作

### 查询数据库

```bash
# 执行只读 SQL
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM stock_basic LIMIT 5"}'

# 检查各表最新交易日
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT '\''daily_price'\'' t, MAX(trade_date) d FROM daily_price UNION ALL SELECT '\''market_index'\'', MAX(trade_date) FROM market_index UNION ALL SELECT '\''stock_moneyflow'\'', MAX(trade_date) FROM stock_moneyflow UNION ALL SELECT '\''market_sentiment'\'', MAX(trade_date) FROM market_sentiment;"}'
```

### 数据同步与情绪重算

```bash
# 统一 ETL 同步
curl -X POST http://localhost:8000/admin/etl/sync \
  -H "Content-Type: application/json" \
  -d '{"task":"daily","years":1}'

# 同步后自动重算情绪
curl -X POST "http://localhost:8000/admin/etl/sync" \
  -H "Content-Type: application/json" \
  -d '{"task":"daily","years":1,"refresh_sentiment":true,"sentiment_days":30}'

# 仅重算情绪 (快)
curl -X POST "http://localhost:8000/admin/etl/sentiment?days=365&sync_index=false"

# 先同步指数再重算 (慢)
curl -X POST "http://localhost:8000/admin/etl/sentiment?days=365&sync_index=true"
```

### 获取建议

```bash
# EOD 正式建议 (收盘后)
curl "http://localhost:8000/admin/market/suggestion?use_preview=false"

# 盘中预估建议 (建议 14:50)
curl "http://localhost:8000/admin/market/suggestion?use_preview=true"
```

### 数据质量检查

```bash
# 完整性报告
curl "http://localhost:8000/admin/integrity?start_date=2025-01-01&end_date=2026-12-31"

# 任务状态
curl "http://localhost:8000/admin/tasks/status"

# 最近情绪
curl "http://localhost:8000/admin/market_sentiment?days=30"
```

---

## 推荐日常使用流程 (实盘)

1. **14:50** 调用 `GET /admin/market/suggestion?use_preview=true` 获取盘中建议
2. **收盘后** 执行 `POST /admin/etl/sync`，并开启 `refresh3. **次日_sentiment=true`
** 调用 `GET /admin/market/suggestion?use_preview=false` 查看正式 EOD 建议
4. 对比盘中预估与 EOD 偏差，持续优化阈值与权重

---

## 回测优化

策略支持网格搜索参数优化：

- `leverage` - 杠杆倍数
- `trend_floor_pos` - 趋势底仓
- `fee_rate` - 手续费率

配置位置: `backend/strategy/sentiment/config.py` -> `SENTIMENT_CONFIG["backtest"]["optimizer"]`

回测报告路径: `backend/strategy/sentiment/backtest_report.md`

**注意**: 历史回测高收益不代表未来收益，无法对未来做收益保证。

---

## 技术栈

- **后端**: FastAPI, uvicorn, pandas, duckdb, pydantic, tushare, apscheduler
- **前端**: Vue 3, Vite, Pinia, Vue Router, ECharts, Axios, TailwindCSS
- **数据库**: DuckDB (分析型，不适合高并发写入)

---

## 开发指南

### 新增选股插件

1. 在 `backend/strategy/recommend/plugins/` 下新建 Python 文件
2. 继承 `BaseStrategyPlugin` 类
3. 实现 `run()` 和 `recommend()` 方法
4. 在 `documentation` 属性 de 定义策略逻辑 (会自动同步至前端)

### 新增 ETL 任务

1. 在 `backend/etl/tasks/` 下创建任务类
2. 继承基类或遵循现有模式
3. 在 `sync.py` 中注册或通过 API 暴露

---

## 注意事项

- DuckDB 使用单进程嵌入式模型，请通过 API (`/admin/db/query`) 查询，避免直接打开新连接
- Tushare API 有限流，请使用 tenacity 实现重试逻辑
- 盘中预估依赖 `realtime_quote` 的可用性与权限
- 策略优化目标是「提高盈亏比 + 降低回撤」，无法承诺单日或单阶段绝对盈利

---

## 深度专业点评与高胜率演进建议

### 1. 核心架构评审 (Professional Review)

#### 技术优势
*   **高性能存储引擎**: 采用 **DuckDB** 极大地提升了百万级行情的向量化计算效率，相比传统关系型数据库在复杂因子回溯上具有数量级优势。
*   **插件化策略体系**: `FalconPlugin` 架构实现了逻辑解耦，支持策略的快速并行迭代与自动参数演进（Evolution）。
*   **全流程自动化**: 实现了从 Tushare 自动抓取到 DuckDB 存储，再到前端多维可视化的完整量化研究闭环。

#### 潜在风险
*   **动量陷阱**: 当前策略较多依赖单股动量突破，在 A 股频繁的风格切换或震荡市中，容易因“假突破”导致胜率波动。
*   **缺乏板块共振**: 过滤逻辑目前集中在个股层面，对“行业主线”和“板块强度”的深度量化仍有提升空间。

### 2. 高胜率演进建议 (High Win-Rate Roadmap)

为了显著提升系统的实战胜率与稳定性，建议从以下四个维度进行深度优化：

#### A. 引入市场环境“全局开关” (Global Market Filter)
*   **逻辑**: 不在赚钱效应差的环境中交易。
*   **建议**: 计算全市场“股价 > 20日均线”的股票占比。当占比 < 25% 时，强制进入空仓观望状态。

#### B. 从“单股突破”转向“板块共振” (Sector Convergence)
*   **逻辑**: A 股的强势股必须有板块支撑。
*   **实现**: 引入 **RPS (Relative Price Strength)** 排名。每日计算板块 RPS20/RPS250，优先选择所属板块 RPS 处于 Top 20% 的个股。

#### C. 强化“涨停基因”与“异动统计” (Limit-Up Quantification)
*   **建议**: 深度量化涨停板特征。
*   **指标**: 增加 `count_limit_up_30d`（近30天涨停数）及“首板时间”分析。在 A 股，涨停是最高层级的购买力确认。

#### D. 动态仓位管理与精细化风控 (Dynamic Sizing)
*   **建议**: 引入 **ATR 动态止损** 与凯利公式头寸管理。
*   **实现**: 根据策略近期胜率与盈亏比动态调整单笔投入比例，并利用 ATR 波动率调整止盈止损位，避免在震荡中被洗出。

### 3. 技术优化路径 (Technical Roadmap)

1.  **计算加速**: 逐步将 `Pandas` 因子逻辑迁移至 `Polars`，利用多线程并行提升 5-10 倍计算速度。
2.  **特征增强**: 集成 **北向资金** 每日增减持数据，作为中大盘股胜率修正的关键因子。
3.  **模型集成**: 尝试引入 **XGBoost/LightGBM** 对多因子进行非线性排序预测，取代简单的线性加权。

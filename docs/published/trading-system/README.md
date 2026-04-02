# Jarvis-Quant 决策系统

FastAPI 后端与 Vue 3 前端构建。

## 专题文档

- [经典 K 线识别引擎设计说明](./kline-pattern-recognition.md)
- [经典 K 线识别实施与测试路线](./kline-pattern-implementation-playbook.md)

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
- **日频基础指标**: `daily_basic`
- **行业归属**: `index_member_all`
- **业绩快报**: `express`
- **财务指标**: `stock_fina_indicator` (推荐), `stock_income`
- **资金流向**: `stock_moneyflow`
- **概念板块**: `concept`, `concept_detail`
- **融资融券**: `margin`, `margin_detail`

### 数据可靠性约束

- 概念同步采用 staging + 原子发布：先写入 `stock_concepts__staging` / `stock_concept_details__staging`，校验通过后再一次性覆盖正式表，避免主线/龙头接口读取半成品
- `/admin/etl/sentiment?sync_index=true` 统一复用 `sync_core_market_indices`，`/admin/system/trigger_daily_sync` 统一复用 `perform_daily_data_update`
- `stock_income` / `stock_fina_indicator` 已纳入数据库初始化 schema，新环境不再依赖手工补建
- `/admin/data/day_status` 与 `/admin/integrity` 对 `daily_price` / `stock_daily_basic` / `stock_moneyflow` / `stock_margin` 使用一致的 4000 条阈值判定交易日完整性
- 新增 `stock_daily_basic` / `stock_index_member_all` / `stock_express` / `stock_factor_daily` 四张因子层数据表；`daily_price.factors` 会回写换手率、量比、估值、资金和综合因子分，供主线龙头评分直接复用

### 2. 策略引擎 (三层递进架构)

#### 层级一：市场情绪分析

- **目标**: 解决「何时买」的问题
- **核心逻辑**: 基于全市场 5000+ 股票的涨跌分布、连板高度、跌停家数等指标，构建自适应情绪导数模型
- **输出**: 情绪分值 (0-100) 与交易预案 (BUY/SELL/HOLD/WATCH)
- **增强风控**: 高波动 + 弱广度时倾向 WATCH 或降仓，输出明确止损止盈参数

#### 层级二：主线板块分析

- **目标**: 解决「买什么方向」的问题
- **核心逻辑**: 通过概念共振算法，先将过细概念归并为宽主题，再计算各方向的赚钱效应评分；同时引入行业锚点与重复概念衰减，解决「一票多概念」和相近题材堆叠导致的归类偏向问题
- **细分示例**: 通信链会进一步拆成 `光通信` / `通信网络` / `算力基建`，避免把不同子方向压成同一主线
- **输出**: 核心领涨主线排名、近 10 个交易日主线复盘摘要、当前最强/10日连续主线，以及主线下 Top 5 龙头股列表
- **盘中预估**: 使用 realtime_quote 给出盘中主线强弱排序 (14:50 可用)

### Dashboard 首页

- **情绪驾驶舱**: 首页顶部只负责回答“今天能不能打、打多大”，头部直接输出结论，不再额外拆标题，也不重复展示日期/分区标签；头部样式与主线区保持同一套紧凑规格，下方把市场节奏、建议仓位、进攻方式、风控底线收进固定四格；“预测情绪”改为按钮触发弹窗参考，不再在首页常驻单独结果区
- **主线作战板**: 首页底部只负责回答“先看什么方向”，按“持续性 + 强度 + 共振”筛出最多 3 条主线；顶部只保留轮动正文，不再额外挂标题和提示文案，龙头样本改为在左侧方向卡片内按点击懒加载展开，减少右侧拥挤和首屏等待
- **加载稳定性**: Dashboard 内按钮触发的新内容应避免挤压首页主布局；情绪预测改为弹窗参考，主线趋势图和回测诊断保持固定容器，避免异步结果把卡片撑乱
- **视觉风格**: 保持与 Watchlist 一致的克制商务底色，并用低饱和暖色区分情绪、低饱和冷色区分主线，避免单调但不做花哨装饰
- **阅读顺序**: 设计目标是先用情绪板定节奏和风险敞口，再用主线板定方向优先级并按需展开龙头样本，减少跨区重复阅读

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
| `/admin/stock/analyze` | POST | 个股 AI 分析（摘要驱动、结论优先，并带入定位与关键点位定义） |
| `/admin/watchlist` | GET/POST | 当前登录用户的自选列表管理 |
| `/admin/watchlist/{ts_code}` | DELETE | 删除当前登录用户自选 |
| `/admin/watchlist/realtime` | GET | 当前登录用户的实时自选行情，支持 `analysis_depth=compact|full`，交易日优先展示当日快照 |
| `/admin/watchlist/{ts_code}/analysis` | GET | 当前登录用户自选内单只股票深度分析（返回 `classification` / `level_methodology` / `key_levels[*].note`） |

### 策略接口

| 接口 | 方法 | 说明 |
|------|------|------|
| `/admin/market/suggestion` | GET | 统一建议 (EOD/盘中) |
| `/admin/factor/diagnostics` | GET | 因子诊断（IC/RankIC/分层收益，支持行业中性开关） |
| `/admin/mainline_history` | GET | 主线历史（含近10日主线复盘、当前最强/连续主线与Top5个股摘要，并返回 `display_name` / `focus_tags` / `driver_summary` 等细分驱动字段） |
| `/admin/mainline/leaders` | GET | 主线及主线下龙头（支持前端按Top5展示，优先近10日持续性方向，剔除北交所，并按趋势先行/资金追踪/热度/题材命中综合评分，返回 `display_sector` / `focus_tags` / `driver_summary`） |
| `/admin/mainline/preview` | GET | 盘中主线预估 |
| `/admin/portfolio/recommendation` | GET | 组合建议（regime + theme + stock rank + position sizing） |
| `/admin/stock/{ts_code}/mainline_analysis` | GET | 个股主线归属分析（返回 `mapped_sector` / `is_mainline` 等） |
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

# 先同步核心指数再重算 (慢)
curl -X POST "http://localhost:8000/admin/etl/sentiment?days=365&sync_index=true"

# 补日频基础指标 / 申万行业归属 / 业绩快报 / 因子宽表
curl -X POST "http://localhost:8000/admin/etl/sync" \
  -H "Content-Type: application/json" \
  -d '{"task":"daily_basic","days":3}'
curl -X POST "http://localhost:8000/admin/etl/sync" \
  -H "Content-Type: application/json" \
  -d '{"task":"index_members"}'
curl -X POST "http://localhost:8000/admin/etl/sync" \
  -H "Content-Type: application/json" \
  -d '{"task":"express","days":120}'
curl -X POST "http://localhost:8000/admin/etl/sync" \
  -H "Content-Type: application/json" \
  -d '{"task":"factors","start_date":"2026-03-20","end_date":"2026-03-27"}'
```

### 获取建议

```bash
# EOD 正式建议 (收盘后)
curl "http://localhost:8000/admin/market/suggestion?use_preview=false"

# 盘中预估建议 (建议 14:50)
curl "http://localhost:8000/admin/market/suggestion?use_preview=true"

# 横截面因子诊断
curl "http://localhost:8000/admin/factor/diagnostics?factor=factor_score&horizon=5&days=60"

# 组合建议
curl "http://localhost:8000/admin/portfolio/recommendation?top_n=8&leaders_per_mainline=5"
```

### 主线复盘与龙头

```bash
# 最近 10 个交易日主线复盘，返回宽主题归并后的 series + analysis.review_10d
# 同时会补充细分驱动字段（display_name / focus_tags / driver_summary）
# 其中 review_10d.mainlines[i].leaders 默认可直接支撑前端 Top 5 展示
curl "http://localhost:8000/admin/mainline_history?days=10"

# 主线和主线下龙头，优先输出近 10 日持续性更强的方向
# Dashboard 当前使用 limit=5 拉取主线龙头，页面优先突出每条主线最值得先看的几只核心龙头
# 返回结果会保留宽主题 sector，并额外提供 display_sector / focus_tags / driver_summary
curl "http://localhost:8000/admin/mainline/leaders?limit=5&min_score=60"

# 查看单只股票被映射到哪条主线，排查“新能源 vs 电力公用”这类相近题材误判
curl "http://localhost:8000/admin/stock/603693.SH/mainline_analysis"
```

### 数据质量检查

```bash
# 完整性报告
curl "http://localhost:8000/admin/integrity?start_date=2025-01-01&end_date=2026-12-31"

# 指定交易日状态（与完整性报告使用一致阈值）
curl "http://localhost:8000/admin/data/day_status?date=2026-03-27"

# 任务状态
curl "http://localhost:8000/admin/tasks/status"

# 最近情绪
curl "http://localhost:8000/admin/market_sentiment?days=30"
```

### 自选盯盘

```bash
# 登录获取 token（示例：admin / admin）
TOKEN=$(curl -s -X POST "http://localhost:8000/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin" | python -c "import json,sys; print(json.load(sys.stdin)['access_token'])")

# 自选盯盘接口按当前登录用户隔离，调用时需携带 token
# 高频轮询建议走 compact，减少接口体积和后端内存抖动
curl "http://localhost:8000/admin/watchlist/realtime?analysis_depth=compact&src=sina" \
  -H "Authorization: Bearer ${TOKEN}"

# 若今日日线尚未同步，Watchlist 实时接口和 /admin/stock/{ts_code}/kline
# 仍会优先拼接今日实时/收盘后快照；遗留无效代码会以空占位返回，便于前端清理

# 某一只股票打开详情时，再按需取深度分析
curl "http://localhost:8000/admin/watchlist/600519.SH/analysis" \
  -H "Authorization: Bearer ${TOKEN}"

# detail 内会补充：
# - classification：当前更按哪条产业方向理解，以及为什么这样归类
# - level_methodology：支撑/压力位的筛选定义
# - key_levels[*].note：单个点位来自哪条均线/区间高低点、距离现价多远、失效怎么看
```

### AI 分析

```bash
# 登录获取 token（默认开发账号：admin / admin）
curl -X POST "http://localhost:8000/auth/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=admin"

# 个股 AI 分析：将上一步返回的 access_token 填入 Authorization
# 后端会先把行情/资金/市场压缩成摘要，再要求模型输出结论、关键价位和动作
# commentary_snapshot 会额外带入个股定位以及支撑/压力位的定义，减少模型把旧行业标签直接当主判断
curl -X POST "http://localhost:8000/admin/stock/analyze" \
  -H "Authorization: Bearer <access_token>" \
  -H "Content-Type: application/json" \
  -d '{"ts_code":"600519.SH","force_refresh":true}'
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
- 不要用 Python 脚本直接 `duckdb.connect()` 打开 `jarvis.duckdb` 做排查；诊断查询统一走后端接口
- 日常开发直接在主机编辑代码即可，前后端代码会热加载到容器，不需要手工 copy，也不需要为日常改动在主机额外执行 `npm run build`
- DuckDB 连接会读取 `duckdb_memory_limit` 和 `duckdb_threads` 配置，建议在 `.env` 中按容器资源显式设置
- Tushare API 有限流，请使用 tenacity 实现重试逻辑
- 概念数据同步已支持“`short token` 优先同花顺、失败回退 Tushare concept/concept_detail”；当前若 `stock_concepts.src` 只有 `ts`，说明库内概念尚未切到同花顺口径
- 如果发现概念覆盖不完整，可先用 `/admin/db/query` 抽样核对个股概念；例如新易盛在仅有 `ts` 数据时可能只落一条概念，需切换 `TUSHARE_TOKEN_TYPE=short` 后重新同步概念库
- 主线识别采用“宽主题稳定跟踪 + 强势股细分驱动拆解”的双层输出：`mapped_name` 负责稳定归并，`focus_tags` / `driver_summary` 负责解释最近真正上涨的细分原因；当 `stock_basic.industry` 仍停留在上游宽口径、但概念强证据更明显指向下游高权重赛道时，会允许概念主导重分类，避免把半导体材料/设备转型公司压回传统材料或泛电子
- `/admin/mainline/leaders` 的龙头评分会剔除北交所标的，并以近10日趋势先行、持续资金跟踪、成交热度和题材命中为主，不再退化成单日涨幅榜
- 盘中预估依赖 `realtime_quote` 的可用性与权限
- 高频自选刷新建议使用 `/admin/watchlist/realtime?analysis_depth=compact`，详情页再单独调用 `/admin/watchlist/{ts_code}/analysis`；若今日日线尚未 ETL 入库，接口仍会优先返回当日实时/收盘后快照，`/admin/stock/{ts_code}/kline` 也会补一根当日临时 bar
- 自选盯盘接口按当前登录用户隔离，`watchlist` 表使用 `user_id + ts_code` 复合主键；旧版全局自选会在启动时迁移到现有用户名下
- 新增自选仅允许 `stock_basic` 中已存在的代码；遗留无效代码会以空占位返回，便于前端删除，不再拖垮整批刷新
- Watchlist 页面当前拆成“持仓跟踪 + 观察列表”：非持仓自选会自动归到观察列表，列表默认只展示结论不展示 10D 历史，顶部不再单独放汇总卡，`试错 / 主动进攻` 信号会标红浮出，专注模式只展示持仓股
- `/admin/stock/analyze` 默认不再注入关联个股推荐段，自定义模板建议优先使用 `{stock_snapshot}`、`{capital_flow_snapshot}`、`{market_context}`、`{holding_context}`、`{commentary_snapshot}`、`{analysis_snapshot}`
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

---

## 文档维护说明

### ⚠️ AI工作规范（必读）

#### 1. 容器化开发

**代码在Docker容器中运行**，修改文件后：
- ✅ **应该做**：代码自动同步到容器，uvicorn `--reload` 会自动重载
- ❌ **不要做**：不需要重启容器、不需要重新编译前端
- ✅ **应该做**：修改后通过 `curl` 调用API验证功能
- ✅ **应该做**：进入容器验证Python代码可行性（如需要）

#### 2. 修改后验证

每次代码修改后，**必须验证功能**：
1. 验证代码是否按预期工作
2. 不要假设代码能运行，必须实际测试

#### 3. 节省Token消耗

**减少不必要的索引和搜索操作**：
- ❌ **不要做**：不要多次搜索同一文件
- ❌ **不要做**：不要重复读取已读过的文件
- ✅ **应该做**：一次读取足够上下文（用limit参数）
- ✅ **应该做**：批量执行独立的工具调用
- ✅ **应该做**：直接定位文件路径，避免全局搜索

### 文档同步规则

**重要**：每次AI修改代码后，必须同步更新文档。

#### 需要更新文档的情况

- 新增/修改API接口
- 新增/修改ETL任务
- 新增/修改数据库表
- 新增/修改策略模块
- 修改项目结构
- 修改配置参数

#### 更新内容

1. **AGENTS.md**：
   - 更新"可用系统端点"表格
   - 更新"当前ETL任务"列表
   - 更新"当前策略模块"列表
   - 更新"当前数据库表"列表
   - 更新"项目结构"目录结构

2. **docs/README.md**：
   - 更新API接口表格
   - 更新数据库表结构
   - 更新核心模块描述
   - 更新系统架构图

#### 更新流程

1. 代码修改完成后，总结变更内容
2. 更新AGENTS.md相关部分
3. 更新docs/README.md相关部分
4. 确保两份文档保持一致

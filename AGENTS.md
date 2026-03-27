# AGENTS.md - Jarvis-Quant 开发指南

## ⚠️ 关键：AI 工作规范（必读）

### 1. 容器化开发

**代码在Docker容器中运行**，修改文件后：
- ✅ **应该做**：代码自动同步到容器，uvicorn `--reload` 会自动重载
- ❌ **不要做**：不需要重启容器、不需要重新编译前端
- ✅ **应该做**：修改后通过 `curl` 调用API验证功能
- ✅ **应该做**：进入容器验证Python代码可行性（如需要）

```bash
# 验证后端代码（直接curl即可）
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT 1"}'

# 如需进入容器调试
docker-compose exec backend bash
```

### 2. 修改后验证

每次代码修改后，**必须验证功能**：
1. 验证代码是否按预期工作
2. 不要假设代码能运行，必须实际测试

### 3. 节省Token消耗

**减少不必要的索引和搜索操作**：
- ❌ **不要做**：不要多次搜索同一文件
- ❌ **不要做**：不要重复读取已读过的文件
- ✅ **应该做**：一次读取足够上下文（用limit参数）
- ✅ **应该做**：批量执行独立的工具调用
- ✅ **应该做**：直接定位文件路径，避免全局搜索

### 4. 文档同步

代码修改后，**必须更新文档**：
- 更新AGENTS.md中的相关列表（API端点、ETL任务、数据库表等）
- 更新docs/README.md中的相应部分

---

## 项目概述

Jarvis-Quant 是一个A股量化交易决策系统，采用FastAPI后端（Python 3.12）和Vue 3前端。

## 构建与开发命令

### 后端

```bash
# 安装依赖
cd backend
pip install -r requirements.txt

# 运行开发服务器（在Docker中或本地）
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 对于重ETL/重计算任务，建议使用单进程（避免reload worker）
uvicorn main:app --host 0.0.0.0 --port 8000

# 使用Docker运行
docker-compose up backend
```

### 前端

```bash
cd frontend
npm install
npm run dev      # 开发服务器
npm run build    # 生产构建
npm run preview  # 预览生产构建
```

### 使用Docker Compose运行

```bash
# 启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f backend
docker-compose logs -f frontend

# 重启服务
docker-compose restart backend
```

### 测试

目前项目中**没有正式的单元测试**。测试功能：

```bash
# 使用curl测试API端点（见下文）
curl http://localhost:8000/

# 检查系统状态
curl http://localhost:8000/system/status
```

## 数据库访问（重要）

**本项目使用DuckDB嵌入式单进程模型**。为减少`Unique file handle conflict`和锁竞争：

- 后端使用**进程级共享DuckDB连接**（单例模式），采用序列化访问
- **不要**从其他进程直接打开临时DuckDB会话进行常规读取
- 开发期间进行诊断/查询时，建议使用**HTTP API + curl**（`/admin/db/query`），而不是直接执行数据库命令

### 通过API查询数据库

```bash
# 执行只读SQL查询
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM stock_basic LIMIT 5"}'

# 获取市场情绪
curl "http://localhost:8000/admin/market_sentiment?days=30"

# 获取数据完整性报告
curl "http://localhost:8000/admin/integrity?start_date=2025-01-01&end_date=2025-12-31"

# 统一ETL同步入口（示例）
curl -X POST http://localhost:8000/admin/etl/sync \
  -H "Content-Type: application/json" \
  -d '{"task":"daily","years":1}'

# 统一情绪分析入口
curl -X POST "http://localhost:8000/admin/etl/sentiment?days=365&sync_index=false"
```

### 可用系统端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/backtest_grid` | GET | 回测网格搜索 |
| `/backtest_result` | GET | 回测结果 |
| `/backtest_walkforward` | GET | 回测前向验证 |
| `/data/dashboard` | GET | 数据管理仪表盘（表统计、日期范围） |
| `/data/day_status` | GET | 指定日期各表数据状态 |
| `/data/sync_date` | POST | 触发指定日期的全量数据刷新 |
| `/data_verify` | GET | 数据验证（API vs DB对比） |
| `/db/query` | POST | 执行SQL查询 |
| `/etl/sentiment` | POST | 情绪分析ETL |
| `/etl/sync` | POST | 数据同步ETL（支持start_date/end_date） |
| `/etl/train_kline_patterns` | POST | 训练K线形态 |
| `/integrity` | GET | 数据完整性检查（含summaries汇总） |
| `/mainline_history` | GET | 主线历史记录（聚焦近10日主线演变、当前最强/连续主线及Top5个股摘要） |
| `/mainline/leaders` | GET | 主线及主线下龙头（支持前端按Top5展示，优先近10日持续性方向） |
| `/market/suggestion` | GET | 市场建议 |
| `/market_sentiment` | GET | 市场情绪 |
| `/sentiment/preview` | GET | 情绪预览 |
| `/stock/analyze` | POST | 个股AI分析（摘要驱动、结论优先、返回更短） |
| `/stock/search` | GET | 股票搜索（支持拼音首字母） |
| `/stock/{ts_code}/kline` | GET | 股票K线数据（交易日且日线未落库时会补当日临时 bar） |
| `/stock/{ts_code}/mainline_analysis` | GET | 个股主线归属分析（返回 `mapped_sector` / `is_mainline` 等） |
| `/tasks/status` | GET | 任务状态 |
| `/auth/token` | POST | 获取令牌 |
| `/users` | GET | 用户列表 |
| `/users` | POST | 创建用户 |
| `/users/me/ai-config` | GET | 获取AI配置 |
| `/users/me/ai-config` | PUT | 更新AI配置 |
| `/users/me/holdings` | GET | 获取持仓 |
| `/users/me/holdings/{ts_code}` | PUT | 更新持仓 |
| `/users/me/holdings/{ts_code}` | DELETE | 删除持仓 |
| `/users/me/prompt-templates` | GET | 获取提示模板 |
| `/users/me/prompt-templates` | POST | 创建提示模板 |
| `/users/me/prompt-templates/{template_id}` | PUT | 更新提示模板 |
| `/users/me/prompt-templates/{template_id}` | DELETE | 删除提示模板 |
| `/users/me/selected-template` | GET | 获取选中模板 |
| `/users/me/selected-template` | PUT | 更新选中模板 |
| `/users/password` | PUT | 修改密码 |
| `/users/{user_id}` | DELETE | 删除用户 |
| `/watchlist` | GET | 当前登录用户的关注列表 |
| `/watchlist` | POST | 为当前登录用户添加关注 |
| `/watchlist/realtime` | GET | 当前登录用户的实时关注数据（支持 `analysis_depth=compact|full`，交易日优先展示当日快照，高频轮询推荐 `compact`） |
| `/watchlist/{ts_code}/analysis` | GET | 当前登录用户自选内单只股票的深度分析（详情弹窗按需加载） |
| `/watchlist/{ts_code}` | DELETE | 删除当前登录用户的关注 |

## 代码风格指南

### Python（后端）

- **导入顺序**：标准库 → 第三方库 → 本地模块
- **格式化**：使用Black兼容风格（最大行长度100）
- **类型**：对所有API请求/响应模式使用Pydantic模型
- **命名**：
  - `snake_case`用于函数、变量、数据库列
  - `PascalCase`用于类、Pydantic模型
  - `UPPER_SNAKE_CASE`用于常量
- **错误处理**：使用try/except记录日志，API错误抛出HTTPException
- **数据库**：使用`db/connection.py`中的共享连接助手；避免在业务模块中直接打开新的`duckdb.connect()`
- **异步**：对FastAPI端点使用`async/await`，用`asyncio.to_thread()`包装同步函数

### Vue 3（前端）

- **风格**：使用Composition API和`<script setup>`语法
- **状态**：使用Pinia进行全局状态管理
- **HTTP**：使用axios进行API调用
- **组件**：遵循Vue 3命名规范（PascalCase）

### 通用规范

- **日志**：使用Python的`logging`模块，包含时间戳（上海时区）
- **配置**：使用`.env`文件，通过`os.getenv()`或pydantic-settings访问
- **DuckDB资源**：通过 `duckdb_memory_limit` / `duckdb_threads` 控制连接资源占用，避免默认配置吃满内存
- **日期**：使用`arrow`库处理日期，在DuckDB中存储为DATE类型

## 项目结构

```
/root/jarvis
├── backend/
│   ├── api/           # FastAPI路由（admin.py, auth.py）
│   ├── core/          # 核心常量和配置
│   ├── db/            # 数据库连接和schema
│   ├── etl/           # 数据同步任务和提供者
│   │   ├── tasks/    # 独立数据任务
│   │   ├── providers/# 数据源提供者（Tushare）
│   │   └── utils/    # 工具（回填、因子、K线形态）
│   ├── strategy/      # 量化策略
│   │   ├── sentiment/# 市场情绪分析
│   │   └── mainline/ # 主流板块分析
│   └── main.py       # FastAPI应用入口
├── frontend/         # Vue 3 + Vite + TailwindCSS
├── docs/             # 文档
├── scripts/          # 实用脚本
└── data/             # DuckDB数据库文件
```

## 关键技术

- **后端**：FastAPI, uvicorn, pandas, duckdb, pydantic, tushare
- **前端**：Vue 3, Vite, Pinia, Vue Router, ECharts, Axios, TailwindCSS
- **数据库**：DuckDB（专注分析，不适合高并发写入）
- **调度**：APScheduler用于定时任务

## 常见开发任务

### 添加新ETL任务

1. 在`backend/etl/tasks/`创建任务类
2. 继承基类或遵循现有模式
3. 在`sync.py`中注册或通过API端点暴露

### 添加新API端点

1. 在`backend/api/admin.py`添加路由或创建新路由文件
2. 使用Pydantic模型进行请求/响应验证
3. 使用`get_db_connection()`进行数据库操作

### 修改数据库模式

1. 通过API创建表：`POST /system/create_tables`
2. 或修改`db/schema.py`并重启后端

### 当前ETL任务

- `base_task`
- `calendar_task`
- `concepts_task`
- `daily_price_task`
- `financials_task`
- `fx_task`
- `margin_task`
- `market_index_task`
- `moneyflow_task`
- `sentiment_history_task`
- `stock_basic_task`

### 当前策略模块

- `mainline/analyst`
- `mainline/config`
- `sentiment/analyst`
- `sentiment/config`

### 当前数据库表

- `users`
- `stock_basic`（包含pinyin和pinyin_abbr字段支持拼音搜索）
- `daily_price`
- `stock_concepts`
- `stock_concept_details`
- `stock_moneyflow`
- `market_index`
- `market_sentiment`
- `stock_margin`
- `watchlist`（按 `user_id + ts_code` 复合主键隔离每个用户的自选盯盘）
- `mainline_scores`
- `user_ai_config`
- `user_prompt_templates`
- `user_holdings`
- `ai_analysis_cache`
- `etl_tasks`

## 注意事项

- DuckDB有并发限制——建议基于API读取（`curl /admin/db/query`），避免进程外直接数据库访问
- 主线分析会对过细概念做宽主题归并，并过滤预增、次新、地域国改等事件噪声，避免把短期标签误判为主线
- 主线映射当前会把 `stock_basic.industry` 作为全行业锚点，并对同方向的重复概念做衰减，减少“相关概念堆叠”造成的误判
- 通信链当前已拆分为 `光通信` / `通信网络` / `算力基建`，避免把 `中际旭创`、`烽火通信` 这类不同子方向压成同一主线
- Dashboard 首页当前拆成“情绪驾驶舱 + 主线作战板”，并与 Watchlist 保持同一套克制风格：情绪区顶部收成摘要条和条目式结论，再下钻到更短的情绪趋势图；主线最多展示 3 条，并以紧凑行列表呈现，板块内 Top5 龙头默认折叠，避免首页信息过载
- Dashboard 配色当前采用低饱和冷暖分区：情绪区偏暖色、主线区偏冷色，只做轻微层次变化，不使用高饱和装饰色块
- Watchlist 当前拆成“持仓跟踪 + 观察列表”两段：非持仓自选自动归入观察列表，列表默认只展示结论不展示 10D 历史，顶部不再单独放汇总卡；出现“试错 / 主动进攻”时需标红浮出，专注模式只展示持仓股
- 当前 `concepts_task` 已支持“`short token` 优先同花顺、失败回退 Tushare concept/concept_detail”；若库内 `stock_concepts.src` 只有 `ts`，说明目前并未拿到同花顺概念成分
- 高频盯盘列表请优先调用 `/admin/watchlist/realtime?analysis_depth=compact`，详情再单独请求 `/admin/watchlist/{ts_code}/analysis`；若今日日线尚未 ETL 入库，接口仍会优先返回当日实时/收盘后快照，`/admin/stock/{ts_code}/kline` 也会补一根当日临时 bar
- 自选盯盘接口现按当前登录用户隔离，调用 `/admin/watchlist*` 时需携带 `Authorization: Bearer <token>`
- 新增自选仅允许 `stock_basic` 中存在的代码；遗留无效代码会以空占位返回，便于前端清理，不再阻塞其他股票刷新
- `/admin/stock/analyze` 默认使用压缩后的行情/资金/市场摘要构造提示词，不再注入关联个股推荐段；自定义模板优先使用 `{stock_snapshot}`、`{capital_flow_snapshot}`、`{market_context}`、`{holding_context}`、`{commentary_snapshot}`、`{analysis_snapshot}`
- 使用任务队列（`/admin/tasks/status`）处理长时间运行的操作
- Tushare API有限流——使用tenacity实现重试逻辑
- 市场数据在交易时间后（上海时间16:00+）更新

## K线形态识别

系统包含一个全面的K线形态识别引擎，位于`backend/etl/utils/kline_patterns.py`。该模块提供：

- 向量化形态检测，覆盖所有主要K线形态
- 支持校准的置信度评分
- 历史性能分析和回测
- 兼容新旧代码的接口

### 关键函数

- `detect_all_patterns(df)`：在DataFrame上运行所有形态识别
- `get_latest_signals(df)`：提取最新信号及置信度分数
- `PatternRecognizer`：兼容旧版接口的形态识别类
- `get_professional_commentary(df, patterns)`：生成分析评论
- `get_professional_commentary_detailed(df, patterns)`：生成结构化专业点评，返回 `decision` / `trade_plan` / `key_levels` / `observation_points`

### 支持的形态

**看涨形态**：锤子线、吞没形态、刺透形态、启明星、三白兵、上升三法、仙人指路、老鸭头、蓄势

**看跌形态**：上吊线、流星线、看跌吞没、乌云盖顶、黄昏星、三黑鸦、下降三法、背离

**中性形态**：十字星、极度缩量

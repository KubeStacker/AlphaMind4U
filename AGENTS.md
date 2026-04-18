# AGENTS.md - Jarvis-Quant 开发指南

## ⚠️ 关键：AI 工作规范（必读）

### 1. 容器化开发

**代码在Docker容器中运行**，修改文件后：
- ✅ **应该做**：Codex 直接在主机环境执行命令和编辑文件，不要额外在沙箱里再跑一套开发流程
- ✅ **应该做**：代码自动同步到容器，uvicorn `--reload` 会自动重载
- ❌ **不要做**：不需要重启容器、不需要手工 copy 文件到容器
- ❌ **不要做**：不需要在主机额外重新编译前端，不要为了日常改动执行 `npm run build`
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
3. ❌ **不要做**：不要使用 `npm run build`、`npm run dev` 等构建/开发命令验证前端，代码会通过Docker挂载直接动态更新到容器
4. ❌ **不要做**：不要使用 `pip install` 重新安装后端依赖来验证，容器内已包含所需依赖
5. ✅ **应该做**：后端修改后通过 `curl` 调用API验证功能
6. ✅ **应该做**：前端修改后通过浏览器访问容器暴露的端口验证
7. ✅ **应该做**：前端较大改动或反复无法正确修改时，建议使用无头浏览器访问验证，用户名 `yuanpeng`，密码 `1qaz2wsx`

### 3. Tushare Pro 数据源（重要）

**本项目优先使用 Tushare Pro 接口获取A股数据**：
- ✅ **应该做**：新增数据同步任务时，优先查找并使用 Tushare Pro 接口
- ✅ **应该做**：参考 `backend/etl/providers/` 中已有的 Tushare 接口封装
- ⚠️ **注意**：当前 Tushare Pro Token 仅有 **2000 积分**，部分高积分接口（如 `stk_factor`、`daily_hf` 等）无权限调用
- ⚠️ **注意**：如果遇到积分不足导致无法验证的情况，**给出说明即可，不用强求**，不要花费大量Token尝试绕过或寻找替代验证方案
- ❌ **不要做**：不要假设所有 Tushare 接口都可用，实现前先确认接口所需积分

### 4. 节省Token消耗

**减少不必要的索引和搜索操作**：
- ❌ **不要做**：不要多次搜索同一文件
- ❌ **不要做**：不要重复读取已读过的文件
- ✅ **应该做**：一次读取足够上下文（用limit参数）
- ✅ **应该做**：批量执行独立的工具调用
- ✅ **应该做**：直接定位文件路径，避免全局搜索

### 5. 文档同步

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
- **不要**用 Python 脚本直接 `duckdb.connect()` 打开 `jarvis.duckdb` 做诊断查询
- 开发期间所有数据库诊断/查询，统一走**后端 HTTP API + curl**（优先 `/admin/db/query`）
- 如果需要验证某段 Python 逻辑，应进入后端容器复用应用环境，而不是在宿主机直接打开 DuckDB 文件

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
| `/factor/diagnostics` | GET | 因子诊断（IC/RankIC/分层收益，支持行业中性开关） |
| `/integrity` | GET | 数据完整性检查（含summaries汇总） |
| `/mainline_history` | GET | 主线历史记录（聚焦近10日主线演变、当前最强/连续主线及Top5个股摘要，并返回 `display_name` / `focus_tags` / `driver_summary` 等细分驱动字段） |
| `/mainline/leaders` | GET | 主线及主线下龙头（支持前端按Top5展示，优先近10日持续性方向，剔除北交所，并按趋势先行/资金追踪/热度/题材命中综合评分，返回 `display_sector` / `focus_tags` / `driver_summary`） |
| `/market/suggestion` | GET | 市场建议 |
| `/market_sentiment` | GET | 市场情绪历史 + 自动补算最新收盘情绪；交易时段按上证/创业板实时叠加，返回 10Y/Pizza 风险旁证、自动刷新提示，以及涨停/跌停回填后的情绪明细 |
| `/portfolio/recommendation` | GET | 组合建议（regime + theme + stock rank + position sizing） |
| `/strategy-plaza/strategies` | GET | 策略广场策略列表（全局公共策略、显示顺序、启用状态） |
| `/strategy-plaza/observations` | GET | 策略广场指定策略/日期的新进入观察标的列表，附 3/5/10 日结果简表 |
| `/strategy-plaza/summary` | GET | 策略广场指定策略/日期的滚动统计摘要与总结文本；若当日无新进入观察标的则返回 `null`，避免空列表下仍展示历史总结 |
| `/strategy-plaza/run` | POST | 手动触发策略广场任务，支持指定历史日期和单个策略重跑，任务进入 `etl_tasks` 队列 |
| `/sentiment/preview` | GET | 情绪预览 |
| `/stock/analyze` | POST | 个股AI分析（摘要驱动、结论优先、返回更短；默认只注入标的自身行情/资金/持仓等客观数据，不再带入市场情绪、主线映射或程序评分） |
| `/stock/search` | GET | 股票搜索（支持拼音首字母） |
| `/stock/{ts_code}/kline` | GET | 股票K线数据（交易日且日线未落库时会补当日临时 bar） |
| `/stock/{ts_code}/mainline_analysis` | GET | 个股主线归属分析（返回 `mapped_sector` / `is_mainline` 等） |
| `/tasks/status` | GET | 任务状态 |
| `/auth/token` | POST | 获取令牌 |
| `/users` | GET | 用户列表 |
| `/users` | POST | 创建用户 |
| `/users/me/ai-config` | GET | 获取当前AI配置（返回当前 provider 顶层字段，并附带 `provider_configs` 供前端切换时回显各家历史配置） |
| `/users/me/ai-config` | PUT | 更新当前AI配置（仅覆盖当前 provider，同时把该 provider 镜像回 `user_ai_config` 兼容旧调用方） |
| `/users/me/holdings` | GET | 获取持仓 |
| `/users/me/holdings/batch` | POST | 批量更新持仓（支持同步到自选、可选按提交结果替换旧持仓） |
| `/users/me/holdings/parse-image` | POST | 使用 OpenAI 配置的多模态模型识别持仓截图，并基于 `stock_basic` 做“代码提取 + 名称归一化”自动匹配后返回预览；`unmatched` 表示图片已识别但待确认代码，不等于识别失败；若未配置 OpenAI API Key 则直接报错，其他 AI 分析仍使用当前选中的 provider |
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
| `/watchlist/realtime` | GET | 当前登录用户的实时关注数据（支持 `analysis_depth=compact|full` 与 `sort_mode=auto|manual`，交易日优先展示当日快照；`compact` 会返回 `action_signal` / `signal_reasons` / `key_levels` / `technical.volume_ratio`，并额外直出 `decision.state_bucket` / `decision.recommendation_score` / `breakout` / `entry_quality` / `ranking.rank_reason` 以及行级 `volume_ratio` / `turnover_rate`，供观察列表自动排序与首屏展示） |
| `/watchlist/levels/backtest` | GET | Watchlist 点位回测诊断（默认聚焦创业板/科创板高流动性样本；也支持 `codes=...` 对当前自选/指定股票做定向回放，对比 `adaptive` 与 `legacy` 的支撑/压力反应率、主点位距离与二级间距异常） |
| `/watchlist/{ts_code}/analysis` | GET | 当前登录用户自选内单只股票的深度分析（详情弹窗按需加载，返回 `action_signal` / `signal_reasons` / `intraday_context` / `classification` / `level_methodology` / `key_levels[*].note`） |
| `/watchlist/{ts_code}` | DELETE | 删除当前登录用户的关注 |
| `/docs/list` | GET | 获取文档列表（支持include_published筛选published目录文档） |
| `/docs/published/list` | GET | 获取published目录下文档列表 |
| `/docs/{doc_id}` | GET | 获取文档内容 |
| `/docs/{doc_id}/progress` | GET/POST | 获取/更新文档阅读进度（记忆功能） |
| `/docs/tags` | GET/POST | 获取/创建用户自定义标签 |
| `/docs/tags/{tag_id}` | PUT/DELETE | 更新/删除用户自定义标签 |
| `/docs/{doc_id}/tags` | GET/POST | 获取/设置文档关联的标签 |
| `/docs/{doc_id}/notes` | GET/POST | 获取/创建文档笔记 |
| `/docs/notes/{note_id}` | PUT/DELETE | 更新/删除文档笔记 |
| `/docs/notes/all` | GET | 获取所有笔记汇总（支持按日期范围筛选） |

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

### 前端设计规范

- **克制表达**：避免与功能无关的提示性描述、装饰性标签、解释性文案；界面只展示决策所需信息，不为"看起来完整"而增加内容
- **高级感**：保持页面简洁、留白充足、信息层次清晰；使用低饱和度色彩，避免花哨装饰和高饱和色块
- **设计一致性**：整个系统保持统一的视觉语言，包括间距、圆角、阴影、字体大小、色彩体系等；同一类组件使用相同的设计模式
- **色彩规范**：**禁止使用AI常见的紫色渐变、靛蓝色调**（如 `purple-*`、`indigo-*`、`violet-*` 渐变）；采用低饱和冷暖分区——情绪区偏暖色、主线区偏冷色，只做轻微层次变化；主色调以中性灰、深灰、白色为基础，辅以克制的红色（买入信号）和绿色（卖出信号）
- **信息密度**：首屏直出核心信息，支撑/压力/触发/失效等关键点位必须可见，不能被抽象总结替代
- **减少层级**：能在原卡片内解决的不新增额外容器、概览区或折叠层
- **去重原则**：同一信息只在一个位置展示一次，禁止在chip、标签、结论段落中重复渲染相同内容；信号chip显示"试错"后，结论段落不再重复"试错：xxx"
- **小白友好**：不展示UI使用指南（如"先处理动作和风险，再看解释细节"）、不展示filter解释文案、不展示冗长的刷新状态描述；状态标签用最简短词汇（如"实时刷新"而非"自动实时刷新"）
- **后端数据精简**：API返回的 `headline` / `current_action_text` / `status` 等字段只返回动作关键词（如"试错"），详细操作指导（如"优先盯支撑承接，只有放量确认再执行"）只保留在详情弹窗中，不在列表卡片渲染

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
│   │   ├── mainline/ # 主流板块分析
│   │   └── plaza/    # 策略广场插件、归档与回测服务
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
- `factor_data_task`
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
- `plaza/builtin/head7_dragon_return`
- `plaza/builtin/single_yang_hold`
- `plaza/builtin/golden_eye`
- `plaza/registry`
- `plaza/service`
- `plaza/summarizer`
- `sentiment/analyst`
- `sentiment/config`

### 当前数据库表

- `users`
- `stock_basic`（包含pinyin和pinyin_abbr字段支持拼音搜索）
- `daily_price`
- `stock_daily_basic`
- `stock_index_member_all`
- `stock_express`
- `stock_factor_daily`
- `stock_concepts`
- `stock_concept_details`
- `stock_moneyflow`
- `market_index`
- `market_sentiment`
- `stock_margin`
- `watchlist`（按 `user_id + ts_code` 复合主键隔离每个用户的自选盯盘）
- `mainline_scores`
- `strategy_definitions`
- `strategy_observations`
- `strategy_backtest_runs`
- `strategy_daily_summaries`
- `user_ai_config`
- `user_ai_provider_configs`（按 `user_id + provider` 分别保存 OpenAI / DeepSeek / Gemini 等模型配置，切换 provider 不再互相覆盖）
- `user_prompt_templates`
- `user_holdings`
- `ai_analysis_cache`
- `etl_tasks`
- `doc_reading_progress`（用户文档阅读位置记忆）
- `doc_user_tags`（用户自定义文档标签）
- `doc_notes`（文档笔记/点评）
- `doc_tag_mapping`（文档标签关联表）

## 注意事项

- DuckDB有并发限制——建议基于API读取（`curl /admin/db/query`），避免进程外直接数据库访问
- 主线分析会对过细概念做宽主题归并，并过滤预增、次新、地域国改等事件噪声，避免把短期标签误判为主线
- 主线映射当前会把 `stock_basic.industry` 作为全行业锚点，并对同方向的重复概念做衰减；但当基础行业只是 `化工材料` / `电子硬件` 这类上游宽口径、而概念强证据明显指向更高权重下游方向时，会允许概念主导重分类，减少把 `鼎龙股份` 这类“传统行业口径 + 半导体材料转型”误压回原行业
- 主线龙头评分当前会剔除北交所标的，不再按单日涨幅直接排队；更强调近10日趋势先行、持续资金跟踪、成交热度和细分题材命中度
- 通信链当前已拆分为 `光通信` / `通信网络` / `算力基建`，避免把 `中际旭创`、`烽火通信` 这类不同子方向压成同一主线
- Dashboard 首页当前拆成“情绪驾驶舱 + 主线作战板”，并与 Watchlist 保持同一套克制风格：情绪区头部直接输出结论，不再拆单独标题，也不重复展示日期/分区标签，只负责节奏、仓位、进攻方式和风控；“预测情绪”改为按钮触发弹窗参考，不在首页固定占位；主线区只负责方向优先级和龙头观察，主体最多展示 3 条主线，顶部只保留轮动正文，Top5 龙头改为在左侧方向卡片内按点击懒加载展开，避免右侧信息堆叠和首屏重复计算
- 策略广场当前作为与 Watchlist 并列的一级入口，只展示“某策略某日期新进入观察的标的 + 3/5/10 日结果摘要 + 滚动总结”，不展示中间阶段池；策略本体由 `backend/strategy/plaza/builtin/` 下的后端代码决定，可自行定义多阶段触发、过滤、延迟入观察和回测锚点
- 策略广场当前内置 `头7龙回头`、`单阳不破`、`大眼睛/空中加油` 三个本地指标策略，不依赖 AI 提示词；板块阈值按代码前缀区分主板与创业板/科创板，`301/300/688` 统一按高弹性板块处理
- `头7龙回头` 当前按“最后一个封死涨停板或放量启动阳线触发后，先定位该段最高点，再在最高点后 5-8 个交易日缩量回调找二波启动点”实现：回撤保护与起涨点判断始终锚定触发K，而不是后续最高点那根K；主板要求回调低点不跌破触发涨停实体回撤一半、重点看 MA10 支撑；创业板/科创板允许回到触发K起涨点附近、重点看 MA20 支撑；最新观察日必须满足量能萎缩到触发段参考量的 `1/3` 以内，并出现小阴小阳止跌
- 策略广场结果表当前支持点击标的名称，直接复用 Watchlist 同链路的 K 线弹窗与 `/admin/stock/{ts_code}/kline` 数据，不再额外维护一套独立图表逻辑
- Dashboard 情绪区当前会按接口返回的 `dashboard_refresh_seconds` 自动刷新：交易时段只用 `上证指数 + 创业板指` 实时快照修正盘中节奏，收盘后优先补算最新交易日情绪并直接返回收盘基线；10Y 优先取 Tushare `us_tycr` 的最近可用 `y10`，若失败再回退海外源，Pizza 只解析 live 区段的 DOUGHCON/当前 spike，统一作为外部风险旁证，若容器环境无法访问海外站点则 `/admin/market_sentiment` 宏观字段会返回 unavailable，前端应展示降级态而不是假定有值
- Dashboard 内按钮触发的新内容需要使用固定宽度、固定分栏和占位容器，避免回测弹窗、诊断结果或异步内容加载时出现挤压、拉伸或表格畸形
- Dashboard 配色当前采用低饱和冷暖分区：情绪区偏暖色、主线区偏冷色，只做轻微层次变化，不使用高饱和装饰色块
- AI 配置当前采用“双层存储”：`user_ai_provider_configs` 保存各 provider 的历史配置，`user_ai_config` 仅镜像当前选中的 provider 供分析入口兼容读取；修改设置时不要再假设一行 `user_ai_config` 就代表全部 provider 状态
- Watchlist 当前拆成“持仓跟踪 + 观察列表”两段：非持仓自选自动归入观察列表，列表默认只展示结论不展示 10D 历史，顶部不再单独放汇总卡；持仓区统一按持仓总市值倒序排列，专注模式复用同一排序结果；观察列表改为后端 `recommendation_score + state_bucket` 自动排序，不再支持人工拖拽；观察列表默认折叠，折叠状态下自动刷新优先只更新持仓，观察价格改为手动展开后按需刷新；页面刷新时优先恢复本地快照并后台并行更新自选与持仓，减少空白等待；专注模式会在全屏持仓视图里只保留代码、现价、动作词与主支撑/主压力/触发/失效位，并进一步压成更紧凑的窄版双列点位块，不再显示涨跌幅、颜色提示或长句指导
- Watchlist 搜索添加 / 删除观察股当前采用“本地先更新 + 单票补拉”策略：添加后先局部插入卡片，再只请求该股票的 compact 实时数据；删除时先本地移除，不再为单条操作阻塞整页重算
- 持仓图片导入当前也采用“先识别预览、再确认写入”的两段式流程：识别成功只生成预览，不会自动改持仓；只有点击底部写入按钮后，才会调用 `/admin/users/me/holdings/batch`。真正写入时以前端提交、后端返回的标准化 `items` 为准，先本地更新 `holdings` 和必要的 watchlist 占位行，再后台补拉受影响股票；预览里的 `待确认` 明确表示图片已识别，但该项尚未自动匹配到 `stock_basic`
- 持仓批量写入同步自选时，后端会兼容旧版 `watchlist`（无 `sort_order` 列）与新版结构，避免历史库在 `/admin/users/me/holdings/batch` 上报 500
- 持仓图片识别在构建 `stock_basic` 匹配索引时，后端会兼容无 `pinyin` / `pinyin_abbr` 列的旧库，自动回退到 `ts_code + symbol + name` 匹配路径
- Watchlist 当前支持本地持久化的“隐私模式”：开启后会遮罩持仓股数、成本、盈亏、总市值，以及持仓图片识别预览里的股数/成本；图片导入弹窗底部确认栏也需要保留 `safe-area inset`，避免手机端被系统手势区或浏览器底栏压住
- Watchlist 首屏快照当前同时持久化到 `sessionStorage + localStorage`：页面刷新时优先恢复最近一次本地快照，再后台并行更新自选与持仓，减少空白等待
- Watchlist 搜索候选层、控制台下拉入口和 Settings 顶部 tab 必须使用清晰的近不透明实底浮层/按钮样式，不能与页面背景混成“透明态”，保证代码和名称在首屏直接可辨识
- Watchlist / Dashboard 的首屏 UI 只保留直接决策信息，不额外增加“汇总卡 / 统计概览 / 解释性摘要”这类弱信息层；支撑 / 压力 / 触发 / 失效等目标点位必须在卡片首屏直出，不能被摘要文案替代，也不能默认藏到二级弹窗
- 当前 `concepts_task` 已支持“`short token` 优先同花顺、失败回退 Tushare concept/concept_detail”；若库内 `stock_concepts.src` 只有 `ts`，说明目前并未拿到同花顺概念成分
- 概念同步当前采用 staging + 原子发布：先写入 `stock_concepts__staging` / `stock_concept_details__staging`，校验通过后再一次性覆盖正式表，避免刷新过程中主线/龙头接口读到半成品
- 因子层当前新增 `stock_daily_basic` / `stock_index_member_all` / `stock_express` / `stock_factor_daily`：`/admin/etl/sync` 支持 `daily_basic` / `index_members` / `express` / `factors`，并把换手率、量比、估值、资金和综合分回写到 `daily_price.factors`
- 高频盯盘列表请优先调用 `/admin/watchlist/realtime?analysis_depth=compact&sort_mode=auto`，详情再单独请求 `/admin/watchlist/{ts_code}/analysis`；`compact` 必须保持批量轻分析路径，只返回列表首屏所需的动作信号/量比/换手/关键位，以及自动排序所需的 `state_bucket` / `recommendation_score` / `breakout` / `entry_quality` / `ranking.rank_reason`，不要在列表接口里逐股跑完整形态与深度点评；若今日日线尚未 ETL 入库，接口仍会优先返回当日实时/收盘后快照，`/admin/stock/{ts_code}/kline` 也会补一根当日临时 bar
- 自选盯盘接口现按当前登录用户隔离，调用 `/admin/watchlist*` 时需携带 `Authorization: Bearer <token>`
- 新增自选仅允许 `stock_basic` 中存在的代码；遗留无效代码会以空占位返回，便于前端清理，不再阻塞其他股票刷新
- Watchlist 点位与动作信号当前以趋势、量价、主力资金、因子分和 realtime 位置主导，K 线形态仅作辅助参考；颜色语义统一为红色偏买入、绿色偏卖出、白色偏观望
- Watchlist 的支撑/压力位当前采用“rolling window 多源候选 + ATR/振幅归一化去重 + 结构触线共振”的统一算法：同时评估 MA5/10/20/60、近 5/10/20/60 日高低点、确认后的 swing high/low、趋势线、未补缺口和近 60 日成交密集区，再按来源共振、触碰次数、量能与最近性综合排序；其中结构压力要求至少 1 次阳线收盘锚定 + 2 次上影摸线，且至少 1 次上影发生在阳线收盘之后，但该锚点只作为触线证据之一，最终压力位会优先看趋势线 / 缺口 / 均线 / 成交密集区等多源共振，不再让单根K线开收盘价主导；结构支撑要求至少 4 次下探承接，含阴线收盘/阳线开盘锚点、2 次下影试探，并在第 4 次或之后出现放量小阳确认；主点位优先从近端可交易带里选，深层结构位下沉为二级缓冲位，避免首个点位离现价过远
- 点位引擎当前进一步按主板 / 创业板 / 科创板三套波动画像做自适应校准：创业板更保留趋势延续下的回踩空间，科创板则额外压缩二级间距并提升趋势/成交密集区权重；扩张日会额外生成 `近端波动防守位` 兜住一号支撑，统一修正聚类后距离口径，减少回踩/回落场景下点位过近或过远
- `/admin/watchlist/levels/backtest` 可直接对创业板/科创板样本做自驱动点位回放，输出 `adaptive` vs `legacy` 的支撑/压力反应率、主点位距离和二级间距异常，后续调参数优先以该接口验证
- `/admin/stock/analyze` 当前默认只向模型注入标的自身的客观数据摘要，不再带入市场情绪、主线映射、方向榜单或程序评分；`{commentary_snapshot}` 仅保留换手/量比/主力净额占比等客观补充字段；自定义模板优先使用 `{stock_snapshot}`、`{capital_flow_snapshot}`、`{holding_context}`、`{commentary_snapshot}`、`{analysis_snapshot}`，旧的 `{sector_context}` / `{market_context}` / `{market_sentiment}` / `{mainline}` 会在清洗时剔除
- 日线数据单位当前统一按 Tushare 口径处理：`daily_price.vol` / `/admin/stock/{ts_code}/kline[*].vol` 为“手”，`daily_price.amount` / `/admin/stock/{ts_code}/kline[*].amount` 为“千元”；前端若展示“亿”，应按 `amount / 1e5` 换算，AI 提示词也必须按同一口径格式化，不能把 `amount` 误当成“元”；K 线弹窗卡片和图例需显式标出单位，避免只显示“成交额 / 成交量”这种歧义标题
- `/admin/stock/{ts_code}/mainline_analysis` 当前即使遇到“非主线板块”或评分一票否决，也会返回已补齐的 `sector_resonance` / `breakout` / `capital_flow` 字段，不再因空评分字典触发 500
- `/admin/etl/sentiment?sync_index=true` 当前复用 `sync_core_market_indices`，`/admin/system/trigger_daily_sync` 复用 `perform_daily_data_update`，不要再调用旧的 `sync_core_indices` / `sync_daily_update`
- `stock_income` / `stock_fina_indicator` 已纳入 `db/schema.py` 初始化；`/admin/data/day_status` 与 `/admin/integrity` 对 `daily_price` / `stock_moneyflow` / `stock_margin` 统一使用 4000 条阈值判定交易日数据完整性
- `/admin/factor/diagnostics` 提供 `factor_score` / `trend_score` / `quality_score` 等因子的 IC、RankIC 和分层收益；`/admin/portfolio/recommendation` 则把情绪仓位、主线方向、横截面因子和相关性约束收口成组合建议
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
- `get_professional_commentary(df, patterns, context=None)`：生成分析评论
- `get_professional_commentary_detailed(df, patterns, context=None)`：生成结构化专业点评，返回 `decision` / `trade_plan` / `key_levels` / `level_methodology` / `classification` / `observation_points`

### 支持的形态

**看涨形态**：锤子线、吞没形态、刺透形态、启明星、三白兵、上升三法、仙人指路、老鸭头、蓄势

**看跌形态**：上吊线、流星线、看跌吞没、乌云盖顶、黄昏星、三黑鸦、下降三法、背离

**中性形态**：十字星、极度缩量

## 开发经验总结

### 数据展示问题处理原则

当用户要求修改前端展示内容时（例如删除特定文本），应遵循以下原则：

1. **优先在数据源头修改**：首先考虑在后端数据生成逻辑中修改，而不是在前端进行字符串匹配、替换或过滤。
2. **避免前端硬编码处理**：前端应尽量保持简洁，只负责展示后端提供的数据。复杂的字符串处理（如正则匹配删除）会增加维护成本，且可能因后端格式变化而失效。
3. **修改需彻底**：如果决定修改，应确保所有相关的地方都得到更新，避免遗漏。

### UI精简原则

当用户要求优化 Watchlist、Dashboard 等交易界面时，应优先遵循以下原则：

1. **首屏只放决策信息**：先展示信号、目标点位、触发条件、失效条件，再考虑解释性文案。
2. **不为“看起来完整”增加汇总层**：若汇总卡、统计条、概览区不直接帮助交易决策，应默认不加。
3. **点位优先于摘要**：支撑、压力、目标位等关键价位必须直接可见，不能被抽象总结替代。
4. **少一层就是更好**：能在原卡片内解决，不新增额外容器、概览区或折叠层。

**案例：删除 watchlist 中的“定位 通信网络”**
- **错误做法**：在前端 `Watchlist.vue` 中添加正则表达式，匹配并删除“定位 [标签]；”字符串。
- **正确做法**：直接在后端 `kline_patterns.py` 的 `get_professional_commentary_detailed` 函数中，移除生成 `decision_summary` 和 `observation_points` 时添加的定位信息。
- **原因**：前端处理是“治标不治本”，增加了代码复杂度，且当后端数据格式变化时容易失效。后端修改是根本解决方案，能保证所有使用该数据的地方都保持一致。

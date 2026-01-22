# 肥羊数据分析系统

一个基于 FastAPI + React 的肥羊数据采集、分析和可视化系统，支持肥羊搜索、热度榜、板块分析、资金流向、AI智能推荐等功能。

## 📋 目录

- [功能特性](#功能特性)
- [技术栈](#技术栈)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [数据库设计](#数据库设计)
- [API 文档](#api-文档)
- [模型老K（T10结构狙击者模型）](#模型老k)
- [定时任务](#定时任务)
- [部署说明](#部署说明)
- [环境配置](#环境配置)

## ✨ 功能特性

### 核心功能
- **肥羊搜索**：支持肥羊代码和名称搜索
- **肥羊日K数据**：查看肥羊历史K线数据
- **资金流向**：分析肥羊主力资金流向（主力、超大单、大单、中单、小单）
- **热度榜**：实时展示市场热门肥羊排名
- **热门板块**：基于虚拟板块聚合算法，智能匹配热门股到主板块
- **板块分析**：查看板块K线和板块内肥羊
- **板块资金流**：分析板块资金净流入，识别热门板块
- **概念管理**：管理肥羊概念和虚拟板块映射

### 模型老K（T10结构狙击者模型 v1.0）
- **智能推荐**：基于T10模型的结构化狙击推荐系统
  - 纯L1数据驱动：只使用价量数据，拒绝虚假资金流
  - 极致缩量识别：精准捕捉主力锁仓的"极致缩量"信号
  - 黄金坑回踩：识别主升浪中的缩量回调机会
  - 阴线低吸：优先推荐收跌个股，降低追高风险
- **回测功能**：时光机逻辑，验证策略历史表现
- **历史战绩**：自动保存和验证推荐记录
- **市场状态识别**：自动识别市场状态（进攻/防守/震荡）并调整策略

### 系统管理
- **用户认证**：JWT认证，支持登录日志和安全锁定
- **用户管理**：支持用户创建、更新、删除（仅管理员）
- **AI管理**：支持多AI模型配置和管理
- **数据管理**：数据完整性检查、缺失数据刷新、数据清理

## 🛠 技术栈

### 后端
- **框架**：FastAPI 0.104.1
- **数据库**：MySQL 8.0
- **ORM**：SQLAlchemy 2.0
- **数据源**：akshare（肥羊数据）
- **定时任务**：APScheduler 3.10
- **认证**：JWT (python-jose)
- **量化分析**：pandas, numpy, statsmodels, scipy

### 前端
- **框架**：React 18.2 + TypeScript
- **构建工具**：Vite 5.0
- **UI组件**：Ant Design 5.11
- **图表**：ECharts 5.4
- **路由**：React Router 6.20
- **Markdown渲染**：react-markdown

### 部署
- **容器化**：Docker + Docker Compose
- **Web服务器**：Nginx (前端)

## 📁 项目结构

```
/root/app/
├── backend/                    # 后端服务
│   ├── api/                    # API层：接口实现
│   │   ├── __init__.py
│   │   └── main.py            # FastAPI主应用
│   ├── auth/                   # 认证模块
│   │   ├── auth.py            # JWT认证逻辑
│   │   └── init_admin.py      # 初始化管理员
│   ├── db/                     # DB层：数据库操作
│   │   ├── __init__.py
│   │   ├── database.py        # 数据库连接管理
│   │   ├── sheep_repository.py
│   │   ├── money_flow_repository.py
│   │   ├── sector_money_flow_repository.py
│   │   ├── concept_repository.py
│   │   ├── hot_rank_repository.py
│   │   ├── index_repository.py
│   │   ├── ai_config_repository.py
│   │   ├── ai_model_config_repository.py
│   │   ├── ai_cache_repository.py
│   │   ├── strategy_recommendation_repository.py

│   ├── etl/                    # ETL层：数据采集适配器
│   │   ├── __init__.py
│   │   ├── trade_date_adapter.py
│   │   ├── sheep_adapter.py
│   │   ├── concept_adapter.py
│   │   ├── concept_filter.py
│   │   ├── hot_rank_adapter.py
│   │   ├── index_adapter.py
│   │   └── sector_money_flow_adapter.py
│   ├── services/               # Service层：业务逻辑
│   │   ├── __init__.py
│   │   ├── data_collection_service.py
│   │   ├── sheep_service.py
│   │   ├── hot_rank_service.py
│   │   ├── concept_service.py
│   │   ├── concept_management_service.py
│   │   ├── sector_matching_service.py
│   │   ├── sector_money_flow_service.py
│   │   ├── user_service.py
│   │   ├── ai_service.py
│   │   ├── alpha_model_t4.py
│   │   ├── alpha_model_t6_resonance.py
│   │   ├── alpha_model_t10.py
│   │   └── backtest_engine.py
│   ├── scripts/                # 维护脚本
│   │   └── check_data_gaps.py # 数据完整性检查工具
│   ├── config.py              # 配置文件
│   ├── scheduler.py            # 定时任务调度器
│   ├── requirements.txt        # Python依赖
│   ├── Dockerfile              # Docker配置
│   └── PERFORMANCE_OPTIMIZATION.md  # 性能优化文档
├── frontend/                   # 前端应用
│   ├── src/
│   │   ├── api/               # API客户端
│   │   ├── components/        # 组件
│   │   ├── contexts/          # Context
│   │   ├── pages/             # 页面
│   │   └── main.tsx           # 入口文件
│   ├── package.json
│   ├── vite.config.ts
│   └── Dockerfile
├── database/
│   └── init.sql               # 数据库初始化脚本
├── docs/                      # 文档目录
│   ├── model-k-usage.md      # 模型老K使用指南
│   ├── SECTOR_RESONANCE_EXPLAIN.md  # 板块共振说明
│   └── RSRS_MARKET_REGIME.md  # RSRS市场状态识别说明
├── docker-compose.yml         # Docker Compose配置
├── deploy.sh                  # 一键部署脚本
└── README.md                  # 本文档
```

## 🚀 快速开始

### 前置要求

- Docker 和 Docker Compose
- 或 Python 3.11+ 和 Node.js 18+

### 使用 Docker Compose（推荐）

```bash
# 克隆项目
cd /root/app

# 一键部署
chmod +x deploy.sh
./deploy.sh

# 或手动部署
docker-compose up -d --build
```

访问地址：
- 前端：http://localhost
- 后端API：http://localhost:8000
- API文档：http://localhost:8000/docs

默认管理员账号：
- 用户名：`admin`
- 密码：`admin123`（首次登录后请修改）

### 手动部署

#### 后端

```bash
cd backend
pip install -r requirements.txt

# 配置环境变量（创建.env文件）
export DB_HOST=localhost
export DB_USER=admin
export DB_PASSWORD=admin
export DB_NAME=stock

# 启动服务
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

#### 前端

```bash
cd frontend
npm install
npm run dev
```

## 🗄 数据库设计

### 核心表结构

1. **sheep_basic** - 肥羊基本信息表
2. **sheep_daily** - 肥羊交易日数据表
3. **sheep_money_flow** - 肥羊资金流向表
4. **concept_theme** - 概念主题表
5. **stock_concept_mapping** - 肥羊与概念关联关系表（多对多）
6. **virtual_board_aggregation** - 虚拟板块聚合表
7. **market_hot_rank** - 市场热度排名表
8. **sector_money_flow** - 板块资金流向表
9. **market_index_daily** - 大盘指数日K数据表
10. **users** - 用户表
11. **login_logs** - 登录日志表
12. **ai_config** - AI配置表
13. **ai_model_config** - AI模型配置表
14. **ai_cache** - AI分析缓存表
15. **strategy_recommendation** - 策略推荐记录表

详细表结构见 `database/init.sql`

## 📡 API 文档

### 认证相关

#### 用户登录
```http
POST /api/auth/login
Content-Type: application/json

{
  "username": "admin",
  "password": "admin123"
}
```

响应：
```json
{
  "access_token": "eyJ...",
  "token_type": "bearer",
  "username": "admin"
}
```

#### 获取当前用户信息
```http
GET /api/auth/me
Authorization: Bearer {token}
```

### 肥羊相关

#### 搜索肥羊
```http
GET /api/sheep/search?q=平安
```

#### 获取肥羊日K数据
```http
GET /api/sheep/{sheep_code}/daily
Authorization: Bearer {token}
```

#### 获取资金流向数据
```http
GET /api/sheep/{sheep_code}/capital-flow?days=60
Authorization: Bearer {token}
```

### 热度榜相关

#### 获取热度榜
```http
GET /api/hot-sheep?source=xueqiu
```

#### 获取热门板块
```http
GET /api/hot-sectors
```

### 板块相关

#### 获取板块K线
```http
GET /api/sectors/{sector_name}/daily
Authorization: Bearer {token}
```

#### 获取板块肥羊
```http
GET /api/sectors/{sector_name}/sheep
Authorization: Bearer {token}
```

#### 获取板块资金净流入推荐
```http
GET /api/sector-money-flow/recommend?days=1&limit=30
Authorization: Bearer {token}
```

### 模型老K相关

#### 获取智能推荐
```http
POST /api/model-k/recommend
Authorization: Bearer {token}
Content-Type: application/json

{
  "params": {
    "min_change_pct": 2.0,
    "max_change_pct": 9.5,
    "vol_threshold": 1.5,
    "rps_threshold": 80,
    "concept_boost": true,
    "ai_filter": true,
    "min_win_probability": 45
  },
  "trade_date": "2026-01-15",
  "top_n": 20
}
```

#### 执行回测
```http
POST /api/model-k/backtest
Authorization: Bearer {token}
Content-Type: application/json

{
  "start_date": "2025-01-01",
  "end_date": "2025-12-31",
  "params": {
    "min_change_pct": 2.0,
    "max_change_pct": 9.5,
    "vol_threshold": 1.5
  }
}
```

#### 获取推荐历史
```http
GET /api/model-k/history?run_date=2026-01-15&limit=100&offset=0
Authorization: Bearer {token}
```

### AI相关

#### AI推荐肥羊
```http
POST /api/ai/recommend-sheep
Authorization: Bearer {token}
Content-Type: application/json

{
  "model_name": "gpt-4"
}
```

#### AI分析肥羊
```http
POST /api/ai/analyze-sheep/{sheep_code}
Authorization: Bearer {token}
Content-Type: application/json

{
  "model_name": "gpt-4"
}
```

### 管理相关（仅管理员）

#### 手动触发全量数据采集
```http
POST /api/admin/collect-all-data
Authorization: Bearer {token}
```

#### 检查数据缺失
```http
GET /api/admin/check-data-gaps?days=30&data_type=all
Authorization: Bearer {token}
```

#### 刷新缺失数据
```http
POST /api/admin/refresh-missing-data
Authorization: Bearer {token}
Content-Type: application/json

{
  "data_type": "all",
  "days": 30
}
```

更多API文档请访问：http://localhost:8000/docs

## 🚀 模型老K（T10结构狙击者模型 v1.0）

### 功能概述

模型老K是一个基于T10结构狙击者模型v1.0的智能选股推荐系统，核心思想是：L1价量驱动 + 极致缩量识别 + 黄金坑回踩 + 阴线低吸。

**T10 v1.0 核心特性：**
1. **纯L1驱动**：只用OHLCV数据，不依赖易被操纵的资金流数据
2. **4层漏斗架构**：战场选择 -> 股性基因 -> 狙击形态 -> 筹码评分
3. **极致缩量**：捕捉成交量萎缩至MA5均量0.6倍以下的转折点
4. **黄金坑形态**：识别-3%至1%区间的缩量回调，回踩MA20支撑
5. **阴线低吸优先**：主动避开追涨，优先选择当日收跌的优质标的
6. **板块效应锚定**：个股必须有板块MA20向上的趋势作为掩护
7. **筹码评分引擎**：综合缩量分、换手健康分和RPS护盘分

### 快速开始

1. 进入"模型老K"页面
2. 调整策略参数（可选，默认参数已优化）
3. **选择推荐日期**（可选，留空使用最近交易日）
4. 点击"智能推荐 (Get Alpha)"按钮
5. 查看推荐结果和回测数据

### 核心功能

#### 智能推荐
- **实时推荐**：基于最新市场数据，动态生成推荐
- **历史日期推荐**：支持选择任意历史日期，查看该日期的推荐结果
- **自动日期处理**：自动处理非交易日，转换为最近的交易日
- **参数可调**：支持实时调整策略参数，立即生效

#### 回测验证
- **时光机逻辑**：使用历史数据模拟交易
- **多维度指标**：胜率、Alpha收益率、总收益率、最大回撤
- **收益曲线**：可视化展示策略历史表现

#### 历史战绩
- **自动保存**：每次推荐自动保存记录
- **自动验证**：推荐日期距离今天超过5个交易日自动验证
- **结果分析**：显示后5日最大涨幅、后5日涨幅、成功/失败标记

### 详细文档

- [模型老K使用指南](./docs/model-k-usage.md) - 完整的使用说明和参数配置
- [板块共振说明](./docs/SECTOR_RESONANCE_EXPLAIN.md) - 板块共振算法详解
- [RSRS市场状态识别](./docs/RSRS_MARKET_REGIME.md) - 市场状态识别算法说明

## ⏰ 定时任务

定时任务在 `backend/scheduler.py` 中配置，使用 APScheduler 实现：

### 自然日数据采集（每天18:00）
- **热度榜数据**：采集市场热门肥羊排名

### 交易日数据采集（每天15:00，仅在交易日）
- **肥羊日K数据**：采集肥羊历史K线数据
- **资金流向数据**：采集肥羊主力资金流向
- **板块资金流向数据**：采集板块资金流向
- **大盘指数数据**：采集中证1000指数数据（用于RSRS计算）

### 概念板块数据采集（每天03:00）
- **概念板块列表**：采集概念主题列表
- **肥羊-概念关联关系**：采集肥羊与概念的关联关系

### 板块资金流实时采集（交易时间每30分钟）
- **板块资金流数据**：交易时间（9:00-14:59）每30分钟刷新一次

### 数据清理任务（每天04:00）
- **资金流数据清理**：保留最近3年数据
- **板块资金流数据清理**：保留最近3个月数据

### 补偿检查任务（每小时，15:00-23:59）
- **错过任务检查**：检查并触发错过的数据采集任务

## 🚢 部署说明

### Docker Compose 部署（推荐）

1. **准备环境**
   ```bash
   # 确保Docker和Docker Compose已安装
   docker --version
   docker-compose --version
   ```

2. **配置数据目录**
   ```bash
   # 创建MySQL数据目录（可选，docker-compose会自动创建）
   mkdir -p /data/mysql/{data,conf,logs}
   ```

3. **一键部署**
   ```bash
   chmod +x deploy.sh
   ./deploy.sh
   ```

4. **查看服务状态**
   ```bash
   docker-compose ps
   docker-compose logs -f
   ```

### 生产环境建议

1. **使用环境变量**：不要在代码中硬编码敏感信息
2. **配置HTTPS**：使用Nginx反向代理配置SSL证书
3. **数据库备份**：定期备份MySQL数据
4. **日志管理**：配置日志轮转和监控
5. **资源限制**：为Docker容器设置资源限制

## ⚙️ 环境配置

### 后端环境变量

创建 `backend/.env` 文件：

```env
# 数据库配置
DB_HOST=localhost
DB_PORT=3306
DB_USER=admin
DB_PASSWORD=admin
DB_NAME=stock

# JWT配置
JWT_SECRET_KEY=your-secret-key-change-in-production

# 数据保留天数
STOCK_DATA_RETENTION_DAYS=1095  # 3年
SECTOR_DATA_RETENTION_DAYS=90   # 3个月

# 定时任务配置
DATA_UPDATE_HOUR=18
DATA_UPDATE_MINUTE=0
```

### 前端环境变量

创建 `frontend/.env` 文件：

```env
VITE_API_BASE_URL=http://localhost:8000
```

### 数据库初始化

数据库初始化脚本位于 `database/init.sql`，Docker Compose 会自动执行。

首次部署后，系统会自动创建管理员用户：
- 用户名：`admin`
- 密码：`admin123`（首次登录后请修改）

## 📝 代码分层说明

### ETL层（数据采集适配器）
- **职责**：封装akshare接口，负责类型转换和异常处理
- **特点**：与数据源解耦，便于切换数据源

### DB层（数据仓储）
- **职责**：数据库操作，包括CRUD操作
- **特点**：封装SQL语句，提供统一的数据访问接口

### Service层（业务逻辑）
- **职责**：处理业务逻辑，包括交易日判断、数据采集调度等
- **特点**：协调ETL层和DB层，实现业务功能

### API层（接口实现）
- **职责**：为前端提供RESTful API接口
- **特点**：处理HTTP请求，调用Service层完成业务逻辑

## 🔧 开发指南

### 后端开发

```bash
cd backend
pip install -r requirements.txt

# 运行开发服务器（支持热重载）
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
```

### 前端开发

```bash
cd frontend
npm install
npm run dev
```

### 代码规范

- 后端：遵循 PEP 8 Python 代码规范
- 前端：使用 ESLint 和 Prettier 格式化代码
- 提交信息：使用清晰的提交信息

## 📚 文档

- [模型老K使用指南](./docs/model-k-usage.md) - T10模型详细使用说明
- [板块共振说明](./docs/SECTOR_RESONANCE_EXPLAIN.md) - 板块共振算法详解
- [RSRS市场状态识别](./docs/RSRS_MARKET_REGIME.md) - 市场状态识别算法说明
- [性能优化文档](./backend/PERFORMANCE_OPTIMIZATION.md) - 后端性能优化说明

## 📄 许可证

本项目仅供学习和研究使用。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📞 联系方式

如有问题，请提交 Issue 或联系项目维护者。

---

**最后更新**：2026-01-15

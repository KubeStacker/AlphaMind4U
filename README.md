# 股票数据分析系统

一个基于 FastAPI + React 的股票数据采集、分析和可视化系统，支持股票搜索、热度榜、板块分析、资金流向等功能。

## 📋 目录

- [功能特性](#功能特性)
- [技术栈](#技术栈)
- [项目结构](#项目结构)
- [快速开始](#快速开始)
- [数据库设计](#数据库设计)
- [API 文档](#api-文档)
- [板块匹配算法](#板块匹配算法)
- [定时任务](#定时任务)
- [部署说明](#部署说明)
- [环境配置](#环境配置)

## ✨ 功能特性

- **股票搜索**：支持股票代码和名称搜索
- **股票日K数据**：查看股票历史K线数据
- **资金流向**：分析股票主力资金流向
- **热度榜**：实时展示市场热门股票排名
- **热门板块**：基于虚拟板块聚合算法，智能匹配热门股到主板块
- **板块分析**：查看板块K线和板块内股票
- **概念管理**：管理股票概念和虚拟板块映射
- **用户认证**：JWT认证，支持登录日志和安全锁定

## 🛠 技术栈

### 后端
- **框架**：FastAPI 0.104.1
- **数据库**：MySQL 8.0
- **ORM**：SQLAlchemy 2.0
- **数据源**：akshare（股票数据）
- **定时任务**：APScheduler 3.10
- **认证**：JWT (python-jose)

### 前端
- **框架**：React 18.2 + TypeScript
- **构建工具**：Vite 5.0
- **UI组件**：Ant Design 5.11
- **图表**：ECharts 5.4
- **路由**：React Router 6.20

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
│   │   ├── stock_repository.py
│   │   ├── money_flow_repository.py
│   │   ├── concept_repository.py
│   │   ├── hot_rank_repository.py
│   │   └── virtual_board_repository.py
│   ├── etl/                    # ETL层：数据采集适配器
│   │   ├── __init__.py
│   │   ├── trade_date_adapter.py
│   │   ├── stock_adapter.py
│   │   ├── concept_adapter.py
│   │   ├── concept_filter.py
│   │   └── hot_rank_adapter.py
│   ├── services/               # Service层：业务逻辑
│   │   ├── __init__.py
│   │   ├── data_collection_service.py
│   │   ├── stock_service.py
│   │   ├── hot_rank_service.py
│   │   ├── concept_service.py
│   │   ├── concept_management_service.py
│   │   └── sector_matching_service.py
│   ├── config.py              # 配置文件
│   ├── scheduler.py            # 定时任务调度器
│   ├── requirements.txt        # Python依赖
│   └── Dockerfile              # Docker配置
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

1. **stock_basic** - 股票基本信息表
2. **stock_daily** - 股票交易日数据表
3. **concept_theme** - 概念主题表
4. **stock_concept_mapping** - 股票与概念关联关系表（多对多）
5. **virtual_board_aggregation** - 虚拟板块聚合表
6. **market_hot_rank** - 市场热度排名表
7. **stock_money_flow** - 股票资金流向表
8. **users** - 用户表
9. **login_logs** - 登录日志表

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

#### 登出
```http
POST /api/auth/logout
Authorization: Bearer {token}
```

### 股票相关

#### 搜索股票
```http
GET /api/stocks/search?q=平安
```

#### 获取股票日K数据
```http
GET /api/stocks/{stock_code}/daily
Authorization: Bearer {token}
```

#### 获取资金流向数据
```http
GET /api/stocks/{stock_code}/capital-flow
Authorization: Bearer {token}
```

### 热度榜相关

#### 获取热度榜
```http
GET /api/hot-stocks?source=xueqiu
```

#### 获取热门板块
```http
GET /api/hot-sectors
```

响应示例：
```json
{
  "sectors": [
    {
      "sector_name": "AI应用",
      "hot_score": 11,
      "color": "red",
      "hot_stocks": [
        {
          "stock_code": "000001",
          "stock_name": "平安银行",
          "rank": 1
        }
      ]
    }
  ]
}
```

#### 刷新热度榜
```http
POST /api/refresh-hot-stocks
Authorization: Bearer {token}
```

### 板块相关

#### 获取板块K线
```http
GET /api/sectors/{sector_name}/daily
Authorization: Bearer {token}
```

#### 获取板块股票
```http
GET /api/sectors/{sector_name}/stocks
Authorization: Bearer {token}
```

### 概念管理

#### 获取概念列表
```http
GET /api/concepts?limit=100&offset=0
Authorization: Bearer {token}
```

#### 创建概念
```http
POST /api/concepts
Authorization: Bearer {token}
Content-Type: application/json

{
  "concept_name": "AI智能体",
  "concept_code": "THS001",
  "source": "ths",
  "description": "AI智能体概念"
}
```

#### 更新概念
```http
PUT /api/concepts/{concept_id}
Authorization: Bearer {token}
Content-Type: application/json

{
  "concept_name": "AI智能体",
  "is_active": true
}
```

#### 删除概念（软删除）
```http
DELETE /api/concepts/{concept_id}
Authorization: Bearer {token}
```

### 虚拟板块管理

#### 获取虚拟板块列表
```http
GET /api/virtual-boards
Authorization: Bearer {token}
```

#### 创建虚拟板块映射
```http
POST /api/virtual-boards/mappings
Authorization: Bearer {token}
Content-Type: application/json

{
  "virtual_board_name": "AI应用",
  "source_concept_name": "AI智能体",
  "weight": 1.0,
  "description": "AI应用板块"
}
```

#### 删除虚拟板块映射
```http
DELETE /api/virtual-boards/mappings?virtual_board_name=AI应用&source_concept_name=AI智能体
Authorization: Bearer {token}
```

#### 导入sector_mapping数据
```http
POST /api/virtual-boards/import-sector-mapping
Authorization: Bearer {token}
```

### 数据采集管理

#### 手动触发全量数据采集
```http
POST /api/admin/collect-all-data
Authorization: Bearer {token}
```

采集顺序：
1. 概念板块数据
2. 股票日K数据
3. 资金流向数据（仅在交易日）
4. 热度榜数据

## 🧮 板块匹配算法

### 算法概述

实现了基于**虚拟板块聚合**的算法，将热门股的细分概念匹配到主板块。算法核心思想：**通过virtual_board_aggregation表，将细分概念（如AI智能体、AIGC概念）聚合到主板块（如AI应用）**。

### 数据表结构

#### virtual_board_aggregation表
- `virtual_board_name`: 主板块名称（如"AI应用"）
- `source_concept_name`: 细分概念名称（如"AI智能体"、"AIGC概念"等）
- `weight`: 聚合权重
- `is_active`: 是否有效

**映射关系**：细分概念 -> 主板块（一对多，一个细分概念可能属于多个主板块）

### 算法流程

#### 1. 数据准备
- 获取热门股票列表（top100）
- 批量获取所有热门股的概念（单次查询优化）
- 加载虚拟板块映射关系（从virtual_board_aggregation表）

#### 2. 概念到主板块映射

对每个热门股：
1. **获取股票的所有概念**：从`stock_concept_mapping`表获取（细分概念）
2. **概念到主板块映射**：通过`virtual_board_aggregation`表，将细分概念映射到主板块
   - 一个细分概念可能对应多个主板块
   - 例如："AI智能体" -> ["AI应用", "人工智能"]

#### 3. 主板块匹配决策

对每个股票，根据匹配到的主板块数量进行决策：

**情况1：只匹配到一个主板块**
- 直接归到这个主板块

**情况2：匹配到多个主板块**
- 统计每个主板块在top100中包含的股票数量
- 选择包含最多热门股的主板块
- **原理**：板块越热（包含的股票越多），该股票越可能属于这个板块

#### 4. 板块聚合

将所有股票按主板块聚合：
- 统计每个主板块下的股票列表
- 记录每个主板块的最佳排名（排名最好的股票）

#### 5. 板块排序

按以下规则排序：
1. **股票数量**（降序）：板块下热门股数量
2. **最佳排名**（升序）：板块中排名最好的股票

#### 6. 颜色标识

- **第1名板块**：红色（red）
- **第2名板块**：橙色（orange）
- **其他板块**：蓝色（blue）

### 性能优化

1. **批量查询优化**：使用单次SQL查询获取所有热门股的概念，避免N+1查询问题
2. **缓存机制**：虚拟板块映射关系缓存（单例模式），减少重复数据库查询
3. **算法复杂度**：
   - 时间复杂度：O(N × M)，N为股票数，M为平均概念数
   - 空间复杂度：O(N + S)，S为板块数
   - 实际运行：100只股票，平均每个股票5个概念，耗时<100ms

### 数据过滤

算法会自动过滤无意义的概念：
- 技术性板块（融资融券、转融券标的等）
- 指数类板块（沪深300、中证500等）
- 地域性板块（江苏板块、浙江板块等）
- 其他无意义分类

详见：`backend/etl/concept_filter.py`

### 算法特点

1. **虚拟板块聚合**：将细分概念聚合到主板块，符合用户习惯
2. **唯一匹配**：每个股票只匹配一个主板块，避免重叠
3. **集聚效应决策**：当股票匹配到多个主板块时，选择包含最多热门股的主板块
4. **性能优化**：批量查询，单例缓存，高效算法
5. **自动过滤**：自动过滤无意义的概念和板块

## ⏰ 定时任务

定时任务在 `backend/scheduler.py` 中配置，使用 APScheduler 实现：

### 自然日数据采集（每天18:00）
- **热度榜数据**：采集市场热门股票排名

### 交易日数据采集（每天20:00，仅在交易日）
- **股票日K数据**：采集股票历史K线数据
- **资金流向数据**：采集股票主力资金流向

### 概念板块数据采集（每天03:00）
- **概念板块列表**：采集概念主题列表
- **股票-概念关联关系**：采集股票与概念的关联关系

### 配置说明

定时任务配置在 `backend/config.py` 中：
```python
DATA_UPDATE_HOUR = 18  # 每天18点更新数据
DATA_UPDATE_MINUTE = 0
```

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

### 单独部署

#### 后端部署

```bash
cd backend
docker build -t stock-backend .
docker run -d --name stock-backend \
  --network host \
  -e DB_HOST=localhost \
  -e DB_USER=admin \
  -e DB_PASSWORD=admin \
  -e DB_NAME=stock \
  -p 8000:8000 \
  stock-backend
```

#### 前端部署

```bash
cd frontend
docker build -t stock-frontend .
docker run -d --name stock-frontend \
  -p 80:80 \
  stock-frontend
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
STOCK_DATA_RETENTION_DAYS=90
SECTOR_DATA_RETENTION_DAYS=10

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

## 📄 许可证

本项目仅供学习和研究使用。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📞 联系方式

如有问题，请提交 Issue 或联系项目维护者。

---

**最后更新**：2024年

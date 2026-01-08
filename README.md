# 股票数据分析系统

一个基于akshare的股票数据采集、分析和展示系统，支持K线图展示、热度榜分析、板块聚类和智能推荐。

## 功能特性

1. **股票数据采集**
   - 自动采集最近3个月的所有A股日K数据（开、收、高、低、量、均线等）
   - 采集主力资金流入数据
   - 采集板块量价数据（最近10天）
   - 每天18点自动更新数据
   - 自动清理过期数据

2. **股票分析（Tab1）**
   - 股票搜索和选择
   - 最近3个月K线图展示（支持MA5/10/20/30/60均线）
   - 主力资金流入情况展示

3. **热度榜单（Tab2）**
   - 雪球和东财热度榜数据展示
   - 基于热度榜前100的板块聚类
   - 最热门5个板块展示（点击查看板块K线和板块内股票）
   - 支持手动刷新数据
   - 显示连续上榜天数

4. **智能推荐（Tab3，默认页面）**
   - 结合涨幅榜和热度榜的热门板块推荐
   - 热门股票推荐
   - 资金持续5天正流入股票推荐

## 技术栈

- **前端**: React 18 + TypeScript + Vite + Ant Design + ECharts
- **后端**: Python 3.11 + FastAPI + SQLAlchemy
- **数据库**: MySQL 8.0
- **数据源**: akshare
- **部署**: Docker + Docker Compose

## 目录结构

```
app/
├── frontend/          # 前端代码
│   ├── src/
│   │   ├── pages/     # 页面组件
│   │   ├── components/# 公共组件
│   │   └── api/       # API接口
│   ├── Dockerfile
│   └── package.json
├── backend/           # 后端代码
│   ├── main.py        # FastAPI主应用
│   ├── data_collector.py  # 数据采集模块
│   ├── scheduler.py   # 定时任务
│   ├── database.py    # 数据库连接
│   ├── config.py      # 配置文件
│   ├── Dockerfile
│   └── requirements.txt
├── database/          # 数据库脚本
│   └── init.sql      # 初始化SQL
├── deploy/           # 部署脚本
│   ├── deploy.sh     # 一键部署脚本
│   └── mysql-docker.sh  # MySQL单独部署脚本
└── docker-compose.yml # Docker Compose配置
```

## 快速开始

### 方式一：一键部署（推荐）

```bash
cd /root/app
./deploy/deploy.sh
```

### 方式二：手动部署

1. **启动MySQL数据库**

```bash
./deploy/mysql-docker.sh
```

等待30秒后，MySQL服务启动完成。

2. **启动后端服务**

```bash
cd backend
docker build -t stock-backend .
docker run -d --name stock-backend \
  --network host \
  -e DB_HOST=localhost \
  -e DB_USER=admin \
  -e DB_PASSWORD=admin \
  -e DB_NAME=stock \
  stock-backend
```

3. **启动前端服务**

```bash
cd frontend
docker build -t stock-frontend .
docker run -d --name stock-frontend \
  -p 80:80 \
  --network host \
  stock-frontend
```

### 方式三：使用Docker Compose（推荐）

```bash
cd /root/app

# 创建数据目录
mkdir -p /data/mysql/data /data/mysql/conf /data/mysql/logs

# 启动所有服务
docker-compose up -d --build

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down
```

## 数据库配置

- **数据库名**: stock
- **用户名**: admin
- **密码**: admin
- **管理员密码**: password
- **数据存储路径**: /data/mysql/data

## 访问地址

- **前端页面**: http://localhost 或 http://服务器IP
- **后端API**: http://localhost:8000
- **API文档**: http://localhost:8000/docs

## 数据采集

系统会在每天18:00自动采集数据。也可以手动触发：

```bash
# 进入后端容器
docker exec -it stock-backend bash

# 手动执行数据采集
python -c "from data_collector import DataCollector; c = DataCollector(); c.collect_stock_daily_data(); c.close()"
```

## API接口

### 股票相关
- `GET /api/stocks/search?q={query}` - 搜索股票
- `GET /api/stocks/{stock_code}/daily` - 获取股票日K数据
- `GET /api/stocks/{stock_code}/capital-flow` - 获取资金流入数据

### 热度榜相关
- `GET /api/hot-stocks?source={source}` - 获取热度榜
- `GET /api/hot-sectors` - 获取热门板块
- `GET /api/sectors/{sector_name}/daily` - 获取板块K线
- `GET /api/sectors/{sector_name}/stocks` - 获取板块股票
- `POST /api/refresh-hot-stocks` - 刷新热度榜

### 推荐相关
- `GET /api/recommendations` - 获取推荐数据

详细API文档请访问: http://localhost:8000/docs

## 注意事项

1. **数据采集时间**: 首次部署后，数据采集需要一定时间，建议等待数据采集完成后再使用
2. **akshare限制**: akshare可能有请求频率限制，如果遇到问题请适当调整采集间隔
3. **数据存储**: 确保 `/data/mysql/data` 目录有足够的磁盘空间
4. **网络要求**: 需要能够访问akshare数据源

## 故障排查

### 查看服务日志
```bash
docker-compose logs -f [service_name]
```

### 重启服务
```bash
docker-compose restart [service_name]
```

### 重新构建
```bash
docker-compose up -d --build
```

## 开发

### 前端开发
```bash
cd frontend
npm install
npm run dev
```

### 后端开发
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --reload
```

## 许可证

MIT License

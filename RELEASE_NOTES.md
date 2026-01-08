# 版本发布说明

## v1.0.1 (2026-01-08)

### 主要功能
- ✅ 标的日K数据展示（支持MA5/10/20/30/60均线）
- ✅ 热度榜数据展示（雪球、东财）
- ✅ 智能推荐功能（热门板块、热门标的、资金流入）
- ✅ 板块聚类分析
- ✅ 资金流入分析

### 技术特性
- 前端：React 18 + TypeScript + Ant Design + ECharts
- 后端：FastAPI + Python 3.11
- 数据库：MySQL 8.0
- 数据源：akshare
- 部署：Docker + Docker Compose

### 已知问题
- 标的数据需要手动触发采集（首次部署后）
- 东财热度榜数据可能不完整（依赖akshare接口）

### 部署说明
详见 `DEPLOYMENT.md`

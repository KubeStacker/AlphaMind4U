# AGENTS.md - Jarvis-Quant 项目约束规范

> 定义 AI 智能体在 Jarvis-Quant 项目中的工作约束和技能使用方式。

## 项目概述

A股量化交易决策系统，FastAPI + Vue 3，DuckDB 嵌入式数据库。

## 核心约束

### 数据库
- DuckDB 单进程模型，通过 `/admin/db/query` API 查询
- 禁止直接 `duckdb.connect()` 打开 jarvis.duckdb

### 数据源
- 优先 Tushare Pro，当前 Token 2000 积分
- 实现前确认接口所需积分

### 前端规范
- 克制设计，禁止紫色/靛蓝色调（purple-*, indigo-*, violet-*）
- 低饱和冷暖分区：情绪区偏暖、主线区偏冷

### 验证方式
- 后端：curl 调用 API（如 `curl http://localhost:8000/admin/db/query`）
- 前端：浏览器访问容器端口，禁止 `npm run build` 验证

## 技能目录

`skills/` 包含项目专属技能，可 softlink 到 CLI：
```bash
ln -s /root/jarvis/docs/spec/skills/kline-pattern-recognizer ~/.config/opencode/superpowers/skills/
```

## 导航
- `concepts/` - 项目约束详解
- `skills/` - 项目专属技能
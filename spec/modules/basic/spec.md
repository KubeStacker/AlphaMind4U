# 基础设置 (Basic)

## 概述

涵盖用户管理、认证、AI 配置、K 线图组件、在线文档等跨模块基础设施。对应 Settings 页面的 users/AI/db 三个 tab，以及全局共享的登录页和 K 线图组件。

## 前端

### 页面/组件

| 文件 | 说明 |
|------|------|
| `frontend/src/views/Settings.vue` | Settings 主页面 — 包含 users/AI/data/db 四个 tab |
| `frontend/src/views/Login.vue` | 登录页（独立页面，无 AppLayout） |
| `frontend/src/composables/useKlineChart.js` | ECharts K线图（Watchlist 和 Strategy 共享） |
| `frontend/src/stores/auth.js` | Pinia 认证状态管理 |
| `frontend/src/services/api.js` | Axios API 层 |
| `frontend/src/router/index.js` | 路由配置 + 全局 auth guard |

### Settings Tab 说明

| Tab | 权限 | 功能 |
|-----|------|------|
| users | 管理员 | 用户 CRUD（创建/删除/改密） |
| AI | 所有用户 | AI 模型配置（OpenAI/DeepSeek/Gemini）、prompt 模板 CRUD |
| data | 管理员 | 数据同步（见 04-data-sync） |
| db | 管理员 | SQL 查询控制台 |

### 核心交互

- **AI 配置**：双层存储结构 — `user_ai_provider_configs` 保存各 provider 配置，`user_ai_config` 镜像当前选中 provider
- **切换 provider**：直接使用 `provider_configs` 回显历史配置，不互相覆盖
- **K线图**：ECharts 实现，标注单位（vol=手，amount=千元），Watchlist 和 Strategy 共享同一组件和 `/admin/stock/{ts_code}/kline` 数据链

## 后端

### API 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/auth/token` | POST | 登录获取 JWT token |
| `/admin/users` | GET | 用户列表 |
| `/admin/users` | POST | 创建用户 |
| `/admin/users/{user_id}` | DELETE | 删除用户 |
| `/admin/users/password` | PUT | 修改密码 |
| `/admin/users/me/ai-config` | GET | 获取 AI 配置（含 provider_configs） |
| `/admin/users/me/ai-config` | PUT | 更新 AI 配置（仅当前 provider） |
| `/admin/users/me/prompt-templates` | GET/POST | 提示模板管理 |
| `/admin/users/me/prompt-templates/{id}` | PUT/DELETE | 单个模板操作 |
| `/admin/users/me/selected-template` | GET/PUT | 获取/更新选中模板 |
| `/admin/docs/list` | GET | 文档列表 |
| `/admin/docs/{doc_id}` | GET | 文档内容 |
| `/admin/docs/{doc_id}/progress` | GET/POST | 文档阅读进度 |
| `/admin/docs/tags` | GET/POST | 用户标签管理 |
| `/admin/docs/{doc_id}/tags` | GET/POST | 文档标签关联 |
| `/admin/docs/{doc_id}/notes` | GET/POST | 文档笔记 |
| `/admin/docs/notes/all` | GET | 所有笔记汇总 |
| `/admin/system/status` | GET | 市场状态（交易/休市） |
| `/admin/system/db_check` | GET | 数据库检查 |
| `/` | GET | 健康检查 |

### 核心模块

| 模块 | 说明 |
|------|------|
| `api/auth.py` | JWT 认证（OAuth2 密码流） |
| `api/routes/users.py` | 用户管理、AI 配置、持仓 |
| `api/routes/ai.py` | AI 分析、prompt 模板 |
| `api/routes/docs.py` | 在线文档 |
| `api/routes/system.py` | 系统状态 |

### AI 配置（双层存储）

```
user_ai_provider_configs  → 各 provider 历史配置（user_id + provider 复合主键）
user_ai_config            → 当前选中 provider 的镜像（兼容旧调用方）
```

切换 provider 不互相覆盖，Settings 回显用 `provider_configs`。

### 数据库表

- `users` — 用户表
- `user_ai_config` — 当前 AI 配置镜像
- `user_ai_provider_configs` — 按 provider 独立保存
- `user_prompt_templates` — prompt 模板
- `ai_analysis_cache` — AI 分析缓存（15分钟 TTL）
- `doc_reading_progress` — 阅读进度
- `doc_user_tags` — 用户标签
- `doc_notes` — 文档笔记
- `doc_tag_mapping` — 标签关联

## 约束与注意事项

- 登录认证使用 OAuth2 密码流，返回 JWT access token
- 测试账号：yuanpeng / 1qaz2wsx；jwt_token / jwt_token
- K线图组件标注成交量(手)和成交额(千元)单位
- AI 配置修改不再假设一行 `user_ai_config` 代表全部 provider 状态
- prompt 模板中 `{sector_context}` / `{market_context}` / `{market_sentiment}` / `{mainline}` 旧占位符已废弃

# AGENTS.md — Jarvis-Quant 项目约束规范

> AI 智能体在 Jarvis-Quant 中的工作约束和规范驱动开发流程。

## 项目概述

A股量化交易决策系统，FastAPI + Vue 3，DuckDB 嵌入式数据库。5 个模块对应前端 tab。

## 规范驱动开发

**任何功能变更必须遵循 spec-first 流程：**

1. 阅读对应模块 spec（`modules/` 目录）
2. 使用 `skills/spec-writer` 技能澄清意图、起草规范
3. 用户确认规范后实施代码
4. 实施完成后更新 spec

### 模块 Spec

| 模块 | 目录 |
|------|------|
| 盯盘 | [modules/watchlist/](modules/watchlist/spec.md) |
| 仪表盘 | [modules/dashboard/](modules/dashboard/spec.md) |
| 策略广场 | [modules/strategy/](modules/strategy/spec.md) |
| 数据同步 | [modules/data-sync/](modules/data-sync/spec.md) |
| 基础设置 | [modules/basic/](modules/basic/spec.md) |

每个模块目录包含 `spec.md`（前后端完整视图），后续可扩展子文档。

## 跨切面约束

见 `concepts/`：
- [01-core-constraints](concepts/01-core-constraints.md) — DuckDB、Tushare、容器化
- [02-frontend-guidelines](concepts/02-frontend-guidelines.md) — 克制设计、色彩规范
- [03-backend-patterns](concepts/03-backend-patterns.md) — FastAPI 模式、ETL 结构

## 技能目录

`skills/` 位于项目根目录，含 6 个项目专属技能，可 softlink 到 Claude Code。

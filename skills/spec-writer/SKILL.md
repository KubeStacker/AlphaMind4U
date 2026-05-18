# 规范编写技能 (Spec Writer)

## 描述

当用户提出功能需求或修改请求时，先澄清意图、起草规范、写入对应模块 spec，经用户确认后再实施。

## 触发条件

用户消息中包含以下任一关键词时触发：
- "新增" / "添加" / "修改" / "删除" / "优化" + 功能描述
- "帮我" / "写一个" / "实现" + 功能
- "改一下" / "调整" / "重构" + 模块
- 明确提到某个模块名（盯盘/Watchlist/仪表盘/Dashboard/策略广场/Strategy/数据同步/Data Sync/基础设置/Basic）

## 工作流程

### 1. 澄清意图

提出问题确认：
- **范围**：影响哪个模块？（watchlist / dashboard / strategy / data-sync / basic）
- **前端**：UI 变化？新组件？修改现有页面？新的交互流程？
- **后端**：新 API？修改现有 API？新增 ETL 任务？数据库变更？
- **约束**：是否影响其他模块？有什么特殊限制？

使用 `AskUserQuestion` 工具，问题不超过 3 个。

### 2. 起草规范

基于用户回答，在对应的模块 spec 文件中追加或修改内容（或新建模块 spec）：

- 新建功能：在模块 spec 中新增子章节，描述前端和后端变更
- 修改功能：更新模块 spec 中对应章节
- 新建模块：在 `docs/spec/modules/` 下创建新文件，并在 `AGENTS.md` 中注册

规范草稿格式：
```markdown
## <功能名> (待确认)

**状态**: 草稿

### 前端
- 页面/组件：
- 交互流程：
- 状态/数据流：

### 后端
- API 端点：
- 模块路径：
- 数据库变更：

### 约束
```

### 3. 确认后实施

将草稿内容展示给用户确认。用户批准后：
- 将"草稿"状态改为当前日期（如 `2026-05-09 确认`）
- 开始实施代码变更
- 实施完成后移除状态标记

## 模块文件映射

| 模块 | Spec 文件 |
|------|-----------|
| 盯盘 / Watchlist / tab1 | `docs/spec/modules/01-watchlist.md` |
| 仪表盘 / Dashboard / tab2 / 复盘 | `docs/spec/modules/02-dashboard.md` |
| 策略广场 / Strategy / tab3 | `docs/spec/modules/03-strategy.md` |
| 数据同步 / Data Sync / 数据管理 | `docs/spec/modules/04-data-sync.md` |
| 基础设置 / Basic / Settings / 用户/AI/K线 | `docs/spec/modules/05-basic.md` |

## 注意事项

- 单个需求可能涉及多个模块，需同时更新多个 spec 文件
- 规范文档是事实来源（source of truth），实施必须与规范一致
- 如果用户说"直接改不用写 spec"，跳过规范编写直接实施
- 跨切面约束（DuckDB、Tushare、配色等）记录在 `concepts/` 中，不重复写入模块 spec

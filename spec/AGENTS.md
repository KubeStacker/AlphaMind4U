# Jarvis 规范

项目仅包含两个有效模块：

| 模块 | Spec |
|------|------|
| 原始数据采集 | [modules/data-sync/spec.md](modules/data-sync/spec.md) |
| MCP 数据库读取 | [modules/mcp/spec.md](modules/mcp/spec.md) |

任何功能变更继续遵循 spec-first：阅读对应 spec，使用
`ai/skills/spec-writer` 起草，获得用户确认后实施并更新文档。

跨切面约束：

- [concepts/01-core-constraints.md](concepts/01-core-constraints.md)
- [concepts/03-backend-patterns.md](concepts/03-backend-patterns.md)

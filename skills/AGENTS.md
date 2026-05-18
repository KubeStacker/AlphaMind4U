# skills/ — 项目专属技能

项目专属 AI 智能体技能，可通过 softlink 链接到各 CLI 的 skills 目录。

## 技能列表

| 技能 | 目录 | 说明 |
|------|------|------|
| 规范编写 | `spec-writer/` | 澄清意图、起草规范、确认后实施 |
| Tushare数据获取 | `tushare-data-fetcher/` | 接口封装、积分检查、重试逻辑 |
| DuckDB查询优化 | `duckdb-query-optimizer/` | 连接管理、批量导入、临时视图清理 |

## 安装到 Claude Code

```bash
for skill in /root/jarvis/skills/*/; do
  name=$(basename "$skill")
  [ "$name" = "AGENTS.md" ] && continue
  ln -sf "$skill" ~/.claude/skills/"$name"
done
```

## 技能结构

每个技能目录包含：
- `SKILL.md` — 技能入口（描述、触发条件、调用方式、注意事项）

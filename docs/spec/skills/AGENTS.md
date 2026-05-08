# skills/ - 项目专属技能

本目录包含 Jarvis-Quant 项目专属的 AI 智能体技能，可通过 softlink 链接到各 CLI 的 skills 目录。

## 技能列表

| 技能 | 目录 | 说明 |
|------|------|------|
| K线形态识别 | `kline-pattern-recognizer/` | 向量化检测、置信度评分、回测 |
| 市场情绪分析 | `sentiment-analyzer/` | 情绪历史、自动补算、实时叠加 |
| 策略广场 | `strategy-plaza/` | 头7龙回头、单阳不破、大眼睛策略 |
| Tushare数据获取 | `tushare-data-fetcher/` | 接口封装、积分检查、重试逻辑 |
| DuckDB查询优化 | `duckdb-query-optimizer/` | 连接管理、批量导入、临时视图清理 |

## Softlink 使用方式

### 链接到 opencode

```bash
ln -s /root/jarvis/docs/spec/skills/kline-pattern-recognizer ~/.config/opencode/superpowers/skills/
```

### 链接到其他 CLI（如支持）

```bash
ln -s /root/jarvis/docs/spec/skills/kline-pattern-recognizer ~/.claude/skills/
```

## 技能结构

每个技能目录包含：
- `SKILL.md` - 技能入口（描述、场景、调用方式、注意事项）
- `references/` - 参考资料（可选）

## 添加新技能

1. 在 `skills/` 下创建新目录
2. 创建 `SKILL.md` 文件
3. 更新本文件的技能列表
4. Commit 变更

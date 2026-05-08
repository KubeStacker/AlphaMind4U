# Jarvis-Quant Harness 规范

本项目在 `docs/spec/` 目录下建立了 Harness 工程约束文档和项目专属技能目录。

## 目录结构

| 路径 | 说明 |
|------|------|
| `AGENTS.md` | 入口文件：项目约束规范 |
| `concepts/` | 项目约束详解（4篇） |
| `skills/` | 项目专属技能（5个） |

## 快速开始

1. 阅读 `AGENTS.md` 了解核心约束
2. 深入 `concepts/` 查看详细规范
3. 使用 `skills/` 中的项目专属技能

## 技能使用

技能可通过 softlink 链接到各 CLI 的 skills 目录：

```bash
ln -s /root/jarvis/docs/spec/skills/kline-pattern-recognizer ~/.config/opencode/superpowers/skills/
```

## 参考

- [Harness Engineering 学习指南](https://github.com/deusyu/harness-engineering)
- [Jarvis-Quant 项目 AGENTS.md](../AGENTS.md)

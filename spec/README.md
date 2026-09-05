# spec/README — spec 管理方法与索引

> spec 按“域目录”组织，不用扁平文件堆。全局只留三份，其余按域归位。

## 1. 目录规则（一套管理方法）

1. 一域一目录：`backend/`、`frontend/`、`ops/`、`strategy/`；后端一服务一子目录。
   `strategy/` 集中量化策略研究计划，运行时仍归属 quant-backend，不新增服务边界。
2. 每个目录必有 `README.md` 做索引（文件列表 + 一句话 + 端口/归属）。
3. 单文件 ≤150 行，超了就拆；新主题开新文件，禁往旧文件无限制追加。
4. 跨域内容只写在产生域，另一域给链接，不复制。
5. 全局文件仅三份：`README.md`（本方法）、`architecture.md`（目标树地图）、
   `sdd.md`（工作流程）。`database`、`ports`、`ops` 片段归各服务目录，
   全局注册表（端口总表）放 `backend/ports.md`。

## 2. 索引

```text
spec/
  README.md  architecture.md  sdd.md
  backend/
    README.md  ports.md
    quant/{README,api,etl,database,publish,ops}.md
    mcp/{README,proxy}.md
    _template/{README,checklist}.md
  frontend/{README,style,routing}.md
  ops/{README,jarvis-sh,retention,drills,systemd}.md
  strategy/{README,plan,history}.md
  strategy/acceptance/ — 原始验收记录与证据
```

- [strategy/README.md](strategy/README.md) — 全A股上月成交额Top20统一方案、编号待办与历史证据。

## 3. 扩展示例

- 加后端：复制 `backend/_template/` 为 `backend/<name>/`，按其 checklist 落齐。
- 加主题：如 quant 加风控，在 `backend/quant/` 新建 `risk.md` 并登记到其 README。

# spec/sdd.md — SDD 流程、DoD、禁顺手改

## 1. 流程（vibe coding / Spec-Driven）

`spec → plan → implement → verify`。改功能先改 `spec/` + `shared/contracts`，
再写 `plan/PLAN-*.md`，再动代码。无 spec 的修改直接打回。

## 2. 上下文节流（省 token）

- 接任务只读：根 `AGENTS.md` + 对应 `services/*/AGENTS.md` + 对应一份 contract。
- 不全仓扫描；跨域改动必须拆多任务。

## 3. DoD（缺一不可）

1. 契约测试通过（datasets 白名单、query 限流、etl 任务名、错误码）。
2. `go vet ./...` + `npm run build`（涉前端）通过。
3. 新 SQL 有 `EXPLAIN` 说明；新表有迁移文件。
4. `jarvis.sh restart <svc>` 后健康正常：quant为`/`，mcp为`/health`，frontend为`/`。
5. spec 与 contract 已同步更新。

## 4. 禁顺手改

- diff 只含本任务 spec 范围；发现相邻坏味道只记 `TODO(<issue>)`，另起任务。
- 命名/注释不合规（缺中文注释、超 120 列）即打回。
- 删除历史包袱外的新删除必须先记入 `spec/architecture.md` 删除清单。

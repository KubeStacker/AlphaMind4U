# AGENTS.md — Jarvis 总纲（AI 必读）

> 目标：轻量、准确。Go 重构一步到位，不背历史包袱。
> 详规下沉 `spec/`，本文件只讲边界与命令，不贴代码。

## 1. 模块边界

| 模块 | 路径 | 语言 | 职责 |
|---|---|---|---|
| 量化后端 | `services/quant-backend/` | Go | 数据采集 ETL + 分析 + strategy，同域同库，唯一可碰量化库的进程 |
| 只读代理 | `services/mcp-server/` | Go | 无库，只调量化后端 HTTP，对外 MCP 协议 |
| 未来后端 | `services/<name>-backend/` | Go 优先 | 独立域独立库，复制 `_template-backend` 接入，禁直连他人库 |
| 前端 | `frontend/` | Vite+Vue3+TS | 单页面管多后端，按 URI 前缀路由 |
| 契约 | `shared/contracts/` | yaml/json | 前后端唯一真源，先改契约再改代码 |

铁律：禁跨服务 import；禁直连他人库文件（单写者独占）；改接口先改 `shared/contracts`。

## 2. 唯一命令

```sh
./jarvis.sh start|stop|restart|status|logs [quant|mcp|frontend|all]
```

- 日志：`data/logs/<svc>.log`；PID：`data/run/<svc>.pid`；二进制：各 `bin/`。
- 前端开发：`frontend/` 内 `npm i / npm run dev / npm run build`。

## 3. 文档索引（管理方法见 `spec/README.md`）

- `spec/architecture.md` — 目标树、域划分、删除清单（全局地图）
- `spec/sdd.md` — SDD 流程、DoD、禁顺手改（全局工作流）
- `spec/backend/` — 后端域：`README` 总则 + `ports.md` 总表 +
  `quant/{api,etl,database,publish,ops}` + `mcp/{proxy}` + `_template/{checklist}`
- `spec/frontend/` — `style`（方案/token/复用）+ `routing`（URI 映射/目录/数据）
- `spec/ops/` — `jarvis-sh`（命令/资源红线）+ `retention`（清理）+
  `drills`（演练/5日计划）+ `systemd`（自启模板）

## 4. 命名与注释

服务 `kebab-case`；Go 包小写短名；文件 `snake_case`；导出 `CamelCase`；DB/API 沿用 `snake_case` 冻结。每个导出符号一句中文注释，行宽 ≤120 列。

## 5. 接新后端

复制 `services/_template-backend/`，按 `spec/backend/ports.md` 领端口，
补 `openapi.yaml` + 本服务 `AGENTS.md`，前端加 `src/pages/<域>-*` + `src/api/<域>.ts`。

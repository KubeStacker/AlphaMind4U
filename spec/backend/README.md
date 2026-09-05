# spec/backend/README — 后端域总则

> 所有后端（quant/mcp/未来 N）的共享铁律与接入手册。服务细节看各自子目录。

## 1. 共享铁律

- 独立进程、独立端口、独立库文件；禁跨服务 import。
- 禁直连他人库文件（SQLite 单写者独占）；跨域读数只走 HTTP。
- 改接口先改 `shared/contracts`，再改代码。

## 2. 端口规则

- 总表唯一真源：`backend/ports.md`；先领号再开发，8002 起顺序分配。
- 各服务 README 必须声明本服务端口并链回总表。
- 健康接口统一 `GET /health → {status:"ok",service:"<kebab-name>"}`。

## 3. 接新后端（复制模板）

1. 复制 `backend/_template/` 为 `backend/<name>/`，服务名 `kebab-case`。
2. 按 `backend/ports.md` 领端口，登记总表。
3. 补 `shared/contracts/openapi-<name>.yaml` + 服务 `AGENTS.md`。
4. 前端加 `src/pages/<域>-*` + `src/api/<域>.ts`（见 `frontend/routing.md`）。
5. 过 `backend/_template/checklist.md` 后合入。

## 4. 索引

- `ports.md` — 端口注册表（总表）。
- `quant/` — 量化后端（采集 ETL + 分析 + strategy，:8000）。
- `mcp/` — 只读代理（:8081，无库）。
- `_template/` — 新后端脚手架与合入清单。

# spec/backend/ports — 端口注册表（唯一真源）

| 服务 | 端口 | 健康检查 | 说明 |
|---|---|---|---|
| quant-backend | 8000 | `GET / → {status,service}` | `/admin/datasets/*`、`/admin/etl/*` |
| mcp-server | 8081 | `GET /health` | `POST /mcp`，双 token 鉴权沿用 |
| frontend | 8080 | `GET /healthz` | `vite preview --port 8080` |
| docreader-backend（预留） | 8002 | `GET /health` | 未来第一个新后端 |
| 未来后端 N | 8003～8099 顺序分配 | `GET /health` | 先改此表再占端口 |

规则：`.env` 可覆写但必须同步改此表；健康接口返回体统一 `{status:"ok",service:"<kebab-name>"}`；
前端按 `spec/architecture.md` 前缀表反代，不硬编码 IP。

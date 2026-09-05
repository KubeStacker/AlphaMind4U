# spec/backend/mcp/proxy — 反代布局与鉴权

## 1. 布局（stdlib only）

```text
services/mcp-server/
  cmd/server/main.go
  internal/{proxy,auth}
  go.mod  AGENTS.md  bin/
```

## 2. 鉴权（语义沿用现 Python）

- `Authorization: Bearer <key>` 或 `x-mcp-api-key: <key>` 二选一。
- 对客 `<key>` 缺省回落到 `JARVIS_STATIC_AUTH_TOKEN`：单用户部署只需一个共享 token；
  如需独立对客 key，显式设置 `JARVIS_MCP_API_KEY`。
- `/health`、`/、OPTIONS` 放行，其余 401 关门。

## 3. 工具（原样反代 quant，不碰 DB）

- `list` → `GET /admin/datasets`；`describe` → `GET /admin/datasets/{name}/schema`；
  `query` → `POST /admin/datasets/query`（limit/offset 上下限与 quant 一致）。
- 后端错误原样透出状态码 + 前 500 字，不吞错。

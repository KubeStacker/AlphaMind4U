# spec/backend/mcp/README — 只读代理索引

> 无库服务，只调量化后端 HTTP，对外 MCP 协议。

- 端口：8081（总表见 `../ports.md`）；健康 `GET /health`。
- 启动依赖：quant `/health` 先就绪（`jarvis.sh` 保证顺序）。
- `proxy.md` — 反代布局、鉴权语义、三个工具定义。

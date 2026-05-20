# Jarvis MCP Server 接入 OpenClaw

Jarvis MCP Server 是独立部署的只读 MCP 服务，默认通过 Streamable HTTP 暴露：

- 公网地址: `http://117.72.45.245:8081/mcp`
- 健康检查: `http://117.72.45.245:8081/health`
- 鉴权: `Authorization: Bearer <JARVIS_MCP_API_KEY>` 或 `X-MCP-API-Key: <JARVIS_MCP_API_KEY>`

## 部署

1. 确认主 Jarvis 后端容器在运行，并且 `jarvis_jarvis_net` 网络存在。

```bash
docker compose ps
docker network ls | grep jarvis_jarvis_net
```

2. 设置 MCP 访问密钥。当前服务器已在项目根目录生成 `.env.mcp`，后续可自行轮换。

```bash
cat .env.mcp
```

如果 MCP 工具需要访问 Jarvis 后端里要求登录的接口，使用后端静态服务令牌。`jarvis_get_holdings` 和 `jarvis_get_realtime_quotes` 依赖当前用户上下文，当前配置为 `admin` 用户；`backend/.env` 和 MCP `.env.mcp` 必须使用同一个 `JARVIS_STATIC_AUTH_TOKEN`。

```bash
grep JARVIS_STATIC_AUTH_TOKEN backend/.env .env.mcp
grep JARVIS_STATIC_AUTH_USERNAME backend/.env
```

3. 启动独立 MCP 服务。

```bash
docker compose --env-file .env.mcp -f docker-compose.mcp.yml up -d --build
```

4. 验证健康状态。

```bash
curl http://127.0.0.1:8081/health
curl -H "Authorization: Bearer $(grep JARVIS_MCP_API_KEY .env.mcp | cut -d= -f2)" http://127.0.0.1:8081/mcp
```

## OpenClaw 配置示例

在本地 OpenClaw 的 MCP 配置中增加一个 Streamable HTTP server：

```json
{
  "mcpServers": {
    "jarvis-quant": {
      "type": "streamableHttp",
      "url": "http://117.72.45.245:8081/mcp",
      "headers": {
        "Authorization": "Bearer replace-with-your-jarvis-mcp-api-key"
      }
    }
  }
}
```

如果 OpenClaw 使用 `streamable-http` 作为类型名，则改成：

```json
{
  "mcpServers": {
    "jarvis-quant": {
      "type": "streamable-http",
      "url": "http://117.72.45.245:8081/mcp",
      "headers": {
        "Authorization": "Bearer replace-with-your-jarvis-mcp-api-key"
      }
    }
  }
}
```

## 当前开放工具

- `jarvis_search_stock`
- `jarvis_get_stock_kline`
- `jarvis_get_stock_indicators`
- `jarvis_get_market_suggestion`
- `jarvis_get_mainline_leaders`
- `jarvis_get_stock_mainline_analysis`
- `jarvis_get_holdings`
- `jarvis_get_realtime_quotes`
- `jarvis_get_strategy_plaza_strategies`
- `jarvis_get_strategy_plaza_summary`
- `jarvis_get_strategy_plaza_observations`

所有工具都是只读工具，并且限制了返回数量。MCP server 不开放任意 SQL、ETL、用户管理、系统维护和写入类接口。

## 端口说明

主 `docker-compose.yml` 已释放 `8081`，该端口由 MCP server 使用。Jarvis 后端容器内部仍监听 `8000`，MCP 容器通过 Docker 网络访问 `http://jarvis-backend:8000/admin`。

# services/mcp-server — 只读 MCP 代理（Go, stdlib only）

无库服务，只调量化后端 HTTP，对外最小可用 MCP over HTTP。端口 8081（总表见 `spec/backend/ports.md`）。

## 可读文件

- 可读：本目录全部（`cmd/server/main.go`、`internal/proxy/proxy.go`、`internal/auth/auth.go`）。
- 可调（HTTP 只读）：`GET {JARVIS_API_BASE_URL}/datasets`、
  `GET {JARVIS_API_BASE_URL}/datasets/{name}/schema`、`POST {JARVIS_API_BASE_URL}/datasets/query`。
- 只读参考（不改）：`spec/backend/mcp/*`。

## 禁区

- 禁直连任何 DB 文件；禁跨服务 import；禁碰 `services/mcp-server/` 之外目录。
- 禁写全量数据日志，只许启动/错误/采样汇总。禁提交二进制（`bin/` 已忽略）。

## 环境变量

- `JARVIS_API_BASE_URL` 默认 `http://127.0.0.1:8000/admin`；`MCP_HOST` 默认 `0.0.0.0`。
- `MCP_PORT` 默认 `8081`；`JARVIS_REQUEST_TIMEOUT` 默认 `20s`（支持 `20` 秒数或 Go duration）。
- `JARVIS_STATIC_AUTH_TOKEN` 必填（回源 quant 的 Bearer）；`JARVIS_MCP_API_KEY` 缺省回落到共享令牌（单用户单 token 即可），缺则启动失败。

## 本地命令

```sh
go vet ./...
go build -o bin/mcp-server ./cmd/server
JARVIS_MCP_API_KEY=k JARVIS_STATIC_AUTH_TOKEN=t JARVIS_API_BASE_URL=http://127.0.0.1:8000/admin ./bin/mcp-server
```

## 最小冒烟（curl，另起终端）

```sh
export MCP_BASE=http://127.0.0.1:8081 JARVIS_MCP_API_KEY=k
curl -s $MCP_BASE/health
curl -s $MCP_BASE/ && echo
curl -s -X POST $MCP_BASE/mcp | head -c 200; echo        # 无 key 也 401
curl -s -X POST $MCP_BASE/mcp -H "x-mcp-api-key: $JARVIS_MCP_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}'
curl -s -X POST $MCP_BASE/mcp -H "Authorization: Bearer $JARVIS_MCP_API_KEY" \
  -H 'Content-Type: application/json' -d @- <<'EOF'
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":
 {"name":"jarvis_query_dataset","arguments":{"dataset":"daily","limit":99999,"offset":-5}}}
EOF
# 预期：/health 200 且含 transport=streamable-http；无 key 的 POST /mcp 401 {"error":"unauthorized"}；
# tools/list 返回三个 jarvis_* 工具；query 的 limit/offset 被钳制后转发，后端错误透出原状态码+前500字。
```

## 全链路冒烟（含假 quant 回源鉴权）

```sh
python3 - <<'EOF' &
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def _ok(self, obj, code=200):
        b = json.dumps(obj).encode()
        self.send_response(code); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_GET(self):
        print("quant got", self.path, self.headers.get("Authorization"))
        self._ok({"datasets": ["daily"]})
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0)); body = self.rfile.read(n).decode()
        print("quant got POST", self.path, body[:200])
        self._ok({"echo": json.loads(body)})
    def log_message(self, *a): pass
HTTPServer(("127.0.0.1", 18099), H).serve_forever()
EOF
JARVIS_MCP_API_KEY=k JARVIS_STATIC_AUTH_TOKEN=t JARVIS_API_BASE_URL=http://127.0.0.1:18099 \
  MCP_PORT=18081 go run ./cmd/server &
sleep 1
export MCP_BASE=http://127.0.0.1:18081 JARVIS_MCP_API_KEY=k
curl -s $MCP_BASE/health; echo
curl -s $MCP_BASE/admin/datasets -H "x-mcp-api-key: $JARVIS_MCP_API_KEY"; echo
kill %1 %2 2>/dev/null; true
```

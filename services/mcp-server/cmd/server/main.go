package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"mcpserver/internal/auth"
	"mcpserver/internal/proxy"
)

// Version 是服务版本号，随代码发布。
const Version = "1.0.0"

// ServiceName 是健康检查与日志中的服务名，对齐旧版。
const ServiceName = "jarvis-data-mcp"

// Instructions 是 MCP initialize 下发的只读说明，对齐旧版。
const Instructions = "Read-only access to raw A-share datasets collected into Jarvis DuckDB. " +
	"The tools do not calculate indicators, scores, recommendations, or forecasts."

// Config 是服务启动配置，全部来自环境变量。
type Config struct {
	APIBaseURL  string
	StaticToken string
	APIKey      string
	Host        string
	Port        string
	Timeout     time.Duration
}

// LoadConfig 从环境变量加载配置，缺必填 Key 时返回错误。
func LoadConfig() (Config, error) {
	cfg := Config{
		APIBaseURL:  strings.TrimSuffix(strings.TrimSpace(envOr("JARVIS_API_BASE_URL", "http://127.0.0.1:8000/admin")), "/"),
		StaticToken: strings.TrimSpace(os.Getenv("JARVIS_STATIC_AUTH_TOKEN")),
		// 对客 Key 缺省回落到共享令牌：单用户控制台只用一个 token 即可。
		APIKey:      firstNonEmpty("JARVIS_MCP_API_KEY", "JARVIS_STATIC_AUTH_TOKEN"),
		Host:        envOr("MCP_HOST", "0.0.0.0"),
		Port:        envOr("MCP_PORT", "8081"),
		Timeout:     parseTimeout(envOr("JARVIS_REQUEST_TIMEOUT", "20s")),
	}
	if cfg.APIBaseURL == "" {
		cfg.APIBaseURL = "http://127.0.0.1:8000/admin"
	}
	if cfg.StaticToken == "" {
		return cfg, errMissing("JARVIS_STATIC_AUTH_TOKEN is required to access the backend")
	}
	if cfg.APIKey == "" {
		return cfg, errMissing("JARVIS_MCP_API_KEY or JARVIS_STATIC_AUTH_TOKEN is required")
	}
	return cfg, nil
}

// Server 是 HTTP 服务装配体，持有反代客户端与健康元信息。
type Server struct {
	proxy *proxy.Proxy
	cfg   Config
}

// NewServer 用配置装配反代客户端与路由依赖。
func NewServer(cfg Config) *Server {
	return &Server{cfg: cfg, proxy: proxy.New(proxy.Config{
		BaseURL: cfg.APIBaseURL, StaticToken: cfg.StaticToken, Timeout: cfg.Timeout,
	})}
}

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("ERROR mcp-server config: %v", err)
	}
	srv := NewServer(cfg)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", srv.handleHealth)
	mux.HandleFunc("GET /", srv.handleRoot)
	mux.HandleFunc("GET /admin/datasets", srv.handleList)
	mux.HandleFunc("GET /admin/datasets/{name}/schema", srv.handleDescribe)
	mux.HandleFunc("POST /admin/datasets/query", srv.handleQuery)
	mux.HandleFunc("POST /mcp", srv.handleMCP)
	handler := withOptionsPreflight(auth.New(cfg.APIKey).Middleware(mux))
	addr := cfg.Host + ":" + cfg.Port
	log.Printf("starting mcp-server on %s base=%s transport=streamable-http mcp_path=/mcp", addr, cfg.APIBaseURL)
	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("ERROR mcp-server serve: %v", err)
	}
}

// handleHealth 返回健康检查，对齐旧版字段。
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": ServiceName,
		"jarvis_api_base_url": s.cfg.APIBaseURL, "transport": "streamable-http", "mcp_path": "/mcp"})
}

// handleRoot 返回根路径提示，免鉴权仅供存活探测。
func (s *Server) handleRoot(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "service": ServiceName})
}

// handleList 反代 quant 的数据集列表。
func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	data, err := s.proxy.ListDatasets(r.Context())
	if err != nil {
		proxy.LogSummary("jarvis_list_datasets", "", err)
		writeUpstreamErr(w, err)
		return
	}
	proxy.LogSummary("jarvis_list_datasets", "", nil)
	writeJSON(w, http.StatusOK, data)
}

// handleDescribe 反代单个数据集的 schema。
func (s *Server) handleDescribe(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		name = strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/admin/datasets/"), "/schema")
	}
	data, err := s.proxy.DescribeDataset(r.Context(), name)
	if err != nil {
		proxy.LogSummary("jarvis_describe_dataset", name, err)
		writeUpstreamErr(w, err)
		return
	}
	proxy.LogSummary("jarvis_describe_dataset", name, nil)
	writeJSON(w, http.StatusOK, data)
}

// handleQuery 反代数据集查询，limit/offset 由 proxy 钳制。
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var payload map[string]any
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json body"})
		return
	}
	dataset, _ := payload["dataset"].(string)
	data, err := s.proxy.QueryDataset(r.Context(), payload)
	if err != nil {
		proxy.LogSummary("jarvis_query_dataset", dataset, err)
		writeUpstreamErr(w, err)
		return
	}
	proxy.LogSummary("jarvis_query_dataset", dataset, nil)
	writeJSON(w, http.StatusOK, data)
}

type rpcRequest struct {
	JSONRPC string           `json:"jsonrpc"`
	ID      *json.RawMessage `json:"id"`
	Method  string           `json:"method"`
	Params  json.RawMessage  `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// handleMCP 实现最小可用 MCP over HTTP，按 JSON-RPC 分发三个工具。
func (s *Server) handleMCP(w http.ResponseWriter, r *http.Request) {
	raw, ok := readBodyLimit(w, r, 1<<20)
	if !ok {
		return
	}
	if isBatch(raw) {
		var reqs []rpcRequest
		if err := json.Unmarshal(raw, &reqs); err != nil {
			writeJSON(w, http.StatusBadRequest, rpcErr(nil, -32700, "parse error"))
			return
		}
		resps := make([]any, 0, len(reqs))
		for _, req := range reqs {
			if resp, skip := s.dispatchOne(r.Context(), req); !skip {
				resps = append(resps, resp)
			}
		}
		writeJSON(w, http.StatusOK, resps)
		return
	}
	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, rpcErr(nil, -32700, "parse error"))
		return
	}
	resp, skip := s.dispatchOne(r.Context(), req)
	if skip {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) dispatchOne(ctx context.Context, req rpcRequest) (any, bool) {
	if req.ID == nil && strings.HasPrefix(req.Method, "notifications/") {
		return nil, true
	}
	switch req.Method {
	case "initialize":
		return rpcOK(req.ID, map[string]any{"protocolVersion": "2025-03-26",
			"capabilities": map[string]any{"tools": map[string]any{}},
			"serverInfo":   map[string]any{"name": "jarvis-data", "version": Version},
			"instructions": Instructions}), false
	case "ping":
		return rpcOK(req.ID, map[string]any{}), false
	case "tools/list":
		return rpcOK(req.ID, map[string]any{"tools": proxy.ListTools()}), false
	case "tools/call":
		var params struct {
			Name      string         `json:"name"`
			Arguments map[string]any `json:"arguments"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil || params.Name == "" {
			return rpcErr(req.ID, -32602, "invalid params: name is required"), false
		}
		dataset, _ := params.Arguments["dataset"].(string)
		data, err := s.proxy.DispatchCall(ctx, params.Name, params.Arguments)
		if err != nil {
			proxy.LogSummary(params.Name, dataset, err)
			return rpcErr(req.ID, rpcCode(err), proxy.TruncatePreview(err.Error())), false
		}
		proxy.LogSummary(params.Name, dataset, nil)
		text, _ := json.Marshal(data)
		return rpcOK(req.ID, map[string]any{"content": []any{
			map[string]any{"type": "text", "text": string(text)},
		}}), false
	default:
		if req.ID == nil {
			return nil, true
		}
		return rpcErr(req.ID, -32601, "method not found: "+req.Method), false
	}
}

func rpcOK(id *json.RawMessage, result any) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": idValue(id), "result": result}
}

func rpcErr(id *json.RawMessage, code int, msg string) map[string]any {
	return map[string]any{"jsonrpc": "2.0", "id": idValue(id),
		"error": rpcError{Code: code, Message: msg}}
}

func rpcCode(err error) int {
	if be, ok := err.(*proxy.BackendError); ok {
		_ = be
		return -32000
	}
	return -32602
}

func idValue(id *json.RawMessage) any {
	if id == nil {
		return nil
	}
	var v any
	if err := json.Unmarshal(*id, &v); err != nil {
		return nil
	}
	return v
}

func isBatch(raw []byte) bool {
	for _, c := range raw {
		if c == ' ' || c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		return c == '['
	}
	return false
}

func readBodyLimit(w http.ResponseWriter, r *http.Request, n int64) ([]byte, bool) {
	raw := http.MaxBytesReader(w, r.Body, n)
	var buf strings.Builder
	_ = buf
	data := make([]byte, 0, 4096)
	tmp := make([]byte, 4096)
	for {
		m, err := raw.Read(tmp)
		if m > 0 {
			data = append(data, tmp[:m]...)
		}
		if err != nil {
			break
		}
		if int64(len(data)) > n {
			writeJSON(w, http.StatusBadRequest, rpcErr(nil, -32700, "parse error"))
			return nil, false
		}
	}
	if len(data) == 0 {
		writeJSON(w, http.StatusBadRequest, rpcErr(nil, -32700, "parse error"))
		return nil, false
	}
	return data, true
}

func writeUpstreamErr(w http.ResponseWriter, err error) {
	if be, ok := err.(*proxy.BackendError); ok {
		writeJSON(w, be.StatusCode, map[string]any{"error": be.Error()})
		return
	}
	writeJSON(w, http.StatusBadGateway, map[string]any{"error": proxy.TruncatePreview(err.Error())})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func withOptionsPreflight(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func envOr(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// firstNonEmpty 取首个非空环境变量（去空格），用于 Key 回落合并。
func firstNonEmpty(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func parseTimeout(s string) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return 20 * time.Second
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(f * float64(time.Second))
	}
	return 20 * time.Second
}

type missingErr struct{ msg string }

func (e missingErr) Error() string { return e.msg }

func errMissing(msg string) error { return missingErr{msg: msg} }

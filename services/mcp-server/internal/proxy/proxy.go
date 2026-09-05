package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"time"
)

// MaxPreview 是后端错误透出时的最大字数（前 500 字）。
const MaxPreview = 500

// DefaultLimit 是 query 未传 limit 时的默认值。
const DefaultLimit = 100

// Config 是反代 quant 后端所需的配置。
type Config struct {
	BaseURL     string
	StaticToken string
	Timeout     time.Duration
}

// Proxy 是只读反代客户端，不碰 DB，只调 quant HTTP。
type Proxy struct {
	cfg    Config
	client *http.Client
}

// BackendError 是 quant 后端返回的错误，携带状态码与预览。
type BackendError struct {
	StatusCode int
	Preview    string
}

// Error 返回带状态码与预览的错误描述。
func (e *BackendError) Error() string {
	return fmt.Sprintf("Jarvis data API failed: %d %s", e.StatusCode, e.Preview)
}

// QueryArgs 是 query 工具的参数子集，仅做钳制用。
type QueryArgs struct {
	Limit  int
	Offset int
}

// Tool 是 MCP tools/list 下发的单个工具描述。
type Tool struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

// New 新建反代客户端，自动按超时构造 http.Client。
func New(cfg Config) *Proxy {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	return &Proxy{cfg: cfg, client: &http.Client{Timeout: timeout}}
}

// ClampLimit 将 limit 钳制到 1..1000，与旧版一致。
func ClampLimit(limit int) int {
	if limit < 1 {
		return 1
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

// ClampOffset 将 offset 钳制到 0..1000000，与旧版一致。
func ClampOffset(offset int) int {
	if offset < 0 {
		return 0
	}
	if offset > 1000000 {
		return 1000000
	}
	return offset
}

// TruncatePreview 按 rune 截断到前 500 字，用于错误透出。
func TruncatePreview(s string) string {
	runes := []rune(s)
	if len(runes) > MaxPreview {
		return string(runes[:MaxPreview])
	}
	return s
}

// ListTools 返回三个只读工具的 MCP 描述。
func ListTools() []Tool {
	return []Tool{
		{Name: "jarvis_list_datasets", Description: "List the raw collected datasets that may be queried.",
			InputSchema: map[string]any{"type": "object", "properties": map[string]any{}}},
		{Name: "jarvis_describe_dataset", Description: "Describe columns and types for one allowed raw dataset.",
			InputSchema: map[string]any{"type": "object",
				"properties": map[string]any{"dataset": map[string]any{"type": "string"}},
				"required":   []string{"dataset"}}},
		{Name: "jarvis_query_dataset",
			Description: "Query rows from one allowed raw dataset without SQL, joins, or aggregation.",
			InputSchema: map[string]any{"type": "object",
				"properties": map[string]any{"dataset": map[string]any{"type": "string"},
					"columns":    map[string]any{"type": "array"},
					"filters":    map[string]any{"type": "object"},
					"start_date": map[string]any{"type": "string"},
					"end_date":   map[string]any{"type": "string"},
					"order_by":   map[string]any{"type": "string"},
					"descending": map[string]any{"type": "boolean"},
					"limit":      map[string]any{"type": "integer"},
					"offset":     map[string]any{"type": "integer"}},
				"required": []string{"dataset"}}},
	}
}

// ListDatasets 反代 GET /admin/datasets，只读不碰库。
func (p *Proxy) ListDatasets(ctx context.Context) (any, error) {
	return p.do(ctx, http.MethodGet, "/datasets", nil)
}

// DescribeDataset 反代 GET /admin/datasets/{name}/schema，只读不碰库。
func (p *Proxy) DescribeDataset(ctx context.Context, dataset string) (any, error) {
	return p.do(ctx, http.MethodGet, "/datasets/"+url.PathEscape(dataset)+"/schema", nil)
}

// QueryDataset 反代 POST /admin/datasets/query，先钳制 limit/offset。
func (p *Proxy) QueryDataset(ctx context.Context, payload map[string]any) (any, error) {
	if payload == nil {
		payload = map[string]any{}
	}
	payload["limit"] = ClampLimit(toInt(payload["limit"], DefaultLimit))
	payload["offset"] = ClampOffset(toInt(payload["offset"], 0))
	if _, ok := payload["filters"]; !ok || payload["filters"] == nil {
		payload["filters"] = map[string]any{}
	}
	return p.do(ctx, http.MethodPost, "/datasets/query", payload)
}

// DispatchCall 按 MCP tools/call 的名称分发到三个只读工具。
func (p *Proxy) DispatchCall(ctx context.Context, name string, args map[string]any) (any, error) {
	if args == nil {
		args = map[string]any{}
	}
	switch name {
	case "jarvis_list_datasets":
		return p.ListDatasets(ctx)
	case "jarvis_describe_dataset":
		dataset, _ := args["dataset"].(string)
		if dataset == "" {
			return nil, fmt.Errorf("missing required argument: dataset")
		}
		return p.DescribeDataset(ctx, dataset)
	case "jarvis_query_dataset":
		dataset, _ := args["dataset"].(string)
		if dataset == "" {
			return nil, fmt.Errorf("missing required argument: dataset")
		}
		return p.QueryDataset(ctx, args)
	default:
		return nil, fmt.Errorf("unknown tool: %s", name)
	}
}

// LogSummary 只打采样汇总，不打全量数据。
func LogSummary(tool, dataset string, err error) {
	if err != nil {
		log.Printf("ERROR proxy tool=%s dataset=%s err=%v", tool, dataset, err)
		return
	}
	log.Printf("proxy tool=%s dataset=%s ok", tool, dataset)
}

func (p *Proxy) do(ctx context.Context, method, path string, payload map[string]any) (any, error) {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.cfg.BaseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+p.cfg.StaticToken)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, &BackendError{StatusCode: resp.StatusCode, Preview: TruncatePreview(string(raw))}
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("decode upstream json failed: %w", err)
	}
	return decoded, nil
}

func toInt(v any, fallback int) int {
	switch n := v.(type) {
	case int:
		return n
	case int64:
		return int(n)
	case float64:
		return int(n)
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return int(i)
		}
		return fallback
	case nil:
		return fallback
	default:
		return fallback
	}
}

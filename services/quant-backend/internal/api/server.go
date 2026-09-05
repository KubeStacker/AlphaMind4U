// Package api /admin HTTP 接口：数据集只读 + ETL 任务队列。
package api

import (
	"crypto/subtle"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"quantbackend/internal/db"
	"quantbackend/internal/engine"
)

// Server 接口服务器依赖。
type Server struct {
	strategyMu sync.Mutex // 串行研究入队的去重与额度检查。
	qualityMu  sync.RWMutex
	qualityAt  time.Time
	qualityBuf []byte
	DB         *db.DB
	Engine     *engine.Engine
	Token      string
	// AdminUser/AdminPassword 控制台单用户账号，未配置则登录停用。
	AdminUser     string
	AdminPassword string
}

// New 构造服务。
func New(d *db.DB, e *engine.Engine, token string) *Server {
	return &Server{DB: d, Engine: e, Token: strings.TrimSpace(token)}
}

// NewWithAdmin 带管理员账号构造服务。
func NewWithAdmin(d *db.DB, e *engine.Engine, token, adminUser, adminPassword string) *Server {
	s := New(d, e, token)
	s.AdminUser = strings.TrimSpace(adminUser)
	s.AdminPassword = adminPassword
	return s
}

// Handler 组装路由。
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", s.handleRoot)
	mux.HandleFunc("GET /admin/datasets", s.handleListDatasets)
	mux.HandleFunc("GET /admin/datasets/{name}/schema", s.handleDescribeDataset)
	mux.HandleFunc("POST /admin/datasets/query", s.handleQueryDataset)
	mux.HandleFunc("POST /admin/etl/sync", s.handleTriggerSync)
	mux.HandleFunc("GET /admin/tasks/status", s.handleTasksStatus)
	mux.HandleFunc("GET /admin/quality/coverage", s.handleQualityCoverage)
	mux.HandleFunc("POST /admin/strategy/backtests", s.handleRunBacktest)
	mux.HandleFunc("GET /admin/strategy/backtests", s.handleListBacktests)
	mux.HandleFunc("GET /admin/strategy/backtests/{id}", s.handleGetBacktest)
	mux.HandleFunc("DELETE /admin/strategy/backtests/{id}", s.handleCancelBacktest)
	mux.HandleFunc("GET /admin/strategy/pools", s.handleGetPool)
	mux.HandleFunc("POST /admin/auth/login", s.handleLogin)
	return s.withAuth(mux)
}

// withAuth /admin 下 Bearer 鉴权，其他放行；登录接口免鉴权。
func (s *Server) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions || !strings.HasPrefix(r.URL.Path, "/admin") ||
			r.URL.Path == "/admin/auth/login" {
			next.ServeHTTP(w, r)
			return
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || s.Token == "" {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"detail": "静态令牌无效或缺失"})
			return
		}
		if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(auth[7:])),
			[]byte(s.Token)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"detail": "静态令牌无效或缺失"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleRoot 健康检查，仅精确匹配 /，其余走 404。
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeJSON(w, http.StatusNotFound, map[string]string{"detail": "不存在的路径"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "quant-backend"})
}

// writeJSON 写 JSON 响应。
func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// decodeJSON 限 1MB 读 JSON 请求体。
func decodeJSON(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, 1<<20)
	return json.NewDecoder(r.Body).Decode(v)
}

// sanitize 清洗 Inf/NaN 为 nil（json 无法编码）。
func sanitize(v any) any {
	switch t := v.(type) {
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return nil
		}
		return t
	case []byte:
		return string(t)
	default:
		return v
	}
}

// nowStr 上海当前时间文本。
func nowStr() string {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	return time.Now().In(loc).Format("2006-01-02 15:04:05")
}

package auth

import (
	"net/http"
	"strings"
)

// Checker 是 MCP 公钥鉴权器，只认一个 API Key。
type Checker struct {
	APIKey string
}

// New 新建一个使用给定 API Key 的鉴权器。
func New(apiKey string) *Checker {
	return &Checker{APIKey: apiKey}
}

// ExtractKey 从请求头中提取 Bearer 或 x-mcp-api-key 的候选密钥。
func ExtractKey(r *http.Request) (bearer, headerKey string) {
	auth := r.Header.Get("Authorization")
	if len(auth) >= 7 && strings.HasPrefix(strings.ToLower(auth[:7]), "bearer ") {
		bearer = strings.TrimSpace(auth[7:])
	}
	headerKey = strings.TrimSpace(r.Header.Get("x-mcp-api-key"))
	return bearer, headerKey
}

// IsAuthorized 判断请求是否携带正确的 API Key，二选一即放行。
func (c *Checker) IsAuthorized(r *http.Request) bool {
	bearer, headerKey := ExtractKey(r)
	return c.APIKey != "" && (bearer == c.APIKey || headerKey == c.APIKey)
}

// IsOpenPath 判断是否为免鉴权路径，对齐旧版语义。
func IsOpenPath(r *http.Request) bool {
	if r.Method == http.MethodOptions {
		return true
	}
	return r.URL.Path == "/health" || r.URL.Path == "/"
}

// Middleware 是鉴权中间件，未授权时返回 401。
func (c *Checker) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if IsOpenPath(r) {
			next.ServeHTTP(w, r)
			return
		}
		if !c.IsAuthorized(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
			return
		}
		next.ServeHTTP(w, r)
	})
}

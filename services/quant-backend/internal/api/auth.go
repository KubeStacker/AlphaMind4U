// Auth 单用户控制台登录：用户名、密码、共享令牌三者同时匹配才通过。
package api

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// loginReq 登录请求体，对齐 contracts AuthLoginReq。
type loginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Token    string `json:"token"`
}

// match 常量时间比对，空期望值直接失败。
func match(got, want string) bool {
	if want == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// handleLogin 控制台登录；服务端未配齐账号时 503 明确拒绝。
func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	if s.AdminUser == "" || s.AdminPassword == "" || s.Token == "" {
		writeJSON(w, http.StatusServiceUnavailable,
			map[string]string{"detail": "服务端未配置管理员账号，请设置 JARVIS_ADMIN_USER/JARVIS_ADMIN_PASSWORD"})
		return
	}
	var req loginReq
	if err := decodeJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"detail": "请求体非法"})
		return
	}
	if strings.TrimSpace(req.Username) != s.AdminUser ||
		!match(req.Password, s.AdminPassword) ||
		!match(strings.TrimSpace(req.Token), s.Token) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"detail": "用户名或密码或令牌不正确"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"username": s.AdminUser, "token": s.Token})
}

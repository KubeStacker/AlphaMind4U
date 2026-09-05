// Auth 登录接口测试：三者匹配放行、任一 mismatch 401、未配置 503。
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"quantbackend/internal/calendar"
	"quantbackend/internal/db"
	"quantbackend/internal/engine"
	"quantbackend/internal/provider"
)

// testAdminServer 带管理员账号的测试服务。
func testAdminServer(t *testing.T) *Server {
	t.Helper()
	database, err := db.Open(":memory:")
	if err != nil {
		t.Skip("sqlite 不可用")
	}
	t.Cleanup(func() { database.Close() })
	if err := database.InitSchema(); err != nil {
		t.Fatal(err)
	}
	e := engine.New(database, provider.New("long", "", ""), calendar.New(database))
	return NewWithAdmin(database, e, "shared-token", "admin", "secret-pw")
}

// postLogin 调登录接口。
func postLogin(s *Server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/admin/auth/login", strings.NewReader(body))
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// TestLoginSuccess 三者匹配返回共用令牌。
func TestLoginSuccess(t *testing.T) {
	s := testAdminServer(t)
	rec := postLogin(s, `{"username":"admin","password":"secret-pw","token":"shared-token"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200 got %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Username string `json:"username"`
		Token    string `json:"token"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Username != "admin" || body.Token != "shared-token" {
		t.Fatalf("返回错误: %+v", body)
	}
}

// TestLoginMismatch 任一不匹配均 401。
func TestLoginMismatch(t *testing.T) {
	s := testAdminServer(t)
	cases := []string{
		`{"username":"root","password":"secret-pw","token":"shared-token"}`,
		`{"username":"admin","password":"wrong","token":"shared-token"}`,
		`{"username":"admin","password":"secret-pw","token":"other-token"}`,
		`{"username":"admin","password":"","token":"shared-token"}`,
		`not-json`,
	}
	for _, c := range cases {
		if rec := postLogin(s, c); rec.Code != http.StatusBadRequest && rec.Code != http.StatusUnauthorized {
			t.Fatalf("应 400/401: %s got %d", c, rec.Code)
		}
	}
}

// TestLoginUnconfigured 未配账号时 503。
func TestLoginUnconfigured(t *testing.T) {
	s := testServer(t)
	rec := postLogin(s, `{"username":"admin","password":"x","token":"test-token"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503 got %d", rec.Code)
	}
}

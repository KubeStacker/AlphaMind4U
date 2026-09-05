// 鉴权与参数钳制测试。
package auth

import (
	"net/http/httptest"
	"testing"
)

// TestIsAuthorized 双 key 任一通过即放行。
func TestIsAuthorized(t *testing.T) {
	c := New("secret")
	ok := httptest.NewRequest("POST", "/mcp", nil)
	ok.Header.Set("Authorization", "Bearer secret")
	if !c.IsAuthorized(ok) {
		t.Fatalf("Bearer 应放行")
	}
	ok2 := httptest.NewRequest("POST", "/mcp", nil)
	ok2.Header.Set("x-mcp-api-key", "secret")
	if !c.IsAuthorized(ok2) {
		t.Fatalf("x-mcp-api-key 应放行")
	}
	bad := httptest.NewRequest("POST", "/mcp", nil)
	bad.Header.Set("Authorization", "Bearer wrong")
	if c.IsAuthorized(bad) {
		t.Fatalf("错误 key 应拒绝")
	}
}

// TestIsOpenPath 健康与根路径放行。
func TestIsOpenPath(t *testing.T) {
	for _, p := range []string{"/health", "/"} {
		r := httptest.NewRequest("GET", p, nil)
		if !IsOpenPath(r) {
			t.Fatalf("%s 应放行", p)
		}
	}
	r := httptest.NewRequest("POST", "/mcp", nil)
	if IsOpenPath(r) {
		t.Fatalf("/mcp 不应放行")
	}
}

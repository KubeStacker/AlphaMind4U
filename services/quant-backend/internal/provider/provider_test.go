// Provider 错误分类与日期工具测试。
package provider

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestClassifyRemoteError 远端错误必须分级正确，分钟限流绝不能记日限额。
func TestClassifyRemoteError(t *testing.T) {
	cases := []struct {
		msg  string
		want errKind
	}{
		{"抱歉，您每天最多访问5000次", errQuota},
		{"抱歉，您每分钟最多访问60次", errThrottle},
		{"接口过快，请稍后再试", errThrottle},
		{"请求频繁", errThrottle},
		{"无权限访问该接口", errPermission},
		{"auth failed", errPermission},
		{"系统繁忙", errFatal},
	}
	for _, c := range cases {
		if got := classifyRemoteError(c.msg); got != c.want {
			t.Errorf("classifyRemoteError(%q)=%v want %v", c.msg, got, c.want)
		}
	}
}

// mockTushare 模拟远端，按调用次数返回预设响应。
func mockTushare(t *testing.T, bodies []string) (*Provider, *int32) {
	t.Helper()
	var n int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i := int(atomic.AddInt32(&n, 1)) - 1
		if i >= len(bodies) {
			i = len(bodies) - 1
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(bodies[i]))
	}))
	t.Cleanup(srv.Close)
	p := New("long", "", "x")
	p.baseURL = srv.URL
	p.client = srv.Client()
	return p, &n
}

// TestQuotaSkipAndReset 日限额当天跳过（nil,nil）并标记，再次调用不再打远端。
func TestQuotaSkipAndReset(t *testing.T) {
	ok := `{"code":0,"msg":"","data":{"fields":["a"],"items":[[1]]}}`
	quota := `{"code":502,"msg":"抱歉，您每天最多访问5000次"}`
	p, n := mockTushare(t, []string{quota, ok})
	rows, err := p.Moneyflow("2026-09-04")
	if err != nil || rows != nil {
		t.Fatalf("限额应跳过: %v %v", rows, err)
	}
	rows, err = p.Moneyflow("2026-09-04")
	if err != nil || rows != nil {
		t.Fatalf("标记后应直接跳过: %v %v", rows, err)
	}
	if atomic.LoadInt32(n) != 1 {
		t.Fatalf("标记后不应再请求远端，实际 %d 次", *n)
	}
}

// TestThrottleRetry 分钟限流必须重试而不是记日限额。
func TestThrottleRetry(t *testing.T) {
	throttle := `{"code":503,"msg":"抱歉，您每分钟最多访问60次"}`
	ok := `{"code":0,"msg":"","data":{"fields":["a"],"items":[[1]]}}`
	p, n := mockTushare(t, []string{throttle, ok})
	rows, err := p.Moneyflow("2026-09-04")
	if err != nil || len(rows) != 1 {
		t.Fatalf("限流后应重试成功: %v %v", rows, err)
	}
	if atomic.LoadInt32(n) != 2 {
		t.Fatalf("应请求 2 次，实际 %d", *n)
	}
	if day, marked := p.limited["moneyflow"]; marked {
		t.Fatalf("限流不应记日限额: %s", day)
	}
}

// TestPermissionError 无权限必须返回哨兵错误，供上游记 SKIPPED。
func TestPermissionError(t *testing.T) {
	deny := `{"code":2002,"msg":"无权限访问该接口"}`
	p, _ := mockTushare(t, []string{deny})
	_, err := p.MarginDetail("2026-09-04")
	var perm *PermissionError
	if !errors.As(err, &perm) {
		t.Fatalf("应为 PermissionError，got %v", err)
	}
}

// TestQuotaReset 昨天标记今天必须失效，请求正常放行。
func TestQuotaReset(t *testing.T) {
	ok := `{"code":0,"msg":"","data":{"fields":["a"],"items":[[1]]}}`
	p, n := mockTushare(t, []string{ok})
	p.mu.Lock()
	p.limited["moneyflow"] = "2000-01-01"
	p.mu.Unlock()
	rows, err := p.Moneyflow("2026-09-04")
	if err != nil || len(rows) != 1 {
		t.Fatalf("旧标记应失效并放行: %v %v", rows, err)
	}
	if atomic.LoadInt32(n) != 1 {
		t.Fatalf("应请求远端 1 次，实际 %d", *n)
	}
}

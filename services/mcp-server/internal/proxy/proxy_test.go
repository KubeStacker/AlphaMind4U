// 参数钳制与工具清单测试。
package proxy

import "testing"

// TestClamp 越界 limit/offset 必须钳制。
func TestClamp(t *testing.T) {
	if ClampLimit(99999) != 1000 || ClampLimit(0) != 1 || ClampLimit(50) != 50 {
		t.Fatalf("limit 钳制异常")
	}
	if ClampOffset(-5) != 0 || ClampOffset(2000000) != 1000000 || ClampOffset(10) != 10 {
		t.Fatalf("offset 钳制异常")
	}
}

// TestListTools 三个只读工具必须存在。
func TestListTools(t *testing.T) {
	tools := ListTools()
	if len(tools) != 3 {
		t.Fatalf("want 3 tools got %d", len(tools))
	}
}

// Engine 季度工具与任务分发测试。
package engine

import (
	"strings"
	"testing"

	"quantbackend/internal/calendar"
	"quantbackend/internal/db"
	"quantbackend/internal/provider"
)

// TestRecentQuarters 季度序列倒序且格式正确。
func TestRecentQuarters(t *testing.T) {
	qs := recentQuarters(6)
	if len(qs) != 6 {
		t.Fatalf("want 6 got %d", len(qs))
	}
	for _, q := range qs {
		if len(q) != 8 {
			t.Fatalf("季度格式非法: %q", q)
		}
	}
	for i := 1; i < len(qs); i++ {
		if qs[i] >= qs[i-1] {
			t.Fatalf("非倒序: %v", qs)
		}
	}
}

// TestRunTaskUnknown 未知任务必须返回错误，不能静默成功。
func TestRunTaskUnknown(t *testing.T) {
	database, err := db.Open(":memory:")
	if err != nil {
		t.Skip("sqlite 不可用")
	}
	defer database.Close()
	e := New(database, provider.New("long", "", ""), calendar.New(database))
	if err := e.RunTask(SyncParams{Task: "不存在的任务"}); err == nil ||
		!strings.Contains(err.Error(), "不支持") {
		t.Fatalf("未知任务应报错，got %v", err)
	}
}

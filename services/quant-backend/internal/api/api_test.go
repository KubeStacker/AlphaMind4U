// API 白名单与鉴权测试（内存库，不碰生产库）。
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

// testServer 内存库测试服务。
func testServer(t *testing.T) *Server {
	t.Helper()
	database, err := db.Open(":memory:")
	if err != nil {
		t.Skip("sqlite 不可用")
	}
	t.Cleanup(func() { database.Close() })
	if err := database.InitSchema(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Migrate(); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(
		`INSERT INTO stock_basic (ts_code, name, list_status) VALUES ('000001.SZ','平安银行','L')`); err != nil {
		t.Fatal(err)
	}
	e := engine.New(database, provider.New("long", "", ""), calendar.New(database))
	return New(database, e, "test-token")
}

// TestAuth 无令牌必须 401。
func TestAuth(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest("GET", "/admin/datasets", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 got %d", rec.Code)
	}
}

// TestListDatasets 白名单列表可读。
func TestListDatasets(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest("GET", "/admin/datasets", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200 got %d", rec.Code)
	}
	var body struct {
		Datasets []struct {
			Name string `json:"name"`
		} `json:"datasets"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Datasets) != 22 {
		t.Fatalf("want 22 datasets got %d", len(body.Datasets))
	}
}

// TestUnknownDataset 非白名单表必须拒绝。
func TestUnknownDataset(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest("GET", "/admin/datasets/users/schema", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404 got %d", rec.Code)
	}
}

// TestQueryClamp limit 超限必须钳制到 1000。
func TestQueryClamp(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest("POST", "/admin/datasets/query",
		strings.NewReader(`{"dataset":"stock_basic","limit":99999}`))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200 got %d", rec.Code)
	}
	var body struct {
		Limit int `json:"limit"`
		Count int `json:"count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Limit != 1000 || body.Count != 1 {
		t.Fatalf("钳制失败 limit=%d count=%d", body.Limit, body.Count)
	}
}

// TestEnqueueDedupe 同参数重复入队复用 ID，完成后可再入队（续跑）。
func TestEnqueueDedupe(t *testing.T) {
	s := testServer(t)
	p := engine.SyncParams{Task: "price", Days: 3}
	id1, dup1, err := s.Enqueue(p)
	if err != nil || dup1 || id1 == "" {
		t.Fatalf("首次入队应成功: %v %v %s", err, dup1, id1)
	}
	id2, dup2, err := s.Enqueue(p)
	if err != nil || !dup2 || id2 != id1 {
		t.Fatalf("重复入队应复用: %v %v %s vs %s", err, dup2, id2, id1)
	}
	// 终态后同参可再入队，支持失败续跑。
	if _, err := s.DB.Exec(`UPDATE etl_tasks SET status='FAILED' WHERE task_id=?`, id1); err != nil {
		t.Fatal(err)
	}
	id3, dup3, err := s.Enqueue(p)
	if err != nil || dup3 || id3 == id1 {
		t.Fatalf("终态后应可新入队: %v %v", err, dup3)
	}
}

// TestTaskPattern 匹配模式必须精确命中归属任务。
//
// 回归：raw 字符串 `"%"` 为 3 字符，尾部多写一个引号会导致 LIKE 永不命中，
// 且单元测试与冒烟都曾因只看 task 变量而漏检。必须断言完整模式字节。
func TestTaskPattern(t *testing.T) {
	if got := taskPattern("price"); got != `%"task":"price"%` {
		t.Fatalf("模式字节非法: %q", got)
	}
	s := testServer(t)
	if _, _, err := s.Enqueue(engine.SyncParams{Task: "price", Days: 3}); err != nil {
		t.Fatal(err)
	}
	rows, err := s.DB.QueryRows(`SELECT task_id FROM etl_tasks WHERE params_json LIKE ?`,
		taskPattern("price"))
	if err != nil || len(rows) != 1 {
		t.Fatalf("模式应命中 1 行: %v %v", rows, err)
	}
}

// TestRootCatchAll 任意不存在路径必须 404，不能回健康成功。
func TestRootCatchAll(t *testing.T) {
	s := testServer(t)
	req := httptest.NewRequest("GET", "/不存在的路径", nil)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("want 404 got %d", rec.Code)
	}
}

// Strategy 接口测试：参数校验 + 内存库 S0 全链路，不碰生产库。
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"quantbackend/internal/db"
	"quantbackend/internal/strategy"
)

// seedS0 注入 2 股含预热期行情与 9 月池快照（日内波动 ±1% 内，不触发 3% 跳空取消）。
func seedS0(t *testing.T, s *Server) {
	t.Helper()
	calCols := []string{"exchange", "cal_date", "is_open"}
	calRows := make([]db.Row, 0, 100)
	tStart, _ := time.Parse("2006-01-02", "2026-04-01")
	var openDates []string
	for day := 0; day < 170; day++ {
		cur := tStart.AddDate(0, 0, day)
		isOpen := "1"
		if cur.Weekday() == time.Saturday || cur.Weekday() == time.Sunday {
			isOpen = "0"
		} else {
			openDates = append(openDates, cur.Format("2006-01-02"))
		}
		calRows = append(calRows, db.Row{"exchange": "SSE", "cal_date": cur.Format("2006-01-02"), "is_open": isOpen})
	}
	if err := s.DB.BulkUpsert("trade_calendar", calCols, []string{"exchange", "cal_date"}, calRows, 100); err != nil {
		t.Fatal(err)
	}
	cols := []string{"trade_date", "ts_code", "open", "high", "low", "close",
		"pre_close", "change", "pct_chg", "vol", "amount", "adj_factor"}
	rows := make([]db.Row, 0, len(openDates)*2)
	for _, d := range openDates {
		for _, code := range []string{"A", "B"} {
			rows = append(rows, db.Row{"trade_date": d, "ts_code": code, "open": 10.0, "high": 10.5,
				"low": 9.5, "close": 10.0, "pre_close": 10.0, "change": 0.0, "pct_chg": 0.0,
				"vol": 1000.0, "amount": 100.0, "adj_factor": 1.0})
		}
	}
	if err := s.DB.BulkUpsert("daily_price", cols, []string{"trade_date", "ts_code"}, rows, 100); err != nil {
		t.Fatal(err)
	}
	pool := strategy.PoolVersion{Month: "2026-09", RangeFrom: "2026-08-01", RangeTo: "2026-08-29",
		DatasetV: "vtest", Entries: []strategy.PoolEntry{
			{Rank: 1, TsCode: "A", Amount: 20000}, {Rank: 2, TsCode: "B", Amount: 10000},
		}}
	if err := strategy.SavePool(s.DB, pool); err != nil {
		t.Fatal(err)
	}
}

// postBacktest 调回测接口。
func postBacktest(s *Server, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/admin/strategy/backtests", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer test-token")
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// TestBacktestValidation 非法参数逐项 400。
func TestBacktestValidation(t *testing.T) {
	s := testServer(t)
	cases := []string{
		`{"version":"SX","effect_months":["2026-09"],"from":"2026-09-01","to":"2026-09-07"}`,
		`{"version":"S1","effect_months":[],"from":"2026-09-01","to":"2026-09-07"}`,
		`{"version":"S1","effect_months":["2026-09"],"from":"2026-09-07","to":"2026-09-01"}`,
		`{"version":"S1","effect_months":["2026-09"],"from":"2025-01-01","to":"2026-09-01"}`,
		`{"version":"S1","effect_months":["2026-9"],"from":"2026-09-01","to":"2026-09-07"}`,
		`not-json`,
	}
	for _, c := range cases {
		if rec := postBacktest(s, c); rec.Code != http.StatusBadRequest {
			t.Fatalf("应 400: %s got %d", c, rec.Code)
		}
	}
}

// TestBacktestS0HappyPath 内存库 S0 全链路：5 日序列、买入两只。
func TestBacktestS0HappyPath(t *testing.T) {
	s := testServer(t)
	seedS0(t, s)
	rec := postBacktest(s, `{"version":"S0","effect_months":["2026-09"],"from":"2026-09-01","to":"2026-09-07"}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("want 202 got %d: %s", rec.Code, rec.Body.String())
	}
	var enq struct {
		RunID string `json:"run_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &enq); err != nil {
		t.Fatal(err)
	}
	s.consumeBacktest(nil)

	getReq := httptest.NewRequest("GET", "/admin/strategy/backtests/"+enq.RunID, nil)
	getReq.Header.Set("Authorization", "Bearer test-token")
	getRec := httptest.NewRecorder()
	s.Handler().ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK {
		t.Fatalf("want 200 got %d: %s", getRec.Code, getRec.Body.String())
	}
	var job struct {
		Status string `json:"status"`
		Error  string `json:"error"`
		Result struct {
			Points int `json:"points"`
			Trades int `json:"trades"`
			Series []struct {
				Date string  `json:"date"`
				NAV  float64 `json:"nav"`
			} `json:"series"`
			Funnel      map[string]any   `json:"funnel"`
			OpenIntents []map[string]any `json:"open_intents"`
		} `json:"result"`
	}
	if err := json.Unmarshal(getRec.Body.Bytes(), &job); err != nil {
		t.Fatal(err)
	}
	if job.Status != "COMPLETED" {
		t.Fatalf("want COMPLETED got %s, err=%s", job.Status, job.Error)
	}
	body := job.Result
	if body.Points != 5 || body.Trades != 2 {
		t.Fatalf("点数/成交错误: %+v", body)
	}
	if body.OpenIntents == nil {
		t.Fatalf("open_intents 应为数组（可空数组），不得缺字段")
	}
	fmt.Printf("s0 nav末=%.0f\n", body.Series[len(body.Series)-1].NAV)
}

// TestGetPool 快照查询：有则 200，无则 404，月份非法 400。
func TestGetPool(t *testing.T) {
	s := testServer(t)
	seedS0(t, s)
	get := func(month string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "/admin/strategy/pools?month="+month, nil)
		req.Header.Set("Authorization", "Bearer test-token")
		rec := httptest.NewRecorder()
		s.Handler().ServeHTTP(rec, req)
		return rec
	}
	if rec := get("2026-09"); rec.Code != http.StatusOK {
		t.Fatalf("want 200 got %d", rec.Code)
	}
	if rec := get("2026-10"); rec.Code != http.StatusNotFound {
		t.Fatalf("want 404 got %d", rec.Code)
	}
	if rec := get("2026-13"); rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400 got %d", rec.Code)
	}
}

// 行情加载单测：复权归一、单位换算、并集日期，不碰真实 DB。
package strategy

import (
	"testing"

	"quantbackend/internal/db"
)

// TestLoadStockBars 复权归一到末因子：除权日前价格折半，金额千元转元。
func TestLoadStockBars(t *testing.T) {
	q := &fakeQ{bars: []db.Row{
		{"trade_date": "2026-08-01", "open": 20.0, "high": 21.0, "low": 19.0, "close": 20.0,
			"amount": 5.0, "adj_factor": 1.0},
		{"trade_date": "2026-08-04", "open": 10.0, "high": 10.5, "low": 9.5, "close": 10.0,
			"amount": 6.0, "adj_factor": 2.0},
	}}
	got, err := LoadStockBars(q, []string{"A"}, "2026-08-01", "2026-08-31", 800)
	if err != nil {
		t.Fatalf("加载失败: %v", err)
	}
	pm := got["A"]
	if pm["2026-08-01"].Close != 20.0 || pm["2026-08-01"].AdjFactor != 1.0 {
		t.Fatalf("应保留原始价格与因子: %+v", pm["2026-08-01"])
	}
	if pm["2026-08-04"].Close != 10.0 || pm["2026-08-04"].AdjFactor != 2.0 {
		t.Fatalf("末日原始价格与因子应保持: %+v", pm["2026-08-04"])
	}
	if pm["2026-08-01"].Amount != 5000.0 {
		t.Fatalf("金额应千元转元: %v", pm["2026-08-01"].Amount)
	}
	if _, err := LoadStockBars(q, nil, "2026-08-01", "2026-08-31", 800); err == nil {
		t.Fatalf("空代码应报错")
	}
	if _, err := LoadStockBars(q, []string{"A"}, "2026-09-01", "2026-08-01", 800); err == nil {
		t.Fatalf("逆序区间应报错")
	}
}

// TestLoadIndexCloses 指数序列过滤非法收盘。
func TestLoadIndexCloses(t *testing.T) {
	q := &fakeQ{index: []db.Row{
		{"trade_date": "2026-08-01", "close": 4000.0},
		{"trade_date": "2026-08-04", "close": 0.0},
	}}
	if _, err := LoadIndexCloses(q, "000300.SH", "2026-08-01", "2026-08-31", 800); err == nil {
		t.Fatalf("非法零收盘应报错")
	}
}

// TestUnionDatesAndPoolCodes 并集日期升序、池代码去重。
func TestUnionDatesAndPoolCodes(t *testing.T) {
	bars := map[string]map[string]Bar{
		"A": {"2026-08-04": {}, "2026-08-01": {}},
		"B": {"2026-08-04": {}},
	}
	dates := UnionDates(bars, "2026-08-01", "2026-08-31")
	if len(dates) != 2 || dates[0] != "2026-08-01" || dates[1] != "2026-08-04" {
		t.Fatalf("并集错误: %v", dates)
	}
	pools := map[string]PoolVersion{
		"2026-09": {Entries: []PoolEntry{{TsCode: "B"}, {TsCode: "A"}}},
		"2026-10": {Entries: []PoolEntry{{TsCode: "A"}, {TsCode: "C"}}},
	}
	codes := PoolCodes(pools)
	if len(codes) != 3 || codes[0] != "A" || codes[2] != "C" {
		t.Fatalf("去重排序错误: %v", codes)
	}
}

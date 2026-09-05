// G1 单测：DB 重建、S0 选择与回放、趋势装配，不碰真实 DB。
package strategy

import (
	"testing"

	"quantbackend/internal/db"
)

// fakeQ 内存 Queryer：按查询关键字返回预设行。
type fakeQ struct {
	months []db.Row
	tops   []db.Row
	bars   []db.Row
	index  []db.Row
}

// QueryRows 伪查询实现。
func (f *fakeQ) QueryRows(query string, args ...any) ([]db.Row, error) {
	switch {
	case contains(query, "dmin"):
		return f.months, nil
	case contains(query, "SUM(amount)"):
		return f.tops, nil
	case contains(query, "market_index"):
		return f.index, nil
	default:
		return f.bars, nil
	}
}

// contains 子串判断。
func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestFetchMonthlyTop20 SQL 重建：千元转元、同额代码升序、取 20。
func TestFetchMonthlyTop20(t *testing.T) {
	q := &fakeQ{
		months: []db.Row{{"dmin": "2026-08-01", "dmax": "2026-08-29", "dnum": int64(21)}},
		tops: []db.Row{
			{"ts_code": "000002.SZ", "sum_amt": float64(6000)},
			{"ts_code": "000001.SZ", "sum_amt": float64(6000)},
			{"ts_code": "600000.SH", "sum_amt": float64(1)},
		},
	}
	p, err := FetchMonthlyTop20(q, "2026-09", "2026-08", "vtest")
	if err != nil {
		t.Fatalf("重建失败: %v", err)
	}
	if p.RangeFrom != "2026-08-01" || p.RangeTo != "2026-08-29" {
		t.Fatalf("日期边界错误: %+v", p)
	}
	if len(p.Entries) != 3 || p.Entries[0].TsCode != "000001.SZ" {
		t.Fatalf("排序错误: %+v", p.Entries)
	}
	if p.Entries[0].Amount != 6000000 {
		t.Fatalf("千元转元错误: %v", p.Entries[0].Amount)
	}
}

// TestSelectS0 成交额顺序选两只，跳过禁买，不递补池外。
func TestSelectS0(t *testing.T) {
	pool := PoolVersion{Month: "2026-09", Entries: []PoolEntry{
		{Rank: 1, TsCode: "A"}, {Rank: 2, TsCode: "B"}, {Rank: 3, TsCode: "C"},
	}}
	got := SelectS0(pool, map[string]Qualify{"A": {TsCode: "A", Allowed: false, Reason: "ST"}}, 2)
	if len(got) != 2 || got[0].TsCode != "B" || got[1].TsCode != "C" {
		t.Fatalf("S0 选择错误: %+v", got)
	}
}

// TestRunS0Backtest 合成回放：首月买两只、次月离池意图次日卖出、持仓恒<=2。
func TestRunS0Backtest(t *testing.T) {
	poolSep := PoolVersion{Month: "2026-09", Entries: []PoolEntry{{Rank: 1, TsCode: "A"}, {Rank: 2, TsCode: "B"}}}
	poolOct := PoolVersion{Month: "2026-10", Entries: []PoolEntry{{Rank: 1, TsCode: "C"}, {Rank: 2, TsCode: "D"}}}
	mk := func(o, c float64) S0Quote { return S0Quote{Open: o, Close: c, AdjFactor: 1.0} }
	in := S0Input{
		Account: DefaultAccount(), Params: DefaultParams(),
		Pools: map[string]PoolVersion{"2026-09": poolSep, "2026-10": poolOct},
		Quals: map[string]map[string]Qualify{},
		Prices: map[string]map[string]S0Quote{
			"A": {"2026-09-01": mk(10, 10), "2026-09-02": mk(10, 10),
				"2026-10-08": mk(11, 11), "2026-10-09": mk(11, 11)},
			"B": {"2026-09-01": mk(20, 20), "2026-09-02": mk(20, 20),
				"2026-10-08": mk(21, 21), "2026-10-09": mk(21, 21)},
			"C": {"2026-10-08": mk(5, 5), "2026-10-09": mk(5, 5)},
			"D": {"2026-10-08": mk(6, 6), "2026-10-09": mk(6, 6)},
		},
		Dates: []string{"2026-09-01", "2026-09-02", "2026-10-08", "2026-10-09"},
	}
	res := RunS0Backtest(in)
	if res.Trades < 2 {
		t.Fatalf("首月应买入两只: %+v", res.Funnel)
	}
	for _, p := range res.Series {
		if p.Held > 2 {
			t.Fatalf("持仓超两只: %+v", p)
		}
	}
	if res.Ledger.Holdings["A"] != nil || res.Ledger.Holdings["B"] != nil {
		t.Fatalf("次月离池意图应在次日执行卖出旧仓: %+v", res.Ledger.Holdings)
	}
	if len(res.Series) != 4 {
		t.Fatalf("净值序列应逐日: %d", len(res.Series))
	}
}

// TestBuildEntry 趋势装配：突破与追高过滤语义。
func TestBuildEntry(t *testing.T) {
	p := DefaultParams()
	ok := func(v float64) FactorResult { return FactorResult{Value: v} }
	e := BuildEntry(TrendSnapshot{Close: 11, MA20: ok(10), MA60: ok(9), MA20Prev: ok(9.5),
		RS: ok(0.05), HC20: ok(10.5), AmountR: ok(1.5), ATR: ok(0.5)}, true, true, true, p)
	if !EntrySignal(e, p) {
		t.Fatalf("应触发: %+v", e)
	}
	e2 := BuildEntry(TrendSnapshot{Close: 12, MA20: ok(10), MA60: ok(9), MA20Prev: ok(9.5),
		RS: ok(0.05), HC20: ok(10.5), AmountR: ok(1.5), ATR: ok(0.5)}, true, true, true, p)
	if EntrySignal(e2, p) {
		t.Fatalf("超 MA20+3ATR 不应触发: %+v", e2)
	}
	if got := MarketBudget(StateTrend, DefaultThresholds(), DrawdownState{CapFactor: 1}); got != 0.80 {
		t.Fatalf("趋势预算错误: %v", got)
	}
	if got := MarketBudget(StateHot, DefaultThresholds(), DrawdownState{CapFactor: 1}); got != 0 {
		t.Fatalf("过热应禁新增: %v", got)
	}
}

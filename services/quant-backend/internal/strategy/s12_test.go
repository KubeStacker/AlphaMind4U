// S1/S2 单测：合成缓涨股应触发入场并持仓≤2，退潮市场应空仓。
package strategy

import (
	"fmt"
	"math"
	"testing"
	"time"
)

// synthDates 生成 n 个工作日交易日。
func synthDates(from string, n int) []string {
	t, _ := time.Parse("2006-01-02", from)
	out := []string{}
	for len(out) < n {
		if t.Weekday() != time.Saturday && t.Weekday() != time.Sunday {
			out = append(out, t.Format("2006-01-02"))
		}
		t = t.AddDate(0, 0, 1)
	}
	return out
}

// synthMarket 趋势/退潮两档市场输入。
func synthMarket(dates []string, trend bool) map[string]MarketInput {
	m := map[string]MarketInput{}
	b := 0.60
	if !trend {
		b = 0.20
	}
	for _, d := range dates {
		m[d] = MarketInput{B: FactorResult{Value: b}, B5Ago: FactorResult{Value: b - 0.01},
			V: FactorResult{Value: 1.0}, Trend: trend, DataOK: true}
	}
	return m
}

// synthS12 组装输入：A 缓涨满足 S1，B 横盘无信号，指数走平。
func synthS12(dates []string, trend bool, ver S12Version) S12Input {
	barsA, barsB := map[string]Bar{}, map[string]Bar{}
	idx := map[string]float64{}
	for i, d := range dates {
		// 缓涨 0.03/天：EXT≈2.4 不触发追高过滤，MOM20 为正，逐日突破前高。
		c := 10 + float64(i)*0.03
		// 金额按日复利 2% 递增：量能比≈1.23，落入 [1.2,2.5] 触发区间。
		barsA[d] = Bar{Date: d, Open: c, High: c * 1.005, Low: c * 0.995, Close: c, Amount: 1e8 * math.Pow(1.02, float64(i)), AdjFactor: 1.0}
		barsB[d] = Bar{Date: d, Open: 20, High: 20.1, Low: 19.9, Close: 20, Amount: 1e8, AdjFactor: 1.0}
		idx[d] = 4000
	}
	pools := map[string]PoolVersion{}
	decMonths := map[string]string{}
	for _, d := range dates {
		m := monthOf(d)
		decMonths[d] = m
		if _, ok := pools[m]; !ok {
			pools[m] = PoolVersion{Month: m, Entries: []PoolEntry{
				{Rank: 1, TsCode: "A"}, {Rank: 2, TsCode: "B"},
			}}
		}
	}
	return S12Input{
		Account: DefaultAccount(), Params: DefaultParams(), Threshold: DefaultThresholds(),
		Version:        ver,
		Pools:          pools,
		Bars:           map[string]map[string]Bar{"A": barsA, "B": barsB},
		Index:          idx,
		Market:         synthMarket(dates, trend),
		Dates:          dates,
		DecisionMonths: decMonths,
		Lot:            func(string) int { return 100 },
	}
}

// TestRunS12Trend 缓涨 A 应被买入，B 横盘无信号，持仓恒≤2。
func TestRunS12Trend(t *testing.T) {
	dates := synthDates("2026-01-05", 90)
	res := RunS12Backtest(synthS12(dates, true, VerF1))
	if res.Trades == 0 {
		t.Fatalf("趋势市应有成交: %+v", res.Funnel)
	}
	for _, p := range res.Series {
		if p.Held > 2 {
			t.Fatalf("持仓超两只: %+v", p)
		}
	}
	if len(res.Series) != len(dates) {
		t.Fatalf("序列长度错误: %d", len(res.Series))
	}
	fmt.Printf("trend trades=%d funnel=%+v lastNAV=%.0f\n", res.Trades, res.Funnel, res.Series[len(res.Series)-1].NAV)
}

// TestRunS12Retreat 退潮市 S2 应空仓无成交。
func TestRunS12Retreat(t *testing.T) {
	dates := synthDates("2026-01-05", 90)
	res := RunS12Backtest(synthS12(dates, false, VerS2))
	if res.Trades != 0 {
		t.Fatalf("退潮市不应成交: %+v", res.Funnel)
	}
	if len(res.Ledger.Holdings) != 0 {
		t.Fatalf("退潮市应空仓: %+v", res.Ledger.Holdings)
	}
}

// TestRunS12Weekly S1 周频不应 panic 且漏斗自洽。
func TestRunS12Weekly(t *testing.T) {
	dates := synthDates("2026-01-05", 90)
	res := RunS12Backtest(synthS12(dates, true, VerS1))
	if res.Funnel.SignalHit < res.Funnel.Filled {
		t.Fatalf("漏斗倒置: %+v", res.Funnel)
	}
}

// T01 正式回归：成交时钟与未成交意图（C1/C3/C4/C6），常驻不依赖 /tmp。
//
// 预期账本均为独立手工计算，不复用被测代码路径；旧错值仅作注释对照。
package strategy

import (
	"testing"
)

// s0Pool 单股池夹具。
func s0Pool(code string) map[string]PoolVersion {
	return map[string]PoolVersion{"2026-09": {Month: "2026-09",
		Entries: []PoolEntry{{Rank: 1, TsCode: code}}}}
}

// TestT01C1S0CloseMustNotDecideSameDayOpen 当天收盘不得决定当天开盘交易。
func TestT01C1S0CloseMustNotDecideSameDayOpen(t *testing.T) {
	dates := synthDates("2026-09-01", 6)
	quotes := map[string]S0Quote{}
	for _, d := range dates {
		quotes[d] = S0Quote{Open: 10, Close: 10, AdjFactor: 1.0}
	}
	quotes[dates[5]] = S0Quote{Open: 10, Close: 8, AdjFactor: 1.0}
	res := RunS0Backtest(S0Input{Account: DefaultAccount(), Params: DefaultParams(),
		Pools: s0Pool("A"), Prices: map[string]map[string]S0Quote{"A": quotes}, Dates: dates})
	h := res.Ledger.Holdings["A"]
	if h == nil {
		t.Fatalf("第6日收盘8被用于当天开盘10卖出（旧错 NAV=499807.83）；持仓应保留至次日")
	}
	if h.Shares != 4400 {
		t.Fatalf("冻结股数错误: %d", h.Shares)
	}
	// 独立账本：4400股@10.01，佣金13.21，现金=500000-44044-13.21=455942.79。
	if diff := res.Ledger.Cash - 455942.79; diff > 0.01 || diff < -0.01 {
		t.Fatalf("现金错误: %.2f", res.Ledger.Cash)
	}
	if len(res.OpenIntents) != 1 || res.OpenIntents[0].Side != "sell" ||
		res.OpenIntents[0].TsCode != "A" || res.OpenIntents[0].SignalDate != dates[5] ||
		res.OpenIntents[0].Reason != ExitStop {
		t.Fatalf("卖出意图应在末日冻结并保留: %+v", res.OpenIntents)
	}
}

// TestT01C1bS0ExitExecutesNextOpen 卖出意图次日开盘执行，期末精确对账。
func TestT01C1bS0ExitExecutesNextOpen(t *testing.T) {
	dates := synthDates("2026-09-01", 7)
	quotes := map[string]S0Quote{}
	for _, d := range dates {
		quotes[d] = S0Quote{Open: 10, Close: 10, AdjFactor: 1.0}
	}
	quotes[dates[5]] = S0Quote{Open: 10, Close: 8, AdjFactor: 1.0}
	quotes[dates[6]] = S0Quote{Open: 8, Close: 8, AdjFactor: 1.0}
	res := RunS0Backtest(S0Input{Account: DefaultAccount(), Params: DefaultParams(),
		Pools: s0Pool("A"), Prices: map[string]map[string]S0Quote{"A": quotes}, Dates: dates})
	if res.Ledger.Holdings["A"] != nil {
		t.Fatalf("次日开盘应已卖出")
	}
	// 独立账本：卖出价7.992，毛35164.80，佣金10.55+印花17.58，期末=491079.45。
	if diff := res.Series[6].NAV - 491079.45; diff > 0.01 || diff < -0.01 {
		t.Fatalf("期末净值错误: %.2f", res.Series[6].NAV)
	}
	if res.Trades != 1 || len(res.OpenIntents) != 0 {
		t.Fatalf("成交/残留意错误: trades=%d open=%+v", res.Trades, res.OpenIntents)
	}
}

// TestT01C3BuyDayStopExecutesNextDay 买入日收盘破线形成次日卖单。
func TestT01C3BuyDayStopExecutesNextDay(t *testing.T) {
	dates := synthDates("2026-01-05", 62)
	in := synthS12(dates, true, VerF1)
	for _, i := range []int{60, 61} {
		b := in.Bars["A"][dates[i]]
		b.Close, b.Low = 9, 8.9
		if i == 61 {
			b.Open = 9
		}
		in.Bars["A"][dates[i]] = b
	}
	res := RunS12Backtest(in)
	if res.Trades != 1 {
		t.Fatalf("夹具成交错误: %d", res.Trades)
	}
	if res.Ledger.Holdings["A"] != nil {
		t.Fatalf("买入日止损意图被推迟：次日收盘后仍持仓，NAV=%.2f", res.Series[61].NAV)
	}
	if len(res.OpenIntents) != 0 {
		t.Fatalf("不应有残留意图: %+v", res.OpenIntents)
	}
}

// TestT01C4BlockedExitRemainsPending 端点受阻保留原由，恢复后下一可执行日卖出。
func TestT01C4BlockedExitRemainsPending(t *testing.T) {
	dates := synthDates("2026-01-05", 64)
	in := synthS12(dates, true, VerF1)
	b := in.Bars["A"][dates[61]]
	b.Close, b.Low = 11.4, 11.3
	in.Bars["A"][dates[61]] = b
	b = in.Bars["A"][dates[62]]
	b.Open, b.Low = 10.26, 10.26
	in.Bars["A"][dates[62]] = b
	in.LimitDay = map[string]map[string]bool{"A": {dates[62]: true}}
	res := RunS12Backtest(in)
	if res.Trades != 1 {
		t.Fatalf("夹具成交错误: %d", res.Trades)
	}
	if res.Ledger.Holdings["A"] != nil {
		t.Fatal("受阻后收盘恢复即丢失意图；下一可执行日仍未卖出")
	}
	if len(res.OpenIntents) != 0 {
		t.Fatalf("不应有残留意图: %+v", res.OpenIntents)
	}
}

// TestT01C6HaltMustNotBypassLimit 停用不豁免端点限制，持仓与意图保留。
func TestT01C6HaltMustNotBypassLimit(t *testing.T) {
	dates := synthDates("2026-01-05", 63)
	in := synthS12(dates, true, VerF1)
	in.Account.MaxPositions, in.Account.SingleCapRatio, in.Account.SingleRisk = 1, 1, 0.05
	in.Account.TotalRisk = 0.05
	for _, i := range []int{61, 62} {
		b := in.Bars["A"][dates[i]]
		b.Open, b.Close, b.Low, b.High = 7, 7, 7, 7
		in.Bars["A"][dates[i]] = b
	}
	in.LimitDay = map[string]map[string]bool{"A": {dates[62]: true}}
	res := RunS12Backtest(in)
	if !res.Halted {
		t.Fatal("夹具未触发停用")
	}
	if res.Ledger.Holdings["A"] == nil {
		t.Fatal("停用账户在明确阻断的端点日被全平为现金")
	}
	if len(res.OpenIntents) != 1 || res.OpenIntents[0].Reason != ExitHalt ||
		res.OpenIntents[0].TsCode != "A" || res.OpenIntents[0].Side != "sell" {
		t.Fatalf("停用意图应保留原由: %+v", res.OpenIntents)
	}
}

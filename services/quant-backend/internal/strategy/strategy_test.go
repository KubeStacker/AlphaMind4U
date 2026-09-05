// 策略骨架单测：只验公式与规则，不碰 DB、不联网。
package strategy

import (
	"math"
	"testing"
)

// TestBuildMonthlyTop20 上月累计 Top20：金额汇总、同额代码升序、截断 20。
func TestBuildMonthlyTop20(t *testing.T) {
	rows := []DailyAmount{{TsCode: "000002.SZ", Amount: 10}, {TsCode: "000001.SZ", Amount: 10}, {TsCode: "000001.SZ", Amount: 5}}
	p := BuildMonthlyTop20("2026-09", "2026-08-01", "2026-08-31", rows, "v1")
	if len(p.Entries) != 2 || p.Entries[0].TsCode != "000001.SZ" || p.Entries[0].Amount != 15 {
		t.Fatalf("排名错误: %+v", p.Entries)
	}
}

// TestFactors wishes MOM/RS/ATR/量能比基本语义。
func TestFactors(t *testing.T) {
	closes := []float64{10, 11, 12}
	if v := MOM(closes, 2); math.Abs(v.Value-0.2) > 1e-9 {
		t.Fatalf("MOM 错误: %v", v)
	}
	idx := []float64{10, 10, 10}
	if v := RS20(closes, idx, 2); math.Abs(v.Value-0.2) > 1e-9 {
		t.Fatalf("RS 错误: %v", v)
	}
	bars := []Bar{{Close: 10, High: 11, Low: 9}, {Close: 11, High: 12, Low: 10}, {Close: 12, High: 13, Low: 11}}
	if v := ATR20(bars, 2); !v.Available() || v.Value <= 0 {
		t.Fatalf("ATR 错误: %v", v)
	}
	if v := AmountRatio([]float64{10, 10, 30}, 2); math.Abs(v.Value-3) > 1e-9 {
		t.Fatalf("量能比错误: %v", v)
	}
	if v := SMA([]float64{1}, 20); v.Available() {
		t.Fatalf("窗口不足应缺失: %v", v)
	}
}

// TestClassifyState 退潮优先于趋势，缺失走不可用。
func TestClassifyState(t *testing.T) {
	th := DefaultThresholds()
	ok := FactorResult{Value: 0.6}
	s := ClassifyState(MarketInput{B: ok, B5Ago: FactorResult{Value: 0.5}, Trend: true, DataOK: true}, th)
	if s != StateTrend {
		t.Fatalf("应为趋势: %s", s)
	}
	low := FactorResult{Value: 0.2}
	if s := ClassifyState(MarketInput{B: low, DataOK: true}, th); s != StateRetreat {
		t.Fatalf("应为退潮: %s", s)
	}
	if s := ClassifyState(MarketInput{DataOK: false}, th); s != StateUnavailable {
		t.Fatalf("应为不可用: %s", s)
	}
}

// TestEntryExit 入场全条件与退出优先级。
func TestEntryExit(t *testing.T) {
	p := DefaultParams()
	in := EntryInput{CloseGTMa20: true, Ma20GTMa60: true, SlopeUp: true, RS: FactorResult{Value: 0.05},
		Breakout: true, AmountRatio: FactorResult{Value: 1.5}, NotExtended: true, HistoryOK: true,
		Tradable: true, CooldownOver: true}
	if !EntrySignal(in, p) {
		t.Fatalf("应触发入场")
	}
	in.Breakout = false
	if EntrySignal(in, p) {
		t.Fatalf("缺突破不应触发")
	}
	if r := ExitSignal(ExitInput{BelowStop: true, OffPool: true}); r != ExitOffPool {
		t.Fatalf("离池应优先于跌破: %s", r)
	}
}

// TestBudgetAndSize 预算与风险定仓三取最小。
func TestBudgetAndSize(t *testing.T) {
	a := DefaultAccount()
	total, avail := TargetBudget(BudgetInput{NAV: 500000, Cash: 500000, MarketCap: 0.8, Account: a})
	if total != 400000 || avail != 400000 {
		t.Fatalf("预算错误: %v %v", total, avail)
	}
	// 退出距离 8% 时 1% 风险只允许 62500。
	sh, reason := SizeShares(500000, 200000, 0.08, 10, a, 100)
	if reason != "" || float64(sh)*10 != 62500 && float64(sh)*10 > 62500 {
		t.Fatalf("定仓错误: %d %s", sh, reason)
	}
	if s := DrawdownCover(400000, 500000); !s.Halt {
		t.Fatalf("20%% 回撤应停用: %+v", s)
	}
}

// TestLedgerBuySell 账本买卖与净值。
func TestLedgerBuySell(t *testing.T) {
	l := NewLedger(500000)
	l.ApplyBuy("000001.SZ", 100, 10, 5, 9)
	if l.Cash != 500000-1000-5 {
		t.Fatalf("买入现金错误: %v", l.Cash)
	}
	l.ApplySell("000001.SZ", 100, 11, 5)
	if len(l.Holdings) != 0 {
		t.Fatalf("应已清仓: %+v", l.Holdings)
	}
	if g := CheckBuyGate(10, 10.5, 10.5, 9, 10, 0.5, true, DefaultParams()); g == "" {
		t.Fatalf("涨跌停应拒单")
	}
}

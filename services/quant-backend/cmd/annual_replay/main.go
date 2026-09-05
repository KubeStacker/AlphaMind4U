package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"quantbackend/internal/db"
	"quantbackend/internal/engine"
	"quantbackend/internal/strategy"
)

func main() {
	dbPath := "/root/jarvis/data/quant.db"
	database, err := db.Open(dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer database.Close()

	eng := engine.New(database, nil, nil)
	months := []string{
		"2025-09", "2025-10", "2025-11", "2025-12",
		"2026-01", "2026-02", "2026-03", "2026-04",
		"2026-05", "2026-06", "2026-07", "2026-08", "2026-09",
	}
	from := "2025-09-05"
	to := "2026-09-04"
	account := strategy.DefaultAccount()

	fmt.Println("=== 运行 S0 年度回测 (2025-09-05 至 2026-09-04) ===")
	s0In, err := eng.PrepareS0(months, from, to, account)
	if err != nil {
		log.Fatalf("PrepareS0 失败: %v", err)
	}
	s0Res := strategy.RunS0Backtest(s0In)
	if s0Res.Err != nil {
		log.Fatalf("S0 回放失败: %v", s0Res.Err)
	}
	cycles0, sum0 := s0Res.Ledger.TradeCycles()
	winRate0 := 0.0
	if sum0.WinRate != nil {
		winRate0 = *sum0.WinRate * 100
	}
	lastNAV0 := s0Res.Series[len(s0Res.Series)-1].NAV
	maxDD0 := 0.0
	for _, p := range s0Res.Series {
		if p.Drawdown > maxDD0 {
			maxDD0 = p.Drawdown
		}
	}
	fmt.Printf("S0 完成! Trades=%d, FinalNAV=%.2f, Return=%.2f%%, MaxDD=%.2f%%, WinRate=%.2f%%, ClosedPnL=%.2f\n",
		s0Res.Trades, lastNAV0,
		(lastNAV0/account.InitialCapital-1)*100,
		maxDD0*100,
		winRate0, sum0.ClosedNetPnL)
	fmt.Printf("S0 现金分红总额: %.2f, 费用总额: %.2f, 最终持仓只数: %d\n",
		s0Res.Ledger.DividendsReceived, s0Res.Ledger.FeesPaid, len(s0Res.Ledger.Holdings))
	fmt.Printf("S0 机会漏斗: %+v\n", s0Res.Funnel)
	fmt.Printf("S0 闭合周期数=%d, 未闭合周期数=%d, 盈利=%d, 亏损=%d, 平=%d\n",
		sum0.ClosedCount, sum0.OpenCount, sum0.Wins, sum0.Losses, sum0.Flats)

	fmt.Println("\n=== 运行 S1 年度回测 (2025-09-05 至 2026-09-04) ===")
	s1In, err := eng.PrepareS12(strategy.VerS1, months, from, to, nil, account)
	if err != nil {
		log.Fatalf("PrepareS1 失败: %v", err)
	}
	s1Res := strategy.RunS12Backtest(s1In)
	if s1Res.Err != nil {
		log.Fatalf("S1 回放失败: %v", s1Res.Err)
	}
	cycles1, sum1 := s1Res.Ledger.TradeCycles()
	winRate1 := 0.0
	if sum1.WinRate != nil {
		winRate1 = *sum1.WinRate * 100
	}
	lastNAV1 := s1Res.Series[len(s1Res.Series)-1].NAV
	maxDD1 := 0.0
	for _, p := range s1Res.Series {
		if p.Drawdown > maxDD1 {
			maxDD1 = p.Drawdown
		}
	}
	fmt.Printf("S1 完成! Trades=%d, FinalNAV=%.2f, Return=%.2f%%, MaxDD=%.2f%%, WinRate=%.2f%%, ClosedPnL=%.2f\n",
		s1Res.Trades, lastNAV1,
		(lastNAV1/account.InitialCapital-1)*100,
		maxDD1*100,
		winRate1, sum1.ClosedNetPnL)
	fmt.Printf("S1 现金分红总额: %.2f, 费用总额: %.2f, 最终持仓只数: %d\n",
		s1Res.Ledger.DividendsReceived, s1Res.Ledger.FeesPaid, len(s1Res.Ledger.Holdings))
	fmt.Printf("S1 机会漏斗: %+v\n", s1Res.Funnel)
	fmt.Printf("S1 闭合周期数=%d, 未闭合周期数=%d, 盈利=%d, 亏损=%d, 平=%d\n",
		sum1.ClosedCount, sum1.OpenCount, sum1.Wins, sum1.Losses, sum1.Flats)

	fmt.Println("\n=== 运行 S2 年度回测 (2025-09-05 至 2026-09-04) ===")
	s2In, err := eng.PrepareS12(strategy.VerS2, months, from, to, nil, account)
	if err != nil {
		log.Fatalf("PrepareS2 失败: %v", err)
	}
	s2Res := strategy.RunS12Backtest(s2In)
	if s2Res.Err != nil {
		log.Fatalf("S2 回放失败: %v", s2Res.Err)
	}
	cycles2, sum2 := s2Res.Ledger.TradeCycles()
	winRate2 := 0.0
	if sum2.WinRate != nil {
		winRate2 = *sum2.WinRate * 100
	}
	lastNAV2 := s2Res.Series[len(s2Res.Series)-1].NAV
	maxDD2 := 0.0
	for _, p := range s2Res.Series {
		if p.Drawdown > maxDD2 {
			maxDD2 = p.Drawdown
		}
	}
	fmt.Printf("S2 完成! Trades=%d, FinalNAV=%.2f, Return=%.2f%%, MaxDD=%.2f%%, WinRate=%.2f%%, ClosedPnL=%.2f\n",
		s2Res.Trades, lastNAV2,
		(lastNAV2/account.InitialCapital-1)*100,
		maxDD2*100,
		winRate2, sum2.ClosedNetPnL)

	fmt.Println("\n=== 运行 F1 年度回测 (2025-09-05 至 2026-09-04) ===")
	f1In, err := eng.PrepareS12(strategy.VerF1, months, from, to, nil, account)
	if err != nil {
		log.Fatalf("PrepareF1 失败: %v", err)
	}
	f1Res := strategy.RunS12Backtest(f1In)
	if f1Res.Err != nil {
		log.Fatalf("F1 回放失败: %v", f1Res.Err)
	}
	cyclesF1, sumF1 := f1Res.Ledger.TradeCycles()
	winRateF1 := 0.0
	if sumF1.WinRate != nil {
		winRateF1 = *sumF1.WinRate * 100
	}
	lastNAVF1 := f1Res.Series[len(f1Res.Series)-1].NAV
	maxDDF1 := 0.0
	for _, p := range f1Res.Series {
		if p.Drawdown > maxDDF1 {
			maxDDF1 = p.Drawdown
		}
	}
	fmt.Printf("F1 完成! Trades=%d, FinalNAV=%.2f, Return=%.2f%%, MaxDD=%.2f%%, WinRate=%.2f%%, ClosedPnL=%.2f\n",
		f1Res.Trades, lastNAVF1,
		(lastNAVF1/account.InitialCapital-1)*100,
		maxDDF1*100,
		winRateF1, sumF1.ClosedNetPnL)

	// Save summary JSON
	report := map[string]any{
		"s0": map[string]any{
			"trades":        s0Res.Trades,
			"final_nav":     lastNAV0,
			"total_return":  (lastNAV0/account.InitialCapital - 1) * 100,
			"max_drawdown":  maxDD0 * 100,
			"dividends":     s0Res.Ledger.DividendsReceived,
			"fees":          s0Res.Ledger.FeesPaid,
			"funnel":        s0Res.Funnel,
			"trade_summary": sum0,
			"cycles_count":  len(cycles0),
		},
		"s1": map[string]any{
			"trades":        s1Res.Trades,
			"final_nav":     lastNAV1,
			"total_return":  (lastNAV1/account.InitialCapital - 1) * 100,
			"max_drawdown":  maxDD1 * 100,
			"dividends":     s1Res.Ledger.DividendsReceived,
			"fees":          s1Res.Ledger.FeesPaid,
			"funnel":        s1Res.Funnel,
			"trade_summary": sum1,
			"cycles_count":  len(cycles1),
		},
		"s2": map[string]any{
			"trades":        s2Res.Trades,
			"final_nav":     lastNAV2,
			"total_return":  (lastNAV2/account.InitialCapital - 1) * 100,
			"max_drawdown":  maxDD2 * 100,
			"trade_summary": sum2,
			"cycles_count":  len(cycles2),
		},
		"f1": map[string]any{
			"trades":        f1Res.Trades,
			"final_nav":     lastNAVF1,
			"total_return":  (lastNAVF1/account.InitialCapital - 1) * 100,
			"max_drawdown":  maxDDF1 * 100,
			"trade_summary": sumF1,
			"cycles_count":  len(cyclesF1),
		},
	}
	repBytes, _ := json.MarshalIndent(report, "", "  ")
	_ = os.WriteFile("/root/.gemini/antigravity-cli/brain/77a03466-0db8-4e35-9202-23a13f0fcdcf/scratch/annual_report.json", repBytes, 0644)
	fmt.Println("\n报告已保存至 scratch/annual_report.json")
}

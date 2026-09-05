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

type GridResult struct {
	BreakoutN    int      `json:"breakout_n"`
	TrailATR     float64  `json:"trail_atr"`
	Trades       int      `json:"trades"`
	FinalNAV     float64  `json:"final_nav"`
	TotalReturn  float64  `json:"total_return_pct"`
	MaxDrawdown  float64  `json:"max_drawdown_pct"`
	WinRate      *float64 `json:"win_rate_pct"`
	ClosedPnL    float64  `json:"closed_pnl"`
	ClosedCount  int      `json:"closed_count"`
	ProfitFactor *float64 `json:"profit_factor"`
	FeesPaid     float64  `json:"fees_paid"`
}

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

	breakouts := []int{15, 20, 25}
	trailATRs := []float64{2.0, 2.5, 3.0}

	fmt.Println("=========================================================================")
	fmt.Println("   S1 9组参数网格调优 (突破周期 15/20/25日 × 跟踪ATR 2.0/2.5/3.0)")
	fmt.Println("=========================================================================")
	fmt.Printf("%-10s %-10s %-8s %-12s %-12s %-10s %-10s %-10s\n",
		"Breakout", "TrailATR", "Trades", "FinalNAV", "Return(%)", "MaxDD(%)", "WinRate(%)", "ProfitFactor")

	var s1Grid []GridResult
	for _, b := range breakouts {
		for _, tr := range trailATRs {
			p := strategy.DefaultParams()
			p.BreakoutN = b
			p.TrailAtr = tr
			p.InitStopAtr = 2.5

			in, err := eng.PrepareS12(strategy.VerS1, months, from, to, nil, account)
			if err != nil {
				log.Fatalf("PrepareS1 失败: %v", err)
			}
			in.Params = p
			res := strategy.RunS12Backtest(in)
			if res.Err != nil {
				log.Fatalf("回测失败 (b=%d, tr=%.1f): %v", b, tr, res.Err)
			}
			_, sum := res.Ledger.TradeCycles()
			lastNAV := res.Series[len(res.Series)-1].NAV
			maxDD := 0.0
			for _, pt := range res.Series {
				if pt.Drawdown > maxDD {
					maxDD = pt.Drawdown
				}
			}
			var wr *float64
			if sum.WinRate != nil {
				val := *sum.WinRate * 100
				wr = &val
			}
			wrStr := "N/A"
			if wr != nil {
				wrStr = fmt.Sprintf("%.1f%%", *wr)
			}
			pfStr := "N/A"
			if sum.ProfitFactor != nil {
				pfStr = fmt.Sprintf("%.2f", *sum.ProfitFactor)
			}

			retPct := (lastNAV/account.InitialCapital - 1) * 100
			fmt.Printf("%-10d %-10.1f %-8d %-12.2f %-12.2f %-10.2f %-10s %-10s\n",
				b, tr, res.Trades, lastNAV, retPct, maxDD*100, wrStr, pfStr)

			s1Grid = append(s1Grid, GridResult{
				BreakoutN:    b,
				TrailATR:     tr,
				Trades:       res.Trades,
				FinalNAV:     lastNAV,
				TotalReturn:  retPct,
				MaxDrawdown:  maxDD * 100,
				WinRate:      wr,
				ClosedPnL:    sum.ClosedNetPnL,
				ClosedCount:  sum.ClosedCount,
				ProfitFactor: sum.ProfitFactor,
				FeesPaid:     res.Ledger.FeesPaid,
			})
		}
	}

	fmt.Println("\n=========================================================================")
	fmt.Println("   S1 / S2 / F1 压力测试 (佣金滑点加倍: 佣金万六, 滑点20bps)")
	fmt.Println("=========================================================================")
	stressParams := strategy.DefaultParams()
	stressParams.Commission = 0.0006
	stressParams.SlippageBps = 20

	testVersions := []strategy.S12Version{strategy.VerS1, strategy.VerS2, strategy.VerF1}
	var stressResults []map[string]any

	for _, ver := range testVersions {
		in, err := eng.PrepareS12(ver, months, from, to, nil, account)
		if err != nil {
			log.Fatalf("Prepare stress %s 失败: %v", ver, err)
		}
		in.Params = stressParams
		res := strategy.RunS12Backtest(in)
		if res.Err != nil {
			log.Fatalf("压力测试 %s 失败: %v", ver, res.Err)
		}
		_, sum := res.Ledger.TradeCycles()
		lastNAV := res.Series[len(res.Series)-1].NAV
		maxDD := 0.0
		for _, pt := range res.Series {
			if pt.Drawdown > maxDD {
				maxDD = pt.Drawdown
			}
		}
		wrStr := "N/A"
		if sum.WinRate != nil {
			wrStr = fmt.Sprintf("%.1f%%", *sum.WinRate*100)
		}
		retPct := (lastNAV/account.InitialCapital - 1) * 100
		fmt.Printf("版本 %s [压力]: Trades=%d, FinalNAV=%.2f, Return=%.2f%%, MaxDD=%.2f%%, WinRate=%s, Fees=%.2f\n",
			ver, res.Trades, lastNAV, retPct, maxDD*100, wrStr, res.Ledger.FeesPaid)

		stressResults = append(stressResults, map[string]any{
			"version":          ver,
			"trades":           res.Trades,
			"final_nav":        lastNAV,
			"total_return_pct": retPct,
			"max_drawdown_pct": maxDD * 100,
			"win_rate":         sum.WinRate,
			"fees_paid":        res.Ledger.FeesPaid,
			"trade_summary":    sum,
		})
	}

	fullReport := map[string]any{
		"s1_grid": s1Grid,
		"stress":  stressResults,
	}
	b, _ := json.MarshalIndent(fullReport, "", "  ")
	_ = os.WriteFile("/root/.gemini/antigravity-cli/brain/77a03466-0db8-4e35-9202-23a13f0fcdcf/scratch/grid_report.json", b, 0644)
	fmt.Println("\n调优网格与压力测试报告已保存至 scratch/grid_report.json")
}

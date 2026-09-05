// Package engine 每日更新流水线与任务分发入口。
package engine

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"quantbackend/internal/provider"
)

// PerformDailyDataUpdate 收盘后分步更新并做完整性校验。
//
// 任一步骤失败则整体返回 error，调用方（队列/调度）据此记 FAILED，
// 队列不再出现“子步骤失败却显示 COMPLETED”。
func (e *Engine) PerformDailyDataUpdate() error {
	log.Printf("========== 每日数据更新开始 ==========")
	type step struct {
		label  string
		status string
		detail string
		cost   float64
	}
	var steps []step
	runStep := func(label string, fn func() error) {
		start := time.Now()
		var detail string
		status := "[OK]"
		if err := fn(); err != nil {
			var perm *provider.PermissionError
			if errors.As(err, &perm) {
				status = "[SKIP]"
			} else {
				status = "[FAIL]"
			}
			detail = err.Error()
			if len(detail) > 80 {
				detail = detail[:80]
			}
			logWarn("%s %s | %s | %.1fs", status, label, detail, time.Since(start).Seconds())
		} else {
			detail = "完成"
		}
		cost := time.Since(start).Seconds()
		steps = append(steps, step{label, status, detail, cost})
		log.Printf("%s %-12s | %s | %5.1fs", status, label, detail, cost)
	}
	runStep("业绩快报", func() error { return e.SyncExpressData(120, "", "") })
	runStep("业绩预告", func() error { return e.SyncForecastData(120, "", "", "") })
	runStep("日线行情", func() error { return e.SyncDailyMarketData(1, false) })
	runStep("资金流向", func() error { return e.SyncCapitalFlow(0, 3, false) })
	runStep("日频基础数据", func() error { return e.SyncDailyBasic(0, 3, false) })
	runStep("沪深港通资金", func() error { return e.SyncMoneyflowHsgt(10, "", "") })
	runStep("停复牌数据", func() error { return e.SyncSuspendData(0, 3, false) })
	runStep("核心指数", func() error { return e.SyncCoreMarketIndices(0, 5) })
	runStep("指数权重", func() error { return e.SyncIndexWeightData("", "", "", 90) })
	runStep("融资融券", func() error { return e.SyncMarginTradingData(0, 3, false) })
	runStep("完整性校验", func() error { return e.validateDailyUpdate() })
	ok, fail := 0, 0
	var total float64
	var firstFail string
	for _, s := range steps {
		total += s.cost
		if s.status == "[OK]" {
			ok++
		} else {
			fail++
			if firstFail == "" {
				firstFail = s.label + ": " + s.detail
			}
		}
	}
	log.Printf("========== 每日数据更新汇总 | 共 %d 项 成功:%d 失败:%d | 总耗时 %.1fs ==========",
		len(steps), ok, fail, total)
	if fail > 0 {
		return fmt.Errorf("每日更新 %d 项失败，首错: %s", fail, firstFail)
	}
	return nil
}

// validateDailyUpdate 校验最新交易日行情与资金流完整性，不达标返回 error。
func (e *Engine) validateDailyUpdate() error {
	latest := e.Cal.LatestSyncDate()
	daily, err := e.DB.QueryInt("SELECT COUNT(*) FROM daily_price WHERE trade_date=?", latest)
	if err != nil {
		return fmt.Errorf("完整性校验查询失败: %w", err)
	}
	stocks, err := e.DB.QueryInt("SELECT COUNT(*) FROM stock_basic WHERE list_status='L'")
	if err != nil {
		return fmt.Errorf("完整性校验查询失败: %w", err)
	}
	if stocks > 0 {
		ratio := float64(daily) / float64(stocks) * 100
		if float64(daily) < float64(stocks)*0.9 {
			return fmt.Errorf("完整性未达标: %s 行情 %d/%d (%.1f%%)", latest, daily, stocks, ratio)
		}
		log.Printf("数据完整性验证通过: %s 行情数据 %d/%d (%.1f%%)", latest, daily, stocks, ratio)
	}
	mf, err := e.DB.QueryInt("SELECT COUNT(*) FROM stock_moneyflow WHERE trade_date=?", latest)
	if err != nil {
		return fmt.Errorf("完整性校验查询失败: %w", err)
	}
	if stocks > 0 {
		ratio := float64(mf) / float64(stocks) * 100
		if float64(mf) < float64(stocks)*0.8 {
			return fmt.Errorf("资金流未达标: %s %d/%d (%.1f%%)", latest, mf, stocks, ratio)
		}
		log.Printf("资金流数据验证通过: %s %d/%d (%.1f%%)", latest, mf, stocks, ratio)
	}
	return nil
}

// RunTask 按任务名分发同步任务。
func (e *Engine) RunTask(p SyncParams) error {
	task := strings.ToLower(strings.TrimSpace(p.Task))
	years, days, force := p.Years, p.Days, p.Force
	start, end, code := p.StartDate, p.EndDate, p.TsCode
	switch task {
	case "basic":
		_, err := e.SyncStockBasic()
		return err
	case "calendar":
		if start == "" {
			start = "2015-01-01"
		}
		if end == "" {
			end = "2030-12-31"
		}
		return e.SyncTradeCalendar(start, end)
	case "price", "daily":
		if start != "" && start == end {
			return e.SyncDailyDataByDate(start)
		}
		if years < 1 {
			years = 1
		}
		return e.SyncDailyMarketData(years, force)
	case "moneyflow":
		return e.SyncCapitalFlow(years, days, force, start, end)
	case "daily_basic":
		return e.SyncDailyBasic(years, days, force)
	case "concepts":
		return e.SyncConceptClassification()
	case "index":
		if code != "" {
			return e.SyncMarketIndex(code, years, days)
		}
		return e.SyncCoreMarketIndices(years, days)
	case "index_members":
		return e.SyncIndexMemberAll()
	case "express":
		return e.SyncExpressData(days, start, end)
	case "income", "financials":
		return e.SyncQuarterlyIncome(code, force)
	case "balancesheet", "balance_sheet":
		return e.SyncBalanceSheet(code, force)
	case "cashflow":
		return e.SyncCashflow(code, force)
	case "fina_indicator":
		return e.SyncFinancialIndicators(code, force)
	case "dividend":
		return e.SyncDividendData(code)
	case "forecast":
		return e.SyncForecastData(days, start, end, code)
	case "suspend":
		return e.SyncSuspendData(years, days, force)
	case "namechange":
		return e.SyncNamechangeData(code)
	case "index_weight":
		return e.SyncIndexWeightData(code, start, end, days)
	case "moneyflow_hsgt", "hsgt":
		return e.SyncMoneyflowHsgt(days, start, end)
	case "margin":
		return e.SyncMarginTradingData(years, days, force, start, end)
	case "daily_update":
		return e.PerformDailyDataUpdate()
	default:
		return fmt.Errorf("不支持的采集任务: %s", p.Task)
	}
}

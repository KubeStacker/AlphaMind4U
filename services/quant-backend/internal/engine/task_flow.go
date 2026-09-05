// Package engine 资金流任务：个股资金流与沪深港通资金流。
package engine

import (
	"log"
)

// SyncCapitalFlow 同步个股资金流向，支持年份、天数或精确起止范围。
func (e *Engine) SyncCapitalFlow(years, days int, force bool, dateBounds ...string) error {
	end := e.Cal.LatestSyncDate()
	start := dateShift(end, years, days)
	if len(dateBounds) >= 2 && dateBounds[0] != "" && dateBounds[1] != "" {
		start, end = dateBounds[0], dateBounds[1]
	}
	if s, en := dateRange(start, end); s != "" {
		start, end = s, en
	}
	log.Printf("开始同步资金流向 %s ~ %s...", start, end)
	targets := e.Cal.TradingDays(start, end)
	exist := map[string]bool{}
	if !force {
		exist = e.existingDatesWithCount("stock_moneyflow", "trade_date", start, end,
			e.expectedCount("stock_moneyflow", "trade_date", 0.85))
	}
	cols := []string{"ts_code", "trade_date", "buy_sm_vol", "buy_sm_amount", "sell_sm_vol",
		"sell_sm_amount", "buy_md_vol", "buy_md_amount", "sell_md_vol", "sell_md_amount",
		"buy_lg_vol", "buy_lg_amount", "sell_lg_vol", "sell_lg_amount", "buy_elg_vol",
		"buy_elg_amount", "sell_elg_vol", "sell_elg_amount", "net_mf_vol", "net_mf_amount"}
	n, processed := 0, 0
	for i := len(targets) - 1; i >= 0; i-- {
		d := targets[i]
		if exist[d] {
			continue
		}
		rows, err := e.P.Moneyflow(d)
		if err != nil {
			logWarn("同步资金流向 %s 失败: %v", d, err)
			continue
		}
		if len(rows) > 0 {
			normDateFields(rows, "trade_date")
			if err := e.dedupeUpsert("stock_moneyflow", cols,
				[]string{"ts_code", "trade_date"}, rows); err != nil {
				logWarn("写入资金流向 %s 失败: %v", d, err)
			} else {
				n += len(rows)
			}
		}
		processed++
		if processed%10 == 0 || processed == len(targets) {
			log.Printf("资金流向补采进度: 已拉取 %d 天, 累计 %d 条", processed, n)
		}
		sleepNormal()
	}
	log.Printf("资金流向同步完成: %d 条 (处理 %d 个交易日)", n, processed)
	return nil
}

// SyncMoneyflowHsgt 同步沪深港通资金流向。
func (e *Engine) SyncMoneyflowHsgt(days int, startDate, endDate string) error {
	if endDate == "" {
		endDate = e.Cal.LatestSyncDate()
	}
	if startDate == "" {
		startDate = dateShift(endDate, 0, days)
	}
	log.Printf("开始同步沪深港通资金流向: %s ~ %s...", startDate, endDate)
	rows, err := e.P.MoneyflowHsgt("", startDate, endDate)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		log.Printf("沪深港通资金流向未返回数据")
		return nil
	}
	cols := []string{"trade_date", "ggt_ss", "ggt_sz", "hgt", "sgt", "north_money", "south_money"}
	normDateFields(rows, "trade_date")
	if err := e.dedupeUpsert("moneyflow_hsgt", cols, []string{"trade_date"}, rows); err != nil {
		return err
	}
	log.Printf("沪深港通资金流向同步完成: %d 条", len(rows))
	return nil
}

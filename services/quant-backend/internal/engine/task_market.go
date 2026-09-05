// Package engine 行情与指数任务：日线、停复牌、指数、融资融券。
package engine

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"time"
)

// SyncDailyMarketData 按日期范围同步日线行情。
func (e *Engine) SyncDailyMarketData(years int, force bool) error {
	end := e.Cal.LatestSyncDate()
	start := dateShift(end, years, 0)
	if s, en := dateRange(start, end); s != "" {
		start, end = s, en
	}
	log.Printf("开始同步日线数据 %s ~ %s...", start, end)
	targets := e.Cal.TradingDays(start, end)
	exist := map[string]bool{}
	if !force {
		exist = e.existingDatesWithCount("daily_price", "trade_date", start, end,
			e.expectedCount("daily_price", "trade_date", 0.95))
	}
	n := 0
	for i := len(targets) - 1; i >= 0; i-- {
		if exist[targets[i]] {
			continue
		}
		if err := e.SyncDailyDataByDate(targets[i]); err != nil {
			return fmt.Errorf("同步 %s 失败: %w", targets[i], err)
		}
		n++
	}
	log.Printf("日线数据同步完成: %d 个交易日", n)
	return nil
}

// SyncDailyDataByDate 同步单个交易日行情与复权因子。
func (e *Engine) SyncDailyDataByDate(date string) error {
	log.Printf("正在同步 %s 的行情...", date)
	daily, err := e.P.Daily(date, "", "", "")
	if err != nil {
		return err
	}
	if len(daily) == 0 {
		return fmt.Errorf("%s 没有行情数据，未写入数据库", date)
	}
	adj, err := e.P.AdjFactor(date, "", "", "")
	if err != nil {
		return fmt.Errorf("%s 复权因子拉取失败，本日未写入: %w", date, err)
	}
	factor := map[string]float64{}
	for _, r := range adj {
		c := strVal(r["ts_code"])
		v, parseErr := strconv.ParseFloat(strVal(r["adj_factor"]), 64)
		_, duplicate := factor[c]
		if c == "" || duplicate || normDate(r["trade_date"]) != date ||
			parseErr != nil || v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			return fmt.Errorf("%s %s 复权因子/日期非法或重复，本日未写入", date, c)
		}
		factor[c] = v
	}
	seen := map[string]bool{}
	for _, r := range daily {
		c := strVal(r["ts_code"])
		v, ok := factor[c]
		if c == "" || seen[c] || normDate(r["trade_date"]) != date || !ok {
			return fmt.Errorf("%s %s 行情日期/证券非法、重复或缺复权因子，本日未写入", date, c)
		}
		seen[c] = true
		r["adj_factor"] = v
	}
	normDateFields(daily, "trade_date")
	cols := []string{"trade_date", "ts_code", "open", "high", "low", "close", "pre_close",
		"change", "pct_chg", "vol", "amount", "adj_factor"}
	if err := e.dedupeUpsert("daily_price", cols, []string{"trade_date", "ts_code"}, daily); err != nil {
		return err
	}
	sleepNormal()
	return nil
}

// SyncSuspendData 同步停复牌数据。
func (e *Engine) SyncSuspendData(years, days int, force bool) error {
	end := e.Cal.LatestSyncDate()
	start := dateShift(end, years, days)
	if s, en := dateRange(start, end); s != "" {
		start, end = s, en
	}
	log.Printf("开始同步停复牌数据 %s ~ %s...", start, end)
	targets := e.Cal.TradingDays(start, end)
	exist := map[string]bool{}
	if !force {
		exist = e.existingDates("stock_suspend", "trade_date", start, end)
	}
	cols := []string{"ts_code", "trade_date", "suspend_timing", "suspend_type"}
	for i := len(targets) - 1; i >= 0; i-- {
		d := targets[i]
		if exist[d] {
			continue
		}
		rows, err := e.P.SuspendD(d, "", "", "")
		if err != nil {
			logWarn("同步停复牌 %s 失败: %v", d, err)
			continue
		}
		if len(rows) > 0 {
			normDateFields(rows, "trade_date")
			if err := e.dedupeUpsert("stock_suspend", cols,
				[]string{"ts_code", "trade_date"}, rows); err != nil {
				logWarn("写入停复牌 %s 失败: %v", d, err)
			}
		}
		sleepNormal()
	}
	log.Printf("停复牌数据同步完成")
	return nil
}

// SyncMarketIndex 同步单指数日线。
func (e *Engine) SyncMarketIndex(tsCode string, years, days int) error {
	log.Printf("同步市场指数 %s...", tsCode)
	end := e.Cal.LatestSyncDate()
	start := dateShift(end, years, days)
	rows, err := e.P.IndexDaily(tsCode, start, end)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		log.Printf("指数 %s 未返回数据", tsCode)
		return nil
	}
	normDateFields(rows, "trade_date")
	cols := []string{"trade_date", "ts_code", "open", "high", "low", "close",
		"pre_close", "change", "pct_chg", "vol", "amount"}
	if err := e.dedupeUpsert("market_index", cols, []string{"trade_date", "ts_code"}, rows); err != nil {
		return err
	}
	sleepNormal()
	log.Printf("指数 %s 同步完成: %d 条", tsCode, len(rows))
	return nil
}

// SyncCoreMarketIndices 循环同步5个核心指数。
func (e *Engine) SyncCoreMarketIndices(years, days int) error {
	for _, code := range []string{"000001.SH", "399006.SZ", "000300.SH", "399001.SZ", "000688.SH"} {
		if err := e.SyncMarketIndex(code, years, days); err != nil {
			logWarn("同步指数 %s 失败: %v", code, err)
		}
	}
	log.Printf("核心指数同步完成")
	return nil
}

// SyncMarginTradingData 按日同步融资融券明细，支持年份、天数与起止范围，force 跳过已存在检查。
func (e *Engine) SyncMarginTradingData(years, days int, force bool, dateBounds ...string) error {
	end := e.Cal.LatestSyncDate()
	start := dateShift(end, years, days)
	if len(dateBounds) >= 2 && dateBounds[0] != "" && dateBounds[1] != "" {
		start, end = dateBounds[0], dateBounds[1]
	}
	if s, en := dateRange(start, end); s != "" {
		start, end = s, en
	}
	log.Printf("开始同步融资融券 %s ~ %s...", start, end)
	targets := e.Cal.TradingDays(start, end)
	exist := map[string]bool{}
	if !force {
		exist = e.existingDatesWithCount("stock_margin", "trade_date", start, end,
			e.expectedCount("stock_margin", "trade_date", 0.70))
	}
	cols := []string{"ts_code", "trade_date", "rzye", "rzmre", "rzche", "rqye", "rqmcl", "rzrqye", "rqyl"}
	n, totalRows := 0, 0
	for i := len(targets) - 1; i >= 0; i-- {
		d := targets[i]
		if exist[d] {
			continue
		}
		rows, err := e.P.MarginDetail(d)
		if err != nil {
			logWarn("同步融资融券 %s 失败: %v", d, err)
			time.Sleep(3 * time.Second)
			continue
		}
		if len(rows) == 0 {
			logWarn("融资融券 %s 无数据", d)
			sleepNormal()
			continue
		}
		normDateFields(rows, "trade_date")
		kept := rows[:0]
		for _, r := range rows {
			if strVal(r["ts_code"]) != "" {
				kept = append(kept, r)
			}
		}
		if len(kept) > 0 {
			if err := e.dedupeUpsert("stock_margin", cols,
				[]string{"ts_code", "trade_date"}, kept); err != nil {
				logWarn("写入融资融券 %s 失败: %v", d, err)
			} else {
				n++
				totalRows += len(kept)
			}
		}
		if n%10 == 0 || i == 0 {
			log.Printf("融资融券补采进度: 已拉取 %d 天, 累计 %d 条", n, totalRows)
		}
		sleepNormal()
	}
	log.Printf("融资融券数据同步完成: %d 个交易日 (累计 %d 条)", n, totalRows)
	return nil
}

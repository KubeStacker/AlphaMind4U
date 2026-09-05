// Package engine 基础与参考数据任务：股票目录、日历、日频指标等。
package engine

import (
	"errors"
	"log"
	"time"

	"quantbackend/internal/db"
	"quantbackend/internal/provider"
)

// SyncStockBasic 同步股票基础信息，返回写入条数。
func (e *Engine) SyncStockBasic() (int, error) {
	log.Printf("正在同步股票基础信息...")
	rows, err := e.P.StockBasic()
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		log.Printf("未获取到股票基础信息")
		return 0, nil
	}
	cols := []string{"ts_code", "symbol", "name", "area", "industry", "market", "list_date",
		"fullname", "enname", "curr_type", "list_status", "is_hs", "delist_date", "exchange", "cnspell"}
	normDateFields(rows, "list_date", "delist_date")
	if err := e.dedupeUpsert("stock_basic", cols, []string{"ts_code"}, rows); err != nil {
		return 0, err
	}
	log.Printf("成功同步 %d 条股票基础信息", len(rows))
	return len(rows), nil
}

// SyncTradeCalendar 同步交易日历。
func (e *Engine) SyncTradeCalendar(start, end string) error {
	if start == "" {
		start = "2015-01-01"
	}
	if end == "" {
		end = "2030-12-31"
	}
	log.Printf("正在同步交易日历 %s ~ %s...", start, end)
	rows, err := e.P.TradeCal(start, end)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		log.Printf("未获取到交易日历数据")
		return nil
	}
	cols := []string{"exchange", "cal_date", "is_open", "pretrade_date"}
	normDateFields(rows, "cal_date", "pretrade_date")
	if err := e.dedupeUpsert("trade_calendar", cols, []string{"exchange", "cal_date"}, rows); err != nil {
		return err
	}
	log.Printf("交易日历同步完成: %d 条", len(rows))
	return nil
}

// SyncDailyBasic 同步日频基础指标。
func (e *Engine) SyncDailyBasic(years, days int, force bool) error {
	end := e.Cal.LatestSyncDate()
	start := dateShift(end, years, days)
	if s, en := dateRange(start, end); s != "" {
		start, end = s, en
	}
	log.Printf("开始同步日频基础指标 %s ~ %s...", start, end)
	targets := e.Cal.TradingDays(start, end)
	exist := map[string]bool{}
	if !force {
		exist = e.existingDatesWithCount("stock_daily_basic", "trade_date", start, end,
			e.expectedCount("stock_daily_basic", "trade_date", 0.92))
	}
	cols := []string{"trade_date", "ts_code", "close", "turnover_rate", "turnover_rate_f",
		"volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm", "dv_ratio", "dv_ttm",
		"total_share", "float_share", "free_share", "total_mv", "circ_mv"}
	n := 0
	fails := 0
	for i := len(targets) - 1; i >= 0; i-- {
		d := targets[i]
		if exist[d] {
			continue
		}
		rows, err := e.P.DailyBasic(d, "", "", "")
		if err != nil {
			logWarn("同步 daily_basic %s 失败: %v", d, err)
			fails++
			continue
		}
		if len(rows) == 0 {
			logWarn("%s 未返回 daily_basic 数据", d)
			fails++
			continue
		}
		normDateFields(rows, "trade_date")
		if err := e.dedupeUpsert("stock_daily_basic", cols,
			[]string{"trade_date", "ts_code"}, rows); err != nil {
			logWarn("写入 daily_basic %s 失败: %v", d, err)
			fails++
		} else {
			n += len(rows)
		}
		sleepNormal()
	}
	// P0 真实性：远端/写入失败不再静默返回 nil，全败或部分失败均如实上报。
	if n == 0 && fails > 0 {
		return errors.New("daily_basic 同步失败")
	}
	if fails > 0 {
		return errors.New("daily_basic 部分日期失败")
	}
	log.Printf("日频基础指标同步完成: %d 条", n)
	return nil
}

// SyncIndexMemberAll 全量替换申万行业归属（单事务，防半替换）。
func (e *Engine) SyncIndexMemberAll() error {
	log.Printf("开始同步申万行业归属...")
	rows, err := e.P.IndexMemberAll("Y")
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		log.Printf("index_member_all 未返回数据，跳过")
		return nil
	}
	cols := []string{"l1_code", "l1_name", "l2_code", "l2_name", "l3_code", "l3_name",
		"ts_code", "name", "in_date", "out_date", "is_new"}
	normDateFields(rows, "in_date", "out_date")
	rows = db.Dedupe(rows, []string{"ts_code", "l3_code", "in_date"})
	if err := e.DB.ReplaceAll("stock_index_member_all", cols, rows, 2000); err != nil {
		return err
	}
	log.Printf("申万行业归属同步完成: %d 条", len(rows))
	return nil
}

// SyncExpressData 同步业绩快报。
func (e *Engine) SyncExpressData(days int, startDate, endDate string) error {
	if endDate == "" {
		endDate = e.Cal.LatestSyncDate()
	}
	if startDate == "" {
		startDate = dateShift(endDate, 0, days)
	}
	log.Printf("开始同步业绩快报: %s ~ %s...", startDate, endDate)
	rows, err := e.P.Express("", startDate, endDate)
	if err != nil {
		return err
	}
	if len(rows) == 0 {
		log.Printf("业绩快报未返回数据，跳过")
		return nil
	}
	cols := []string{"ts_code", "ann_date", "end_date", "revenue", "operate_profit", "total_profit",
		"n_income", "total_assets", "total_hldr_eqy_exc_min_int", "diluted_eps", "diluted_roe",
		"yoy_net_profit", "bps", "perf_summary", "update_flag"}
	normDateFields(rows, "ann_date", "end_date")
	if err := e.dedupeUpsert("stock_express", cols,
		[]string{"ts_code", "end_date", "ann_date"}, rows); err != nil {
		return err
	}
	log.Printf("业绩快报同步完成: %d 条", len(rows))
	return nil
}

// SyncNamechangeData 同步股票更名记录。
func (e *Engine) SyncNamechangeData(tsCode string) error {
	log.Printf("开始同步股票更名记录...")
	cols := []string{"ts_code", "name", "start_date", "end_date", "ann_date", "change_reason"}
	if tsCode == "" {
		rows, err := e.P.Namechange("", "", "")
		if err == nil && len(rows) > 0 {
			normDateFields(rows, "start_date", "end_date", "ann_date")
			if err := e.dedupeUpsert("stock_namechange", cols,
				[]string{"ts_code", "start_date"}, rows); err == nil {
				log.Printf("全量股票更名记录同步完成: +%d 条", len(rows))
				return nil
			}
		}
	}
	var stocks []string
	if tsCode != "" {
		stocks = []string{tsCode}
	} else {
		var err error
		stocks, err = e.stockCodes()
		if err != nil {
			return err
		}
	}
	if len(stocks) == 0 {
		return nil
	}
	total := 0
	for i, code := range stocks {
		rows, err := e.P.Namechange(code, "", "")
		if err != nil {
			var perm *provider.PermissionError
			if errors.As(err, &perm) {
				return perm
			}
			continue
		}
		if len(rows) > 0 {
			normDateFields(rows, "start_date", "end_date", "ann_date")
			if err := e.dedupeUpsert("stock_namechange", cols,
				[]string{"ts_code", "start_date"}, rows); err == nil {
				total += len(rows)
			}
		}
		if (i+1)%500 == 0 {
			log.Printf("更名记录进度: %d/%d", i+1, len(stocks))
			time.Sleep(1 * time.Second)
		} else {
			sleepNormal()
		}
	}
	log.Printf("股票更名记录同步完成: +%d 条", total)
	return nil
}

// SyncIndexWeightData 同步指数成分权重。
func (e *Engine) SyncIndexWeightData(indexCode, startDate, endDate string, days int) error {
	if endDate == "" {
		endDate = e.Cal.LatestSyncDate()
	}
	if startDate == "" {
		startDate = dateShift(endDate, 0, days)
	}
	indices := []string{"000300.SH", "000905.SH", "000852.SH", "000016.SH"}
	if indexCode != "" {
		indices = []string{indexCode}
	}
	log.Printf("开始同步指数成分权重: %v...", indices)
	cols := []string{"index_code", "con_code", "trade_date", "weight"}
	total := 0
	for _, idx := range indices {
		rows, err := e.P.IndexWeight(idx, "", startDate, endDate)
		if err != nil {
			logWarn("同步指数权重 %s 失败: %v", idx, err)
			continue
		}
		if len(rows) > 0 {
			normDateFields(rows, "trade_date")
			if err := e.dedupeUpsert("index_weight", cols,
				[]string{"index_code", "con_code", "trade_date"}, rows); err == nil {
				total += len(rows)
			}
		}
		sleepNormal()
	}
	log.Printf("指数成分权重同步完成: %d 条", total)
	return nil
}

// Package engine 财务任务：三张表、财务指标、分红、预告。
package engine

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"quantbackend/internal/db"
	"quantbackend/internal/provider"
)

// SyncQuarterlyIncome 同步利润表。
func (e *Engine) SyncQuarterlyIncome(tsCode string, force bool) error {
	log.Printf("开始季度利润表同步...")
	stocks, err := e.targetStocks(tsCode)
	if err != nil {
		return err
	}
	cols := []string{"ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type",
		"end_type", "basic_eps", "diluted_eps", "total_revenue", "revenue", "int_income",
		"prem_earned", "comm_income", "n_commis_income", "n_oth_income", "n_oth_b_income",
		"prem_income", "total_cogs", "oper_cost", "int_exp", "comm_exp", "biz_tax_surchg",
		"sell_exp", "admin_exp", "fin_exp", "assets_impair_loss", "operate_profit", "total_profit",
		"income_tax", "n_income", "n_income_attr_p", "minority_gain", "total_operate_income",
		"operate_exp", "total_operate_cost", "rd_exp"}
	if e.P.IsShort() {
		return e.syncFinancialShort(tsCode, force, cols, "income", "stock_income")
	}
	return e.syncFinancialLong(stocks, force, cols, "income", "stock_income")
}

// SyncBalanceSheet 同步资产负债表。
func (e *Engine) SyncBalanceSheet(tsCode string, force bool) error {
	log.Printf("开始资产负债表同步...")
	stocks, err := e.targetStocks(tsCode)
	if err != nil {
		return err
	}
	cols := []string{"ts_code", "ann_date", "f_ann_date", "end_date", "report_type", "comp_type",
		"end_type", "total_share", "money_cap", "trad_asset", "notes_receiv", "accounts_receiv",
		"oth_receiv", "prepayment", "inventories", "total_cur_assets", "fix_assets", "cip",
		"intan_assets", "goodwill", "lt_rec", "lt_eqt_invest", "total_assets", "st_borr",
		"notes_payable", "acct_payable", "adv_receipts", "contract_liab", "payroll_payable",
		"taxes_payable", "total_cur_liab", "lt_borr", "bond_payable", "total_liab", "cap_rese",
		"surplus_rese", "undistr_porfit", "total_hldr_eqy_exc_min_int", "minority_int",
		"total_hldr_eqy_inc_min_int"}
	if e.P.IsShort() {
		return e.syncFinancialShort(tsCode, force, cols, "balancesheet", "stock_balance_sheet")
	}
	return e.syncFinancialLong(stocks, force, cols, "balancesheet", "stock_balance_sheet")
}

// SyncCashflow 同步现金流量表。
func (e *Engine) SyncCashflow(tsCode string, force bool) error {
	log.Printf("开始现金流量表同步...")
	stocks, err := e.targetStocks(tsCode)
	if err != nil {
		return err
	}
	cols := []string{"ts_code", "ann_date", "f_ann_date", "end_date", "comp_type", "report_type",
		"end_type", "net_profit", "n_cashflow_act", "c_fr_sale_sg", "recp_tax_rends",
		"n_depos_incr_fi", "c_paid_goods_s", "c_paid_to_for_empl", "c_paid_for_taxes",
		"n_cashflow_inv_act", "c_disp_withdrwl_invest", "c_recp_return_invest",
		"c_pay_acq_const_fi_and_ot_la", "c_paid_invest", "n_cash_flows_fnc_act", "c_recp_borrow",
		"c_pay_dist_dpcp_int_exp", "n_incr_cash_cash_equ", "c_cash_equ_beg_period",
		"c_cash_equ_end_period", "free_cashflow"}
	if e.P.IsShort() {
		return e.syncFinancialShort(tsCode, force, cols, "cashflow", "stock_cashflow")
	}
	return e.syncFinancialLong(stocks, force, cols, "cashflow", "stock_cashflow")
}

// targetStocks 解析目标股票列表。
func (e *Engine) targetStocks(tsCode string) ([]string, error) {
	if tsCode != "" {
		return []string{tsCode}, nil
	}
	stocks, err := e.stockCodes()
	if err != nil {
		return nil, err
	}
	if len(stocks) == 0 {
		log.Printf("没有股票数据")
	}
	return stocks, nil
}

// fetchFinancial 按类型拉取单股票财报，period 为报告期 YYYYMMDD。
func (e *Engine) fetchFinancial(funcType, tsCode, period string) ([]db.Row, error) {
	switch funcType {
	case "income":
		return e.P.Income(tsCode, "", "", period)
	case "balancesheet":
		return e.P.BalanceSheet(tsCode, "", "", period)
	default:
		return e.P.Cashflow(tsCode, "", "", period)
	}
}

// prepFinancialRows 清洗财报行并做列名兼容。
func prepFinancialRows(rows []db.Row, funcType string) []db.Row {
	normDateFields(rows, "ann_date", "end_date", "f_ann_date")
	if funcType == "cashflow" {
		for _, r := range rows {
			if _, ok := r["c_pay_acq_const_fi_and_ot_la"]; !ok {
				if v, has := r["c_pay_acq_const_fiolta"]; has {
					r["c_pay_acq_const_fi_and_ot_la"] = v
				}
			}
		}
	}
	return rows
}

// recentQuarters 返回最近 n 个可披露季度末 YYYYMMDD（倒序）。
//
// 规则：往年季度全含；当年 Q1 需 4 月后、Q2 需 8 月后、Q3 需 11 月后，当年 Q4 永不含。
func recentQuarters(n int) []string {
	now := time.Now()
	y, m := now.Year(), int(now.Month())
	ends := []struct {
		month int
		suff  string
	}{{12, "1231"}, {9, "0930"}, {6, "0630"}, {3, "0331"}}
	need := map[int]int{3: 4, 6: 8, 9: 11, 12: 13}
	var out []string
	for len(out) < n {
		for _, q := range ends {
			if y == now.Year() && m < need[q.month] {
				continue
			}
			out = append(out, time.Date(y, time.Month(q.month), 1,
				0, 0, 0, 0, time.UTC).Format("2006")+q.suff)
			if len(out) >= n {
				break
			}
		}
		y--
	}
	return out
}

// quarterCoverage 查询某季度已覆盖股票数（end_date 按 YYYY-MM-DD 比对）。
func (e *Engine) quarterCoverage(table, quarter string) int {
	qd := quarter[:4] + "-" + quarter[4:6] + "-" + quarter[6:8]
	n, err := e.DB.QueryInt("SELECT COUNT(DISTINCT ts_code) FROM "+table+" WHERE end_date=?", qd)
	if err != nil {
		return 0
	}
	return int(n)
}

// listedCount 上市股票总数。
func (e *Engine) listedCount() int {
	n, err := e.DB.QueryInt("SELECT COUNT(*) FROM stock_basic WHERE list_status='L'")
	if err != nil || n <= 0 {
		return 5000
	}
	return int(n)
}

// syncedStocksForQuarter 查询某季度已同步股票集合。
func (e *Engine) syncedStocksForQuarter(quarter, table string) map[string]bool {
	out := map[string]bool{}
	qd := quarter[:4] + "-" + quarter[4:6] + "-" + quarter[6:8]
	rows, err := e.DB.QueryRows("SELECT DISTINCT ts_code FROM "+table+" WHERE end_date=?", qd)
	if err != nil {
		return out
	}
	for _, r := range rows {
		out[strVal(r["ts_code"])] = true
	}
	return out
}

// quarterUniverse 某季度末时已上市股票数，查不到回退当前全池。
func (e *Engine) quarterUniverse(quarter string) int64 {
	qd := quarter
	if len(quarter) == 8 {
		qd = quarter[:4] + "-" + quarter[4:6] + "-" + quarter[6:8]
	}
	n, err := e.DB.QueryInt(`SELECT COUNT(*) FROM stock_basic
		WHERE list_status='L' AND list_date <= ?`, qd)
	if err != nil || n <= 0 {
		return int64(e.listedCount())
	}
	return n
}

// syncFinancialShort 短 token 按缺口季度批量同步财报。
func (e *Engine) syncFinancialShort(tsCode string, force bool, cols []string, funcType, table string) error {
	quarters := recentQuarters(42)
	total := 0
	for _, q := range quarters {
		if tsCode == "" {
			target := int(float64(e.quarterUniverse(q)) * 0.90)
			cov := e.quarterCoverage(table, q)
			if !force && cov >= target {
				continue
			}
			log.Printf("%s 补季度 %s（当前 %d/%d）...", table, q, cov, target)
		}
		var rows []db.Row
		var err error
		switch funcType {
		case "income":
			rows, err = e.P.IncomeVip(tsCode, "", "", q)
		case "balancesheet":
			rows, err = e.P.BalanceSheetVip(tsCode, "", "", q)
		case "cashflow":
			rows, err = e.P.CashflowVip(tsCode, "", "", q)
		}
		if err != nil {
			var perm *provider.PermissionError
			if errors.As(err, &perm) {
				return perm
			}
			logWarn("%s 季度 %s 失败: %.50s", table, q, err.Error())
			continue
		}
		if len(rows) == 0 {
			continue
		}
		rows = prepFinancialRows(rows, funcType)
		avail := availCols(cols, rows)
		if err := e.dedupeUpsert(table, avail,
			[]string{"ts_code", "end_date", "report_type"}, rows); err != nil {
			logWarn("%s 写入季度 %s 失败: %v", table, q, err)
		} else {
			total += len(rows)
		}
		sleepNormal()
	}
	log.Printf("%s 短 token 同步完成: +%d 条", table, total)
	return nil
}

// syncFinancialLong 长 token 按缺口串行同步最近 4 个季度。
//
// 完成判定改为“季度覆盖率≥95%”，不再是“该季度有一条记录即完整”。
func (e *Engine) syncFinancialLong(stocks []string, force bool, cols []string, funcType, table string) error {
	const dailyLimit = 500
	universe := e.listedCount()
	quarters := recentQuarters(4)
	total := 0
	for _, q := range quarters {
		if !force && float64(e.quarterCoverage(table, q)) >= float64(universe)*0.95 {
			log.Printf("%s 季度 %s 已完整，跳过", table, q)
			continue
		}
		log.Printf("%s 同步季度 %s...", table, q)
		synced := e.syncedStocksForQuarter(q, table)
		for i, code := range stocks {
			if total >= dailyLimit {
				log.Printf("已达到每日限制 %d 条，停止同步", dailyLimit)
				return nil
			}
			if synced[code] {
				continue
			}
			rows, err := e.fetchFinancial(funcType, code, q)
			if err != nil {
				var perm *provider.PermissionError
				if errors.As(err, &perm) {
					return perm
				}
				if strings.Contains(err.Error(), "限流") || strings.Contains(err.Error(), "频繁") {
					logWarn("触发限流，暂停 60 秒")
					time.Sleep(60 * time.Second)
				}
				continue
			}
			if len(rows) > 0 {
				rows = prepFinancialRows(rows, funcType)
				avail := availCols(cols, rows)
				if err := e.dedupeUpsert(table, avail,
					[]string{"ts_code", "end_date", "report_type"}, rows); err == nil {
					total += len(rows)
				}
			}
			if (i+1)%100 == 0 {
				log.Printf("进度: %d/%d, +%d 条", i+1, len(stocks), total)
				time.Sleep(2 * time.Second)
			} else {
				sleepNormal()
			}
		}
	}
	log.Printf("%s 长 token 同步完成: +%d 条", table, total)
	return nil
}

// availCols 取目标列与返回列交集。
func availCols(cols []string, rows []db.Row) []string {
	if len(rows) == 0 {
		return cols
	}
	keys := map[string]bool{}
	for _, r := range rows {
		for k := range r {
			keys[k] = true
		}
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		if keys[c] {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		return cols
	}
	return out
}

// syncFinancialIndicatorsShort 短 token 按季度批量同步财务指标。
func (e *Engine) syncFinancialIndicatorsShort(force bool) error {
	log.Printf("开始短 token VIP 财务指标批量同步...")
	cols := []string{"ts_code", "ann_date", "end_date", "eps", "dt_eps", "eps_yoy", "bvps", "roe",
		"roe_waa", "roe_dt", "roe_yoy", "roa", "debt_to_assets", "current_ratio", "quick_ratio",
		"net_profit_margin", "net_profit_margin_yoy", "gross_profit_margin", "gross_profit_margin_yoy",
		"total_rev", "total_rev_yoy", "rev_ps", "profit", "profit_yoy", "profit_ps", "ocfps",
		"cfps", "or_yoy", "op_yoy", "ebt_yoy", "rd_exp", "update_flag"}
	mapping := map[string]string{
		"grossprofit_margin":     "gross_profit_margin",
		"netprofit_margin":       "net_profit_margin",
		"grossprofit_margin_yoy": "gross_profit_margin_yoy",
		"netprofit_margin_yoy":   "net_profit_margin_yoy",
		"bps":                    "bvps",
	}
	quarters := recentQuarters(42)
	total := 0
	for _, q := range quarters {
		target := int(float64(e.quarterUniverse(q)) * 0.90)
		curr := e.quarterCoverage("stock_fina_indicator", q)
		if !force && curr >= target {
			continue
		}
		log.Printf("stock_fina_indicator 补季度 %s（当前 %d/%d）...", q, curr, target)
		rows, err := e.P.FinaIndicatorVip("", "", "", q)
		if err != nil {
			var perm *provider.PermissionError
			if errors.As(err, &perm) {
				return perm
			}
			logWarn("fina_indicator_vip %s 失败: %v", q, err)
			continue
		}
		if len(rows) == 0 {
			continue
		}
		for _, r := range rows {
			for a, b := range mapping {
				if v, ok := r[a]; ok {
					if _, has := r[b]; !has {
						r[b] = v
					}
				}
			}
		}
		normDateFields(rows, "ann_date", "end_date")
		avail := availCols(cols, rows)
		if err := e.dedupeUpsert("stock_fina_indicator", avail,
			[]string{"ts_code", "end_date"}, rows); err != nil {
			logWarn("stock_fina_indicator 写入季度 %s 失败: %v", q, err)
		} else {
			total += len(rows)
		}
		sleepNormal()
	}
	log.Printf("财务指标 VIP 批量同步完成: +%d 条", total)
	return nil
}

// SyncFinancialIndicators 同步财务指标。
func (e *Engine) SyncFinancialIndicators(tsCode string, force bool) error {
	log.Printf("开始同步财务指标...")
	if tsCode == "" && e.P.IsShort() {
		return e.syncFinancialIndicatorsShort(force)
	}
	stocks, err := e.targetStocks(tsCode)
	if err != nil {
		return err
	}
	cols := []string{"ts_code", "ann_date", "end_date", "eps", "dt_eps", "eps_yoy", "bvps", "roe",
		"roe_waa", "roe_dt", "roe_yoy", "roa", "debt_to_assets", "current_ratio", "quick_ratio",
		"net_profit_margin", "net_profit_margin_yoy", "gross_profit_margin", "gross_profit_margin_yoy",
		"total_rev", "total_rev_yoy", "rev_ps", "profit", "profit_yoy", "profit_ps", "ocfps",
		"cfps", "or_yoy", "op_yoy", "ebt_yoy", "rd_exp", "update_flag"}
	mapping := map[string]string{
		"grossprofit_margin":     "gross_profit_margin",
		"netprofit_margin":       "net_profit_margin",
		"grossprofit_margin_yoy": "gross_profit_margin_yoy",
		"netprofit_margin_yoy":   "net_profit_margin_yoy",
		"bps":                    "bvps",
	}
	total := 0
	for i, code := range stocks {
		rows, err := e.P.FinaIndicator(code)
		if err != nil {
			var perm *provider.PermissionError
			if errors.As(err, &perm) {
				return perm
			}
			continue
		}
		if len(rows) > 0 {
			for _, r := range rows {
				for a, b := range mapping {
					if v, ok := r[a]; ok {
						if _, has := r[b]; !has {
							r[b] = v
						}
					}
				}
			}
			normDateFields(rows, "ann_date", "end_date")
			avail := availCols(cols, rows)
			if err := e.dedupeUpsert("stock_fina_indicator", avail,
				[]string{"ts_code", "end_date"}, rows); err != nil {
				logWarn("stock_fina_indicator upsert %s 失败: %v", code, err)
			} else {
				total += len(rows)
			}
		}
		if (i+1)%500 == 0 {
			log.Printf("进度: %d/%d", i+1, len(stocks))
			time.Sleep(1 * time.Second)
		} else {
			sleepNormal()
		}
	}
	log.Printf("财务指标同步完成: +%d 条", total)
	return nil
}

// SyncDividendData 同步分红送股。
func (e *Engine) SyncDividendData(tsCode string) error {
	log.Printf("开始同步分红送股数据...")
	stocks, err := e.targetStocks(tsCode)
	if err != nil {
		return err
	}
	cols := []string{"ts_code", "end_date", "ann_date", "div_proc", "stk_div", "stk_bo_rate",
		"stk_co_rate", "cash_div", "cash_div_tax", "record_date", "ex_date", "pay_date",
		"div_listdate", "imp_ann_date", "base_date", "base_share"}
	total := 0
	for i, code := range stocks {
		rows, err := e.P.Dividend(code, "", "", "")
		if err != nil {
			return fmt.Errorf("分红送股采集 %s: %w", code, err)
		}
		for _, row := range rows {
			for _, field := range []string{"ts_code", "end_date", "div_proc"} {
				if strings.TrimSpace(strVal(row[field])) == "" {
					return fmt.Errorf("分红送股 %s: 必需字段 %s 为空", code, field)
				}
			}
		}
		if len(rows) > 0 {
			normDateFields(rows, "ann_date", "end_date", "record_date", "ex_date", "pay_date",
				"div_listdate", "imp_ann_date", "base_date")
			if err := e.dedupeUpsert("stock_dividend", cols,
				cols, rows); err != nil {
				return fmt.Errorf("分红送股入库 %s: %w", code, err)
			}
			total += len(rows)
		}
		if (i+1)%500 == 0 {
			log.Printf("分红同步进度: %d/%d", i+1, len(stocks))
			time.Sleep(1 * time.Second)
		} else {
			sleepNormal()
		}
	}
	log.Printf("分红送股同步完成: +%d 条", total)
	return nil
}

// SyncForecastData 同步业绩预告（单股或日期范围）。
func (e *Engine) SyncForecastData(days int, startDate, endDate, tsCode string) error {
	cols := []string{"ts_code", "ann_date", "end_date", "type", "p_change_min", "p_change_max",
		"net_profit_min", "net_profit_max", "last_parent_net", "first_ann_date", "summary", "change_reason"}
	save := func(rows []db.Row) {
		if len(rows) == 0 {
			return
		}
		normDateFields(rows, "ann_date", "end_date", "first_ann_date")
		_ = e.dedupeUpsert("stock_forecast", cols, []string{"ts_code", "end_date", "ann_date"}, rows)
	}
	if tsCode != "" {
		log.Printf("同步个股业绩预告: %s", tsCode)
		rows, err := e.P.Forecast(tsCode, "", "", "", "")
		if err != nil {
			return err
		}
		save(rows)
		return nil
	}
	if endDate == "" {
		endDate = e.Cal.LatestSyncDate()
	}
	if startDate == "" {
		startDate = dateShift(endDate, 0, days)
	}
	log.Printf("开始按公告日期同步业绩预告: %s ~ %s...", startDate, endDate)
	s, err1 := time.Parse("2006-01-02", startDate)
	en, err2 := time.Parse("2006-01-02", endDate)
	if err1 != nil || err2 != nil {
		return nil
	}
	for d := s; !d.After(en); d = d.AddDate(0, 0, 1) {
		ann := d.Format("20060102")
		rows, err := e.P.Forecast("", ann, "", "", "")
		if err != nil {
			logWarn("同步 %s 业绩预告失败: %v", ann, err)
			continue
		}
		save(rows)
		sleepNormal()
	}
	log.Printf("业绩预告范围同步完成: %s ~ %s", startDate, endDate)
	return nil
}

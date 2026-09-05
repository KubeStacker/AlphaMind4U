// Schema 现役 24 表 SQLite DDL（DATE 存 TEXT 'YYYY-MM-DD'）。
package db

// Schema 建表语句，按依赖顺序执行。
const Schema = `
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code TEXT PRIMARY KEY, symbol TEXT, name TEXT,
    area TEXT, industry TEXT, market TEXT, list_date TEXT,
    fullname TEXT, enname TEXT, curr_type TEXT,
    list_status TEXT, is_hs TEXT,
    delist_date TEXT, exchange TEXT, cnspell TEXT
);
CREATE TABLE IF NOT EXISTS daily_price (
    trade_date TEXT NOT NULL, ts_code TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL, pre_close REAL,
    change REAL, pct_chg REAL, vol REAL, amount REAL, adj_factor REAL
);
CREATE TABLE IF NOT EXISTS stock_concepts (
    code TEXT PRIMARY KEY, name TEXT, src TEXT
);
CREATE TABLE IF NOT EXISTS stock_concept_details (
    id TEXT, concept_name TEXT, ts_code TEXT,
    name TEXT, PRIMARY KEY (id, ts_code)
);
CREATE TABLE IF NOT EXISTS stock_moneyflow (
    ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
    buy_sm_vol REAL, buy_sm_amount REAL, sell_sm_vol REAL, sell_sm_amount REAL,
    buy_md_vol REAL, buy_md_amount REAL, sell_md_vol REAL, sell_md_amount REAL,
    buy_lg_vol REAL, buy_lg_amount REAL, sell_lg_vol REAL, sell_lg_amount REAL,
    buy_elg_vol REAL, buy_elg_amount REAL, sell_elg_vol REAL, sell_elg_amount REAL,
    net_mf_vol REAL, net_mf_amount REAL
);
CREATE TABLE IF NOT EXISTS market_index (
    trade_date TEXT NOT NULL, ts_code TEXT NOT NULL,
    open REAL, high REAL, low REAL, close REAL, pre_close REAL,
    change REAL, pct_chg REAL, vol REAL, amount REAL
);
CREATE TABLE IF NOT EXISTS stock_margin (
    ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
    rzye REAL, rzmre REAL, rzche REAL, rqye REAL, rqmcl REAL,
    rzrqye REAL, rqyl REAL
);
CREATE TABLE IF NOT EXISTS stock_income (
    ts_code TEXT NOT NULL, ann_date TEXT, f_ann_date TEXT,
    end_date TEXT NOT NULL, report_type INTEGER, comp_type INTEGER,
    end_type TEXT, basic_eps REAL, diluted_eps REAL,
    total_revenue REAL, revenue REAL, int_income REAL, prem_earned REAL,
    comm_income REAL, n_commis_income REAL, n_oth_income REAL,
    n_oth_b_income REAL, prem_income REAL, total_cogs REAL,
    oper_cost REAL, int_exp REAL, comm_exp REAL, biz_tax_surchg REAL,
    sell_exp REAL, admin_exp REAL, fin_exp REAL, assets_impair_loss REAL,
    operate_profit REAL, total_profit REAL, income_tax REAL,
    n_income REAL, n_income_attr_p REAL, minority_gain REAL,
    total_operate_income REAL, operate_exp REAL, total_operate_cost REAL,
    rd_exp REAL,
    PRIMARY KEY (ts_code, end_date, report_type)
);
CREATE TABLE IF NOT EXISTS stock_balance_sheet (
    ts_code TEXT NOT NULL, ann_date TEXT, f_ann_date TEXT,
    end_date TEXT NOT NULL, report_type INTEGER, comp_type INTEGER,
    end_type TEXT, total_share REAL,
    money_cap REAL, trad_asset REAL, notes_receiv REAL, accounts_receiv REAL,
    oth_receiv REAL, prepayment REAL, inventories REAL, total_cur_assets REAL,
    fix_assets REAL, cip REAL, intan_assets REAL, goodwill REAL,
    lt_rec REAL, lt_eqt_invest REAL, total_assets REAL,
    st_borr REAL, notes_payable REAL, acct_payable REAL, adv_receipts REAL,
    contract_liab REAL, payroll_payable REAL, taxes_payable REAL,
    total_cur_liab REAL, lt_borr REAL, bond_payable REAL, total_liab REAL,
    cap_rese REAL, surplus_rese REAL, undistr_porfit REAL,
    total_hldr_eqy_exc_min_int REAL, minority_int REAL, total_hldr_eqy_inc_min_int REAL,
    PRIMARY KEY (ts_code, end_date, report_type)
);
CREATE TABLE IF NOT EXISTS stock_cashflow (
    ts_code TEXT NOT NULL, ann_date TEXT, f_ann_date TEXT,
    end_date TEXT NOT NULL, comp_type INTEGER, report_type INTEGER,
    end_type TEXT, net_profit REAL,
    n_cashflow_act REAL, c_fr_sale_sg REAL, recp_tax_rends REAL, n_depos_incr_fi REAL,
    c_paid_goods_s REAL, c_paid_to_for_empl REAL, c_paid_for_taxes REAL,
    n_cashflow_inv_act REAL, c_disp_withdrwl_invest REAL, c_recp_return_invest REAL,
    c_pay_acq_const_fi_and_ot_la REAL, c_paid_invest REAL,
    n_cash_flows_fnc_act REAL, c_recp_borrow REAL, c_pay_dist_dpcp_int_exp REAL,
    n_incr_cash_cash_equ REAL, c_cash_equ_beg_period REAL, c_cash_equ_end_period REAL,
    free_cashflow REAL,
    PRIMARY KEY (ts_code, end_date, report_type)
);
CREATE TABLE IF NOT EXISTS stock_fina_indicator (
    ts_code TEXT NOT NULL, ann_date TEXT, end_date TEXT NOT NULL,
    eps REAL, dt_eps REAL, eps_yoy REAL, bvps REAL, roe REAL, roe_waa REAL, roe_dt REAL, roe_yoy REAL,
    roa REAL, debt_to_assets REAL, current_ratio REAL, quick_ratio REAL,
    net_profit_margin REAL, net_profit_margin_yoy REAL,
    gross_profit_margin REAL, gross_profit_margin_yoy REAL,
    total_rev REAL, total_rev_yoy REAL, rev_ps REAL,
    profit REAL, profit_yoy REAL, profit_ps REAL,
    ocfps REAL, cfps REAL, or_yoy REAL, op_yoy REAL, ebt_yoy REAL,
    rd_exp REAL, update_flag TEXT,
    PRIMARY KEY (ts_code, end_date)
);
CREATE TABLE IF NOT EXISTS stock_daily_basic (
    trade_date TEXT NOT NULL, ts_code TEXT NOT NULL, close REAL,
    turnover_rate REAL, turnover_rate_f REAL, volume_ratio REAL,
    pe REAL, pe_ttm REAL, pb REAL, ps REAL, ps_ttm REAL,
    dv_ratio REAL, dv_ttm REAL, total_share REAL, float_share REAL,
    free_share REAL, total_mv REAL, circ_mv REAL
);
CREATE TABLE IF NOT EXISTS stock_index_member_all (
    l1_code TEXT, l1_name TEXT, l2_code TEXT,
    l2_name TEXT, l3_code TEXT, l3_name TEXT,
    ts_code TEXT NOT NULL, name TEXT, in_date TEXT,
    out_date TEXT, is_new TEXT, PRIMARY KEY (ts_code, l3_code, in_date)
);
CREATE TABLE IF NOT EXISTS stock_express (
    ts_code TEXT NOT NULL, ann_date TEXT, end_date TEXT NOT NULL,
    revenue REAL, operate_profit REAL, total_profit REAL, n_income REAL,
    total_assets REAL, total_hldr_eqy_exc_min_int REAL, diluted_eps REAL,
    diluted_roe REAL, yoy_net_profit REAL, bps REAL, perf_summary TEXT,
    update_flag TEXT, PRIMARY KEY (ts_code, end_date, ann_date)
);
CREATE TABLE IF NOT EXISTS stock_suspend (
    ts_code TEXT NOT NULL, trade_date TEXT NOT NULL,
    suspend_timing TEXT, suspend_type TEXT,
    PRIMARY KEY (ts_code, trade_date)
);
CREATE TABLE IF NOT EXISTS stock_namechange (
    ts_code TEXT NOT NULL, name TEXT,
    start_date TEXT NOT NULL, end_date TEXT, ann_date TEXT,
    change_reason TEXT,
    PRIMARY KEY (ts_code, start_date)
);
CREATE TABLE IF NOT EXISTS stock_dividend (
    ts_code TEXT NOT NULL, end_date TEXT NOT NULL, ann_date TEXT,
    div_proc TEXT, stk_div REAL, stk_bo_rate REAL, stk_co_rate REAL,
    cash_div REAL, cash_div_tax REAL, record_date TEXT, ex_date TEXT,
    pay_date TEXT, div_listdate TEXT, imp_ann_date TEXT, base_date TEXT, base_share REAL,
    PRIMARY KEY (ts_code, end_date, ann_date)
);
CREATE TABLE IF NOT EXISTS index_weight (
    index_code TEXT NOT NULL, con_code TEXT NOT NULL,
    trade_date TEXT NOT NULL, weight REAL,
    PRIMARY KEY (index_code, con_code, trade_date)
);
CREATE TABLE IF NOT EXISTS stock_forecast (
    ts_code TEXT NOT NULL, ann_date TEXT, end_date TEXT NOT NULL,
    type TEXT, p_change_min REAL, p_change_max REAL,
    net_profit_min REAL, net_profit_max REAL, last_parent_net REAL,
    first_ann_date TEXT, summary TEXT, change_reason TEXT,
    PRIMARY KEY (ts_code, end_date, ann_date)
);
CREATE TABLE IF NOT EXISTS moneyflow_hsgt (
    trade_date TEXT NOT NULL PRIMARY KEY,
    ggt_ss REAL, ggt_sz REAL, hgt REAL, sgt REAL,
    north_money REAL, south_money REAL
);
CREATE TABLE IF NOT EXISTS trade_calendar (
    exchange TEXT NOT NULL, cal_date TEXT NOT NULL, is_open INTEGER,
    pretrade_date TEXT, PRIMARY KEY (exchange, cal_date)
);
CREATE TABLE IF NOT EXISTS etl_tasks (
    task_id TEXT PRIMARY KEY, task_key TEXT UNIQUE,
    task_type TEXT, params_json TEXT, status TEXT,
    created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now')),
    started_at TEXT, heartbeat_at TEXT, finished_at TEXT, error TEXT,
    progress REAL DEFAULT 0.0
);
CREATE TABLE IF NOT EXISTS strategy_pool_month (
    effect_month TEXT PRIMARY KEY, rank_month TEXT NOT NULL,
    range_from TEXT NOT NULL, range_to TEXT NOT NULL,
    dataset_v TEXT NOT NULL, created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
);
CREATE TABLE IF NOT EXISTS strategy_pool_entry (
    effect_month TEXT NOT NULL, rank INTEGER NOT NULL, ts_code TEXT NOT NULL, amount REAL NOT NULL,
    PRIMARY KEY (effect_month, rank)
);
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY, applied_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
);
-- 事实表键列复合索引（SQLite 行存必需，否则大表全扫描；DuckDB 时代不需要）。
CREATE INDEX IF NOT EXISTS idx_daily_price_key ON daily_price (trade_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_daily_price_tscode ON daily_price (ts_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_moneyflow_key ON stock_moneyflow (ts_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_moneyflow_date ON stock_moneyflow (trade_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_stock_daily_basic_key ON stock_daily_basic (trade_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_stock_daily_basic_tscode ON stock_daily_basic (ts_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_market_index_key ON market_index (trade_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_stock_margin_key ON stock_margin (ts_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_stock_margin_date ON stock_margin (trade_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_stock_income_enddate ON stock_income (end_date);
CREATE INDEX IF NOT EXISTS idx_stock_balance_sheet_enddate ON stock_balance_sheet (end_date);
CREATE INDEX IF NOT EXISTS idx_stock_cashflow_enddate ON stock_cashflow (end_date);
CREATE INDEX IF NOT EXISTS idx_stock_fina_indicator_enddate ON stock_fina_indicator (end_date);
CREATE INDEX IF NOT EXISTS idx_stock_dividend_enddate ON stock_dividend (end_date);
CREATE INDEX IF NOT EXISTS idx_stock_suspend_date ON stock_suspend (trade_date);
`

// InitSchema 建现役 24 表 + 版本表。
func (d *DB) InitSchema() error {
	for _, stmt := range splitStmts(Schema) {
		if _, err := d.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

// splitStmts 按分号切分 DDL。
func splitStmts(schema string) []string {
	parts := splitBy(schema, ';')
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := trimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func splitBy(s string, sep rune) []string {
	var out []string
	cur := make([]rune, 0, len(s))
	for _, r := range s {
		if r == sep {
			out = append(out, string(cur))
			cur = cur[:0]
			continue
		}
		cur = append(cur, r)
	}
	out = append(out, string(cur))
	return out
}

func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\n' || s[start] == '\t' || s[start] == '\r') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\n' || s[end-1] == '\t' || s[end-1] == '\r') {
		end--
	}
	return s[start:end]
}

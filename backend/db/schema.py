"""DuckDB schema for raw collected data and ETL operations only."""

CREATE_STOCK_BASIC_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code VARCHAR(15) PRIMARY KEY, symbol VARCHAR(10), name VARCHAR(50),
    area VARCHAR(50), industry VARCHAR(50), market VARCHAR(50), list_date DATE,
    fullname VARCHAR(100), enname VARCHAR(100), curr_type VARCHAR(10),
    list_status VARCHAR(5), is_hs VARCHAR(5),
    delist_date DATE, exchange VARCHAR(10), cnspell VARCHAR(20)
);
"""

CREATE_DAILY_PRICE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_price (
    trade_date DATE NOT NULL, ts_code VARCHAR(15) NOT NULL,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, pre_close DOUBLE,
    change DOUBLE, pct_chg DOUBLE, vol DOUBLE, amount DOUBLE, adj_factor DOUBLE
);
"""

CREATE_STOCK_CONCEPTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_concepts (
    code VARCHAR(20) PRIMARY KEY, name VARCHAR(50), src VARCHAR(20)
);
"""

CREATE_STOCK_CONCEPT_DETAILS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_concept_details (
    id VARCHAR(20), concept_name VARCHAR(50), ts_code VARCHAR(15),
    name VARCHAR(50), PRIMARY KEY (id, ts_code)
);
"""

CREATE_STOCK_MONEYFLOW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_moneyflow (
    ts_code VARCHAR(15) NOT NULL, trade_date DATE NOT NULL,
    buy_sm_vol DOUBLE, buy_sm_amount DOUBLE, sell_sm_vol DOUBLE, sell_sm_amount DOUBLE,
    buy_md_vol DOUBLE, buy_md_amount DOUBLE, sell_md_vol DOUBLE, sell_md_amount DOUBLE,
    buy_lg_vol DOUBLE, buy_lg_amount DOUBLE, sell_lg_vol DOUBLE, sell_lg_amount DOUBLE,
    buy_elg_vol DOUBLE, buy_elg_amount DOUBLE, sell_elg_vol DOUBLE, sell_elg_amount DOUBLE,
    net_mf_vol DOUBLE, net_mf_amount DOUBLE
);
"""

CREATE_MARKET_INDEX_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS market_index (
    trade_date DATE NOT NULL, ts_code VARCHAR(15) NOT NULL,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, pre_close DOUBLE,
    change DOUBLE, pct_chg DOUBLE, vol DOUBLE, amount DOUBLE
);
"""

CREATE_STOCK_MARGIN_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_margin (
    ts_code VARCHAR(15) NOT NULL, trade_date DATE NOT NULL,
    rzye DOUBLE, rzmre DOUBLE, rzche DOUBLE, rqye DOUBLE, rqmcl DOUBLE,
    rzrqye DOUBLE, rqyl DOUBLE
);
"""

CREATE_STOCK_INCOME_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_income (
    ts_code VARCHAR(15) NOT NULL, ann_date DATE, f_ann_date DATE,
    end_date DATE NOT NULL, report_type INTEGER, comp_type INTEGER,
    end_type VARCHAR(10), basic_eps DOUBLE, diluted_eps DOUBLE,
    total_revenue DOUBLE, revenue DOUBLE, int_income DOUBLE, prem_earned DOUBLE,
    comm_income DOUBLE, n_commis_income DOUBLE, n_oth_income DOUBLE,
    n_oth_b_income DOUBLE, prem_income DOUBLE, total_cogs DOUBLE,
    oper_cost DOUBLE, int_exp DOUBLE, comm_exp DOUBLE, biz_tax_surchg DOUBLE,
    sell_exp DOUBLE, admin_exp DOUBLE, fin_exp DOUBLE, assets_impair_loss DOUBLE,
    operate_profit DOUBLE, total_profit DOUBLE, income_tax DOUBLE,
    n_income DOUBLE, n_income_attr_p DOUBLE, minority_gain DOUBLE,
    total_operate_income DOUBLE, operate_exp DOUBLE, total_operate_cost DOUBLE,
    rd_exp DOUBLE,
    PRIMARY KEY (ts_code, end_date, report_type)
);
"""

CREATE_STOCK_BALANCE_SHEET_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_balance_sheet (
    ts_code VARCHAR(15) NOT NULL, ann_date DATE, f_ann_date DATE,
    end_date DATE NOT NULL, report_type INTEGER, comp_type INTEGER,
    end_type VARCHAR(10), total_share DOUBLE,
    money_cap DOUBLE, trad_asset DOUBLE, notes_receiv DOUBLE, accounts_receiv DOUBLE,
    oth_receiv DOUBLE, prepayment DOUBLE, inventories DOUBLE, total_cur_assets DOUBLE,
    fix_assets DOUBLE, cip DOUBLE, intan_assets DOUBLE, goodwill DOUBLE,
    lt_rec DOUBLE, lt_eqt_invest DOUBLE, total_assets DOUBLE,
    st_borr DOUBLE, notes_payable DOUBLE, acct_payable DOUBLE, adv_receipts DOUBLE,
    contract_liab DOUBLE, payroll_payable DOUBLE, taxes_payable DOUBLE,
    total_cur_liab DOUBLE, lt_borr DOUBLE, bond_payable DOUBLE, total_liab DOUBLE,
    cap_rese DOUBLE, surplus_rese DOUBLE, undistr_porfit DOUBLE,
    total_hldr_eqy_exc_min_int DOUBLE, minority_int DOUBLE, total_hldr_eqy_inc_min_int DOUBLE,
    PRIMARY KEY (ts_code, end_date, report_type)
);
"""

CREATE_STOCK_CASHFLOW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_cashflow (
    ts_code VARCHAR(15) NOT NULL, ann_date DATE, f_ann_date DATE,
    end_date DATE NOT NULL, comp_type INTEGER, report_type INTEGER,
    end_type VARCHAR(10), net_profit DOUBLE,
    n_cashflow_act DOUBLE, c_fr_sale_sg DOUBLE, recp_tax_rends DOUBLE, n_depos_incr_fi DOUBLE,
    c_paid_goods_s DOUBLE, c_paid_to_for_empl DOUBLE, c_paid_for_taxes DOUBLE,
    n_cashflow_inv_act DOUBLE, c_disp_withdrwl_invest DOUBLE, c_recp_return_invest DOUBLE,
    c_pay_acq_const_fi_and_ot_la DOUBLE, c_paid_invest DOUBLE,
    n_cash_flows_fnc_act DOUBLE, c_recp_borrow DOUBLE, c_pay_dist_dpcp_int_exp DOUBLE,
    n_incr_cash_cash_equ DOUBLE, c_cash_equ_beg_period DOUBLE, c_cash_equ_end_period DOUBLE,
    free_cashflow DOUBLE,
    PRIMARY KEY (ts_code, end_date, report_type)
);
"""

CREATE_STOCK_FINA_INDICATOR_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_fina_indicator (
    ts_code VARCHAR(15) NOT NULL, ann_date DATE, end_date DATE NOT NULL,
    eps DOUBLE, dt_eps DOUBLE, eps_yoy DOUBLE, bvps DOUBLE, roe DOUBLE, roe_waa DOUBLE, roe_dt DOUBLE, roe_yoy DOUBLE,
    roa DOUBLE, debt_to_assets DOUBLE, current_ratio DOUBLE, quick_ratio DOUBLE,
    net_profit_margin DOUBLE, net_profit_margin_yoy DOUBLE,
    gross_profit_margin DOUBLE, gross_profit_margin_yoy DOUBLE,
    total_rev DOUBLE, total_rev_yoy DOUBLE, rev_ps DOUBLE,
    profit DOUBLE, profit_yoy DOUBLE, profit_ps DOUBLE,
    ocfps DOUBLE, cfps DOUBLE, or_yoy DOUBLE, op_yoy DOUBLE, ebt_yoy DOUBLE,
    rd_exp DOUBLE, update_flag VARCHAR(10),
    PRIMARY KEY (ts_code, end_date)
);
"""

CREATE_STOCK_DAILY_BASIC_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily_basic (
    trade_date DATE NOT NULL, ts_code VARCHAR(15) NOT NULL, close DOUBLE,
    turnover_rate DOUBLE, turnover_rate_f DOUBLE, volume_ratio DOUBLE,
    pe DOUBLE, pe_ttm DOUBLE, pb DOUBLE, ps DOUBLE, ps_ttm DOUBLE,
    dv_ratio DOUBLE, dv_ttm DOUBLE, total_share DOUBLE, float_share DOUBLE,
    free_share DOUBLE, total_mv DOUBLE, circ_mv DOUBLE
);
"""

CREATE_STOCK_INDEX_MEMBER_ALL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_index_member_all (
    l1_code VARCHAR(20), l1_name VARCHAR(100), l2_code VARCHAR(20),
    l2_name VARCHAR(100), l3_code VARCHAR(20), l3_name VARCHAR(100),
    ts_code VARCHAR(15) NOT NULL, name VARCHAR(100), in_date DATE,
    out_date DATE, is_new VARCHAR(5), PRIMARY KEY (ts_code, l3_code, in_date)
);
"""

CREATE_STOCK_EXPRESS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_express (
    ts_code VARCHAR(15) NOT NULL, ann_date DATE, end_date DATE NOT NULL,
    revenue DOUBLE, operate_profit DOUBLE, total_profit DOUBLE, n_income DOUBLE,
    total_assets DOUBLE, total_hldr_eqy_exc_min_int DOUBLE, diluted_eps DOUBLE,
    diluted_roe DOUBLE, yoy_net_profit DOUBLE, bps DOUBLE, perf_summary TEXT,
    update_flag VARCHAR(10), PRIMARY KEY (ts_code, end_date, ann_date)
);
"""

CREATE_STOCK_SUSPEND_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_suspend (
    ts_code VARCHAR(15) NOT NULL, trade_date DATE NOT NULL,
    suspend_timing VARCHAR(20), suspend_type VARCHAR(50),
    PRIMARY KEY (ts_code, trade_date)
);
"""

CREATE_STOCK_NAMECHANGE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_namechange (
    ts_code VARCHAR(15) NOT NULL, name VARCHAR(50),
    start_date DATE NOT NULL, end_date DATE, ann_date DATE,
    change_reason VARCHAR(200),
    PRIMARY KEY (ts_code, start_date)
);
"""

CREATE_STOCK_DIVIDEND_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_dividend (
    ts_code VARCHAR(15) NOT NULL, end_date DATE NOT NULL, ann_date DATE,
    div_proc VARCHAR(20), stk_div DOUBLE, stk_bo_rate DOUBLE, stk_co_rate DOUBLE,
    cash_div DOUBLE, cash_div_tax DOUBLE, record_date DATE, ex_date DATE,
    pay_date DATE, div_listdate DATE, imp_ann_date DATE, base_date DATE, base_share DOUBLE,
    PRIMARY KEY (ts_code, end_date, ann_date)
);
"""

CREATE_INDEX_WEIGHT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS index_weight (
    index_code VARCHAR(20) NOT NULL, con_code VARCHAR(15) NOT NULL,
    trade_date DATE NOT NULL, weight DOUBLE,
    PRIMARY KEY (index_code, con_code, trade_date)
);
"""

CREATE_STOCK_FORECAST_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_forecast (
    ts_code VARCHAR(15) NOT NULL, ann_date DATE, end_date DATE NOT NULL,
    type VARCHAR(50), p_change_min DOUBLE, p_change_max DOUBLE,
    net_profit_min DOUBLE, net_profit_max DOUBLE, last_parent_net DOUBLE,
    first_ann_date DATE, summary TEXT, change_reason TEXT,
    PRIMARY KEY (ts_code, end_date, ann_date)
);
"""

CREATE_MONEYFLOW_HSGT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS moneyflow_hsgt (
    trade_date DATE NOT NULL PRIMARY KEY,
    ggt_ss DOUBLE, ggt_sz DOUBLE, hgt DOUBLE, sgt DOUBLE,
    north_money DOUBLE, south_money DOUBLE
);
"""

CREATE_TRADE_CALENDAR_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trade_calendar (
    exchange VARCHAR(10) NOT NULL, cal_date DATE NOT NULL, is_open INTEGER,
    pretrade_date DATE, PRIMARY KEY (exchange, cal_date)
);
"""

CREATE_ETL_TASKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS etl_tasks (
    task_id VARCHAR(50) PRIMARY KEY, task_key VARCHAR(100) UNIQUE,
    task_type VARCHAR(50), params_json JSON, status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, started_at TIMESTAMP,
    heartbeat_at TIMESTAMP, finished_at TIMESTAMP, error TEXT,
    progress DOUBLE DEFAULT 0.0
);
"""

ALL_TABLES_SQL = [
    CREATE_STOCK_BASIC_TABLE_SQL,
    CREATE_DAILY_PRICE_TABLE_SQL,
    CREATE_STOCK_CONCEPTS_TABLE_SQL,
    CREATE_STOCK_CONCEPT_DETAILS_TABLE_SQL,
    CREATE_STOCK_MONEYFLOW_TABLE_SQL,
    CREATE_MARKET_INDEX_TABLE_SQL,
    CREATE_STOCK_MARGIN_TABLE_SQL,
    CREATE_STOCK_INCOME_TABLE_SQL,
    CREATE_STOCK_BALANCE_SHEET_TABLE_SQL,
    CREATE_STOCK_CASHFLOW_TABLE_SQL,
    CREATE_STOCK_FINA_INDICATOR_TABLE_SQL,
    CREATE_STOCK_DAILY_BASIC_TABLE_SQL,
    CREATE_STOCK_INDEX_MEMBER_ALL_TABLE_SQL,
    CREATE_STOCK_EXPRESS_TABLE_SQL,
    CREATE_STOCK_SUSPEND_TABLE_SQL,
    CREATE_STOCK_NAMECHANGE_TABLE_SQL,
    CREATE_STOCK_DIVIDEND_TABLE_SQL,
    CREATE_INDEX_WEIGHT_TABLE_SQL,
    CREATE_STOCK_FORECAST_TABLE_SQL,
    CREATE_MONEYFLOW_HSGT_TABLE_SQL,
    CREATE_TRADE_CALENDAR_TABLE_SQL,
    CREATE_ETL_TASKS_TABLE_SQL,
]

# /backend/db/schema.py

# -----------------------------------------------------------------------------
# Jarvis-AI DuckDB 数据库表结构定义
# -----------------------------------------------------------------------------

# -- 用户表 (users) --
CREATE_USERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id              INTEGER PRIMARY KEY DEFAULT nextval('users_id_seq'),
    username        VARCHAR(255) UNIQUE NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    role            VARCHAR(50) DEFAULT 'viewer' NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# -- 股票基础信息表 (stock_basic) --
CREATE_STOCK_BASIC_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_basic (
    ts_code     VARCHAR(15) PRIMARY KEY,
    symbol      VARCHAR(10),
    name        VARCHAR(50),
    area        VARCHAR(50),
    industry    VARCHAR(50),
    market      VARCHAR(50),
    list_date   DATE,
    fullname    VARCHAR(100),
    enname      VARCHAR(100),
    curr_type   VARCHAR(10),
    list_status VARCHAR(5),
    is_hs       VARCHAR(5)
);
"""

# -- 股票日线行情表 (daily_price) --
CREATE_DAILY_PRICE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_price (
    trade_date      DATE NOT NULL,
    ts_code         VARCHAR(15) NOT NULL,
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    pre_close       DOUBLE,
    change          DOUBLE,
    pct_chg         DOUBLE,
    vol             DOUBLE,
    amount          DOUBLE,
    factors         JSON,
    adj_factor      DOUBLE,
    PRIMARY KEY (trade_date, ts_code)
);
"""

# -- 概念分类表 (stock_concepts) --
CREATE_STOCK_CONCEPTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_concepts (
    code        VARCHAR(20) PRIMARY KEY,
    name        VARCHAR(50),
    src         VARCHAR(20)
);
"""

# -- 概念明细表 (stock_concept_details) --
CREATE_STOCK_CONCEPT_DETAILS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_concept_details (
    id          VARCHAR(20),
    concept_name VARCHAR(50),
    ts_code     VARCHAR(15),
    name        VARCHAR(50),
    PRIMARY KEY (id, ts_code)
);
"""

# -- 财务指标表 (stock_financials) --
CREATE_STOCK_FINANCIALS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_financials (
    ts_code     VARCHAR(15),
    ann_date    DATE,
    end_date    DATE,
    eps         DOUBLE,
    dt_eps      DOUBLE,
    total_revenue_ps DOUBLE,
    revenue_ps  DOUBLE,
    capital_rese_ps DOUBLE,
    surplus_rese_ps DOUBLE,
    undist_profit_ps DOUBLE,
    extra_item  DOUBLE,
    profit_dedt DOUBLE,
    gross_margin DOUBLE,
    net_profit_margin DOUBLE,
    roe         DOUBLE,
    roa         DOUBLE,
    debt_to_assets DOUBLE,
    PRIMARY KEY (ts_code, end_date)
);
"""

# -- 个股资金流向表 (stock_moneyflow) --
CREATE_STOCK_MONEYFLOW_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_moneyflow (
    ts_code         VARCHAR(15) NOT NULL,
    trade_date      DATE NOT NULL,
    buy_sm_vol      DOUBLE,
    buy_sm_amount   DOUBLE,
    sell_sm_vol     DOUBLE,
    sell_sm_amount  DOUBLE,
    buy_md_vol      DOUBLE,
    buy_md_amount   DOUBLE,
    sell_md_vol     DOUBLE,
    sell_md_amount  DOUBLE,
    buy_lg_vol      DOUBLE,
    buy_lg_amount   DOUBLE,
    sell_lg_vol     DOUBLE,
    sell_lg_amount  DOUBLE,
    buy_elg_vol     DOUBLE,
    buy_elg_amount  DOUBLE,
    sell_elg_vol    DOUBLE,
    sell_elg_amount DOUBLE,
    net_mf_vol      DOUBLE,
    net_mf_amount   DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
);
"""

# -- 策略推荐记录表 (用于回测) --
CREATE_STRATEGY_RECOMMENDATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_recommendations (
    recommend_date  DATE NOT NULL,
    ts_code         VARCHAR(15) NOT NULL,
    name            VARCHAR(50),
    score           DOUBLE,
    strategy_name   VARCHAR(50),
    filters_used    JSON,
    p1_return       DOUBLE,
    p3_return       DOUBLE,
    p5_return       DOUBLE,
    p10_return      DOUBLE,
    PRIMARY KEY (recommend_date, ts_code, strategy_name)
);
"""

# -- 市场指数表 (market_index) --

CREATE_MARKET_INDEX_TABLE_SQL = """

CREATE TABLE IF NOT EXISTS market_index (

    trade_date      DATE NOT NULL,

    ts_code         VARCHAR(15) NOT NULL, -- e.g., 000001.SH

    open            DOUBLE,

    high            DOUBLE,

    low             DOUBLE,

    close           DOUBLE,

    pre_close       DOUBLE,

    change          DOUBLE,

    pct_chg         DOUBLE,

    vol             DOUBLE,

    amount          DOUBLE,

    PRIMARY KEY (trade_date, ts_code)

);

"""



# -- 市场情绪指标表 (market_sentiment) --

CREATE_MARKET_SENTIMENT_TABLE_SQL = """

CREATE TABLE IF NOT EXISTS market_sentiment (

    trade_date      DATE PRIMARY KEY,

    score           DOUBLE, -- 0-100, 50=Neutral

    label           VARCHAR(20), -- e.g., Panic, Fear, Neutral, Greed, Extreme Greed

    details         JSON -- 存储构成情绪的分项指标

);

"""


# -- 融资融券表 (stock_margin) --

CREATE_STOCK_MARGIN_TABLE_SQL = """

CREATE TABLE IF NOT EXISTS stock_margin (

    ts_code         VARCHAR(15) NOT NULL,

    trade_date      DATE NOT NULL,

    rzye            DOUBLE, -- 融资余额 (Financing Balance)

    rzmre           DOUBLE, -- 融资买入额 (Financing Buy)

    rzche           DOUBLE, -- 融资偿还额 (Financing Repayment)

    rqye            DOUBLE, -- 融券余额 (Margin Short Balance)

    rqmcl           DOUBLE, -- 融券卖出量 (Margin Short Sell)

    rzrqye          DOUBLE, -- 融资融券余额 (Total Margin Balance)

    rqyl            DOUBLE, -- 融券余量 (Margin Short Volume)

    PRIMARY KEY (ts_code, trade_date)

);

"""


# -- 盯盘自选表 (watchlist) --

CREATE_WATCHLIST_TABLE_SQL = """

CREATE TABLE IF NOT EXISTS watchlist (

    ts_code         VARCHAR(15) PRIMARY KEY,

    name            VARCHAR(50),

    remark          VARCHAR(255),

    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

"""


# -- 主线评分历史表 (mainline_scores) --

CREATE_MAINLINE_SCORES_TABLE_SQL = """

CREATE TABLE IF NOT EXISTS mainline_scores (

    trade_date      DATE NOT NULL,

    mapped_name     VARCHAR(50) NOT NULL,

    score           DOUBLE,

    limit_ups       INTEGER,

    stock_count     INTEGER,

    top_stocks      JSON,

    PRIMARY KEY (trade_date, mapped_name)

);

"""

# -- Falcon 运行记录 --
CREATE_FALCON_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS falcon_runs (
    run_id BIGINT PRIMARY KEY DEFAULT nextval('falcon_runs_id_seq'),
    strategy_id VARCHAR(80) NOT NULL,
    strategy_version INTEGER NOT NULL,
    trade_date DATE NOT NULL,
    params_json JSON,
    summary_json JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_deleted BOOLEAN DEFAULT FALSE,
    deleted_at TIMESTAMP,
    deleted_by VARCHAR(120)
);
"""

CREATE_FALCON_PICKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS falcon_picks (
    run_id BIGINT NOT NULL,
    rank_no INTEGER NOT NULL,
    strategy_id VARCHAR(80) NOT NULL,
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    name VARCHAR(80),
    strategy_score DOUBLE,
    confidence DOUBLE,
    signal_label VARCHAR(40),
    score_breakdown JSON,
    is_deleted BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (run_id, rank_no)
);
"""

CREATE_FALCON_EVAL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS falcon_pick_eval (
    run_id BIGINT NOT NULL,
    strategy_id VARCHAR(80) NOT NULL,
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    ret_5d DOUBLE,
    ret_10d DOUBLE,
    hit_5d BOOLEAN,
    hit_10d BOOLEAN,
    PRIMARY KEY (run_id, ts_code)
);
"""

CREATE_FALCON_STATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS falcon_strategy_state (
    strategy_id VARCHAR(80) PRIMARY KEY,
    version INTEGER NOT NULL,
    params_json JSON,
    note VARCHAR(200),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_FALCON_DAILY_SCORE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS falcon_strategy_daily_score (
    strategy_id VARCHAR(80) NOT NULL,
    trade_date DATE NOT NULL,
    score DOUBLE,
    details JSON,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (strategy_id, trade_date)
);
"""

CREATE_FALCON_EVOLUTION_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS falcon_evolution_log (
    id BIGINT PRIMARY KEY DEFAULT nextval('falcon_runs_id_seq'),
    strategy_id VARCHAR(80) NOT NULL,
    prev_version INTEGER,
    next_version INTEGER,
    prev_params JSON,
    next_params JSON,
    score_before DOUBLE,
    score_after DOUBLE,
    promoted BOOLEAN,
    details JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_FALCON_OP_LOG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS falcon_op_log (
    id BIGINT PRIMARY KEY DEFAULT nextval('falcon_runs_id_seq'),
    strategy_id VARCHAR(80),
    op_type VARCHAR(40) NOT NULL,
    run_ids JSON,
    detail JSON,
    operator VARCHAR(120),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# -- ETL 任务持久化表 (etl_tasks) --
CREATE_ETL_TASKS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS etl_tasks (
    task_id         VARCHAR(50) PRIMARY KEY,
    task_key        VARCHAR(100) UNIQUE,
    task_type       VARCHAR(50),
    params_json     JSON,
    status          VARCHAR(20),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at      TIMESTAMP,
    heartbeat_at    TIMESTAMP,
    finished_at     TIMESTAMP,
    error           TEXT,
    progress        DOUBLE DEFAULT 0.0
);
"""

ALL_TABLES_SQL = [
    "CREATE SEQUENCE IF NOT EXISTS users_id_seq START 1;",
    "CREATE SEQUENCE IF NOT EXISTS falcon_runs_id_seq START 1;",
    CREATE_USERS_TABLE_SQL,
    CREATE_STOCK_BASIC_TABLE_SQL,
    CREATE_DAILY_PRICE_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_daily_price_tscode ON daily_price (ts_code);",
    "CREATE INDEX IF NOT EXISTS idx_daily_price_date ON daily_price (trade_date);",
    CREATE_STOCK_CONCEPTS_TABLE_SQL,
    CREATE_STOCK_CONCEPT_DETAILS_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_concept_details_tscode ON stock_concept_details (ts_code);",
    CREATE_STOCK_FINANCIALS_TABLE_SQL,
    CREATE_STOCK_MONEYFLOW_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_moneyflow_date ON stock_moneyflow (trade_date);",
    CREATE_STRATEGY_RECOMMENDATIONS_TABLE_SQL,
    CREATE_MARKET_INDEX_TABLE_SQL,
    CREATE_MARKET_SENTIMENT_TABLE_SQL,
    CREATE_STOCK_MARGIN_TABLE_SQL,
    CREATE_WATCHLIST_TABLE_SQL,
    CREATE_MAINLINE_SCORES_TABLE_SQL,
    CREATE_FALCON_RUNS_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_stock_margin_date ON stock_margin (trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_stock_margin_tscode ON stock_margin (ts_code);",
    CREATE_FALCON_PICKS_TABLE_SQL,
    CREATE_FALCON_EVAL_TABLE_SQL,
    CREATE_FALCON_STATE_TABLE_SQL,
    CREATE_FALCON_DAILY_SCORE_TABLE_SQL,
    CREATE_FALCON_EVOLUTION_LOG_TABLE_SQL,
    CREATE_FALCON_OP_LOG_TABLE_SQL,
    CREATE_ETL_TASKS_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_falcon_runs_strategy_date ON falcon_runs (strategy_id, trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_falcon_picks_run ON falcon_picks (run_id);",
    "CREATE INDEX IF NOT EXISTS idx_falcon_eval_run ON falcon_pick_eval (run_id);",
    "CREATE INDEX IF NOT EXISTS idx_falcon_op_strategy_time ON falcon_op_log (strategy_id, created_at);",
]

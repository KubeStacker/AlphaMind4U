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
    is_hs       VARCHAR(5),
    pinyin      VARCHAR(100),
    pinyin_abbr VARCHAR(20)
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
    net_mf_ratio    DOUBLE,
    PRIMARY KEY (ts_code, trade_date)
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


# -- 季度利润表 (stock_income) --
CREATE_STOCK_INCOME_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_income (
    ts_code         VARCHAR(15) NOT NULL,
    ann_date        DATE,
    f_ann_date      DATE,
    end_date        DATE NOT NULL,
    report_type     INTEGER,
    comp_type       INTEGER,
    end_type        VARCHAR(10),
    basic_eps       DOUBLE,
    diluted_eps     DOUBLE,
    total_revenue   DOUBLE,
    revenue         DOUBLE,
    int_income      DOUBLE,
    prem_earned     DOUBLE,
    comm_income     DOUBLE,
    n_commis_income DOUBLE,
    n_oth_income    DOUBLE,
    n_oth_b_income  DOUBLE,
    prem_income     DOUBLE,
    total_cogs      DOUBLE,
    oper_cost       DOUBLE,
    int_exp         DOUBLE,
    comm_exp        DOUBLE,
    biz_tax_surchg  DOUBLE,
    sell_exp        DOUBLE,
    admin_exp       DOUBLE,
    fin_exp         DOUBLE,
    assets_impair_loss DOUBLE,
    operate_profit  DOUBLE,
    total_profit    DOUBLE,
    income_tax      DOUBLE,
    n_income        DOUBLE,
    n_income_attr_p DOUBLE,
    minority_gain   DOUBLE,
    total_operate_income DOUBLE,
    operate_exp     DOUBLE,
    total_operate_cost DOUBLE,
    PRIMARY KEY (ts_code, end_date, report_type)
);
"""


# -- 财务指标表 (stock_fina_indicator) --
CREATE_STOCK_FINA_INDICATOR_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_fina_indicator (
    ts_code VARCHAR(15) NOT NULL,
    ann_date DATE,
    end_date DATE NOT NULL,
    eps DOUBLE,
    eps_yoy DOUBLE,
    bvps DOUBLE,
    roe DOUBLE,
    roe_yoy DOUBLE,
    net_profit_margin DOUBLE,
    net_profit_margin_yoy DOUBLE,
    gross_profit_margin DOUBLE,
    gross_profit_margin_yoy DOUBLE,
    total_rev DOUBLE,
    total_rev_yoy DOUBLE,
    rev_ps DOUBLE,
    profit DOUBLE,
    profit_yoy DOUBLE,
    profit_ps DOUBLE,
    PRIMARY KEY (ts_code, end_date)
);
"""


# -- 股票日频基础指标表 (stock_daily_basic) --
CREATE_STOCK_DAILY_BASIC_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_daily_basic (
    trade_date       DATE NOT NULL,
    ts_code          VARCHAR(15) NOT NULL,
    close            DOUBLE,
    turnover_rate    DOUBLE,
    turnover_rate_f  DOUBLE,
    volume_ratio     DOUBLE,
    pe               DOUBLE,
    pe_ttm           DOUBLE,
    pb               DOUBLE,
    ps               DOUBLE,
    ps_ttm           DOUBLE,
    dv_ratio         DOUBLE,
    dv_ttm           DOUBLE,
    total_share      DOUBLE,
    float_share      DOUBLE,
    free_share       DOUBLE,
    total_mv         DOUBLE,
    circ_mv          DOUBLE,
    PRIMARY KEY (trade_date, ts_code)
);
"""


# -- 股票申万行业归属表 (stock_index_member_all) --
CREATE_STOCK_INDEX_MEMBER_ALL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_index_member_all (
    l1_code          VARCHAR(20),
    l1_name          VARCHAR(100),
    l2_code          VARCHAR(20),
    l2_name          VARCHAR(100),
    l3_code          VARCHAR(20),
    l3_name          VARCHAR(100),
    ts_code          VARCHAR(15) NOT NULL,
    name             VARCHAR(100),
    in_date          DATE,
    out_date         DATE,
    is_new           VARCHAR(5),
    PRIMARY KEY (ts_code, l3_code, in_date)
);
"""


# -- 业绩快报表 (stock_express) --
CREATE_STOCK_EXPRESS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_express (
    ts_code                      VARCHAR(15) NOT NULL,
    ann_date                     DATE,
    end_date                     DATE NOT NULL,
    revenue                      DOUBLE,
    operate_profit               DOUBLE,
    total_profit                 DOUBLE,
    n_income                     DOUBLE,
    total_assets                 DOUBLE,
    total_hldr_eqy_exc_min_int   DOUBLE,
    diluted_eps                  DOUBLE,
    diluted_roe                  DOUBLE,
    yoy_net_profit               DOUBLE,
    bps                          DOUBLE,
    perf_summary                 TEXT,
    update_flag                  VARCHAR(10),
    PRIMARY KEY (ts_code, end_date, ann_date)
);
"""


# -- 股票因子宽表 (stock_factor_daily) --
CREATE_STOCK_FACTOR_DAILY_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS stock_factor_daily (
    trade_date             DATE NOT NULL,
    ts_code                VARCHAR(15) NOT NULL,
    industry               VARCHAR(100),
    market                 VARCHAR(50),
    sw_l1_name             VARCHAR(100),
    sw_l2_name             VARCHAR(100),
    sw_l3_name             VARCHAR(100),
    close                  DOUBLE,
    pct_chg                DOUBLE,
    ma5                    DOUBLE,
    ma10                   DOUBLE,
    ma20                   DOUBLE,
    ma60                   DOUBLE,
    ma120                  DOUBLE,
    high_250               DOUBLE,
    vol_ma5                DOUBLE,
    avg_ret_60             DOUBLE,
    vol_60                 DOUBLE,
    rps_20                 DOUBLE,
    rps_50                 DOUBLE,
    rps_120                DOUBLE,
    rps_250                DOUBLE,
    turnover_rate          DOUBLE,
    turnover_rate_f        DOUBLE,
    volume_ratio           DOUBLE,
    pe                     DOUBLE,
    pe_ttm                 DOUBLE,
    pb                     DOUBLE,
    ps                     DOUBLE,
    ps_ttm                 DOUBLE,
    total_mv               DOUBLE,
    circ_mv                DOUBLE,
    net_mf_amount          DOUBLE,
    net_mf_ratio           DOUBLE,
    big_order_ratio        DOUBLE,
    roe                    DOUBLE,
    roe_yoy                DOUBLE,
    net_profit_margin      DOUBLE,
    gross_profit_margin    DOUBLE,
    total_rev_yoy          DOUBLE,
    profit_yoy             DOUBLE,
    express_revenue        DOUBLE,
    express_operate_profit DOUBLE,
    express_n_income       DOUBLE,
    express_yoy_net_profit DOUBLE,
    trend_score            DOUBLE,
    liquidity_score        DOUBLE,
    quality_score          DOUBLE,
    value_score            DOUBLE,
    flow_score             DOUBLE,
    event_score            DOUBLE,
    factor_score           DOUBLE,
    PRIMARY KEY (trade_date, ts_code)
);
"""


# -- 盯盘自选表 (watchlist) --

CREATE_WATCHLIST_TABLE_SQL = """

CREATE TABLE IF NOT EXISTS watchlist (

    user_id         INTEGER NOT NULL,

    ts_code         VARCHAR(15) NOT NULL,

    name            VARCHAR(50),

    remark          VARCHAR(255),

    sort_order      INTEGER DEFAULT 0,

    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (user_id, ts_code)

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

# -- 用户AI配置表 (user_ai_config) --
CREATE_USER_AI_CONFIG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS user_ai_config (
    user_id         INTEGER PRIMARY KEY,
    model_provider  VARCHAR(50) DEFAULT 'deepseek',
    model_name      VARCHAR(100),
    api_key         VARCHAR(500),
    base_url        VARCHAR(500),
    system_prompt   TEXT,
    max_tokens      INTEGER DEFAULT 4096,
    temperature     DOUBLE DEFAULT 0.7,
    selected_template_id INTEGER,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# -- 用户分 Provider 的 AI 配置表 --
CREATE_USER_AI_PROVIDER_CONFIG_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS user_ai_provider_configs (
    user_id         INTEGER NOT NULL,
    provider        VARCHAR(50) NOT NULL,
    model_name      VARCHAR(100),
    api_key         VARCHAR(500),
    base_url        VARCHAR(500),
    system_prompt   TEXT,
    max_tokens      INTEGER DEFAULT 1200,
    temperature     DOUBLE DEFAULT 0.35,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, provider)
);
"""

# -- 用户提示词模板表 (user_prompt_templates) --
CREATE_USER_PROMPT_TEMPLATES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS user_prompt_templates (
    id              INTEGER PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    name            VARCHAR(100) NOT NULL,
    content         TEXT NOT NULL,
    is_default      BOOLEAN DEFAULT FALSE,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# -- 用户持仓表 (user_holdings) --
CREATE_USER_HOLDINGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS user_holdings (
    user_id         INTEGER NOT NULL,
    ts_code         VARCHAR(15) NOT NULL,
    shares          DOUBLE DEFAULT 0,
    avg_cost        DOUBLE DEFAULT 0,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, ts_code)
);
"""

# -- AI分析结果缓存表 (ai_analysis_cache) --
CREATE_AI_ANALYSIS_CACHE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ai_analysis_cache (
    id              INTEGER,
    user_id         INTEGER NOT NULL,
    ts_code         VARCHAR(15) NOT NULL,
    trade_date      DATE NOT NULL,
    analysis_result TEXT NOT NULL,
    model_name      VARCHAR(100),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, ts_code, trade_date)
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

# -- 策略广场策略定义表 (strategy_definitions) --
CREATE_STRATEGY_DEFINITIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_definitions (
    strategy_key     VARCHAR(100) PRIMARY KEY,
    name             VARCHAR(100) NOT NULL,
    description      TEXT,
    enabled          BOOLEAN DEFAULT TRUE,
    display_order    INTEGER DEFAULT 100,
    engine_version   VARCHAR(50) DEFAULT 'v1',
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# -- 策略广场观察归档表 (strategy_observations) --
CREATE_STRATEGY_OBSERVATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_observations (
    strategy_key      VARCHAR(100) NOT NULL,
    trade_date        DATE NOT NULL,
    observation_date  DATE NOT NULL,
    ts_code           VARCHAR(15) NOT NULL,
    name              VARCHAR(50),
    reason            VARCHAR(255),
    tags_json         JSON,
    entry_anchor_date DATE NOT NULL,
    trace_json        JSON,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (strategy_key, observation_date, ts_code)
);
"""

# -- 策略广场回测结果表 (strategy_backtest_runs) --
CREATE_STRATEGY_BACKTEST_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_backtest_runs (
    strategy_key            VARCHAR(100) NOT NULL,
    observation_date        DATE NOT NULL,
    ts_code                 VARCHAR(15) NOT NULL,
    entry_anchor_date       DATE NOT NULL,
    entry_price             DOUBLE,
    entry_price_source      VARCHAR(50) DEFAULT 'close_on_anchor',
    status                  VARCHAR(20) DEFAULT 'PENDING',
    ret_3d                  DOUBLE,
    ret_5d                  DOUBLE,
    ret_10d                 DOUBLE,
    max_gain_3d             DOUBLE,
    max_gain_5d             DOUBLE,
    max_gain_10d            DOUBLE,
    max_drawdown_3d         DOUBLE,
    max_drawdown_5d         DOUBLE,
    max_drawdown_10d        DOUBLE,
    last_completed_horizon  INTEGER DEFAULT 0,
    last_eval_date          DATE,
    error                   TEXT,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (strategy_key, observation_date, ts_code)
);
"""

# -- 策略广场摘要表 (strategy_daily_summaries) --
CREATE_STRATEGY_DAILY_SUMMARIES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_daily_summaries (
    strategy_key             VARCHAR(100) NOT NULL,
    trade_date               DATE NOT NULL,
    window_trade_days        INTEGER DEFAULT 120,
    observation_count        INTEGER DEFAULT 0,
    completed_count_3d       INTEGER DEFAULT 0,
    completed_count_5d       INTEGER DEFAULT 0,
    completed_count_10d      INTEGER DEFAULT 0,
    win_rate_3d              DOUBLE,
    win_rate_5d              DOUBLE,
    win_rate_10d             DOUBLE,
    avg_ret_3d               DOUBLE,
    avg_ret_5d               DOUBLE,
    avg_ret_10d              DOUBLE,
    median_ret_3d            DOUBLE,
    median_ret_5d            DOUBLE,
    median_ret_10d           DOUBLE,
    avg_max_gain_3d          DOUBLE,
    avg_max_gain_5d          DOUBLE,
    avg_max_gain_10d         DOUBLE,
    avg_max_drawdown_3d      DOUBLE,
    avg_max_drawdown_5d      DOUBLE,
    avg_max_drawdown_10d     DOUBLE,
    summary_text             TEXT,
    updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (strategy_key, trade_date)
);
"""

# -- 文档阅读进度表 (doc_reading_progress) --
CREATE_DOC_READING_PROGRESS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS doc_reading_progress (
    user_id         INTEGER NOT NULL,
    doc_id          VARCHAR(100) NOT NULL,
    scroll_position INTEGER DEFAULT 0,
    last_line       INTEGER DEFAULT 0,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, doc_id)
);
"""

# -- 用户自定义文档标签表 (doc_user_tags) --
CREATE_DOC_USER_TAGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS doc_user_tags (
    id              INTEGER PRIMARY KEY DEFAULT nextval('doc_user_tags_id_seq'),
    user_id         INTEGER NOT NULL,
    tag_name        VARCHAR(50) NOT NULL,
    color           VARCHAR(20) DEFAULT '#64748b',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, tag_name)
);
"""

# -- 文档笔记/点评表 (doc_notes) --
CREATE_DOC_NOTES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS doc_notes (
    id              INTEGER PRIMARY KEY DEFAULT nextval('doc_notes_id_seq'),
    user_id         INTEGER NOT NULL,
    doc_id          VARCHAR(100) NOT NULL,
    note_content    TEXT NOT NULL,
    note_type       VARCHAR(20) DEFAULT 'note',
    line_number     INTEGER DEFAULT 0,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# -- 文档标签关联表 (doc_tag_mapping) --
# -- AI趋势表 (ai_trends) --
CREATE_AI_TRENDS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS ai_trends (
    id INTEGER PRIMARY KEY DEFAULT nextval('ai_trends_id_seq'),
    source VARCHAR(50) NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    summary TEXT,
    keywords JSON,
    relevance_score DOUBLE DEFAULT 0,
    published_date DATE,
    collected_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT FALSE
);
"""

CREATE_DOC_TAG_MAPPING_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS doc_tag_mapping (
    user_id         INTEGER NOT NULL,
    doc_id          VARCHAR(100) NOT NULL,
    tag_id          INTEGER NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, doc_id, tag_id)
);
"""

ALL_TABLES_SQL = [
    "CREATE SEQUENCE IF NOT EXISTS users_id_seq START 1;",
    "CREATE SEQUENCE IF NOT EXISTS doc_user_tags_id_seq START 1;",
    "CREATE SEQUENCE IF NOT EXISTS doc_notes_id_seq START 1;",
    "CREATE SEQUENCE IF NOT EXISTS ai_trends_id_seq START 1;",
    CREATE_USERS_TABLE_SQL,
    CREATE_STOCK_BASIC_TABLE_SQL,
    CREATE_DAILY_PRICE_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_daily_price_tscode ON daily_price (ts_code);",
    "CREATE INDEX IF NOT EXISTS idx_daily_price_date ON daily_price (trade_date);",
    CREATE_STOCK_CONCEPTS_TABLE_SQL,
    CREATE_STOCK_CONCEPT_DETAILS_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_concept_details_tscode ON stock_concept_details (ts_code);",
    CREATE_STOCK_MONEYFLOW_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_moneyflow_date ON stock_moneyflow (trade_date);",
    CREATE_STOCK_DAILY_BASIC_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_stock_daily_basic_date ON stock_daily_basic (trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_stock_daily_basic_tscode ON stock_daily_basic (ts_code);",
    CREATE_MARKET_INDEX_TABLE_SQL,
    CREATE_MARKET_SENTIMENT_TABLE_SQL,
    CREATE_STOCK_MARGIN_TABLE_SQL,
    CREATE_STOCK_INCOME_TABLE_SQL,
    CREATE_STOCK_FINA_INDICATOR_TABLE_SQL,
    CREATE_STOCK_INDEX_MEMBER_ALL_TABLE_SQL,
    CREATE_STOCK_EXPRESS_TABLE_SQL,
    CREATE_STOCK_FACTOR_DAILY_TABLE_SQL,
    CREATE_WATCHLIST_TABLE_SQL,
    CREATE_MAINLINE_SCORES_TABLE_SQL,
    CREATE_USER_AI_CONFIG_TABLE_SQL,
    CREATE_USER_AI_PROVIDER_CONFIG_TABLE_SQL,
    CREATE_USER_PROMPT_TEMPLATES_TABLE_SQL,
    CREATE_USER_HOLDINGS_TABLE_SQL,
    CREATE_AI_ANALYSIS_CACHE_TABLE_SQL,
    CREATE_ETL_TASKS_TABLE_SQL,
    CREATE_STRATEGY_DEFINITIONS_TABLE_SQL,
    CREATE_STRATEGY_OBSERVATIONS_TABLE_SQL,
    CREATE_STRATEGY_BACKTEST_RUNS_TABLE_SQL,
    CREATE_STRATEGY_DAILY_SUMMARIES_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_stock_margin_date ON stock_margin (trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_stock_margin_tscode ON stock_margin (ts_code);",
    "CREATE INDEX IF NOT EXISTS idx_stock_index_member_tscode ON stock_index_member_all (ts_code);",
    "CREATE INDEX IF NOT EXISTS idx_stock_express_tscode ON stock_express (ts_code);",
    "CREATE INDEX IF NOT EXISTS idx_stock_factor_daily_date ON stock_factor_daily (trade_date);",
    "CREATE INDEX IF NOT EXISTS idx_stock_factor_daily_tscode ON stock_factor_daily (ts_code);",
    "CREATE INDEX IF NOT EXISTS idx_ai_analysis_cache_user_tscode ON ai_analysis_cache (user_id, ts_code);",
    "CREATE INDEX IF NOT EXISTS idx_strategy_observations_date ON strategy_observations (observation_date);",
    "CREATE INDEX IF NOT EXISTS idx_strategy_observations_key ON strategy_observations (strategy_key);",
    "CREATE INDEX IF NOT EXISTS idx_strategy_backtest_status ON strategy_backtest_runs (status);",
    "CREATE INDEX IF NOT EXISTS idx_strategy_summary_date ON strategy_daily_summaries (trade_date);",
    CREATE_DOC_READING_PROGRESS_TABLE_SQL,
    CREATE_DOC_USER_TAGS_TABLE_SQL,
    CREATE_DOC_NOTES_TABLE_SQL,
    CREATE_AI_TRENDS_TABLE_SQL,
    CREATE_DOC_TAG_MAPPING_TABLE_SQL,
    "CREATE INDEX IF NOT EXISTS idx_doc_notes_user_doc ON doc_notes (user_id, doc_id);",
    "CREATE INDEX IF NOT EXISTS idx_doc_tag_mapping_user_doc ON doc_tag_mapping (user_id, doc_id);",
]

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

ALL_TABLES_SQL = [
    "CREATE SEQUENCE IF NOT EXISTS users_id_seq START 1;",
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
    CREATE_MAINLINE_SCORES_TABLE_SQL
]

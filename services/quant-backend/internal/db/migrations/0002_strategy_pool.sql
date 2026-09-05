-- 0002 月度 Top20 股票池快照（universe.md 口径，修订另存版本不倒改）。
CREATE TABLE IF NOT EXISTS strategy_pool_month (
    effect_month TEXT PRIMARY KEY, rank_month TEXT NOT NULL,
    range_from TEXT NOT NULL, range_to TEXT NOT NULL,
    dataset_v TEXT NOT NULL, created_at TEXT DEFAULT (strftime('%Y-%m-%d %H:%M:%S','now'))
);
CREATE TABLE IF NOT EXISTS strategy_pool_entry (
    effect_month TEXT NOT NULL, rank INTEGER NOT NULL, ts_code TEXT NOT NULL, amount REAL NOT NULL,
    PRIMARY KEY (effect_month, rank)
);

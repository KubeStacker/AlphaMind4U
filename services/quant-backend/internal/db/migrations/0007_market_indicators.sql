-- 市场状态与公共特征快照表：日批与回测共享，每个日期只算一次。
CREATE TABLE IF NOT EXISTS market_indicators (
    trade_date TEXT PRIMARY KEY,
    breadth REAL NOT NULL,
    breadth_prev REAL,
    breadth_5ago REAL,
    volume_ratio REAL,
    index_trend INTEGER NOT NULL,
    data_ok INTEGER NOT NULL DEFAULT 1
);

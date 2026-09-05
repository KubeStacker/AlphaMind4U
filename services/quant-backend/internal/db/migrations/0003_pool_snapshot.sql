-- 月池完整内容原子追加，保留旧可变表作为历史，不伪造来源时间。
CREATE TABLE IF NOT EXISTS strategy_pool_snapshot (
    id INTEGER PRIMARY KEY,
    effect_month TEXT NOT NULL,
    version TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_pool_snapshot_month ON strategy_pool_snapshot(effect_month, id);

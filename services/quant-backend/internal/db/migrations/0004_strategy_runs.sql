-- 研究任务、冻结输入和已发布结果，失败及取消记录保留。
CREATE TABLE IF NOT EXISTS strategy_runs (
    run_id TEXT PRIMARY KEY, request_key TEXT NOT NULL, request_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('PENDING','RUNNING','COMPLETED','BLOCKED','FAILED','CANCELLED')),
    progress REAL NOT NULL DEFAULT 0, input_hash TEXT NOT NULL DEFAULT '',
    model_version TEXT NOT NULL, input_json TEXT NOT NULL DEFAULT '', result_json TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_strategy_runs_queue ON strategy_runs(status, created_at, run_id);
CREATE INDEX IF NOT EXISTS idx_strategy_runs_recent ON strategy_runs(created_at, run_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_runs_active ON strategy_runs(request_key)
    WHERE status IN ('PENDING','RUNNING');

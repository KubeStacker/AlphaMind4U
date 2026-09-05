-- 0001 财务与事件表首次采集时间（历史时点起点，后续修订靠 UPDATE 对比）。
ALTER TABLE stock_income ADD COLUMN first_seen_at TEXT;
ALTER TABLE stock_balance_sheet ADD COLUMN first_seen_at TEXT;
ALTER TABLE stock_cashflow ADD COLUMN first_seen_at TEXT;
ALTER TABLE stock_fina_indicator ADD COLUMN first_seen_at TEXT;
ALTER TABLE stock_dividend ADD COLUMN first_seen_at TEXT;
ALTER TABLE stock_forecast ADD COLUMN first_seen_at TEXT;
ALTER TABLE stock_express ADD COLUMN first_seen_at TEXT;

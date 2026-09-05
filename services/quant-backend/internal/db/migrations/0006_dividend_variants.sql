-- 同公告日同阶段也可能有不同基准股本与方案，内容去重替代四列唯一约束。
CREATE TABLE stock_dividend_variants (
    ts_code TEXT NOT NULL, end_date TEXT NOT NULL, ann_date TEXT,
    div_proc TEXT, stk_div REAL, stk_bo_rate REAL, stk_co_rate REAL,
    cash_div REAL, cash_div_tax REAL, record_date TEXT, ex_date TEXT,
    pay_date TEXT, div_listdate TEXT, imp_ann_date TEXT, base_date TEXT, base_share REAL,
    first_seen_at TEXT
);
INSERT INTO stock_dividend_variants (
    ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,
    record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share,first_seen_at
) SELECT ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,
    record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share,first_seen_at
FROM stock_dividend;
DROP TABLE stock_dividend;
ALTER TABLE stock_dividend_variants RENAME TO stock_dividend;
CREATE INDEX idx_dividend_event ON stock_dividend(ts_code,end_date,ann_date,div_proc);

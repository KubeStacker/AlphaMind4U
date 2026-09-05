-- 保留同公告日的不同分红阶段，旧记录和首次采集时间逐列迁移。
CREATE TABLE stock_dividend_stages (
    ts_code TEXT NOT NULL, end_date TEXT NOT NULL, ann_date TEXT,
    div_proc TEXT, stk_div REAL, stk_bo_rate REAL, stk_co_rate REAL,
    cash_div REAL, cash_div_tax REAL, record_date TEXT, ex_date TEXT,
    pay_date TEXT, div_listdate TEXT, imp_ann_date TEXT, base_date TEXT, base_share REAL,
    first_seen_at TEXT,
    PRIMARY KEY (ts_code, end_date, ann_date, div_proc)
);
INSERT INTO stock_dividend_stages (
    ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,
    record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share,first_seen_at
) SELECT ts_code,end_date,ann_date,div_proc,stk_div,stk_bo_rate,stk_co_rate,cash_div,cash_div_tax,
    record_date,ex_date,pay_date,div_listdate,imp_ann_date,base_date,base_share,first_seen_at
FROM stock_dividend;
DROP TABLE stock_dividend;
ALTER TABLE stock_dividend_stages RENAME TO stock_dividend;

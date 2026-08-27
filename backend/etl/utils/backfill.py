import logging
import time
import pandas as pd
import arrow
from etl.sync import sync_engine
from db.connection import fetch_df, get_db_connection
from etl.utils.art_safe_write import dedupe_by_key, update_then_insert_missing

logger = logging.getLogger(__name__)


def safe_backfill(days=3):
    logger.info("开始安全补全最近 %d 天的数据...", days)

    # 1. 获取所有股票
    stocks_df = fetch_df("SELECT ts_code FROM stock_basic")
    if stocks_df.empty:
        logger.warning("未发现股票基础信息，请先运行 sync_stock_basic")
        return

    all_stocks = stocks_df['ts_code'].tolist()

    # 2. 确定日期范围
    end_date = arrow.now().format("YYYYMMDD")
    start_date = arrow.now().shift(days=-days).format("YYYYMMDD")

    logger.info("同步范围: %s 至 %s, 共 %d 只股票", start_date, end_date, len(all_stocks))

    count = 0
    success = 0
    for ts_code in all_stocks:
        count += 1
        try:
            df = sync_engine.provider.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

            if not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                if 'adj_factor' not in df.columns:
                    df['adj_factor'] = 1.0

                cols = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close',
                        'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'adj_factor']
                df_to_save = dedupe_by_key(
                    df[cols],
                    key_cols=["trade_date", "ts_code"],
                    table_name="daily_price",
                    logger=logger,
                )
                with get_db_connection() as con:
                    update_then_insert_missing(
                        con,
                        df=df_to_save,
                        table_name="daily_price",
                        view_name="backfill_daily_view",
                        columns=cols,
                        key_cols=["trade_date", "ts_code"],
                        logger=logger,
                    )

                success += 1

            if count % 50 == 0:
                logger.info("回补进度: %d/%d, 成功: %d", count, len(all_stocks), success)

            time.sleep(0.5)

        except Exception as e:
            logger.warning("同步 %s 失败: %s", ts_code, e)
            time.sleep(2)

    logger.info("安全补全完成: 共 %d 只, 成功 %d", len(all_stocks), success)


if __name__ == "__main__":
    safe_backfill()

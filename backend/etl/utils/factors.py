import arrow
import logging
import json
from db.connection import get_db_connection

logger = logging.getLogger(__name__)

class FactorCalculator:
    def calculate_daily(self, trade_date: str):
        """ 
        计算全市场指定交易日的技术因子 (MA, RPS等) 并持久化到 daily_price.factors 字段。
        """
        logger.info(f"正在通过 SQL 计算并更新 {trade_date} 的技术指标因子...")
        
        try:
            target_date = arrow.get(trade_date).format("YYYY-MM-DD")
            start_date = arrow.get(trade_date).shift(days=-450).format("YYYY-MM-DD")
        except Exception as e:
            logger.error(f"日期格式解析失败 {trade_date}: {e}")
            return

        update_query = """
        WITH RawData AS (
            SELECT 
                ts_code, 
                trade_date, 
                close,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as ma60,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) as ma120,
                MAX(high) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) as high_250,
                AVG(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as avg_ret_60,
                STDDEV(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as vol_60,
                (close - LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_20,
                (close - LAG(close, 50) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 50) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_50,
                (close - LAG(close, 120) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 120) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_120,
                (close - LAG(close, 250) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 250) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_250,
                COUNT(*) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as row_num
            FROM daily_price
            WHERE trade_date BETWEEN ? AND ?
        ),
        FilteredData AS (
            SELECT * FROM RawData WHERE trade_date = ? AND row_num >= 20
        ),
        RPSData AS (
            SELECT 
                *,
                PERCENT_RANK() OVER (ORDER BY mom_20) * 100 as rps_20,
                PERCENT_RANK() OVER (ORDER BY mom_50) * 100 as rps_50,
                PERCENT_RANK() OVER (ORDER BY mom_120) * 100 as rps_120,
                PERCENT_RANK() OVER (ORDER BY mom_250) * 100 as rps_250
            FROM FilteredData
        ),
        FinalFactors AS (
            SELECT 
                ts_code,
                trade_date,
                json_object(
                    'ma5', CASE WHEN isnan(ma5) THEN NULL ELSE round(ma5, 2) END, 
                    'ma10', CASE WHEN isnan(ma10) THEN NULL ELSE round(ma10, 2) END, 
                    'ma20', CASE WHEN isnan(ma20) THEN NULL ELSE round(ma20, 2) END, 
                    'ma60', CASE WHEN isnan(ma60) THEN NULL ELSE round(ma60, 2) END, 
                    'ma120', CASE WHEN isnan(ma120) THEN NULL ELSE round(ma120, 2) END, 
                    'high_250', CASE WHEN isnan(high_250) THEN NULL ELSE round(high_250, 2) END,
                    'avg_ret_60', CASE WHEN isnan(avg_ret_60) THEN NULL ELSE round(avg_ret_60, 4) END,
                    'vol_60', CASE WHEN isnan(vol_60) THEN NULL ELSE round(vol_60, 4) END,
                    'rps_20', CASE WHEN isnan(rps_20) THEN NULL ELSE round(rps_20, 1) END,
                    'rps_50', CASE WHEN isnan(rps_50) THEN NULL ELSE round(rps_50, 1) END,
                    'rps_120', CASE WHEN isnan(rps_120) THEN NULL ELSE round(rps_120, 1) END,
                    'rps_250', CASE WHEN isnan(rps_250) THEN NULL ELSE round(rps_250, 1) END
                ) as factors_json
            FROM RPSData
        )
        UPDATE daily_price
        SET factors = sub.factors_json
        FROM FinalFactors sub
        WHERE daily_price.ts_code = sub.ts_code AND daily_price.trade_date = sub.trade_date
        """
        
        try:
            with get_db_connection() as con:
                con.execute(update_query, [start_date, target_date, target_date])
                logger.info(f"已成功完成 {target_date} 的因子 SQL 更新")
        except Exception as e:
            logger.error(f"SQL 因子更新失败 {target_date}: {e}")

    def calculate_batch(self, start_date_str: str, end_date_str: str):
        logger.info(f"正在批量计算 {start_date_str} 至 {end_date_str} 的因子...")
        
        window_start = arrow.get(start_date_str).shift(days=-450).format("YYYY-MM-DD")
        
        update_query = """
        WITH RawData AS (
            SELECT 
                ts_code, 
                trade_date, 
                close,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as ma60,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 119 PRECEDING AND CURRENT ROW) as ma120,
                MAX(high) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 249 PRECEDING AND CURRENT ROW) as high_250,
                AVG(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as avg_ret_60,
                STDDEV(pct_chg) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as vol_60,
                (close - LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_20,
                (close - LAG(close, 50) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 50) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_50,
                (close - LAG(close, 120) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 120) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_120,
                (close - LAG(close, 250) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 250) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_250,
                COUNT(*) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as row_num
            FROM daily_price
            WHERE trade_date BETWEEN ? AND ?
        ),
        FilteredData AS (
            SELECT * FROM RawData WHERE trade_date BETWEEN ? AND ? AND row_num >= 20
        ),
        RPSData AS (
            SELECT 
                *,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_20) * 100 as rps_20,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_50) * 100 as rps_50,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_120) * 100 as rps_120,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_250) * 100 as rps_250
            FROM FilteredData
        ),
        FinalFactors AS (
            SELECT 
                ts_code,
                trade_date,
                json_object(
                    'ma5', CASE WHEN isnan(ma5) THEN NULL ELSE round(ma5, 2) END, 
                    'ma10', CASE WHEN isnan(ma10) THEN NULL ELSE round(ma10, 2) END, 
                    'ma20', CASE WHEN isnan(ma20) THEN NULL ELSE round(ma20, 2) END, 
                    'ma60', CASE WHEN isnan(ma60) THEN NULL ELSE round(ma60, 2) END, 
                    'ma120', CASE WHEN isnan(ma120) THEN NULL ELSE round(ma120, 2) END, 
                    'high_250', CASE WHEN isnan(high_250) THEN NULL ELSE round(high_250, 2) END,
                    'avg_ret_60', CASE WHEN isnan(avg_ret_60) THEN NULL ELSE round(avg_ret_60, 4) END,
                    'vol_60', CASE WHEN isnan(vol_60) THEN NULL ELSE round(vol_60, 4) END,
                    'rps_20', CASE WHEN isnan(rps_20) THEN NULL ELSE round(rps_20, 1) END,
                    'rps_50', CASE WHEN isnan(rps_50) THEN NULL ELSE round(rps_50, 1) END,
                    'rps_120', CASE WHEN isnan(rps_120) THEN NULL ELSE round(rps_120, 1) END,
                    'rps_250', CASE WHEN isnan(rps_250) THEN NULL ELSE round(rps_250, 1) END
                ) as factors_json
            FROM RPSData
        )
        UPDATE daily_price
        SET factors = sub.factors_json
        FROM FinalFactors sub
        WHERE daily_price.ts_code = sub.ts_code AND daily_price.trade_date = sub.trade_date
        """
        
        try:
            with get_db_connection() as con:
                con.execute(update_query, [window_start, end_date_str, start_date_str, end_date_str])
                logger.info(f"已完成 {start_date_str} 至 {end_date_str} 的因子批量更新")
        except Exception as e:
            logger.error(f"批量因子更新失败 {start_date_str}-{end_date_str}: {e}")

factor_calculator = FactorCalculator()

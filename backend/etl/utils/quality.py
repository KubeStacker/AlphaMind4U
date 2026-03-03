# /backend/etl/quality.py

import arrow
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from db.connection import fetch_df
from etl.calendar import trading_calendar
import time
import threading

class DataQualityChecker:
    """
    数据质量和完整性检查器。
    """

    def __init__(self):
        self._cache = {}
        self._cache_lock = threading.Lock()
        self._cache_ttl = 300  # 5分钟缓存

    def _get_cache(self, key):
        with self._cache_lock:
            if key in self._cache:
                cached_time, cached_value = self._cache[key]
                if time.time() - cached_time < self._cache_ttl:
                    return cached_value
        return None

    def _set_cache(self, key, value):
        with self._cache_lock:
            self._cache[key] = (time.time(), value)

    def get_integrity_report(self, table_name: str, date_column: str, start_date: str, end_date: str, expected_min_count: int = 1, use_cache: bool = True) -> list[dict]:
        """
        优化后的数据完整性检查：单次聚合查询。
        """
        cache_key = f"{table_name}:{date_column}:{start_date}:{end_date}:{expected_min_count}"
        cached = self._get_cache(cache_key)
        if cached is not None and use_cache:
            return cached

        # 使用 DuckDB 的日期函数生成序列，并直接 LEFT JOIN 结果，大幅提升速度
        query = f"""
        WITH date_series AS (
            SELECT CAST(i AS DATE) as date
            FROM generate_series(CAST(? AS DATE), CAST(? AS DATE), INTERVAL 1 DAY) t(i)
        ),
        daily_counts AS (
            SELECT 
                {date_column} as date, 
                COUNT(*) as count 
            FROM 
                {table_name} 
            WHERE 
                {date_column} BETWEEN ? AND ?
            GROUP BY 
                {date_column}
        )
        SELECT 
            ds.date,
            COALESCE(dc.count, 0) as count
        FROM date_series ds
        LEFT JOIN daily_counts dc ON ds.date = dc.date
        ORDER BY ds.date
        """
        
        try:
            results_df = fetch_df(query, params=[start_date, end_date, start_date, end_date])
        except Exception as e:
            import logging
            logging.getLogger(__name__).error(f"查询完整性报告失败 (table={table_name}): {e}")
            return []

        report = []
        for _, row in results_df.iterrows():
            current_date = row['date']
            
            # 确保 current_date 是 date 对象
            if isinstance(current_date, str):
                try:
                    current_date = arrow.get(current_date).date()
                except:
                    continue
            elif hasattr(current_date, 'to_pydatetime'): # pandas Timestamp
                current_date = current_date.to_pydatetime().date()
            elif hasattr(current_date, 'date'): # other datetime-like
                current_date = current_date.date()
            
            date_str = current_date.strftime('%Y-%m-%d')
            count = int(row['count'])
            
            is_trading = trading_calendar.is_trading_day(current_date)
            
            day_report = {
                "date": date_str,
                "count": count,
                "status": "UNKNOWN"
            }
            
            if is_trading:
                if count >= expected_min_count:
                    day_report['status'] = "FULL"
                elif count > 0:
                    day_report['status'] = "PARTIAL"
                else:
                    day_report['status'] = "MISSING"
            else:
                day_report['status'] = "FULL" if count > 0 else "HOLIDAY"
            
            report.append(day_report)
        
        self._set_cache(cache_key, report)
        return report

    def get_daily_price_integrity_report(self, start_date: str, end_date: str) -> list[dict]:
        # A股日线，预期 4000+ 条
        return self.get_integrity_report("daily_price", "trade_date", start_date, end_date, expected_min_count=4000)

    def get_moneyflow_integrity_report(self, start_date: str, end_date: str) -> list[dict]:
        # 资金流，预期 4000+ 条
        return self.get_integrity_report("stock_moneyflow", "trade_date", start_date, end_date, expected_min_count=4000)

    def get_index_integrity_report(self, start_date: str, end_date: str) -> list[dict]:
        # 指数数据，一个交易日至少有 1 条 (上证指数)
        return self.get_integrity_report("market_index", "trade_date", start_date, end_date, expected_min_count=1)

    def get_margin_integrity_report(self, start_date: str, end_date: str) -> list[dict]:
        # 融资融券数据，预期 4000+ 条
        return self.get_integrity_report("stock_margin", "trade_date", start_date, end_date, expected_min_count=4000)

    def get_comprehensive_report(self, start_date: str, end_date: str) -> dict:
        """
        获取全方位的指标数据监控报告。
        使用并行查询提升速度。
        """
        # 使用并行查询
        results = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            future_to_key = {
                executor.submit(self.get_daily_price_integrity_report, start_date, end_date): "daily_price",
                executor.submit(self.get_moneyflow_integrity_report, start_date, end_date): "stock_moneyflow",
                executor.submit(self.get_index_integrity_report, start_date, end_date): "market_index",
                executor.submit(self.get_margin_integrity_report, start_date, end_date): "stock_margin"
            }
            
            for future in as_completed(future_to_key):
                key = future_to_key[future]
                try:
                    results[key] = future.result()
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).error(f"获取 {key} 完整性报告失败: {e}")
                    results[key] = []
        
        return results

# 创建全局实例
quality_checker = DataQualityChecker()

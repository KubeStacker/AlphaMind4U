import arrow
import time
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df, get_fresh_connection
from etl.calendar import trading_calendar

class DailyPriceTask(BaseTask):
    def _get_expected_daily_count(self) -> int:
        """估算单日应有的行情条数，用于识别不完整交易日。"""
        try:
            df = fetch_df(
                """
                SELECT COUNT(*) AS cnt
                FROM stock_basic
                WHERE list_status = 'L'
                """
            )
            if not df.empty and int(df.iloc[0]["cnt"]) > 0:
                # 留出少量停牌/接口缺失波动空间
                return max(1000, int(df.iloc[0]["cnt"] * 0.95))
        except Exception:
            pass
        return 1000

    def sync(self, years: int = 1, force: bool = False, calc_factors: bool = True):
        """ 同步 A 股日线数据 """
        self.logger.info(f"开始同步日线数据 (年限: {years})...")
        self._sync_by_date(years, force, calc_factors)

    def _sync_by_date(self, years: int, force: bool, calc_factors: bool):
        """ 按日期同步 (适用于 Tushare) """
        # 优化：获取当前可同步的最晚日期 (如果是收盘后则包含今日)
        end_date = arrow.get(trading_calendar.get_latest_sync_date())
        start_date = end_date.shift(years=-years)
        
        # 1. 获取目标范围内的所有交易日
        target_dates = {r.format("YYYY-MM-DD") for r in arrow.Arrow.range('day', start_date, end_date) 
                         if trading_calendar.is_trading_day(r.date())}
        
        # 2. 检查已存在日期
        existing_dates = set()
        if not force:
            expected_min_count = self._get_expected_daily_count()
            query = """
                SELECT trade_date
                FROM daily_price
                WHERE trade_date BETWEEN ? AND ?
                GROUP BY trade_date
                HAVING COUNT(*) >= ?
            """
            df_existing = fetch_df(
                query, params=[start_date.date(), end_date.date(), expected_min_count]
            )
            if not df_existing.empty:
                existing_dates = {d.strftime("%Y-%m-%d") for d in df_existing['trade_date']}

        dates_to_sync = sorted(list(target_dates - existing_dates), reverse=True)
        
        if not dates_to_sync:
            self.logger.info("所有日期数据已完整。")
            return

        for d_str in dates_to_sync:
            self._process_single_date(d_str, calc_factors)

    def _process_single_date(self, date_str: str, calc_factors: bool):
        self.logger.info(f"正在同步 {date_str} 的行情...")
        try:
            # 1. 获取行情
            df_daily = self.provider.daily(trade_date=date_str)
            if df_daily.empty:
                self.logger.warning(f"{date_str} 没有行情数据，未写入数据库")
                return

            # 2. 获取复权因子
            df_adj = self.provider.adj_factor(trade_date=date_str)
            if not df_adj.empty:
                df = pd.merge(df_daily, df_adj[['ts_code', 'adj_factor']], on='ts_code', how='left')
                df['adj_factor'] = df['adj_factor'].fillna(1.0)
            else:
                df = df_daily.copy()
                if 'adj_factor' not in df.columns:
                    df['adj_factor'] = 1.0

            df['factors'] = '{}'
            # 确保日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 保存
            self._save_to_db(df)
            
            if calc_factors:
                self.calculate_factors(date_str)
                
            time.sleep(0.5) # 避免限流

        except Exception as e:
            self.logger.error(f"同步 {date_str} 失败: {e}")
            time.sleep(5)

    def _save_to_db(self, df: pd.DataFrame):
        cols = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'factors', 'adj_factor']
        # 确保列对齐
        for c in cols:
            if c not in df.columns:
                df[c] = None
        
        df_to_save = df[cols]
        with get_db_connection() as con:
            con.execute("INSERT INTO daily_price SELECT * FROM df_to_save ON CONFLICT (trade_date, ts_code) DO NOTHING")

    def calculate_factors(self, trade_date: str):
        # 使用独立的计算器以避免循环引用
        from etl.utils.factors import factor_calculator
        factor_calculator.calculate_daily(trade_date)

    def calculate_factors_batch(self, start_date: str, end_date: str):
        from etl.utils.factors import factor_calculator
        factor_calculator.calculate_batch(start_date, end_date)

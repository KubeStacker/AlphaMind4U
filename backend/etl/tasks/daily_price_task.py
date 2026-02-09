import arrow
import time
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df, get_fresh_connection
from core.calendar import trading_calendar

class DailyPriceTask(BaseTask):
    def sync(self, years: int = 1, force: bool = False, calc_factors: bool = True):
        """ 
        同步 A 股日线数据。
        根据 Provider 的 sync_mode 自动选择最优策略。
        """
        self.logger.info(f"开始同步日线数据 (模式: {self.provider.sync_mode}, 年限: {years})...")
        
        if self.provider.sync_mode == 'date':
            self._sync_by_date(years, force, calc_factors)
        else:
            self._sync_by_ticker(years, force, calc_factors)

    def _sync_by_date(self, years: int, force: bool, calc_factors: bool):
        """ 按日期同步 (适用于 Tushare) """
        end_date = arrow.now()
        start_date = end_date.shift(years=-years)
        
        # 1. 获取目标范围内的所有交易日
        target_dates = {r.format("YYYY-MM-DD") for r in arrow.Arrow.range('day', start_date, end_date) 
                         if trading_calendar.is_trading_day(r.date())}
        
        # 2. 检查已存在日期
        existing_dates = set()
        if not force:
            query = "SELECT trade_date FROM daily_price WHERE trade_date BETWEEN ? AND ? GROUP BY trade_date HAVING COUNT(*) > 1000"
            df_existing = fetch_df(query, params=[start_date.date(), end_date.date()])
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
                self.logger.warning(f"{date_str} 没有行情数据")
                return

            # 2. 获取复权因子
            df_adj = self.provider.adj_factor(trade_date=date_str)
            if not df_adj.empty:
                df = pd.merge(df_daily, df_adj[['ts_code', 'adj_factor']], on='ts_code', how='left')
                df['adj_factor'] = df['adj_factor'].fillna(1.0)
            else:
                df = df_daily.copy()
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

    def _sync_by_ticker(self, years: int, force: bool, calc_factors: bool):
        """ 按股票同步 (适用于 Akshare) """
        # 1. 获取所有股票列表
        stocks_df = fetch_df("SELECT ts_code FROM stock_basic")
        if stocks_df.empty:
            self.logger.warning("股票列表为空，请先同步 stock_basic")
            return
        
        all_stocks = stocks_df['ts_code'].tolist()
        self.logger.info(f"需同步 {len(all_stocks)} 只股票的历史数据...")
        
        start_date = arrow.now().shift(years=-years).format("YYYY-MM-DD")
        end_date = arrow.now().format("YYYY-MM-DD")
        
        count = 0
        for ts_code in all_stocks:
            try:
                # 简单检查该股票是否已有足够数据 (优化点: 可以更精细)
                if not force:
                    check = fetch_df("SELECT COUNT(*) as c FROM daily_price WHERE ts_code=? AND trade_date >= ?", params=[ts_code, start_date])
                    if check['c'][0] > 200: # 假设一年约250交易日
                        continue
                
                self.logger.info(f"正在同步 {ts_code} ({count}/{len(all_stocks)})...")
                df = self.provider.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                if not df.empty:
                    df['factors'] = '{}'
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    # Akshare 历史数据通常已复权 (qfq)，所以 adj_factor 设为 1 或不设
                    if 'adj_factor' not in df.columns:
                        df['adj_factor'] = 1.0
                        
                    self._save_to_db(df)
                
                count += 1
                time.sleep(0.3) # Akshare 本地库可能不需要 sleep，但如果频繁请求网络接口则需要
                
            except Exception as e:
                self.logger.error(f"同步 {ts_code} 失败: {e}")

        # 批量因子计算 (因为按股票同步无法逐日计算全市场因子)
        if calc_factors:
            self.calculate_factors_batch(start_date, end_date)

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
        from etl.factors import factor_calculator
        factor_calculator.calculate_daily(trade_date)

    def calculate_factors_batch(self, start_date: str, end_date: str):
        from etl.factors import factor_calculator
        factor_calculator.calculate_batch(start_date, end_date)

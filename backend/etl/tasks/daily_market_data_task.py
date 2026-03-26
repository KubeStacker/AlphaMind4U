import arrow
import time
import logging
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
from etl.calendar import trading_calendar

logger = logging.getLogger(__name__)

class DailyMarketDataTask(BaseTask):
    """每日市场数据同步任务
    
    负责同步A股日线行情数据和复权因子
    """
    
    def get_expected_daily_record_count(self) -> int:
        """估算单日应有的行情记录数，用于识别不完整交易日。
        
        优化策略:
        1. 基于最近一个完整交易日的实际数据量
        2. 考虑新股上市和退市的影响
        3. 使用动态阈值而不是固定百分比
        
        Returns:
            int: 预期的每日最小记录数
        """
        try:
            # 首先尝试获取最近一个完整交易日的数据量
            df_recent = fetch_df("""
                SELECT COUNT(*) AS cnt
                FROM daily_price
                WHERE trade_date = (
                    SELECT MAX(trade_date) 
                    FROM daily_price 
                    WHERE trade_date < CURRENT_DATE - INTERVAL '1 day'
                )
            """)
            
            if not df_recent.empty and int(df_recent.iloc[0]["cnt"]) > 0:
                recent_count = int(df_recent.iloc[0]["cnt"])
                # 基于最近数据量，留出5%的波动空间
                return max(1000, int(recent_count * 0.95))
            
            # 如果没有历史数据，使用股票列表估算
            df_stocks = fetch_df("""
                SELECT COUNT(*) AS cnt
                FROM stock_basic
                WHERE list_status = 'L'
            """)
            
            if not df_stocks.empty and int(df_stocks.iloc[0]["cnt"]) > 0:
                stock_count = int(df_stocks.iloc[0]["cnt"])
                # 考虑停牌股票，使用90%阈值
                return max(1000, int(stock_count * 0.9))
                
        except Exception as e:
            logger.warning(f"获取预期日线数量失败: {e}")
        
        # 默认值
        return 1000

    def sync_daily_data(self, years: int = 1, force: bool = False, calc_factors: bool = True):
        """同步A股日线数据
        
        Args:
            years: 同步的年数，默认1年
            force: 是否强制重新同步已存在的数据
            calc_factors: 是否计算技术因子
        """
        logger.info(f"开始同步日线数据 (年限: {years})...")
        self._sync_by_date_range(years, force, calc_factors)

    def _sync_by_date_range(self, years: int, force: bool, calc_factors: bool):
        """按日期范围同步数据
        
        Args:
            years: 同步的年数
            force: 是否强制重新同步
            calc_factors: 是否计算技术因子
        """
        # 优化：获取当前可同步的最晚日期 (如果是收盘后则包含今日)
        end_date = arrow.get(trading_calendar.get_latest_sync_date())
        start_date = end_date.shift(years=-years)
        
        # 1. 获取目标范围内的所有交易日
        target_dates = {r.format("YYYY-MM-DD") for r in arrow.Arrow.range('day', start_date, end_date) 
                         if trading_calendar.is_trading_day(r.date())}
        
        # 2. 检查已存在日期
        existing_dates = set()
        if not force:
            expected_min_count = self.get_expected_daily_record_count()
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
            logger.info("所有日期数据已完整。")
            return

        for date_str in dates_to_sync:
            self.fetch_and_save_daily_data(date_str, calc_factors)

    def fetch_and_save_daily_data(self, date_str: str, calc_factors: bool):
        """获取并保存指定日期的市场数据
        
        Args:
            date_str: 交易日期字符串 (YYYY-MM-DD)
            calc_factors: 是否计算技术因子
        """
        logger.info(f"正在同步 {date_str} 的行情...")
        try:
            # 1. 获取行情数据
            df_daily = self.provider.daily(trade_date=date_str)
            if df_daily.empty:
                logger.warning(f"{date_str} 没有行情数据，未写入数据库")
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
            
            # 保存到数据库
            self._upsert_daily_data(df)
            
            if calc_factors:
                self.calculate_technical_factors(date_str)
                
            time.sleep(0.5) # 避免限流

        except Exception as e:
            logger.error(f"同步 {date_str} 失败: {e}")
            time.sleep(5)

    def _upsert_daily_data(self, df: pd.DataFrame):
        """更新或插入日线数据到数据库
        
        Args:
            df: 包含日线数据的DataFrame
        """
        cols = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'factors', 'adj_factor']
        # 确保列对齐
        for c in cols:
            if c not in df.columns:
                df[c] = None
        
        df_to_save = df[cols]
        with get_db_connection() as con:
            # 使用 ON CONFLICT DO UPDATE 确保数据更新，而不是忽略
            con.execute("""
                INSERT INTO daily_price 
                SELECT * FROM df_to_save 
                ON CONFLICT (trade_date, ts_code) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    pre_close = excluded.pre_close,
                    change = excluded.change,
                    pct_chg = excluded.pct_chg,
                    vol = excluded.vol,
                    amount = excluded.amount,
                    adj_factor = excluded.adj_factor
                -- 注意: factors 字段不更新，因为它由因子计算单独管理
            """)

    def calculate_technical_factors(self, trade_date: str):
        """计算指定日期的技术因子
        
        Args:
            trade_date: 交易日期字符串
        """
        # 使用独立的计算器以避免循环引用
        from etl.utils.factors import factor_calculator
        factor_calculator.calculate_daily(trade_date)

    def calculate_technical_factors_batch(self, start_date: str, end_date: str):
        """批量计算技术因子
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
        """
        from etl.utils.factors import factor_calculator
        factor_calculator.calculate_batch(start_date, end_date)

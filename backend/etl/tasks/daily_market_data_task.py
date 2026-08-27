import arrow
import time
import logging
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed
from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
from etl.calendar import trading_calendar
from etl.utils.art_safe_write import dedupe_by_key, update_then_insert_missing

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

    def sync_daily_data(self, years: int = 1, force: bool = False):
        """同步A股日线数据
        
        Args:
            years: 同步的年数，默认1年
            force: 是否强制重新同步已存在的数据
        """
        logger.info(f"开始同步日线数据 (年限: {years})...")
        self._sync_by_date_range(years, force)

    def _sync_by_date_range(self, years: int, force: bool):
        """按日期范围同步数据
        
        Args:
            years: 同步的年数
            force: 是否强制重新同步
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
            self.fetch_and_save_daily_data(date_str)

    def fetch_and_save_daily_data(self, date_str: str):
        """获取并保存指定日期的市场数据
        
        Args:
            date_str: 交易日期字符串 (YYYY-MM-DD)
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

            # 确保日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
            
            # 保存到数据库
            self._upsert_daily_data(df)
            
            time.sleep(0.5) # 避免限流

        except Exception as e:
            logger.error(f"同步 {date_str} 失败: {e}")
            time.sleep(5)

    def _upsert_daily_data(self, df: pd.DataFrame):
        """更新或插入日线数据到数据库
        
        Args:
            df: 包含日线数据的DataFrame
        """
        cols = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'adj_factor']
        for c in cols:
            if c not in df.columns:
                df[c] = None
        
        df_to_save = df[cols].copy()
        df_to_save['trade_date'] = pd.to_datetime(df_to_save['trade_date']).dt.date
        df_to_save = dedupe_by_key(
            df_to_save,
            key_cols=['trade_date', 'ts_code'],
            table_name='daily_price',
            logger=logger,
        )
        
        with get_db_connection() as con:
            update_then_insert_missing(
                con,
                df=df_to_save,
                table_name='daily_price',
                view_name='df_daily_view',
                columns=cols,
                key_cols=['trade_date', 'ts_code'],
                logger=logger,
            )

    def sync_suspend_data(self, years: int = 0, days: int = 3, force: bool = False):
        """同步股票停复牌数据"""
        logger.info(f"开始同步停复牌数据 (years={years}, days={days}, force={force})...")
        end_date = arrow.get(trading_calendar.get_latest_sync_date())
        start_date = end_date.shift(years=-years) if years > 0 else end_date.shift(days=-days)
        target_dates = {
            r.format("YYYY-MM-DD")
            for r in arrow.Arrow.range("day", start_date, end_date)
            if trading_calendar.is_trading_day(r.date())
        }

        existing_dates = set()
        if not force:
            try:
                df_existing = fetch_df(
                    """
                    SELECT trade_date FROM stock_suspend
                    WHERE trade_date BETWEEN ? AND ?
                    GROUP BY trade_date
                    """,
                    params=[start_date.date(), end_date.date()],
                )
                if not df_existing.empty:
                    existing_dates = {d.strftime("%Y-%m-%d") for d in df_existing['trade_date']}
            except Exception:
                pass

        dates_to_sync = sorted(list(target_dates - existing_dates), reverse=True)
        if not dates_to_sync:
            logger.info("停复牌数据已是最新。")
            return

        for date_str in dates_to_sync:
            try:
                df = self.provider.suspend_d(trade_date=date_str)
                if not df.empty:
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    self._upsert_suspend_data(df)
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"同步停复牌数据 {date_str} 失败: {e}")

    def _upsert_suspend_data(self, df: pd.DataFrame):
        cols = ['ts_code', 'trade_date', 'suspend_timing', 'suspend_type']
        for c in cols:
            if c not in df.columns:
                df[c] = None
        df_to_save = df[cols].copy()
        df_to_save['trade_date'] = pd.to_datetime(df_to_save['trade_date']).dt.date
        df_to_save = dedupe_by_key(
            df_to_save,
            key_cols=['ts_code', 'trade_date'],
            table_name='stock_suspend',
            logger=logger,
        )

        with get_db_connection() as con:
            update_then_insert_missing(
                con,
                df=df_to_save,
                table_name='stock_suspend',
                view_name='suspend_view',
                columns=cols,
                key_cols=['ts_code', 'trade_date'],
                logger=logger,
            )


from etl.tasks.base_task import BaseTask
import arrow
import logging
from etl.calendar import trading_calendar
from db.connection import get_db_connection, fetch_df
import pandas as pd
import time

logger = logging.getLogger(__name__)

class CapitalFlowTask(BaseTask):
    """资金流向数据同步任务
    
    负责同步A股个股资金流向数据
    """
    
    def get_expected_daily_record_count(self) -> int:
        """估算单日应有的资金流记录数，用于识别不完整交易日。
        
        优化策略:
        1. 基于最近一个完整交易日的实际数据量
        2. 考虑资金流数据可能比行情数据少（某些股票可能没有资金流数据）
        3. 使用动态阈值
        
        Returns:
            int: 预期的每日最小记录数
        """
        try:
            # 首先尝试获取最近一个完整交易日的资金流数据量
            df_recent = fetch_df("""
                SELECT COUNT(*) AS cnt
                FROM stock_moneyflow
                WHERE trade_date = (
                    SELECT MAX(trade_date) 
                    FROM stock_moneyflow 
                    WHERE trade_date < CURRENT_DATE - INTERVAL '1 day'
                )
            """)
            
            if not df_recent.empty and int(df_recent.iloc[0]["cnt"]) > 0:
                recent_count = int(df_recent.iloc[0]["cnt"])
                # 基于最近数据量，留出15%的波动空间（资金流数据波动较大）
                return max(1000, int(recent_count * 0.85))
            
            # 如果没有历史数据，使用股票列表估算
            df_stocks = fetch_df("""
                SELECT COUNT(*) AS cnt
                FROM stock_basic
                WHERE list_status = 'L'
            """)
            
            if not df_stocks.empty and int(df_stocks.iloc[0]["cnt"]) > 0:
                stock_count = int(df_stocks.iloc[0]["cnt"])
                # 考虑资金流数据可能不全，使用80%阈值
                return max(1000, int(stock_count * 0.8))
                
        except Exception as e:
            logger.warning(f"获取预期资金流数量失败: {e}")
        
        # 默认值
        return 1000

    def sync_capital_flow(self, years: int = 0, days: int = 3, force: bool = False):
        """同步资金流向数据
        
        Args:
            years: 同步的年数，0表示只同步最近几天
            days: 同步的天数（当years=0时生效）
            force: 是否强制重新同步已存在的数据
        """
        logger.info(f"开始同步资金流向 (模式: {self.provider.sync_mode})...")
        
        # 优化：获取当前可同步的最晚日期 (如果是收盘后则包含今日)
        end_date = arrow.get(trading_calendar.get_latest_sync_date())
        if years > 0:
            start_date = end_date.shift(years=-years)
        else:
            start_date = end_date.shift(days=-days)
        
        target_dates = {r.format("YYYY-MM-DD") for r in arrow.Arrow.range('day', start_date, end_date) 
                         if trading_calendar.is_trading_day(r.date())}
        
        existing_dates = set()
        if not force:
            expected_min_count = self.get_expected_daily_record_count()
            df_existing = fetch_df(
                """
                SELECT trade_date
                FROM stock_moneyflow
                WHERE trade_date BETWEEN ? AND ?
                GROUP BY trade_date
                HAVING COUNT(*) >= ?
                """,
                params=[start_date.date(), end_date.date(), expected_min_count],
            )
            if not df_existing.empty:
                existing_dates = {d.strftime("%Y-%m-%d") for d in df_existing['trade_date']}

        dates_to_sync = sorted(list(target_dates - existing_dates), reverse=True)
        
        for date_str in dates_to_sync:
            try:
                df = self.provider.moneyflow(trade_date=date_str)
                if not df.empty:
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    self._upsert_capital_flow_data(df)
                time.sleep(0.5)
            except Exception as e:
                logger.error(f"同步资金流向 {date_str} 失败: {e}")

    def _upsert_capital_flow_data(self, df: pd.DataFrame):
        """更新或插入资金流向数据
        
        Args:
            df: 包含资金流向数据的DataFrame
        """
        # 确保DataFrame有net_mf_ratio列（Tushare API可能不返回此列）
        if 'net_mf_ratio' not in df.columns:
            df['net_mf_ratio'] = None
        
        with get_db_connection() as con:
            con.register('df_view', df)
            con.execute("""
                INSERT INTO stock_moneyflow 
                SELECT * FROM df_view
                ON CONFLICT (ts_code, trade_date) DO UPDATE SET
                    buy_sm_vol = excluded.buy_sm_vol,
                    buy_sm_amount = excluded.buy_sm_amount,
                    sell_sm_vol = excluded.sell_sm_vol,
                    sell_sm_amount = excluded.sell_sm_amount,
                    buy_md_vol = excluded.buy_md_vol,
                    buy_md_amount = excluded.buy_md_amount,
                    sell_md_vol = excluded.sell_md_vol,
                    sell_md_amount = excluded.sell_md_amount,
                    buy_lg_vol = excluded.buy_lg_vol,
                    buy_lg_amount = excluded.buy_lg_amount,
                    sell_lg_vol = excluded.sell_lg_vol,
                    sell_lg_amount = excluded.sell_lg_amount,
                    buy_elg_vol = excluded.buy_elg_vol,
                    buy_elg_amount = excluded.buy_elg_amount,
                    sell_elg_vol = excluded.sell_elg_vol,
                    sell_elg_amount = excluded.sell_elg_amount,
                    net_mf_vol = excluded.net_mf_vol,
                    net_mf_amount = excluded.net_mf_amount,
                    net_mf_ratio = excluded.net_mf_ratio
            """)

import arrow
import time
from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection, fetch_df
from etl.calendar import trading_calendar
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class MarginTradingTask(BaseTask):
    """融资融券数据同步任务
    
    负责同步A股融资融券数据
    margin_detail 是 long token 接口，需要逐日调用
    """

    def sync_margin_trading(self, days: int = 90):
        """同步融资融券数据（按日期逐天同步）

        Args:
            days: 同步的天数，默认90天
        """
        logger.info(f"开始同步融资融券数据 (最近{days}天)...")
        
        end_date = arrow.get(trading_calendar.get_latest_sync_date())
        start_date = end_date.shift(days=-days)
        
        # 获取目标范围内的所有交易日
        target_dates = {r.format("YYYY-MM-DD") for r in arrow.Arrow.range('day', start_date, end_date)
                        if trading_calendar.is_trading_day(r.date())}
        
        # 找出已有数据的日期（数据量 >= 1000 认为完整）
        existing_dates = set()
        try:
            df_existing = fetch_df("""
                SELECT trade_date FROM stock_margin
                WHERE trade_date BETWEEN ? AND ?
                GROUP BY trade_date
                HAVING COUNT(*) >= 1000
            """, params=[start_date.date(), end_date.date()])
            if not df_existing.empty:
                existing_dates = {d.strftime("%Y-%m-%d") for d in df_existing['trade_date']}
        except Exception:
            pass
        
        dates_to_sync = sorted(list(target_dates - existing_dates), reverse=True)
        
        if not dates_to_sync:
            logger.info("融资融券数据已完整。")
            return
        
        logger.info(f"需要同步 {len(dates_to_sync)} 个交易日的融资融券数据")
        
        for date_str in dates_to_sync:
            try:
                self._sync_single_date(date_str)
                time.sleep(1)  # Tushare rate limit
            except Exception as e:
                logger.error(f"同步融资融券 {date_str} 失败: {e}")
                time.sleep(5)
        
        logger.info("融资融券数据同步完成")

    def _sync_single_date(self, date_str: str):
        """同步单日融资融券数据"""
        try:
            df = self.provider.margin_detail(trade_date=date_str)
            if df.empty:
                logger.warning(f"融资融券 {date_str} 无数据")
                return
            
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y-%m-%d')
            df = df[df['ts_code'].notna()]
            if df.empty:
                return
            
            target_cols = ['ts_code', 'trade_date', 'rzye', 'rzmre', 'rzche', 'rqye', 'rqmcl', 'rzrqye', 'rqyl']
            for col in target_cols:
                if col not in df.columns:
                    df[col] = None
            df = df[[c for c in target_cols if c in df.columns]]
            
            with get_db_connection() as con:
                con.begin()
                con.execute("DELETE FROM stock_margin WHERE trade_date = ?", [date_str])
                con.register('df_view', df)
                cols = df.columns.tolist()
                col_str = ','.join(cols)
                con.execute(f"INSERT INTO stock_margin ({col_str}) SELECT {col_str} FROM df_view")
                con.commit()
            
            logger.info(f"融资融券 {date_str} 同步完成: {len(df)} 条")
            
        except Exception as e:
            logger.error(f"融资融券 {date_str} 同步失败: {e}")
            raise

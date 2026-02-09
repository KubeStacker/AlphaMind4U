from etl.tasks.base_task import BaseTask
import arrow
from core.calendar import trading_calendar
from db.connection import get_db_connection, fetch_df
import pandas as pd
import time

class MoneyFlowTask(BaseTask):
    def sync(self, years: int = 1, force: bool = False):
        self.logger.info(f"开始同步资金流向 (模式: {self.provider.sync_mode})...")
        
        # Akshare 暂不支持资金流向
        if self.provider.provider_name == 'akshare':
            self.logger.warning("Akshare 数据源暂不支持个股资金流向同步")
            return

        end_date = arrow.now()
        start_date = end_date.shift(years=-years)
        
        target_dates = {r.format("YYYY-MM-DD") for r in arrow.Arrow.range('day', start_date, end_date) 
                         if trading_calendar.is_trading_day(r.date())}
        
        existing_dates = set()
        if not force:
            df_existing = fetch_df("SELECT trade_date FROM stock_moneyflow WHERE trade_date BETWEEN ? AND ? GROUP BY trade_date HAVING COUNT(*) > 1000", params=[start_date.date(), end_date.date()])
            if not df_existing.empty:
                existing_dates = {d.strftime("%Y-%m-%d") for d in df_existing['trade_date']}

        dates_to_sync = sorted(list(target_dates - existing_dates), reverse=True)
        
        for d_str in dates_to_sync:
            try:
                df = self.provider.moneyflow(trade_date=d_str)
                if not df.empty:
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    with get_db_connection() as con:
                        con.execute("INSERT INTO stock_moneyflow SELECT * FROM df ON CONFLICT DO NOTHING")
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"同步资金流向 {d_str} 失败: {e}")

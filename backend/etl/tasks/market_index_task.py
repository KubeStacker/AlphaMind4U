from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection
import arrow
import pandas as pd

class MarketIndexTask(BaseTask):
    def sync(self, ts_code: str = '000001.SH', years: int = 0, days: int = 3):
        self.logger.info(f"同步市场指数 {ts_code}...")
        from etl.calendar import trading_calendar
        end_date = arrow.get(trading_calendar.get_latest_sync_date())
        if years > 0:
            start_date = end_date.shift(years=-years).format("YYYYMMDD")
        else:
            start_date = end_date.shift(days=-days).format("YYYYMMDD")
        end_date_str = end_date.format("YYYYMMDD")
        
        try:
            df = self.provider.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date_str)
            if not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                
                cols = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
                for c in cols:
                    if c not in df.columns:
                        df[c] = None
                        
                df_to_save = df[cols]
                with get_db_connection() as con:
                    con.execute("INSERT INTO market_index SELECT * FROM df_to_save ON CONFLICT (trade_date, ts_code) DO NOTHING")
        except Exception as e:
            self.logger.error(f"同步指数失败: {e}")

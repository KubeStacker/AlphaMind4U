import pandas as pd
from etl.tasks.base_task import BaseTask
from db.connection import get_db_connection

class CalendarTask(BaseTask):
    def sync(self, start_date: str = "2020-01-01", end_date: str = "2026-12-31"):
        """ 同步交易日历到数据库 """
        self.logger.info(f"正在从 Tushare 同步 {start_date} 到 {end_date} 的交易日历...")
        
        try:
            df = self.provider.trade_cal(start_date=start_date, end_date=end_date)
            if df.empty:
                self.logger.warning("未获取到交易日历数据")
                return

            # 转换日期格式
            df['cal_date'] = pd.to_datetime(df['cal_date']).dt.date
            df['pretrade_date'] = pd.to_datetime(df['pretrade_date']).dt.date
            
            # 保存到数据库
            self._save_to_db(df)
            self.logger.info("交易日历同步完成")
            
        except Exception as e:
            self.logger.error(f"同步交易日历失败: {e}")

    def _save_to_db(self, df: pd.DataFrame):
        cols = ['exchange', 'cal_date', 'is_open', 'pretrade_date']
        df_to_save = df[cols]
        
        with get_db_connection() as con:
            con.execute("INSERT INTO trade_calendar SELECT * FROM df_to_save ON CONFLICT (exchange, cal_date) DO UPDATE SET is_open = excluded.is_open, pretrade_date = excluded.pretrade_date")

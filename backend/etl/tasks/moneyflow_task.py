from etl.tasks.base_task import BaseTask
import arrow
from etl.calendar import trading_calendar
from db.connection import get_db_connection, fetch_df
import pandas as pd
import time

class MoneyFlowTask(BaseTask):
    def _get_expected_daily_count(self) -> int:
        """估算单日应有的资金流条数，用于识别不完整交易日。"""
        try:
            df = fetch_df(
                """
                SELECT COUNT(*) AS cnt
                FROM stock_basic
                WHERE list_status = 'L'
                """
            )
            if not df.empty and int(df.iloc[0]["cnt"]) > 0:
                return max(1000, int(df.iloc[0]["cnt"] * 0.85))
        except Exception:
            pass
        return 1000

    def sync(self, years: int = 0, days: int = 3, force: bool = False):
        self.logger.info(f"开始同步资金流向 (模式: {self.provider.sync_mode})...")
        
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
            expected_min_count = self._get_expected_daily_count()
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
        
        for d_str in dates_to_sync:
            try:
                df = self.provider.moneyflow(trade_date=d_str)
                if not df.empty:
                    df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date
                    with get_db_connection() as con:
                        con.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_moneyflow AS SELECT * FROM stock_moneyflow WHERE 1=0")
                        con.execute("INSERT INTO tmp_moneyflow SELECT * FROM df")
                        con.execute("""
                            INSERT INTO stock_moneyflow 
                            SELECT * FROM tmp_moneyflow
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
                                net_mf_amount = excluded.net_mf_amount
                        """)
                time.sleep(0.5)
            except Exception as e:
                self.logger.error(f"同步资金流向 {d_str} 失败: {e}")

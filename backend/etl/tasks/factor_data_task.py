import arrow
import logging
import pandas as pd
import time

from db.connection import fetch_df, get_db_connection
from etl.calendar import trading_calendar
from etl.tasks.base_task import BaseTask

logger = logging.getLogger(__name__)


class FactorDataTask(BaseTask):
    """因子层依赖的数据同步任务。"""

    DAILY_BASIC_COLUMNS = [
        "trade_date",
        "ts_code",
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    ]

    EXPRESS_COLUMNS = [
        "ts_code",
        "ann_date",
        "end_date",
        "revenue",
        "operate_profit",
        "total_profit",
        "n_income",
        "total_assets",
        "total_hldr_eqy_exc_min_int",
        "diluted_eps",
        "diluted_roe",
        "yoy_net_profit",
        "bps",
        "perf_summary",
        "update_flag",
    ]

    INDEX_MEMBER_COLUMNS = [
        "l1_code",
        "l1_name",
        "l2_code",
        "l2_name",
        "l3_code",
        "l3_name",
        "ts_code",
        "name",
        "in_date",
        "out_date",
        "is_new",
    ]

    def get_expected_daily_basic_count(self) -> int:
        try:
            df_recent = fetch_df(
                """
                SELECT COUNT(*) AS cnt
                FROM stock_daily_basic
                WHERE trade_date = (
                    SELECT MAX(trade_date)
                    FROM stock_daily_basic
                    WHERE trade_date < CURRENT_DATE - INTERVAL '1 day'
                )
                """
            )
            if not df_recent.empty and int(df_recent.iloc[0]["cnt"]) > 0:
                return max(1000, int(int(df_recent.iloc[0]["cnt"]) * 0.92))
        except Exception as exc:
            logger.warning(f"获取 daily_basic 参考记录数失败: {exc}")

        try:
            df_stocks = fetch_df("SELECT COUNT(*) AS cnt FROM stock_basic WHERE list_status = 'L'")
            if not df_stocks.empty and int(df_stocks.iloc[0]["cnt"]) > 0:
                return max(1000, int(int(df_stocks.iloc[0]["cnt"]) * 0.9))
        except Exception as exc:
            logger.warning(f"获取股票总数失败: {exc}")

        return 1000

    def sync_daily_basic(self, years: int = 0, days: int = 3, force: bool = False):
        logger.info(f"开始同步日频基础指标 (years={years}, days={days}, force={force})")

        end_date = arrow.get(trading_calendar.get_latest_sync_date())
        start_date = end_date.shift(years=-years) if years > 0 else end_date.shift(days=-days)
        target_dates = {
            dt.format("YYYY-MM-DD")
            for dt in arrow.Arrow.range("day", start_date, end_date)
            if trading_calendar.is_trading_day(dt.date())
        }

        existing_dates = set()
        if not force:
            expected_min = self.get_expected_daily_basic_count()
            df_existing = fetch_df(
                """
                SELECT trade_date
                FROM stock_daily_basic
                WHERE trade_date BETWEEN ? AND ?
                GROUP BY trade_date
                HAVING COUNT(*) >= ?
                """,
                params=[start_date.date(), end_date.date(), expected_min],
            )
            if not df_existing.empty:
                existing_dates = {d.strftime("%Y-%m-%d") for d in df_existing["trade_date"]}

        dates_to_sync = sorted(target_dates - existing_dates, reverse=True)
        if not dates_to_sync:
            logger.info("日频基础指标已是最新。")
            return

        for date_str in dates_to_sync:
            try:
                df = self.provider.daily_basic(trade_date=date_str)
                if df.empty:
                    logger.warning(f"{date_str} 未返回 daily_basic 数据")
                    continue

                self._prepare_daily_basic_df(df)
                self._upsert_daily_basic(df)
                time.sleep(0.5)
            except Exception as exc:
                logger.error(f"同步 daily_basic {date_str} 失败: {exc}")

    def sync_daily_basic_for_date(self, trade_date: str):
        df = self.provider.daily_basic(trade_date=trade_date)
        if df.empty:
            logger.warning(f"{trade_date} 未返回 daily_basic 数据")
            return
        self._prepare_daily_basic_df(df)
        self._upsert_daily_basic(df)

    def sync_index_member_all(self):
        logger.info("开始同步申万行业归属(index_member_all)...")
        df = self.provider.index_member_all(is_new="Y")
        if df.empty:
            logger.warning("index_member_all 未返回数据，跳过")
            return

        df = df.copy()
        for col in ("in_date", "out_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        for col in self.INDEX_MEMBER_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[self.INDEX_MEMBER_COLUMNS].drop_duplicates(
            subset=["ts_code", "l3_code", "in_date"], keep="last"
        )

        with get_db_connection() as con:
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("DELETE FROM stock_index_member_all")
                con.register("factor_index_member_view", df)
                con.execute(
                    """
                    INSERT INTO stock_index_member_all
                    SELECT * FROM factor_index_member_view
                    """
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise

        logger.info(f"申万行业归属同步完成: {len(df)} 条")

    def sync_express(
        self,
        days: int = 120,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        if not end_date:
            end_date = arrow.get(trading_calendar.get_latest_sync_date()).format("YYYY-MM-DD")
        if not start_date:
            start_date = arrow.get(end_date).shift(days=-days).format("YYYY-MM-DD")

        logger.info(f"开始同步业绩快报: {start_date} ~ {end_date}")
        df = self.provider.express(start_date=start_date, end_date=end_date)
        if df.empty:
            logger.warning("业绩快报未返回数据，跳过")
            return

        df = df.copy()
        for col in ("ann_date", "end_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        for col in self.EXPRESS_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[self.EXPRESS_COLUMNS].drop_duplicates(
            subset=["ts_code", "end_date", "ann_date"], keep="last"
        )

        with get_db_connection() as con:
            con.register("factor_express_view", df)
            col_str = ",".join(df.columns.tolist())
            update_cols = [col for col in df.columns if col not in ("ts_code", "end_date", "ann_date")]
            update_set = ",".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
            con.execute(
                f"""
                INSERT INTO stock_express ({col_str})
                SELECT {col_str} FROM factor_express_view
                ON CONFLICT (ts_code, end_date, ann_date) DO UPDATE SET
                    {update_set}
                """
            )

        logger.info(f"业绩快报同步完成: {len(df)} 条")

    def _prepare_daily_basic_df(self, df: pd.DataFrame):
        for col in ("trade_date",):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        for col in self.DAILY_BASIC_COLUMNS:
            if col not in df.columns:
                df[col] = None

    def _upsert_daily_basic(self, df: pd.DataFrame):
        df_to_save = df[self.DAILY_BASIC_COLUMNS]
        with get_db_connection() as con:
            con.register("factor_daily_basic_view", df_to_save)
            con.execute(
                """
                INSERT INTO stock_daily_basic
                SELECT * FROM factor_daily_basic_view
                ON CONFLICT (trade_date, ts_code) DO UPDATE SET
                    close = EXCLUDED.close,
                    turnover_rate = EXCLUDED.turnover_rate,
                    turnover_rate_f = EXCLUDED.turnover_rate_f,
                    volume_ratio = EXCLUDED.volume_ratio,
                    pe = EXCLUDED.pe,
                    pe_ttm = EXCLUDED.pe_ttm,
                    pb = EXCLUDED.pb,
                    ps = EXCLUDED.ps,
                    ps_ttm = EXCLUDED.ps_ttm,
                    dv_ratio = EXCLUDED.dv_ratio,
                    dv_ttm = EXCLUDED.dv_ttm,
                    total_share = EXCLUDED.total_share,
                    float_share = EXCLUDED.float_share,
                    free_share = EXCLUDED.free_share,
                    total_mv = EXCLUDED.total_mv,
                    circ_mv = EXCLUDED.circ_mv
                """
            )

import arrow
import logging
import pandas as pd
import time

from db.connection import fetch_df, get_db_connection
from etl.calendar import trading_calendar
from etl.tasks.base_task import BaseTask
from etl.utils.art_safe_write import dedupe_by_key, update_then_insert_missing

logger = logging.getLogger(__name__)


class ReferenceDataTask(BaseTask):
    """同步 Tushare 日频基础、行业成分和业绩快报原始数据。"""

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
                con.register("index_member_view", df)
                con.execute(
                    """
                    INSERT INTO stock_index_member_all
                    SELECT * FROM index_member_view
                    """
                )
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            finally:
                try:
                    con.unregister("index_member_view")
                except Exception:
                    pass

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
            try:
                con.register("express_view", df)
                col_str = ",".join(df.columns.tolist())
                update_cols = [col for col in df.columns if col not in ("ts_code", "end_date", "ann_date")]
                update_set = ",".join([f"{col}=EXCLUDED.{col}" for col in update_cols])
                con.execute(
                    f"""
                    INSERT INTO stock_express ({col_str})
                    SELECT {col_str} FROM express_view
                    ON CONFLICT (ts_code, end_date, ann_date) DO UPDATE SET
                        {update_set}
                    """
                )
            finally:
                try:
                    con.unregister("express_view")
                except Exception:
                    pass

        logger.info(f"业绩快报同步完成: {len(df)} 条")

    def _prepare_daily_basic_df(self, df: pd.DataFrame):
        for col in ("trade_date",):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
        for col in self.DAILY_BASIC_COLUMNS:
            if col not in df.columns:
                df[col] = None

    def _upsert_daily_basic(self, df: pd.DataFrame):
        for col in self.DAILY_BASIC_COLUMNS:
            if col not in df.columns:
                df[col] = None

        df_to_save = df[self.DAILY_BASIC_COLUMNS].copy()
        df_to_save["trade_date"] = pd.to_datetime(df_to_save["trade_date"]).dt.date
        df_to_save = dedupe_by_key(
            df_to_save,
            key_cols=["trade_date", "ts_code"],
            table_name="stock_daily_basic",
            logger=logger,
        )
        with get_db_connection() as con:
            update_then_insert_missing(
                con,
                df=df_to_save,
                table_name="stock_daily_basic",
                view_name="daily_basic_view",
                columns=self.DAILY_BASIC_COLUMNS,
                key_cols=["trade_date", "ts_code"],
                logger=logger,
            )

    def sync_namechange(self, ts_code: str | None = None):
        """同步股票更名历史记录 (追踪 ST / *ST 变更)"""
        logger.info("开始同步股票更名记录...")
        stocks = [ts_code] if ts_code else fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        if not stocks:
            return

        cols = ["ts_code", "name", "start_date", "end_date", "ann_date", "change_reason"]
        success_count = 0

        for idx, s_code in enumerate(stocks):
            try:
                df = self.provider.namechange(ts_code=s_code)
                if df.empty:
                    continue

                for dcol in ("start_date", "end_date", "ann_date"):
                    if dcol in df.columns:
                        df[dcol] = pd.to_datetime(df[dcol], errors="coerce").dt.date

                for col in cols:
                    if col not in df.columns:
                        df[col] = None

                df_to_save = df[cols].copy()
                df_to_save = dedupe_by_key(
                    df_to_save,
                    key_cols=["ts_code", "start_date"],
                    table_name="stock_namechange",
                    logger=logger,
                )

                with get_db_connection() as con:
                    update_then_insert_missing(
                        con,
                        df=df_to_save,
                        table_name="stock_namechange",
                        view_name="namechange_view",
                        columns=cols,
                        key_cols=["ts_code", "start_date"],
                        logger=logger,
                    )
                success_count += len(df_to_save)
            except Exception as exc:
                if "权限" in str(exc):
                    logger.warning(f"更名记录权限不足: {str(exc)[:50]}")
                    break

            if (idx + 1) % 500 == 0:
                logger.info(f"更名记录进度: {idx+1}/{len(stocks)}")
                time.sleep(1)

        logger.info(f"股票更名记录同步完成: +{success_count} 条")

    def sync_index_weight(
        self,
        index_code: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        days: int = 90,
    ):
        """同步核心指数成分股权重 (沪深300/中证500/中证1000/上证50等)"""
        if not end_date:
            end_date = arrow.get(trading_calendar.get_latest_sync_date()).format("YYYY-MM-DD")
        if not start_date:
            start_date = arrow.get(end_date).shift(days=-days).format("YYYY-MM-DD")

        indices = [index_code] if index_code else ["000300.SH", "000905.SH", "000852.SH", "000016.SH"]
        logger.info(f"开始同步指数成分权重: {indices} ({start_date} ~ {end_date})")

        cols = ["index_code", "con_code", "trade_date", "weight"]
        total_rows = 0

        for idx_code in indices:
            try:
                df = self.provider.index_weight(
                    index_code=idx_code,
                    start_date=start_date,
                    end_date=end_date,
                )
                if df.empty:
                    continue

                if "trade_date" in df.columns:
                    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date

                for col in cols:
                    if col not in df.columns:
                        df[col] = None

                df_to_save = df[cols].copy()
                df_to_save = dedupe_by_key(
                    df_to_save,
                    key_cols=["index_code", "con_code", "trade_date"],
                    table_name="index_weight",
                    logger=logger,
                )

                with get_db_connection() as con:
                    update_then_insert_missing(
                        con,
                        df=df_to_save,
                        table_name="index_weight",
                        view_name="index_weight_view",
                        columns=cols,
                        key_cols=["index_code", "con_code", "trade_date"],
                        logger=logger,
                    )
                total_rows += len(df_to_save)
                time.sleep(0.5)
            except Exception as exc:
                logger.error(f"同步指数权重 {idx_code} 失败: {exc}")

        logger.info(f"指数成分权重同步完成: {total_rows} 条")


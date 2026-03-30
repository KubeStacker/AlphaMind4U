import json
import logging

import arrow
import numpy as np
import pandas as pd

from db.connection import fetch_df, get_db_connection

logger = logging.getLogger(__name__)


class FactorCalculator:
    def calculate_daily(self, trade_date: str):
        """
        计算指定交易日的技术因子，并刷新因子宽表快照。
        """
        logger.info(f"开始计算 {trade_date} 的技术因子与宽表因子...")

        try:
            target_date = arrow.get(trade_date).format("YYYY-MM-DD")
            start_date = arrow.get(trade_date).shift(days=-450).format("YYYY-MM-DD")
        except Exception as exc:
            logger.error(f"日期格式解析失败 {trade_date}: {exc}")
            return

        update_query = """
        WITH RawData AS (
            SELECT
                d.ts_code,
                d.trade_date,
                d.close,
                d.vol,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS ma5,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                ) AS ma10,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ma20,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma60,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS ma120,
                AVG(d.vol) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS vol_ma5,
                MAX(d.high) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
                ) AS high_250,
                AVG(d.pct_chg) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS avg_ret_60,
                STDDEV(d.pct_chg) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS vol_60,
                (d.close - LAG(d.close, 20) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date))
                    / NULLIF(LAG(d.close, 20) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date), 0) AS mom_20,
                (d.close - LAG(d.close, 50) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date))
                    / NULLIF(LAG(d.close, 50) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date), 0) AS mom_50,
                (d.close - LAG(d.close, 120) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date))
                    / NULLIF(LAG(d.close, 120) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date), 0) AS mom_120,
                (d.close - LAG(d.close, 250) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date))
                    / NULLIF(LAG(d.close, 250) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date), 0) AS mom_250,
                COUNT(*) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS row_num
            FROM daily_price d
            WHERE d.trade_date BETWEEN ? AND ?
        ),
        FilteredData AS (
            SELECT * FROM RawData WHERE trade_date = ? AND row_num >= 20
        ),
        RPSData AS (
            SELECT
                *,
                PERCENT_RANK() OVER (ORDER BY mom_20) * 100 AS rps_20,
                PERCENT_RANK() OVER (ORDER BY mom_50) * 100 AS rps_50,
                PERCENT_RANK() OVER (ORDER BY mom_120) * 100 AS rps_120,
                PERCENT_RANK() OVER (ORDER BY mom_250) * 100 AS rps_250
            FROM FilteredData
        ),
        FinalFactors AS (
            SELECT
                ts_code,
                trade_date,
                json_object(
                    'ma5', CASE WHEN isnan(ma5) THEN NULL ELSE round(ma5, 2) END,
                    'ma10', CASE WHEN isnan(ma10) THEN NULL ELSE round(ma10, 2) END,
                    'ma20', CASE WHEN isnan(ma20) THEN NULL ELSE round(ma20, 2) END,
                    'ma60', CASE WHEN isnan(ma60) THEN NULL ELSE round(ma60, 2) END,
                    'ma120', CASE WHEN isnan(ma120) THEN NULL ELSE round(ma120, 2) END,
                    'vol_ma5', CASE WHEN isnan(vol_ma5) THEN NULL ELSE round(vol_ma5, 2) END,
                    'high_250', CASE WHEN isnan(high_250) THEN NULL ELSE round(high_250, 2) END,
                    'avg_ret_60', CASE WHEN isnan(avg_ret_60) THEN NULL ELSE round(avg_ret_60, 4) END,
                    'vol_60', CASE WHEN isnan(vol_60) THEN NULL ELSE round(vol_60, 4) END,
                    'rps_20', CASE WHEN isnan(rps_20) THEN NULL ELSE round(rps_20, 1) END,
                    'rps_50', CASE WHEN isnan(rps_50) THEN NULL ELSE round(rps_50, 1) END,
                    'rps_120', CASE WHEN isnan(rps_120) THEN NULL ELSE round(rps_120, 1) END,
                    'rps_250', CASE WHEN isnan(rps_250) THEN NULL ELSE round(rps_250, 1) END
                ) AS factors_json
            FROM RPSData
        )
        UPDATE daily_price
        SET factors = sub.factors_json
        FROM FinalFactors sub
        WHERE daily_price.ts_code = sub.ts_code
          AND daily_price.trade_date = sub.trade_date
        """

        try:
            with get_db_connection() as con:
                con.execute(update_query, [start_date, target_date, target_date])
            self.refresh_factor_snapshot(target_date)
            logger.info(f"已完成 {target_date} 的技术因子与宽表因子更新")
        except Exception as exc:
            logger.error(f"因子更新失败 {target_date}: {exc}")

    def calculate_batch(self, start_date_str: str, end_date_str: str):
        logger.info(f"批量计算 {start_date_str} 至 {end_date_str} 的因子...")

        window_start = arrow.get(start_date_str).shift(days=-450).format("YYYY-MM-DD")

        update_query = """
        WITH RawData AS (
            SELECT
                d.ts_code,
                d.trade_date,
                d.close,
                d.vol,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS ma5,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
                ) AS ma10,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS ma20,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS ma60,
                AVG(d.close) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 119 PRECEDING AND CURRENT ROW
                ) AS ma120,
                AVG(d.vol) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
                ) AS vol_ma5,
                MAX(d.high) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 249 PRECEDING AND CURRENT ROW
                ) AS high_250,
                AVG(d.pct_chg) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS avg_ret_60,
                STDDEV(d.pct_chg) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                ) AS vol_60,
                (d.close - LAG(d.close, 20) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date))
                    / NULLIF(LAG(d.close, 20) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date), 0) AS mom_20,
                (d.close - LAG(d.close, 50) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date))
                    / NULLIF(LAG(d.close, 50) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date), 0) AS mom_50,
                (d.close - LAG(d.close, 120) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date))
                    / NULLIF(LAG(d.close, 120) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date), 0) AS mom_120,
                (d.close - LAG(d.close, 250) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date))
                    / NULLIF(LAG(d.close, 250) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date), 0) AS mom_250,
                COUNT(*) OVER (
                    PARTITION BY d.ts_code
                    ORDER BY d.trade_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS row_num
            FROM daily_price d
            WHERE d.trade_date BETWEEN ? AND ?
        ),
        FilteredData AS (
            SELECT * FROM RawData WHERE trade_date BETWEEN ? AND ? AND row_num >= 20
        ),
        RPSData AS (
            SELECT
                *,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_20) * 100 AS rps_20,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_50) * 100 AS rps_50,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_120) * 100 AS rps_120,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_250) * 100 AS rps_250
            FROM FilteredData
        ),
        FinalFactors AS (
            SELECT
                ts_code,
                trade_date,
                json_object(
                    'ma5', CASE WHEN isnan(ma5) THEN NULL ELSE round(ma5, 2) END,
                    'ma10', CASE WHEN isnan(ma10) THEN NULL ELSE round(ma10, 2) END,
                    'ma20', CASE WHEN isnan(ma20) THEN NULL ELSE round(ma20, 2) END,
                    'ma60', CASE WHEN isnan(ma60) THEN NULL ELSE round(ma60, 2) END,
                    'ma120', CASE WHEN isnan(ma120) THEN NULL ELSE round(ma120, 2) END,
                    'vol_ma5', CASE WHEN isnan(vol_ma5) THEN NULL ELSE round(vol_ma5, 2) END,
                    'high_250', CASE WHEN isnan(high_250) THEN NULL ELSE round(high_250, 2) END,
                    'avg_ret_60', CASE WHEN isnan(avg_ret_60) THEN NULL ELSE round(avg_ret_60, 4) END,
                    'vol_60', CASE WHEN isnan(vol_60) THEN NULL ELSE round(vol_60, 4) END,
                    'rps_20', CASE WHEN isnan(rps_20) THEN NULL ELSE round(rps_20, 1) END,
                    'rps_50', CASE WHEN isnan(rps_50) THEN NULL ELSE round(rps_50, 1) END,
                    'rps_120', CASE WHEN isnan(rps_120) THEN NULL ELSE round(rps_120, 1) END,
                    'rps_250', CASE WHEN isnan(rps_250) THEN NULL ELSE round(rps_250, 1) END
                ) AS factors_json
            FROM RPSData
        )
        UPDATE daily_price
        SET factors = sub.factors_json
        FROM FinalFactors sub
        WHERE daily_price.ts_code = sub.ts_code
          AND daily_price.trade_date = sub.trade_date
        """

        try:
            with get_db_connection() as con:
                con.execute(update_query, [window_start, end_date_str, start_date_str, end_date_str])
            date_rows = fetch_df(
                """
                SELECT DISTINCT trade_date
                FROM daily_price
                WHERE trade_date BETWEEN ? AND ?
                ORDER BY trade_date
                """,
                params=[start_date_str, end_date_str],
            )
            for trade_date in date_rows["trade_date"].tolist():
                day = trade_date.strftime("%Y-%m-%d") if hasattr(trade_date, "strftime") else str(trade_date)
                self.refresh_factor_snapshot(day)
            logger.info(f"已完成 {start_date_str} 至 {end_date_str} 的因子批量更新")
        except Exception as exc:
            logger.error(f"批量因子更新失败 {start_date_str}-{end_date_str}: {exc}")

    def refresh_factor_snapshot(self, trade_date: str):
        """刷新单日 stock_factor_daily 宽表，并回写扩展因子到 daily_price.factors。"""
        try:
            target_date = arrow.get(trade_date).format("YYYY-MM-DD")
        except Exception as exc:
            logger.error(f"日期格式解析失败 {trade_date}: {exc}")
            return

        df = self._load_factor_inputs(target_date)
        if df.empty:
            logger.warning(f"{target_date} 缺少因子快照输入数据")
            return

        df = self._normalize_factor_inputs(df)
        factor_table_df, _ = self._build_factor_snapshot(df)
        if factor_table_df.empty:
            logger.warning(f"{target_date} 因子快照为空，跳过")
            return

        self._upsert_factor_snapshot(factor_table_df)
        self._sync_daily_price_factors_from_snapshot(target_date)

    def _load_factor_inputs(self, trade_date: str) -> pd.DataFrame:
        query = """
        WITH latest_fina AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ts_code
                        ORDER BY end_date DESC, ann_date DESC
                    ) AS rn
                FROM stock_fina_indicator
                WHERE end_date <= ?
            ) t
            WHERE rn = 1
        ),
        latest_express AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ts_code
                        ORDER BY end_date DESC, ann_date DESC
                    ) AS rn
                FROM stock_express
                WHERE end_date <= ?
            ) t
            WHERE rn = 1
        ),
        latest_members AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY ts_code
                        ORDER BY COALESCE(out_date, DATE '2999-12-31') DESC,
                                 COALESCE(in_date, DATE '1900-01-01') DESC,
                                 COALESCE(l3_code, '') DESC
                    ) AS rn
                FROM stock_index_member_all
                WHERE (in_date IS NULL OR in_date <= ?)
                  AND (out_date IS NULL OR out_date >= ?)
            ) t
            WHERE rn = 1
        )
        SELECT
            d.trade_date,
            d.ts_code,
            d.close,
            d.pct_chg,
            d.vol,
            d.amount,
            d.factors,
            b.industry,
            b.market,
            db.turnover_rate,
            db.turnover_rate_f,
            db.volume_ratio AS daily_basic_volume_ratio,
            db.pe,
            db.pe_ttm,
            db.pb,
            db.ps,
            db.ps_ttm,
            db.total_mv,
            db.circ_mv,
            COALESCE(mf.net_mf_amount, 0) AS net_mf_amount,
            mf.net_mf_ratio,
            mf.buy_lg_amount,
            mf.buy_elg_amount,
            mf.sell_lg_amount,
            mf.sell_elg_amount,
            fi.roe,
            fi.roe_yoy,
            fi.net_profit_margin,
            fi.gross_profit_margin,
            fi.total_rev_yoy,
            fi.profit_yoy,
            ex.revenue AS express_revenue,
            ex.operate_profit AS express_operate_profit,
            ex.n_income AS express_n_income,
            ex.yoy_net_profit AS express_yoy_net_profit,
            m.l1_name AS sw_l1_name,
            m.l2_name AS sw_l2_name,
            m.l3_name AS sw_l3_name
        FROM daily_price d
        LEFT JOIN stock_basic b
            ON d.ts_code = b.ts_code
        LEFT JOIN stock_daily_basic db
            ON d.ts_code = db.ts_code AND d.trade_date = db.trade_date
        LEFT JOIN stock_moneyflow mf
            ON d.ts_code = mf.ts_code AND d.trade_date = mf.trade_date
        LEFT JOIN latest_fina fi
            ON d.ts_code = fi.ts_code
        LEFT JOIN latest_express ex
            ON d.ts_code = ex.ts_code
        LEFT JOIN latest_members m
            ON d.ts_code = m.ts_code
        WHERE d.trade_date = ?
          AND d.vol > 0
        """
        return fetch_df(query, params=[trade_date, trade_date, trade_date, trade_date, trade_date])

    def _normalize_factor_inputs(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        factor_rows = []
        for value in df.get("factors", []):
            if not value:
                factor_rows.append({})
                continue
            if isinstance(value, dict):
                factor_rows.append(value)
                continue
            try:
                factor_rows.append(json.loads(value))
            except Exception:
                factor_rows.append({})

        factor_df = pd.DataFrame(factor_rows)
        if not factor_df.empty:
            factor_df.columns = [str(col) for col in factor_df.columns]
            df = pd.concat([df.drop(columns=["factors"]), factor_df], axis=1)
            df = df.loc[:, ~df.columns.duplicated(keep="first")]
        else:
            df = df.drop(columns=["factors"])

        numeric_cols = [
            "close",
            "pct_chg",
            "vol",
            "amount",
            "ma5",
            "ma10",
            "ma20",
            "ma60",
            "ma120",
            "vol_ma5",
            "high_250",
            "avg_ret_60",
            "vol_60",
            "rps_20",
            "rps_50",
            "rps_120",
            "rps_250",
            "turnover_rate",
            "turnover_rate_f",
            "daily_basic_volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "total_mv",
            "circ_mv",
            "net_mf_amount",
            "net_mf_ratio",
            "buy_lg_amount",
            "buy_elg_amount",
            "sell_lg_amount",
            "sell_elg_amount",
            "roe",
            "roe_yoy",
            "net_profit_margin",
            "gross_profit_margin",
            "total_rev_yoy",
            "profit_yoy",
            "express_revenue",
            "express_operate_profit",
            "express_n_income",
            "express_yoy_net_profit",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["volume_ratio"] = df["daily_basic_volume_ratio"]
        fallback_ratio = df["vol"] / df["vol_ma5"].replace(0, np.nan)
        df["volume_ratio"] = df["volume_ratio"].fillna(fallback_ratio).replace([np.inf, -np.inf], np.nan)
        df["big_order_ratio"] = (
            (df["buy_lg_amount"].fillna(0) + df["buy_elg_amount"].fillna(0))
            - (df["sell_lg_amount"].fillna(0) + df["sell_elg_amount"].fillna(0))
        ) / df["amount"].replace(0, np.nan)
        df["big_order_ratio"] = (df["big_order_ratio"] * 100).replace([np.inf, -np.inf], np.nan)

        df["ma20_gap"] = df["close"] / df["ma20"].replace(0, np.nan) - 1
        df["ma60_gap"] = df["close"] / df["ma60"].replace(0, np.nan) - 1
        df["high_250_gap"] = df["close"] / df["high_250"].replace(0, np.nan) - 1

        # 负估值或 0 估值不视为有效 value 因子
        for col in ("pe", "pe_ttm", "pb", "ps", "ps_ttm"):
            if col in df.columns:
                df.loc[df[col] <= 0, col] = np.nan

        return df

    def _build_factor_snapshot(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        df = df.copy()
        df["industry"] = df["industry"].fillna("")
        df["market"] = df["market"].fillna("")
        for col in ("sw_l1_name", "sw_l2_name", "sw_l3_name"):
            if col not in df.columns:
                df[col] = ""
            df[col] = df[col].fillna("")

        df["trend_score"] = self._weighted_available_score(
            {
                "rps_20": df["rps_20"],
                "rps_50": df["rps_50"],
                "rps_120": df["rps_120"],
                "rps_250": df["rps_250"],
                "ma20_gap": self._percentile_rank(df["ma20_gap"]),
                "ma60_gap": self._percentile_rank(df["ma60_gap"]),
                "high_250_gap": self._percentile_rank(df["high_250_gap"]),
            },
            {
                "rps_20": 0.12,
                "rps_50": 0.12,
                "rps_120": 0.18,
                "rps_250": 0.18,
                "ma20_gap": 0.14,
                "ma60_gap": 0.14,
                "high_250_gap": 0.12,
            },
        )

        df["liquidity_score"] = self._weighted_available_score(
            {
                "turnover_rate": self._percentile_rank(df["turnover_rate"]),
                "turnover_rate_f": self._percentile_rank(df["turnover_rate_f"]),
                "volume_ratio": self._percentile_rank(df["volume_ratio"]),
                "circ_mv": self._percentile_rank(df["circ_mv"], higher_is_better=False),
            },
            {
                "turnover_rate": 0.35,
                "turnover_rate_f": 0.15,
                "volume_ratio": 0.35,
                "circ_mv": 0.15,
            },
        )

        df["quality_score"] = self._weighted_available_score(
            {
                "roe": self._percentile_rank(df["roe"]),
                "roe_yoy": self._percentile_rank(df["roe_yoy"]),
                "net_profit_margin": self._percentile_rank(df["net_profit_margin"]),
                "gross_profit_margin": self._percentile_rank(df["gross_profit_margin"]),
                "total_rev_yoy": self._percentile_rank(df["total_rev_yoy"]),
                "profit_yoy": self._percentile_rank(df["profit_yoy"]),
            },
            {
                "roe": 0.24,
                "roe_yoy": 0.12,
                "net_profit_margin": 0.16,
                "gross_profit_margin": 0.12,
                "total_rev_yoy": 0.18,
                "profit_yoy": 0.18,
            },
        )

        df["value_score"] = self._weighted_available_score(
            {
                "pe_ttm": self._percentile_rank(df["pe_ttm"], higher_is_better=False),
                "pb": self._percentile_rank(df["pb"], higher_is_better=False),
                "ps_ttm": self._percentile_rank(df["ps_ttm"], higher_is_better=False),
            },
            {
                "pe_ttm": 0.45,
                "pb": 0.30,
                "ps_ttm": 0.25,
            },
        )

        df["flow_score"] = self._weighted_available_score(
            {
                "net_mf_amount": self._percentile_rank(df["net_mf_amount"]),
                "net_mf_ratio": self._percentile_rank(df["net_mf_ratio"]),
                "big_order_ratio": self._percentile_rank(df["big_order_ratio"]),
            },
            {
                "net_mf_amount": 0.20,
                "net_mf_ratio": 0.45,
                "big_order_ratio": 0.35,
            },
        )

        df["event_score"] = self._weighted_available_score(
            {
                "express_revenue": self._percentile_rank(df["express_revenue"]),
                "express_operate_profit": self._percentile_rank(df["express_operate_profit"]),
                "express_n_income": self._percentile_rank(df["express_n_income"]),
                "express_yoy_net_profit": self._percentile_rank(df["express_yoy_net_profit"]),
            },
            {
                "express_revenue": 0.20,
                "express_operate_profit": 0.25,
                "express_n_income": 0.25,
                "express_yoy_net_profit": 0.30,
            },
        )

        df["factor_score"] = self._weighted_available_score(
            {
                "trend_score": df["trend_score"],
                "liquidity_score": df["liquidity_score"],
                "quality_score": df["quality_score"],
                "value_score": df["value_score"],
                "flow_score": df["flow_score"],
                "event_score": df["event_score"],
            },
            {
                "trend_score": 0.28,
                "liquidity_score": 0.14,
                "quality_score": 0.18,
                "value_score": 0.12,
                "flow_score": 0.18,
                "event_score": 0.10,
            },
        )

        factor_table_cols = [
            "trade_date",
            "ts_code",
            "industry",
            "market",
            "sw_l1_name",
            "sw_l2_name",
            "sw_l3_name",
            "close",
            "pct_chg",
            "ma5",
            "ma10",
            "ma20",
            "ma60",
            "ma120",
            "high_250",
            "vol_ma5",
            "avg_ret_60",
            "vol_60",
            "rps_20",
            "rps_50",
            "rps_120",
            "rps_250",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "total_mv",
            "circ_mv",
            "net_mf_amount",
            "net_mf_ratio",
            "big_order_ratio",
            "roe",
            "roe_yoy",
            "net_profit_margin",
            "gross_profit_margin",
            "total_rev_yoy",
            "profit_yoy",
            "express_revenue",
            "express_operate_profit",
            "express_n_income",
            "express_yoy_net_profit",
            "trend_score",
            "liquidity_score",
            "quality_score",
            "value_score",
            "flow_score",
            "event_score",
            "factor_score",
        ]

        factor_table_df = df[factor_table_cols].copy()
        factor_table_df = factor_table_df.replace([np.inf, -np.inf], np.nan)

        payload_df = df[["trade_date", "ts_code"]].copy()
        payload_df["factors_json"] = df.apply(self._build_daily_price_payload, axis=1)

        return factor_table_df, payload_df

    def _build_daily_price_payload(self, row: pd.Series) -> str:
        payload = {}
        for key in (
            "ma5",
            "ma10",
            "ma20",
            "ma60",
            "ma120",
            "vol_ma5",
            "high_250",
            "avg_ret_60",
            "vol_60",
            "rps_20",
            "rps_50",
            "rps_120",
            "rps_250",
            "turnover_rate",
            "turnover_rate_f",
            "volume_ratio",
            "pe",
            "pe_ttm",
            "pb",
            "ps",
            "ps_ttm",
            "total_mv",
            "circ_mv",
            "net_mf_ratio",
            "big_order_ratio",
            "trend_score",
            "liquidity_score",
            "quality_score",
            "value_score",
            "flow_score",
            "event_score",
            "factor_score",
        ):
            value = row.get(key)
            if value is None or (isinstance(value, float) and (np.isnan(value) or np.isinf(value))):
                payload[key] = None
            elif isinstance(value, (np.floating, float)):
                payload[key] = round(float(value), 4)
            else:
                payload[key] = value
        return json.dumps(payload, ensure_ascii=False)

    def _upsert_factor_snapshot(self, df: pd.DataFrame):
        if df.empty:
            return

        with get_db_connection() as con:
            con.register("factor_snapshot_view", df)
            cols = df.columns.tolist()
            col_str = ",".join(cols)
            update_set = ",".join(
                f"{col}=EXCLUDED.{col}" for col in cols if col not in ("trade_date", "ts_code")
            )
            con.execute(
                f"""
                INSERT INTO stock_factor_daily ({col_str})
                SELECT {col_str} FROM factor_snapshot_view
                ON CONFLICT (trade_date, ts_code) DO UPDATE SET
                    {update_set}
                """
            )

    def _sync_daily_price_factors_from_snapshot(self, trade_date: str):
        with get_db_connection() as con:
            con.execute(
                """
                UPDATE daily_price
                SET factors = json_object(
                    'ma5', round(f.ma5, 4),
                    'ma10', round(f.ma10, 4),
                    'ma20', round(f.ma20, 4),
                    'ma60', round(f.ma60, 4),
                    'ma120', round(f.ma120, 4),
                    'vol_ma5', round(f.vol_ma5, 4),
                    'high_250', round(f.high_250, 4),
                    'avg_ret_60', round(f.avg_ret_60, 4),
                    'vol_60', round(f.vol_60, 4),
                    'rps_20', round(f.rps_20, 4),
                    'rps_50', round(f.rps_50, 4),
                    'rps_120', round(f.rps_120, 4),
                    'rps_250', round(f.rps_250, 4),
                    'turnover_rate', round(f.turnover_rate, 4),
                    'turnover_rate_f', round(f.turnover_rate_f, 4),
                    'volume_ratio', round(f.volume_ratio, 4),
                    'pe', round(f.pe, 4),
                    'pe_ttm', round(f.pe_ttm, 4),
                    'pb', round(f.pb, 4),
                    'ps', round(f.ps, 4),
                    'ps_ttm', round(f.ps_ttm, 4),
                    'total_mv', round(f.total_mv, 4),
                    'circ_mv', round(f.circ_mv, 4),
                    'net_mf_ratio', round(f.net_mf_ratio, 4),
                    'big_order_ratio', round(f.big_order_ratio, 4),
                    'trend_score', round(f.trend_score, 4),
                    'liquidity_score', round(f.liquidity_score, 4),
                    'quality_score', round(f.quality_score, 4),
                    'value_score', round(f.value_score, 4),
                    'flow_score', round(f.flow_score, 4),
                    'event_score', round(f.event_score, 4),
                    'factor_score', round(f.factor_score, 4)
                )
                FROM stock_factor_daily f
                WHERE daily_price.trade_date = f.trade_date
                  AND daily_price.ts_code = f.ts_code
                  AND f.trade_date = ?
                """,
                [trade_date],
            )

    def _percentile_rank(self, series: pd.Series, higher_is_better: bool = True) -> pd.Series:
        series = pd.to_numeric(series, errors="coerce")
        if series.notna().sum() == 0:
            return pd.Series(np.nan, index=series.index)
        ranked = series.rank(pct=True, ascending=higher_is_better)
        return ranked * 100

    def _weighted_available_score(self, inputs: dict[str, pd.Series], weights: dict[str, float]) -> pd.Series:
        result = pd.Series(0.0, index=next(iter(inputs.values())).index, dtype="float64")
        weight_sum = pd.Series(0.0, index=result.index, dtype="float64")

        for key, values in inputs.items():
            numeric = pd.to_numeric(values, errors="coerce")
            mask = numeric.notna()
            result.loc[mask] += numeric.loc[mask] * float(weights.get(key, 0.0))
            weight_sum.loc[mask] += float(weights.get(key, 0.0))

        scored = result / weight_sum.replace(0, np.nan)
        return scored.fillna(50.0).clip(lower=0.0, upper=100.0).round(2)


factor_calculator = FactorCalculator()

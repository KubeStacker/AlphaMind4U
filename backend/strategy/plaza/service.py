from __future__ import annotations

import json

import arrow
import pandas as pd

from db.connection import fetch_df, get_db_connection
from etl.calendar import trading_calendar
from strategy.plaza.base import ObservationCandidate
from strategy.plaza.registry import list_enabled_strategies, list_registered_strategies
from strategy.plaza.summarizer import build_strategy_summary_text


def _shift_trade_date(date_str: str, offset: int) -> str:
    current = arrow.get(date_str)
    moved = 0
    while moved < offset:
        current = current.shift(days=1)
        if trading_calendar.is_trading_day(current.date()):
            moved += 1
    return current.format("YYYY-MM-DD")


def build_horizon_metrics(price_df: pd.DataFrame, entry_price: float, horizon: int) -> dict:
    if price_df.empty or len(price_df) < horizon + 1 or not entry_price:
        return {}

    window = price_df.iloc[: horizon + 1].copy()
    exit_close = float(window.iloc[-1]["close"])
    max_high = float(window["high"].max())
    min_low = float(window["low"].min())
    return {
        "ret_pct": round((exit_close / entry_price - 1.0) * 100.0, 4),
        "max_gain_pct": round((max_high / entry_price - 1.0) * 100.0, 4),
        "max_drawdown_pct": round((min_low / entry_price - 1.0) * 100.0, 4),
    }


class StrategyPlazaService:
    def __init__(self) -> None:
        self._history_cache: dict[str, tuple[int, pd.DataFrame]] = {}
        self._sector_overview_cache: dict[str, dict] = {}
        self._market_regime_cache: dict[str, dict] = {}

    def load_history_frame(
        self,
        trade_date: str,
        lookback_days: int = 260,
        ts_codes: list[str] | None = None,
    ) -> pd.DataFrame:
        if not ts_codes:
            cached = self._history_cache.get(trade_date)
            if cached is not None:
                cached_lookback, cached_frame = cached
                if cached_lookback >= lookback_days:
                    return cached_frame

        params: list = [trade_date]
        universe_filters = [
            "d.trade_date = ?",
            "d.vol > 0",
            "d.close >= 3",
            "d.amount >= 50000",
            "COALESCE(b.list_status, 'L') = 'L'",
        ]
        if ts_codes:
            normalized_codes = [str(code or "").strip().upper() for code in ts_codes if str(code or "").strip()]
            if not normalized_codes:
                return pd.DataFrame()
            placeholders = ",".join(["?"] * len(normalized_codes))
            universe_filters.append(f"d.ts_code IN ({placeholders})")
            params.extend(normalized_codes)

        params.extend([trade_date, lookback_days])
        history_df = fetch_df(
            f"""
            WITH universe AS (
                SELECT d.ts_code
                FROM daily_price d
                JOIN stock_basic b
                  ON d.ts_code = b.ts_code
                WHERE {' AND '.join(universe_filters)}
            ),
            ranked AS (
                SELECT
                    CAST(d.trade_date AS VARCHAR) AS trade_date,
                    d.ts_code,
                    b.name,
                    d.open,
                    d.high,
                    d.low,
                    d.close,
                    d.pct_chg,
                    d.vol,
                    d.amount,
                    f.ma5,
                    f.ma10,
                    f.ma20,
                    f.ma60,
                    f.turnover_rate,
                    f.volume_ratio,
                    f.rps_20,
                    f.rps_50,
                    f.rps_120,
                    f.trend_score,
                    f.factor_score,
                    ROW_NUMBER() OVER (PARTITION BY d.ts_code ORDER BY d.trade_date DESC) AS rn
                FROM daily_price d
                JOIN universe u
                  ON d.ts_code = u.ts_code
                JOIN stock_basic b
                  ON d.ts_code = b.ts_code
                LEFT JOIN stock_factor_daily f
                  ON d.ts_code = f.ts_code AND d.trade_date = f.trade_date
                WHERE d.trade_date <= ?
                  AND COALESCE(b.list_status, 'L') = 'L'
            )
            SELECT *
            FROM ranked
            WHERE rn <= ?
            ORDER BY ts_code, trade_date
            """,
            params,
        )
        if not ts_codes:
            self._history_cache[trade_date] = (lookback_days, history_df)
        return history_df

    def get_sector_meta(self, trade_date: str, ts_codes: list[str]) -> dict[str, dict]:
        overview = self._load_sector_overview(trade_date)
        if not ts_codes:
            return {}
        try:
            from strategy.mainline.analyst import mainline_analyst

            stock_map_df = mainline_analyst.get_stock_mainline_map(ts_codes=ts_codes)
        except Exception:
            stock_map_df = pd.DataFrame(columns=["ts_code", "mapped_name"])

        mapping = {}
        if stock_map_df is not None and not stock_map_df.empty:
            mapping = {
                str(row["ts_code"]).strip().upper(): str(row["mapped_name"]).strip()
                for _, row in stock_map_df.iterrows()
                if str(row.get("ts_code") or "").strip() and str(row.get("mapped_name") or "").strip()
            }

        result: dict[str, dict] = {}
        review_map = overview.get("review_map", {})
        rank_map = overview.get("rank_map", {})
        total = int(overview.get("sector_total") or 0)
        for code in ts_codes:
            norm_code = str(code or "").strip().upper()
            mapped_sector = mapping.get(norm_code, "")
            review_item = review_map.get(mapped_sector, {}) if mapped_sector else {}
            result[norm_code] = {
                "mapped_sector": mapped_sector,
                "display_name": review_item.get("display_name") or mapped_sector,
                "focus_tags": review_item.get("focus_tags", []) or [],
                "driver_summary": review_item.get("driver_summary", "") or "",
                "sector_heat_rank": rank_map.get(mapped_sector),
                "sector_heat_total": total,
                "is_mainline": mapped_sector in rank_map,
            }
        return result

    def get_market_regime(self, trade_date: str) -> dict:
        cached = self._market_regime_cache.get(trade_date)
        if cached is not None:
            return cached

        index_df = fetch_df(
            """
            WITH ranked AS (
                SELECT
                    CAST(trade_date AS VARCHAR) AS trade_date,
                    ts_code,
                    close,
                    ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) AS rn
                FROM market_index
                WHERE trade_date <= ?
                  AND ts_code IN ('000001.SH', '399006.SZ')
            )
            SELECT *
            FROM ranked
            WHERE rn <= 25
            ORDER BY ts_code, trade_date
            """,
            [trade_date],
        )
        if index_df.empty:
            regime = {"is_supportive": True, "reason": "大盘环境暂无数据"}
            self._market_regime_cache[trade_date] = regime
            return regime

        work = index_df.copy()
        work["close"] = pd.to_numeric(work["close"], errors="coerce")
        supportive = False
        notes = []
        for ts_code, group in work.groupby("ts_code", sort=False):
            series = group.sort_values("trade_date").reset_index(drop=True)
            latest_close = float(series.iloc[-1]["close"])
            ma20 = float(series["close"].rolling(20, min_periods=1).mean().iloc[-1])
            ma10 = float(series["close"].rolling(10, min_periods=1).mean().iloc[-1])
            rebound = len(series) >= 3 and latest_close >= float(series["close"].tail(3).min())
            current_ok = latest_close >= ma20 or (latest_close >= ma10 and rebound)
            supportive = supportive or current_ok
            notes.append(f"{ts_code} {'站上' if current_ok else '未站上'} MA20")
        regime = {"is_supportive": supportive, "reason": "；".join(notes[:2])}
        self._market_regime_cache[trade_date] = regime
        return regime

    def _load_sector_overview(self, trade_date: str) -> dict:
        cached = self._sector_overview_cache.get(trade_date)
        if cached is not None:
            return cached

        latest_rows = fetch_df(
            """
            SELECT mapped_name, score
            FROM mainline_scores
            WHERE trade_date = (
                SELECT MAX(trade_date)
                FROM mainline_scores
                WHERE trade_date <= ?
            )
            ORDER BY score DESC
            LIMIT 12
            """,
            [trade_date],
        )
        rank_map = {
            str(row["mapped_name"]).strip(): idx + 1
            for idx, (_, row) in enumerate(latest_rows.iterrows())
            if str(row.get("mapped_name") or "").strip()
        }
        review_map: dict[str, dict] = {}
        try:
            from strategy.mainline.analyst import mainline_analyst

            history = mainline_analyst.get_history(days=10) or {}
            review_mainlines = (((history.get("analysis") or {}).get("review_10d") or {}).get("mainlines") or [])
            review_map = {
                str(item.get("name") or "").strip(): item
                for item in review_mainlines
                if str(item.get("name") or "").strip()
            }
        except Exception:
            review_map = {}

        payload = {
            "rank_map": rank_map,
            "sector_total": len(rank_map),
            "review_map": review_map,
        }
        self._sector_overview_cache[trade_date] = payload
        return payload

    def sync_definitions(self) -> list[dict]:
        rows = []
        strategies = list_registered_strategies()

        with get_db_connection() as con:
            registered_keys = [strategy.meta().strategy_key for strategy in strategies]
            if registered_keys:
                placeholders = ",".join(["?"] * len(registered_keys))
                con.execute(
                    f"DELETE FROM strategy_definitions WHERE strategy_key NOT IN ({placeholders})",
                    registered_keys,
                )
            else:
                con.execute("DELETE FROM strategy_definitions")

            for strategy in strategies:
                meta = strategy.meta()
                con.execute(
                    """
                    INSERT OR REPLACE INTO strategy_definitions (
                        strategy_key, name, description, enabled, display_order, engine_version, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        meta.strategy_key,
                        meta.name,
                        meta.description,
                        meta.enabled,
                        meta.display_order,
                        meta.engine_version,
                    ),
                )
                rows.append(
                    {
                        "strategy_key": meta.strategy_key,
                        "name": meta.name,
                        "description": meta.description,
                        "enabled": meta.enabled,
                        "display_order": meta.display_order,
                        "engine_version": meta.engine_version,
                    }
                )
        return rows

    def run_for_date(self, trade_date: str, strategy_key: str | None = None) -> dict:
        self._clear_runtime_cache()
        try:
            self.sync_definitions()
            strategies = [
                strategy
                for strategy in list_enabled_strategies()
                if not strategy_key or strategy.meta().strategy_key == strategy_key
            ]

            observation_count = 0
            processed_keys = []
            for strategy in strategies:
                current_key = strategy.meta().strategy_key
                items = strategy.run_for_date(trade_date, context=self)
                observation_count += self._persist_strategy_rows(current_key, trade_date, items)
                self._refresh_strategy_summary(current_key, trade_date)
                processed_keys.append(current_key)

            completed = self.complete_pending_backtests()
            if completed:
                for current_key in processed_keys:
                    self._refresh_strategy_summary(current_key, trade_date)
            return {
                "trade_date": trade_date,
                "strategy_count": len(strategies),
                "observation_count": observation_count,
                "completed_backtests": completed,
            }
        finally:
            self._clear_runtime_cache()

    def _clear_runtime_cache(self) -> None:
        self._history_cache.clear()
        self._sector_overview_cache.clear()
        self._market_regime_cache.clear()

    def _persist_strategy_rows(
        self,
        strategy_key: str,
        trade_date: str,
        rows: list[ObservationCandidate],
    ) -> int:
        with get_db_connection() as con:
            con.execute(
                """
                DELETE FROM strategy_observations
                WHERE strategy_key = ? AND observation_date = ?
                """,
                (strategy_key, trade_date),
            )
            con.execute(
                """
                DELETE FROM strategy_backtest_runs
                WHERE strategy_key = ? AND observation_date = ?
                """,
                (strategy_key, trade_date),
            )
            for item in rows:
                con.execute(
                    """
                    INSERT OR REPLACE INTO strategy_observations (
                        strategy_key, trade_date, observation_date, ts_code, name, reason,
                        tags_json, entry_anchor_date, trace_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        strategy_key,
                        trade_date,
                        item.observation_date,
                        item.ts_code,
                        item.name,
                        item.reason,
                        json.dumps(item.tags, ensure_ascii=False),
                        item.entry_anchor_date,
                        json.dumps(item.trace, ensure_ascii=False),
                    ),
                )
                entry_price = self._resolve_entry_price(item.ts_code, item.entry_anchor_date, item.entry_price_source)
                con.execute(
                    """
                    INSERT OR REPLACE INTO strategy_backtest_runs (
                        strategy_key, observation_date, ts_code, entry_anchor_date,
                        entry_price, entry_price_source, status, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'PENDING', CURRENT_TIMESTAMP)
                    """,
                    (
                        strategy_key,
                        item.observation_date,
                        item.ts_code,
                        item.entry_anchor_date,
                        entry_price,
                        item.entry_price_source,
                    ),
                )
        return len(rows)

    def _refresh_strategy_summary(self, strategy_key: str, trade_date: str) -> None:
        window_trade_days = 120
        summary_df = fetch_df(
            """
            WITH latest_days AS (
                SELECT DISTINCT observation_date
                FROM strategy_backtest_runs
                WHERE strategy_key = ?
                  AND observation_date <= ?
                ORDER BY observation_date DESC
                LIMIT ?
            )
            SELECT
                COUNT(*) AS observation_count,
                COUNT(ret_3d) AS completed_count_3d,
                COUNT(ret_5d) AS completed_count_5d,
                COUNT(ret_10d) AS completed_count_10d,
                AVG(CASE WHEN ret_3d > 0 THEN 100.0 ELSE 0.0 END) AS win_rate_3d,
                AVG(CASE WHEN ret_5d > 0 THEN 100.0 ELSE 0.0 END) AS win_rate_5d,
                AVG(CASE WHEN ret_10d > 0 THEN 100.0 ELSE 0.0 END) AS win_rate_10d,
                AVG(ret_3d) AS avg_ret_3d,
                AVG(ret_5d) AS avg_ret_5d,
                AVG(ret_10d) AS avg_ret_10d,
                MEDIAN(ret_3d) AS median_ret_3d,
                MEDIAN(ret_5d) AS median_ret_5d,
                MEDIAN(ret_10d) AS median_ret_10d,
                AVG(max_gain_3d) AS avg_max_gain_3d,
                AVG(max_gain_5d) AS avg_max_gain_5d,
                AVG(max_gain_10d) AS avg_max_gain_10d,
                AVG(max_drawdown_3d) AS avg_max_drawdown_3d,
                AVG(max_drawdown_5d) AS avg_max_drawdown_5d,
                AVG(max_drawdown_10d) AS avg_max_drawdown_10d
            FROM strategy_backtest_runs
            WHERE strategy_key = ?
              AND observation_date IN (SELECT observation_date FROM latest_days)
            """,
            [strategy_key, trade_date, window_trade_days, strategy_key],
        )
        summary = summary_df.iloc[0].to_dict() if not summary_df.empty else {"observation_count": 0}
        summary_text = build_strategy_summary_text(summary)

        with get_db_connection() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO strategy_daily_summaries (
                    strategy_key, trade_date, window_trade_days, observation_count,
                    completed_count_3d, completed_count_5d, completed_count_10d,
                    win_rate_3d, win_rate_5d, win_rate_10d,
                    avg_ret_3d, avg_ret_5d, avg_ret_10d,
                    median_ret_3d, median_ret_5d, median_ret_10d,
                    avg_max_gain_3d, avg_max_gain_5d, avg_max_gain_10d,
                    avg_max_drawdown_3d, avg_max_drawdown_5d, avg_max_drawdown_10d,
                    summary_text, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    strategy_key,
                    trade_date,
                    window_trade_days,
                    int(summary.get("observation_count") or 0),
                    int(summary.get("completed_count_3d") or 0),
                    int(summary.get("completed_count_5d") or 0),
                    int(summary.get("completed_count_10d") or 0),
                    summary.get("win_rate_3d"),
                    summary.get("win_rate_5d"),
                    summary.get("win_rate_10d"),
                    summary.get("avg_ret_3d"),
                    summary.get("avg_ret_5d"),
                    summary.get("avg_ret_10d"),
                    summary.get("median_ret_3d"),
                    summary.get("median_ret_5d"),
                    summary.get("median_ret_10d"),
                    summary.get("avg_max_gain_3d"),
                    summary.get("avg_max_gain_5d"),
                    summary.get("avg_max_gain_10d"),
                    summary.get("avg_max_drawdown_3d"),
                    summary.get("avg_max_drawdown_5d"),
                    summary.get("avg_max_drawdown_10d"),
                    summary_text,
                ),
            )

    def complete_pending_backtests(self, limit: int = 200) -> int:
        pending = fetch_df(
            """
            SELECT strategy_key, observation_date, ts_code, entry_anchor_date, entry_price
            FROM strategy_backtest_runs
            WHERE status IN ('PENDING', 'PARTIAL')
            ORDER BY observation_date, ts_code
            LIMIT ?
            """,
            [limit],
        )
        completed = 0
        if pending.empty:
            return completed

        with get_db_connection() as con:
            for _, row in pending.iterrows():
                if not row["entry_price"]:
                    continue

                price_df = fetch_df(
                    """
                    SELECT CAST(trade_date AS VARCHAR) AS trade_date, close, high, low
                    FROM daily_price
                    WHERE ts_code = ? AND trade_date >= ?
                    ORDER BY trade_date
                    LIMIT 16
                    """,
                    [row["ts_code"], row["entry_anchor_date"]],
                )
                metrics_3d = build_horizon_metrics(price_df, float(row["entry_price"]), 3)
                metrics_5d = build_horizon_metrics(price_df, float(row["entry_price"]), 5)
                metrics_10d = build_horizon_metrics(price_df, float(row["entry_price"]), 10)

                completed_horizon = 0
                if metrics_3d:
                    completed_horizon = 3
                if metrics_5d:
                    completed_horizon = 5
                if metrics_10d:
                    completed_horizon = 10
                status = "COMPLETED" if metrics_10d else ("PARTIAL" if completed_horizon else "PENDING")
                last_eval_date = price_df.iloc[-1]["trade_date"] if not price_df.empty else None

                con.execute(
                    """
                    UPDATE strategy_backtest_runs
                    SET ret_3d = ?, max_gain_3d = ?, max_drawdown_3d = ?,
                        ret_5d = ?, max_gain_5d = ?, max_drawdown_5d = ?,
                        ret_10d = ?, max_gain_10d = ?, max_drawdown_10d = ?,
                        last_completed_horizon = ?, last_eval_date = ?, status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE strategy_key = ? AND observation_date = ? AND ts_code = ?
                    """,
                    (
                        metrics_3d.get("ret_pct"),
                        metrics_3d.get("max_gain_pct"),
                        metrics_3d.get("max_drawdown_pct"),
                        metrics_5d.get("ret_pct"),
                        metrics_5d.get("max_gain_pct"),
                        metrics_5d.get("max_drawdown_pct"),
                        metrics_10d.get("ret_pct"),
                        metrics_10d.get("max_gain_pct"),
                        metrics_10d.get("max_drawdown_pct"),
                        completed_horizon,
                        last_eval_date,
                        status,
                        row["strategy_key"],
                        row["observation_date"],
                        row["ts_code"],
                    ),
                )
                completed += 1
        return completed

    def _resolve_entry_price(self, ts_code: str, entry_anchor_date: str, entry_price_source: str) -> float | None:
        if entry_price_source == "open_next_trade_day":
            target_date = _shift_trade_date(entry_anchor_date, 1)
            field = "open"
        elif entry_price_source == "close_next_trade_day":
            target_date = _shift_trade_date(entry_anchor_date, 1)
            field = "close"
        else:
            target_date = entry_anchor_date
            field = "close"

        df = fetch_df(
            f"SELECT {field} AS price FROM daily_price WHERE ts_code = ? AND trade_date = ? LIMIT 1",
            [ts_code, target_date],
        )
        if df.empty or pd.isna(df.iloc[0]["price"]):
            return None
        return float(df.iloc[0]["price"])

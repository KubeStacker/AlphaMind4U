from __future__ import annotations

import json
import os
from typing import Any

import numpy as np
import pandas as pd

from db.connection import fetch_df
from strategy.mainline import mainline_analyst


class DuckDbFalconDataProvider:
    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            v = float(value)
            if pd.isna(v):
                return float(default)
            return v
        except Exception:
            return float(default)

    @staticmethod
    def _score_to_target_position(score: float, benchmark_ok: bool) -> float:
        if not benchmark_ok:
            return 0.0
        if score >= 75:
            return 0.9
        if score >= 65:
            return 0.7
        if score >= 55:
            return 0.5
        if score >= 45:
            return 0.3
        return 0.0

    def latest_trade_date(self) -> str:
        # 优化：先取一个较近的时间范围（最近30天），避免全表扫描进行分组计数
        df = fetch_df(
            """
            SELECT trade_date
            FROM daily_price
            WHERE trade_date >= (SELECT MAX(trade_date) FROM daily_price) - INTERVAL 30 DAY
            GROUP BY trade_date
            HAVING COUNT(*) > 1000
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        if df.empty:
            # 如果最近30天没找到符合条件的，则退化为全表（兜底，通常不会发生）
            df = fetch_df(
                """
                SELECT trade_date
                FROM daily_price
                GROUP BY trade_date
                HAVING COUNT(*) > 1000
                ORDER BY trade_date DESC
                LIMIT 1
                """
            )

        if df.empty:
            raise ValueError("无可用交易日数据")
        return str(df.iloc[0]["trade_date"])[:10]

    def list_trade_dates(self, start_date: str, end_date: str) -> list[str]:
        if not start_date or not end_date:
            return []
        df = fetch_df(
            """
            SELECT trade_date
            FROM daily_price
            WHERE trade_date BETWEEN ? AND ?
            GROUP BY trade_date
            HAVING COUNT(*) > 1000
            ORDER BY trade_date ASC
            """,
            [start_date, end_date],
        )
        if df.empty:
            return []
        return [str(v)[:10] for v in df["trade_date"].tolist()]

    def _load_benchmark_state(self, trade_date: str) -> dict[str, Any]:
        env_code = os.getenv("FALCON_BENCHMARK_CODE", "SPY").strip()
        candidates: list[str] = []
        for c in [env_code, "SPY", "^GSPC", "000001.SH", "399300.SZ"]:
            if c and c not in candidates:
                candidates.append(c)

        def _query_state(table_name: str, code: str) -> dict[str, Any] | None:
            q = f"""
            WITH h AS (
                SELECT trade_date,
                       close,
                       AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                       AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
                       LAG(close, 20) OVER (ORDER BY trade_date) AS close_20
                FROM {table_name}
                WHERE ts_code = ? AND trade_date <= ?
            )
            SELECT trade_date, close, ma20, ma50, close_20
            FROM h
            WHERE trade_date = ?
            LIMIT 1
            """
            d = fetch_df(q, [code, trade_date, trade_date])
            if d.empty:
                return None
            r = d.iloc[0]
            close = float(r.get("close") or 0.0)
            ma20 = float(r.get("ma20") or 0.0)
            ma50 = float(r.get("ma50") or 0.0)
            close_20 = float(r.get("close_20") or 0.0)
            ret20 = ((close / close_20) - 1.0) * 100 if close_20 > 0 else 0.0
            ok = bool(close > ma50 and ma20 > ma50)
            return {
                "benchmark_code": code,
                "benchmark_ok": ok,
                "benchmark_close": close,
                "benchmark_ma20": ma20,
                "benchmark_ma50": ma50,
                "benchmark_ret20": ret20,
                "benchmark_source": table_name,
            }

        for code in candidates:
            state = _query_state("market_index", code)
            if state:
                return state
            state = _query_state("daily_price", code)
            if state:
                return state

        return {
            "benchmark_code": env_code or "SPY",
            "benchmark_ok": True,
            "benchmark_close": 0.0,
            "benchmark_ma20": 0.0,
            "benchmark_ma50": 0.0,
            "benchmark_ret20": 0.0,
            "benchmark_source": "missing",
        }

    def _load_market_sentiment_state(self, trade_date: str, benchmark_state: dict[str, Any]) -> dict[str, Any]:
        fallback_score = 65.0 if bool(benchmark_state.get("benchmark_ok")) else 35.0
        fallback_target = self._score_to_target_position(
            fallback_score,
            bool(benchmark_state.get("benchmark_ok")),
        )
        fallback = {
            "market_trade_date": trade_date,
            "market_sentiment_score": fallback_score,
            "market_label": "趋势跟随" if benchmark_state.get("benchmark_ok") else "风控观望",
            "market_signal": "PLAN_BUY_PARTIAL" if benchmark_state.get("benchmark_ok") else "PLAN_WATCH",
            "market_action": "BUY" if benchmark_state.get("benchmark_ok") else "WATCH",
            "market_target_position": fallback_target,
            "market_confidence": 55.0,
            "market_risk_factor": 45.0 if benchmark_state.get("benchmark_ok") else 75.0,
            "market_opportunity_factor": 60.0 if benchmark_state.get("benchmark_ok") else 25.0,
            "market_mood": "BULL" if benchmark_state.get("benchmark_ok") else "CONFUSED",
            "market_strategy_text": "基于基准趋势的默认仓位",
        }
        try:
            df = fetch_df(
                """
                SELECT trade_date, score, label, details
                FROM market_sentiment
                WHERE trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                [trade_date],
            )
        except Exception:
            return fallback

        if df.empty:
            return fallback

        row = df.iloc[0]
        details = row.get("details")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except Exception:
                details = {}
        details = details if isinstance(details, dict) else {}
        execution = details.get("execution", {}) if isinstance(details.get("execution"), dict) else {}
        score = self._safe_float(row.get("score"), fallback_score)
        signal = str(details.get("signal") or fallback["market_signal"])
        action = str(details.get("action") or execution.get("action") or fallback["market_action"])
        derived_target = self._score_to_target_position(score, bool(benchmark_state.get("benchmark_ok")))
        target_position = self._safe_float(execution.get("target_position"), -1.0)
        if action in {"BUY", "HOLD"}:
            target_position = max(target_position, derived_target)
        elif target_position < 0:
            target_position = derived_target

        return {
            "market_trade_date": str(row.get("trade_date"))[:10],
            "market_sentiment_score": round(score, 2),
            "market_label": str(row.get("label") or fallback["market_label"]),
            "market_signal": signal,
            "market_action": action,
            "market_target_position": round(float(max(0.0, min(1.0, target_position))), 4),
            "market_confidence": round(self._safe_float(execution.get("confidence"), 50.0), 2),
            "market_risk_factor": round(self._safe_float(details.get("risk"), details.get("risk_factor", 50.0)), 2),
            "market_opportunity_factor": round(self._safe_float(details.get("opp"), details.get("opportunity_factor", 50.0)), 2),
            "market_mood": str(details.get("mood") or fallback["market_mood"]),
            "market_strategy_text": str(details.get("strategy") or fallback["market_strategy_text"]),
        }

    def _load_mainline_persistence_snapshot(
        self,
        trade_date: str,
        lookback_days: int = 30,
    ) -> pd.DataFrame:
        empty_df = pd.DataFrame(
            columns=[
                "mainline_name",
                "mainline_persistence_score",
                "mainline_top3_days",
                "mainline_top8_days",
                "mainline_current_streak",
                "mainline_avg_rank_hist",
            ]
        )
        try:
            hist_df = fetch_df(
                """
                WITH recent_dates AS (
                    SELECT trade_date
                    FROM mainline_scores
                    WHERE trade_date <= ?
                    GROUP BY trade_date
                    ORDER BY trade_date DESC
                    LIMIT ?
                ),
                ranked AS (
                    SELECT
                        s.trade_date,
                        s.mapped_name,
                        s.score,
                        s.limit_ups,
                        s.stock_count,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.trade_date
                            ORDER BY s.score DESC, s.limit_ups DESC, s.stock_count DESC
                        ) AS daily_rank
                    FROM mainline_scores s
                    JOIN recent_dates d
                      ON s.trade_date = d.trade_date
                )
                SELECT trade_date, mapped_name, score, daily_rank
                FROM ranked
                WHERE daily_rank <= 12
                ORDER BY trade_date ASC, daily_rank ASC
                """,
                [trade_date, max(10, int(lookback_days))],
            )
        except Exception:
            return empty_df

        if hist_df.empty:
            return empty_df

        hist_df["trade_date"] = pd.to_datetime(hist_df["trade_date"])
        hist_df["score"] = pd.to_numeric(hist_df["score"], errors="coerce").fillna(0.0)
        hist_df["daily_rank"] = pd.to_numeric(hist_df["daily_rank"], errors="coerce").fillna(99.0)
        recent_dates = sorted(hist_df["trade_date"].drop_duplicates().tolist())
        if not recent_dates:
            return empty_df

        date_pos = {d: idx for idx, d in enumerate(recent_dates)}
        total_days = len(recent_dates)
        rows: list[dict[str, Any]] = []
        for mapped_name, group in hist_df.groupby("mapped_name", sort=False):
            grp = group.sort_values("trade_date")
            top3_days = int((grp["daily_rank"] <= 3).sum())
            top8_days = int((grp["daily_rank"] <= 8).sum())
            avg_rank = float(grp["daily_rank"].mean()) if not grp.empty else 99.0
            avg_score = float(grp["score"].mean()) if not grp.empty else 0.0

            pos_set = {date_pos[d] for d in grp["trade_date"].tolist() if d in date_pos}
            current_streak = 0
            cursor = total_days - 1
            while cursor in pos_set:
                current_streak += 1
                cursor -= 1
                if cursor < 0:
                    break

            appear_ratio = top8_days / max(total_days, 1)
            leader_ratio = top3_days / max(total_days, 1)
            streak_ratio = min(current_streak / 5.0, 1.0)
            rank_ratio = max(0.0, (13.0 - avg_rank) / 12.0)
            score_ratio = min(max(avg_score, 0.0) / 45.0, 1.0)
            persistence_score = float(
                np.clip(
                    100.0
                    * (
                        0.34 * appear_ratio
                        + 0.22 * leader_ratio
                        + 0.24 * streak_ratio
                        + 0.12 * rank_ratio
                        + 0.08 * score_ratio
                    ),
                    0.0,
                    100.0,
                )
            )
            rows.append(
                {
                    "mainline_name": str(mapped_name or ""),
                    "mainline_persistence_score": persistence_score,
                    "mainline_top3_days": top3_days,
                    "mainline_top8_days": top8_days,
                    "mainline_current_streak": current_streak,
                    "mainline_avg_rank_hist": avg_rank,
                }
            )

        if not rows:
            return empty_df
        return pd.DataFrame(rows)

    def _load_mainline_snapshot(self, trade_date: str) -> tuple[pd.DataFrame, dict[str, Any]]:
        empty_df = pd.DataFrame(
            columns=["ts_code", "mainline_name", "mainline_score", "mainline_rank", "sector_strength"]
        )
        try:
            stock_map = mainline_analyst._build_stock_mainline_map(trade_date, trade_date)
        except Exception:
            stock_map = pd.DataFrame(columns=["ts_code", "mapped_name"])

        try:
            date_df = fetch_df(
                """
                SELECT trade_date
                FROM mainline_scores
                WHERE trade_date <= ?
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 1
                """,
                [trade_date],
            )
        except Exception:
            date_df = pd.DataFrame()

        score_df = pd.DataFrame()
        score_date = trade_date
        if not date_df.empty:
            score_date = str(date_df.iloc[0]["trade_date"])[:10]
            stale_days = abs((pd.Timestamp(trade_date) - pd.Timestamp(score_date)).days)
            if stale_days > 7:
                score_df = pd.DataFrame()
            else:
                try:
                    score_df = fetch_df(
                        """
                        SELECT mapped_name, score, limit_ups, stock_count
                        FROM mainline_scores
                        WHERE trade_date = ?
                        ORDER BY score DESC, limit_ups DESC, stock_count DESC
                        """,
                        [score_date],
                    )
                except Exception:
                    score_df = pd.DataFrame()

        if score_df.empty:
            fallback_rows = mainline_analyst.analyze(days=1, limit=40, trade_date=trade_date)
            if fallback_rows:
                score_df = pd.DataFrame(
                    [
                        {
                            "mapped_name": row.get("name"),
                            "score": row.get("score", 0.0),
                            "limit_ups": row.get("limit_ups", 0),
                            "stock_count": row.get("stock_count", 0),
                        }
                        for row in fallback_rows
                        if row.get("name")
                    ]
                )

        if score_df.empty:
            if stock_map.empty:
                return empty_df, {"trade_date": trade_date, "top_mainlines": []}
            try:
                snapshot_df = fetch_df(
                    """
                    SELECT ts_code, pct_chg, amount
                    FROM daily_price
                    WHERE trade_date = ? AND vol > 0
                    """,
                    [trade_date],
                )
            except Exception:
                snapshot_df = pd.DataFrame()
            if snapshot_df.empty:
                return empty_df, {"trade_date": trade_date, "top_mainlines": []}

            merged_snapshot = snapshot_df.merge(stock_map, on="ts_code", how="inner")
            if merged_snapshot.empty:
                return empty_df, {"trade_date": trade_date, "top_mainlines": []}

            grouped = (
                merged_snapshot.groupby("mapped_name")
                .agg(
                    avg_ret=("pct_chg", "mean"),
                    total_amt=("amount", "sum"),
                    stock_count=("ts_code", "nunique"),
                    up_count=("pct_chg", lambda x: int((x > 0).sum())),
                    strong_count=("pct_chg", lambda x: int((x >= 5).sum())),
                    limit_ups=("pct_chg", lambda x: int((x >= 9.5).sum())),
                )
                .reset_index()
            )
            if grouped.empty:
                return empty_df, {"trade_date": trade_date, "top_mainlines": []}
            grouped["breadth"] = grouped["up_count"] / grouped["stock_count"].clip(lower=1)
            grouped["strong_ratio"] = grouped["strong_count"] / grouped["stock_count"].clip(lower=1)
            grouped["lu_ratio"] = grouped["limit_ups"] / grouped["stock_count"].clip(lower=1)
            grouped["leader_count"] = grouped["strong_count"]
            grouped["leader_ratio"] = grouped["leader_count"] / grouped["stock_count"].clip(lower=1)
            grouped["score"] = (
                grouped["avg_ret"] * 2.0
                + grouped["lu_ratio"] * 48.0
                + grouped["breadth"] * 22.0
                + grouped["strong_ratio"] * 12.0
                + np.log1p(grouped["total_amt"]) * 0.35
            )
            weak_mask = (grouped["breadth"] < 0.15) & (grouped["strong_ratio"] < 0.03)
            grouped.loc[weak_mask, "score"] = grouped.loc[weak_mask, "score"] * 0.45
            grouped = grouped.sort_values(["score", "limit_ups", "total_amt"], ascending=[False, False, False])
            score_df = grouped.rename(columns={"mapped_name": "mainline_name", "score": "mainline_score"})
            score_df["mapped_name"] = score_df["mainline_name"]
            score_date = trade_date

        score_df = score_df.copy()
        def _score_col(name: str, default: float = 0.0) -> pd.Series:
            if name in score_df.columns:
                return pd.to_numeric(score_df[name], errors="coerce").fillna(default)
            return pd.Series(default, index=score_df.index, dtype="float64")

        if "mainline_name" not in score_df.columns:
            score_df["mainline_name"] = score_df["mapped_name"].astype(str)
        if "mainline_score" in score_df.columns:
            score_df["mainline_score"] = _score_col("mainline_score")
        elif "score" in score_df.columns:
            score_df["mainline_score"] = _score_col("score")
        else:
            score_df["mainline_score"] = pd.Series(0.0, index=score_df.index, dtype="float64")
        score_df["limit_ups"] = _score_col("limit_ups")
        score_df["stock_count"] = _score_col("stock_count")
        score_df["breadth"] = _score_col("breadth")
        score_df["strong_ratio"] = _score_col("strong_ratio")
        score_df["leader_ratio"] = _score_col("leader_ratio")
        score_df["lu_ratio"] = (
            score_df["limit_ups"] / score_df["stock_count"].clip(lower=1.0)
        ).fillna(0.0)
        score_df["mainline_crowding_score"] = np.clip(
            score_df["mainline_score"] * 1.05
            + score_df["limit_ups"] * 2.60
            + score_df["lu_ratio"] * 42.0
            + score_df["leader_ratio"] * 18.0
            + score_df["strong_ratio"] * 12.0,
            0.0,
            100.0,
        )
        score_df["mainline_phase"] = np.select(
            [
                score_df["mainline_crowding_score"] >= 80.0,
                score_df["mainline_crowding_score"] >= 62.0,
                score_df["mainline_crowding_score"] >= 45.0,
            ],
            ["拥挤高潮", "主升扩散", "发酵升温"],
            default="轮动观察",
        )
        score_df["mainline_rank"] = range(1, len(score_df) + 1)
        persistence_df = self._load_mainline_persistence_snapshot(score_date, lookback_days=30)
        if not persistence_df.empty:
            score_df = score_df.merge(persistence_df, on="mainline_name", how="left")
        else:
            score_df["mainline_persistence_score"] = 0.0
            score_df["mainline_top3_days"] = 0.0
            score_df["mainline_top8_days"] = 0.0
            score_df["mainline_current_streak"] = 0.0
            score_df["mainline_avg_rank_hist"] = 99.0
        score_df["mainline_persistence_score"] = pd.to_numeric(
            score_df.get("mainline_persistence_score"),
            errors="coerce",
        ).fillna(0.0)
        score_df["mainline_top3_days"] = pd.to_numeric(
            score_df.get("mainline_top3_days"),
            errors="coerce",
        ).fillna(0.0)
        score_df["mainline_top8_days"] = pd.to_numeric(
            score_df.get("mainline_top8_days"),
            errors="coerce",
        ).fillna(0.0)
        score_df["mainline_current_streak"] = pd.to_numeric(
            score_df.get("mainline_current_streak"),
            errors="coerce",
        ).fillna(0.0)
        score_df["mainline_avg_rank_hist"] = pd.to_numeric(
            score_df.get("mainline_avg_rank_hist"),
            errors="coerce",
        ).fillna(99.0)
        top_mainlines = (
            score_df.head(8)[
                [
                    "mainline_name",
                    "mainline_score",
                    "mainline_rank",
                    "limit_ups",
                    "stock_count",
                    "breadth",
                    "leader_ratio",
                    "mainline_crowding_score",
                    "mainline_phase",
                    "mainline_persistence_score",
                    "mainline_top3_days",
                    "mainline_top8_days",
                    "mainline_current_streak",
                ]
            ]
            .to_dict("records")
        )

        if stock_map.empty:
            return empty_df, {"trade_date": score_date, "top_mainlines": top_mainlines}

        stock_map = stock_map.rename(columns={"mapped_name": "mainline_name"})
        merged = stock_map.merge(
            score_df[
                [
                    "mainline_name",
                    "mainline_score",
                    "mainline_rank",
                    "mainline_crowding_score",
                    "mainline_phase",
                    "limit_ups",
                    "stock_count",
                    "leader_ratio",
                    "mainline_persistence_score",
                    "mainline_top3_days",
                    "mainline_top8_days",
                    "mainline_current_streak",
                ]
            ],
            on="mainline_name",
            how="left",
        )
        merged["mainline_score"] = pd.to_numeric(
            merged.get("mainline_score"),
            errors="coerce",
        ).fillna(0.0)
        merged["mainline_rank"] = pd.to_numeric(
            merged.get("mainline_rank"),
            errors="coerce",
        ).fillna(999).astype(int)
        merged["mainline_crowding_score"] = pd.to_numeric(
            merged.get("mainline_crowding_score"),
            errors="coerce",
        ).fillna(0.0)
        merged["mainline_limit_ups"] = pd.to_numeric(
            merged.get("limit_ups"),
            errors="coerce",
        ).fillna(0.0)
        merged["mainline_stock_count"] = pd.to_numeric(
            merged.get("stock_count"),
            errors="coerce",
        ).fillna(0.0)
        merged["mainline_leader_ratio"] = pd.to_numeric(
            merged.get("leader_ratio"),
            errors="coerce",
        ).fillna(0.0)
        merged["mainline_phase"] = merged.get("mainline_phase", "轮动观察").fillna("轮动观察").astype(str)
        merged["mainline_persistence_score"] = pd.to_numeric(
            merged.get("mainline_persistence_score"),
            errors="coerce",
        ).fillna(0.0)
        merged["mainline_top3_days"] = pd.to_numeric(
            merged.get("mainline_top3_days"),
            errors="coerce",
        ).fillna(0.0)
        merged["mainline_top8_days"] = pd.to_numeric(
            merged.get("mainline_top8_days"),
            errors="coerce",
        ).fillna(0.0)
        merged["mainline_current_streak"] = pd.to_numeric(
            merged.get("mainline_current_streak"),
            errors="coerce",
        ).fillna(0.0)
        merged["sector_strength"] = merged["mainline_score"]
        return (
            merged[
                [
                    "ts_code",
                    "mainline_name",
                    "mainline_score",
                    "mainline_rank",
                    "mainline_crowding_score",
                    "mainline_limit_ups",
                    "mainline_stock_count",
                    "mainline_leader_ratio",
                    "mainline_phase",
                    "mainline_persistence_score",
                    "mainline_top3_days",
                    "mainline_top8_days",
                    "mainline_current_streak",
                    "sector_strength",
                ]
            ],
            {"trade_date": score_date, "top_mainlines": top_mainlines},
        )

    def load_universe_snapshot(self, trade_date: str) -> pd.DataFrame:
        # ma50 / ATR / 30日涨停统计需要一定历史窗口，100 个交易日足够覆盖。
        lookback_df = fetch_df(
            """
            SELECT MIN(trade_date) AS start_date
            FROM (
                SELECT DISTINCT trade_date
                FROM daily_price
                WHERE trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT 100
            )
            """,
            [trade_date],
        )
        if lookback_df.empty or pd.isna(lookback_df.iloc[0]["start_date"]):
            return pd.DataFrame()

        start_date = lookback_df.iloc[0]["start_date"]

        query = """
        WITH raw_hist AS (
            SELECT
                d.ts_code,
                d.trade_date,
                d.open,
                d.close,
                d.high,
                d.low,
                d.vol,
                d.amount,
                d.pct_chg,
                LAG(d.close, 1) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS prev_close,
                AVG(d.close) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                AVG(d.close) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
                MAX(d.high) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 49 PRECEDING AND 1 PRECEDING) AS high_50_prev,
                MAX(d.high) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING) AS high_20_prev,
                MIN(d.low) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 19 PRECEDING AND 1 PRECEDING) AS low_20_prev,
                MIN(d.low) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING) AS low_10_prev,
                STDDEV(d.pct_chg) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS vol_10,
                LAG(d.close, 5) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS close_5,
                LAG(d.close, 10) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS close_10,
                LAG(d.close, 20) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS close_20,
                AVG(d.amount) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_amount_20,
                SUM(CASE WHEN d.pct_chg >= 9.5 THEN 1 ELSE 0 END) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS limit_up_5d,
                SUM(CASE WHEN d.pct_chg >= 9.5 THEN 1 ELSE 0 END) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS limit_up_10d,
                SUM(CASE WHEN d.pct_chg >= 9.5 THEN 1 ELSE 0 END) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS limit_up_30d,
                SUM(CASE WHEN d.pct_chg >= 7.0 THEN 1 ELSE 0 END) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS leader_up_5d,
                SUM(CASE WHEN d.pct_chg >= 5.0 THEN 1 ELSE 0 END) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS strong_up_20d
            FROM daily_price d
            WHERE d.trade_date <= ? AND d.trade_date >= ?
        ),
        hist AS (
            SELECT
                *,
                SUM(
                    CASE
                        WHEN high >= COALESCE(prev_close, close) * 1.095 AND pct_chg < 9.5 THEN 1
                        ELSE 0
                    END
                ) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS board_break_20d,
                AVG(
                    GREATEST(
                        high - low,
                        ABS(high - COALESCE(prev_close, close)),
                        ABS(low - COALESCE(prev_close, close))
                    )
                ) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS atr14
            FROM raw_hist
        ),
        mf_hist AS (
            SELECT
                m.ts_code,
                m.trade_date,
                m.net_mf_vol,
                AVG(m.net_mf_vol) OVER (PARTITION BY m.ts_code ORDER BY m.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mf_5
            FROM stock_moneyflow m
            WHERE m.trade_date <= ? AND m.trade_date >= ?
        )
        SELECT
            h.ts_code,
            COALESCE(b.name, h.ts_code) AS name,
            h.open,
            h.close,
            h.high,
            h.low,
            h.pct_chg,
            h.amount,
            CASE WHEN h.close_5 IS NULL OR h.close_5 = 0 THEN NULL ELSE (h.close / h.close_5 - 1.0) * 100 END AS ret_5,
            CASE WHEN h.close_10 IS NULL OR h.close_10 = 0 THEN NULL ELSE (h.close / h.close_10 - 1.0) * 100 END AS ret_10,
            CASE WHEN h.close_20 IS NULL OR h.close_20 = 0 THEN NULL ELSE (h.close / h.close_20 - 1.0) * 100 END AS ret_20,
            COALESCE(h.vol_10, 0) AS vol_10,
            COALESCE(mf.net_mf_vol, 0) AS mf_today,
            COALESCE(mf.mf_5, 0) AS mf_5,
            CASE WHEN h.avg_amount_20 IS NULL OR h.avg_amount_20 = 0 THEN 1.0 ELSE h.amount / h.avg_amount_20 END AS amt_ratio_20,
            h.ma20,
            h.ma50,
            h.high_50_prev,
            h.high_20_prev,
            h.low_20_prev,
            h.low_10_prev,
            COALESCE(h.atr14, 0) AS atr14,
            CASE WHEN h.close IS NULL OR h.close = 0 OR h.atr14 IS NULL THEN NULL ELSE h.atr14 / h.close * 100 END AS atr_pct_14,
            COALESCE(h.limit_up_5d, 0) AS limit_up_5d,
            COALESCE(h.limit_up_10d, 0) AS limit_up_10d,
            COALESCE(h.limit_up_30d, 0) AS limit_up_30d,
            COALESCE(h.leader_up_5d, 0) AS leader_up_5d,
            COALESCE(h.board_break_20d, 0) AS board_break_20d,
            COALESCE(h.strong_up_20d, 0) AS strong_up_20d
        FROM hist h
        LEFT JOIN stock_basic b ON b.ts_code = h.ts_code
        LEFT JOIN mf_hist mf ON mf.ts_code = h.ts_code AND mf.trade_date = h.trade_date
        WHERE h.trade_date = ?
          AND h.vol > 0
        """
        df = fetch_df(query, [trade_date, start_date, trade_date, start_date, trade_date])
        if df.empty:
            return df

        bench = self._load_benchmark_state(trade_date)
        market_state = self._load_market_sentiment_state(trade_date, bench)
        mainline_df, mainline_context = self._load_mainline_snapshot(trade_date)

        if not mainline_df.empty:
            df = df.merge(mainline_df, on="ts_code", how="left")
        else:
            df["mainline_name"] = ""
            df["mainline_score"] = 0.0
            df["mainline_rank"] = 999
            df["mainline_crowding_score"] = 0.0
            df["mainline_limit_ups"] = 0.0
            df["mainline_stock_count"] = 0.0
            df["mainline_leader_ratio"] = 0.0
            df["mainline_phase"] = "轮动观察"
            df["mainline_persistence_score"] = 0.0
            df["mainline_top3_days"] = 0.0
            df["mainline_top8_days"] = 0.0
            df["mainline_current_streak"] = 0.0
            df["sector_strength"] = 0.0

        for k, v in bench.items():
            df[k] = v
        for k, v in market_state.items():
            df[k] = v

        df["mainline_name"] = df.get("mainline_name", "").fillna("").astype(str)
        df["mainline_score"] = pd.to_numeric(df.get("mainline_score"), errors="coerce").fillna(0.0)
        df["mainline_rank"] = pd.to_numeric(df.get("mainline_rank"), errors="coerce").fillna(999).astype(int)
        df["mainline_crowding_score"] = pd.to_numeric(
            df.get("mainline_crowding_score"),
            errors="coerce",
        ).fillna(0.0)
        df["mainline_limit_ups"] = pd.to_numeric(
            df.get("mainline_limit_ups"),
            errors="coerce",
        ).fillna(0.0)
        df["mainline_stock_count"] = pd.to_numeric(
            df.get("mainline_stock_count"),
            errors="coerce",
        ).fillna(0.0)
        df["mainline_leader_ratio"] = pd.to_numeric(
            df.get("mainline_leader_ratio"),
            errors="coerce",
        ).fillna(0.0)
        df["mainline_phase"] = df.get("mainline_phase", "轮动观察").fillna("轮动观察").astype(str)
        df["mainline_persistence_score"] = pd.to_numeric(
            df.get("mainline_persistence_score"),
            errors="coerce",
        ).fillna(0.0)
        df["mainline_top3_days"] = pd.to_numeric(
            df.get("mainline_top3_days"),
            errors="coerce",
        ).fillna(0.0)
        df["mainline_top8_days"] = pd.to_numeric(
            df.get("mainline_top8_days"),
            errors="coerce",
        ).fillna(0.0)
        df["mainline_current_streak"] = pd.to_numeric(
            df.get("mainline_current_streak"),
            errors="coerce",
        ).fillna(0.0)
        df["sector_strength"] = pd.to_numeric(df.get("sector_strength"), errors="coerce").fillna(0.0)

        df.attrs["market_context"] = market_state
        df.attrs["mainline_context"] = mainline_context
        return df

    def forward_returns(self, trade_date: str, ts_codes: list[str], horizon: int) -> pd.DataFrame:
        if not ts_codes:
            return pd.DataFrame(columns=["ts_code", "fwd_ret"])
        h = max(1, int(horizon))
        placeholders = ",".join(["?"] * len(ts_codes))
        query = f"""
        WITH base AS (
            SELECT ts_code, close AS base_close
            FROM daily_price
            WHERE trade_date = ?
              AND ts_code IN ({placeholders})
        ),
        target_date AS (
            SELECT trade_date
            FROM daily_price
            WHERE trade_date > ?
            GROUP BY trade_date
            ORDER BY trade_date ASC
            LIMIT 1 OFFSET {h - 1}
        ),
        target AS (
            SELECT ts_code, close AS target_close
            FROM daily_price
            WHERE trade_date = (SELECT trade_date FROM target_date)
              AND ts_code IN ({placeholders})
        )
        SELECT b.ts_code,
               CASE WHEN b.base_close IS NULL OR b.base_close = 0 OR t.target_close IS NULL THEN NULL
                    ELSE (t.target_close / b.base_close - 1.0)
               END AS fwd_ret
        FROM base b
        LEFT JOIN target t ON t.ts_code = b.ts_code
        """
        params = [trade_date] + ts_codes + [trade_date] + ts_codes
        return fetch_df(query, params)

    def event_exit_returns(
        self,
        trade_date: str,
        ts_codes: list[str],
        stop_loss_map: dict[str, float],
        max_horizon: int = 20,
    ) -> pd.DataFrame:
        if not ts_codes:
            return pd.DataFrame(columns=["ts_code", "fwd_ret", "hold_days", "exit_reason"])

        h = max(1, int(max_horizon))
        placeholders = ",".join(["?"] * len(ts_codes))
        query = f"""
        WITH td AS (
            SELECT trade_date, ROW_NUMBER() OVER (ORDER BY trade_date) AS rn
            FROM (SELECT DISTINCT trade_date FROM daily_price)
        ),
        base_idx AS (
            SELECT rn AS base_rn FROM td WHERE trade_date = ?
        ),
        base AS (
            SELECT ts_code, close AS base_close
            FROM daily_price
            WHERE trade_date = ?
              AND ts_code IN ({placeholders})
        ),
        fut AS (
            SELECT
                d.ts_code,
                d.trade_date,
                d.close,
                ROW_NUMBER() OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS step_no
            FROM daily_price d
            JOIN td ON td.trade_date = d.trade_date
            JOIN base_idx bi ON td.rn > bi.base_rn AND td.rn <= bi.base_rn + {h}
            WHERE d.ts_code IN ({placeholders})
        )
        SELECT f.ts_code, f.trade_date, f.close, f.step_no, b.base_close
        FROM fut f
        JOIN base b ON b.ts_code = f.ts_code
        ORDER BY f.ts_code, f.step_no
        """
        params = [trade_date, trade_date] + ts_codes + ts_codes
        path_df = fetch_df(query, params)
        if path_df.empty:
            return pd.DataFrame(columns=["ts_code", "fwd_ret", "hold_days", "exit_reason"])

        rows: list[dict] = []
        grouped = path_df.groupby("ts_code", sort=False)
        for code, g in grouped:
            c = str(code)
            g = g.sort_values("step_no")
            base_close = float(g.iloc[0]["base_close"]) if not g.empty else 0.0
            if base_close <= 0:
                rows.append({"ts_code": c, "fwd_ret": None, "hold_days": None, "exit_reason": "no_base"})
                continue

            stop_price = float(stop_loss_map.get(c, 0.0) or 0.0)
            exit_close = None
            hold_days = None
            exit_reason = "timeout"

            for _, r in g.iterrows():
                close = float(r["close"])
                step_no = int(r["step_no"])
                if stop_price > 0 and close < stop_price:
                    exit_close = close
                    hold_days = step_no
                    exit_reason = "stop_loss"
                    break

            if exit_close is None:
                last = g.iloc[-1]
                exit_close = float(last["close"])
                hold_days = int(last["step_no"])

            fwd_ret = (exit_close / base_close - 1.0) if base_close > 0 else None
            rows.append(
                {
                    "ts_code": c,
                    "fwd_ret": fwd_ret,
                    "hold_days": hold_days,
                    "exit_reason": exit_reason,
                }
            )

        return pd.DataFrame(rows)

    def load_history_panel(
        self,
        trade_date: str,
        ts_codes: list[str],
        lookback_days: int = 60,
    ) -> pd.DataFrame:
        """
        批量加载一组股票的历史面板数据。
        
        Args:
            trade_date: 基准交易日 (包含此日期)
            ts_codes: 股票代码列表
            lookback_days: 回溯天数
            
        Returns:
            DataFrame: 包含 trade_date, ts_code, open, high, low, close, vol, amount
        """
        if not ts_codes:
            return pd.DataFrame(columns=["trade_date", "ts_code", "open", "high", "low", "close", "vol", "amount"])

        # 优化：计算一个保守的起始日期，利用索引减少扫描量
        lookback_days_int = int(lookback_days)
        start_date_df = fetch_df(
            """
            SELECT MIN(trade_date) as start_date
            FROM (
                SELECT DISTINCT trade_date
                FROM daily_price
                WHERE trade_date <= ?
                ORDER BY trade_date DESC
                LIMIT ?
            )
            """,
            [trade_date, lookback_days_int + 10]  # 多留10天缓冲
        )
        if start_date_df.empty or pd.isna(start_date_df.iloc[0]["start_date"]):
            start_date = '1990-01-01'
        else:
            start_date = start_date_df.iloc[0]["start_date"]

        # 为每个 ts_code 提取最近 N 条记录，由于 DuckDB SQL 限制，我们使用 ROW_NUMBER 分组取前 N 条
        placeholders = ",".join(["?"] * len(ts_codes))
        query = f"""
        WITH ranked AS (
            SELECT
                trade_date,
                ts_code,
                open,
                high,
                low,
                close,
                vol,
                amount,
                ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as rn
            FROM daily_price
            WHERE trade_date <= ? AND trade_date >= ?
              AND ts_code IN ({placeholders})
        )
        SELECT trade_date, ts_code, open, high, low, close, vol, amount
        FROM ranked
        WHERE rn <= ?
        ORDER BY ts_code, trade_date ASC
        """
        params = [trade_date, start_date] + ts_codes + [lookback_days_int]
        return fetch_df(query, params)

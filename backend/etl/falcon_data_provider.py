from __future__ import annotations

import os

import pandas as pd

from db.connection import fetch_df


class DuckDbFalconDataProvider:
    def latest_trade_date(self) -> str:
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

    def _load_benchmark_state(self, trade_date: str) -> dict:
        env_code = os.getenv("FALCON_BENCHMARK_CODE", "SPY").strip()
        candidates: list[str] = []
        for c in [env_code, "SPY", "^GSPC", "000001.SH", "399300.SZ"]:
            if c and c not in candidates:
                candidates.append(c)

        def _query_state(table_name: str, code: str) -> dict | None:
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
            # Market filter: trend regime first, avoid overfitting to short lookback return sign.
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

    def load_universe_snapshot(self, trade_date: str) -> pd.DataFrame:
        query = """
        WITH hist AS (
            SELECT
                d.ts_code,
                d.trade_date,
                d.close,
                d.high,
                d.low,
                d.vol,
                d.amount,
                d.pct_chg,
                AVG(d.close) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,
                AVG(d.close) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 49 PRECEDING AND CURRENT ROW) AS ma50,
                MAX(d.high) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 49 PRECEDING AND 1 PRECEDING) AS high_50_prev,
                MIN(d.low) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 9 PRECEDING AND 1 PRECEDING) AS low_10_prev,
                STDDEV(d.pct_chg) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS vol_10,
                LAG(d.close, 5) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS close_5,
                LAG(d.close, 10) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS close_10,
                LAG(d.close, 20) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date) AS close_20,
                AVG(d.amount) OVER (PARTITION BY d.ts_code ORDER BY d.trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS avg_amount_20
            FROM daily_price d
            WHERE d.trade_date <= ?
        ),
        mf_hist AS (
            SELECT
                m.ts_code,
                m.trade_date,
                m.net_mf_vol,
                AVG(m.net_mf_vol) OVER (PARTITION BY m.ts_code ORDER BY m.trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS mf_5
            FROM stock_moneyflow m
            WHERE m.trade_date <= ?
        )
        SELECT
            h.ts_code,
            COALESCE(b.name, h.ts_code) AS name,
            h.close,
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
            h.low_10_prev
        FROM hist h
        LEFT JOIN stock_basic b ON b.ts_code = h.ts_code
        LEFT JOIN mf_hist mf ON mf.ts_code = h.ts_code AND mf.trade_date = h.trade_date
        WHERE h.trade_date = ?
          AND h.vol > 0
        """
        df = fetch_df(query, [trade_date, trade_date, trade_date])
        if df.empty:
            return df

        bench = self._load_benchmark_state(trade_date)
        for k, v in bench.items():
            df[k] = v
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

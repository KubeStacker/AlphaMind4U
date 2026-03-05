from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .base import FalconPlugin


class FalconMomentumPlugin(FalconPlugin):
    strategy_id = "falcon_momentum"
    display_name = "Falcon 动量引擎"
    version = "2.1.0"

    def default_params(self) -> dict[str, Any]:
        return {
            "top_n": 10,
            "min_amount": 2.0e6,
            "require_benchmark": True,
            "require_stock_env": True,
            "max_daily_jump": 9.7,
            "max_vol_10": 7.0,
            "min_ret_20": -3.0,
            "min_breakout_buffer_pct": 0.0,
            "min_stop_buffer_pct": 0.3,
            "w_breakout": 0.38,
            "w_trend": 0.34,
            "w_flow": 0.16,
            "w_liq": 0.16,
            "w_risk": 0.12,
            "min_score_to_pick": 35.0,
            "min_confidence_to_pick": 55.0,
            "score_drop_limit": 22.0,
            "winsor_clip": 3.0,
        }

    @staticmethod
    def _safe_num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
        return pd.to_numeric(df.get(col), errors="coerce").fillna(default)

    @staticmethod
    def _robust_zscore(s: pd.Series, clip: float = 3.0) -> pd.Series:
        x = pd.to_numeric(s, errors="coerce").replace([np.inf, -np.inf], np.nan)
        med = x.median()
        mad = (x - med).abs().median()
        if not np.isfinite(mad) or mad <= 1e-12:
            std = x.std(ddof=0)
            if not np.isfinite(std) or std <= 1e-12:
                return pd.Series(0.0, index=x.index)
            z = (x - x.mean()) / std
        else:
            z = (x - med) / (1.4826 * mad)
        return z.clip(-abs(float(clip)), abs(float(clip))).fillna(0.0)

    def run(self, universe_df: pd.DataFrame, as_of_date: str, params: dict[str, Any]) -> pd.DataFrame:
        if universe_df is None or universe_df.empty:
            return pd.DataFrame(columns=[
                "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
            ])

        p = {**self.default_params(), **(params or {})}
        d = self.default_params()
        # Backward compatibility: normalize legacy stored params from old momentum versions.
        if float(p.get("w_breakout", d["w_breakout"])) > 1.0:
            p["w_breakout"] = d["w_breakout"]
        if float(p.get("w_trend", d["w_trend"])) > 1.0:
            p["w_trend"] = d["w_trend"]
        if float(p.get("w_liq", d["w_liq"])) > 1.0:
            p["w_liq"] = d["w_liq"]
        if float(p.get("w_risk", d["w_risk"])) > 1.0:
            p["w_risk"] = d["w_risk"]
        if float(p.get("w_flow", d["w_flow"])) > 1.0:
            p["w_flow"] = d["w_flow"]
        p["top_n"] = max(1, min(int(p.get("top_n", d["top_n"])), 15))
        # Avoid legacy high liquidity threshold choking candidate universe.
        p["min_amount"] = min(float(p.get("min_amount", d["min_amount"])), float(d["min_amount"]))
        df = universe_df.copy()

        # Core signals
        df["close"] = self._safe_num(df, "close")
        df["ret_5"] = self._safe_num(df, "ret_5")
        df["ret_10"] = self._safe_num(df, "ret_10")
        df["ret_20"] = self._safe_num(df, "ret_20")
        df["vol_10"] = self._safe_num(df, "vol_10")
        df["amount"] = self._safe_num(df, "amount")
        df["pct_chg"] = self._safe_num(df, "pct_chg")
        df["mf_today"] = self._safe_num(df, "mf_today")
        df["mf_5"] = self._safe_num(df, "mf_5")
        df["ma20"] = self._safe_num(df, "ma20")
        df["ma50"] = self._safe_num(df, "ma50")
        df["high_50_prev"] = self._safe_num(df, "high_50_prev")
        df["low_10_prev"] = self._safe_num(df, "low_10_prev")
        df["benchmark_ok"] = df.get("benchmark_ok", True).astype(bool)
        df["benchmark_code"] = df.get("benchmark_code", "SPY")

        # 1) 市场环境过滤（SPY 或代理指数）
        if bool(p.get("require_benchmark", True)):
            if not bool(df["benchmark_ok"].iloc[0]):
                return pd.DataFrame(columns=[
                    "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
                ])

        # 2) 个股环境过滤
        min_amount = float(p["min_amount"])
        max_daily_jump = float(p["max_daily_jump"])
        max_vol_10 = float(p["max_vol_10"])
        min_ret_20 = float(p["min_ret_20"])
        min_breakout_buffer = float(p["min_breakout_buffer_pct"])
        min_stop_buffer = float(p["min_stop_buffer_pct"])

        df = df[df["amount"] >= min_amount].copy()
        if df.empty:
            return pd.DataFrame(columns=[
                "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
            ])

        stock_env_ok = (
            (df["close"] > df["ma20"])
            & (df["ma20"] > df["ma50"])
            & (df["ret_20"] >= min_ret_20)
            & (df["vol_10"] <= max_vol_10)
            & (df["pct_chg"] <= max_daily_jump)
            & (df["high_50_prev"] > 0)
            & (df["low_10_prev"] > 0)
        )

        breakout_px = df["high_50_prev"] * (1.0 + min_breakout_buffer / 100.0)
        breakout_ok = df["close"] >= breakout_px

        stop_buffer_pct = (df["close"] / df["low_10_prev"] - 1.0) * 100.0
        stop_ok = stop_buffer_pct >= min_stop_buffer

        if bool(p.get("require_stock_env", True)):
            df = df[stock_env_ok & breakout_ok & stop_ok].copy()
        else:
            df = df[breakout_ok & stop_ok].copy()

        if df.empty:
            return pd.DataFrame(columns=[
                "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
            ])

        # 3) 评分（买强卖弱）
        breakout_strength = (df["close"] / df["high_50_prev"] - 1.0) * 100.0
        ma_slope = (df["ma20"] / df["ma50"] - 1.0) * 100.0
        extension_pct = (df["close"] / df["ma20"] - 1.0) * 100.0

        # qlib-style factor engineering + robust cross-sectional normalization.
        fac_breakout = breakout_strength
        fac_trend = 0.50 * df["ret_20"] + 0.30 * df["ret_10"] + 0.20 * ma_slope
        fac_flow = 0.65 * np.sign(df["mf_5"]) * np.log1p(df["mf_5"].abs()) + 0.35 * np.sign(df["mf_today"]) * np.log1p(df["mf_today"].abs())
        fac_liq = np.log1p(df["amount"])
        fac_risk = (
            -0.50 * df["vol_10"]
            + 0.30 * stop_buffer_pct
            - 0.20 * np.abs(df["pct_chg"])
            - 0.35 * np.maximum(extension_pct - 15.0, 0.0)
            - 0.25 * np.maximum(breakout_strength - 12.0, 0.0)
        )

        clip = float(p.get("winsor_clip", d["winsor_clip"]))
        z_breakout = self._robust_zscore(fac_breakout, clip=clip)
        z_trend = self._robust_zscore(fac_trend, clip=clip)
        z_flow = self._robust_zscore(fac_flow, clip=clip)
        z_liq = self._robust_zscore(fac_liq, clip=clip)
        z_risk = self._robust_zscore(fac_risk, clip=clip)

        w_b = max(0.01, float(p["w_breakout"]))
        w_t = max(0.01, float(p["w_trend"]))
        w_f = max(0.01, float(p["w_flow"]))
        w_l = max(0.01, float(p["w_liq"]))
        w_r = max(0.01, float(p["w_risk"]))
        ws = w_b + w_t + w_f + w_l + w_r
        w_b, w_t, w_f, w_l, w_r = w_b / ws, w_t / ws, w_f / ws, w_l / ws, w_r / ws

        composite_z = w_b * z_breakout + w_t * z_trend + w_f * z_flow + w_l * z_liq + w_r * z_risk

        # Additional crowding penalty keeps buy-strong but avoids euphoric extremes.
        crowding = 0.40 * np.maximum(breakout_strength - 9.0, 0.0) + 0.30 * np.maximum(extension_pct - 12.0, 0.0)
        score_raw = 100.0 * composite_z.rank(pct=True).fillna(0.0) - crowding
        df["strategy_score"] = score_raw.clip(1, 99)

        conf_raw = (
            52
            + 11 * z_trend
            + 9 * z_risk
            + 6 * z_breakout
            + 3 * z_flow
            - 0.30 * np.maximum(np.abs(df["pct_chg"]) - 7.0, 0.0)
        )
        df["confidence"] = conf_raw.clip(1, 99)

        # 4) 质量筛选 + 结果数量控制
        min_score = max(20.0, float(p["min_score_to_pick"]))
        min_conf = max(40.0, float(p["min_confidence_to_pick"]))
        score_drop_limit = max(6.0, float(p["score_drop_limit"]))

        df = df[(df["strategy_score"] >= min_score) & (df["confidence"] >= min_conf)].copy()
        if df.empty:
            return pd.DataFrame(columns=[
                "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
            ])

        out = df.sort_values(["strategy_score", "amount"], ascending=[False, False]).copy()
        top_score = float(out.iloc[0]["strategy_score"])
        quality_cut = max(min_score, top_score - score_drop_limit)
        top_n = max(1, int(p["top_n"]))
        out = out[out["strategy_score"] >= quality_cut].head(top_n)

        def to_label(s: float) -> str:
            if s >= 78:
                return "强信号"
            if s >= 62:
                return "偏强"
            if s >= 48:
                return "观察"
            return "谨慎"

        out["signal_label"] = out["strategy_score"].apply(lambda x: to_label(float(x)))

        out["score_breakdown"] = out.apply(
            lambda r: {
                "entry_logic": "benchmark_ok + stock_env_ok + breakout_50d",
                "exit_logic": "close < low_10_prev => stop_loss",
                "benchmark_code": str(r.get("benchmark_code") or "SPY"),
                "benchmark_ok": bool(r.get("benchmark_ok")),
                "ret_5": round(float(r["ret_5"]), 4),
                "ret_10": round(float(r["ret_10"]), 4),
                "ret_20": round(float(r["ret_20"]), 4),
                "pct_chg": round(float(r["pct_chg"]), 4),
                "vol_10": round(float(r["vol_10"]), 4),
                "ma20": round(float(r["ma20"]), 4),
                "ma50": round(float(r["ma50"]), 4),
                "high_50_prev": round(float(r["high_50_prev"]), 4),
                "low_10_prev": round(float(r["low_10_prev"]), 4),
                "stop_loss_price": round(float(r["low_10_prev"]), 4),
                "stop_buffer_pct": round(float((r["close"] / r["low_10_prev"] - 1.0) * 100.0), 4),
                "breakout_strength_pct": round(float((r["close"] / r["high_50_prev"] - 1.0) * 100.0), 4),
                "amount": float(r["amount"]),
                "mf_today": float(r.get("mf_today", 0.0)),
                "mf_5": float(r.get("mf_5", 0.0)),
                "extension_pct": round(float((r["close"] / r["ma20"] - 1.0) * 100.0), 4),
                "formula": {
                    "w_breakout": round(w_b, 4),
                    "w_trend": round(w_t, 4),
                    "w_flow": round(w_f, 4),
                    "w_liq": round(w_l, 4),
                    "w_risk": round(w_r, 4),
                },
            },
            axis=1,
        )

        return out[["ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"]]

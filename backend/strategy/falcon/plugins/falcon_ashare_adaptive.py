from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .base import FalconPlugin


class FalconAshareAdaptivePlugin(FalconPlugin):
    strategy_id = "falcon_ashare_adaptive"
    display_name = "Falcon A股自适应引擎"
    version = "1.1.0"

    @staticmethod
    def _empty_result() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "ts_code",
                "name",
                "strategy_score",
                "confidence",
                "signal_label",
                "score_breakdown",
            ]
        )

    def default_params(self) -> dict[str, Any]:
        return {
            "top_n": 8,
            "min_amount": 2.0e6,
            "require_benchmark": True,
            "require_market_signal": True,
            "min_market_target_position": 0.25,
            "require_mainline": True,
            "min_mainline_score": 34.0,
            "min_mainline_persistence": 28.0,
            "max_mainline_rank": 12,
            "persistence_rank_slack": 4,
            "max_picks_per_mainline": 2,
            "max_position_count_full": 8,
            "max_single_position": 0.18,
            "max_daily_jump": 7.8,
            "min_ret_20": -6.0,
            "min_amt_ratio_20": 0.85,
            "min_close_to_ma20_ratio": 0.98,
            "max_extension_pct": 11.0,
            "max_breakout_pct": 6.0,
            "max_ret_20": 35.0,
            "max_limit_up_5d": 1,
            "max_limit_up_30d": 3,
            "max_board_break_20d": 2,
            "max_vol_10": 6.5,
            "max_atr_pct_14": 4.2,
            "preferred_stop_buffer_pct": 18.0,
            "crowding_soft_cap": 70.0,
            "crowding_hard_cap": 82.0,
            "hot_theme_extension_cap": 8.5,
            "low_persistence_penalty": 6.0,
            "w_setup": 0.28,
            "w_mainline": 0.22,
            "w_flow": 0.14,
            "w_market": 0.14,
            "w_quality": 0.10,
            "w_risk": 0.12,
            "min_score_to_pick": 54.0,
            "min_confidence_to_pick": 54.0,
            "observe_top_n": 10,
            "observe_score_floor": 46.0,
            "observe_confidence_floor": 46.0,
        }

    @staticmethod
    def _safe_num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
        result = df.get(col)
        if result is None:
            return pd.Series(default, index=df.index, dtype="float64")
        return pd.to_numeric(result, errors="coerce").fillna(default)

    @staticmethod
    def _clip(series: pd.Series | np.ndarray | float, lower: float = 0.0, upper: float = 100.0):
        return np.clip(series, lower, upper)

    @staticmethod
    def _triangle_score(series: pd.Series, center: float, width: float) -> pd.Series:
        safe_width = max(float(width), 1e-6)
        return (100.0 * (1.0 - ((series - center).abs() / safe_width))).clip(lower=0.0, upper=100.0)

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            v = float(value)
            if np.isnan(v) or np.isinf(v):
                return float(default)
            return v
        except Exception:
            return float(default)

    @staticmethod
    def _build_observation_reason(row: pd.Series, params: dict[str, Any]) -> str:
        reasons: list[str] = []
        if float(row.get("pct_chg", 0.0) or 0.0) > float(params["max_daily_jump"]):
            reasons.append("当日涨幅偏大")
        if float(row.get("board_break_20d", 0.0) or 0.0) > float(params["max_board_break_20d"]):
            reasons.append("近期炸板偏多")
        if float(row.get("mainline_crowding_score", 0.0) or 0.0) >= float(params["crowding_hard_cap"]):
            reasons.append("题材拥挤")
        elif float(row.get("mainline_crowding_score", 0.0) or 0.0) >= float(params["crowding_soft_cap"]):
            reasons.append("题材偏热")
        if float(row.get("mainline_persistence_score", 0.0) or 0.0) < float(params["min_mainline_persistence"]):
            reasons.append("主线持续性不足")
        if float(row.get("extension_pct", 0.0) or 0.0) > float(params["hot_theme_extension_cap"]):
            reasons.append("偏离均线较远")
        if float(row.get("limit_up_5d", 0.0) or 0.0) > float(params["max_limit_up_5d"]):
            reasons.append("近期涨停过多")
        if float(row.get("strategy_score", 0.0) or 0.0) < float(params["min_score_to_pick"]):
            reasons.append("交易分不足")
        if float(row.get("confidence", 0.0) or 0.0) < float(params["min_confidence_to_pick"]):
            reasons.append("确定性不足")
        if not reasons:
            reasons.append("等待更优入场点")
        return "，".join(dict.fromkeys(reasons))

    def run(self, universe_df: pd.DataFrame, as_of_date: str, params: dict[str, Any]) -> pd.DataFrame:
        if universe_df is None or universe_df.empty:
            return self._empty_result()

        p = {**self.default_params(), **(params or {})}
        df = universe_df.copy()

        df["close"] = self._safe_num(df, "close")
        df["amount"] = self._safe_num(df, "amount")
        df["pct_chg"] = self._safe_num(df, "pct_chg")
        df["ret_5"] = self._safe_num(df, "ret_5")
        df["ret_10"] = self._safe_num(df, "ret_10")
        df["ret_20"] = self._safe_num(df, "ret_20")
        df["ma20"] = self._safe_num(df, "ma20")
        df["ma50"] = self._safe_num(df, "ma50")
        df["high_20_prev"] = self._safe_num(df, "high_20_prev")
        df["high_50_prev"] = self._safe_num(df, "high_50_prev")
        df["low_10_prev"] = self._safe_num(df, "low_10_prev")
        df["amt_ratio_20"] = self._safe_num(df, "amt_ratio_20", 1.0)
        df["mf_today"] = self._safe_num(df, "mf_today")
        df["mf_5"] = self._safe_num(df, "mf_5")
        df["vol_10"] = self._safe_num(df, "vol_10")
        df["atr_pct_14"] = self._safe_num(df, "atr_pct_14")
        df["limit_up_5d"] = self._safe_num(df, "limit_up_5d")
        df["limit_up_10d"] = self._safe_num(df, "limit_up_10d")
        df["limit_up_30d"] = self._safe_num(df, "limit_up_30d")
        df["leader_up_5d"] = self._safe_num(df, "leader_up_5d")
        df["board_break_20d"] = self._safe_num(df, "board_break_20d")
        df["strong_up_20d"] = self._safe_num(df, "strong_up_20d")
        df["mainline_score"] = self._safe_num(df, "mainline_score")
        df["mainline_rank"] = self._safe_num(df, "mainline_rank", 999.0)
        df["mainline_crowding_score"] = self._safe_num(df, "mainline_crowding_score")
        df["mainline_limit_ups"] = self._safe_num(df, "mainline_limit_ups")
        df["mainline_leader_ratio"] = self._safe_num(df, "mainline_leader_ratio")
        df["mainline_persistence_score"] = self._safe_num(df, "mainline_persistence_score")
        df["mainline_top3_days"] = self._safe_num(df, "mainline_top3_days")
        df["mainline_top8_days"] = self._safe_num(df, "mainline_top8_days")
        df["mainline_current_streak"] = self._safe_num(df, "mainline_current_streak")
        df["market_sentiment_score"] = self._safe_num(df, "market_sentiment_score", 55.0)
        df["market_target_position"] = self._safe_num(df, "market_target_position", 0.5)
        df["market_confidence"] = self._safe_num(df, "market_confidence", 50.0)
        df["market_risk_factor"] = self._safe_num(df, "market_risk_factor", 50.0)

        if "mainline_name" not in df.columns:
            df["mainline_name"] = ""
        df["mainline_name"] = df["mainline_name"].fillna("").astype(str)
        df["mainline_phase"] = df.get("mainline_phase", "轮动观察").fillna("轮动观察").astype(str)
        df["benchmark_ok"] = df.get("benchmark_ok", True).astype(bool)
        df["market_action"] = df.get("market_action", "WATCH").fillna("WATCH").astype(str)
        df["market_signal"] = df.get("market_signal", "").fillna("").astype(str)
        df["market_mood"] = df.get("market_mood", "CONFUSED").fillna("CONFUSED").astype(str)
        df["market_label"] = df.get("market_label", "观望").fillna("观望").astype(str)

        market_context = (
            universe_df.attrs.get("market_context", {})
            if isinstance(universe_df.attrs.get("market_context"), dict)
            else {}
        )
        mainline_context = (
            universe_df.attrs.get("mainline_context", {})
            if isinstance(universe_df.attrs.get("mainline_context"), dict)
            else {}
        )

        benchmark_ok = bool(df["benchmark_ok"].iloc[0])
        market_target_position = self._safe_float(df["market_target_position"].iloc[0], 0.0)
        market_score = self._safe_float(df["market_sentiment_score"].iloc[0], 55.0)
        market_action = str(df["market_action"].iloc[0] or "WATCH")
        market_signal = str(df["market_signal"].iloc[0] or "")
        market_label = str(df["market_label"].iloc[0] or "观望")
        market_mood = str(df["market_mood"].iloc[0] or "CONFUSED")

        if bool(p.get("require_benchmark", True)) and not benchmark_ok:
            return self._empty_result()

        if bool(p.get("require_market_signal", True)):
            if market_action == "SELL" or market_target_position < float(p["min_market_target_position"]):
                return self._empty_result()

        df = df[df["amount"] >= float(p["min_amount"])].copy()
        if df.empty:
            return self._empty_result()

        trend_guard = (
            (df["close"] > 0)
            & (df["ma20"] > 0)
            & (df["ma50"] > 0)
            & (df["close"] >= df["ma20"] * float(p["min_close_to_ma20_ratio"]))
            & (df["ma20"] >= df["ma50"] * 0.99)
            & (df["ret_20"] >= float(p["min_ret_20"]))
            & (df["amt_ratio_20"] >= float(p["min_amt_ratio_20"]))
        )
        df = df[trend_guard].copy()
        if df.empty:
            return self._empty_result()

        if bool(p.get("require_mainline", True)):
            base_mainline_mask = (
                (df["mainline_score"] >= float(p["min_mainline_score"]))
                & (df["mainline_rank"] <= float(p["max_mainline_rank"]))
            )
            durable_mainline_mask = (
                (df["mainline_persistence_score"] >= float(p["min_mainline_persistence"]))
                & (df["mainline_rank"] <= float(p["max_mainline_rank"]) + float(p["persistence_rank_slack"]))
                & (df["mainline_score"] >= float(p["min_mainline_score"]) * 0.82)
            )
            df = df[base_mainline_mask | durable_mainline_mask].copy()
            if df.empty:
                return self._empty_result()

        ma_trend_pct = (df["ma20"] / df["ma50"] - 1.0) * 100.0
        extension_pct = (df["close"] / df["ma20"] - 1.0) * 100.0
        breakout_20_pct = np.where(
            df["high_20_prev"] > 0,
            (df["close"] / df["high_20_prev"] - 1.0) * 100.0,
            -20.0,
        )
        stop_buffer_pct = np.where(
            df["low_10_prev"] > 0,
            (df["close"] / df["low_10_prev"] - 1.0) * 100.0,
            0.0,
        )
        price_action_guard = (
            (df["pct_chg"] <= float(p["max_daily_jump"]))
            & (df["board_break_20d"] <= float(p["max_board_break_20d"]))
        )
        crowded_chase_guard = ~(
            (df["mainline_crowding_score"] >= float(p["crowding_hard_cap"]))
            & (
                (df["pct_chg"] >= float(p["max_daily_jump"]) - 0.6)
                | (extension_pct >= float(p["hot_theme_extension_cap"]))
                | (df["limit_up_5d"] > float(p["max_limit_up_5d"]))
            )
        )
        rank_cap = max(1.0, float(p["max_mainline_rank"]))
        rank_bonus = ((rank_cap + 1.0 - df["mainline_rank"].clip(upper=rank_cap + 1.0)) / rank_cap) * 18.0
        theme_durability_score = self._clip(
            0.45 * df["mainline_persistence_score"]
            + 8.0 * df["mainline_current_streak"]
            + 2.2 * df["mainline_top3_days"]
            + 0.9 * df["mainline_top8_days"]
        )
        mainline_support = self._clip(
            df["mainline_score"] * 0.64
            + rank_bonus
            + 0.22 * theme_durability_score
        )

        flow_signal = (
            0.55 * np.sign(df["mf_5"]) * np.log1p(df["mf_5"].abs())
            + 0.20 * np.sign(df["mf_today"]) * np.log1p(df["mf_today"].abs())
            + 0.25 * np.clip((df["amt_ratio_20"] - 1.0) * 2.0, -1.2, 2.0)
        )
        flow_score = self._clip(50.0 + 16.0 * flow_signal)

        trend_score = self._clip(
            50.0
            + 0.75 * df["ret_20"]
            + 0.55 * df["ret_10"]
            + 1.80 * ma_trend_pct
            - 1.20 * np.maximum(extension_pct - 10.0, 0.0)
        )

        breakout_window = self._triangle_score(pd.Series(breakout_20_pct, index=df.index), center=1.8, width=5.0)
        pullback_window = self._triangle_score(pd.Series(breakout_20_pct, index=df.index), center=-3.5, width=5.5)
        extension_window = self._triangle_score(pd.Series(extension_pct, index=df.index), center=3.0, width=7.0)
        atr_window = self._triangle_score(df["atr_pct_14"].clip(lower=0.2), center=2.8, width=2.0)
        liquidity_score = self._clip(50.0 + 38.0 * (df["amt_ratio_20"] - 1.0))
        leadership_score = (
            0.55 * self._triangle_score(df["limit_up_30d"], center=1.0, width=2.5)
            + 0.45 * self._triangle_score(df["strong_up_20d"], center=2.0, width=4.0)
        )
        leader_continuity_score = (
            0.30 * self._triangle_score(df["limit_up_10d"], center=1.0, width=2.5)
            + 0.25 * self._triangle_score(df["leader_up_5d"], center=1.0, width=2.0)
            + 0.20 * self._triangle_score(df["strong_up_20d"], center=2.0, width=4.0)
            + 0.25 * self._clip(100.0 - df["board_break_20d"] * 28.0)
        )

        trend_breakout_score = (
            0.30 * breakout_window
            + 0.18 * extension_window
            + 0.14 * trend_score
            + 0.13 * flow_score
            + 0.13 * mainline_support
            + 0.12 * leader_continuity_score
        )
        pullback_reentry_score = (
            0.26 * pullback_window
            + 0.18 * extension_window
            + 0.14 * trend_score
            + 0.12 * flow_score
            + 0.08 * liquidity_score
            + 0.10 * leadership_score
            + 0.12 * leader_continuity_score
        )
        entry_style = np.where(trend_breakout_score >= pullback_reentry_score, "主升突破", "分歧回流")
        setup_score = np.maximum(trend_breakout_score, pullback_reentry_score)

        market_score_series = pd.Series(
            self._clip(
                0.55 * market_score
                + 35.0 * market_target_position
                + (8.0 if market_action == "BUY" else 0.0)
                + (4.0 if market_action == "HOLD" else 0.0)
                - (10.0 if market_action == "WATCH" else 0.0)
                - (25.0 if market_action == "SELL" else 0.0)
            ),
            index=df.index,
        )
        quality_score = (
            0.30 * trend_score
            + 0.20 * liquidity_score
            + 0.16 * atr_window
            + 0.14 * leadership_score
            + 0.20 * leader_continuity_score
        )
        crowding_extension_cap = np.where(
            df["mainline_crowding_score"] >= float(p["crowding_soft_cap"]),
            float(p["hot_theme_extension_cap"]),
            float(p["max_extension_pct"]),
        )
        crowding_penalty = (
            0.55 * np.maximum(df["mainline_crowding_score"] - float(p["crowding_soft_cap"]), 0.0)
            + 2.5 * np.maximum(extension_pct - crowding_extension_cap, 0.0)
            + 1.8 * np.maximum(
                breakout_20_pct - np.where(df["mainline_crowding_score"] >= float(p["crowding_soft_cap"]), 4.0, float(p["max_breakout_pct"])),
                0.0,
            )
            + 9.0 * np.maximum(df["limit_up_5d"] - float(p["max_limit_up_5d"]), 0.0)
            + 6.0 * df["board_break_20d"]
            + np.where(
                (df["mainline_crowding_score"] >= float(p["crowding_hard_cap"]))
                & (df["pct_chg"] >= float(p["max_daily_jump"]) - 0.8),
                16.0,
                0.0,
            )
        )
        persistence_penalty = (
            0.22 * np.maximum(float(p["min_mainline_persistence"]) - df["mainline_persistence_score"], 0.0)
            + np.where(
                (df["mainline_persistence_score"] < float(p["min_mainline_persistence"]))
                & (df["mainline_crowding_score"] >= 55.0),
                float(p["low_persistence_penalty"]),
                0.0,
            )
        )
        crowding_penalty = crowding_penalty + persistence_penalty

        risk_penalty = (
            4.2 * np.maximum(extension_pct - float(p["max_extension_pct"]), 0.0)
            + 5.0 * np.maximum(breakout_20_pct - float(p["max_breakout_pct"]), 0.0)
            + 2.2 * np.maximum(df["ret_20"] - float(p["max_ret_20"]), 0.0)
            + 10.0 * np.maximum(df["limit_up_30d"] - float(p["max_limit_up_30d"]), 0.0)
            + 7.0 * np.maximum(df["vol_10"] - float(p["max_vol_10"]), 0.0)
            + 10.0 * np.maximum(df["atr_pct_14"] - float(p["max_atr_pct_14"]), 0.0)
            + 1.6 * np.maximum(stop_buffer_pct - float(p["preferred_stop_buffer_pct"]), 0.0)
            + crowding_penalty
        )
        if market_action in {"WATCH", "SELL"}:
            risk_penalty = risk_penalty + 8.0
        if market_mood in {"EUPHORIA", "BOILING"} and market_target_position < 0.75:
            risk_penalty = risk_penalty + 6.0
        risk_quality = self._clip(100.0 - risk_penalty)

        weights = {
            "setup": max(0.01, float(p["w_setup"])),
            "mainline": max(0.01, float(p["w_mainline"])),
            "flow": max(0.01, float(p["w_flow"])),
            "market": max(0.01, float(p["w_market"])),
            "quality": max(0.01, float(p["w_quality"])),
            "risk": max(0.01, float(p["w_risk"])),
        }
        weight_sum = sum(weights.values())
        for k in list(weights.keys()):
            weights[k] = weights[k] / weight_sum

        composite = (
            weights["setup"] * setup_score
            + weights["mainline"] * mainline_support
            + weights["flow"] * flow_score
            + weights["market"] * market_score_series
            + weights["quality"] * quality_score
            + weights["risk"] * risk_quality
        )
        confidence = self._clip(
            0.28 * setup_score
            + 0.20 * mainline_support
            + 0.14 * flow_score
            + 0.12 * quality_score
            + 0.14 * market_score_series
            + 0.12 * risk_quality
            + np.where(entry_style == "分歧回流", 3.0, 0.0)
        )

        df["strategy_score"] = self._clip(composite, 1.0, 99.0)
        df["confidence"] = self._clip(confidence, 1.0, 99.0)
        df["setup_score"] = setup_score
        df["mainline_support"] = mainline_support
        df["flow_score"] = flow_score
        df["market_score"] = market_score_series
        df["quality_score"] = quality_score
        df["theme_durability_score"] = theme_durability_score
        df["leader_continuity_score"] = leader_continuity_score
        df["crowding_penalty"] = crowding_penalty
        df["risk_penalty"] = self._clip(risk_penalty, 0.0, 100.0)
        df["risk_quality"] = risk_quality
        df["extension_pct"] = extension_pct
        df["breakout_20_pct"] = breakout_20_pct
        df["stop_buffer_pct"] = stop_buffer_pct
        df["entry_style"] = entry_style

        tradable_mask = (
            price_action_guard
            & crowded_chase_guard
            & (df["strategy_score"] >= float(p["min_score_to_pick"]))
            & (df["confidence"] >= float(p["min_confidence_to_pick"]))
        )
        observation_mask = (
            ~tradable_mask
            & (df["strategy_score"] >= float(p["observe_score_floor"]))
            & (df["confidence"] >= float(p["observe_confidence_floor"]))
        )

        observation_pool = []
        if observation_mask.any():
            obs_df = df.loc[observation_mask].copy().sort_values(
                ["strategy_score", "confidence", "mainline_score", "amount"],
                ascending=[False, False, False, False],
            ).head(int(p["observe_top_n"]))
            observation_pool = [
                {
                    "ts_code": str(r["ts_code"]),
                    "name": str(r.get("name", "")),
                    "strategy_score": round(float(r["strategy_score"]), 2),
                    "confidence": round(float(r["confidence"]), 2),
                    "signal_label": "观察池",
                    "pool_tier": "observe",
                    "observation_reason": self._build_observation_reason(r, p),
                    "mainline_name": str(r.get("mainline_name") or ""),
                    "mainline_phase": str(r.get("mainline_phase") or "轮动观察"),
                    "crowding_score": round(float(r.get("mainline_crowding_score") or 0.0), 2),
                    "score_breakdown": {
                        "entry_style": str(r["entry_style"]),
                        "factor_scores": {
                            "setup_score": round(float(r["setup_score"]), 2),
                            "mainline_score": round(float(r["mainline_support"]), 2),
                            "mainline_persistence_score": round(float(r["mainline_persistence_score"]), 2),
                            "theme_durability_score": round(float(r["theme_durability_score"]), 2),
                            "leader_continuity_score": round(float(r["leader_continuity_score"]), 2),
                            "flow_score": round(float(r["flow_score"]), 2),
                            "market_score": round(float(r["market_score"]), 2),
                            "quality_score": round(float(r["quality_score"]), 2),
                        },
                        "risk_signals": {
                            "pct_chg": round(float(r["pct_chg"]), 2),
                            "extension_pct": round(float(r["extension_pct"]), 2),
                            "mainline_top3_days": int(round(float(r["mainline_top3_days"]))),
                            "mainline_top8_days": int(round(float(r["mainline_top8_days"]))),
                            "mainline_current_streak": int(round(float(r["mainline_current_streak"]))),
                            "limit_up_5d": int(round(float(r["limit_up_5d"]))),
                            "board_break_20d": int(round(float(r["board_break_20d"]))),
                            "mainline_crowding_score": round(float(r["mainline_crowding_score"]), 2),
                            "crowding_penalty": round(float(r["crowding_penalty"]), 2),
                        },
                    },
                }
                for _, r in obs_df.iterrows()
            ]

        base_portfolio_advice = {
            "market_trade_date": market_context.get("market_trade_date", as_of_date),
            "market_signal": market_signal,
            "market_label": market_label,
            "market_mood": market_mood,
            "market_target_position": round(float(market_target_position), 4),
            "suggested_pick_count": 0,
            "max_single_position": round(float(p["max_single_position"]), 4),
            "allocated_position": 0.0,
            "cash_buffer": round(float(max(0.0, market_target_position)), 4),
            "top_mainlines": mainline_context.get("top_mainlines", [])[:5],
        }

        tradable_df = df.loc[tradable_mask].copy()
        if tradable_df.empty:
            empty = self._empty_result()
            empty.attrs["portfolio_advice"] = base_portfolio_advice
            empty.attrs["observation_pool"] = observation_pool
            empty.attrs["pool_split"] = {
                "tradable_count": 0,
                "observation_count": len(observation_pool),
            }
            return empty

        tradable_df = tradable_df.sort_values(
            ["strategy_score", "confidence", "mainline_score", "amount"],
            ascending=[False, False, False, False],
        )

        max_position_count_full = max(1, int(p["max_position_count_full"]))
        budgeted_count = max(
            1,
            int(round(max_position_count_full * max(market_target_position, 0.35))),
        )
        target_n = min(max(1, int(p["top_n"])), budgeted_count)
        max_picks_per_mainline = max(1, int(p["max_picks_per_mainline"]))

        selected_rows: list[pd.Series] = []
        line_counts: dict[str, int] = {}
        for _, row in tradable_df.iterrows():
            line = str(row.get("mainline_name") or "其他")
            if line_counts.get(line, 0) >= max_picks_per_mainline:
                continue
            selected_rows.append(row)
            line_counts[line] = line_counts.get(line, 0) + 1
            if len(selected_rows) >= target_n:
                break

        if not selected_rows:
            empty = self._empty_result()
            empty.attrs["portfolio_advice"] = base_portfolio_advice
            empty.attrs["observation_pool"] = observation_pool
            empty.attrs["pool_split"] = {
                "tradable_count": 0,
                "observation_count": len(observation_pool),
            }
            return empty

        out = pd.DataFrame(selected_rows).reset_index(drop=True)
        raw_weight = (
            (out["strategy_score"].clip(lower=1.0) / 100.0)
            * (0.55 + out["confidence"].clip(lower=1.0) / 100.0 * 0.45)
        )
        alloc = raw_weight / max(float(raw_weight.sum()), 1e-6) * market_target_position
        alloc = alloc.clip(upper=float(p["max_single_position"]))
        out["suggested_position"] = alloc.round(4)

        def to_label(score: float, conf: float) -> str:
            if score >= 78 and conf >= 72:
                return "强信号"
            if score >= 66:
                return "偏强"
            return "观察"

        out["signal_label"] = out.apply(
            lambda r: to_label(float(r["strategy_score"]), float(r["confidence"])),
            axis=1,
        )

        out["score_breakdown"] = out.apply(
            lambda r: {
                "entry_logic": "market regime + mainline resonance + leader continuity - crowding chase",
                "entry_style": str(r["entry_style"]),
                "market_signal": market_signal,
                "market_label": market_label,
                "market_target_position": round(float(market_target_position), 4),
                "mainline_name": str(r.get("mainline_name") or ""),
                "mainline_rank": int(r.get("mainline_rank") or 999),
                "mainline_phase": str(r.get("mainline_phase") or "轮动观察"),
                "factor_scores": {
                    "setup_score": round(float(r["setup_score"]), 2),
                    "mainline_score": round(float(r["mainline_support"]), 2),
                    "mainline_persistence_score": round(float(r["mainline_persistence_score"]), 2),
                    "theme_durability_score": round(float(r["theme_durability_score"]), 2),
                    "leader_continuity_score": round(float(r["leader_continuity_score"]), 2),
                    "flow_score": round(float(r["flow_score"]), 2),
                    "market_score": round(float(r["market_score"]), 2),
                    "quality_score": round(float(r["quality_score"]), 2),
                    "risk_quality": round(float(r["risk_quality"]), 2),
                },
                "risk_signals": {
                    "pct_chg": round(float(r["pct_chg"]), 2),
                    "extension_pct": round(float(r["extension_pct"]), 2),
                    "breakout_20_pct": round(float(r["breakout_20_pct"]), 2),
                    "stop_buffer_pct": round(float(r["stop_buffer_pct"]), 2),
                    "ret_20": round(float(r["ret_20"]), 2),
                    "vol_10": round(float(r["vol_10"]), 2),
                    "atr_pct_14": round(float(r["atr_pct_14"]), 2),
                    "mainline_top3_days": int(round(float(r["mainline_top3_days"]))),
                    "mainline_top8_days": int(round(float(r["mainline_top8_days"]))),
                    "mainline_current_streak": int(round(float(r["mainline_current_streak"]))),
                    "limit_up_5d": int(round(float(r["limit_up_5d"]))),
                    "limit_up_10d": int(round(float(r["limit_up_10d"]))),
                    "limit_up_30d": int(round(float(r["limit_up_30d"]))),
                    "leader_up_5d": int(round(float(r["leader_up_5d"]))),
                    "board_break_20d": int(round(float(r["board_break_20d"]))),
                    "mainline_crowding_score": round(float(r["mainline_crowding_score"]), 2),
                    "crowding_penalty": round(float(r["crowding_penalty"]), 2),
                    "risk_penalty": round(float(r["risk_penalty"]), 2),
                },
                "position_plan": {
                    "suggested_position_pct": round(float(r["suggested_position"]) * 100.0, 2),
                    "max_single_position_pct": round(float(p["max_single_position"]) * 100.0, 2),
                    "target_book_exposure_pct": round(float(market_target_position) * 100.0, 2),
                },
                "pool_tier": "tradable",
                "formula": {k: round(v, 4) for k, v in weights.items()},
            },
            axis=1,
        )

        total_alloc = float(out["suggested_position"].sum())
        portfolio_advice = {
            **base_portfolio_advice,
            "suggested_pick_count": int(len(out)),
            "allocated_position": round(total_alloc, 4),
            "cash_buffer": round(max(0.0, float(market_target_position) - total_alloc), 4),
        }
        out.attrs["portfolio_advice"] = portfolio_advice
        out.attrs["observation_pool"] = observation_pool
        out.attrs["pool_split"] = {
            "tradable_count": int(len(out)),
            "observation_count": len(observation_pool),
        }

        return out[
            [
                "ts_code",
                "name",
                "strategy_score",
                "confidence",
                "signal_label",
                "score_breakdown",
            ]
        ]

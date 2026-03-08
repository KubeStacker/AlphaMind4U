from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from etl.utils.kline_patterns import (
    DEFAULT_CALIBRATION_PATH,
    ALL_PATTERN_COLS,
    BULLISH_PATTERNS,
    BEARISH_PATTERNS,
    PATTERN_CN_MAP,
    detect_all_patterns,
    get_latest_signals,
)
from strategy.falcon.plugins.base import FalconPlugin

logger = logging.getLogger(__name__)

CALIBRATION_PATH = Path(__file__).parent.parent.parent.parent / "etl" / "utils" / "kline_pattern_calibration.json"


def _load_calibration() -> dict[str, Any] | None:
    if not CALIBRATION_PATH.exists():
        logger.warning(f"Calibration file not found: {CALIBRATION_PATH}, using raw confidence only")
        return None
    try:
        import json
        with open(CALIBRATION_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load calibration: {e}")
        return None


class ClassicKlineRecommenderPlugin(FalconPlugin):
    strategy_id = "classic_kline_recommender"
    display_name = "经典K线识别推荐"
    version = "1.0.0"

    def default_params(self) -> dict[str, Any]:
        return {
            "top_n": 10,
            "min_amount": 2.0e6,
            "require_benchmark": True,
            "history_days": 60,
            "min_pattern_confidence": 0.55,
            "min_historical_hit_rate": 0.50,
            "min_sample_count": 30,
            "w_pattern": 0.30,
            "w_trend": 0.22,
            "w_flow": 0.16,
            "w_sector": 0.16,
            "w_market": 0.10,
            "w_risk": 0.18,
            "max_bearish_override": 3,
            "min_score_to_pick": 35.0,
            "min_confidence_to_pick": 50.0,
            "calibration_horizon": 5,
        }

    @staticmethod
    def _safe_num(df: pd.DataFrame, col: str, default: float = 0.0) -> pd.Series:
        result = df.get(col)
        if result is None:
            return pd.Series([default], index=df.index)
        return pd.to_numeric(result, errors="coerce").fillna(default)

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

    def _fetch_stock_history(self, ts_code: str, as_of_date: str, days: int) -> pd.DataFrame | None:
        from db.connection import get_db_connection
        try:
            with get_db_connection() as conn:
                query = f"""
                    SELECT trade_date, ts_code, open, high, low, close, vol, amount
                    FROM daily_price
                    WHERE ts_code = '{ts_code}'
                      AND trade_date <= '{as_of_date}'
                    ORDER BY trade_date DESC
                    LIMIT {days}
                """
                result = conn.execute(query).df()
                if result is not None and not result.empty:
                    result = result.sort_values("trade_date").reset_index(drop=True)
                return result
        except Exception as e:
            logger.warning(f"Failed to fetch history for {ts_code}: {e}")
            return None

    def _calculate_pattern_score(
        self,
        history_df: pd.DataFrame,
        calibration: dict[str, Any] | None,
        calibration_horizon: int,
        min_confidence: float,
        min_hit_rate: float,
        min_sample_count: int,
        max_bearish_override: int,
    ) -> dict[str, Any]:
        if history_df is None or len(history_df) < 30:
            return {
                "pattern_score": 0.0,
                "confidence": 0.0,
                "bullish_signals": [],
                "bearish_signals": [],
                "has_valid_signal": False,
            }

        try:
            enriched = detect_all_patterns(history_df)
        except Exception as e:
            logger.warning(f"Pattern detection failed: {e}")
            return {
                "pattern_score": 0.0,
                "confidence": 0.0,
                "bullish_signals": [],
                "bearish_signals": [],
                "has_valid_signal": False,
            }

        signals = get_latest_signals(
            enriched,
            min_confidence=min_confidence,
            calibration=calibration,
            calibration_horizon=calibration_horizon,
        )

        bullish_signals = [s for s in signals if s.get("direction") == "bullish"]
        bearish_signals = [s for s in signals if s.get("direction") == "bearish"]

        if not signals:
            return {
                "pattern_score": 0.0,
                "confidence": 0.0,
                "bullish_signals": [],
                "bearish_signals": [],
                "has_valid_signal": False,
            }

        if calibration:
            valid_bullish = []
            for s in bullish_signals:
                code = s.get("code", "")
                if code in calibration.get("patterns", {}):
                    pattern_stats = calibration["patterns"][code]
                    stats_5d = pattern_stats.get(f"{calibration_horizon}d", {})
                    sample_count = stats_5d.get("sample_count", 0)
                    hit_rate = stats_5d.get("bayes_hit_rate", stats_5d.get("hit_rate"))
                    if sample_count >= min_sample_count and (hit_rate is None or hit_rate >= min_hit_rate):
                        valid_bullish.append(s)

            valid_bearish = []
            for s in bearish_signals:
                code = s.get("code", "")
                if code in calibration.get("patterns", {}):
                    pattern_stats = calibration["patterns"][code]
                    stats_5d = pattern_stats.get(f"{calibration_horizon}d", {})
                    sample_count = stats_5d.get("sample_count", 0)
                    hit_rate = stats_5d.get("bayes_hit_rate", stats_5d.get("hit_rate"))
                    if sample_count >= min_sample_count and (hit_rate is None or hit_rate >= min_hit_rate):
                        valid_bearish.append(s)

            bullish_signals = valid_bullish
            bearish_signals = valid_bearish

        top_bullish = sorted(bullish_signals, key=lambda x: -x.get("confidence", 0.0))[:3]
        top_bearish = sorted(bearish_signals, key=lambda x: -x.get("confidence", 0.0))[:max_bearish_override]

        bullish_score = sum(s.get("confidence", 0.0) for s in top_bullish) / max(1, len(top_bullish)) * 100.0
        bearish_score = sum(s.get("confidence", 0.0) for s in top_bearish) / max(1, len(top_bearish)) * 100.0

        pattern_score = bullish_score - bearish_score

        has_valid = len(top_bullish) > 0 or len(top_bearish) > 0
        avg_conf = np.mean([s.get("confidence", 0.0) for s in signals]) if signals else 0.0

        return {
            "pattern_score": round(float(pattern_score), 2),
            "confidence": round(float(avg_conf * 100), 2),
            "bullish_signals": top_bullish,
            "bearish_signals": top_bearish,
            "has_valid_signal": has_valid,
        }

    def _calculate_trend_score(self, snapshot: pd.Series) -> float:
        ret_20 = self._safe_num(pd.DataFrame([snapshot]), "ret_20").iloc[0]
        ma20 = self._safe_num(pd.DataFrame([snapshot]), "ma20").iloc[0]
        ma50 = self._safe_num(pd.DataFrame([snapshot]), "ma50").iloc[0]
        close = self._safe_num(pd.DataFrame([snapshot]), "close").iloc[0]

        if ma20 <= 0 or ma50 <= 0 or close <= 0:
            return 50.0

        ma_slope = (ma20 / ma50 - 1.0) * 100.0
        position_score = (close / ma20 - 1.0) * 100.0 if ma20 > 0 else 0.0

        trend_z = 0.5 * ret_20 + 0.3 * ma_slope + 0.2 * position_score
        trend_score = 50.0 + 20.0 * trend_z
        return float(np.clip(trend_score, 0, 100))

    def _calculate_flow_score(self, snapshot: pd.Series) -> float:
        mf_today = self._safe_num(pd.DataFrame([snapshot]), "mf_today").iloc[0]
        mf_5 = self._safe_num(pd.DataFrame([snapshot]), "mf_5").iloc[0]

        flow_signal = 0.65 * np.sign(mf_5) * np.log1p(abs(mf_5)) + 0.35 * np.sign(mf_today) * np.log1p(abs(mf_today))
        flow_score = 50.0 + 15.0 * flow_signal
        return float(np.clip(flow_score, 0, 100))

    def _calculate_sector_score(self, snapshot: pd.Series, default: float = 50.0) -> float:
        sector_strength = self._safe_num(pd.DataFrame([snapshot]), "sector_strength").iloc[0]
        mainline_score = self._safe_num(pd.DataFrame([snapshot]), "mainline_score").iloc[0]

        if sector_strength == 0 and mainline_score == 0:
            return default

        sector_val = sector_strength if sector_strength > 0 else 0.0
        mainline_val = mainline_score if mainline_score > 0 else 0.0

        return float(np.clip(50.0 + 0.5 * sector_val + 0.3 * mainline_val, 0, 100))

    def _calculate_market_regime_score(self, snapshot: pd.Series) -> float:
        benchmark_ok = snapshot.get("benchmark_ok", True)
        if not benchmark_ok:
            return 30.0
        return 70.0

    def _calculate_risk_penalty(
        self,
        snapshot: pd.Series,
        pattern_result: dict[str, Any],
    ) -> float:
        pct_chg = abs(self._safe_num(pd.DataFrame([snapshot]), "pct_chg").iloc[0])
        vol_10 = self._safe_num(pd.DataFrame([snapshot]), "vol_10").iloc[0]
        ret_20 = self._safe_num(pd.DataFrame([snapshot]), "ret_20").iloc[0]
        high_50_prev = self._safe_num(pd.DataFrame([snapshot]), "high_50_prev").iloc[0]
        close = self._safe_num(pd.DataFrame([snapshot]), "close").iloc[0]

        bearish_signals = pattern_result.get("bearish_signals", [])
        has_bearish = len(bearish_signals) > 0

        penalty = 0.0

        if pct_chg > 9.0:
            penalty += 15.0
        elif pct_chg > 7.0:
            penalty += 8.0

        if vol_10 > 8.0:
            penalty += 12.0
        elif vol_10 > 6.0:
            penalty += 6.0

        if high_50_prev > 0 and close > 0:
            breakout_strength = (close / high_50_prev - 1.0) * 100.0
            if breakout_strength > 15.0:
                penalty += 10.0
            elif breakout_strength > 10.0:
                penalty += 5.0

        if has_bearish:
            penalty += 8.0

        if ret_20 > 25.0:
            penalty += 10.0
        elif ret_20 > 15.0:
            penalty += 5.0

        return float(np.clip(penalty, 0, 50))

    def run(self, universe_df: pd.DataFrame, as_of_date: str, params: dict[str, Any]) -> pd.DataFrame:
        if universe_df is None or universe_df.empty:
            return pd.DataFrame(columns=[
                "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
            ])

        p = {**self.default_params(), **(params or {})}
        d = self.default_params()

        history_days = int(p.get("history_days", d["history_days"]))
        min_pattern_conf = float(p.get("min_pattern_confidence", d["min_pattern_confidence"]))
        min_hit_rate = float(p.get("min_historical_hit_rate", d["min_historical_hit_rate"]))
        min_sample_count = int(p.get("min_sample_count", d["min_sample_count"]))
        max_bearish_override = int(p.get("max_bearish_override", d["max_bearish_override"]))
        calibration_horizon = int(p.get("calibration_horizon", d["calibration_horizon"]))

        calibration = _load_calibration()

        if bool(p.get("require_benchmark", True)):
            benchmark_ok = universe_df.get("benchmark_ok", pd.Series([True] * len(universe_df)))
            if not benchmark_ok.any():
                return pd.DataFrame(columns=[
                    "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
                ])

        min_amount = float(p.get("min_amount", d["min_amount"]))
        universe_df = universe_df[universe_df["amount"] >= min_amount].copy()
        if universe_df.empty:
            return pd.DataFrame(columns=[
                "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
            ])

        results: list[dict[str, Any]] = []

        for idx, row in universe_df.iterrows():
            ts_code = row.get("ts_code")
            if not ts_code:
                continue

            try:
                history_df = self._fetch_stock_history(ts_code, as_of_date, history_days)

                pattern_result = self._calculate_pattern_score(
                    history_df,
                    calibration,
                    calibration_horizon,
                    min_pattern_conf,
                    min_hit_rate,
                    min_sample_count,
                    max_bearish_override,
                )

                if not pattern_result.get("has_valid_signal", False):
                    continue

                trend_score = self._calculate_trend_score(row)
                flow_score = self._calculate_flow_score(row)
                sector_score = self._calculate_sector_score(row, default=50.0)
                market_regime_score = self._calculate_market_regime_score(row)
                risk_penalty = self._calculate_risk_penalty(row, pattern_result)
            except Exception as e:
                logger.warning(f"Error processing {ts_code}: {e}")
                continue

            w_pattern = max(0.01, float(p.get("w_pattern", d["w_pattern"])))
            w_trend = max(0.01, float(p.get("w_trend", d["w_trend"])))
            w_flow = max(0.01, float(p.get("w_flow", d["w_flow"])))
            w_sector = max(0.01, float(p.get("w_sector", d["w_sector"])))
            w_market = max(0.01, float(p.get("w_market", d["w_market"])))
            w_risk = max(0.01, float(p.get("w_risk", d["w_risk"])))

            ws = w_pattern + w_trend + w_flow + w_sector + w_market + w_risk
            w_pattern /= ws
            w_trend /= ws
            w_flow /= ws
            w_sector /= ws
            w_market /= ws
            w_risk /= ws

            composite_score = (
                w_pattern * pattern_result["pattern_score"]
                + w_trend * trend_score
                + w_flow * flow_score
                + w_sector * sector_score
                + w_market * market_regime_score
                - w_risk * risk_penalty
            )

            final_score = float(np.clip(composite_score, 1, 99))
            final_conf = float(np.clip(pattern_result["confidence"], 1, 99))

            results.append({
                "ts_code": ts_code,
                "name": row.get("name", ""),
                "strategy_score": final_score,
                "confidence": final_conf,
                "pattern_result": pattern_result,
                "trend_score": trend_score,
                "flow_score": flow_score,
                "sector_score": sector_score,
                "market_regime_score": market_regime_score,
                "risk_penalty": risk_penalty,
                "snapshot": row.to_dict(),
            })

        if not results:
            return pd.DataFrame(columns=[
                "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
            ])

        result_df = pd.DataFrame(results)

        min_score = max(20.0, float(p.get("min_score_to_pick", d["min_score_to_pick"])))
        min_conf = max(40.0, float(p.get("min_confidence_to_pick", d["min_confidence_to_pick"])))

        result_df = result_df[
            (result_df["strategy_score"] >= min_score) &
            (result_df["confidence"] >= min_conf)
        ].copy()

        if result_df.empty:
            return pd.DataFrame(columns=[
                "ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"
            ])

        result_df = result_df.sort_values(["strategy_score", "confidence"], ascending=[False, False])
        top_n = max(1, int(p.get("top_n", d["top_n"])))
        result_df = result_df.head(top_n)

        def to_label(s: float) -> str:
            if s >= 75:
                return "强信号"
            if s >= 60:
                return "偏强"
            if s >= 45:
                return "观察"
            return "谨慎"

        result_df["signal_label"] = result_df["strategy_score"].apply(lambda x: to_label(float(x)))

        def build_breakdown(r: dict) -> dict:
            bullish = r.get("pattern_result", {}).get("bullish_signals", [])
            bearish = r.get("pattern_result", {}).get("bearish_signals", [])

            kline_signals = []
            for s in bullish + bearish:
                kline_signals.append({
                    "pattern_code": s.get("code", ""),
                    "pattern_name": s.get("pattern", ""),
                    "direction": s.get("direction", ""),
                    "raw_confidence": s.get("raw_confidence", 0.0),
                    "calibrated_confidence": s.get("confidence", 0.0),
                    "historical_hit_rate": s.get("historical_hit_rate"),
                    "historical_avg_ret": s.get("historical_avg_ret"),
                })

            return {
                "entry_logic": "classic_kline + trend + flow + sector - risk",
                "kline_signals": kline_signals,
                "factor_scores": {
                    "pattern_score": r.get("pattern_result", {}).get("pattern_score", 0.0),
                    "trend_score": r.get("trend_score", 0.0),
                    "flow_score": r.get("flow_score", 0.0),
                    "sector_score": r.get("sector_score", 0.0),
                    "market_regime_score": r.get("market_regime_score", 0.0),
                    "risk_penalty": r.get("risk_penalty", 0.0),
                },
                "weights": {
                    "w_pattern": round(w_pattern, 4),
                    "w_trend": round(w_trend, 4),
                    "w_flow": round(w_flow, 4),
                    "w_sector": round(w_sector, 4),
                    "w_market": round(w_market, 4),
                    "w_risk": round(w_risk, 4),
                },
                "market_env": {
                    "benchmark_ok": bool(r.get("snapshot", {}).get("benchmark_ok", True)),
                    "risk_mode": "normal" if r.get("risk_penalty", 0) < 15 else "high",
                },
            }

        result_df["score_breakdown"] = result_df.apply(build_breakdown, axis=1)

        return result_df[["ts_code", "name", "strategy_score", "confidence", "signal_label", "score_breakdown"]]

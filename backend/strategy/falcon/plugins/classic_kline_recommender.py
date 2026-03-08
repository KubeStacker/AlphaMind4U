import logging
import json
import os
import time
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

_CALIBRATION_CACHE = None
_CALIBRATION_MTIME = 0

def _load_calibration() -> dict[str, Any] | None:
    global _CALIBRATION_CACHE, _CALIBRATION_MTIME
    if not CALIBRATION_PATH.exists():
        return None
        
    try:
        mtime = os.path.getmtime(CALIBRATION_PATH)
        if _CALIBRATION_CACHE is not None and mtime <= _CALIBRATION_MTIME:
            return _CALIBRATION_CACHE
            
        with open(CALIBRATION_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            _CALIBRATION_CACHE = data
            _CALIBRATION_MTIME = mtime
            return data
    except Exception as e:
        logger.warning(f"Failed to load calibration: {e}")
        return _CALIBRATION_CACHE # 优先保留旧缓存


class ClassicKlineRecommenderPlugin(FalconPlugin):
    strategy_id = "classic_kline_recommender"
    display_name = "经典K线识别推荐"
    version = "1.1.0"

    def default_params(self) -> dict[str, Any]:
        return {
            "top_n": 10,
            "min_amount": 2.0e6,
            "require_benchmark": True,
            "history_days": 60,
            "batch_size": 200,
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

    def _calculate_pattern_score_batch(
        self,
        panel_df: pd.DataFrame,
        calibration: dict[str, Any] | None,
        params: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        """批量计算形态得分"""
        if panel_df.empty:
            return {}
            
        results = {}
        grouped = panel_df.groupby("ts_code")
        
        for ts_code, history_df in grouped:
            if len(history_df) < 15: # 基础识别至少需要一定数量
                continue
                
            try:
                # 排序确保时间升序
                history_df = history_df.sort_values("trade_date")
                enriched = detect_all_patterns(history_df)
                
                signals = get_latest_signals(
                    enriched,
                    min_confidence=params["min_pattern_confidence"],
                    calibration=calibration,
                    calibration_horizon=params["calibration_horizon"],
                )
                
                if not signals:
                    continue
                    
                bullish_signals = [s for s in signals if s.get("direction") == "bullish"]
                bearish_signals = [s for s in signals if s.get("direction") == "bearish"]
                
                # 校准过滤
                if calibration:
                    valid_bullish = []
                    for s in bullish_signals:
                        stats = calibration.get("patterns", {}).get(s["code"], {}).get(f"{params['calibration_horizon']}d", {})
                        # 仅当统计信息存在且不满足阈值时才过滤
                        if stats:
                            if stats.get("sample_count", 0) < params["min_sample_count"] or \
                               stats.get("bayes_hit_rate", 1.0) < params["min_historical_hit_rate"]:
                                continue
                        valid_bullish.append(s)
                    bullish_signals = valid_bullish
                    
                    valid_bearish = []
                    for s in bearish_signals:
                        stats = calibration.get("patterns", {}).get(s["code"], {}).get(f"{params['calibration_horizon']}d", {})
                        # 仅当统计信息存在且样本数太少时才过滤 (看跌形态不要求胜率，只看样本可靠性)
                        if stats and stats.get("sample_count", 0) < params["min_sample_count"]:
                            continue
                        valid_bearish.append(s)
                    bearish_signals = valid_bearish

                if not bullish_signals and not bearish_signals:
                    continue

                top_bullish = sorted(bullish_signals, key=lambda x: -x.get("confidence", 0.0))[:3]
                top_bearish = sorted(bearish_signals, key=lambda x: -x.get("confidence", 0.0))[:params["max_bearish_override"]]

                bull_score = sum(s.get("confidence", 0.0) for s in top_bullish) / max(1, len(top_bullish)) * 100.0
                bear_score = sum(s.get("confidence", 0.0) for s in top_bearish) / max(1, len(top_bearish)) * 100.0
                
                avg_conf = np.mean([s.get("confidence", 0.0) for s in signals]) if signals else 0.0

                results[ts_code] = {
                    "pattern_score": round(float(bull_score - bear_score), 2),
                    "confidence": round(float(avg_conf * 100), 2),
                    "bullish_signals": top_bullish,
                    "bearish_signals": top_bearish,
                    "has_valid_signal": True
                }
            except Exception as e:
                logger.warning(f"Error processing patterns for {ts_code}: {e}")
                
        return results

    def _calculate_trend_score(self, snapshot: pd.Series) -> float:
        ret_20 = float(snapshot.get("ret_20") or 0.0)
        ma20 = float(snapshot.get("ma20") or 0.0)
        ma50 = float(snapshot.get("ma50") or 0.0)
        close = float(snapshot.get("close") or 0.0)

        if ma20 <= 0 or ma50 <= 0 or close <= 0:
            return 50.0

        ma_slope = (ma20 / ma50 - 1.0) * 100.0
        position_score = (close / ma20 - 1.0) * 100.0 if ma20 > 0 else 0.0

        trend_z = 0.5 * ret_20 + 0.3 * ma_slope + 0.2 * position_score
        trend_score = 50.0 + 20.0 * trend_z
        return float(np.clip(trend_score, 0, 100))

    def _calculate_flow_score(self, snapshot: pd.Series) -> float:
        mf_today = float(snapshot.get("mf_today") or 0.0)
        mf_5 = float(snapshot.get("mf_5") or 0.0)

        flow_signal = 0.65 * np.sign(mf_5) * np.log1p(abs(mf_5)) + 0.35 * np.sign(mf_today) * np.log1p(abs(mf_today))
        flow_score = 50.0 + 15.0 * flow_signal
        return float(np.clip(flow_score, 0, 100))

    def _calculate_sector_score(self, snapshot: pd.Series, default: float = 50.0) -> float:
        sector_strength = float(snapshot.get("sector_strength") or 0.0)
        mainline_score = float(snapshot.get("mainline_score") or 0.0)
        if sector_strength == 0 and mainline_score == 0:
            return default
        return float(np.clip(50.0 + 0.5 * sector_strength + 0.3 * mainline_score, 0, 100))

    def _calculate_market_regime_score(self, snapshot: pd.Series) -> float:
        return 70.0 if bool(snapshot.get("benchmark_ok", True)) else 30.0

    def _calculate_risk_penalty(self, snapshot: pd.Series, pattern_result: dict[str, Any]) -> float:
        pct_chg = abs(float(snapshot.get("pct_chg") or 0.0))
        vol_10 = float(snapshot.get("vol_10") or 0.0)
        ret_20 = float(snapshot.get("ret_20") or 0.0)
        high_50_prev = float(snapshot.get("high_50_prev") or 0.0)
        close = float(snapshot.get("close") or 0.0)
        
        penalty = 0.0
        if pct_chg > 9.0: penalty += 15.0
        elif pct_chg > 7.0: penalty += 8.0
        
        if vol_10 > 8.0: penalty += 12.0
        elif vol_10 > 6.0: penalty += 6.0
        
        if high_50_prev > 0 and close > 0:
            breakout = (close / high_50_prev - 1.0) * 100.0
            if breakout > 15.0: penalty += 10.0
            elif breakout > 10.0: penalty += 5.0
            
        if pattern_result.get("bearish_signals"): penalty += 8.0
        if ret_20 > 25.0: penalty += 10.0
        elif ret_20 > 15.0: penalty += 5.0
        
        return float(np.clip(penalty, 0, 50))

    def run(self, universe_df: pd.DataFrame, as_of_date: str, params: dict[str, Any]) -> pd.DataFrame:
        t0 = time.time()
        if universe_df is None or universe_df.empty:
            logger.warning(f"[{self.display_name}] Empty universe provided.")
            return pd.DataFrame()

        p = {**self.default_params(), **(params or {})}
        
        # 基础过滤
        min_amount = float(p["min_amount"])
        total_universe = len(universe_df)
        work_df = universe_df[universe_df["amount"] >= min_amount].copy()
        if p["require_benchmark"]:
            work_df = work_df[work_df.get("benchmark_ok", True)]
            
        filtered_count = len(work_df)
        logger.info(f"[{self.display_name}] Universe: {total_universe} -> Filtered: {filtered_count} (Amount>={min_amount/1e4:.0f}w, BenchOK={p['require_benchmark']})")

        if work_df.empty:
            return pd.DataFrame()

        calibration = _load_calibration()
        all_codes = work_df["ts_code"].tolist()
        batch_size = int(p["batch_size"])
        
        from etl.falcon_data_provider import DuckDbFalconDataProvider
        provider = DuckDbFalconDataProvider()
        
        pattern_results = {}
        # 批量拉取历史并识别
        t_all_batches_start = time.time()
        for i in range(0, len(all_codes), batch_size):
            t_chunk_start = time.time()
            chunk_codes = all_codes[i:i+batch_size]
            try:
                # 保持 DuckDB 单进程友好，顺序执行
                panel = provider.load_history_panel(as_of_date, chunk_codes, lookback_days=int(p["history_days"]))
                chunk_results = self._calculate_pattern_score_batch(panel, calibration, p)
                pattern_results.update(chunk_results)
                logger.debug(f"[{self.display_name}] Batch {i//batch_size + 1} processed {len(chunk_codes)} stocks in {time.time() - t_chunk_start:.2f}s")
            except Exception as e:
                logger.error(f"[{self.display_name}] Batch {i//batch_size + 1} failed: {e}")
        
        t_all_batches_end = time.time()
        logger.info(f"[{self.display_name}] Pattern recognition: {len(pattern_results)} hits out of {len(all_codes)}. Total batch time: {t_all_batches_end - t_all_batches_start:.2f}s")

        # 组装结果
        final_list = []
        for idx, row in work_df.iterrows():
            ts_code = row["ts_code"]
            pres = pattern_results.get(ts_code)
            if not pres or not pres.get("has_valid_signal"):
                continue
                
            trend_score = self._calculate_trend_score(row)
            flow_score = self._calculate_flow_score(row)
            sector_score = self._calculate_sector_score(row)
            market_score = self._calculate_market_regime_score(row)
            risk_penalty = self._calculate_risk_penalty(row, pres)
            
            w = {k: max(0.01, float(p.get(k, self.default_params()[k]))) 
                 for k in ["w_pattern", "w_trend", "w_flow", "w_sector", "w_market", "w_risk"]}
            ws = sum(w.values())
            
            composite = (
                (w["w_pattern"]/ws) * pres["pattern_score"] +
                (w["w_trend"]/ws) * trend_score +
                (w["w_flow"]/ws) * flow_score +
                (w["w_sector"]/ws) * sector_score +
                (w["w_market"]/ws) * market_score -
                (w["w_risk"]/ws) * risk_penalty
            )
            
            final_score = float(np.clip(composite, 1, 99))
            if final_score < float(p["min_score_to_pick"]) or pres["confidence"] < float(p["min_confidence_to_pick"]):
                continue

            # 生成可读理由
            bullish_names = [s["pattern"] for s in pres.get("bullish_signals", [])]
            reason_parts = []
            if bullish_names:
                reason_parts.append(f"形态:{','.join(bullish_names)}")
            if trend_score > 60:
                reason_parts.append("趋势强")
            elif trend_score < 40:
                reason_parts.append("趋势弱")
            
            if flow_score > 60:
                reason_parts.append("资金流入")
            
            if sector_score > 60:
                reason_parts.append("板块热")
                
            final_list.append({
                "ts_code": ts_code,
                "name": row.get("name", ""),
                "strategy_score": final_score,
                "confidence": pres["confidence"],
                "signal_label": "强信号" if final_score >= 75 else "偏强" if final_score >= 60 else "观察",
                "reason": " + ".join(reason_parts),
                "score_breakdown": {
                    "kline_signals": pres["bullish_signals"] + pres["bearish_signals"],
                    "factor_scores": {
                        "pattern_score": pres["pattern_score"],
                        "trend_score": trend_score,
                        "flow_score": flow_score,
                        "sector_score": sector_score,
                        "market_regime_score": market_score,
                        "risk_penalty": risk_penalty
                    }
                }
            })

        t1 = time.time()
        logger.info(f"[{self.display_name}] Finished. Selected {len(final_list)} candidates. Total time: {t1 - t0:.2f}s")

        if not final_list:
            return pd.DataFrame()
            
        res_df = pd.DataFrame(final_list).sort_values("strategy_score", ascending=False).head(int(p["top_n"]))
        return res_df

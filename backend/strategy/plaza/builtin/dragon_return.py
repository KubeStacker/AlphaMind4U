from __future__ import annotations

from typing import Any

import pandas as pd

from strategy.plaza.base import StrategyMeta
from strategy.plaza.builtin.common import (
    StrategySignal,
    ensure_analysis_columns,
    format_reason,
    infer_market_profile,
    is_basic_eligible,
    market_filter_passed,
    sector_filter_passed,
)


class Head7DragonReturnStrategy:
    def meta(self) -> StrategyMeta:
        return StrategyMeta(
            strategy_key="head7_dragon_return",
            name="头7龙回头",
            description="最后一个强势封板/最高点后 5-8 日缩量回踩，贴近 MA10/20 寻找二波启动点。",
            enabled=True,
            display_order=10,
            engine_version="v1",
        )

    def run_for_date(self, trade_date: str, context: Any) -> list:
        history_df = context.load_history_frame(trade_date, lookback_days=40)
        if history_df.empty:
            return []

        raw_signals: list[StrategySignal] = []
        for ts_code, frame in history_df.groupby("ts_code", sort=False):
            work = ensure_analysis_columns(frame)
            signal = self._evaluate_stock(work)
            if signal:
                raw_signals.append(signal)

        if not raw_signals:
            return []

        sector_meta_map = context.get_sector_meta(trade_date, [item.ts_code for item in raw_signals])
        market_meta = context.get_market_regime(trade_date)
        final_signals: list[StrategySignal] = []
        for signal in raw_signals:
            sector_meta = sector_meta_map.get(signal.ts_code, {})
            sector_ok, sector_note = sector_filter_passed(sector_meta)
            market_ok, market_note = market_filter_passed(market_meta)
            if not sector_ok or not market_ok:
                continue
            reason = format_reason(
                signal.score,
                [
                    signal.trace.get("stage_note"),
                    signal.trace.get("volume_note"),
                    signal.trace.get("support_note"),
                    sector_note,
                    market_note,
                ],
            )
            final_signals.append(
                StrategySignal(
                    ts_code=signal.ts_code,
                    name=signal.name,
                    score=signal.score,
                    reason=reason,
                    tags=signal.tags + [sector_note],
                    trace={**signal.trace, "sector": sector_meta, "market": market_meta},
                    entry_price_source=signal.entry_price_source,
                )
            )

        return [item.to_observation(trade_date) for item in sorted(final_signals, key=lambda x: x.score, reverse=True)]

    def _evaluate_stock(self, frame: pd.DataFrame) -> StrategySignal | None:
        if len(frame) < 10:
            return None
        latest = frame.iloc[-1]
        stock_name = str(latest.get("name") or "").strip()
        if not is_basic_eligible(stock_name):
            return None

        profile = infer_market_profile(str(latest.get("ts_code") or ""))
        setup = self._find_setup_indices(frame, profile.limit_up_pct)
        if setup is None:
            return None
        anchor_idx, pivot_idx = setup
        if pivot_idx >= len(frame) - 1:
            return None

        anchor = frame.iloc[anchor_idx]
        pivot = frame.iloc[pivot_idx]
        latest_close = float(latest["close"])
        latest_open = float(latest["open"])
        latest_low = float(latest["low"])
        latest_volume = float(latest["vol"])
        gap = len(frame) - 1 - pivot_idx
        if gap < 5 or gap > 8:
            return None

        pullback = frame.iloc[pivot_idx + 1 :].copy()
        if pullback.empty:
            return None

        score = 0
        pieces: dict[str, str] = {}

        score += 24
        pieces["stage_note"] = f"基准后第 {gap} 日"

        pivot_high = float(pivot["high"])
        anchor_low = float(anchor["low"])
        anchor_open = float(anchor["open"])
        anchor_close = float(anchor["close"])
        post_high = float(pullback["high"].max())
        if post_high <= pivot_high * 1.05:
            score += 12

        pullback_min_low = float(pullback["low"].min())
        if profile.board_key == "main_board":
            body_size = max(anchor_close - anchor_open, 0.0)
            if body_size <= 0:
                return None
            retrace_floor = anchor_close - body_size * 0.5
            if pullback_min_low >= retrace_floor:
                score += 16
                pieces["support_note"] = "回撤守住半实体"
            else:
                return None
        else:
            launch_floor = min(anchor_open, anchor_low)
            if pullback_min_low >= launch_floor * 0.98:
                score += 16
                pieces["support_note"] = "回撤至起涨点附近"
            else:
                return None

        reference_volume = max(float(anchor["vol"]) or 0.0, float(pivot["vol"]) or 0.0)
        shrink_ratio = latest_volume / reference_volume if reference_volume > 0 else 9.9
        if shrink_ratio <= 0.33:
            score += 18
        else:
            return None
        pieces["volume_note"] = f"缩量至 {shrink_ratio:.2f}"

        support_ma = float(latest.get(f"ma{profile.support_ma}") or 0.0)
        if support_ma <= 0:
            return None
        support_distance = abs(latest_close / support_ma - 1.0)
        support_floor = support_ma * (1 - 0.015 * profile.k)
        if support_distance <= 0.035 * profile.k and latest_low >= support_floor:
            score += 12
            pieces["support_note"] = f"贴近 MA{profile.support_ma} 未破位"
        else:
            return None

        body_pct = abs(latest_close / latest_open - 1.0) * 100.0 if latest_open > 0 else 99.0
        amplitude_pct = float(latest.get("amplitude_pct") or 99.0)
        if body_pct <= 2.0 * profile.k and amplitude_pct <= 4.5 * profile.k:
            score += 8
            pieces["stage_note"] = f"基准后第 {gap} 日，小阴小阳止跌"
        else:
            return None

        rps_20 = float(latest.get("rps_20") or 0.0)
        if rps_20 >= 85:
            score += 10
        elif rps_20 >= 70:
            score += 6

        if score < 72:
            return None

        return StrategySignal(
            ts_code=str(latest["ts_code"]),
            name=stock_name,
            score=int(min(score, 100)),
            reason="",
            tags=["头7", profile.board_label],
            trace={
                **pieces,
                "anchor_trade_date": str(anchor.get("trade_date") or ""),
                "pivot_trade_date": str(pivot.get("trade_date") or ""),
            },
            entry_price_source="open_next_trade_day",
        )

    def _find_setup_indices(self, frame: pd.DataFrame, limit_up_pct: float) -> tuple[int, int] | None:
        recent = frame.tail(20).reset_index()
        recent = recent.iloc[:-1]
        limit_hits = recent[
            (recent["pct_chg"] >= limit_up_pct)
            & (recent["close"] >= recent["high"] * 0.998)
            & (recent["vol"] >= recent["vol_ma5"] * 1.3)
        ]
        if not limit_hits.empty:
            anchor_idx = int(limit_hits.iloc[-1]["index"])
            pivot_idx = self._find_pivot_index(frame, anchor_idx)
            return anchor_idx, pivot_idx

        surge_hits = recent[
            (recent["pct_chg"] >= max(limit_up_pct * 0.7, 7.0))
            & (recent["vol"] >= recent["vol_ma5"] * 1.5)
        ]
        if not surge_hits.empty:
            anchor_idx = int(surge_hits.iloc[-1]["index"])
            pivot_idx = self._find_pivot_index(frame, anchor_idx)
            return anchor_idx, pivot_idx
        return None

    def _find_pivot_index(self, frame: pd.DataFrame, anchor_idx: int) -> int:
        phase = frame.iloc[anchor_idx:-1]
        if phase.empty:
            return anchor_idx
        return int(phase["high"].idxmax())

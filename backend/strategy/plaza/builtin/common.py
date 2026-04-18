from __future__ import annotations

from dataclasses import dataclass, field
from math import ceil
from typing import Any

import pandas as pd

from strategy.plaza.base import ObservationCandidate


@dataclass(frozen=True)
class MarketProfile:
    board_key: str
    board_label: str
    limit_up_pct: float
    k: float
    breakout_pct: float
    consolidation_amp_pct: float
    support_ma: int


@dataclass(frozen=True)
class StrategySignal:
    ts_code: str
    name: str
    score: int
    reason: str
    tags: list[str] = field(default_factory=list)
    trace: dict[str, Any] = field(default_factory=dict)
    entry_price_source: str = "open_next_trade_day"

    def to_observation(self, trade_date: str) -> ObservationCandidate:
        return ObservationCandidate(
            ts_code=self.ts_code,
            name=self.name,
            observation_date=trade_date,
            entry_anchor_date=trade_date,
            reason=self.reason,
            tags=self.tags,
            trace=self.trace,
            entry_price_source=self.entry_price_source,
        )


def infer_market_profile(ts_code: str) -> MarketProfile:
    code = str(ts_code or "").strip().upper()
    prefix = code[:3]
    if prefix in {"300", "301", "688"}:
        return MarketProfile(
            board_key="growth_board",
            board_label="创业板/科创板",
            limit_up_pct=19.8,
            k=2.0,
            breakout_pct=12.0,
            consolidation_amp_pct=8.0,
            support_ma=20,
        )
    return MarketProfile(
        board_key="main_board",
        board_label="主板",
        limit_up_pct=9.8,
        k=1.0,
        breakout_pct=7.0,
        consolidation_amp_pct=4.0,
        support_ma=10,
    )


def ensure_analysis_columns(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.copy().sort_values("trade_date").reset_index(drop=True)
    numeric_cols = [
        "open",
        "high",
        "low",
        "close",
        "vol",
        "amount",
        "pct_chg",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "turnover_rate",
        "volume_ratio",
        "rps_20",
        "rps_50",
        "rps_120",
        "trend_score",
        "factor_score",
    ]
    for column in numeric_cols:
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce")
    for window in (5, 10, 20, 60, 120):
        column = f"ma{window}"
        if column not in work.columns:
            work[column] = work["close"].rolling(window, min_periods=1).mean()
    if "vol_ma5" not in work.columns:
        work["vol_ma5"] = work["vol"].rolling(5, min_periods=1).mean()
    if "vol_ma20" not in work.columns:
        work["vol_ma20"] = work["vol"].rolling(20, min_periods=1).mean()
    close = pd.to_numeric(work["close"], errors="coerce")
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    work["dif"] = ema12 - ema26
    work["dea"] = work["dif"].ewm(span=9, adjust=False).mean()
    work["macd_hist"] = (work["dif"] - work["dea"]) * 2
    work["amplitude_pct"] = ((work["high"] / work["low"]) - 1.0) * 100.0
    return work


def is_basic_eligible(name: str) -> bool:
    stock_name = str(name or "").strip().upper()
    if not stock_name:
        return False
    return "ST" not in stock_name and "退" not in stock_name


def sector_filter_passed(sector_meta: dict[str, Any]) -> tuple[bool, str]:
    total = int(sector_meta.get("sector_heat_total") or 0)
    rank = int(sector_meta.get("sector_heat_rank") or 0)
    if total <= 0 or rank <= 0:
        return True, "板块热度暂无可靠排名"
    limit_rank = max(1, ceil(total / 3))
    return rank <= limit_rank, f"板块热度 {rank}/{total}"


def market_filter_passed(market_meta: dict[str, Any]) -> tuple[bool, str]:
    if not market_meta:
        return True, "大盘环境暂无数据"
    ok = bool(market_meta.get("is_supportive"))
    return ok, str(market_meta.get("reason") or "大盘环境过滤")


def format_reason(score: int, pieces: list[str]) -> str:
    core = [str(item).strip() for item in pieces if str(item).strip()]
    body = "；".join(core[:4])
    return f"总分 {score} | {body}" if body else f"总分 {score}"

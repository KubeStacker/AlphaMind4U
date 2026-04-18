from __future__ import annotations

from typing import Any


STATE_ORDER = {
    "A_BREAKOUT_READY": 0,
    "B_PULLBACK_READY": 1,
    "C_TREND_CONTINUE": 2,
    "D_NEUTRAL_WAIT": 3,
    "E_RISK_AVOID": 4,
}


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, float(value)))


def build_watch_recommendation(snapshot: dict[str, Any], levels: dict[str, Any]) -> dict[str, Any]:
    close = float(snapshot.get("close") or 0.0)
    volume_ratio = float(snapshot.get("volume_ratio") or 1.0)
    turnover = float(snapshot.get("turnover") or 0.0)
    net_mf_ratio = float(snapshot.get("net_mf_ratio") or 0.0)
    factor_score = float(snapshot.get("factor_score") or 50.0)
    trend_factor = float(snapshot.get("trend_factor") or 50.0)
    theme_alignment_score = float(snapshot.get("theme_alignment_score") or 50.0)
    support_1 = levels.get("support_1") or {}
    support_2 = levels.get("support_2") or {}
    resistance_1 = levels.get("resistance_1") or {}
    resistance_2 = levels.get("resistance_2") or {}
    support_distance = float(support_1.get("distance_pct") or 99.0)
    resistance_distance = float(resistance_1.get("distance_pct") or 99.0)
    support_strength = float(support_1.get("level_strength_score") or 50.0)
    resistance_strength = float(resistance_1.get("level_strength_score") or 50.0)
    overhead_supply_score = float(levels.get("overhead_supply_score") or 0.0)

    breakout_score = _clamp(
        35.0
        + max(0.0, 3.2 - resistance_distance) * 12.0
        + max(0.0, volume_ratio - 1.0) * 14.0
        + max(0.0, net_mf_ratio) * 2.2
        + max(0.0, trend_factor - 50.0) * 0.45
        + max(0.0, factor_score - 50.0) * 0.30
        - overhead_supply_score * 0.20
    )
    breakout_strength = _clamp(
        30.0
        + max(0.0, turnover - 4.0) * 3.0
        + max(0.0, theme_alignment_score - 50.0) * 0.65
        + max(0.0, resistance_strength - 60.0) * 0.35
        + max(0.0, factor_score - 50.0) * 0.35
    )
    entry_quality_score = _clamp(
        40.0
        + max(0.0, 2.4 - support_distance) * 13.0
        + max(0.0, support_strength - 60.0) * 0.50
        + max(0.0, 6.0 - resistance_distance) * 2.5
    )
    risk_penalty_score = _clamp(
        max(0.0, overhead_supply_score - 10.0) * 1.6
        + (12.0 if resistance_distance < 0.6 else 0.0)
        + max(0.0, 1.0 - volume_ratio) * 22.0
    )

    if breakout_score >= 70.0 and breakout_strength >= 60.0:
        state_bucket = "A_BREAKOUT_READY"
    elif entry_quality_score >= 68.0:
        state_bucket = "B_PULLBACK_READY"
    elif breakout_score >= 55.0:
        state_bucket = "C_TREND_CONTINUE"
    elif risk_penalty_score >= 60.0:
        state_bucket = "E_RISK_AVOID"
    else:
        state_bucket = "D_NEUTRAL_WAIT"

    recommendation_score = _clamp(
        breakout_score * 0.42
        + breakout_strength * 0.18
        + entry_quality_score * 0.30
        + theme_alignment_score * 0.10
        - risk_penalty_score * 0.22
    )
    confirm_price = float(
        resistance_1.get("break_confirm_price") or resistance_1.get("price") or close
    )
    fail_price = float(
        levels.get("fail_threshold_price")
        or support_1.get("fail_threshold_price")
        or support_1.get("price")
        or close
    )
    expected_move_pct = float(resistance_1.get("expected_move_pct") or 0.0)
    reward_risk_ratio = round(
        max(
            0.0,
            (
                (confirm_price - close)
                / max(
                    close - fail_price,
                    0.01,
                )
            ),
        ),
        2,
    )

    return {
        "state_bucket": state_bucket,
        "recommendation_score": round(recommendation_score, 2),
        "breakout": {
            "score": round(breakout_score, 2),
            "strength": round(breakout_strength, 2),
            "confirm_price": round(confirm_price, 2),
            "fail_price": round(fail_price, 2),
            "expected_move_pct": round(expected_move_pct, 2),
        },
        "entry_quality": {
            "score": round(entry_quality_score, 2),
            "reward_risk_ratio": reward_risk_ratio,
        },
        "risk_penalty_score": round(risk_penalty_score, 2),
        "levels": {
            "support_1": support_1,
            "support_2": support_2,
            "resistance_1": resistance_1,
            "resistance_2": resistance_2,
        },
    }


def sort_watch_candidates(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            STATE_ORDER.get(item["recommendation"]["state_bucket"], 99),
            -float(item["recommendation"]["recommendation_score"]),
            -float(item["recommendation"]["breakout"]["score"]),
            -float(item["recommendation"]["entry_quality"]["score"]),
            float(item["recommendation"]["risk_penalty_score"]),
            -float(item.get("amount") or 0.0),
            -float(item.get("pct") or 0.0),
            -float(item.get("volume_ratio") or 0.0),
        ),
    )

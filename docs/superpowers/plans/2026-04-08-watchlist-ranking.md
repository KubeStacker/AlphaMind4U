# Watchlist Auto Ranking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build automatic ranking for watchlist observation rows, upgrade key levels with breakout/failure semantics, and expose breakout/entry/ranking fields through the existing compact watchlist API and UI.

**Architecture:** Keep the existing `/admin/watchlist/realtime?analysis_depth=compact` path as the only observation-list entry point. Extract the new ranking logic into a small pure-Python helper module that scores a normalized stock snapshot plus enhanced structural levels, then let `backend/api/routes/stocks.py` assemble/sort rows and let `frontend/src/views/Watchlist.vue` render the new compact ranking metadata without drag-and-drop.

**Tech Stack:** FastAPI, pandas, DuckDB-backed fetch helpers, existing `kline_patterns.py` structural level engine, Vue 3 Composition API, Axios, pytest, curl.

---

## File Structure

- Create: `backend/strategy/watchlist/__init__.py`
- Create: `backend/strategy/watchlist/recommendation.py`
- Create: `backend/tests/strategy/watchlist/test_recommendation.py`
- Modify: `backend/requirements.txt`
- Modify: `backend/etl/utils/kline_patterns.py`
- Modify: `backend/api/routes/stocks.py`
- Modify: `frontend/src/services/api.js`
- Modify: `frontend/src/views/Watchlist.vue`
- Modify: `AGENTS.md`
- Modify: `docs/README.md`
- Modify: `docs/published/trading-system/README.md`

This split keeps scoring/ranking logic testable and contained. `stocks.py` stays as the API orchestration layer, `kline_patterns.py` remains the source of structural levels, and the frontend only consumes returned metadata.

### Task 1: Add a Testable Watchlist Recommendation Module

**Files:**
- Create: `backend/strategy/watchlist/__init__.py`
- Create: `backend/strategy/watchlist/recommendation.py`
- Create: `backend/tests/strategy/watchlist/test_recommendation.py`
- Modify: `backend/requirements.txt`

- [ ] **Step 1: Write the failing test for breakout-ready vs pullback-ready ranking**

```python
# backend/tests/strategy/watchlist/test_recommendation.py
from strategy.watchlist.recommendation import (
    build_watch_recommendation,
    sort_watch_candidates,
)


def _sample_snapshot(**overrides):
    payload = {
        "ts_code": "300308.SZ",
        "close": 21.40,
        "pct_today": 2.8,
        "volume_ratio": 1.72,
        "turnover": 9.4,
        "net_mf_amount": 8200.0,
        "net_mf_ratio": 3.8,
        "factor_score": 68.0,
        "trend_factor": 71.0,
        "flow_factor": 66.0,
        "quality_factor": 63.0,
        "ma5": 21.05,
        "ma10": 20.72,
        "ma20": 20.18,
        "ma60": 18.94,
        "amount": 156000.0,
        "theme_alignment_score": 72.0,
        "market_regime": "strong",
    }
    payload.update(overrides)
    return payload


def _sample_levels(**overrides):
    payload = {
        "support_1": {"price": 20.95, "level_strength_score": 78.0, "distance_pct": 2.10},
        "support_2": {"price": 20.32, "level_strength_score": 69.0, "distance_pct": 5.05},
        "resistance_1": {
            "price": 21.68,
            "level_strength_score": 81.0,
            "distance_pct": 1.31,
            "break_confirm_price": 21.75,
            "expected_move_pct": 6.40,
        },
        "resistance_2": {"price": 22.48, "level_strength_score": 70.0, "distance_pct": 5.04},
        "fail_threshold_price": 20.78,
        "overhead_supply_score": 24.0,
    }
    payload.update(overrides)
    return payload


def test_breakout_ready_candidate_scores_ahead_of_pullback_candidate():
    breakout_ready = build_watch_recommendation(
        snapshot=_sample_snapshot(),
        levels=_sample_levels(),
    )
    pullback_ready = build_watch_recommendation(
        snapshot=_sample_snapshot(
            ts_code="002463.SZ",
            close=18.12,
            pct_today=0.6,
            volume_ratio=1.08,
            turnover=4.8,
            net_mf_amount=2300.0,
            net_mf_ratio=1.1,
            factor_score=61.0,
            trend_factor=58.0,
            flow_factor=52.0,
            theme_alignment_score=55.0,
            market_regime="range",
        ),
        levels=_sample_levels(
            support_1={"price": 17.98, "level_strength_score": 84.0, "distance_pct": 0.77},
            resistance_1={
                "price": 18.92,
                "level_strength_score": 67.0,
                "distance_pct": 4.42,
                "break_confirm_price": 19.02,
                "expected_move_pct": 4.10,
            },
            fail_threshold_price=17.72,
            overhead_supply_score=18.0,
        ),
    )

    ordered = sort_watch_candidates(
        [
            {"ts_code": "002463.SZ", "recommendation": pullback_ready},
            {"ts_code": "300308.SZ", "recommendation": breakout_ready},
        ]
    )

    assert breakout_ready["state_bucket"] == "A_BREAKOUT_READY"
    assert pullback_ready["state_bucket"] == "B_PULLBACK_READY"
    assert breakout_ready["breakout"]["score"] > pullback_ready["breakout"]["score"]
    assert ordered[0]["ts_code"] == "300308.SZ"
```

- [ ] **Step 2: Run the test to verify it fails for the right reason**

Run:

```bash
cd /root/jarvis/backend && pytest tests/strategy/watchlist/test_recommendation.py -q
```

Expected:

```text
FAIL ... ModuleNotFoundError: No module named 'strategy.watchlist'
```

- [ ] **Step 3: Add pytest and the minimal recommendation module skeleton**

```python
# backend/strategy/watchlist/__init__.py
from .recommendation import build_watch_recommendation, sort_watch_candidates

__all__ = ["build_watch_recommendation", "sort_watch_candidates"]
```

```python
# backend/strategy/watchlist/recommendation.py
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
    resistance_1 = levels.get("resistance_1") or {}
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
        + max(0.0, resistance_distance < 0.6) * 12.0
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
    reward_risk_ratio = round(
        max(
            0.0,
            ((float(resistance_1.get("break_confirm_price") or resistance_1.get("price") or close) - close) /
             max(close - float(levels.get("fail_threshold_price") or support_1.get("price") or close - 0.01), 0.01))
        ),
        2,
    )

    return {
        "state_bucket": state_bucket,
        "recommendation_score": round(recommendation_score, 2),
        "breakout": {
            "score": round(breakout_score, 2),
            "strength": round(breakout_strength, 2),
        },
        "entry_quality": {
            "score": round(entry_quality_score, 2),
            "reward_risk_ratio": reward_risk_ratio,
        },
        "risk_penalty_score": round(risk_penalty_score, 2),
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
        ),
    )
```

```text
# backend/requirements.txt
pytest
```

- [ ] **Step 4: Run the focused test to verify it passes**

Run:

```bash
cd /root/jarvis/backend && pytest tests/strategy/watchlist/test_recommendation.py -q
```

Expected:

```text
1 passed
```

- [ ] **Step 5: Commit the isolated scoring scaffold**

```bash
cd /root/jarvis
git add backend/requirements.txt backend/strategy/watchlist/__init__.py backend/strategy/watchlist/recommendation.py backend/tests/strategy/watchlist/test_recommendation.py
git commit -m "feat: add watchlist recommendation scoring scaffold"
```

### Task 2: Upgrade Structural Levels With Trading Semantics

**Files:**
- Modify: `backend/etl/utils/kline_patterns.py`
- Modify: `backend/tests/strategy/watchlist/test_recommendation.py`

- [ ] **Step 1: Extend the failing test to require breakout confirmation and failure thresholds**

```python
def test_breakout_payload_exposes_confirmation_and_failure_levels():
    recommendation = build_watch_recommendation(
        snapshot=_sample_snapshot(),
        levels=_sample_levels(),
    )

    assert recommendation["breakout"]["confirm_price"] == 21.75
    assert recommendation["breakout"]["fail_price"] == 20.78
    assert recommendation["breakout"]["expected_move_pct"] == 6.4
    assert recommendation["levels"]["support_1"]["level_strength_score"] == 78.0
```

- [ ] **Step 2: Run the test to verify it fails on missing keys**

Run:

```bash
cd /root/jarvis/backend && pytest tests/strategy/watchlist/test_recommendation.py::test_breakout_payload_exposes_confirmation_and_failure_levels -q
```

Expected:

```text
FAIL ... KeyError: 'confirm_price'
```

- [ ] **Step 3: Enhance the level output contract and recommendation payload**

```python
# backend/etl/utils/kline_patterns.py
def _augment_selected_level(
    item: dict[str, Any],
    close: float,
    selection_gap: float,
    level_role: str,
    fail_threshold_price: float | None = None,
) -> dict[str, Any]:
    price = float(item.get("price") or 0.0)
    distance_pct = round(abs(price - close) / close * 100.0, 2) if close > 0 and price > 0 else None
    base_strength = float(item.get("strength_score") or 0.0)
    source_count = len(item.get("sources") or [item.get("source")]) if item.get("source") or item.get("sources") else 0
    touch_count = int(item.get("touch_count") or 0)
    expected_move_pct = round(max(selection_gap / max(close, 0.01) * 100.0 * 2.2, 1.2), 2) if close > 0 else None
    confirm_price = round(price + selection_gap * 0.25, 2) if level_role == "resistance" and price > 0 else None
    fail_price = round(float(fail_threshold_price if fail_threshold_price is not None else price - selection_gap * 0.25), 2) if level_role == "support" and price > 0 else None
    item["level_role"] = level_role
    item["level_strength_score"] = round(max(base_strength, 0.0), 2)
    item["distance_pct"] = distance_pct
    item["source_resonance"] = source_count
    item["touch_quality"] = round(min(max(touch_count * 12.0, 0.0), 100.0), 2)
    item["freshness_score"] = round(max(0.0, 100.0 - float(item.get("age", 0.0)) * 2.5), 2)
    item["break_confirm_price"] = confirm_price
    item["fail_threshold_price"] = fail_price
    item["expected_move_pct"] = expected_move_pct
    return item
```

```python
# backend/strategy/watchlist/recommendation.py
def build_watch_recommendation(snapshot: dict[str, Any], levels: dict[str, Any]) -> dict[str, Any]:
    ...
    confirm_price = float(resistance_1.get("break_confirm_price") or resistance_1.get("price") or close)
    fail_price = float(levels.get("fail_threshold_price") or support_1.get("fail_threshold_price") or support_1.get("price") or close)
    expected_move_pct = float(resistance_1.get("expected_move_pct") or 0.0)
    ...
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
            "support_2": levels.get("support_2") or {},
            "resistance_1": resistance_1,
            "resistance_2": levels.get("resistance_2") or {},
        },
    }
```

- [ ] **Step 4: Re-run the focused tests to verify they pass**

Run:

```bash
cd /root/jarvis/backend && pytest tests/strategy/watchlist/test_recommendation.py -q
```

Expected:

```text
2 passed
```

- [ ] **Step 5: Commit the level contract upgrade**

```bash
cd /root/jarvis
git add backend/etl/utils/kline_patterns.py backend/strategy/watchlist/recommendation.py backend/tests/strategy/watchlist/test_recommendation.py
git commit -m "feat: enrich watchlist structural level metadata"
```

### Task 3: Wire Recommendation Fields Into Compact Analysis and API Sorting

**Files:**
- Modify: `backend/api/routes/stocks.py`
- Modify: `backend/tests/strategy/watchlist/test_recommendation.py`

- [ ] **Step 1: Write the failing test for liquidity-based tie breaking**

```python
def test_sort_watch_candidates_prefers_higher_liquidity_when_primary_scores_tie():
    liquid = {
        "ts_code": "300502.SZ",
        "recommendation": {
            "state_bucket": "A_BREAKOUT_READY",
            "recommendation_score": 78.4,
            "breakout": {"score": 79.0},
            "entry_quality": {"score": 66.0},
            "risk_penalty_score": 18.0,
        },
        "amount": 220000.0,
        "pct": 3.4,
        "volume_ratio": 1.82,
    }
    thin = {
        "ts_code": "601138.SH",
        "recommendation": {
            "state_bucket": "A_BREAKOUT_READY",
            "recommendation_score": 78.4,
            "breakout": {"score": 79.0},
            "entry_quality": {"score": 66.0},
            "risk_penalty_score": 18.0,
        },
        "amount": 91000.0,
        "pct": 3.4,
        "volume_ratio": 1.20,
    }

    ordered = sort_watch_candidates([thin, liquid])

    assert ordered[0]["ts_code"] == "300502.SZ"
```

- [ ] **Step 2: Run the focused tests to confirm the current helper output is insufficient**

Run:

```bash
cd /root/jarvis/backend && pytest tests/strategy/watchlist/test_recommendation.py -q
```

Expected:

```text
FAIL ... AssertionError: assert '601138.SH' == '300502.SZ'
```

- [ ] **Step 3: Integrate the helper into compact analysis and auto-sort observation rows**

```python
# backend/api/routes/stocks.py
from strategy.watchlist.recommendation import build_watch_recommendation, sort_watch_candidates
```

```python
# backend/api/routes/stocks.py function signature
async def get_watchlist_realtime(
    request: Request,
    codes: Optional[str] = None,
    src: str = "sina",
    include_analysis: bool = True,
    analysis_depth: str = "full",
    sort_mode: str = "auto",
):
```

```python
# backend/api/routes/stocks.py inside _build_compact_watch_analysis()
level_bundle = build_structural_price_levels(work, top_n=2)
support_levels = list(level_bundle.get("support_levels") or [])
resistance_levels = list(level_bundle.get("resistance_levels") or [])
support_1 = support_levels[0] if support_levels else {}
support_2 = support_levels[1] if len(support_levels) > 1 else {}
resistance_1 = resistance_levels[0] if resistance_levels else {}
resistance_2 = resistance_levels[1] if len(resistance_levels) > 1 else {}
recommendation = build_watch_recommendation(
    snapshot={
        "ts_code": ts_code,
        "close": close,
        "pct_today": pct_today,
        "volume_ratio": volume_ratio,
        "turnover": turnover,
        "net_mf_amount": net_mf_amount,
        "net_mf_ratio": net_mf_ratio,
        "factor_score": factor_score,
        "trend_factor": trend_factor,
        "flow_factor": flow_factor,
        "quality_factor": quality_factor,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
        "amount": amount,
        "theme_alignment_score": _safe_float(merged.get("theme_alignment_score"), factor_score or 50.0),
        "market_regime": "strong" if close >= ma20 >= ma60 else "range" if close >= ma20 else "weak",
    },
    levels={
        "support_1": support_1,
        "support_2": support_2,
        "resistance_1": resistance_1,
        "resistance_2": resistance_2,
        "fail_threshold_price": support_1.get("fail_threshold_price") if support_1 else None,
        "overhead_supply_score": _safe_float(resistance_1.get("source_resonance"), 0.0) * 8.0 if resistance_1 else 0.0,
    },
)
```

```python
# backend/api/routes/stocks.py detail payload additions
"decision": {
    "score": round(score, 1),
    "bias": "bullish" if signal_color == "buy" else "bearish" if signal_color == "sell" else "neutral",
    "action": action,
    "confidence": "high" if score >= 72 or score <= 32 else "medium",
    "style": "realtime_compact",
    "summary": "",
    "state_bucket": recommendation["state_bucket"],
    "recommendation_score": recommendation["recommendation_score"],
},
"breakout": {
    **recommendation["breakout"],
    "status": "逼近确认" if recommendation["state_bucket"] == "A_BREAKOUT_READY" else "待突破",
},
"entry_quality": recommendation["entry_quality"],
"ranking": {
    "rank_reason": "放量预热 + 靠近突破位" if recommendation["state_bucket"] == "A_BREAKOUT_READY" else "支撑共振 + 盈亏比优" if recommendation["state_bucket"] == "B_PULLBACK_READY" else "趋势延续但位置一般" if recommendation["state_bucket"] == "C_TREND_CONTINUE" else "普通观察" if recommendation["state_bucket"] == "D_NEUTRAL_WAIT" else "冲高失败风险偏高",
    "sort_key": recommendation["recommendation_score"],
},
"risk_penalty_score": recommendation["risk_penalty_score"],
```

```python
# backend/api/routes/stocks.py inside get_watchlist_realtime(), after rows are assembled
if analysis_depth == "compact" and sort_mode == "auto":
    sortable_rows = []
    for row in rows:
        detail = ((row.get("analyze") or {}).get("detail") or {})
        decision = detail.get("decision") or {}
        breakout = detail.get("breakout") or {}
        entry_quality = detail.get("entry_quality") or {}
        sortable_rows.append(
            {
                "ts_code": row["ts_code"],
                "row": row,
                "amount": float(row.get("amount") or 0.0),
                "pct": float(row.get("pct") or 0.0),
                "volume_ratio": float(row.get("volume_ratio") or 0.0),
                "recommendation": {
                    "state_bucket": decision.get("state_bucket", "D_NEUTRAL_WAIT"),
                    "recommendation_score": float(decision.get("recommendation_score") or 0.0),
                    "breakout": {"score": float(breakout.get("score") or 0.0)},
                    "entry_quality": {"score": float(entry_quality.get("score") or 0.0)},
                    "risk_penalty_score": float(detail.get("risk_penalty_score") or 0.0),
                },
            }
        )
    rows = [item["row"] for item in sort_watch_candidates(sortable_rows)]
```

```python
# backend/strategy/watchlist/recommendation.py
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
```

- [ ] **Step 4: Run tests and API verification**

Run:

```bash
cd /root/jarvis/backend && pytest tests/strategy/watchlist/test_recommendation.py -q
curl -s "http://localhost:8000/admin/watchlist/realtime?analysis_depth=compact" -H "Authorization: Bearer <TOKEN>"
```

Expected:

```text
pytest: all tests in backend/tests/strategy/watchlist/test_recommendation.py pass
curl: each observation row returns detail.decision.state_bucket, detail.decision.recommendation_score, detail.breakout.*, detail.entry_quality.*, detail.ranking.*, and observation rows are returned in recommendation order
```

- [ ] **Step 5: Commit the backend compact-analysis integration**

```bash
cd /root/jarvis
git add backend/api/routes/stocks.py backend/strategy/watchlist/recommendation.py backend/tests/strategy/watchlist/test_recommendation.py
git commit -m "feat: auto-rank watchlist compact analysis"
```

### Task 4: Switch the Frontend Observation List to Automatic Ranking

**Files:**
- Modify: `frontend/src/services/api.js`
- Modify: `frontend/src/views/Watchlist.vue`

- [ ] **Step 1: Capture the pre-change frontend surface that must be removed**

Run:

```bash
rg -n "drag|reorderWatchlist|拖拽可调整顺序|cursor-grab" /root/jarvis/frontend/src/views/Watchlist.vue /root/jarvis/frontend/src/services/api.js
```

Expected:

```text
Matches for drag handlers, reorderWatchlist import/call, and the drag hint copy
```

- [ ] **Step 2: Remove manual ordering and render automatic ranking fields**

```javascript
// frontend/src/services/api.js
export const getWatchlistRealtime = (codes, src = 'sina', analysisDepth = 'compact', sortMode = 'auto') =>
  apiClient.get('/admin/watchlist/realtime', { params: { codes, src, analysis_depth: analysisDepth, sort_mode: sortMode } });
```

```javascript
// frontend/src/views/Watchlist.vue
import { getWatchlistRealtime, getWatchlistAnalysis, addToWatchlist, removeFromWatchlist, getStockKline, getUserHoldings, updateUserHolding, deleteUserHolding, analyzeStockWithAI, parseHoldingsImage, batchUpdateUserHoldings } from '@/services/api';
```

```javascript
// frontend/src/views/Watchlist.vue
const getRecommendationScore = (item) => Number(item?.analyze?.detail?.decision?.recommendation_score || 0);
const getStateBucket = (item) => String(item?.analyze?.detail?.decision?.state_bucket || 'D_NEUTRAL_WAIT');
const getRankReason = (item) => String(item?.analyze?.detail?.ranking?.rank_reason || '').trim();
const getBreakoutScore = (item) => Number(item?.analyze?.detail?.breakout?.score || 0);
const getBreakoutStrength = (item) => Number(item?.analyze?.detail?.breakout?.strength || 0);
const getBreakConfirmPrice = (item) => Number(item?.analyze?.detail?.breakout?.confirm_price || NaN);
const getFailPrice = (item) => Number(item?.analyze?.detail?.breakout?.fail_price || NaN);
const getEntryQualityScore = (item) => Number(item?.analyze?.detail?.entry_quality?.score || 0);
const stateBucketLabelMap = {
  A_BREAKOUT_READY: '突破候选',
  B_PULLBACK_READY: '回踩候选',
  C_TREND_CONTINUE: '趋势跟踪',
  D_NEUTRAL_WAIT: '普通观察',
  E_RISK_AVOID: '风险回避',
};
const getStateBucketLabel = (item) => stateBucketLabelMap[getStateBucket(item)] || '普通观察';
```

```javascript
// frontend/src/views/Watchlist.vue
const observationRows = computed(() => rows.value.filter(item => !hasHolding(item.ts_code)));
```

```vue
<!-- frontend/src/views/Watchlist.vue -->
<div class="flex items-center gap-2">
  <span class="chip chip-neutral">{{ filteredObservationRows.length }}<span class="text-slate-600">/{{ observationRows.length }}</span></span>
  <span class="text-[9px] text-slate-700">自动排序</span>
</div>
```

```vue
<!-- frontend/src/views/Watchlist.vue -->
<div class="mt-1.5 flex flex-wrap items-center gap-1.5">
  <span class="chip chip-data">推荐 {{ fmt(getRecommendationScore(item), 1) }}</span>
  <span class="chip" :class="watchSignalClass(item)">{{ getStateBucketLabel(item) }}</span>
  <span class="chip chip-data">突破 {{ fmt(getBreakoutScore(item), 0) }}/{{ fmt(getBreakoutStrength(item), 0) }}</span>
  <span class="chip chip-data">位置 {{ fmt(getEntryQualityScore(item), 0) }}</span>
</div>
<p v-if="getRankReason(item)" class="mt-1 text-[10px] leading-4 text-slate-400">{{ getRankReason(item) }}</p>
<div class="mt-1.5 flex flex-wrap gap-1">
  <span v-if="Number.isFinite(getBreakConfirmPrice(item))" class="chip chip-resistance">破 {{ fmt(getBreakConfirmPrice(item), 2) }}</span>
  <span v-if="Number.isFinite(getFailPrice(item))" class="chip chip-support">失 {{ fmt(getFailPrice(item), 2) }}</span>
</div>
```

- [ ] **Step 3: Verify the UI contract with live API data**

Run:

```bash
curl -s "http://localhost:8000/admin/watchlist/realtime?analysis_depth=compact&sort_mode=auto" -H "Authorization: Bearer <TOKEN>"
```

Expected:

```text
Rows return the ranking metadata needed by Watchlist.vue, and the frontend no longer depends on drag/reorder fields for observation ordering
```

- [ ] **Step 4: Commit the frontend automatic-ordering switch**

```bash
cd /root/jarvis
git add frontend/src/services/api.js frontend/src/views/Watchlist.vue
git commit -m "feat: show auto-ranked watchlist observations"
```

### Task 5: Update Documentation and End-to-End Verification

**Files:**
- Modify: `AGENTS.md`
- Modify: `docs/README.md`
- Modify: `docs/published/trading-system/README.md`

- [ ] **Step 1: Write the failing documentation diff by locating outdated manual-order wording**

Run:

```bash
rg -n "拖拽|reorder|sort_order|观察列表" /root/jarvis/AGENTS.md /root/jarvis/docs/README.md /root/jarvis/docs/published/trading-system/README.md
```

Expected:

```text
Existing wording still implies observation ordering is user-maintained or does not mention automatic ranking/breakout fields
```

- [ ] **Step 2: Update the docs to match the shipped behavior**

```markdown
# AGENTS.md and docs/README.md / docs/published/trading-system/README.md
- Watchlist 观察列表当前取消人工拖拽排序，统一按后端 `recommendation_score + state_bucket` 自动排序；首屏优先展示突破候选、回踩候选和风险回避状态。
- `/admin/watchlist/realtime?analysis_depth=compact` 现在额外返回 `detail.decision.state_bucket`、`detail.decision.recommendation_score`、`detail.breakout.*`、`detail.entry_quality.*`、`detail.ranking.*`，供观察列表首屏直接展示推荐分、突破强度、突破确认价和失效价。
- `watchlist.sort_order` 仅保留兼容旧数据，不再作为观察列表主排序依据。
```

- [ ] **Step 3: Run end-to-end API verification after the docs-aligned implementation**

Run:

```bash
curl -s "http://localhost:8000/admin/watchlist/realtime?analysis_depth=compact&sort_mode=auto" -H "Authorization: Bearer <TOKEN>"
curl -s "http://localhost:8000/admin/watchlist/300308.SZ/analysis" -H "Authorization: Bearer <TOKEN>"
```

Expected:

```text
compact 接口返回自动排序后的观察列表，单票详情仍能正常打开并展示增强点位与突破字段
```

- [ ] **Step 4: Run final targeted verification**

Run:

```bash
cd /root/jarvis/backend && pytest tests/strategy/watchlist/test_recommendation.py -q
git status --short
```

Expected:

```text
pytest: all watchlist recommendation tests pass
git status: only the intended implementation files remain modified before the final commit
```

- [ ] **Step 5: Commit docs and verification-aligned cleanup**

```bash
cd /root/jarvis
git add AGENTS.md docs/README.md docs/published/trading-system/README.md
git commit -m "docs: document watchlist auto ranking"
```

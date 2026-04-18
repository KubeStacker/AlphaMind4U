# Strategy Plaza Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new first-level `策略广场` surface that lists globally shared strategy observations by date, archives their 3/5/10-day follow-up results, and supports backend-coded staged strategies plus manual or scheduled execution.

**Architecture:** Keep strategy logic in a new `backend/strategy/plaza/` plugin area where each strategy computes only its final observation entries for a trade date. The platform layer owns storage, backtest metric completion, summaries, task dispatch, and API delivery. The frontend adds a lightweight `/strategies` page plus nav entry, and it only reads archived results instead of running strategies live.

**Tech Stack:** FastAPI, DuckDB shared connection helpers, APScheduler, existing ETL task queue, Python `unittest`, Vue 3 Composition API, Axios, Node `node:test`, curl.

---

## File Structure

- Create: `backend/strategy/plaza/__init__.py`
- Create: `backend/strategy/plaza/base.py`
- Create: `backend/strategy/plaza/registry.py`
- Create: `backend/strategy/plaza/summarizer.py`
- Create: `backend/strategy/plaza/service.py`
- Create: `backend/strategy/plaza/builtin/__init__.py`
- Create: `backend/api/routes/strategy_plaza.py`
- Create: `backend/tests/strategy/plaza/__init__.py`
- Create: `backend/tests/strategy/plaza/test_service.py`
- Create: `backend/tests/api/routes/test_strategy_plaza.py`
- Create: `frontend/src/views/Strategies.vue`
- Create: `frontend/src/composables/useStrategyPlaza.js`
- Create: `frontend/tests/strategy-plaza.test.mjs`
- Modify: `backend/db/schema.py`
- Modify: `backend/api/routes/system.py`
- Modify: `backend/api/routes/__init__.py`
- Modify: `backend/main.py`
- Modify: `backend/api/routes/etl.py`
- Modify: `backend/etl/sync.py`
- Modify: `backend/etl/scheduler.py`
- Modify: `frontend/src/router/index.js`
- Modify: `frontend/src/components/AppLayout.vue`
- Modify: `frontend/src/services/api.js`
- Modify: `AGENTS.md`
- Modify: `docs/README.md`
- Modify: `docs/published/trading-system/README.md`

This split keeps staged strategy code isolated from API orchestration and UI concerns. The backend can stay empty-strategy-safe, while the frontend can ship a stable page that works whether there are zero strategies or many.

### Task 1: Add Strategy Plaza Storage and Pure Backend Helpers

**Files:**
- Create: `backend/strategy/plaza/__init__.py`
- Create: `backend/strategy/plaza/base.py`
- Create: `backend/strategy/plaza/registry.py`
- Create: `backend/strategy/plaza/summarizer.py`
- Create: `backend/strategy/plaza/builtin/__init__.py`
- Create: `backend/tests/strategy/plaza/__init__.py`
- Create: `backend/tests/strategy/plaza/test_service.py`
- Modify: `backend/db/schema.py`
- Modify: `backend/api/routes/system.py`

- [ ] **Step 1: Write the failing tests for the empty registry and summary text helper**

```python
# backend/tests/strategy/plaza/test_service.py
import unittest

from strategy.plaza.registry import list_registered_strategies
from strategy.plaza.summarizer import build_strategy_summary_text


class StrategyPlazaCoreTests(unittest.TestCase):
    def test_registry_defaults_to_empty_list_when_no_builtins_exist(self):
        self.assertEqual([], list_registered_strategies())

    def test_build_strategy_summary_text_mentions_win_rate_mean_and_drawdown(self):
        summary = build_strategy_summary_text(
            {
                "observation_count": 12,
                "completed_count_5d": 10,
                "win_rate_5d": 60.0,
                "avg_ret_5d": 3.2,
                "avg_max_drawdown_5d": -1.8,
            }
        )

        self.assertIn("12", summary)
        self.assertIn("5日胜率 60.0%", summary)
        self.assertIn("5日均值 +3.20%", summary)
        self.assertIn("回撤 -1.80%", summary)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the test to verify it fails for the right reason**

Run:

```bash
cd /root/jarvis/backend && python -m unittest tests/strategy/plaza/test_service.py
```

Expected:

```text
FAILED ... ModuleNotFoundError: No module named 'strategy.plaza'
```

- [ ] **Step 3: Write the minimal backend plaza foundation and schema constants**

```python
# backend/strategy/plaza/base.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass(frozen=True)
class StrategyMeta:
    strategy_key: str
    name: str
    description: str = ""
    enabled: bool = True
    display_order: int = 100
    engine_version: str = "v1"


@dataclass(frozen=True)
class ObservationCandidate:
    ts_code: str
    name: str
    observation_date: str
    entry_anchor_date: str
    reason: str
    tags: list[str] = field(default_factory=list)
    trace: dict[str, Any] = field(default_factory=dict)
    entry_price_source: str = "close_on_anchor"


class PlazaStrategy(Protocol):
    def meta(self) -> StrategyMeta: ...
    def run_for_date(self, trade_date: str, context: Any) -> list[ObservationCandidate]: ...
```

```python
# backend/strategy/plaza/builtin/__init__.py
BUILTIN_STRATEGIES = ()
```

```python
# backend/strategy/plaza/registry.py
from __future__ import annotations

from strategy.plaza.base import PlazaStrategy
from strategy.plaza.builtin import BUILTIN_STRATEGIES


def list_registered_strategies() -> list[PlazaStrategy]:
    return [strategy for strategy in BUILTIN_STRATEGIES]


def list_enabled_strategies() -> list[PlazaStrategy]:
    return [strategy for strategy in list_registered_strategies() if strategy.meta().enabled]
```

```python
# backend/strategy/plaza/summarizer.py
from __future__ import annotations


def build_strategy_summary_text(summary: dict) -> str:
    observation_count = int(summary.get("observation_count") or 0)
    completed_5d = int(summary.get("completed_count_5d") or 0)
    win_rate_5d = float(summary.get("win_rate_5d") or 0.0)
    avg_ret_5d = float(summary.get("avg_ret_5d") or 0.0)
    avg_drawdown_5d = float(summary.get("avg_max_drawdown_5d") or 0.0)

    if observation_count <= 0:
        return "暂无观察样本。"
    if completed_5d <= 0:
        return f"近窗共 {observation_count} 条观察，5日回测尚未补齐。"

    sign = "+" if avg_ret_5d >= 0 else ""
    return (
        f"近窗共 {observation_count} 条观察，5日完成 {completed_5d} 条，"
        f"5日胜率 {win_rate_5d:.1f}%，5日均值 {sign}{avg_ret_5d:.2f}%，"
        f"回撤 {avg_drawdown_5d:.2f}%。"
    )
```

```python
# backend/strategy/plaza/__init__.py
from .base import ObservationCandidate, PlazaStrategy, StrategyMeta
from .registry import list_enabled_strategies, list_registered_strategies
from .summarizer import build_strategy_summary_text

__all__ = [
    "ObservationCandidate",
    "PlazaStrategy",
    "StrategyMeta",
    "build_strategy_summary_text",
    "list_enabled_strategies",
    "list_registered_strategies",
]
```

```python
# backend/db/schema.py
CREATE_STRATEGY_DEFINITIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_definitions (
    strategy_key     VARCHAR(100) PRIMARY KEY,
    name             VARCHAR(100) NOT NULL,
    description      TEXT,
    enabled          BOOLEAN DEFAULT TRUE,
    display_order    INTEGER DEFAULT 100,
    engine_version   VARCHAR(50) DEFAULT 'v1',
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_STRATEGY_OBSERVATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_observations (
    strategy_key      VARCHAR(100) NOT NULL,
    trade_date        DATE NOT NULL,
    observation_date  DATE NOT NULL,
    ts_code           VARCHAR(15) NOT NULL,
    name              VARCHAR(50),
    reason            VARCHAR(255),
    tags_json         JSON,
    entry_anchor_date DATE NOT NULL,
    trace_json        JSON,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (strategy_key, observation_date, ts_code)
);
"""

CREATE_STRATEGY_BACKTEST_RUNS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_backtest_runs (
    strategy_key            VARCHAR(100) NOT NULL,
    observation_date        DATE NOT NULL,
    ts_code                 VARCHAR(15) NOT NULL,
    entry_anchor_date       DATE NOT NULL,
    entry_price             DOUBLE,
    entry_price_source      VARCHAR(50) DEFAULT 'close_on_anchor',
    status                  VARCHAR(20) DEFAULT 'PENDING',
    ret_3d                  DOUBLE,
    ret_5d                  DOUBLE,
    ret_10d                 DOUBLE,
    max_gain_3d             DOUBLE,
    max_gain_5d             DOUBLE,
    max_gain_10d            DOUBLE,
    max_drawdown_3d         DOUBLE,
    max_drawdown_5d         DOUBLE,
    max_drawdown_10d        DOUBLE,
    last_completed_horizon  INTEGER DEFAULT 0,
    last_eval_date          DATE,
    error                   TEXT,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (strategy_key, observation_date, ts_code)
);
"""

CREATE_STRATEGY_DAILY_SUMMARIES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS strategy_daily_summaries (
    strategy_key             VARCHAR(100) NOT NULL,
    trade_date               DATE NOT NULL,
    window_trade_days        INTEGER DEFAULT 120,
    observation_count        INTEGER DEFAULT 0,
    completed_count_3d       INTEGER DEFAULT 0,
    completed_count_5d       INTEGER DEFAULT 0,
    completed_count_10d      INTEGER DEFAULT 0,
    win_rate_3d              DOUBLE,
    win_rate_5d              DOUBLE,
    win_rate_10d             DOUBLE,
    avg_ret_3d               DOUBLE,
    avg_ret_5d               DOUBLE,
    avg_ret_10d              DOUBLE,
    median_ret_3d            DOUBLE,
    median_ret_5d            DOUBLE,
    median_ret_10d           DOUBLE,
    avg_max_gain_3d          DOUBLE,
    avg_max_gain_5d          DOUBLE,
    avg_max_gain_10d         DOUBLE,
    avg_max_drawdown_3d      DOUBLE,
    avg_max_drawdown_5d      DOUBLE,
    avg_max_drawdown_10d     DOUBLE,
    summary_text             TEXT,
    updated_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (strategy_key, trade_date)
);
"""

# append to ALL_TABLES_SQL
CREATE_STRATEGY_DEFINITIONS_TABLE_SQL,
CREATE_STRATEGY_OBSERVATIONS_TABLE_SQL,
CREATE_STRATEGY_BACKTEST_RUNS_TABLE_SQL,
CREATE_STRATEGY_DAILY_SUMMARIES_TABLE_SQL,
"CREATE INDEX IF NOT EXISTS idx_strategy_observations_date ON strategy_observations (observation_date);",
"CREATE INDEX IF NOT EXISTS idx_strategy_observations_key ON strategy_observations (strategy_key);",
"CREATE INDEX IF NOT EXISTS idx_strategy_backtest_status ON strategy_backtest_runs (status);",
"CREATE INDEX IF NOT EXISTS idx_strategy_summary_date ON strategy_daily_summaries (trade_date);",
```

```python
# backend/api/routes/system.py
from db.schema import (
    CREATE_STRATEGY_BACKTEST_RUNS_TABLE_SQL,
    CREATE_STRATEGY_DAILY_SUMMARIES_TABLE_SQL,
    CREATE_STRATEGY_DEFINITIONS_TABLE_SQL,
    CREATE_STRATEGY_OBSERVATIONS_TABLE_SQL,
)

# add to create_missing_tables()
("strategy_definitions", CREATE_STRATEGY_DEFINITIONS_TABLE_SQL),
("strategy_observations", CREATE_STRATEGY_OBSERVATIONS_TABLE_SQL),
("strategy_backtest_runs", CREATE_STRATEGY_BACKTEST_RUNS_TABLE_SQL),
("strategy_daily_summaries", CREATE_STRATEGY_DAILY_SUMMARIES_TABLE_SQL),

# add to get_data_dashboard() table_queries
"strategy_definitions": {"date_col": "updated_at", "label": "策略定义"},
"strategy_observations": {"date_col": "observation_date", "label": "策略观察归档"},
"strategy_backtest_runs": {"date_col": "last_eval_date", "label": "策略回测结果"},
"strategy_daily_summaries": {"date_col": "trade_date", "label": "策略回测摘要"},
```

- [ ] **Step 4: Run the core test again**

Run:

```bash
cd /root/jarvis/backend && python -m unittest tests/strategy/plaza/test_service.py
```

Expected:

```text
..
----------------------------------------------------------------------
Ran 2 tests in ...

OK
```

- [ ] **Step 5: Commit the storage and helper foundation**

```bash
git add backend/strategy/plaza backend/tests/strategy/plaza/test_service.py backend/db/schema.py backend/api/routes/system.py
git commit -m "feat: scaffold strategy plaza storage and helpers"
```

### Task 2: Implement the Strategy Plaza Service and Backtest Metric Engine

**Files:**
- Modify: `backend/strategy/plaza/__init__.py`
- Create: `backend/strategy/plaza/service.py`
- Modify: `backend/tests/strategy/plaza/test_service.py`

- [ ] **Step 1: Extend the failing tests to cover horizon metric calculation and empty-strategy-safe execution**

```python
# append to backend/tests/strategy/plaza/test_service.py
from unittest.mock import patch

import pandas as pd

from strategy.plaza.base import ObservationCandidate, StrategyMeta
from strategy.plaza.service import StrategyPlazaService, build_horizon_metrics


class _FakeStrategy:
    def meta(self):
        return StrategyMeta(
            strategy_key="demo_strategy",
            name="Demo Strategy",
            description="demo",
            enabled=True,
            display_order=10,
            engine_version="v1",
        )

    def run_for_date(self, trade_date: str, context):
        return [
            ObservationCandidate(
                ts_code="300308.SZ",
                name="中际旭创",
                observation_date=trade_date,
                entry_anchor_date=trade_date,
                reason="示例观察",
                tags=["demo"],
                trace={"stage": "final"},
                entry_price_source="close_on_anchor",
            )
        ]


class StrategyPlazaServiceTests(unittest.TestCase):
    def test_build_horizon_metrics_uses_entry_price_and_future_closes(self):
        price_df = pd.DataFrame(
            [
                {"trade_date": "2026-04-01", "close": 10.0, "high": 10.2, "low": 9.8},
                {"trade_date": "2026-04-02", "close": 10.8, "high": 11.0, "low": 10.1},
                {"trade_date": "2026-04-03", "close": 11.2, "high": 11.4, "low": 10.6},
                {"trade_date": "2026-04-06", "close": 11.5, "high": 11.6, "low": 10.9},
            ]
        )

        metrics = build_horizon_metrics(price_df=price_df, entry_price=10.0, horizon=3)

        self.assertAlmostEqual(15.0, metrics["ret_pct"], places=4)
        self.assertAlmostEqual(16.0, metrics["max_gain_pct"], places=4)
        self.assertAlmostEqual(-2.0, metrics["max_drawdown_pct"], places=4)

    @patch("strategy.plaza.service.list_enabled_strategies", return_value=[])
    def test_run_for_date_returns_zero_counts_when_no_strategies_are_enabled(self, _mocked):
        service = StrategyPlazaService()

        result = service.run_for_date("2026-04-08")

        self.assertEqual(0, result["strategy_count"])
        self.assertEqual(0, result["observation_count"])

    @patch("strategy.plaza.service.list_enabled_strategies", return_value=[_FakeStrategy()])
    @patch.object(StrategyPlazaService, "_persist_strategy_rows", return_value=1)
    @patch.object(StrategyPlazaService, "_refresh_strategy_summary", return_value=None)
    @patch.object(StrategyPlazaService, "complete_pending_backtests", return_value=0)
    def test_run_for_date_persists_rows_for_each_enabled_strategy(self, _pending, _summary, _persist, _strategies):
        service = StrategyPlazaService()

        result = service.run_for_date("2026-04-08")

        self.assertEqual(1, result["strategy_count"])
        self.assertEqual(1, result["observation_count"])
```

- [ ] **Step 2: Run the tests to verify the missing service fails**

Run:

```bash
cd /root/jarvis/backend && python -m unittest tests/strategy/plaza/test_service.py
```

Expected:

```text
FAILED ... ModuleNotFoundError: No module named 'strategy.plaza.service'
```

- [ ] **Step 3: Implement the service layer and backtest helper**

```python
# backend/strategy/plaza/service.py
from __future__ import annotations

import json
from dataclasses import asdict

import arrow
import pandas as pd

from db.connection import fetch_df, get_db_connection
from etl.calendar import trading_calendar
from strategy.plaza.base import ObservationCandidate
from strategy.plaza.registry import list_enabled_strategies, list_registered_strategies
from strategy.plaza.summarizer import build_strategy_summary_text


def _shift_trade_date(date_str: str, offset: int) -> str:
    current = arrow.get(date_str)
    moved = 0
    while moved < offset:
        current = current.shift(days=1)
        if trading_calendar.is_trading_day(current.date()):
            moved += 1
    return current.format("YYYY-MM-DD")


def build_horizon_metrics(price_df: pd.DataFrame, entry_price: float, horizon: int) -> dict:
    if price_df.empty or len(price_df) < horizon + 1 or not entry_price:
        return {}

    window = price_df.iloc[: horizon + 1].copy()
    exit_close = float(window.iloc[-1]["close"])
    max_high = float(window["high"].max())
    min_low = float(window["low"].min())
    return {
        "ret_pct": round((exit_close / entry_price - 1.0) * 100.0, 4),
        "max_gain_pct": round((max_high / entry_price - 1.0) * 100.0, 4),
        "max_drawdown_pct": round((min_low / entry_price - 1.0) * 100.0, 4),
    }


class StrategyPlazaService:
    def sync_definitions(self) -> list[dict]:
        rows = []
        with get_db_connection() as con:
            for strategy in list_registered_strategies():
                meta = strategy.meta()
                con.execute(
                    """
                    INSERT OR REPLACE INTO strategy_definitions (
                        strategy_key, name, description, enabled, display_order, engine_version, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        meta.strategy_key,
                        meta.name,
                        meta.description,
                        meta.enabled,
                        meta.display_order,
                        meta.engine_version,
                    ),
                )
                rows.append(asdict(meta))
        return rows

    def run_for_date(self, trade_date: str, strategy_key: str | None = None) -> dict:
        strategies = [
            strategy
            for strategy in list_enabled_strategies()
            if not strategy_key or strategy.meta().strategy_key == strategy_key
        ]
        self.sync_definitions()

        observation_count = 0
        for strategy in strategies:
            items = strategy.run_for_date(trade_date, context=self)
            observation_count += self._persist_strategy_rows(strategy.meta().strategy_key, trade_date, items)
            self._refresh_strategy_summary(strategy.meta().strategy_key, trade_date)

        completed = self.complete_pending_backtests()
        return {
            "trade_date": trade_date,
            "strategy_count": len(strategies),
            "observation_count": observation_count,
            "completed_backtests": completed,
        }

    def _persist_strategy_rows(self, strategy_key: str, trade_date: str, rows: list[ObservationCandidate]) -> int:
        with get_db_connection() as con:
            for item in rows:
                con.execute(
                    """
                    INSERT OR REPLACE INTO strategy_observations (
                        strategy_key, trade_date, observation_date, ts_code, name, reason,
                        tags_json, entry_anchor_date, trace_json, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    (
                        strategy_key,
                        trade_date,
                        item.observation_date,
                        item.ts_code,
                        item.name,
                        item.reason,
                        json.dumps(item.tags, ensure_ascii=False),
                        item.entry_anchor_date,
                        json.dumps(item.trace, ensure_ascii=False),
                    ),
                )
                entry_price = self._resolve_entry_price(item.ts_code, item.entry_anchor_date, item.entry_price_source)
                con.execute(
                    """
                    INSERT OR REPLACE INTO strategy_backtest_runs (
                        strategy_key, observation_date, ts_code, entry_anchor_date,
                        entry_price, entry_price_source, status, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, 'PENDING', CURRENT_TIMESTAMP)
                    """,
                    (
                        strategy_key,
                        item.observation_date,
                        item.ts_code,
                        item.entry_anchor_date,
                        entry_price,
                        item.entry_price_source,
                    ),
                )
        return len(rows)

    def _resolve_entry_price(self, ts_code: str, entry_anchor_date: str, entry_price_source: str) -> float | None:
        if entry_price_source == "open_next_trade_day":
            target_date = _shift_trade_date(entry_anchor_date, 1)
            field = "open"
        elif entry_price_source == "close_next_trade_day":
            target_date = _shift_trade_date(entry_anchor_date, 1)
            field = "close"
        else:
            target_date = entry_anchor_date
            field = "close"

        df = fetch_df(
            f"SELECT {field} AS price FROM daily_price WHERE ts_code = ? AND trade_date = ? LIMIT 1",
            [ts_code, target_date],
        )
        if df.empty or pd.isna(df.iloc[0]["price"]):
            return None
        return float(df.iloc[0]["price"])

    def complete_pending_backtests(self, limit: int = 200) -> int:
        pending = fetch_df(
            """
            SELECT strategy_key, observation_date, ts_code, entry_anchor_date, entry_price
            FROM strategy_backtest_runs
            WHERE status IN ('PENDING', 'PARTIAL')
            ORDER BY observation_date, ts_code
            LIMIT ?
            """,
            [limit],
        )
        completed = 0
        if pending.empty:
            return completed

        with get_db_connection() as con:
            for _, row in pending.iterrows():
                if not row["entry_price"]:
                    continue
                price_df = fetch_df(
                    """
                    SELECT CAST(trade_date AS VARCHAR) AS trade_date, close, high, low
                    FROM daily_price
                    WHERE ts_code = ? AND trade_date >= ?
                    ORDER BY trade_date
                    LIMIT 16
                    """,
                    [row["ts_code"], row["entry_anchor_date"]],
                )
                metrics_3d = build_horizon_metrics(price_df, float(row["entry_price"]), 3)
                metrics_5d = build_horizon_metrics(price_df, float(row["entry_price"]), 5)
                metrics_10d = build_horizon_metrics(price_df, float(row["entry_price"]), 10)
                status = "COMPLETED" if metrics_10d else ("PARTIAL" if metrics_3d or metrics_5d else "PENDING")
                con.execute(
                    """
                    UPDATE strategy_backtest_runs
                    SET ret_3d = ?, max_gain_3d = ?, max_drawdown_3d = ?,
                        ret_5d = ?, max_gain_5d = ?, max_drawdown_5d = ?,
                        ret_10d = ?, max_gain_10d = ?, max_drawdown_10d = ?,
                        status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE strategy_key = ? AND observation_date = ? AND ts_code = ?
                    """,
                    (
                        metrics_3d.get("ret_pct"),
                        metrics_3d.get("max_gain_pct"),
                        metrics_3d.get("max_drawdown_pct"),
                        metrics_5d.get("ret_pct"),
                        metrics_5d.get("max_gain_pct"),
                        metrics_5d.get("max_drawdown_pct"),
                        metrics_10d.get("ret_pct"),
                        metrics_10d.get("max_gain_pct"),
                        metrics_10d.get("max_drawdown_pct"),
                        status,
                        row["strategy_key"],
                        row["observation_date"],
                        row["ts_code"],
                    ),
                )
                completed += 1
        return completed

    def _refresh_strategy_summary(self, strategy_key: str, trade_date: str, window_trade_days: int = 120) -> None:
        summary_df = fetch_df(
            """
            WITH latest_days AS (
                SELECT DISTINCT observation_date
                FROM strategy_backtest_runs
                WHERE strategy_key = ?
                ORDER BY observation_date DESC
                LIMIT ?
            )
            SELECT
                COUNT(*) AS observation_count,
                COUNT(ret_3d) AS completed_count_3d,
                COUNT(ret_5d) AS completed_count_5d,
                COUNT(ret_10d) AS completed_count_10d,
                AVG(CASE WHEN ret_3d > 0 THEN 100.0 ELSE 0.0 END) AS win_rate_3d,
                AVG(CASE WHEN ret_5d > 0 THEN 100.0 ELSE 0.0 END) AS win_rate_5d,
                AVG(CASE WHEN ret_10d > 0 THEN 100.0 ELSE 0.0 END) AS win_rate_10d,
                AVG(ret_3d) AS avg_ret_3d,
                AVG(ret_5d) AS avg_ret_5d,
                AVG(ret_10d) AS avg_ret_10d,
                MEDIAN(ret_3d) AS median_ret_3d,
                MEDIAN(ret_5d) AS median_ret_5d,
                MEDIAN(ret_10d) AS median_ret_10d,
                AVG(max_gain_3d) AS avg_max_gain_3d,
                AVG(max_gain_5d) AS avg_max_gain_5d,
                AVG(max_gain_10d) AS avg_max_gain_10d,
                AVG(max_drawdown_3d) AS avg_max_drawdown_3d,
                AVG(max_drawdown_5d) AS avg_max_drawdown_5d,
                AVG(max_drawdown_10d) AS avg_max_drawdown_10d
            FROM strategy_backtest_runs
            WHERE strategy_key = ?
              AND observation_date IN (SELECT observation_date FROM latest_days)
            """,
            [strategy_key, window_trade_days, strategy_key],
        )
        summary = summary_df.iloc[0].to_dict() if not summary_df.empty else {"observation_count": 0}
        summary_text = build_strategy_summary_text(summary)

        with get_db_connection() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO strategy_daily_summaries (
                    strategy_key, trade_date, window_trade_days, observation_count,
                    completed_count_3d, completed_count_5d, completed_count_10d,
                    win_rate_3d, win_rate_5d, win_rate_10d,
                    avg_ret_3d, avg_ret_5d, avg_ret_10d,
                    median_ret_3d, median_ret_5d, median_ret_10d,
                    avg_max_gain_3d, avg_max_gain_5d, avg_max_gain_10d,
                    avg_max_drawdown_3d, avg_max_drawdown_5d, avg_max_drawdown_10d,
                    summary_text, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                (
                    strategy_key,
                    trade_date,
                    window_trade_days,
                    int(summary.get("observation_count") or 0),
                    int(summary.get("completed_count_3d") or 0),
                    int(summary.get("completed_count_5d") or 0),
                    int(summary.get("completed_count_10d") or 0),
                    summary.get("win_rate_3d"),
                    summary.get("win_rate_5d"),
                    summary.get("win_rate_10d"),
                    summary.get("avg_ret_3d"),
                    summary.get("avg_ret_5d"),
                    summary.get("avg_ret_10d"),
                    summary.get("median_ret_3d"),
                    summary.get("median_ret_5d"),
                    summary.get("median_ret_10d"),
                    summary.get("avg_max_gain_3d"),
                    summary.get("avg_max_gain_5d"),
                    summary.get("avg_max_gain_10d"),
                    summary.get("avg_max_drawdown_3d"),
                    summary.get("avg_max_drawdown_5d"),
                    summary.get("avg_max_drawdown_10d"),
                    summary_text,
                ),
            )
```

```python
# backend/strategy/plaza/__init__.py
from .service import StrategyPlazaService, build_horizon_metrics

strategy_plaza_service = StrategyPlazaService()

__all__ = [
    "ObservationCandidate",
    "PlazaStrategy",
    "StrategyMeta",
    "StrategyPlazaService",
    "build_horizon_metrics",
    "build_strategy_summary_text",
    "list_enabled_strategies",
    "list_registered_strategies",
    "strategy_plaza_service",
]
```

- [ ] **Step 4: Run the expanded backend tests**

Run:

```bash
cd /root/jarvis/backend && python -m unittest tests/strategy/plaza/test_service.py
```

Expected:

```text
.....
----------------------------------------------------------------------
Ran 5 tests in ...

OK
```

- [ ] **Step 5: Commit the service and backtest engine**

```bash
git add backend/strategy/plaza backend/tests/strategy/plaza/test_service.py
git commit -m "feat: add strategy plaza service and backtest engine"
```

### Task 3: Expose Strategy Plaza APIs, Task Queue Support, and Scheduler Integration

**Files:**
- Create: `backend/api/routes/strategy_plaza.py`
- Create: `backend/tests/api/routes/test_strategy_plaza.py`
- Modify: `backend/api/routes/__init__.py`
- Modify: `backend/main.py`
- Modify: `backend/api/routes/etl.py`
- Modify: `backend/etl/sync.py`
- Modify: `backend/etl/scheduler.py`

- [ ] **Step 1: Write the failing API tests for list, observation, summary, and queued run**

```python
# backend/tests/api/routes/test_strategy_plaza.py
import sys
import types
import unittest
from unittest.mock import patch

import pandas as pd

_pypinyin = types.ModuleType("pypinyin")
_pypinyin.lazy_pinyin = lambda value, style=None: []


class _Style:
    FIRST_LETTER = "FIRST_LETTER"


_pypinyin.Style = _Style
sys.modules.setdefault("pypinyin", _pypinyin)

import api.routes.strategy_plaza as strategy_plaza


class StrategyPlazaRouteTests(unittest.TestCase):
    @patch("api.routes.strategy_plaza.strategy_plaza_service.sync_definitions", return_value=[])
    @patch("api.routes.strategy_plaza.fetch_df", return_value=pd.DataFrame(columns=["strategy_key"]))
    def test_list_strategies_returns_empty_success_payload(self, _df, _sync):
        result = strategy_plaza.list_strategies()

        self.assertEqual("success", result["status"])
        self.assertEqual([], result["data"]["strategies"])

    @patch("api.routes.strategy_plaza.fetch_df")
    def test_get_observations_merges_backtest_columns(self, mocked_fetch):
        mocked_fetch.side_effect = [
            pd.DataFrame(
                [
                    {
                        "strategy_key": "demo_strategy",
                        "trade_date": "2026-04-08",
                        "observation_date": "2026-04-08",
                        "ts_code": "300308.SZ",
                        "name": "中际旭创",
                        "reason": "示例观察",
                        "tags_json": '["demo"]',
                    }
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "strategy_key": "demo_strategy",
                        "observation_date": "2026-04-08",
                        "ts_code": "300308.SZ",
                        "ret_3d": 5.1,
                        "ret_5d": None,
                        "ret_10d": None,
                        "status": "PARTIAL",
                    }
                ]
            ),
        ]

        result = strategy_plaza.get_observations(strategy_key="demo_strategy", trade_date="2026-04-08")

        self.assertEqual(1, len(result["data"]["items"]))
        self.assertEqual(5.1, result["data"]["items"][0]["ret_3d"])
        self.assertEqual("PARTIAL", result["data"]["items"][0]["backtest_status"])

    @patch("api.routes.strategy_plaza.fetch_df")
    def test_get_summary_returns_null_when_no_summary_exists(self, mocked_fetch):
        mocked_fetch.return_value = pd.DataFrame(columns=["strategy_key"])

        result = strategy_plaza.get_summary(strategy_key="demo_strategy", trade_date="2026-04-08")

        self.assertIsNone(result["data"]["summary"])

    @patch("api.routes.strategy_plaza.TaskRegistry.create_task", return_value=("abc12345", "PENDING"))
    def test_trigger_run_enqueues_strategy_plaza_task(self, mocked_create):
        payload = strategy_plaza.StrategyPlazaRunParams(trade_date="2026-04-08", strategy_key=None)

        result = strategy_plaza.trigger_strategy_run(payload)

        mocked_create.assert_called_once()
        self.assertEqual("abc12345", result["task_id"])


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run the API test to verify the route module is missing**

Run:

```bash
cd /root/jarvis/backend && python -m unittest tests/api/routes/test_strategy_plaza.py
```

Expected:

```text
FAILED ... ModuleNotFoundError: No module named 'api.routes.strategy_plaza'
```

- [ ] **Step 3: Add the new route module, queue runner, sync hook, and 18:00 scheduler job**

```python
# backend/api/routes/strategy_plaza.py
import json
import logging
import arrow
import pandas as pd
from fastapi import APIRouter
from pydantic import BaseModel

from api.routes.etl import TaskRegistry
from db.connection import fetch_df
from strategy.plaza import strategy_plaza_service

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Strategy Plaza"])


class StrategyPlazaRunParams(BaseModel):
    trade_date: str | None = None
    strategy_key: str | None = None


@router.get("/strategy-plaza/strategies")
def list_strategies():
    strategy_plaza_service.sync_definitions()
    df = fetch_df(
        """
        SELECT strategy_key, name, description, enabled, display_order, engine_version, updated_at
        FROM strategy_definitions
        ORDER BY display_order, strategy_key
        """
    )
    return {"status": "success", "data": {"strategies": df.to_dict("records") if not df.empty else []}}


@router.get("/strategy-plaza/observations")
def get_observations(strategy_key: str, trade_date: str, limit: int = 100):
    obs_df = fetch_df(
        """
        SELECT strategy_key, CAST(trade_date AS VARCHAR) AS trade_date,
               CAST(observation_date AS VARCHAR) AS observation_date,
               ts_code, name, reason, tags_json
        FROM strategy_observations
        WHERE strategy_key = ? AND observation_date = ?
        ORDER BY ts_code
        LIMIT ?
        """,
        [strategy_key, trade_date, limit],
    )
    backtest_df = fetch_df(
        """
        SELECT strategy_key, CAST(observation_date AS VARCHAR) AS observation_date,
               ts_code, ret_3d, ret_5d, ret_10d, status
        FROM strategy_backtest_runs
        WHERE strategy_key = ? AND observation_date = ?
        """,
        [strategy_key, trade_date],
    )
    if obs_df.empty:
        return {"status": "success", "data": {"items": []}}

    merged = obs_df.merge(backtest_df, on=["strategy_key", "observation_date", "ts_code"], how="left")
    items = []
    for _, row in merged.iterrows():
        items.append(
            {
                "strategy_key": row["strategy_key"],
                "trade_date": row["trade_date"],
                "observation_date": row["observation_date"],
                "ts_code": row["ts_code"],
                "name": row["name"],
                "reason": row["reason"],
                "tags": json.loads(row["tags_json"]) if row.get("tags_json") else [],
                "ret_3d": None if pd.isna(row.get("ret_3d")) else float(row["ret_3d"]),
                "ret_5d": None if pd.isna(row.get("ret_5d")) else float(row["ret_5d"]),
                "ret_10d": None if pd.isna(row.get("ret_10d")) else float(row["ret_10d"]),
                "backtest_status": row.get("status") or "PENDING",
            }
        )
    return {"status": "success", "data": {"items": items}}


@router.get("/strategy-plaza/summary")
def get_summary(strategy_key: str, trade_date: str):
    df = fetch_df(
        """
        SELECT *
        FROM strategy_daily_summaries
        WHERE strategy_key = ? AND trade_date = ?
        LIMIT 1
        """,
        [strategy_key, trade_date],
    )
    return {"status": "success", "data": {"summary": None if df.empty else df.iloc[0].to_dict()}}


@router.post("/strategy-plaza/run", status_code=202)
def trigger_strategy_run(params: StrategyPlazaRunParams):
    payload = params.dict()
    if not payload["trade_date"]:
        payload["trade_date"] = arrow.now("Asia/Shanghai").format("YYYY-MM-DD")
    task_key = f"strategy_plaza:{payload['trade_date']}:{payload.get('strategy_key') or 'all'}"
    task_id, _ = TaskRegistry.create_task("STRATEGY_PLAZA", payload, task_key=task_key)
    return {"message": "策略广场任务已加入队列", "task_id": task_id}
```

```python
# backend/api/routes/__init__.py
from .strategy_plaza import router as strategy_plaza_router

__all__ = [
    "users_router",
    "ai_router",
    "stocks_router",
    "market_router",
    "etl_router",
    "db_router",
    "system_router",
    "docs_router",
    "strategy_plaza_router",
]
```

```python
# backend/main.py
from api.routes import (
    users_router,
    ai_router,
    stocks_router,
    market_router,
    etl_router,
    db_router,
    system_router,
    docs_router,
    strategy_plaza_router,
)

app.include_router(strategy_plaza_router, prefix="/admin")
```

```python
# backend/api/routes/etl.py
def _run_strategy_plaza_task(task_id, params):
    sync_engine.run_strategy_plaza_refresh(
        trade_date=params.get("trade_date"),
        strategy_key=params.get("strategy_key"),
    )

# inside task_worker()
elif task_type == "STRATEGY_PLAZA":
    await asyncio.to_thread(_run_strategy_plaza_task, task_id, params)
```

```python
# backend/etl/sync.py
    def run_strategy_plaza_refresh(self, trade_date: str | None = None, strategy_key: str | None = None):
        from strategy.plaza import strategy_plaza_service
        from etl.calendar import trading_calendar

        target_date = trade_date or (
            trading_calendar.get_latest_sync_date().strftime("%Y-%m-%d")
            if hasattr(trading_calendar.get_latest_sync_date(), "strftime")
            else str(trading_calendar.get_latest_sync_date())
        )
        logger.info(f"执行策略广场刷新: trade_date={target_date}, strategy_key={strategy_key or 'all'}")
        return strategy_plaza_service.run_for_date(target_date, strategy_key=strategy_key)
```

```python
# backend/etl/scheduler.py
    scheduler.add_job(
        sync_engine.run_strategy_plaza_refresh,
        CronTrigger(hour=18, minute=0, timezone=SHANGHAI_TZ),
        id="strategy_plaza_refresh",
        name="策略广场每日刷新",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
        replace_existing=True,
    )
```

- [ ] **Step 4: Run the backend API tests**

Run:

```bash
cd /root/jarvis/backend && python -m unittest tests/api/routes/test_strategy_plaza.py
```

Expected:

```text
....
----------------------------------------------------------------------
Ran 4 tests in ...

OK
```

- [ ] **Step 5: Commit the route, queue, and scheduler integration**

```bash
git add backend/api/routes/strategy_plaza.py backend/tests/api/routes/test_strategy_plaza.py backend/api/routes/__init__.py backend/main.py backend/api/routes/etl.py backend/etl/sync.py backend/etl/scheduler.py
git commit -m "feat: expose strategy plaza routes and scheduling"
```

### Task 4: Add the Frontend Route, Nav Entry, and Lightweight Strategy Plaza Page

**Files:**
- Create: `frontend/src/views/Strategies.vue`
- Create: `frontend/src/composables/useStrategyPlaza.js`
- Create: `frontend/tests/strategy-plaza.test.mjs`
- Modify: `frontend/src/router/index.js`
- Modify: `frontend/src/components/AppLayout.vue`
- Modify: `frontend/src/services/api.js`

- [ ] **Step 1: Write the failing frontend helper and source tests**

```js
// frontend/tests/strategy-plaza.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const routerPath = path.resolve(__dirname, '../src/router/index.js');
const layoutPath = path.resolve(__dirname, '../src/components/AppLayout.vue');
const viewPath = path.resolve(__dirname, '../src/views/Strategies.vue');

test('strategy plaza helper normalizes rows and summary facts', async () => {
  const { normalizeObservationRows, buildSummaryFacts } = await import('../src/composables/useStrategyPlaza.js');

  const rows = normalizeObservationRows([
    { ts_code: '300308.SZ', name: '中际旭创', reason: '示例观察', ret_3d: 5.12, ret_5d: null, ret_10d: -1.5, backtest_status: 'PARTIAL' },
  ]);
  const facts = buildSummaryFacts({
    observation_count: 12,
    win_rate_3d: 58.3,
    avg_ret_3d: 2.56,
    avg_ret_10d: -0.8,
    summary_text: '近窗共 12 条观察。',
  });

  assert.deepEqual(rows[0], {
    ts_code: '300308.SZ',
    name: '中际旭创',
    reason: '示例观察',
    backtestStatus: 'PARTIAL',
    ret3dText: '+5.12%',
    ret5dText: '-',
    ret10dText: '-1.50%',
  });
  assert.equal(facts[0].label, '样本');
  assert.equal(facts[0].value, '12');
  assert.equal(facts[1].value, '58.3%');
});

test('router and app layout expose a first-level strategies entry', () => {
  const routerSource = readFileSync(routerPath, 'utf8');
  const layoutSource = readFileSync(layoutPath, 'utf8');

  assert.match(routerSource, /const Strategies = \(\) => import\('@\/views\/Strategies\.vue'\)/);
  assert.match(routerSource, /path: 'strategies'/);
  assert.match(routerSource, /name: 'strategies'/);
  assert.match(layoutSource, /策略广场/);
  assert.match(layoutSource, /to=\"\/strategies\"/);
});

test('strategy plaza view keeps a compact toolbar list summary structure', () => {
  const source = readFileSync(viewPath, 'utf8');

  assert.match(source, /type=\"date\"/);
  assert.match(source, /新进入观察/);
  assert.match(source, /3日/);
  assert.match(source, /5日/);
  assert.match(source, /10日/);
  assert.match(source, /暂无策略/);
  assert.match(source, /该日暂无新进入观察的标的/);
});
```

- [ ] **Step 2: Run the frontend test to verify the new files are missing**

Run:

```bash
cd /root/jarvis && node --test frontend/tests/strategy-plaza.test.mjs
```

Expected:

```text
not ok ... Cannot find module '../src/composables/useStrategyPlaza.js'
```

- [ ] **Step 3: Implement the frontend page, helper, API wrappers, and nav wiring**

```js
// frontend/src/composables/useStrategyPlaza.js
const pct = (value) => {
  const num = Number(value);
  if (!Number.isFinite(num)) return '-';
  return `${num >= 0 ? '+' : ''}${num.toFixed(2)}%`;
};

export const normalizeObservationRows = (items = []) =>
  (Array.isArray(items) ? items : []).map((item) => ({
    ts_code: item.ts_code || '-',
    name: item.name || item.ts_code || '-',
    reason: item.reason || '-',
    backtestStatus: item.backtest_status || 'PENDING',
    ret3dText: pct(item.ret_3d),
    ret5dText: pct(item.ret_5d),
    ret10dText: pct(item.ret_10d),
  }));

export const buildSummaryFacts = (summary = null) => {
  if (!summary) return [];
  return [
    { label: '样本', value: `${Number(summary.observation_count || 0)}` },
    { label: '3日胜率', value: Number.isFinite(Number(summary.win_rate_3d)) ? `${Number(summary.win_rate_3d).toFixed(1)}%` : '-' },
    { label: '3日均值', value: pct(summary.avg_ret_3d) },
    { label: '10日均值', value: pct(summary.avg_ret_10d) },
  ];
};
```

```js
// frontend/src/services/api.js
export const getStrategyPlazaStrategies = () => apiClient.get('/admin/strategy-plaza/strategies');
export const getStrategyPlazaObservations = (strategyKey, tradeDate, limit = 100) =>
  apiClient.get('/admin/strategy-plaza/observations', { params: { strategy_key: strategyKey, trade_date: tradeDate, limit } });
export const getStrategyPlazaSummary = (strategyKey, tradeDate) =>
  apiClient.get('/admin/strategy-plaza/summary', { params: { strategy_key: strategyKey, trade_date: tradeDate } });
export const runStrategyPlaza = (payload) => apiClient.post('/admin/strategy-plaza/run', payload);
```

```js
// frontend/src/router/index.js
const Strategies = () => import('@/views/Strategies.vue')

{
  path: 'strategies',
  name: 'strategies',
  component: Strategies,
  meta: { requiresAuth: true }
},
```

```vue
<!-- frontend/src/components/AppLayout.vue -->
<router-link
  to="/strategies"
  class="px-3 py-1.5 rounded-lg text-[11px] font-semibold transition-all duration-200"
  :class="isStrategiesPage ? 'bg-white/[0.06] text-white' : 'text-slate-500 hover:text-slate-300 hover:bg-white/[0.03]'"
>
  策略广场
</router-link>

<router-link to="/strategies" class="flex flex-col items-center gap-0.5 transition-all duration-200 py-1 px-3 rounded-xl" :class="isStrategiesPage ? 'text-signal-bull' : 'text-slate-600'">
  <Squares2X2Icon class="w-5 h-5" />
  <span class="text-[9px] font-semibold">策略</span>
</router-link>

import { Squares2X2Icon } from '@heroicons/vue/20/solid'

const isStrategiesPage = computed(() => route.name === 'strategies');
```

```vue
<!-- frontend/src/views/Strategies.vue -->
<template>
  <section class="space-y-3">
    <div class="rounded-2xl border border-white/[0.06] bg-obsidian-900/70 px-4 py-3">
      <div class="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
        <div class="flex flex-1 flex-col gap-3 md:flex-row md:items-center">
          <select v-model="selectedStrategyKey" class="rounded-lg border border-white/[0.08] bg-obsidian-950 px-3 py-2 text-sm text-slate-200">
            <option value="">选择策略</option>
            <option v-for="item in strategies" :key="item.strategy_key" :value="item.strategy_key">{{ item.name }}</option>
          </select>
          <input v-model="selectedDate" type="date" class="rounded-lg border border-white/[0.08] bg-obsidian-950 px-3 py-2 text-sm text-slate-200" />
        </div>
        <button class="rounded-lg border border-white/[0.08] bg-white/[0.03] px-3 py-2 text-xs font-semibold text-slate-200" @click="reloadAll">
          刷新
        </button>
      </div>
    </div>

    <div v-if="!strategies.length && !loading" class="rounded-2xl border border-white/[0.06] bg-obsidian-900/55 px-4 py-10 text-center text-sm text-slate-500">
      暂无策略
    </div>

    <div v-else class="rounded-2xl border border-white/[0.06] bg-obsidian-900/55 overflow-hidden">
      <div class="border-b border-white/[0.06] px-4 py-3 text-sm font-semibold text-slate-200">新进入观察</div>
      <div v-if="!rows.length && !loading" class="px-4 py-10 text-center text-sm text-slate-500">该日暂无新进入观察的标的</div>
      <table v-else class="w-full text-left text-sm">
        <thead class="bg-white/[0.02] text-[11px] uppercase tracking-[0.16em] text-slate-500">
          <tr>
            <th class="px-4 py-3">标的</th>
            <th class="px-4 py-3">理由</th>
            <th class="px-4 py-3">3日</th>
            <th class="px-4 py-3">5日</th>
            <th class="px-4 py-3">10日</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in rows" :key="`${item.ts_code}-${item.reason}`" class="border-t border-white/[0.05]">
            <td class="px-4 py-3">
              <div class="font-semibold text-slate-100">{{ item.name }}</div>
              <div class="mt-1 text-xs text-slate-500">{{ item.ts_code }}</div>
            </td>
            <td class="px-4 py-3 text-slate-300">{{ item.reason }}</td>
            <td class="px-4 py-3 text-slate-300">{{ item.ret3dText }}</td>
            <td class="px-4 py-3 text-slate-300">{{ item.ret5dText }}</td>
            <td class="px-4 py-3 text-slate-300">{{ item.ret10dText }}</td>
          </tr>
        </tbody>
      </table>
    </div>

    <div v-if="summary" class="rounded-2xl border border-white/[0.06] bg-obsidian-900/55 px-4 py-3">
      <div class="grid gap-3 md:grid-cols-4">
        <div v-for="fact in summaryFacts" :key="fact.label">
          <div class="text-[10px] text-slate-500">{{ fact.label }}</div>
          <div class="mt-1 text-sm font-semibold text-slate-200">{{ fact.value }}</div>
        </div>
      </div>
      <div class="mt-3 text-sm text-slate-300">{{ summary.summary_text }}</div>
    </div>
  </section>
</template>

<script setup>
import { computed, onMounted, ref, watch } from 'vue';
import {
  getStrategyPlazaObservations,
  getStrategyPlazaStrategies,
  getStrategyPlazaSummary,
} from '@/services/api';
import { buildSummaryFacts, normalizeObservationRows } from '@/composables/useStrategyPlaza';

const today = new Date().toISOString().slice(0, 10);
const loading = ref(false);
const strategies = ref([]);
const selectedStrategyKey = ref('');
const selectedDate = ref(today);
const observationItems = ref([]);
const summary = ref(null);

const rows = computed(() => normalizeObservationRows(observationItems.value));
const summaryFacts = computed(() => buildSummaryFacts(summary.value));

const reloadAll = async () => {
  loading.value = true;
  try {
    const [strategyRes, observationRes, summaryRes] = await Promise.all([
      getStrategyPlazaStrategies(),
      selectedStrategyKey.value ? getStrategyPlazaObservations(selectedStrategyKey.value, selectedDate.value) : Promise.resolve({ data: { data: { items: [] } } }),
      selectedStrategyKey.value ? getStrategyPlazaSummary(selectedStrategyKey.value, selectedDate.value) : Promise.resolve({ data: { data: { summary: null } } }),
    ]);

    strategies.value = strategyRes.data?.data?.strategies || [];
    if (!selectedStrategyKey.value && strategies.value.length) {
      selectedStrategyKey.value = strategies.value[0].strategy_key;
      return reloadAll();
    }
    observationItems.value = observationRes.data?.data?.items || [];
    summary.value = summaryRes.data?.data?.summary || null;
  } finally {
    loading.value = false;
  }
};

watch([selectedStrategyKey, selectedDate], () => {
  if (selectedStrategyKey.value) void reloadAll();
});

onMounted(() => {
  void reloadAll();
});
</script>
```

- [ ] **Step 4: Run the frontend tests**

Run:

```bash
cd /root/jarvis && node --test frontend/tests/strategy-plaza.test.mjs
```

Expected:

```text
ok 1 - strategy plaza helper normalizes rows and summary facts
ok 2 - router and app layout expose a first-level strategies entry
ok 3 - strategy plaza view keeps a compact toolbar list summary structure
```

- [ ] **Step 5: Commit the strategy plaza UI**

```bash
git add frontend/src/views/Strategies.vue frontend/src/composables/useStrategyPlaza.js frontend/tests/strategy-plaza.test.mjs frontend/src/router/index.js frontend/src/components/AppLayout.vue frontend/src/services/api.js
git commit -m "feat: add strategy plaza frontend page"
```

### Task 5: Sync Docs and Verify the End-to-End Empty-State-Safe Flow

**Files:**
- Modify: `AGENTS.md`
- Modify: `docs/README.md`
- Modify: `docs/published/trading-system/README.md`

- [ ] **Step 1: Update project docs to describe the new routes, tables, and page**

```md
# AGENTS.md
- 新增一级入口 `策略广场`，与 `盯盘` 并列，前端通过 `/admin/strategy-plaza/*` 读取公共策略结果。
- 新增策略广场表：`strategy_definitions`、`strategy_observations`、`strategy_backtest_runs`、`strategy_daily_summaries`。
- 新增接口：
  - `/strategy-plaza/strategies`
  - `/strategy-plaza/observations`
  - `/strategy-plaza/summary`
  - `/strategy-plaza/run`
```

```md
# docs/README.md
### 策略广场

- 固定策略由后端代码注册
- 每日 18:00 自动归档当日新进入观察的标的
- 系统补算 3 / 5 / 10 日表现
- 前端页面只展示最终进入观察的结果，不展示中间阶段池
```

```md
# docs/published/trading-system/README.md
- 策略广场当前定位为“公共策略归档与验证页”，与盯盘并列，只展示最终进入观察的标的及其后续表现。
```

- [ ] **Step 2: Run the backend and frontend regression tests**

Run:

```bash
cd /root/jarvis/backend && python -m unittest tests/strategy/plaza/test_service.py tests/api/routes/test_strategy_plaza.py
```

Expected:

```text
.........
----------------------------------------------------------------------
Ran 9 tests in ...

OK
```

Run:

```bash
cd /root/jarvis && node --test frontend/tests/strategy-plaza.test.mjs
```

Expected:

```text
ok 1 ...
ok 2 ...
ok 3 ...
```

- [ ] **Step 3: Verify the HTTP flow with curl against the running container-mounted app**

Run:

```bash
curl -s http://localhost:8000/admin/strategy-plaza/strategies
```

Expected:

```json
{"status":"success","data":{"strategies":[]}}
```

Run:

```bash
curl -s -X POST http://localhost:8000/admin/strategy-plaza/run \
  -H "Content-Type: application/json" \
  -d '{"trade_date":"2026-04-08"}'
```

Expected:

```json
{"message":"策略广场任务已加入队列","task_id":"..."}
```

Run:

```bash
curl -s "http://localhost:8000/admin/tasks/status?limit=5"
```

Expected:

```json
{"tasks":[{"task_type":"STRATEGY_PLAZA","status":"PENDING|RUNNING|COMPLETED"}]}
```

Run:

```bash
curl -s "http://localhost:8000/admin/strategy-plaza/observations?strategy_key=demo_strategy&trade_date=2026-04-08"
```

Expected:

```json
{"status":"success","data":{"items":[]}}
```

- [ ] **Step 4: Commit the docs and verification-backed completion**

```bash
git add AGENTS.md docs/README.md docs/published/trading-system/README.md
git commit -m "docs: document strategy plaza routes and storage"
```

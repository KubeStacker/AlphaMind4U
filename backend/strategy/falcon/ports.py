from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import pandas as pd


@dataclass(slots=True)
class RunContext:
    strategy_id: str
    as_of_date: str
    params: dict[str, Any]


class FalconRepositoryPort(Protocol):
    def ensure_schema(self) -> None: ...

    def get_strategy_state(self, strategy_id: str) -> dict[str, Any] | None: ...

    def upsert_strategy_state(
        self,
        strategy_id: str,
        version: int,
        params_json: dict[str, Any],
        note: str,
    ) -> None: ...

    def create_run(
        self,
        strategy_id: str,
        strategy_version: int,
        as_of_date: str,
        params_json: dict[str, Any],
    ) -> int: ...

    def complete_run(
        self,
        run_id: int,
        summary_json: dict[str, Any],
    ) -> None: ...

    def insert_picks(self, run_id: int, rows: list[dict[str, Any]]) -> None: ...

    def upsert_eval_rows(self, rows: list[dict[str, Any]]) -> None: ...

    def upsert_strategy_daily_score(self, strategy_id: str, trade_date: str, score: float, details: dict[str, Any]) -> None: ...

    def list_runs(self, strategy_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]: ...
    def list_deleted_runs(self, strategy_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]: ...
    def list_operation_logs(self, strategy_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]: ...

    def get_run_detail(self, run_id: int) -> dict[str, Any] | None: ...

    def soft_delete_run(self, run_id: int, operator: str) -> bool: ...

    def restore_run(self, run_id: int) -> bool: ...
    def restore_runs(self, run_ids: list[int]) -> int: ...

    def hard_delete_run(self, run_id: int) -> bool: ...
    def hard_delete_runs(self, run_ids: list[int]) -> int: ...
    def soft_delete_runs_by_range(
        self,
        strategy_id: str,
        start_date: str,
        end_date: str,
        operator: str,
    ) -> int: ...

    def get_strategy_daily_score(self, strategy_id: str, trade_date: str) -> dict[str, Any] | None: ...

    def get_history_eval_for_strategy(self, strategy_id: str, lookback_runs: int = 120) -> pd.DataFrame: ...

    def record_evolution(
        self,
        strategy_id: str,
        prev_version: int,
        next_version: int,
        prev_params: dict[str, Any],
        next_params: dict[str, Any],
        score_before: float,
        score_after: float,
        promoted: bool,
        details: dict[str, Any],
    ) -> None: ...


class FalconDataProviderPort(Protocol):
    def latest_trade_date(self) -> str: ...

    def load_universe_snapshot(self, trade_date: str) -> pd.DataFrame: ...

    def forward_returns(self, trade_date: str, ts_codes: list[str], horizon: int) -> pd.DataFrame: ...

    def event_exit_returns(
        self,
        trade_date: str,
        ts_codes: list[str],
        stop_loss_map: dict[str, float],
        max_horizon: int = 20,
    ) -> pd.DataFrame: ...

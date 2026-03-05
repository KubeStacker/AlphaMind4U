from __future__ import annotations

from typing import Any

import pandas as pd

from strategy.falcon.backtest import build_forward_eval_rows, summarize_run
from strategy.falcon.evolution import evolve_params_from_history
from strategy.falcon.ports import FalconDataProviderPort, FalconRepositoryPort
from strategy.falcon.registry import falcon_registry


class FalconApplicationService:
    def __init__(
        self,
        repo: FalconRepositoryPort,
        data_provider: FalconDataProviderPort,
    ) -> None:
        self.repo = repo
        self.data_provider = data_provider

    @staticmethod
    def _safe_int(value: Any, default: int) -> int:
        try:
            n = int(value)
            return n
        except Exception:
            return default

    def latest_trade_date(self) -> str:
        return self.data_provider.latest_trade_date()

    def list_strategies(self) -> list[dict[str, Any]]:
        return falcon_registry.list_strategies()

    def run_strategy(
        self,
        strategy_id: str,
        as_of_date: str | None = None,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self.repo.ensure_schema()

        plugin = falcon_registry.get(strategy_id)
        state = self.repo.get_strategy_state(strategy_id) or {}
        version = int(state.get("version", 1))

        effective_date = as_of_date or self.data_provider.latest_trade_date()
        effective_params = {
            **plugin.default_params(),
            **(state.get("params_json") or {}),
            **(params or {}),
        }

        run_id = self.repo.create_run(
            strategy_id=strategy_id,
            strategy_version=version,
            as_of_date=effective_date,
            params_json=effective_params,
        )

        universe_df = self.data_provider.load_universe_snapshot(effective_date)
        picks_df = plugin.run(universe_df=universe_df, as_of_date=effective_date, params=effective_params)

        if picks_df is None:
            picks_df = pd.DataFrame()

        pick_rows: list[dict[str, Any]] = []
        if not picks_df.empty:
            for idx, row in picks_df.reset_index(drop=True).iterrows():
                pick_rows.append(
                    {
                        "run_id": run_id,
                        "rank_no": int(idx + 1),
                        "strategy_id": strategy_id,
                        "trade_date": effective_date,
                        "ts_code": str(row["ts_code"]),
                        "name": str(row.get("name", "")),
                        "strategy_score": float(row.get("strategy_score", 0.0)),
                        "confidence": float(row.get("confidence", 0.0)),
                        "signal_label": str(row.get("signal_label", "观察")),
                        "score_breakdown": row.get("score_breakdown", {}),
                    }
                )
        self.repo.insert_picks(run_id, pick_rows)

        eval_rows = build_forward_eval_rows(
            provider=self.data_provider,
            run_id=run_id,
            strategy_id=strategy_id,
            trade_date=effective_date,
            picks_df=picks_df,
        )
        if eval_rows:
            self.repo.upsert_eval_rows(eval_rows)

        summary = summarize_run(picks_df=picks_df, eval_rows=eval_rows)
        self.repo.complete_run(run_id, summary)

        score_val = summary.get("avg_score")
        if score_val is not None:
            self.repo.upsert_strategy_daily_score(
                strategy_id=strategy_id,
                trade_date=effective_date,
                score=float(score_val),
                details={
                    "run_id": run_id,
                    "pick_count": summary.get("pick_count"),
                    "hit_5d": summary.get("hit_5d"),
                    "hit_10d": summary.get("hit_10d"),
                },
            )

        detail = self.repo.get_run_detail(run_id)
        return {
            "run_id": run_id,
            "strategy_id": strategy_id,
            "as_of_date": effective_date,
            "summary": summary,
            "detail": detail,
        }

    def list_runs(self, strategy_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        self.repo.ensure_schema()
        return self.repo.list_runs(strategy_id=strategy_id, limit=limit)

    def list_deleted_runs(self, strategy_id: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        self.repo.ensure_schema()
        return self.repo.list_deleted_runs(strategy_id=strategy_id, limit=limit)

    def list_operation_logs(self, strategy_id: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        self.repo.ensure_schema()
        return self.repo.list_operation_logs(strategy_id=strategy_id, limit=limit)

    def get_run_detail(self, run_id: int) -> dict[str, Any] | None:
        self.repo.ensure_schema()
        return self.repo.get_run_detail(run_id)

    def get_strategy_daily_score(self, strategy_id: str, trade_date: str) -> dict[str, Any] | None:
        self.repo.ensure_schema()
        return self.repo.get_strategy_daily_score(strategy_id=strategy_id, trade_date=trade_date)

    def delete_run(self, run_id: int, operator: str = "system") -> bool:
        self.repo.ensure_schema()
        return self.repo.soft_delete_run(run_id=run_id, operator=operator)

    def restore_run(self, run_id: int) -> bool:
        self.repo.ensure_schema()
        return self.repo.restore_run(run_id=run_id)

    def restore_runs(self, run_ids: list[int]) -> int:
        self.repo.ensure_schema()
        return self.repo.restore_runs(run_ids=run_ids)

    def hard_delete_run(self, run_id: int) -> bool:
        self.repo.ensure_schema()
        return self.repo.hard_delete_run(run_id=run_id)

    def hard_delete_runs(self, run_ids: list[int]) -> int:
        self.repo.ensure_schema()
        return self.repo.hard_delete_runs(run_ids=run_ids)

    def delete_runs_by_range(
        self,
        strategy_id: str,
        start_date: str,
        end_date: str,
        operator: str = "system",
    ) -> int:
        self.repo.ensure_schema()
        return self.repo.soft_delete_runs_by_range(
            strategy_id=strategy_id,
            start_date=start_date,
            end_date=end_date,
            operator=operator,
        )

    def evolve(self, strategy_id: str) -> dict[str, Any]:
        self.repo.ensure_schema()

        plugin = falcon_registry.get(strategy_id)
        current = self.repo.get_strategy_state(strategy_id) or {
            "version": 1,
            "params_json": plugin.default_params(),
        }
        prev_version = self._safe_int(current.get("version", 1), 1)
        prev_params = dict(current.get("params_json") or plugin.default_params())

        history = self.repo.get_history_eval_for_strategy(strategy_id=strategy_id)
        try:
            next_params, details = evolve_params_from_history(history_df=history, current_params=prev_params)
        except Exception as e:
            # Evolve should be non-blocking for API callers; degrade gracefully.
            next_params = dict(prev_params)
            details = {
                "reason": "evolve_error",
                "message": str(e),
                "selected_top_n": prev_params.get("top_n", 20),
                "grid": [],
            }

        prev_score = float(details.get("grid", [{}])[0].get("score", 0.0)) if details.get("grid") else 0.0
        next_top_n = self._safe_int(next_params.get("top_n", prev_params.get("top_n", 20)), 20)

        promoted = next_params != prev_params
        next_version = prev_version + 1 if promoted else prev_version
        if promoted:
            self.repo.upsert_strategy_state(
                strategy_id=strategy_id,
                version=next_version,
                params_json=next_params,
                note="auto evolution",
            )

        score_after = 0.0
        if details.get("grid"):
            grid = details["grid"]
            for item in grid:
                item_top_n = self._safe_int(item.get("top_n"), -1)
                if item_top_n == next_top_n:
                    score_after = float(item["score"])
                    break

        self.repo.record_evolution(
            strategy_id=strategy_id,
            prev_version=prev_version,
            next_version=next_version,
            prev_params=prev_params,
            next_params=next_params,
            score_before=prev_score,
            score_after=score_after,
            promoted=promoted,
            details=details,
        )

        return {
            "strategy_id": strategy_id,
            "promoted": promoted,
            "prev_version": prev_version,
            "next_version": next_version,
            "prev_params": prev_params,
            "next_params": next_params,
            "details": details,
        }

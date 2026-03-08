from __future__ import annotations

import time
from typing import Any

import pandas as pd

from strategy.falcon.backtest import build_forward_eval_rows, coerce_pick_frame, summarize_eval_frame
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

    @staticmethod
    def _attr_dict(frame: pd.DataFrame | None, key: str) -> dict[str, Any]:
        if hasattr(frame, "attrs") and isinstance(frame.attrs.get(key), dict):
            return frame.attrs.get(key, {})
        return {}

    @staticmethod
    def _attr_list(frame: pd.DataFrame | None, key: str) -> list[dict[str, Any]]:
        if not hasattr(frame, "attrs"):
            return []
        raw = frame.attrs.get(key)
        if not isinstance(raw, list):
            return []
        return [item for item in raw if isinstance(item, dict)]

    @staticmethod
    def _enrich_pool_summary(
        summary: dict[str, Any],
        active_days: int,
        trade_day_count: int,
    ) -> dict[str, Any]:
        pick_count = int(summary.get("pick_count") or 0)
        summary["active_days"] = int(active_days)
        summary["trade_day_count"] = int(trade_day_count)
        summary["active_day_ratio"] = (
            None if trade_day_count <= 0 else float(active_days / trade_day_count)
        )
        summary["avg_picks_per_active_day"] = (
            None if active_days <= 0 else float(pick_count / active_days)
        )
        summary["avg_picks_per_trade_day"] = (
            None if trade_day_count <= 0 else float(pick_count / trade_day_count)
        )
        return summary

    def _resolve_effective_params(
        self,
        strategy_id: str,
        params: dict[str, Any] | None = None,
    ) -> tuple[Any, int, dict[str, Any]]:
        plugin = falcon_registry.get(strategy_id)
        state = self.repo.get_strategy_state(strategy_id) or {}
        version = int(state.get("version", 1))
        effective_params = {
            **plugin.default_params(),
            **(state.get("params_json") or {}),
            **(params or {}),
        }
        return plugin, version, effective_params

    def _execute_plugin(
        self,
        plugin: Any,
        effective_date: str,
        effective_params: dict[str, Any],
    ) -> dict[str, Any]:
        universe_df = self.data_provider.load_universe_snapshot(effective_date)
        market_context = (
            universe_df.attrs.get("market_context", {})
            if hasattr(universe_df, "attrs") and isinstance(universe_df.attrs.get("market_context"), dict)
            else {}
        )
        mainline_context = (
            universe_df.attrs.get("mainline_context", {})
            if hasattr(universe_df, "attrs") and isinstance(universe_df.attrs.get("mainline_context"), dict)
            else {}
        )
        picks_df = plugin.run(universe_df=universe_df, as_of_date=effective_date, params=effective_params)
        if picks_df is None:
            picks_df = pd.DataFrame()
        portfolio_advice = self._attr_dict(picks_df, "portfolio_advice")
        observation_pool = self._attr_list(picks_df, "observation_pool")
        pool_split = self._attr_dict(picks_df, "pool_split")
        if not pool_split:
            pool_split = {
                "tradable_count": int(len(picks_df)),
                "observation_count": int(len(observation_pool)),
            }
        return {
            "market_context": market_context,
            "mainline_context": mainline_context,
            "picks_df": picks_df,
            "portfolio_advice": portfolio_advice,
            "observation_pool": observation_pool,
            "pool_split": pool_split,
        }

    def _evaluate_pool(
        self,
        strategy_id: str,
        trade_date: str,
        picks: pd.DataFrame | list[dict[str, Any]] | None,
        success_threshold_5d: float = 0.0,
        success_threshold_10d: float = 0.0,
    ) -> tuple[pd.DataFrame, list[dict[str, Any]], dict[str, Any]]:
        pick_frame = coerce_pick_frame(picks)
        eval_rows = build_forward_eval_rows(
            provider=self.data_provider,
            run_id=0,
            strategy_id=strategy_id,
            trade_date=trade_date,
            picks_df=pick_frame,
        )
        summary = summarize_eval_frame(
            picks_df=pick_frame,
            eval_rows=eval_rows,
            success_threshold_5d=success_threshold_5d,
            success_threshold_10d=success_threshold_10d,
        )
        return pick_frame, eval_rows, summary

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

        plugin, version, effective_params = self._resolve_effective_params(
            strategy_id=strategy_id,
            params=params,
        )
        effective_date = as_of_date or self.data_provider.latest_trade_date()

        run_id = self.repo.create_run(
            strategy_id=strategy_id,
            strategy_version=version,
            as_of_date=effective_date,
            params_json=effective_params,
        )

        execution = self._execute_plugin(
            plugin=plugin,
            effective_date=effective_date,
            effective_params=effective_params,
        )
        market_context = execution["market_context"]
        mainline_context = execution["mainline_context"]
        picks_df = execution["picks_df"]
        portfolio_advice = execution["portfolio_advice"]
        observation_pool = execution["observation_pool"]
        pool_split = execution["pool_split"]

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

        _, eval_rows, summary = self._evaluate_pool(
            strategy_id=strategy_id,
            trade_date=effective_date,
            picks=picks_df,
        )
        eval_rows = [{**row, "run_id": run_id} for row in eval_rows]
        if eval_rows:
            self.repo.upsert_eval_rows(eval_rows)

        _, _, observation_summary = self._evaluate_pool(
            strategy_id=strategy_id,
            trade_date=effective_date,
            picks=observation_pool,
        )
        summary["observation_pool"] = observation_pool
        summary["observation_summary"] = observation_summary
        summary["pool_split"] = pool_split
        if market_context:
            summary["market_context"] = market_context
        if mainline_context:
            summary["mainline_context"] = mainline_context
        if portfolio_advice:
            summary["portfolio_advice"] = portfolio_advice
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
            "context": {
                "market": market_context,
                "mainline": mainline_context,
                "portfolio_advice": portfolio_advice,
                "observation_pool": observation_pool,
                "pool_split": pool_split,
            },
            "detail": detail,
        }

    def backtest_strategy(
        self,
        strategy_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
        params: dict[str, Any] | None = None,
        success_threshold_5d: float = 0.0,
        success_threshold_10d: float = 0.0,
        include_daily: bool = False,
    ) -> dict[str, Any]:
        self.repo.ensure_schema()

        plugin, version, effective_params = self._resolve_effective_params(
            strategy_id=strategy_id,
            params=params,
        )
        latest_trade_date = self.data_provider.latest_trade_date()
        latest_ts = pd.Timestamp(latest_trade_date)
        requested_end_ts = pd.Timestamp(end_date) if end_date else latest_ts
        requested_end_ts = min(requested_end_ts, latest_ts)
        requested_start_ts = (
            pd.Timestamp(start_date)
            if start_date
            else requested_end_ts - pd.Timedelta(days=370)
        )
        if requested_start_ts > requested_end_ts:
            raise ValueError("回测开始日期不能晚于结束日期")

        requested_start = requested_start_ts.strftime("%Y-%m-%d")
        requested_end = requested_end_ts.strftime("%Y-%m-%d")
        trade_dates = self.data_provider.list_trade_dates(requested_start, requested_end)
        max_horizon = 10
        if len(trade_dates) <= max_horizon:
            raise ValueError("可回测交易日不足，无法完成 10 日前瞻评估")

        eval_dates = trade_dates[:-max_horizon]
        tail_dates = trade_dates[-max_horizon:]
        threshold_5d = max(0.0, float(success_threshold_5d))
        threshold_10d = max(0.0, float(success_threshold_10d))

        tradable_frames: list[pd.DataFrame] = []
        observation_frames: list[pd.DataFrame] = []
        tradable_eval_rows: list[dict[str, Any]] = []
        observation_eval_rows: list[dict[str, Any]] = []
        tradable_active_days = 0
        observation_active_days = 0
        day_seconds: list[float] = []
        daily_results: list[dict[str, Any]] = []

        total_start = time.perf_counter()
        for trade_date in eval_dates:
            day_start = time.perf_counter()
            execution = self._execute_plugin(
                plugin=plugin,
                effective_date=trade_date,
                effective_params=effective_params,
            )
            picks_df = execution["picks_df"]
            observation_pool = execution["observation_pool"]

            tradable_frame, tradable_day_eval, tradable_day_summary = self._evaluate_pool(
                strategy_id=strategy_id,
                trade_date=trade_date,
                picks=picks_df,
                success_threshold_5d=threshold_5d,
                success_threshold_10d=threshold_10d,
            )
            observation_frame, observation_day_eval, observation_day_summary = self._evaluate_pool(
                strategy_id=strategy_id,
                trade_date=trade_date,
                picks=observation_pool,
                success_threshold_5d=threshold_5d,
                success_threshold_10d=threshold_10d,
            )

            if not tradable_frame.empty:
                tradable_frames.append(tradable_frame)
                tradable_active_days += 1
            if not observation_frame.empty:
                observation_frames.append(observation_frame)
                observation_active_days += 1

            tradable_eval_rows.extend(tradable_day_eval)
            observation_eval_rows.extend(observation_day_eval)

            elapsed = time.perf_counter() - day_start
            day_seconds.append(elapsed)
            if include_daily:
                daily_results.append(
                    {
                        "trade_date": trade_date,
                        "tradable": {
                            "pick_count": int(tradable_day_summary.get("pick_count") or 0),
                            "hit_5d": tradable_day_summary.get("hit_5d"),
                            "hit_10d": tradable_day_summary.get("hit_10d"),
                            "ret_5d": tradable_day_summary.get("ret_5d"),
                            "ret_10d": tradable_day_summary.get("ret_10d"),
                        },
                        "observation": {
                            "pick_count": int(observation_day_summary.get("pick_count") or 0),
                            "hit_5d": observation_day_summary.get("hit_5d"),
                            "hit_10d": observation_day_summary.get("hit_10d"),
                            "ret_5d": observation_day_summary.get("ret_5d"),
                            "ret_10d": observation_day_summary.get("ret_10d"),
                        },
                        "elapsed_seconds": round(float(elapsed), 4),
                    }
                )

        total_seconds = time.perf_counter() - total_start

        tradable_picks_df = (
            pd.concat(tradable_frames, ignore_index=True)
            if tradable_frames
            else coerce_pick_frame(None)
        )
        observation_picks_df = (
            pd.concat(observation_frames, ignore_index=True)
            if observation_frames
            else coerce_pick_frame(None)
        )

        tradable_summary = summarize_eval_frame(
            picks_df=tradable_picks_df,
            eval_rows=tradable_eval_rows,
            success_threshold_5d=threshold_5d,
            success_threshold_10d=threshold_10d,
        )
        observation_summary = summarize_eval_frame(
            picks_df=observation_picks_df,
            eval_rows=observation_eval_rows,
            success_threshold_5d=threshold_5d,
            success_threshold_10d=threshold_10d,
        )
        tradable_summary = self._enrich_pool_summary(
            summary=tradable_summary,
            active_days=tradable_active_days,
            trade_day_count=len(eval_dates),
        )
        observation_summary = self._enrich_pool_summary(
            summary=observation_summary,
            active_days=observation_active_days,
            trade_day_count=len(eval_dates),
        )

        timing_series = pd.Series(day_seconds, dtype="float64")
        efficiency = {
            "evaluated_trade_days": int(len(eval_dates)),
            "total_trade_days_in_range": int(len(trade_dates)),
            "excluded_tail_days": int(len(tail_dates)),
            "total_seconds": round(float(total_seconds), 4),
            "avg_seconds_per_day": round(float(timing_series.mean()), 4) if not timing_series.empty else 0.0,
            "median_seconds_per_day": round(float(timing_series.median()), 4) if not timing_series.empty else 0.0,
            "p95_seconds_per_day": round(float(timing_series.quantile(0.95)), 4) if not timing_series.empty else 0.0,
            "max_seconds_per_day": round(float(timing_series.max()), 4) if not timing_series.empty else 0.0,
        }

        result = {
            "strategy_id": strategy_id,
            "strategy_name": plugin.display_name,
            "strategy_version": int(version),
            "requested_range": {
                "start_date": requested_start,
                "end_date": requested_end,
            },
            "evaluated_range": {
                "start_date": eval_dates[0],
                "end_date": eval_dates[-1],
                "excluded_tail_start": tail_dates[0],
                "excluded_tail_end": tail_dates[-1],
                "forward_horizon_days": max_horizon,
            },
            "params": effective_params,
            "tradable_pool": tradable_summary,
            "observation_pool": observation_summary,
            "efficiency": efficiency,
        }
        if include_daily:
            result["daily"] = daily_results
        return result

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

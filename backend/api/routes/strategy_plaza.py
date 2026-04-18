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


def _normalize_summary_payload(record: dict | None) -> dict | None:
    if not record:
        return None

    normalized = {}
    for key, value in record.items():
        if pd.isna(value):
            normalized[key] = None
        elif hasattr(value, "isoformat"):
            normalized[key] = value.isoformat()
        else:
            normalized[key] = value
    return normalized


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
        SELECT
            s.*,
            (
                SELECT COUNT(*)
                FROM strategy_observations o
                WHERE o.strategy_key = s.strategy_key
                  AND o.observation_date = ?
            ) AS same_day_observation_count
        FROM strategy_daily_summaries
        s
        WHERE s.strategy_key = ? AND s.trade_date = ?
        LIMIT 1
        """,
        [trade_date, strategy_key, trade_date],
    )
    summary = None if df.empty else _normalize_summary_payload(df.iloc[0].to_dict())
    if summary:
        same_day_count = summary.pop("same_day_observation_count", None)
        if same_day_count is not None and int(same_day_count or 0) <= 0:
            summary = None
    return {"status": "success", "data": {"summary": summary}}


@router.post("/strategy-plaza/run", status_code=202)
def trigger_strategy_run(params: StrategyPlazaRunParams):
    payload = params.model_dump()
    if not payload["trade_date"]:
        payload["trade_date"] = arrow.now("Asia/Shanghai").format("YYYY-MM-DD")
    task_key = f"strategy_plaza:{payload['trade_date']}:{payload.get('strategy_key') or 'all'}"
    task_id, _ = TaskRegistry.create_task("STRATEGY_PLAZA", payload, task_key=task_key)
    return {"message": "策略广场任务已加入队列", "task_id": task_id}

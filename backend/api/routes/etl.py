"""Minimal API for scheduling and observing raw data collection tasks."""

from __future__ import annotations

import asyncio
import gc
import json
import logging
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from db.connection import get_db_connection
from etl.sync import sync_engine


logger = logging.getLogger(__name__)
router = APIRouter(tags=["ETL"])

ALLOWED_TASKS = {
    "basic",
    "calendar",
    "price",
    "daily",
    "moneyflow",
    "daily_basic",
    "concepts",
    "index",
    "index_members",
    "express",
    "income",
    "financials",
    "balancesheet",
    "balance_sheet",
    "cashflow",
    "fina_indicator",
    "dividend",
    "forecast",
    "suspend",
    "namechange",
    "index_weight",
    "moneyflow_hsgt",
    "hsgt",
    "margin",
    "daily_update",
}


class SyncTaskParams(BaseModel):
    task: str
    years: int = Field(default=0, ge=0, le=20)
    days: int = Field(default=3, ge=1, le=3650)
    start_date: str | None = None
    end_date: str | None = None
    ts_code: str | None = None
    force: bool = False


class TaskRegistry:
    @staticmethod
    def create(params: SyncTaskParams) -> str:
        task_id = str(uuid.uuid4())
        with get_db_connection() as con:
            con.execute(
                """
                INSERT INTO etl_tasks (
                    task_id, task_key, task_type, params_json, status, progress
                ) VALUES (?, ?, 'SYNC', ?, 'PENDING', 0)
                """,
                (task_id, task_id, json.dumps(params.model_dump(), ensure_ascii=False)),
            )
        return task_id

    @staticmethod
    def pending() -> dict[str, Any] | None:
        with get_db_connection() as con:
            row = con.execute(
                """
                SELECT task_id, params_json
                FROM etl_tasks
                WHERE status = 'PENDING'
                ORDER BY created_at
                LIMIT 1
                """
            ).fetchone()
        if not row:
            return None
        raw_params = row[1]
        if isinstance(raw_params, str):
            raw_params = json.loads(raw_params)
        return {"task_id": row[0], "params": raw_params}

    @staticmethod
    def update(task_id: str, status: str, *, progress: float, error: str | None = None) -> None:
        with get_db_connection() as con:
            if status == "RUNNING":
                con.execute(
                    """
                    UPDATE etl_tasks
                    SET status = ?, progress = ?, error = ?,
                        started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
                        heartbeat_at = CURRENT_TIMESTAMP
                    WHERE task_id = ?
                    """,
                    (status, progress, error, task_id),
                )
            else:
                con.execute(
                    """
                    UPDATE etl_tasks
                    SET status = ?, progress = ?, error = ?,
                        heartbeat_at = CURRENT_TIMESTAMP,
                        finished_at = CURRENT_TIMESTAMP
                    WHERE task_id = ?
                    """,
                    (status, progress, error, task_id),
                )


def _run_sync(params: dict[str, Any]) -> None:
    task = str(params["task"]).lower()
    years = int(params.get("years") or 0)
    days = int(params.get("days") or 3)
    force = bool(params.get("force"))
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    ts_code = params.get("ts_code")

    if task == "basic":
        sync_engine.sync_stock_basic()
    elif task == "calendar":
        sync_engine.sync_trade_calendar(start_date or "2020-01-01", end_date or "2030-12-31")
    elif task in {"price", "daily"}:
        if start_date and end_date and start_date == end_date:
            sync_engine.sync_daily_data_by_date(start_date)
        else:
            sync_engine.sync_daily_market_data(years=max(years, 1), force=force)
    elif task == "moneyflow":
        sync_engine.sync_capital_flow(years=years, days=days, force=force)
    elif task == "daily_basic":
        sync_engine.sync_daily_basic(years=years, days=days, force=force)
    elif task == "concepts":
        sync_engine.sync_concept_classification()
    elif task == "index":
        if ts_code:
            sync_engine.sync_market_index(ts_code=ts_code, years=years, days=days)
        else:
            sync_engine.sync_core_market_indices(years=years, days=days)
    elif task == "index_members":
        sync_engine.sync_index_member_all()
    elif task == "express":
        sync_engine.sync_express_data(days=days, start_date=start_date, end_date=end_date)
    elif task in {"income", "financials"}:
        sync_engine.sync_quarterly_income_statement(ts_code=ts_code, force=force)
    elif task in {"balancesheet", "balance_sheet"}:
        sync_engine.sync_balance_sheet(ts_code=ts_code, force=force)
    elif task == "cashflow":
        sync_engine.sync_cashflow(ts_code=ts_code, force=force)
    elif task == "fina_indicator":
        sync_engine.sync_financial_indicators(ts_code=ts_code)
    elif task == "dividend":
        sync_engine.sync_dividend_data(ts_code=ts_code)
    elif task == "forecast":
        sync_engine.sync_forecast_data(days=days, start_date=start_date, end_date=end_date, ts_code=ts_code)
    elif task == "suspend":
        sync_engine.sync_suspend_data(years=years, days=days, force=force)
    elif task == "namechange":
        sync_engine.sync_namechange_data(ts_code=ts_code)
    elif task == "index_weight":
        sync_engine.sync_index_weight_data(index_code=ts_code, start_date=start_date, end_date=end_date, days=days)
    elif task in {"moneyflow_hsgt", "hsgt"}:
        sync_engine.sync_moneyflow_hsgt(days=days, start_date=start_date, end_date=end_date)
    elif task == "margin":
        sync_engine.sync_margin_trading_data(days=days)
    elif task == "daily_update":
        sync_engine.perform_daily_data_update()
    else:
        raise ValueError(f"不支持的采集任务: {task}")


async def task_worker() -> None:
    logger.info("原始数据采集任务消费者已启动")
    while True:
        task = TaskRegistry.pending()
        if task is None:
            await asyncio.sleep(5)
            continue
        task_id = task["task_id"]
        TaskRegistry.update(task_id, "RUNNING", progress=1)
        try:
            await asyncio.to_thread(_run_sync, task["params"])
            TaskRegistry.update(task_id, "COMPLETED", progress=100)
        except Exception as exc:
            logger.exception("采集任务 %s 失败", task_id)
            TaskRegistry.update(task_id, "FAILED", progress=0, error=str(exc))
        finally:
            gc.collect()
        await asyncio.sleep(1)


@router.post("/etl/sync", status_code=202)
async def trigger_sync(params: SyncTaskParams):
    task = params.task.lower()
    if task not in ALLOWED_TASKS:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的采集任务，可选值: {', '.join(sorted(ALLOWED_TASKS))}",
        )
    params.task = task
    task_id = TaskRegistry.create(params)
    return {"message": "采集任务已加入队列", "task_id": task_id}


@router.get("/tasks/status")
def get_tasks_status(limit: int = 20):
    limit = max(1, min(100, limit))
    with get_db_connection() as con:
        rows = con.execute(
            """
            SELECT task_id, task_type, status, error, progress,
                   CAST(created_at AS VARCHAR), CAST(finished_at AS VARCHAR)
            FROM etl_tasks
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return {
        "tasks": [
            {
                "task_id": row[0],
                "task_type": row[1],
                "status": row[2],
                "error": row[3],
                "progress": row[4],
                "created_at": row[5],
                "finished_at": row[6],
            }
            for row in rows
        ]
    }

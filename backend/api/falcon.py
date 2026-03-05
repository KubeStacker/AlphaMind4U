from __future__ import annotations

from fastapi import APIRouter, HTTPException

from strategy.falcon.engine import falcon_service
from strategy.falcon.models import (
    FalconBatchRunsRequest,
    FalconDeleteRangeRequest,
    FalconDeleteRequest,
    FalconEvolveRequest,
    FalconRestoreRequest,
    FalconRunRequest,
)


router = APIRouter(prefix="/falcon", tags=["Falcon"])


@router.get("/strategies")
def list_falcon_strategies():
    return {"status": "success", "data": falcon_service.list_strategies()}


@router.get("/latest_trade_date")
def get_falcon_latest_trade_date():
    try:
        d = falcon_service.latest_trade_date()
        return {"status": "success", "data": {"trade_date": d}}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/run")
def run_falcon(req: FalconRunRequest):
    try:
        data = falcon_service.run_strategy(
            strategy_id=req.strategy_id,
            as_of_date=req.trade_date,
            params=req.params,
        )
        return {"status": "success", "data": data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/runs")
def list_falcon_runs(strategy_id: str | None = None, limit: int = 50):
    try:
        rows = falcon_service.list_runs(strategy_id=strategy_id, limit=limit)
        return {"status": "success", "data": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/runs/deleted")
def list_falcon_deleted_runs(strategy_id: str | None = None, limit: int = 50):
    try:
        rows = falcon_service.list_deleted_runs(strategy_id=strategy_id, limit=limit)
        return {"status": "success", "data": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/ops")
def list_falcon_ops(strategy_id: str | None = None, limit: int = 20):
    try:
        rows = falcon_service.list_operation_logs(strategy_id=strategy_id, limit=limit)
        return {"status": "success", "data": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/runs/{run_id}")
def get_falcon_run(run_id: int):
    data = falcon_service.get_run_detail(run_id=run_id)
    if not data:
        raise HTTPException(status_code=404, detail="运行记录不存在")
    return {"status": "success", "data": data}


@router.delete("/runs")
def delete_falcon_run(req: FalconDeleteRequest):
    ok = falcon_service.hard_delete_run(req.run_id) if req.hard else falcon_service.delete_run(req.run_id, operator="api")
    if not ok:
        raise HTTPException(status_code=404, detail="运行记录不存在")
    return {"status": "success", "message": "删除成功"}


@router.post("/runs/delete_range")
def delete_falcon_runs_by_range(req: FalconDeleteRangeRequest):
    try:
        count = falcon_service.delete_runs_by_range(
            strategy_id=req.strategy_id,
            start_date=req.start_date,
            end_date=req.end_date,
            operator="api",
        )
        return {"status": "success", "data": {"deleted_count": count}}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/runs/restore")
def restore_falcon_run(req: FalconRestoreRequest):
    ok = falcon_service.restore_run(req.run_id)
    if not ok:
        raise HTTPException(status_code=404, detail="运行记录不存在")
    return {"status": "success", "message": "恢复成功"}


@router.post("/runs/restore_batch")
def restore_falcon_runs_batch(req: FalconBatchRunsRequest):
    try:
        count = falcon_service.restore_runs(req.run_ids)
        return {"status": "success", "data": {"restored_count": count}}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/runs/hard_delete_batch")
def hard_delete_falcon_runs_batch(req: FalconBatchRunsRequest):
    try:
        count = falcon_service.hard_delete_runs(req.run_ids)
        return {"status": "success", "data": {"deleted_count": count}}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/score")
def get_falcon_strategy_score(strategy_id: str, trade_date: str):
    data = falcon_service.get_strategy_daily_score(strategy_id=strategy_id, trade_date=trade_date)
    if not data:
        raise HTTPException(status_code=404, detail="该策略当日评分不存在")
    return {"status": "success", "data": data}


@router.post("/evolve")
def evolve_falcon(req: FalconEvolveRequest):
    try:
        data = falcon_service.evolve(strategy_id=req.strategy_id)
        return {"status": "success", "data": data}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

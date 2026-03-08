from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class FalconRunRequest(BaseModel):
    strategy_id: str = Field(..., min_length=1)
    trade_date: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class FalconBacktestRequest(BaseModel):
    strategy_id: str = Field(..., min_length=1)
    start_date: str | None = None
    end_date: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    success_threshold_5d: float = 0.0
    success_threshold_10d: float = 0.0
    include_daily: bool = False


class FalconDeleteRequest(BaseModel):
    run_id: int
    hard: bool = False


class FalconRestoreRequest(BaseModel):
    run_id: int


class FalconEvolveRequest(BaseModel):
    strategy_id: str


class FalconDeleteRangeRequest(BaseModel):
    strategy_id: str
    start_date: str
    end_date: str


class FalconBatchRunsRequest(BaseModel):
    run_ids: list[int] = Field(default_factory=list)

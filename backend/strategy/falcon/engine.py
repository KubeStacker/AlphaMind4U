from __future__ import annotations

from db.falcon_repository import DuckDbFalconRepository
from etl.falcon_data_provider import DuckDbFalconDataProvider
from strategy.falcon.application import FalconApplicationService


def build_falcon_service() -> FalconApplicationService:
    repo = DuckDbFalconRepository()
    provider = DuckDbFalconDataProvider()
    return FalconApplicationService(repo=repo, data_provider=provider)


falcon_service = build_falcon_service()

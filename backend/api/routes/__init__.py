# /backend/api/routes/__init__.py

from .users import router as users_router
from .ai import router as ai_router
from .stocks import router as stocks_router
from .market import router as market_router
from .etl import router as etl_router
from .db import router as db_router
from .system import router as system_router
from .docs import router as docs_router

__all__ = [
    "users_router",
    "ai_router",
    "stocks_router",
    "market_router",
    "etl_router",
    "db_router",
    "system_router",
    "docs_router"
]
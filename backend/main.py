# /backend/main.py

import logging
import asyncio
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse

from core.logging import setup_logging

# 初始化集中式日志系统 (必须在其他 import 之前)
setup_logging()

from contextlib import asynccontextmanager
from db.init_db import initialize_database
from core.config import settings
from db.connection import close_connection, get_connection
from etl.scheduler import start_scheduler
from core.security import is_valid_static_auth_request

from api.routes import datasets_router, etl_router

logger = logging.getLogger(__name__)

# 定义 FastAPI 应用的生命周期事件
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 应用的生命周期管理器。
    在应用启动时执行数据库初始化。
    """
    logger.info("FastAPI 应用启动中...")
    # 1. 初始化数据库
    initialize_database()
    # 1.1 预热共享 DuckDB 连接（进程内单连接）
    get_connection()
    
    # 2. 启动任务中心消费者 (处理顺序同步任务)
    if settings.enable_etl_worker:
        from api.routes.etl import task_worker
        asyncio.create_task(task_worker())
    else:
        logger.info("ETL 任务消费者已按配置禁用")
    
    # 3. 启动定时任务调度器
    if settings.enable_scheduler:
        try:
            start_scheduler()
        except Exception as e:
            logger.error(f"调度器启动失败: {e}")
    else:
        logger.info("定时任务调度器已按配置禁用")
        
    logger.info("FastAPI 应用启动完成。")
    yield
    # 应用程序关闭时执行的逻辑
    logger.info("正在关闭资源...")
    close_connection()
    logger.info("FastAPI 应用关闭。")

# 创建 FastAPI 应用实例
app = FastAPI(
    title="Jarvis Data Backend",
    description="A-share data collection and read-only MCP data backend.",
    version="1.0.0",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.middleware("http")
async def require_static_token_for_admin(request: Request, call_next):
    if request.method != "OPTIONS" and request.url.path.startswith("/admin"):
        if not is_valid_static_auth_request(request):
            return JSONResponse(
                {"detail": "静态令牌无效或缺失"},
                status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
    return await call_next(request)


# 仅注册数据采集与 MCP 数据读取路由。
app.include_router(etl_router, prefix="/admin")
app.include_router(datasets_router, prefix="/admin")

@app.get("/", tags=["System"])
async def read_root():
    """
    根路径接口，用于健康检查或返回基本信息。
    """
    return {"status": "ok", "service": "jarvis-data-backend"}

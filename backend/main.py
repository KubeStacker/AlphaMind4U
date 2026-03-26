# /backend/main.py

import logging
import datetime
import asyncio
from fastapi import FastAPI
import pytz

# 配置全局日志 - 使用上海时区
shanghai_tz = pytz.timezone('Asia/Shanghai')

class ShanghaiTimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(record.created, tz=shanghai_tz)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime('%Y-%m-%d %H:%M:%S')

# 配置全局日志
handler = logging.StreamHandler()
handler.setFormatter(ShanghaiTimeFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logging.root.addHandler(handler)
logging.root.setLevel(logging.INFO)
logging.getLogger('strategy.sentiment.analyst').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

from contextlib import asynccontextmanager
from db.init_db import initialize_database
from db.connection import close_connection, get_connection
from api import auth
from etl.scheduler import start_scheduler

# 导入新的路由模块
from api.routes import (
    users_router,
    ai_router,
    stocks_router,
    market_router,
    etl_router,
    db_router,
    system_router,
    docs_router
)

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
    from api.routes.etl import task_worker
    asyncio.create_task(task_worker())
    
    # 3. 启动定时任务调度器
    try:
        start_scheduler()
    except Exception as e:
        logger.error(f"调度器启动失败: {e}")
        
    logger.info("FastAPI 应用启动完成。")
    yield
    # 应用程序关闭时执行的逻辑
    logger.info("正在关闭资源...")
    close_connection()
    logger.info("FastAPI 应用关闭。")

# 创建 FastAPI 应用实例
app = FastAPI(
    title="Jarvis-Quant Backend",
    description="A lightweight, modular A-share quantitative decision system.",
    version="0.2.0",
    lifespan=lifespan # 注册生命周期事件
)

# 注册 API 路由（添加 /admin 前缀以兼容前端调用）
app.include_router(auth.router)
app.include_router(users_router, prefix="/admin")
app.include_router(ai_router, prefix="/admin")
app.include_router(stocks_router, prefix="/admin")
app.include_router(market_router, prefix="/admin")
app.include_router(etl_router, prefix="/admin")
app.include_router(db_router, prefix="/admin")
app.include_router(system_router, prefix="/admin")
app.include_router(docs_router, prefix="/admin")

@app.get("/", tags=["System"])
async def read_root():
    """
    根路径接口，用于健康检查或返回基本信息。
    """
    return {"message": "Welcome to Jarvis-Quant Backend!"}
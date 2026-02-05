# /backend/main.py

import datetime
from fastapi import FastAPI
from contextlib import asynccontextmanager
from db.init_db import initialize_database
from db.connection import close_connection
from api import admin
from api import strategy
from api import auth
from core.scheduler import start_scheduler

# 定义 FastAPI 应用的生命周期事件
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 应用的生命周期管理器。
    在应用启动时执行数据库初始化。
    """
    print("FastAPI 应用启动中...")
    # 应用程序启动时执行的逻辑
    initialize_database()
    try:
        start_scheduler()
    except Exception as e:
        print(f"调度器启动失败: {e}")
        
    print("FastAPI 应用启动完成。")
    yield
    # 应用程序关闭时执行的逻辑
    print("正在关闭资源...")
    close_connection()
    print("FastAPI 应用关闭。")

# 创建 FastAPI 应用实例
app = FastAPI(
    title="Jarvis-Quant Backend",
    description="A lightweight, modular A-share quantitative decision system.",
    version="0.1.0",
    lifespan=lifespan # 注册生命周期事件
)

# 注册 API 路由
app.include_router(admin.router)
app.include_router(strategy.router)
app.include_router(auth.router)

@app.get("/")
async def read_root():
    """
    根路径接口，用于健康检查或返回基本信息。
    """
    return {"message": "Welcome to Jarvis-Quant Backend!"}

# 可以在此处添加更多的中间件、事件处理器等

from core.calendar import trading_calendar

@app.get("/system/status", tags=["System"])
def get_system_status():
    """ 返回当前系统和市场的状态 """
    is_trading = trading_calendar.is_trading_time()
    return {
        "market_status": "TRADING" if is_trading else "CLOSED",
        "timestamp": datetime.datetime.now()
    }
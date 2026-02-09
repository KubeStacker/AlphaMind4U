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

@app.get("/system/trigger_star50", tags=["System"])
def trigger_star50():
    """ 触发科创50策略回测 """
    from strategy.recommend.plugins.backtest_star50 import run_backtest
    res = run_backtest()
    return {"status": "success" if res else "failed", "data": res}

@app.get("/system/migrate_db", tags=["System"])
def migrate_db():
    """ 数据库结构迁移 """
    from db.connection import get_db_connection
    try:
        with get_db_connection() as con:
            con.execute("ALTER TABLE strategy_recommendations ADD COLUMN p1_return DOUBLE;")
            con.execute("ALTER TABLE strategy_recommendations ADD COLUMN p3_return DOUBLE;")
        return {"status": "success", "message": "Columns added."}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/system/verify_p5", tags=["System"])
def verify_p5():
    """ 强制计算指定日期的收益率 """
    from strategy.recommend import get_plugin
    from db.connection import fetch_df
    recommend_date = "2026-02-04"
    rec_query = f"SELECT ts_code FROM strategy_recommendations WHERE recommend_date = '{recommend_date}'"
    recs = fetch_df(rec_query)
    date_query = f"SELECT DISTINCT trade_date FROM daily_price WHERE trade_date >= '{recommend_date}' ORDER BY trade_date ASC LIMIT 15"
    dates_df = fetch_df(date_query)
    
    backtester = get_plugin("backtester")
    res = backtester.calculate_returns_for_date(recommend_date)
    return {
        "recs_found": len(recs),
        "trading_dates_found": [str(d) for d in dates_df['trade_date'].tolist()],
        "updated_count": res
    }

@app.get("/system/status", tags=["System"])
def get_system_status():
    """ 返回当前系统和市场的状态 """
    is_trading = trading_calendar.is_trading_time()
    return {
        "market_status": "TRADING" if is_trading else "CLOSED",
        "timestamp": datetime.datetime.now()
    }

@app.get("/system/db_check", tags=["System"])
def db_check():
    """ 分析历史极端日的行情特征 """
    from db.connection import fetch_df
    try:
        # 分析 20250407, 20250903, 20251121
        panic_dates = ['2025-04-07', '2025-09-03', '2025-11-21']
        # 注意：由于 daily_price 是按 trade_date, ts_code 存储的，我们需要聚合查询
        query = f"""
        SELECT 
            trade_date,
            COUNT(*) as total_stocks,
            SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) as up_count,
            SUM(CASE WHEN pct_chg < -9.8 THEN 1 ELSE 0 END) as limit_downs,
            MEDIAN(pct_chg) as median_ret
        FROM daily_price 
        WHERE trade_date IN ('2025-04-07', '2025-09-03', '2025-11-21')
        GROUP BY trade_date
        ORDER BY trade_date
        """
        analysis = fetch_df(query).to_dict('records')
        return {
            "panic_days_pathology": analysis
        }
    except Exception as e:
        return {"error": str(e)}
# /backend/api/routes/system.py

import logging
import datetime
from fastapi import APIRouter, BackgroundTasks
from db.connection import get_db_connection, fetch_df
from db.schema import (
    CREATE_STOCK_DAILY_BASIC_TABLE_SQL,
    CREATE_STOCK_EXPRESS_TABLE_SQL,
    CREATE_STOCK_FINA_INDICATOR_TABLE_SQL,
    CREATE_STOCK_FACTOR_DAILY_TABLE_SQL,
    CREATE_STOCK_INCOME_TABLE_SQL,
    CREATE_STOCK_INDEX_MEMBER_ALL_TABLE_SQL,
)
from etl.sync import sync_engine
from etl.calendar import trading_calendar

logger = logging.getLogger(__name__)
router = APIRouter(tags=["System"])

@router.get("/system/migrate_db")
def migrate_db():
    return {"status": "ok", "message": "Database already up to date"}

@router.get("/system/create_tables")
def create_missing_tables():
    """创建缺失的数据库表"""
    tables_created = []

    with get_db_connection() as con:
        for table_name, table_sql in (
            ("stock_income", CREATE_STOCK_INCOME_TABLE_SQL),
            ("stock_fina_indicator", CREATE_STOCK_FINA_INDICATOR_TABLE_SQL),
            ("stock_daily_basic", CREATE_STOCK_DAILY_BASIC_TABLE_SQL),
            ("stock_index_member_all", CREATE_STOCK_INDEX_MEMBER_ALL_TABLE_SQL),
            ("stock_express", CREATE_STOCK_EXPRESS_TABLE_SQL),
            ("stock_factor_daily", CREATE_STOCK_FACTOR_DAILY_TABLE_SQL),
        ):
            try:
                con.execute(table_sql)
                tables_created.append(table_name)
            except Exception:
                pass

    return {"status": "success", "tables_created": tables_created}

@router.get("/system/cleanup_tables")
def cleanup_unused_tables():
    """清理未使用的数据库表"""
    unused_tables = [
        "stock_financials", 
        "corporate_actions",
        "macro_events",
        "margin_market",
        "realtime_quotes",
        "strategy_recommendations"
    ]
    
    tables_dropped = []
    
    with get_db_connection() as con:
        for table in unused_tables:
            try:
                con.execute(f"DROP TABLE IF EXISTS {table}")
                tables_dropped.append(table)
            except Exception as e:
                pass
    
    return {"status": "success", "tables_dropped": tables_dropped}

@router.get("/system/trigger_daily_sync")
def trigger_daily_sync():
    """ 手动触发每日收盘同步 """
    try:
        sync_engine.perform_daily_data_update()
        return {"status": "success", "message": "Daily sync triggered and completed."}
    except Exception as e:
        logger.error(f"Manual sync failed: {e}")
        return {"status": "error", "message": str(e)}

@router.get("/system/backfill_history")
async def backfill_history(background_tasks: BackgroundTasks, days: int = 3):
    """ 异步补全历史数据 """
    from etl.utils.backfill import safe_backfill
    background_tasks.add_task(safe_backfill, days)
    return {"status": "success", "message": f"Backfill for last {days} days started in background."}

@router.get("/system/status")
def get_system_status():
    """ 返回当前系统和市场的状态 """
    is_trading = trading_calendar.is_trading_time()
    return {
        "market_status": "TRADING" if is_trading else "CLOSED",
        "timestamp": datetime.datetime.now()
    }

@router.get("/system/db_check")
def db_check():
    """ 分析历史极端日的行情特征 """
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

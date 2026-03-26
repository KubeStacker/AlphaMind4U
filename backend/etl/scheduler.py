# /backend/etl/scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from etl.sync import sync_engine
import logging
import pytz

logger = logging.getLogger(__name__)

# 上海时区
SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')

# 创建全局调度器实例，指定时区为上海
scheduler = AsyncIOScheduler(timezone=SHANGHAI_TZ)

def start_scheduler():
    """启动定时数据采集任务调度器"""
    
    # 1. 每日凌晨 00:05 更新基础数据 (股票列表)
    scheduler.add_job(
        sync_engine.sync_stock_basic,
        CronTrigger(hour=0, minute=5, timezone=SHANGHAI_TZ),
        id="sync_stock_basic",
        name="同步股票基础信息",
        replace_existing=True
    )
    
    # 2. 每日凌晨 00:10 更新概念分类
    scheduler.add_job(
        sync_engine.sync_concept_classification,
        CronTrigger(hour=0, minute=10, timezone=SHANGHAI_TZ),
        id="sync_concept_classification",
        name="同步概念分类数据",
        replace_existing=True
    )

    # 3. 每日收盘后更新（主任务 + 兜底任务，避免数据源延迟）
    scheduler.add_job(
        sync_engine.perform_daily_data_update,
        CronTrigger(hour=16, minute=45, timezone=SHANGHAI_TZ),
        id="daily_data_update",
        name="每日收盘数据更新(主)",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
        replace_existing=True
    )
    
    # 4. 每日收盘后更新兜底任务（18:30）
    scheduler.add_job(
        sync_engine.perform_daily_data_update,
        CronTrigger(hour=18, minute=30, timezone=SHANGHAI_TZ),
        id="daily_data_update_fallback",
        name="每日收盘数据更新(兜底)",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
        replace_existing=True
    )

    # 5. 每周日凌晨 02:00 同步财务报表数据
    scheduler.add_job(
        sync_engine.sync_financial_statements,
        CronTrigger(day_of_week='sun', hour=2, minute=0, timezone=SHANGHAI_TZ),
        id="sync_financial_statements",
        name="每周财务报表同步",
        kwargs={"limit": 1000},
        replace_existing=True
    )

    # 6. 每日 08:00 同步外汇数据 (美元指数、离岸人民币 - 最近7天)
    scheduler.add_job(
        sync_engine.sync_forex_data,
        CronTrigger(hour=8, minute=0, timezone=SHANGHAI_TZ),
        id="sync_forex_data",
        name="每日外汇数据同步",
        replace_existing=True
    )


    scheduler.start()
    logger.info(
        "定时任务调度器已启动:\n"
        "  - 基础数据: 00:05 (股票列表) / 00:10 (概念分类)\n"
        "  - 收盘数据: 16:45 (主) / 18:30 (兜底)\n"
        "  - 财务报表: 周日 02:00\n"
        "  - 外汇数据: 08:00"
    )

# /backend/core/scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from etl.sync import sync_engine
import logging

logger = logging.getLogger(__name__)

# 创建全局调度器实例
scheduler = AsyncIOScheduler()

def start_scheduler():
    """ 启动定时任务调度器 """
    
    # 1. 每日凌晨 00:05 更新基础数据 (概念等)
    scheduler.add_job(
        sync_engine.sync_stock_basic,
        CronTrigger(hour=0, minute=5),
        id="sync_stock_basic",
        name="同步股票列表",
        replace_existing=True
    )
    scheduler.add_job(
        sync_engine.sync_concepts,
        CronTrigger(hour=0, minute=10),
        id="sync_concepts",
        name="同步概念分类",
        replace_existing=True
    )

    # 2. 每日 15:35 更新收盘数据 (改为每天执行，以防周五数据在节假日漏补)
    scheduler.add_job(
        sync_engine.sync_daily_update,
        CronTrigger(hour=15, minute=35),
        id="sync_daily_update",
        name="每日收盘行情更新",
        replace_existing=True
    )

    # 3. 每周日凌晨 02:00 同步财务指标
    scheduler.add_job(
        sync_engine.sync_financials,
        CronTrigger(day_of_week='sun', hour=2, minute=0),
        id="sync_financials",
        name="每周财务数据同步",
        kwargs={"limit": 1000},
        replace_existing=True
    )


    scheduler.start()
    logger.info("定时任务调度器已启动: [基础数据: 00:05] [收盘数据: 15:35]")

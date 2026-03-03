# /backend/core/scheduler.py

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from etl.sync import sync_engine
import logging
import pytz

logger = logging.getLogger(__name__)

# 上海时区
SHANGHAI_TZ = pytz.timezone('Asia/Shanghai')

# 创建全局调度器实例，指定时区为上海
scheduler = AsyncIOScheduler(timezone=SHANGHAI_TZ)

def start_scheduler():
    """ 启动定时任务 """
    
    # 1. 每日凌晨 00:05 更新基础数据 (概念等)
    scheduler.add_job(
        sync_engine.sync_stock_basic,
        CronTrigger(hour=0, minute=5, timezone=SHANGHAI_TZ),
        id="sync_stock_basic",
        name="同步股票列表",
        replace_existing=True
    )
    scheduler.add_job(
        sync_engine.sync_concepts,
        CronTrigger(hour=0, minute=10, timezone=SHANGHAI_TZ),
        id="sync_concepts",
        name="同步概念分类",
        replace_existing=True
    )

    # 2. 每日收盘后更新（主任务 + 兜底任务，避免数据源延迟）
    scheduler.add_job(
        sync_engine.sync_daily_update,
        CronTrigger(hour=16, minute=45, timezone=SHANGHAI_TZ),
        id="sync_daily_update",
        name="每日收盘行情更新",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
        replace_existing=True
    )
    scheduler.add_job(
        sync_engine.sync_daily_update,
        CronTrigger(hour=18, minute=30, timezone=SHANGHAI_TZ),
        id="sync_daily_update_fallback",
        name="每日收盘行情更新(兜底)",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
        replace_existing=True
    )

    # 3. 每周日凌晨 02:00 同步财务指标
    scheduler.add_job(
        sync_engine.sync_financials,
        CronTrigger(day_of_week='sun', hour=2, minute=0, timezone=SHANGHAI_TZ),
        id="sync_financials",
        name="每周财务数据同步",
        kwargs={"limit": 1000},
        replace_existing=True
    )

    # 4. 每日同步外汇数据 (美元指数、离岸人民币 - 最近7天)
    scheduler.add_job(
        sync_engine.sync_fx,
        CronTrigger(hour=8, minute=0, timezone=SHANGHAI_TZ),
        id="sync_fx",
        name="每日外汇数据同步",
        replace_existing=True
    )


    scheduler.start()
    logger.info(
        "定时任务调度器已启动: [基础数据: 00:05] [收盘数据: 16:45/18:30] [外汇: 08:00]"
    )

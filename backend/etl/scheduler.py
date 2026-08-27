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

# ============================================================
# 任务分组定义
# ============================================================

TASK_GROUPS: dict[str, list[str]] = {
    "基础数据": [
        "sync_stock_basic",
        "sync_concept_classification",
    ],
    "行情数据": [
        "daily_data_update",
        "daily_data_update_fallback",
    ],
    "资金融资": [
        "sync_margin_trading",
    ],
    "财务数据": [
        "sync_financial_statements",
    ],
}


def start_scheduler():
    """启动定时数据采集任务调度器"""
    if scheduler.running:
        logger.info("定时任务调度器已在运行，跳过重复启动")
        return

    # ---- 基础数据 ----
    scheduler.add_job(
        sync_engine.sync_stock_basic,
        CronTrigger(hour=0, minute=5, timezone=SHANGHAI_TZ),
        id="sync_stock_basic",
        name="同步股票基础信息",
        replace_existing=True,
    )
    # 概念分类同步暂禁用: 会触发 DuckDB 主键冲突
    # scheduler.add_job(
    #     sync_engine.sync_concept_classification,
    #     CronTrigger(hour=0, minute=10, timezone=SHANGHAI_TZ),
    #     id="sync_concept_classification",
    #     name="同步概念分类数据",
    #     replace_existing=True,
    # )

    # ---- 行情数据 ----
    scheduler.add_job(
        sync_engine.perform_daily_data_update,
        CronTrigger(hour=16, minute=45, timezone=SHANGHAI_TZ),
        id="daily_data_update",
        name="每日收盘数据更新(主)",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
        replace_existing=True,
    )

    scheduler.add_job(
        sync_engine.perform_daily_data_update,
        CronTrigger(hour=18, minute=30, timezone=SHANGHAI_TZ),
        id="daily_data_update_fallback",
        name="每日收盘数据更新(兜底)",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
        replace_existing=True,
    )

    # ---- 资金融资 ----
    scheduler.add_job(
        sync_engine.sync_margin_trading_data,
        CronTrigger(hour=19, minute=7, timezone=SHANGHAI_TZ),
        id="sync_margin_trading",
        name="每日融资融券数据同步",
        kwargs={"days": 3},
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1,
        replace_existing=True,
    )

    # ---- 财务数据 ----
    scheduler.add_job(
        sync_engine.sync_financial_statements,
        CronTrigger(day_of_week='sun', hour=2, minute=0, timezone=SHANGHAI_TZ),
        id="sync_financial_statements",
        name="每周财务报表同步",
        kwargs={"limit": 1000},
        replace_existing=True,
    )

    scheduler.start()

    # 启动日志 — 按分组展示
    _log_startup_summary()


def _log_startup_summary():
    """打印分组启动摘要"""
    schedule_map = {}
    for job in scheduler.get_jobs():
        schedule_map[job.id] = job

    lines = ["定时任务调度器已启动:"]

    for group_name, job_ids in TASK_GROUPS.items():
        parts = []
        for jid in job_ids:
            job = schedule_map.get(jid)
            if job:
                trigger = job.trigger
                if hasattr(trigger, 'fields') and callable(getattr(trigger, 'fields', None)):
                    fields = trigger.fields
                else:
                    fields = []
                # 生成时间描述
                time_desc = _describe_trigger(trigger)
                parts.append(f"{job.name}({time_desc})")
            else:
                parts.append(f"{jid}(已禁用)")
        if parts:
            lines.append(f"  [{group_name}] {' | '.join(parts)}")

    logger.info("\n".join(lines))


def _describe_trigger(trigger) -> str:
    """生成 CronTrigger 的人类可读描述"""
    from apscheduler.triggers.cron.expressions import AllExpression, WeekdayRangeExpression

    values: dict[str, str] = {}
    if hasattr(trigger, 'fields'):
        for f in trigger.fields:
            name = f.name
            exprs = f.expressions
            if not exprs:
                values[name] = "*"
                continue
            e = exprs[0]
            if isinstance(e, WeekdayRangeExpression):
                # Weekday 返回 'sun','mon' 等, 用 str() 获取
                values[name] = str(e)
            elif hasattr(e, 'first'):
                values[name] = str(e.first)
            elif isinstance(e, AllExpression):
                values[name] = "*"
            else:
                values[name] = str(e)

    hour = values.get("hour", "*")
    minute = values.get("minute", "*")
    dow = values.get("day_of_week", "*")

    if dow not in ("*", "?"):
        dow_map = {"sun": "周日", "mon": "周一", "tue": "周二", "wed": "周三",
                    "thu": "周四", "fri": "周五", "sat": "周六"}
        day_label = dow_map.get(dow.lower(), dow)
        return f"{day_label} {hour}:{minute}"
    return f"{hour}:{minute}"

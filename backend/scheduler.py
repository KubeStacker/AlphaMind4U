"""
定时任务调度器 - 重构版
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from services.data_collection_service import DataCollectionService
from etl.trade_date_adapter import TradeDateAdapter
from config import Config
import logging
from datetime import datetime, date

logger = logging.getLogger(__name__)

def collect_natural_day_data():
    """自然日数据采集任务（每天18点执行）"""
    try:
        service = DataCollectionService()
        
        # 采集热度榜数据（自然日数据）
        service.collect_hot_rank_data()
        
        logger.info("自然日数据采集任务完成")
    except Exception as e:
        logger.error(f"自然日数据采集失败: {e}", exc_info=True)

def collect_trading_day_data():
    """交易日数据采集任务（仅在交易日晚上20点执行）"""
    try:
        today = date.today()
        
        # 判断是否为交易日
        if not TradeDateAdapter.is_trading_day(today):
            logger.info(f"{today} 不是交易日，跳过交易日数据采集")
            return
        
        service = DataCollectionService()
        
        # 采集股票日K数据（仅在交易日）
        service.collect_stock_daily_data()
        
        # 采集资金流向数据（仅在交易日）
        service.collect_money_flow_data()
        
        logger.info("交易日数据采集任务完成")
    except Exception as e:
        logger.error(f"交易日数据采集失败: {e}", exc_info=True)

def collect_concept_data():
    """概念板块数据采集任务（每天凌晨3点执行）"""
    try:
        service = DataCollectionService()
        service.collect_concept_data()
        logger.info("概念板块数据采集任务完成")
    except Exception as e:
        logger.error(f"概念板块数据采集失败: {e}", exc_info=True)

def start_scheduler():
    """启动定时任务调度器"""
    scheduler = BackgroundScheduler()
    
    # 自然日数据：每天18点执行（热度榜）
    scheduler.add_job(
        collect_natural_day_data,
        trigger=CronTrigger(hour=18, minute=0),
        id='natural_day_data_collection',
        name='自然日数据采集（热度榜）',
        replace_existing=True
    )
    
    # 交易日数据：每天20点执行（仅在交易日）
    scheduler.add_job(
        collect_trading_day_data,
        trigger=CronTrigger(hour=20, minute=0),
        id='trading_day_data_collection',
        name='交易日数据采集（股票数据、资金流）',
        replace_existing=True
    )
    
    # 概念板块数据：每天凌晨3点执行
    scheduler.add_job(
        collect_concept_data,
        trigger=CronTrigger(hour=3, minute=0),
        id='concept_data_collection',
        name='概念板块数据采集',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("定时任务调度器已启动")
    logger.info("- 每天18:00执行自然日数据采集（热度榜）")
    logger.info("- 每天20:00执行交易日数据采集（股票数据、资金流，仅在交易日）")
    logger.info("- 每天03:00执行概念板块数据采集")
    
    return scheduler

"""
定时任务调度器
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from data_collector import DataCollector
from config import Config
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_data_collection():
    """执行数据采集任务"""
    try:
        collector = DataCollector()
        
        # 采集股票数据
        collector.collect_stock_daily_data()
        
        # 采集板块数据
        collector.collect_sector_daily_data()
        
        # 采集热度榜
        collector.collect_hot_stocks()
        
        # 采集涨幅榜
        collector.collect_gainers()
        
        # 清理过期数据
        collector.clean_old_data()
        
        collector.close()
        logger.info("定时数据采集任务完成")
        
    except Exception as e:
        logger.error(f"定时任务执行失败: {e}", exc_info=True)

def start_scheduler():
    """启动定时任务调度器"""
    scheduler = BackgroundScheduler()
    
    # 每天18点执行数据采集
    scheduler.add_job(
        run_data_collection,
        trigger=CronTrigger(hour=Config.DATA_UPDATE_HOUR, minute=Config.DATA_UPDATE_MINUTE),
        id='daily_data_collection',
        name='每日数据采集',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info(f"定时任务调度器已启动，每天 {Config.DATA_UPDATE_HOUR}:{Config.DATA_UPDATE_MINUTE:02d} 执行数据采集")
    
    return scheduler

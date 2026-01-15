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
    """交易日数据采集任务（仅在交易日15:00执行）"""
    from db.task_execution_repository import TaskExecutionRepository
    
    task_name = 'trading_day_data_collection'
    today = date.today()
    
    try:
        # 判断是否为交易日
        if not TradeDateAdapter.is_trading_day(today):
            logger.info(f"{today} 不是交易日，跳过交易日数据采集")
            return
        
        service = DataCollectionService()
        
        # 采集肥羊日K数据（仅在交易日）
        service.collect_sheep_daily_data()
        
        # 采集资金流向数据（仅在交易日）
        service.collect_money_flow_data()
        
        # 采集板块资金流向数据（仅在交易日，15:00最终刷新）
        service.collect_sector_money_flow_data(target_date=today)
        
        # 采集大盘指数数据（仅在交易日，用于RSRS牛熊市判断）
        service.collect_index_data(index_code='CSI1000')
        
        # 记录执行成功
        TaskExecutionRepository.record_execution(task_name, 'success', '交易日数据采集完成', today)
        logger.info("交易日数据采集任务完成")
    except Exception as e:
        error_msg = f"交易日数据采集失败: {e}"
        # 记录执行失败
        TaskExecutionRepository.record_execution(task_name, 'failed', error_msg, today)
        logger.error(error_msg, exc_info=True)

def collect_sector_money_flow_realtime():
    """板块资金流实时采集任务（交易时间每30分钟执行）"""
    from datetime import datetime
    
    try:
        # 判断是否为交易时间
        if not TradeDateAdapter.is_trading_hours():
            return
        
        today = date.today()
        # 判断是否为交易日
        if not TradeDateAdapter.is_trading_day(today):
            return
        
        service = DataCollectionService()
        # 采集当日板块资金流数据（实时刷新）
        service.collect_sector_money_flow_data(target_date=today)
        logger.info("板块资金流实时采集任务完成")
    except Exception as e:
        logger.error(f"板块资金流实时采集失败: {e}", exc_info=True)

def collect_concept_data():
    """概念板块数据采集任务（每天凌晨3点执行）"""
    try:
        service = DataCollectionService()
        service.collect_concept_data()
        logger.info("概念板块数据采集任务完成")
    except Exception as e:
        logger.error(f"概念板块数据采集失败: {e}", exc_info=True)

def cleanup_old_data():
    """数据清理任务（每天凌晨4点执行）"""
    try:
        service = DataCollectionService()
        
        # 清理资金流旧数据（保留最近3年）
        service.cleanup_old_money_flow_data()
        
        # 清理板块资金流旧数据（保留最近3个月）
        service.cleanup_old_sector_money_flow_data()
        
        logger.info("数据清理任务完成")
    except Exception as e:
        logger.error(f"数据清理失败: {e}", exc_info=True)

def check_and_trigger_missed_tasks():
    """检查并触发错过的任务（启动时和定期检查）"""
    from db.task_execution_repository import TaskExecutionRepository
    from datetime import datetime
    
    today = date.today()
    current_time = datetime.now()
    
    # 检查交易日数据采集任务
    if TradeDateAdapter.is_trading_day(today):
        task_name = 'trading_day_data_collection'
        has_executed = TaskExecutionRepository.has_executed_today(task_name, today)
        
        # 如果今天还没执行成功，且当前时间已经过了15:00，立即执行
        # 注意：如果之前执行失败，也会重新执行
        if not has_executed and current_time.hour >= 15:
            logger.info(f"检测到今天交易日数据采集任务未执行或执行失败，当前时间已过15:00，立即触发执行")
            try:
                collect_trading_day_data()
            except Exception as e:
                logger.error(f"补偿执行交易日数据采集失败: {e}", exc_info=True)

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
    
    # 交易日数据：每天15点执行（仅在交易日）
    scheduler.add_job(
        collect_trading_day_data,
        trigger=CronTrigger(hour=15, minute=0),
        id='trading_day_data_collection',
        name='交易日数据采集（肥羊数据、资金流）',
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
    
    # 数据清理任务：每天凌晨4点执行
    scheduler.add_job(
        cleanup_old_data,
        trigger=CronTrigger(hour=4, minute=0),
        id='cleanup_old_data',
        name='数据清理（资金流等）',
        replace_existing=True
    )
    
    # 板块资金流实时采集：交易时间每30分钟执行（9:30-15:00）
    scheduler.add_job(
        collect_sector_money_flow_realtime,
        trigger=CronTrigger(hour='9-14', minute='0,30'),  # 9:00-14:59每30分钟
        id='sector_money_flow_realtime',
        name='板块资金流实时采集（交易时间每30分钟）',
        replace_existing=True
    )
    
    # 补偿检查任务：每小时检查一次，如果错过就执行（15:00-23:59之间）
    scheduler.add_job(
        check_and_trigger_missed_tasks,
        trigger=CronTrigger(hour='15-23', minute=0),  # 15:00-23:59每小时检查
        id='check_missed_tasks',
        name='检查错过的任务',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("定时任务调度器已启动")
    logger.info("- 每天18:00执行自然日数据采集（热度榜）")
    logger.info("- 交易日15:00执行交易日数据采集（肥羊数据、资金流、板块资金流，仅在交易日）")
    logger.info("- 交易时间每30分钟执行板块资金流实时采集（9:00-14:59）")
    logger.info("- 每天03:00执行概念板块数据采集")
    logger.info("- 每天04:00执行数据清理（资金流保留3年；板块资金流保留3个月）")
    logger.info("- 每小时检查错过的任务（15:00-23:59）")
    
    # 启动时延迟检查并触发错过的任务（避免阻塞HTTP服务器启动）
    # 使用线程异步执行，确保HTTP服务器先启动
    import threading
    def delayed_check():
        import time
        time.sleep(5)  # 等待5秒，确保HTTP服务器先启动
        check_and_trigger_missed_tasks()
    threading.Thread(target=delayed_check, daemon=True).start()
    
    return scheduler

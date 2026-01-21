"""
Falcon Data Engine 调度器
支持配置化任务调度和静默日志
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from config import Config
from utils.logger import get_logger
from datetime import datetime, date
import logging

logger = get_logger(__name__)

# 抑制APScheduler的内部日志
logging.getLogger('apscheduler').setLevel(logging.CRITICAL)

class FalconScheduler:
    """Falcon Data Engine 调度器"""
    
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self._load_tasks()
    
    def _load_tasks(self):
        """从配置加载任务"""
        scheduler_config = Config.get_scheduler_config()
        tasks = scheduler_config.get('tasks', {})
        
        for task_name, task_config in tasks.items():
            if not task_config.get('enabled', True):
                logger.debug(f"任务 {task_name} 已禁用，跳过")
                continue
            
            cron_expr = task_config.get('cron')
            if not cron_expr:
                logger.warning(f"任务 {task_name} 缺少cron配置，跳过")
                continue
            
            # 获取任务函数
            task_func = self._get_task_function(task_name)
            if task_func is None:
                logger.warning(f"未找到任务函数: {task_name}，跳过")
                continue
            
            # 解析cron表达式并添加任务
            try:
                trigger = self._parse_cron(cron_expr)
                self.scheduler.add_job(
                    task_func,
                    trigger=trigger,
                    id=task_name,
                    name=task_config.get('description', task_name),
                    replace_existing=True
                )
                logger.info(f"已添加任务: {task_name} ({task_config.get('description', '')})")
            except Exception as e:
                logger.error(f"添加任务失败 {task_name}: {e}", exc_info=True)
    
    def _parse_cron(self, cron_expr: str):
        """
        解析cron表达式
        
        支持的格式：
        - "*/1 9-15 * * 1-5" -> CronTrigger(minute="*/1", hour="9-15", day_of_week="mon-fri")
        - "*/30 9-15 * * 1-5" -> CronTrigger(minute="*/30", hour="9-15", day_of_week="mon-fri")
        - "0 16 * * 1-5" -> CronTrigger(minute="0", hour="16", day_of_week="mon-fri")
        """
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(f"无效的cron表达式: {cron_expr}")
        
        minute, hour, day, month, day_of_week = parts
        
        # 转换day_of_week: 1-5 -> mon-fri
        if day_of_week == "1-5":
            day_of_week = "mon-fri"
        elif day_of_week == "*":
            day_of_week = "*"
        
        return CronTrigger(
            minute=minute,
            hour=hour,
            day=day,
            month=month,
            day_of_week=day_of_week
        )
    
    def _get_task_function(self, task_name: str):
        """获取任务函数"""
        from services.falcon_tasks import (
            task_realtime_kline,
            task_market_heat,
            task_daily_settlement,
            task_hot_rank,
            task_concept_money_flow_realtime,
            task_rps_calculation,
            task_vcp_calculation,
            task_sector_rps_calculation,
            task_financial_data,
            task_concept_data,
            task_concept_metadata_sync,
            task_cleanup_old_data
        )
        
        task_map = {
            'realtime_kline': task_realtime_kline,
            'market_heat': task_market_heat,
            'daily_settlement': task_daily_settlement,
            'hot_rank': task_hot_rank,
            'concept_money_flow_realtime': task_concept_money_flow_realtime,
            'rps_calculation': task_rps_calculation,
            'vcp_calculation': task_vcp_calculation,
            'sector_rps_calculation': task_sector_rps_calculation,
            'financial_data': task_financial_data,
            'concept_data': task_concept_data,
            'concept_metadata_sync': task_concept_metadata_sync,
            'cleanup_old_data': task_cleanup_old_data
        }
        
        return task_map.get(task_name)
    
    def start(self):
        """启动调度器"""
        self.scheduler.start()
        logger.info("Falcon Data Engine 调度器已启动")
    
    def shutdown(self):
        """关闭调度器"""
        self.scheduler.shutdown()
        logger.info("Falcon Data Engine 调度器已关闭")

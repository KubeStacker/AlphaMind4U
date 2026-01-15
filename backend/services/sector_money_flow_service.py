"""
板块资金流向服务层
"""
from typing import List, Dict
from datetime import date
from db.sector_money_flow_repository import SectorMoneyFlowRepository
from etl.sector_money_flow_adapter import SectorMoneyFlowAdapter
import logging

logger = logging.getLogger(__name__)

class SectorMoneyFlowService:
    """板块资金流向服务"""
    
    @staticmethod
    def collect_sector_money_flow_data(target_date: date = None):
        """
        采集板块资金流向数据
        
        Args:
            target_date: 目标日期，如果为None则使用今天
        """
        if target_date is None:
            target_date = date.today()
        
        logger.info(f"开始采集板块资金流向数据（日期: {target_date}）...")
        
        # 获取所有板块资金流数据
        all_data = SectorMoneyFlowAdapter.get_all_sector_money_flow_today()
        
        if not all_data:
            logger.warning("未获取到板块资金流数据")
            return
        
        # 更新日期为target_date
        for item in all_data:
            item['trade_date'] = target_date
        
        # 保存到数据库
        SectorMoneyFlowRepository.batch_upsert_sector_money_flow(all_data)
        
        logger.info(f"板块资金流向数据采集完成！共 {len(all_data)} 条")
    
    @staticmethod
    def get_sector_money_flow(sector_name: str, limit: int = 90) -> List[Dict]:
        """
        获取板块资金流向数据
        
        Args:
            sector_name: 板块名称
            limit: 返回记录数，默认90条（3个月）
        """
        return SectorMoneyFlowRepository.get_sector_money_flow(sector_name, limit)
    
    @staticmethod
    def get_top_sectors_by_inflow(days: int = 1, limit: int = 20) -> List[Dict]:
        """
        获取资金净流入最多的板块
        
        Args:
            days: 统计天数（1=当日，3=最近3天，5=最近5天）
            limit: 返回数量
        """
        return SectorMoneyFlowRepository.get_top_sectors_by_inflow(days, limit)
    
    @staticmethod
    def cleanup_old_data(retention_days: int = 90):
        """
        清理旧数据（保留最近N天）
        
        Args:
            retention_days: 保留天数，默认90天（3个月）
        """
        return SectorMoneyFlowRepository.cleanup_old_data(retention_days)

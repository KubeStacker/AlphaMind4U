"""
概念资金流向服务层
"""
from typing import List, Dict, Tuple
from datetime import date
from db.sector_money_flow_repository import SectorMoneyFlowRepository
from etl.sector_money_flow_adapter import SectorMoneyFlowAdapter
from etl.concept_filter import should_filter_concept
import logging

logger = logging.getLogger(__name__)

class ConceptMoneyFlowService:
    """概念资金流向服务"""
    
    @staticmethod
    def collect_concept_money_flow_data(target_date: date = None):
        """
        采集概念资金流向数据
        
        Args:
            target_date: 目标日期，如果为None则使用今天
        """
        if target_date is None:
            target_date = date.today()
        
        logger.debug(f"开始采集概念资金流向数据（日期: {target_date}）...")
        
        # 获取所有概念资金流数据
        all_data = SectorMoneyFlowAdapter.get_all_sector_money_flow_today()
        
        if not all_data:
            logger.warning("未获取到概念资金流数据")
            return
        
        # 更新日期为target_date
        for item in all_data:
            item['trade_date'] = target_date
        
        # 保存到数据库
        SectorMoneyFlowRepository.batch_upsert_sector_money_flow(all_data)
        
        logger.debug(f"概念资金流向数据采集完成！共 {len(all_data)} 条")
    
    @staticmethod
    def get_concept_money_flow(concept_name: str, limit: int = 90) -> List[Dict]:
        """
        获取概念资金流向数据
        
        Args:
            concept_name: 概念名称
            limit: 返回记录数，默认90条（3个月）
        """
        return SectorMoneyFlowRepository.get_sector_money_flow(concept_name, limit)
    
    @staticmethod
    def get_top_concepts_by_inflow(days: int = 1, limit: int = 30) -> Tuple[List[Dict], Dict]:
        """
        获取资金净流入最多的概念（剔除黑名单中的板块）
        返回按净流入降序排列的前N条结果（最多30条）
        
        Args:
            days: 统计天数（1=当日，3=最近3天，5=最近5天）
            limit: 返回数量，默认30，最多30
            
        Returns:
            (概念列表, 元数据字典)
        """
        from typing import Tuple
        
        # 获取原始数据（需要多取一些，因为要过滤掉无意义的板块）
        # 如果limit=30，我们取limit*3=90个，过滤后应该还有足够的数量
        fetch_limit = limit * 3 if limit > 10 else limit + 40
        sectors, metadata = SectorMoneyFlowRepository.get_top_sectors_by_inflow(days, fetch_limit)
        
        # 使用完整的过滤逻辑过滤掉无意义的板块
        filtered_sectors = [
            sector for sector in sectors 
            if not should_filter_concept(sector.get('sector_name', ''))
        ]
        
        # 限制返回数量
        filtered_sectors = filtered_sectors[:limit]
        
        logger.info(f"过滤前: {len(sectors)} 个板块，过滤后: {len(filtered_sectors)} 个板块（使用concept_filter过滤）")
        
        return filtered_sectors, metadata
    
    @staticmethod
    def cleanup_old_data(retention_days: int = 90):
        """
        清理旧数据（保留最近N天）
        
        Args:
            retention_days: 保留天数，默认90天（3个月）
        """
        return SectorMoneyFlowRepository.cleanup_old_data(retention_days)

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
        获取资金净流入最多的概念（剔除黑名单中的板块，并进行动态Jaccard聚类）
        返回按净流入降序排列的前N条结果（最多30条）
        
        Args:
            days: 统计天数（1=当日，3=最近3天，5=最近5天）
            limit: 返回数量，默认30，最多30
            
        Returns:
            (概念列表, 元数据字典)
        """
        from typing import Tuple
        from datetime import date
        from services.dynamic_jaccard_clustering import DynamicJaccardClustering
        
        # 获取原始数据（需要多取一些，因为要过滤掉无意义的板块）
        # 优化：减少fetch_limit以减少Jaccard聚类的输入数据量
        # 如果limit<=10，只取limit*2个，避免过多的两两比对
        if limit <= 10:
            fetch_limit = limit * 2  # 减少到2倍
        else:
            fetch_limit = limit * 2 if limit <= 20 else limit + 20  # 最多limit+20
        sectors, metadata = SectorMoneyFlowRepository.get_top_sectors_by_inflow(days, fetch_limit)
        
        # 使用完整的过滤逻辑过滤掉无意义的板块
        filtered_sectors = [
            sector for sector in sectors 
            if not should_filter_concept(sector.get('sector_name', ''))
        ]
        
        # 动态Jaccard聚类：解决"概念同质化"问题（如"CPO"与"光通信"霸榜）
        if filtered_sectors:
            try:
                # 获取交易日期
                trade_date = metadata.get('latest_date') or date.today()
                if isinstance(trade_date, str):
                    from datetime import datetime
                    trade_date = datetime.strptime(trade_date, '%Y-%m-%d').date()
                
                # 执行聚类
                clustering = DynamicJaccardClustering()
                
                # 确定score_key：根据days选择不同的字段
                if days == 1:
                    score_key = 'main_net_inflow'
                else:
                    score_key = 'total_inflow'
                
                # 对板块进行聚类
                clustered_sectors = clustering.cluster_sectors(
                    filtered_sectors,
                    trade_date,
                    score_key=score_key
                )
                
                logger.debug(
                    f"Jaccard聚类: {len(filtered_sectors)} 个板块 -> {len(clustered_sectors)} 个聚类 "
                    f"(折叠了 {len(filtered_sectors) - len(clustered_sectors)} 个同质板块)"
                )
                
                filtered_sectors = clustered_sectors
                
            except Exception as e:
                logger.warning(f"动态Jaccard聚类失败，使用原始结果: {e}", exc_info=True)
                # 如果聚类失败，继续使用过滤后的原始结果
        
        # 限制返回数量
        filtered_sectors = filtered_sectors[:limit]
        
        logger.debug(f"过滤前: {len(sectors)} 个板块，过滤后: {len(filtered_sectors)} 个板块（使用concept_filter + Jaccard聚类）")
        
        return filtered_sectors, metadata
    
    @staticmethod
    def cleanup_old_data(retention_days: int = 90):
        """
        清理旧数据（保留最近N天）
        
        Args:
            retention_days: 保留天数，默认90天（3个月）
        """
        return SectorMoneyFlowRepository.cleanup_old_data(retention_days)

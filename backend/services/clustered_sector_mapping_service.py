"""
聚类板块映射服务
管理原始概念到聚类后板块的映射关系，确保所有服务都使用最新的聚类分类
"""
import logging
from typing import Dict, List, Optional, Set
from datetime import date
from collections import defaultdict
from services.dynamic_jaccard_clustering import DynamicJaccardClustering
from db.database import get_raw_connection

logger = logging.getLogger(__name__)


class ClusteredSectorMappingService:
    """聚类板块映射服务"""
    
    _mapping_cache: Optional[Dict[str, str]] = None
    _reverse_mapping_cache: Optional[Dict[str, List[str]]] = None
    _cache_trade_date: Optional[date] = None
    
    @classmethod
    def get_clustered_sector_mapping(cls, trade_date: Optional[date] = None) -> Dict[str, str]:
        """
        获取原始概念名到聚类后板块名的映射
        
        Args:
            trade_date: 交易日期，如果为None则使用最新交易日
            
        Returns:
            {原始概念名: 聚类后板块名}
            例如: {"CPO": "人工智能", "光通信": "人工智能", "算力": "人工智能"}
        """
        if trade_date is None:
            trade_date = cls._get_latest_trade_date()
        
        # 如果缓存过期或不存在，重新构建
        if (cls._mapping_cache is None or 
            cls._cache_trade_date != trade_date):
            cls._rebuild_mapping_cache(trade_date)
        
        return cls._mapping_cache.copy()
    
    @classmethod
    def get_reverse_mapping(cls, trade_date: Optional[date] = None) -> Dict[str, List[str]]:
        """
        获取聚类后板块名到原始概念名列表的反向映射
        
        Args:
            trade_date: 交易日期
            
        Returns:
            {聚类后板块名: [原始概念名1, 原始概念名2, ...]}
            例如: {"人工智能": ["CPO", "光通信", "算力", "人工智能"]}
        """
        if trade_date is None:
            trade_date = cls._get_latest_trade_date()
        
        # 如果缓存过期或不存在，重新构建
        if (cls._reverse_mapping_cache is None or 
            cls._cache_trade_date != trade_date):
            cls._rebuild_mapping_cache(trade_date)
        
        return cls._reverse_mapping_cache.copy()
    
    @classmethod
    def map_concept_to_clustered_sector(cls, concept_name: str, trade_date: Optional[date] = None) -> str:
        """
        将原始概念名映射到聚类后的板块名
        
        Args:
            concept_name: 原始概念名
            trade_date: 交易日期
            
        Returns:
            聚类后的板块名，如果未找到则返回原始概念名
        """
        mapping = cls.get_clustered_sector_mapping(trade_date)
        return mapping.get(concept_name, concept_name)
    
    @classmethod
    def map_concepts_to_clustered_sectors(cls, concept_names: List[str], trade_date: Optional[date] = None) -> List[str]:
        """
        批量将原始概念名映射到聚类后的板块名
        
        Args:
            concept_names: 原始概念名列表
            trade_date: 交易日期
            
        Returns:
            聚类后的板块名列表（去重）
        """
        mapping = cls.get_clustered_sector_mapping(trade_date)
        clustered_sectors = set()
        for concept_name in concept_names:
            clustered_sector = mapping.get(concept_name, concept_name)
            clustered_sectors.add(clustered_sector)
        return list(clustered_sectors)
    
    @classmethod
    def _rebuild_mapping_cache(cls, trade_date: date):
        """重建映射缓存 - 使用Jaccard聚类算法"""
        try:
            logger.info(f"重建聚类板块映射缓存 (trade_date: {trade_date}) - 使用Jaccard聚类")
            
            # 直接从数据库获取原始板块数据
            raw_sectors = cls._get_raw_sectors(trade_date)
            
            if not raw_sectors:
                logger.warning("没有获取到原始板块数据，使用空映射")
                cls._mapping_cache = {}
                cls._reverse_mapping_cache = {}
                cls._cache_trade_date = trade_date
                return
            
            # 使用Jaccard聚类算法进行聚类
            clustering = DynamicJaccardClustering()
            clustered_sectors = clustering.cluster_sectors(
                raw_sectors,
                trade_date,
                score_key='main_net_inflow'
            )
            
            # 构建映射
            mapping = {}
            reverse_mapping = defaultdict(list)
            
            for sector in clustered_sectors:
                sector_name = sector.get('sector_name', '')
                aggregated_sectors = sector.get('aggregated_sectors', [])
                
                # 主板块映射到自己
                mapping[sector_name] = sector_name
                reverse_mapping[sector_name].append(sector_name)
                
                # 关联板块映射到主板块
                for aggregated_sector in aggregated_sectors:
                    mapping[aggregated_sector] = sector_name
                    reverse_mapping[sector_name].append(aggregated_sector)
            
            # 对于没有在聚类结果中的概念，映射到自身
            # 获取所有活跃的概念
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT DISTINCT concept_name 
                    FROM concept_theme 
                    WHERE is_active = 1
                """)
                all_concepts = {row[0] for row in cursor.fetchall()}
                cursor.close()
            
            # 为未聚类的概念添加自映射
            for concept_name in all_concepts:
                if concept_name not in mapping:
                    mapping[concept_name] = concept_name
                    reverse_mapping[concept_name].append(concept_name)
            
            cls._mapping_cache = mapping
            cls._reverse_mapping_cache = dict(reverse_mapping)
            cls._cache_trade_date = trade_date
            
            logger.info(
                f"Jaccard聚类板块映射缓存重建完成: {len(mapping)} 个概念映射, "
                f"{len(reverse_mapping)} 个聚类板块"
            )
            
        except Exception as e:
            logger.error(f"重建聚类板块映射缓存失败: {e}", exc_info=True)
            # 如果失败，使用空映射（所有概念映射到自身）
            cls._mapping_cache = {}
            cls._reverse_mapping_cache = {}
            cls._cache_trade_date = trade_date
    
    @classmethod
    def _get_raw_sectors(cls, trade_date: date) -> List[Dict]:
        """
        从数据库获取原始板块数据（未聚类）
        
        Args:
            trade_date: 交易日期
            
        Returns:
            板块列表，每个板块包含 sector_name 和 main_net_inflow
        """
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT sector_name, main_net_inflow
                    FROM sector_money_flow
                    WHERE trade_date = %s
                    ORDER BY main_net_inflow DESC
                    LIMIT 100
                """, (trade_date,))
                
                sectors = [
                    {
                        'sector_name': row[0],
                        'main_net_inflow': float(row[1]) if row[1] else 0.0
                    }
                    for row in cursor.fetchall()
                ]
                cursor.close()
                
                return sectors
        except Exception as e:
            logger.error(f"获取原始板块数据失败: {e}", exc_info=True)
            return []
    
    @classmethod
    def _get_latest_trade_date(cls) -> date:
        """获取最新交易日"""
        try:
            with get_raw_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT MAX(trade_date) FROM sector_money_flow")
                row = cursor.fetchone()
                cursor.close()
                
                if row and row[0]:
                    return row[0]
        except Exception as e:
            logger.warning(f"获取最新交易日失败: {e}")
        
        from datetime import date
        return date.today()
    
    @classmethod
    def clear_cache(cls):
        """清除缓存"""
        cls._mapping_cache = None
        cls._reverse_mapping_cache = None
        cls._cache_trade_date = None
        logger.info("聚类板块映射缓存已清除")
"""
热度榜服务层
"""
from typing import List, Dict, Optional
import logging
from db.hot_rank_repository import HotRankRepository
from db.concept_repository import ConceptRepository
from services.sector_matching_service import SectorMatchingService
from sqlalchemy import text
from db.database import get_db

logger = logging.getLogger(__name__)

class HotRankService:
    """热度榜服务"""
    
    _sector_matcher = None
    
    @classmethod
    def _get_sector_matcher(cls):
        """获取板块匹配器实例（单例模式）"""
        if cls._sector_matcher is None:
            cls._sector_matcher = SectorMatchingService()
        return cls._sector_matcher
    
    @staticmethod
    def get_hot_stocks(source: Optional[str] = None) -> List[Dict]:
        """获取热度榜数据"""
        return HotRankRepository.get_hot_stocks(source)
    
    @classmethod
    def get_hot_sectors(cls) -> List[Dict]:
        """
        获取热门板块
        基于集聚效应的量化算法，为每个热门股匹配最合适的板块
        """
        try:
            # 1. 获取最新交易日
            with get_db() as db:
                max_date_query = text("""
                    SELECT MAX(trade_date) as max_date 
                    FROM market_hot_rank
                """)
                max_date_result = db.execute(max_date_query).fetchone()
                if not max_date_result or not max_date_result[0]:
                    return []
                
                max_date = max_date_result[0]
            
            # 2. 获取热门股票（取前100只，用于板块匹配）
            hot_stocks = HotRankRepository.get_hot_stocks(limit=100)
            
            if not hot_stocks:
                return []
            
            # 3. 使用集聚效应算法匹配板块
            sector_matcher = cls._get_sector_matcher()
            sectors = sector_matcher.match_hot_stocks_to_sectors(
                hot_stocks, 
                max_sectors=12
            )
            
            return sectors
                
        except Exception as e:
            logger.error(f"获取热门板块失败: {e}", exc_info=True)
            return []

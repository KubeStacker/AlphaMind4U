"""
热度榜服务层

Enhanced with T10 Model Integration:
- Provides validated hot rank data for signal confirmation
- Supports cross-service trend validation
- Filters traditional industries for accuracy
"""
from typing import List, Dict, Optional
import logging
from db.hot_rank_repository import HotRankRepository
from db.concept_repository import ConceptRepository
from services.sector_matching_service import SectorMatchingService
from sqlalchemy import text
from db.database import get_db
from etl.concept_filter import should_filter_concept

logger = logging.getLogger(__name__)

# Traditional Industries to Filter
TRADITIONAL_INDUSTRIES = {
    '钢铁', '煤炭', '有色金属', '化工', '建材', '电力', '公用事业',
    '银行', '保险', '房地产', '基建', '水泥', '玻璃', '造纸',
    '纺织服装', '农业', '食品饮料', '传统零售', '酒店餐饮'
}

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
    def get_hot_sheep(source: Optional[str] = None) -> List[Dict]:
        """获取热度榜数据"""
        return HotRankRepository.get_hot_sheep(source)
    
    @staticmethod
    def _is_traditional_industry(sector_name: str) -> bool:
        """判断是否为传统行业"""
        if not sector_name:
            return False
        return any(keyword in sector_name for keyword in TRADITIONAL_INDUSTRIES)
    
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
            
            # 2. 获取热门肥羊（取前100只，用于板块匹配）
            hot_sheep = HotRankRepository.get_hot_sheep(source=None, limit=100)
            
            if not hot_sheep:
                return []
            
            # 3. 使用集聚效应算法匹配板块 (T10-Enhanced: filter traditional industries)
            sector_matcher = cls._get_sector_matcher()
            sectors = sector_matcher.match_hot_sheep_to_sectors(
                hot_sheep, 
                max_sectors=12
            )
            
            # T10 Filter: Remove traditional industries
            filtered_sectors = [
                sector for sector in sectors
                if not cls._is_traditional_industry(sector.get('sector_name', ''))
                and not should_filter_concept(sector.get('sector_name', ''))
            ]
            
            logger.info(f"T10增强: 过滤传统行业后得到 {len(filtered_sectors)}/{len(sectors)} 个热门板块")
            return filtered_sectors
                
        except Exception as e:
            logger.error(f"获取热门板块失败: {e}", exc_info=True)
            return []

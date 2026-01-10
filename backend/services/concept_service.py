"""
概念板块服务层
"""
from typing import List, Dict
import logging
from sqlalchemy import text
from db.database import get_db

logger = logging.getLogger(__name__)

class ConceptService:
    """概念板块服务"""
    
    @staticmethod
    def get_sector_daily(sector_name: str) -> List[Dict]:
        """
        获取板块K线数据
        注意：新表结构中暂时没有板块日K数据表，这里返回空列表
        后续可以根据需要实现板块聚合计算
        """
        # TODO: 实现板块K线数据聚合计算
        return []
    
    @staticmethod
    def get_sector_stocks(sector_name: str) -> List[Dict]:
        """获取板块下的股票"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT 
                        sb.stock_code,
                        sb.stock_name,
                        MIN(hr.rank) as min_rank,
                        COUNT(DISTINCT hr.trade_date) as consecutive_days
                    FROM stock_basic sb
                    INNER JOIN stock_concept_mapping scm ON sb.stock_code = scm.stock_code
                    INNER JOIN concept_theme ct ON scm.concept_id = ct.concept_id
                    LEFT JOIN market_hot_rank hr ON sb.stock_code = hr.stock_code
                        AND hr.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    WHERE ct.concept_name = :sector_name
                      AND ct.is_active = 1
                      AND sb.is_active = 1
                    GROUP BY sb.stock_code, sb.stock_name
                    ORDER BY min_rank ASC, consecutive_days DESC
                    LIMIT 100
                """)
                
                result = db.execute(query, {'sector_name': sector_name})
                
                stocks = [
                    {
                        'stock_code': row[0],
                        'stock_name': row[1],
                        'rank': row[2] if row[2] != 999 else None,
                        'consecutive_days': row[3]
                    }
                    for row in result
                ]
                
                return stocks
                
        except Exception as e:
            logger.error(f"获取板块股票失败: {e}", exc_info=True)
            return []

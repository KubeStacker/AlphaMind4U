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
        # 板块K线数据聚合计算待实现
        return []
    
    @staticmethod
    def get_sector_stocks(sector_name: str) -> List[Dict]:
        """获取板块下的肥羊"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT 
                        sb.sheep_code,
                        sb.sheep_name,
                        MIN(hr.rank) as min_rank,
                        COUNT(DISTINCT hr.trade_date) as consecutive_days
                    FROM sheep_basic sb USE INDEX (idx_is_active)
                    INNER JOIN sheep_concept_mapping scm USE INDEX (idx_sheep_code, idx_concept_id) 
                        ON sb.sheep_code = scm.sheep_code
                    INNER JOIN concept_theme ct USE INDEX (idx_concept_name, idx_is_active) 
                        ON scm.concept_id = ct.concept_id
                    LEFT JOIN market_hot_rank hr USE INDEX (idx_sheep_code, idx_trade_date) 
                        ON sb.sheep_code = hr.sheep_code
                        AND hr.trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    WHERE ct.concept_name = :sector_name
                      AND ct.is_active = 1
                      AND sb.is_active = 1
                    GROUP BY sb.sheep_code, sb.sheep_name
                    ORDER BY min_rank ASC, consecutive_days DESC
                    LIMIT 100
                """)
                
                result = db.execute(query, {'sector_name': sector_name})
                
                stocks = []
                for row in result:
                    sheep_code = row[0]
                    sheep_name_raw = row[1]
                    
                    # 判断名称是否有效（不是代码格式）
                    def is_valid_name(name):
                        if not name or not name.strip():
                            return False
                        name_clean = name.strip()
                        # 如果是6位纯数字，认为是代码
                        if len(name_clean) == 6 and name_clean.isdigit():
                            return False
                        # 如果以SH或SZ开头后跟6位数字，也认为是代码
                        if (name_clean.startswith('SH') or name_clean.startswith('SZ')) and len(name_clean) == 8 and name_clean[2:].isdigit():
                            return False
                        return True
                    
                    # 如果名称无效，使用代码
                    if is_valid_name(sheep_name_raw):
                        sheep_name = sheep_name_raw.strip()
                    else:
                        sheep_name = sheep_code
                    
                    stocks.append({
                        'sheep_code': sheep_code,
                        'sheep_name': sheep_name,
                        'rank': row[2] if row[2] != 999 else None,
                        'consecutive_days': row[3]
                    })
                
                return stocks
                
        except Exception as e:
            logger.error(f"获取板块肥羊失败: {e}", exc_info=True)
            return []

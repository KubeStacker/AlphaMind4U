"""
概念板块数据仓储层
"""
from typing import List, Dict, Optional
from sqlalchemy import text
from db.database import get_db

class ConceptRepository:
    """概念板块数据仓储"""
    
    @staticmethod
    def upsert_concept(concept_name: str, concept_code: Optional[str] = None, 
                      source: str = 'ths', description: Optional[str] = None) -> int:
        """插入或更新概念，返回concept_id"""
        with get_db() as db:
            query = text("""
                INSERT INTO concept_theme (concept_name, concept_code, source, description)
                VALUES (:name, :code, :source, :desc)
                ON DUPLICATE KEY UPDATE
                    concept_code = VALUES(concept_code),
                    description = VALUES(description),
                    updated_at = CURRENT_TIMESTAMP
            """)
            db.execute(query, {
                'name': concept_name,
                'code': concept_code,
                'source': source,
                'desc': description
            })
            
            # 获取concept_id
            select_query = text("""
                SELECT concept_id FROM concept_theme 
                WHERE concept_name = :name AND source = :source
            """)
            result = db.execute(select_query, {'name': concept_name, 'source': source})
            row = result.fetchone()
            return row[0] if row else 0
    
    @staticmethod
    def batch_upsert_stock_concept_mapping(concept_id: int, stock_codes: List[str], 
                                           weights: Optional[List[float]] = None):
        """批量插入或更新股票-概念关联"""
        if not stock_codes:
            return
        
        if weights is None:
            weights = [1.0] * len(stock_codes)
        
        with get_db() as db:
            # 使用ON DUPLICATE KEY UPDATE，避免重复插入
            query = text("""
                INSERT INTO stock_concept_mapping (stock_code, concept_id, weight)
                VALUES (:code, :concept_id, :weight)
                ON DUPLICATE KEY UPDATE
                    weight = VALUES(weight),
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            data_list = [
                {'code': code, 'concept_id': concept_id, 'weight': w}
                for code, w in zip(stock_codes, weights)
            ]
            
            batch_size = 100
            for i in range(0, len(data_list), batch_size):
                batch = data_list[i:i+batch_size]
                db.execute(query, batch)

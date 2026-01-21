"""
概念管理服务层
"""
from typing import List, Dict, Optional
import logging
from db.concept_repository import ConceptRepository

from sqlalchemy import text
from db.database import get_db

logger = logging.getLogger(__name__)

class ConceptManagementService:
    """概念管理服务"""
    
    @staticmethod
    def get_concepts(limit: int = 100, offset: int = 0) -> Dict:
        """获取概念列表"""
        with get_db() as db:
            query = text("""
                SELECT concept_id, concept_name, concept_code, source, description, is_active,
                       created_at, updated_at
                FROM concept_theme
                ORDER BY concept_name
                LIMIT :limit OFFSET :offset
            """)
            result = db.execute(query, {'limit': limit, 'offset': offset})
            
            concepts = []
            for row in result:
                concepts.append({
                    'concept_id': row[0],
                    'concept_name': row[1],
                    'concept_code': row[2],
                    'source': row[3],
                    'description': row[4],
                    'is_active': bool(row[5]),
                    'created_at': row[6].isoformat() if row[6] else None,
                    'updated_at': row[7].isoformat() if row[7] else None,
                })
            
            # 获取总数
            count_query = text("SELECT COUNT(*) FROM concept_theme")
            total = db.execute(count_query).fetchone()[0]
            
            return {
                'concepts': concepts,
                'total': total,
                'limit': limit,
                'offset': offset
            }
    
    @staticmethod
    def create_concept(concept_name: str, concept_code: Optional[str] = None,
                      source: str = 'ths', description: Optional[str] = None) -> int:
        """创建概念"""
        return ConceptRepository.upsert_concept(concept_name, concept_code, source, description)
    
    @staticmethod
    def update_concept(concept_id: int, concept_name: Optional[str] = None,
                      concept_code: Optional[str] = None, description: Optional[str] = None,
                      is_active: Optional[bool] = None) -> bool:
        """更新概念"""
        with get_db() as db:
            updates = []
            params = {'concept_id': concept_id}
            
            if concept_name is not None:
                updates.append("concept_name = :concept_name")
                params['concept_name'] = concept_name
            if concept_code is not None:
                updates.append("concept_code = :concept_code")
                params['concept_code'] = concept_code
            if description is not None:
                updates.append("description = :description")
                params['description'] = description
            if is_active is not None:
                updates.append("is_active = :is_active")
                params['is_active'] = 1 if is_active else 0
            
            if not updates:
                return False
            
            query = text(f"""
                UPDATE concept_theme 
                SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                WHERE concept_id = :concept_id
            """)
            db.execute(query, params)
            return True
    
    @staticmethod
    def delete_concept(concept_id: int) -> bool:
        """删除概念（软删除：设置为非激活）"""
        with get_db() as db:
            query = text("""
                UPDATE concept_theme 
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE concept_id = :concept_id
            """)
            result = db.execute(query, {'concept_id': concept_id})
            return result.rowcount > 0

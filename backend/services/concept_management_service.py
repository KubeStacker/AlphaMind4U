"""
概念管理服务层
"""
from typing import List, Dict, Optional
import logging
from db.concept_repository import ConceptRepository
from db.virtual_board_repository import VirtualBoardRepository
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
    
    @staticmethod
    def import_sector_mapping() -> Dict:
        """从sector_mapping表导入到virtual_board_aggregation"""
        return VirtualBoardRepository.import_from_sector_mapping()
    
    @staticmethod
    def get_virtual_boards() -> List[Dict]:
        """获取虚拟板块列表"""
        with get_db() as db:
            query = text("""
                SELECT DISTINCT virtual_board_name, 
                       COUNT(*) as source_count,
                       MAX(weight) as max_weight
                FROM virtual_board_aggregation
                WHERE is_active = 1
                GROUP BY virtual_board_name
                ORDER BY virtual_board_name
            """)
            result = db.execute(query).fetchall()
            
            return [
                {
                    'virtual_board_name': row[0],
                    'source_count': row[1],
                    'max_weight': float(row[2]) if row[2] else 1.0
                }
                for row in result
            ]
    
    @staticmethod
    def create_virtual_board_mapping(virtual_board_name: str, source_concept_name: str,
                                     weight: float = 1.0, description: Optional[str] = None) -> bool:
        """创建虚拟板块映射"""
        with get_db() as db:
            query = text("""
                INSERT INTO virtual_board_aggregation 
                (virtual_board_name, source_concept_name, weight, description)
                VALUES (:virtual_board, :source_concept, :weight, :description)
                ON DUPLICATE KEY UPDATE
                    weight = VALUES(weight),
                    description = VALUES(description),
                    updated_at = CURRENT_TIMESTAMP
            """)
            db.execute(query, {
                'virtual_board': virtual_board_name,
                'source_concept': source_concept_name,
                'weight': weight,
                'description': description
            })
            return True
    
    @staticmethod
    def delete_virtual_board_mapping(virtual_board_name: str, source_concept_name: str) -> bool:
        """删除虚拟板块映射"""
        with get_db() as db:
            query = text("""
                UPDATE virtual_board_aggregation 
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE virtual_board_name = :virtual_board 
                  AND source_concept_name = :source_concept
            """)
            result = db.execute(query, {
                'virtual_board': virtual_board_name,
                'source_concept': source_concept_name
            })
            return result.rowcount > 0

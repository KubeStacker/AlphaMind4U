"""
虚拟板块聚合数据仓储层
"""
from typing import List, Dict, Optional, Tuple
from sqlalchemy import text
from db.database import get_db

class VirtualBoardRepository:
    """虚拟板块聚合数据仓储"""
    
    @staticmethod
    def import_from_sector_mapping():
        """
        从sector_mapping表导入数据到virtual_board_aggregation表
        """
        with get_db() as db:
            # 检查sector_mapping表是否存在
            check_query = text("""
                SELECT COUNT(*) as cnt 
                FROM information_schema.tables 
                WHERE table_schema = DATABASE() 
                  AND table_name = 'sector_mapping'
            """)
            result = db.execute(check_query).fetchone()
            if not result or result[0] == 0:
                return {"message": "sector_mapping表不存在", "imported": 0}
            
            # 从sector_mapping表读取数据
            select_query = text("""
                SELECT source_sector, target_sector, description, is_active
                FROM sector_mapping
                WHERE is_active = 1
            """)
            mappings = db.execute(select_query).fetchall()
            
            if not mappings:
                return {"message": "sector_mapping表中没有有效数据", "imported": 0}
            
            # 批量插入到virtual_board_aggregation表
            insert_query = text("""
                INSERT INTO virtual_board_aggregation 
                (virtual_board_name, source_concept_name, weight, is_active, description)
                VALUES (:virtual_board, :source_concept, 1.0, :is_active, :description)
                ON DUPLICATE KEY UPDATE
                    weight = VALUES(weight),
                    is_active = VALUES(is_active),
                    description = VALUES(description),
                    updated_at = CURRENT_TIMESTAMP
            """)
            
            imported_count = 0
            for row in mappings:
                source_sector = row[0]
                target_sector = row[1]
                description = row[2]
                is_active = row[3]
                
                db.execute(insert_query, {
                    'virtual_board': target_sector,
                    'source_concept': source_sector,
                    'is_active': is_active,
                    'description': description
                })
                imported_count += 1
            
            db.commit()
            return {"message": f"成功导入 {imported_count} 条映射规则", "imported": imported_count}
    
    @staticmethod
    def get_virtual_board_mapping() -> Dict[str, List[str]]:
        """
        获取虚拟板块映射关系
        返回: {virtual_board_name: [source_concept_names]}
        """
        with get_db() as db:
            query = text("""
                SELECT virtual_board_name, source_concept_name, weight
                FROM virtual_board_aggregation
                WHERE is_active = 1
                ORDER BY virtual_board_name, weight DESC
            """)
            result = db.execute(query).fetchall()
            
            mapping = {}
            for row in result:
                virtual_board = row[0]
                source_concept = row[1]
                if virtual_board not in mapping:
                    mapping[virtual_board] = []
                mapping[virtual_board].append(source_concept)
            
            return mapping
    
    @staticmethod
    def get_concept_to_virtual_boards() -> Dict[str, List[str]]:
        """
        获取概念到虚拟板块的映射关系（一个概念可能对应多个主板块）
        返回: {source_concept_name: [virtual_board_name1, virtual_board_name2, ...]}
        """
        with get_db() as db:
            query = text("""
                SELECT source_concept_name, virtual_board_name, weight
                FROM virtual_board_aggregation
                WHERE is_active = 1
                ORDER BY source_concept_name, weight DESC
            """)
            result = db.execute(query).fetchall()
            
            mapping = {}
            for row in result:
                source_concept = row[0]
                virtual_board = row[1]
                if source_concept not in mapping:
                    mapping[source_concept] = []
                mapping[source_concept].append(virtual_board)
            
            return mapping
    
    @staticmethod
    def get_concept_to_virtual_boards_with_weight() -> Dict[str, List[Tuple[str, float]]]:
        """
        获取概念到虚拟板块的映射关系（带权重）
        返回: {source_concept_name: [(virtual_board_name, weight), ...]}
        """
        with get_db() as db:
            query = text("""
                SELECT source_concept_name, virtual_board_name, weight
                FROM virtual_board_aggregation
                WHERE is_active = 1
                ORDER BY source_concept_name, weight DESC
            """)
            result = db.execute(query).fetchall()
            
            mapping = {}
            for row in result:
                source_concept = row[0]
                virtual_board = row[1]
                weight = float(row[2]) if row[2] else 1.0
                if source_concept not in mapping:
                    mapping[source_concept] = []
                mapping[source_concept].append((virtual_board, weight))
            
            return mapping

"""
AI模型配置数据仓储层
"""
from typing import List, Dict, Optional
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class AIModelConfigRepository:
    """AI模型配置仓储"""
    
    @staticmethod
    def get_all_models() -> List[Dict]:
        """获取所有模型配置（按排序顺序）"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT id, model_name, model_display_name, api_key, api_url, sort_order, is_active
                    FROM ai_model_config
                    ORDER BY sort_order ASC, id ASC
                """)
                result = db.execute(query)
                models = []
                for row in result:
                    models.append({
                        "id": row[0],
                        "model_name": row[1],
                        "model_display_name": row[2],
                        "api_key": row[3] if row[3] else "",
                        "api_url": row[4],
                        "sort_order": row[5],
                        "is_active": bool(row[6]) if row[6] is not None else True
                    })
                return models
        except Exception as e:
            logger.error(f"获取AI模型配置失败: {e}")
            return []
    
    @staticmethod
    def get_active_models() -> List[Dict]:
        """获取所有启用的模型配置"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT id, model_name, model_display_name, api_key, api_url, sort_order
                    FROM ai_model_config
                    WHERE is_active = 1
                    ORDER BY sort_order ASC, id ASC
                """)
                result = db.execute(query)
                models = []
                for row in result:
                    models.append({
                        "id": row[0],
                        "model_name": row[1],
                        "model_display_name": row[2],
                        "api_key": row[3] if row[3] else "",
                        "api_url": row[4],
                        "sort_order": row[5]
                    })
                return models
        except Exception as e:
            logger.error(f"获取启用的AI模型配置失败: {e}")
            return []
    
    @staticmethod
    def get_model_by_name(model_name: str) -> Optional[Dict]:
        """根据模型名称获取模型配置"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT id, model_name, model_display_name, api_key, api_url, sort_order, is_active
                    FROM ai_model_config
                    WHERE model_name = :model_name AND is_active = 1
                """)
                result = db.execute(query, {"model_name": model_name})
                row = result.fetchone()
                if row:
                    return {
                        "id": row[0],
                        "model_name": row[1],
                        "model_display_name": row[2],
                        "api_key": row[3] if row[3] else "",
                        "api_url": row[4],
                        "sort_order": row[5],
                        "is_active": bool(row[6]) if row[6] is not None else True
                    }
                return None
        except Exception as e:
            logger.error(f"获取AI模型配置失败: {e}")
            return None
    
    @staticmethod
    def update_model(model_id: int, api_key: str = None, sort_order: int = None, is_active: bool = None) -> bool:
        """更新模型配置"""
        try:
            with get_db() as db:
                updates = []
                params = {"model_id": model_id}
                
                if api_key is not None:
                    updates.append("api_key = :api_key")
                    params["api_key"] = api_key
                
                if sort_order is not None:
                    updates.append("sort_order = :sort_order")
                    params["sort_order"] = sort_order
                
                if is_active is not None:
                    updates.append("is_active = :is_active")
                    params["is_active"] = 1 if is_active else 0
                
                if not updates:
                    return True
                
                updates.append("updated_at = NOW()")
                query = text(f"""
                    UPDATE ai_model_config 
                    SET {', '.join(updates)}
                    WHERE id = :model_id
                """)
                db.execute(query, params)
                return True
        except Exception as e:
            logger.error(f"更新AI模型配置失败: {e}", exc_info=True)
            return False
    
    @staticmethod
    def create_model(model_name: str, model_display_name: str, api_key: str, api_url: str, sort_order: int = 0) -> int:
        """创建新模型配置"""
        try:
            with get_db() as db:
                query = text("""
                    INSERT INTO ai_model_config (model_name, model_display_name, api_key, api_url, sort_order)
                    VALUES (:model_name, :model_display_name, :api_key, :api_url, :sort_order)
                """)
                result = db.execute(query, {
                    "model_name": model_name,
                    "model_display_name": model_display_name,
                    "api_key": api_key,
                    "api_url": api_url,
                    "sort_order": sort_order
                })
                return result.lastrowid if hasattr(result, 'lastrowid') else 0
        except Exception as e:
            logger.error(f"创建AI模型配置失败: {e}", exc_info=True)
            raise
    
    @staticmethod
    def delete_model(model_id: int) -> bool:
        """删除模型配置"""
        try:
            with get_db() as db:
                query = text("DELETE FROM ai_model_config WHERE id = :model_id")
                db.execute(query, {"model_id": model_id})
                return True
        except Exception as e:
            logger.error(f"删除AI模型配置失败: {e}", exc_info=True)
            return False

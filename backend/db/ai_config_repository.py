"""
AI配置数据仓储层
"""
from typing import Optional, Dict
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class AIConfigRepository:
    """AI配置仓储"""
    
    @staticmethod
    def get_config(config_key: str) -> Optional[str]:
        """获取配置值"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT config_value 
                    FROM ai_config 
                    WHERE config_key = :config_key
                """)
                result = db.execute(query, {"config_key": config_key})
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except Exception as e:
            logger.error(f"获取AI配置失败: {e}")
            return None
    
    @staticmethod
    def set_config(config_key: str, config_value: str, description: Optional[str] = None) -> bool:
        """设置配置值"""
        with get_db() as db:
            try:
                # 检查是否存在
                check_query = text("SELECT id FROM ai_config WHERE config_key = :config_key")
                result = db.execute(check_query, {"config_key": config_key})
                exists = result.fetchone()
                
                if exists:
                    # 更新
                    update_query = text("""
                        UPDATE ai_config 
                        SET config_value = :config_value,
                            description = :description,
                            updated_at = NOW()
                        WHERE config_key = :config_key
                    """)
                    db.execute(update_query, {
                        "config_key": config_key,
                        "config_value": config_value,
                        "description": description
                    })
                else:
                    # 插入
                    insert_query = text("""
                        INSERT INTO ai_config (config_key, config_value, description)
                        VALUES (:config_key, :config_value, :description)
                    """)
                    db.execute(insert_query, {
                        "config_key": config_key,
                        "config_value": config_value,
                        "description": description
                    })
                # get_db() 上下文管理器会在退出时自动提交
                return True
            except Exception as e:
                logger.error(f"设置AI配置失败: {e}", exc_info=True)
                # 重新抛出异常，让调用者处理
                raise
    
    @staticmethod
    def get_all_configs() -> Dict[str, Dict]:
        """获取所有配置"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT config_key, config_value, description, updated_at
                    FROM ai_config
                    ORDER BY config_key
                """)
                result = db.execute(query)
                configs = {}
                for row in result:
                    configs[row[0]] = {
                        "value": row[1],
                        "description": row[2],
                        "updated_at": row[3].isoformat() if row[3] else None
                    }
                return configs
        except Exception as e:
            logger.error(f"获取所有AI配置失败: {e}")
            return {}
    
    @staticmethod
    def delete_config(config_key: str) -> bool:
        """删除配置"""
        try:
            with get_db() as db:
                query = text("DELETE FROM ai_config WHERE config_key = :config_key")
                db.execute(query, {"config_key": config_key})
                # get_db() 上下文管理器会在退出时自动提交
                return True
        except Exception as e:
            logger.error(f"删除AI配置失败: {e}", exc_info=True)
            raise

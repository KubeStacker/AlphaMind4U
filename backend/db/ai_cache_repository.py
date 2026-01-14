"""
AI缓存数据仓储层
"""
from typing import Optional
from datetime import datetime, timedelta
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class AICacheRepository:
    """AI缓存仓储"""
    
    CACHE_EXPIRY_HOURS = 3  # 缓存有效期：3小时
    
    @staticmethod
    def get_cache(cache_key: str, cache_type: str) -> Optional[str]:
        """获取缓存"""
        try:
            with get_db() as db:
                query = text("""
                    SELECT content, created_at 
                    FROM ai_analysis_cache 
                    WHERE cache_key = :cache_key 
                      AND cache_type = :cache_type
                """)
                result = db.execute(query, {
                    "cache_key": cache_key,
                    "cache_type": cache_type
                })
                row = result.fetchone()
                
                if row:
                    content, created_at = row
                    # 检查是否过期（3小时）
                    if created_at:
                        expiry_time = created_at + timedelta(hours=AICacheRepository.CACHE_EXPIRY_HOURS)
                        if datetime.now() < expiry_time:
                            return content
                        else:
                            # 缓存过期，删除
                            AICacheRepository.delete_cache(cache_key, cache_type)
                            return None
                    return content
                return None
        except Exception as e:
            logger.error(f"获取AI缓存失败: {e}")
            return None
    
    @staticmethod
    def set_cache(cache_key: str, cache_type: str, content: str) -> bool:
        """设置缓存"""
        try:
            with get_db() as db:
                # 检查是否存在
                check_query = text("""
                    SELECT id FROM ai_analysis_cache 
                    WHERE cache_key = :cache_key AND cache_type = :cache_type
                """)
                result = db.execute(check_query, {
                    "cache_key": cache_key,
                    "cache_type": cache_type
                })
                exists = result.fetchone()
                
                if exists:
                    # 更新
                    update_query = text("""
                        UPDATE ai_analysis_cache 
                        SET content = :content,
                            created_at = NOW(),
                            updated_at = NOW()
                        WHERE cache_key = :cache_key AND cache_type = :cache_type
                    """)
                    db.execute(update_query, {
                        "cache_key": cache_key,
                        "cache_type": cache_type,
                        "content": content
                    })
                else:
                    # 插入
                    insert_query = text("""
                        INSERT INTO ai_analysis_cache (cache_key, cache_type, content)
                        VALUES (:cache_key, :cache_type, :content)
                    """)
                    db.execute(insert_query, {
                        "cache_key": cache_key,
                        "cache_type": cache_type,
                        "content": content
                    })
                return True
        except Exception as e:
            logger.error(f"设置AI缓存失败: {e}", exc_info=True)
            return False
    
    @staticmethod
    def delete_cache(cache_key: str, cache_type: str) -> bool:
        """删除缓存"""
        try:
            with get_db() as db:
                query = text("""
                    DELETE FROM ai_analysis_cache 
                    WHERE cache_key = :cache_key AND cache_type = :cache_type
                """)
                db.execute(query, {
                    "cache_key": cache_key,
                    "cache_type": cache_type
                })
                return True
        except Exception as e:
            logger.error(f"删除AI缓存失败: {e}", exc_info=True)
            return False
    
    @staticmethod
    def clear_expired_cache() -> int:
        """清理过期缓存"""
        try:
            with get_db() as db:
                query = text("""
                    DELETE FROM ai_analysis_cache 
                    WHERE created_at < DATE_SUB(NOW(), INTERVAL :hours HOUR)
                """)
                result = db.execute(query, {
                    "hours": AICacheRepository.CACHE_EXPIRY_HOURS
                })
                return result.rowcount if hasattr(result, 'rowcount') else 0
        except Exception as e:
            logger.error(f"清理过期缓存失败: {e}", exc_info=True)
            return 0
    
    @staticmethod
    def clear_all_cache() -> int:
        """清空所有缓存"""
        try:
            with get_db() as db:
                query = text("DELETE FROM ai_analysis_cache")
                result = db.execute(query)
                return result.rowcount if hasattr(result, 'rowcount') else 0
        except Exception as e:
            logger.error(f"清空所有缓存失败: {e}", exc_info=True)
            return 0
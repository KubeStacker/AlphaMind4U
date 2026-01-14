"""
定时任务执行记录数据仓储层
"""
from typing import Optional
from datetime import date, datetime
from sqlalchemy import text
from db.database import get_db
import logging

logger = logging.getLogger(__name__)

class TaskExecutionRepository:
    """定时任务执行记录仓储"""
    
    @staticmethod
    def has_executed_today(task_name: str, task_date: Optional[date] = None) -> bool:
        """检查指定任务今天是否已执行"""
        if task_date is None:
            task_date = date.today()
        
        try:
            with get_db() as db:
                query = text("""
                    SELECT id 
                    FROM task_execution_log 
                    WHERE task_name = :task_name 
                      AND task_date = :task_date
                      AND status = 'success'
                """)
                result = db.execute(query, {
                    "task_name": task_name,
                    "task_date": task_date
                })
                return result.fetchone() is not None
        except Exception as e:
            logger.error(f"检查任务执行状态失败: {e}")
            return False
    
    @staticmethod
    def record_execution(task_name: str, status: str = 'success', message: Optional[str] = None, task_date: Optional[date] = None) -> bool:
        """记录任务执行状态"""
        if task_date is None:
            task_date = date.today()
        
        try:
            with get_db() as db:
                # 检查是否存在
                check_query = text("""
                    SELECT id FROM task_execution_log 
                    WHERE task_name = :task_name AND task_date = :task_date
                """)
                result = db.execute(check_query, {
                    "task_name": task_name,
                    "task_date": task_date
                })
                exists = result.fetchone()
                
                if exists:
                    # 更新
                    update_query = text("""
                        UPDATE task_execution_log 
                        SET execution_time = :execution_time,
                            status = :status,
                            message = :message
                        WHERE task_name = :task_name AND task_date = :task_date
                    """)
                    db.execute(update_query, {
                        "task_name": task_name,
                        "task_date": task_date,
                        "execution_time": datetime.now(),
                        "status": status,
                        "message": message
                    })
                else:
                    # 插入
                    insert_query = text("""
                        INSERT INTO task_execution_log (task_name, task_date, execution_time, status, message)
                        VALUES (:task_name, :task_date, :execution_time, :status, :message)
                    """)
                    db.execute(insert_query, {
                        "task_name": task_name,
                        "task_date": task_date,
                        "execution_time": datetime.now(),
                        "status": status,
                        "message": message
                    })
                return True
        except Exception as e:
            logger.error(f"记录任务执行状态失败: {e}", exc_info=True)
            return False

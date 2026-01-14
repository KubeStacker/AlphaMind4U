"""
用户管理服务层
"""
from typing import List, Dict, Optional
import logging
from sqlalchemy import text
from db.database import get_db
from auth.auth import get_password_hash

logger = logging.getLogger(__name__)

class UserService:
    """用户管理服务"""
    
    @staticmethod
    def get_all_users() -> List[Dict]:
        """获取所有用户列表"""
        with get_db() as db:
            query = text("""
                SELECT id, username, is_active, 
                       failed_login_attempts, locked_until, last_login,
                       created_at, updated_at
                FROM users
                ORDER BY created_at DESC
            """)
            result = db.execute(query).fetchall()
            
            return [
                {
                    'id': row[0],
                    'username': row[1],
                    'is_active': bool(row[2]),
                    'failed_login_attempts': row[3] or 0,
                    'locked_until': row[4].isoformat() if row[4] else None,
                    'last_login': row[5].isoformat() if row[5] else None,
                    'created_at': row[6].isoformat() if row[6] else None,
                    'updated_at': row[7].isoformat() if row[7] else None,
                    'is_admin': row[1] == 'admin'  # admin用户标识
                }
                for row in result
            ]
    
    @staticmethod
    def get_user_by_id(user_id: int) -> Optional[Dict]:
        """根据ID获取用户"""
        with get_db() as db:
            query = text("""
                SELECT id, username, is_active, 
                       failed_login_attempts, locked_until, last_login,
                       created_at, updated_at
                FROM users
                WHERE id = :user_id
            """)
            result = db.execute(query, {'user_id': user_id}).fetchone()
            
            if not result:
                return None
            
            return {
                'id': result[0],
                'username': result[1],
                'is_active': bool(result[2]),
                'failed_login_attempts': result[3] or 0,
                'locked_until': result[4].isoformat() if result[4] else None,
                'last_login': result[5].isoformat() if result[5] else None,
                'created_at': result[6].isoformat() if result[6] else None,
                'updated_at': result[7].isoformat() if result[7] else None,
                'is_admin': result[1] == 'admin'
            }
    
    @staticmethod
    def create_user(username: str, password: str) -> int:
        """创建用户"""
        with get_db() as db:
            # 检查用户名是否已存在
            check_query = text("SELECT id FROM users WHERE username = :username")
            existing = db.execute(check_query, {'username': username}).fetchone()
            if existing:
                raise ValueError(f"用户名 '{username}' 已存在")
            
            # 生成密码哈希
            password_hash = get_password_hash(password)
            
            # 插入新用户
            insert_query = text("""
                INSERT INTO users (username, password_hash, is_active)
                VALUES (:username, :password_hash, 1)
            """)
            result = db.execute(insert_query, {
                'username': username,
                'password_hash': password_hash
            })
            db.commit()
            
            return result.lastrowid
    
    @staticmethod
    def update_user(user_id: int, password: Optional[str] = None,
                   is_active: Optional[bool] = None) -> bool:
        """更新用户信息"""
        with get_db() as db:
            # 检查用户是否存在且不是admin
            check_query = text("SELECT username FROM users WHERE id = :user_id")
            user = db.execute(check_query, {'user_id': user_id}).fetchone()
            if not user:
                return False
            
            if user[0] == 'admin':
                # admin用户不允许修改某些字段
                if password is not None:
                    raise ValueError("不能修改admin用户的密码")
                if is_active is not None:
                    raise ValueError("不能禁用admin用户")
            
            updates = []
            params = {'user_id': user_id}
            
            if password is not None:
                password_hash = get_password_hash(password)
                updates.append("password_hash = :password_hash")
                params['password_hash'] = password_hash
            
            if is_active is not None:
                updates.append("is_active = :is_active")
                params['is_active'] = 1 if is_active else 0
            
            if not updates:
                return False
            
            query = text(f"""
                UPDATE users 
                SET {', '.join(updates)}, updated_at = CURRENT_TIMESTAMP
                WHERE id = :user_id
            """)
            db.execute(query, params)
            db.commit()
            return True
    
    @staticmethod
    def delete_user(user_id: int) -> bool:
        """删除用户（软删除：设置为非激活）"""
        with get_db() as db:
            # 检查用户是否存在且不是admin
            check_query = text("SELECT username FROM users WHERE id = :user_id")
            user = db.execute(check_query, {'user_id': user_id}).fetchone()
            if not user:
                return False
            
            if user[0] == 'admin':
                raise ValueError("不能删除admin用户")
            
            query = text("""
                UPDATE users 
                SET is_active = 0, updated_at = CURRENT_TIMESTAMP
                WHERE id = :user_id
            """)
            result = db.execute(query, {'user_id': user_id})
            db.commit()
            return result.rowcount > 0
    

"""
初始化管理员用户脚本
用于设置或重置管理员密码
"""
import sys
from db.database import get_db
from sqlalchemy import text
from auth.auth import get_password_hash

def init_admin(username: str = "admin", password: str = "admin123"):
    """初始化或更新管理员用户"""
    with get_db() as db:
        # 检查用户是否存在
        check_query = text("SELECT id FROM users WHERE username = :username")
        result = db.execute(check_query, {"username": username})
        user = result.fetchone()
        
        # 生成密码哈希
        password_hash = get_password_hash(password)
        
        if user:
            # 更新现有用户
            update_query = text("""
                UPDATE users 
                SET password_hash = :hash,
                    failed_login_attempts = 0,
                    locked_until = NULL,
                    is_active = 1
                WHERE username = :username
            """)
            db.execute(update_query, {"hash": password_hash, "username": username})
            db.commit()
            print(f"✓ 管理员用户 '{username}' 密码已更新")
        else:
            # 创建新用户
            insert_query = text("""
                INSERT INTO users (username, password_hash, is_active)
                VALUES (:username, :hash, 1)
            """)
            db.execute(insert_query, {"username": username, "hash": password_hash})
            db.commit()
            print(f"✓ 管理员用户 '{username}' 已创建")
        
        print(f"  用户名: {username}")
        print(f"  密码: {password}")
        print(f"  密码哈希: {password_hash[:20]}...")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        password = sys.argv[1]
    else:
        password = "admin123"
    
    init_admin("admin", password)

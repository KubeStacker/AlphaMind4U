# /backend/db/init_db.py

import os
from db.connection import get_db_connection
from db.schema import ALL_TABLES_SQL

def initialize_database():
    """
    初始化数据库。如果数据库文件不存在，则创建它，并根据 schema.py 中的定义创建所有表。
    """
    # 确保 'data' 目录存在
    if not os.path.exists('data'):
        os.makedirs('data')
        print("创建 'data' 目录。")
    
    print("正在连接数据库并初始化表结构...")
    try:
        with get_db_connection() as con:
            for table_sql in ALL_TABLES_SQL:
                con.execute(table_sql)
            print("数据库表结构初始化/验证完成。")
            
            # 检查 users 表是否为空，如果为空则添加默认管理员用户
            user_count = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if user_count == 0:
                print("users 表为空，正在添加默认管理员 admin/admin ...")
                # 在实际应用中，密码应该是强哈希值
                # 这里为了简化，使用明文，但 schema 中字段是 hashed_password
                # 我们将在用户管理部分实现真正的哈希
                from passlib.context import CryptContext
                pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
                admin_password_hash = pwd_context.hash("admin")
                con.execute(
                    "INSERT INTO users (username, hashed_password, role) VALUES (?, ?, ?)",
                    ("admin", admin_password_hash, "admin")
                )
                print("默认管理员 'admin' 添加成功。")
            
            # 清理历史脏数据 (针对用户提到的 20260101/0102 幻觉数据)
            con.execute("DELETE FROM daily_price WHERE trade_date IN ('2026-01-01', '2026-01-02')")
            con.execute("DELETE FROM market_sentiment WHERE trade_date IN ('2026-01-01', '2026-01-02')")
            print("已自动清理 2026-01-01/02 的异常占位数据。")

    except Exception as e:
        print(f"数据库初始化失败: {e}")

if __name__ == "__main__":
    # 当直接运行此脚本时，执行初始化
    initialize_database()


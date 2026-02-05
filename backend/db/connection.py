# /backend/db/connection.py

import duckdb
from contextlib import contextmanager
import threading
import logging

# Configure logging
logger = logging.getLogger(__name__)

# 数据库文件路径
DATABASE_PATH = "data/jarvis.duckdb"

# 全局连接对象，使用 Singleton 模式
_global_con = None
_lock = threading.Lock()

def get_connection(read_only=False):
    """ 获取全局单例连接 """
    global _global_con
    if _global_con is not None:
        return _global_con

    with _lock:
        if _global_con is None:
            try:
                # 核心优化：全应用统一使用一个读写连接
                # DuckDB 内部会自动处理多线程读取的并发优化
                _global_con = duckdb.connect(database=DATABASE_PATH, read_only=False)
                # 针对分析型查询的优化配置
                _global_con.execute("SET threads TO 4;") 
                _global_con.execute("SET memory_limit = '2GB';")
            except duckdb.IOException as e:
                if "lock" in str(e).lower():
                    logger.warning("检测到数据库被其他进程占用，回退到只读模式")
                    return duckdb.connect(database=DATABASE_PATH, read_only=True)
                raise e
    return _global_con

@contextmanager
def get_db_connection(read_only=False):
    """
    高性能上下文管理器。
    使用 cursor() 实现线程级隔离，避免并发请求下的事务冲突。
    """
    con = get_connection(read_only)
    cursor = None
    try:
        # 使用 cursor 允许在同一个连接上并行执行多个查询
        cursor = con.cursor()
        yield cursor
    finally:
        if cursor:
            cursor.close()

def fetch_df(sql_query: str, params=None) -> 'pd.DataFrame':
    """
    高性能数据查询接口，支持并发。
    """
    with get_db_connection(read_only=True) as cursor:
        return cursor.execute(sql_query, params).fetchdf()

def fetch_df_read_only(sql_query: str, params=None) -> 'pd.DataFrame':
    return fetch_df(sql_query, params)

def close_connection():
    """ 关闭全局数据库连接 """
    global _global_con
    with _lock:
        if _global_con is not None:
            _global_con.close()
            _global_con = None

def get_fresh_connection(read_only=False):
    """ 
    仅在必须开启独立事务或绕过全局单例时使用。
    由于全局 RW 连接已占坑，此处通常只能以 read_only=True 开启。
    """
    return duckdb.connect(database=DATABASE_PATH, read_only=True)


# /backend/db/connection.py

import duckdb
from contextlib import contextmanager
import threading
import logging
import os
import time

logger = logging.getLogger(__name__)

# 数据库文件路径: 确保指向项目根目录下的 data 目录
# 逻辑：如果环境变量指定了则用环境变量，否则根据文件位置自动推导
if os.getenv("DATABASE_PATH"):
    DATABASE_PATH = os.getenv("DATABASE_PATH")
else:
    # 获取当前文件所在目录的上一级 (db -> backend)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_1 = os.path.dirname(current_dir) # backend or /app
    parent_2 = os.path.dirname(parent_1) # project_root or /
    
    if os.path.exists(os.path.join(parent_1, "data")):
        BASE_DIR = parent_1
    elif os.path.exists(os.path.join(parent_2, "data")):
        BASE_DIR = parent_2
    else:
        BASE_DIR = parent_1 # Fallback
        
    DATABASE_PATH = os.path.join(BASE_DIR, "data", "jarvis.duckdb")


# 进程内共享连接与锁
_DB_LOCK = threading.RLock()
_SHARED_CONN = None


def _is_recoverable_connection_error(err: Exception) -> bool:
    msg = str(err)
    return any(
        s in msg for s in (
            "Unique file handle conflict",
            "already attached",
            "Can't open a connection",
            "Connection Error",
            "database has been closed",
        )
    )


def _open_shared_connection():
    global _SHARED_CONN
    if _SHARED_CONN is None:
        _SHARED_CONN = duckdb.connect(database=DATABASE_PATH, read_only=False)
        logger.info(f"DuckDB 共享连接已建立: {DATABASE_PATH}")
    return _SHARED_CONN


def _reset_shared_connection():
    global _SHARED_CONN
    if _SHARED_CONN is not None:
        try:
            _SHARED_CONN.close()
        except Exception:
            pass
    _SHARED_CONN = None

def get_connection(read_only=False):
    """
    获取数据库连接（进程级共享）。
    注意：为避免 DuckDB 文件句柄冲突，read_only 参数会被忽略，统一复用同一个读写连接。
    """
    with _DB_LOCK:
        try:
            return _open_shared_connection()
        except Exception as e:
            logger.warning(f"数据库连接失败: {e}")
            _reset_shared_connection()
            raise

@contextmanager
def get_db_connection(read_only=False):
    """
    数据库上下文（共享连接 + 串行执行）。
    以锁保护整个上下文，确保多线程任务不会并发写同一连接。
    """
    with _DB_LOCK:
        con = get_connection(read_only=read_only)
        yield con


def _query_df(sql_query: str, params=None):
    with _DB_LOCK:
        con = get_connection(read_only=False)
        return con.execute(sql_query, params).fetchdf()

def fetch_df(sql_query: str, params=None, max_retries=3, retry_delay=2) -> 'pd.DataFrame':
    """
    数据查询接口（共享连接 + 重试 + 自动重连）。
    """
    import pandas as pd
    
    last_error = None
    for attempt in range(max_retries):
        try:
            return _query_df(sql_query, params)
        except Exception as e:
            last_error = e
            logger.warning(f"数据库查询失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            if _is_recoverable_connection_error(e):
                with _DB_LOCK:
                    _reset_shared_connection()
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    logger.error(f"数据库查询最终失败: {last_error}")
    raise last_error

def fetch_df_read_only(sql_query: str, params=None, max_retries=3, retry_delay=2) -> 'pd.DataFrame':
    """只读查询接口（逻辑只读，底层复用共享连接）。"""
    return fetch_df(sql_query, params=params, max_retries=max_retries, retry_delay=retry_delay)

def close_connection():
    """关闭进程内共享连接。"""
    with _DB_LOCK:
        _reset_shared_connection()
        logger.info("DuckDB 共享连接已关闭")

def get_fresh_connection(read_only=False):
    """兼容旧接口：返回共享连接（不再创建新连接）。"""
    return get_connection(read_only=read_only)

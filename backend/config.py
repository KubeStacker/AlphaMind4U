import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # 数据库配置
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
    DB_NAME = os.getenv("DB_NAME", "stock")
    
    # 数据保留天数
    STOCK_DATA_RETENTION_DAYS = 90  # 3个月
    SECTOR_DATA_RETENTION_DAYS = 10  # 10天
    
    # 定时任务配置
    DATA_UPDATE_HOUR = 18  # 每天18点更新数据
    DATA_UPDATE_MINUTE = 0

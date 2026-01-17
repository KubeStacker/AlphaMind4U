import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # 数据库配置
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
    DB_NAME = os.getenv("DB_NAME", "stock")  # 注意：数据库名还是stock，只是表名改为sheep_*
    
    # 数据保留天数
    SHEEP_DATA_RETENTION_DAYS = 1095  # 3年（约1095天），用于模型老K回测
    MONEY_FLOW_RETENTION_DAYS = 1095  # 3年（约1095天），资金流数据保留天数
    SECTOR_DATA_RETENTION_DAYS = 10  # 10天
    SECTOR_MONEY_FLOW_RETENTION_DAYS = 90  # 3个月（约90天），板块资金流数据保留天数
    SECTOR_MONEY_FLOW_RETENTION_DAYS = 90  # 3个月（约90天），板块资金流数据保留天数
    
    # 定时任务配置
    DATA_UPDATE_HOUR = 18  # 每天18点更新数据
    DATA_UPDATE_MINUTE = 0
    
    # RSRS市场状态识别参数
    RSRS_WINDOW = 18  # RSRS回归窗口（N日）

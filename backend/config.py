"""
Falcon Data Engine 配置管理
支持YAML配置文件和环境变量
"""
import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from typing import Optional, Dict, Any

load_dotenv()

class Config:
    """配置类 - 支持YAML配置文件和环境变量"""
    
    _yaml_config: Optional[Dict[str, Any]] = None
    _config_path: Path = Path(__file__).parent / "config.yaml"
    
    @classmethod
    def _load_yaml_config(cls) -> Dict[str, Any]:
        """加载YAML配置文件"""
        if cls._yaml_config is not None:
            return cls._yaml_config
        
        if cls._config_path.exists():
            try:
                with open(cls._config_path, 'r', encoding='utf-8') as f:
                    cls._yaml_config = yaml.safe_load(f) or {}
            except Exception as e:
                print(f"警告: 无法加载YAML配置文件 {cls._config_path}: {e}")
                cls._yaml_config = {}
        else:
            cls._yaml_config = {}
        
        return cls._yaml_config
    
    @classmethod
    def _get_yaml_value(cls, key_path: str, default: Any = None) -> Any:
        """从YAML配置中获取值（支持嵌套路径，如 'system.log_level'）"""
        config = cls._load_yaml_config()
        keys = key_path.split('.')
        value = config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value
    
    # ========== 数据库配置 ==========
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", 3306))
    DB_USER = os.getenv("DB_USER", "admin")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "admin")
    DB_NAME = os.getenv("DB_NAME", "stock")
    
    # ========== 系统配置（从YAML加载） ==========
    @classmethod
    def get_log_level(cls) -> str:
        """日志级别"""
        return cls._get_yaml_value('system.log_level', os.getenv('LOG_LEVEL', 'INFO'))
    
    @classmethod
    def get_concurrency(cls) -> int:
        """并发数"""
        return cls._get_yaml_value('system.concurrency', 10)
    
    @classmethod
    def get_cold_start_days(cls) -> int:
        """冷启动回溯天数"""
        return cls._get_yaml_value('system.cold_start_days', cls._get_yaml_value('data_source.cold_start_days', 365))
    
    # ========== 数据保留天数配置（从YAML加载，向后兼容） ==========
    @classmethod
    def get_sheep_data_retention_days(cls) -> int:
        """肥羊数据保留天数"""
        return cls._get_yaml_value('data_source.retention.sheep_data', 1095)
    
    @classmethod
    def get_money_flow_retention_days(cls) -> int:
        """资金流数据保留天数"""
        return cls._get_yaml_value('data_source.retention.money_flow', 1095)
    
    @classmethod
    def get_sector_money_flow_retention_days(cls) -> int:
        """板块资金流数据保留天数"""
        return cls._get_yaml_value('data_source.retention.sector_money_flow', 90)
    
    @classmethod
    def get_hot_rank_retention_days(cls) -> int:
        """热度榜数据保留天数"""
        return cls._get_yaml_value('data_source.retention.hot_rank', 30)
    
    @classmethod
    def get_sector_data_retention_days(cls) -> int:
        """板块数据保留天数"""
        return cls._get_yaml_value('data_source.retention.sector_data', 10)
    
    # 向后兼容的属性访问（使用类属性）
    @classmethod
    def _init_retention_days(cls):
        """初始化保留天数属性（向后兼容）"""
        if not hasattr(cls, '_retention_initialized'):
            cls.SHEEP_DATA_RETENTION_DAYS = cls.get_sheep_data_retention_days()
            cls.MONEY_FLOW_RETENTION_DAYS = cls.get_money_flow_retention_days()
            cls.SECTOR_MONEY_FLOW_RETENTION_DAYS = cls.get_sector_money_flow_retention_days()
            cls.HOT_RANK_RETENTION_DAYS = cls.get_hot_rank_retention_days()
            cls.SECTOR_DATA_RETENTION_DAYS = cls.get_sector_data_retention_days()
            cls._retention_initialized = True
    
    # 向后兼容：使用类属性
    SHEEP_DATA_RETENTION_DAYS = 1095
    MONEY_FLOW_RETENTION_DAYS = 1095
    SECTOR_MONEY_FLOW_RETENTION_DAYS = 90
    HOT_RANK_RETENTION_DAYS = 30
    SECTOR_DATA_RETENTION_DAYS = 10
    
    # ========== 定时任务配置（向后兼容） ==========
    DATA_UPDATE_HOUR = 18
    DATA_UPDATE_MINUTE = 0
    
    # ========== RSRS市场状态识别参数 ==========
    RSRS_WINDOW = 18
    
    # ========== 数据校验配置（从YAML加载） ==========
    @classmethod
    def enable_atomic_check(cls) -> bool:
        """启用原子性校验"""
        return cls._get_yaml_value('validation.enable_atomic_check', True)
    
    @classmethod
    def enable_money_flow_check(cls) -> bool:
        """启用资金流校验"""
        return cls._get_yaml_value('validation.enable_money_flow_check', True)
    
    @classmethod
    def enable_trading_calendar_check(cls) -> bool:
        """启用交易日历检查"""
        return cls._get_yaml_value('validation.enable_trading_calendar_check', True)
    
    # ========== 日志配置（从YAML加载） ==========
    @classmethod
    def is_quiet_mode(cls) -> bool:
        """静默模式"""
        return cls._get_yaml_value('logging.quiet_mode', True)
    
    @classmethod
    def get_progress_interval(cls) -> int:
        """进度输出间隔"""
        return cls._get_yaml_value('logging.progress_interval', 100)
    
    # ========== 调度任务配置（从YAML加载） ==========
    @classmethod
    def get_scheduler_config(cls) -> Dict[str, Any]:
        """获取调度器配置"""
        return cls._get_yaml_value('scheduler', {})
    
    @classmethod
    def get_task_config(cls, task_name: str) -> Optional[Dict[str, Any]]:
        """获取特定任务的配置"""
        tasks = cls._get_yaml_value('scheduler.tasks', {})
        return tasks.get(task_name)

# 初始化保留天数（向后兼容）
Config._init_retention_days()

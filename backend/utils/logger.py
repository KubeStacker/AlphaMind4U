"""
Falcon Data Engine 日志工具
实现静默模式和日志分级策略
"""
import logging
import sys
from config import Config

class QuietLogger:
    """静默日志记录器 - 严格控制日志输出"""
    
    def __init__(self, name: str, log_level: str = None):
        """
        初始化日志记录器
        
        Args:
            name: 日志记录器名称
            log_level: 日志级别（从配置加载或手动指定）
        """
        self.logger = logging.getLogger(name)
        
        # 从配置加载日志级别
        if log_level is None:
            log_level = Config.get_log_level()
        
        # 设置日志级别
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        self.logger.setLevel(level_map.get(log_level.upper(), logging.INFO))
        
        # 如果还没有处理器，添加一个
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(self.logger.level)
            
            # 格式化器：仅输出关键信息
            if Config.is_quiet_mode():
                # 静默模式：简洁格式
                formatter = logging.Formatter(
                    '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
            else:
                # 详细模式：包含文件名和行号
                formatter = logging.Formatter(
                    '%(asctime)s [%(levelname)s] %(name)s:%(filename)s:%(lineno)d: %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
            
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
        
        # 禁止传播到父记录器（避免重复输出）
        self.logger.propagate = False
    
    def debug(self, message: str, *args, **kwargs):
        """DEBUG级别日志（仅在DEBUG模式下输出）"""
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug(message, *args, **kwargs)
    
    def info(self, message: str, *args, **kwargs):
        """INFO级别日志（任务级里程碑）"""
        if self.logger.isEnabledFor(logging.INFO):
            self.logger.info(message, *args, **kwargs)
    
    def warning(self, message: str, *args, **kwargs):
        """WARNING级别日志"""
        if self.logger.isEnabledFor(logging.WARNING):
            self.logger.warning(message, *args, **kwargs)
    
    def error(self, message: str, *args, **kwargs):
        """ERROR级别日志（记录失败和异常）"""
        if self.logger.isEnabledFor(logging.ERROR):
            exc_info = kwargs.pop('exc_info', False)
            self.logger.error(message, *args, exc_info=exc_info, **kwargs)
    
    def critical(self, message: str, *args, **kwargs):
        """CRITICAL级别日志"""
        if self.logger.isEnabledFor(logging.CRITICAL):
            self.logger.critical(message, *args, **kwargs)
    
    def progress(self, current: int, total: int, prefix: str = "进度"):
        """
        进度输出（仅在达到配置的间隔时输出）
        
        Args:
            current: 当前进度
            total: 总数
            prefix: 前缀文本
        """
        interval = Config.get_progress_interval()
        if current % interval == 0 or current == total:
            percentage = (current / total * 100) if total > 0 else 0
            self.info(f"{prefix}: {current}/{total} ({percentage:.1f}%)")

def get_logger(name: str, log_level: str = None) -> QuietLogger:
    """
    获取日志记录器
    
    Args:
        name: 日志记录器名称（通常是模块名）
        log_level: 可选的日志级别（覆盖配置）
    
    Returns:
        QuietLogger实例
    """
    return QuietLogger(name, log_level)

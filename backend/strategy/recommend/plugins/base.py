# /backend/strategy/plugins/base.py

from abc import ABC, abstractmethod

class BaseStrategyPlugin(ABC):
    """
    策略插件基类 (Base Strategy Plugin)
    所有具体的选股策略都应继承此类，并实现 run 方法和 name 属性。
    """
    
    @abstractmethod
    def run(self, target_date=None, **kwargs):
        """
        执行策略并返回推荐结果
        
        参数:
        - target_date: 目标日期
        - kwargs: 其他策略特定参数
        
        返回:
        - dict: 包含 status, data, conclusion 等字段
        """
        pass

    @property
    @abstractmethod
    def name(self):
        """
        插件唯一标识名 (例如 'alpha_momentum')
        """
        pass
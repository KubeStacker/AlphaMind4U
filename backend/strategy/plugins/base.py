# /backend/strategy/plugins/base.py

from abc import ABC, abstractmethod

class BaseStrategyPlugin(ABC):
    """
    策略插件基类
    """
    @abstractmethod
    def run(self, target_date=None, **kwargs):
        """
        执行策略并返回推荐结果
        """
        pass

    @property
    @abstractmethod
    def name(self):
        """
        插件唯一标识名
        """
        pass

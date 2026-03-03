from abc import ABC, abstractmethod
from typing import Literal
import pandas as pd

class DataProvider(ABC):
    """
    数据源提供者抽象基类
    """
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        """ 数据源名称 """
        pass

    @abstractmethod
    def health_check(self) -> bool:
        """
        检查数据源是否可用
        """
        pass

    @abstractmethod
    def stock_basic(self) -> pd.DataFrame:
        """
        获取股票基础信息
        返回字段需包含: ts_code, symbol, name, area, industry, market, list_date
        """
        pass

    @abstractmethod
    def daily(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        获取日线行情数据。
        """
        pass
    
    @abstractmethod
    def adj_factor(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """ 获取复权因子 """
        pass

    @abstractmethod
    def moneyflow(self, trade_date: str = None, ts_code: str = None) -> pd.DataFrame:
        """ 获取资金流向 """
        pass

    @abstractmethod
    def index_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        """ 获取指数日线 """
        pass
    
    @abstractmethod
    def concept(self) -> pd.DataFrame:
        """ 获取概念分类列表 """
        pass
    
    @abstractmethod
    def concept_detail(self, id: str) -> pd.DataFrame:
        """ 获取概念明细 """
        pass

    @abstractmethod
    def ths_index(self, exchange: str = None, type: str = None) -> pd.DataFrame:
        """ 获取同花顺概念指数 """
        pass
    
    @abstractmethod
    def ths_member(self, ts_code: str = None, con_code: str = None) -> pd.DataFrame:
        """ 获取同花顺概念成分 """
        pass
    
    @abstractmethod
    def fina_indicator(self, ts_code: str) -> pd.DataFrame:
        """ 获取财务指标 """
        pass

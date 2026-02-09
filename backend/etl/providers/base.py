from abc import ABC, abstractmethod
from typing import Literal
import pandas as pd

class DataProvider(ABC):
    """
    数据源提供者抽象基类 (Tushare, Akshare)
    """
    
    @property
    @abstractmethod
    def provider_name(self) -> str:
        """ 数据源名称 """
        pass

    @property
    @abstractmethod
    def sync_mode(self) -> Literal["date", "ticker"]:
        """
        同步模式偏好:
        - 'date': 按日期同步效率更高 (如 Tushare daily 接口)
        - 'ticker': 按股票代码同步效率更高 (如 Akshare)
        """
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
        根据 sync_mode 不同，参数传递方式不同:
        - mode='date': 必须传 trade_date
        - mode='ticker': 必须传 ts_code, start_date, end_date
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
    def fina_indicator(self, ts_code: str) -> pd.DataFrame:
        """ 获取财务指标 """
        pass

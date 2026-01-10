"""
股票服务层
"""
from typing import List, Dict
import logging
from db.stock_repository import StockRepository
from db.money_flow_repository import MoneyFlowRepository

logger = logging.getLogger(__name__)

class StockService:
    """股票服务"""
    
    @staticmethod
    def search_stocks(keyword: str) -> List[Dict]:
        """搜索股票"""
        return StockRepository.search_stocks(keyword)
    
    @staticmethod
    def get_stock_daily(stock_code: str) -> List[Dict]:
        """获取股票日K数据"""
        return StockRepository.get_stock_daily(stock_code)
    
    @staticmethod
    def get_stock_capital_flow(stock_code: str) -> List[Dict]:
        """获取股票资金流向数据"""
        return MoneyFlowRepository.get_stock_money_flow(stock_code)
    
    @staticmethod
    def get_stock_concepts(stock_code: str) -> List[Dict]:
        """获取股票的概念列表"""
        return StockRepository.get_stock_concepts(stock_code)

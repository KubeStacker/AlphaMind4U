"""
肥羊服务层
"""
from typing import List, Dict
import logging
from db.sheep_repository import SheepRepository
from db.money_flow_repository import MoneyFlowRepository

logger = logging.getLogger(__name__)

class SheepService:
    """肥羊服务"""
    
    @staticmethod
    def search_sheep(keyword: str) -> List[Dict]:
        """搜索肥羊"""
        return SheepRepository.search_sheep(keyword)
    
    @staticmethod
    def get_sheep_daily(sheep_code: str) -> List[Dict]:
        """获取肥羊日K数据"""
        return SheepRepository.get_sheep_daily(sheep_code)
    
    @staticmethod
    def get_sheep_capital_flow(sheep_code: str, limit: int = 60) -> List[Dict]:
        """获取肥羊资金流向数据"""
        return MoneyFlowRepository.get_sheep_money_flow(sheep_code, limit=limit)
    
    @staticmethod
    def get_continuous_inflow_stocks(days: int = 5) -> List[Dict]:
        """获取最近N天资金持续流入的标的"""
        return MoneyFlowRepository.get_continuous_inflow_stocks(days=days, min_days=days)

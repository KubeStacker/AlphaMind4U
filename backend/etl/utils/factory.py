from etl.providers.base import DataProvider
from etl.providers.tushare_pro import TushareProvider
from etl.config import settings

class ProviderFactory:
    _instance = None
    
    @classmethod
    def get_provider(cls) -> DataProvider:
        if cls._instance:
            return cls._instance
        
        # 统一使用 Tushare
        cls._instance = TushareProvider()
        return cls._instance

def get_provider() -> DataProvider:
    return ProviderFactory.get_provider()

from etl.providers.base import DataProvider
from etl.providers.tushare_pro import TushareProvider
from etl.providers.akshare_pro import AkshareProvider
from core.config import settings

class ProviderFactory:
    _instance = None
    
    @classmethod
    def get_provider(cls) -> DataProvider:
        if cls._instance:
            return cls._instance
        
        # 默认使用 Tushare, 可以通过环境变量配置
        source = getattr(settings, 'data_source', 'tushare').lower()
        
        if source == 'akshare':
            cls._instance = AkshareProvider()
        else:
            cls._instance = TushareProvider()
            
        return cls._instance

def get_provider() -> DataProvider:
    return ProviderFactory.get_provider()

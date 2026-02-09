import tushare as ts
import pandas as pd
from typing import Literal
from core.config import settings
from etl.providers.base import DataProvider
import logging

logger = logging.getLogger(__name__)

class TushareProvider(DataProvider):
    def __init__(self):
        token = settings.tushare_token
        self.pro = ts.pro_api(token)
        # 兼容旧代码的特殊配置
        self.pro._DataApi__token = token
        self.pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'

    @property
    def provider_name(self) -> str:
        return "tushare"

    @property
    def sync_mode(self) -> Literal["date", "ticker"]:
        return "date"

    def health_check(self) -> bool:
        try:
            df = self.pro.daily(ts_code='000001.SZ', limit=1)
            return not df.empty
        except Exception as e:
            logger.error(f"Tushare health check failed: {e}")
            return False

    def stock_basic(self) -> pd.DataFrame:
        return self.pro.stock_basic(list_status='L', fields='ts_code,symbol,name,area,industry,market,list_date,fullname,enname,curr_type,list_status,is_hs')

    def daily(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        if trade_date:
            # 去除横杠兼容 tushare 格式
            d = trade_date.replace("-", "")
            return self.pro.daily(trade_date=d)
        elif ts_code:
            return self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return pd.DataFrame()

    def adj_factor(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        if trade_date:
            d = trade_date.replace("-", "")
            return self.pro.adj_factor(trade_date=d)
        elif ts_code:
            return self.pro.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        return pd.DataFrame()

    def moneyflow(self, trade_date: str = None, ts_code: str = None) -> pd.DataFrame:
        if trade_date:
            d = trade_date.replace("-", "")
            return self.pro.moneyflow(trade_date=d)
        return pd.DataFrame()

    def index_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self.pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
    
    def concept(self) -> pd.DataFrame:
        return self.pro.concept(src='ts')
    
    def concept_detail(self, id: str) -> pd.DataFrame:
        return self.pro.concept_detail(id=id)

    def fina_indicator(self, ts_code: str) -> pd.DataFrame:
        return self.pro.fina_indicator(ts_code=ts_code)

import akshare as ak
import pandas as pd
from typing import Literal
from etl.providers.base import DataProvider
import logging
import time

logger = logging.getLogger(__name__)

class AkshareProvider(DataProvider):
    @property
    def provider_name(self) -> str:
        return "akshare"

    @property
    def sync_mode(self) -> Literal["date", "ticker"]:
        return "ticker"

    def health_check(self) -> bool:
        try:
            # 获取平安银行实时数据作为健康检查
            df = ak.stock_zh_a_spot_em()
            return not df.empty
        except Exception as e:
            logger.error(f"Akshare health check failed: {e}")
            return False

    def _add_ts_suffix(self, code: str) -> str:
        """ 为股票代码添加后缀 (简易版) """
        if code.startswith('6'):
            return f"{code}.SH"
        elif code.startswith('0') or code.startswith('3'):
            return f"{code}.SZ"
        elif code.startswith('8') or code.startswith('4'):
            return f"{code}.BJ"
        else:
            return code # Fallback

    def stock_basic(self) -> pd.DataFrame:
        """
        注意: Akshare 获取全量基础信息(含上市日期、行业)较慢，这里主要返回代码表。
        使用 stock_zh_a_spot_em 获取最新全市场列表。
        """
        try:
            df = ak.stock_zh_a_spot_em()
            # columns: 序号, 代码, 名称, 最新价, ...
            # 映射到 standard columns
            df['ts_code'] = df['代码'].apply(self._add_ts_suffix)
            df['symbol'] = df['代码']
            df['name'] = df['名称']
            
            # 缺失字段填充默认值，因为 spot 接口不包含这些静态信息
            # 如果需要完整信息，可能需要结合其他接口或离线表
            df['area'] = ''
            df['industry'] = ''
            df['market'] = ''
            df['list_date'] = '' # 很难一次性获取
            df['fullname'] = df['name']
            df['enname'] = ''
            df['curr_type'] = 'CNY'
            df['list_status'] = 'L'
            df['is_hs'] = ''
            
            return df[['ts_code','symbol','name','area','industry','market','list_date','fullname','enname','curr_type','list_status','is_hs']]
        except Exception as e:
            logger.error(f"Akshare stock_basic error: {e}")
            return pd.DataFrame()

    def daily(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        Akshare 主要支持按代码获取历史。
        如果指定了 ts_code, 则获取该股历史。
        """
        if ts_code:
            symbol = ts_code.split('.')[0]
            try:
                # 调整日期格式 YYYYMMDD
                s_date = start_date.replace("-", "") if start_date else "19900101"
                e_date = end_date.replace("-", "") if end_date else "20500101"
                
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=s_date, end_date=e_date, adjust="qfq")
                if df.empty:
                    return pd.DataFrame()
                
                # 字段映射
                # 日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
                df = df.rename(columns={
                    '日期': 'trade_date',
                    '开盘': 'open',
                    '收盘': 'close',
                    '最高': 'high',
                    '最低': 'low',
                    '成交量': 'vol',
                    '成交额': 'amount',
                    '涨跌幅': 'pct_chg',
                    '涨跌额': 'change'
                })
                
                df['ts_code'] = ts_code
                df['pre_close'] = df['close'] - df['change'] # 估算 pre_close
                df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
                
                return df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']]
            except Exception as e:
                logger.error(f"Akshare daily error for {ts_code}: {e}")
                return pd.DataFrame()
        
        return pd.DataFrame()

    def adj_factor(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        # Akshare stock_zh_a_hist 已经可以返回前复权(qfq)数据，通常不需要单独的复权因子表。
        # 为了兼容接口，返回空或模拟数据
        return pd.DataFrame()

    def moneyflow(self, trade_date: str = None, ts_code: str = None) -> pd.DataFrame:
        # Akshare 个股资金流向接口需要具体调研，暂时返回空
        return pd.DataFrame()

    def index_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        # 指数数据
        # 000001.SH -> sh000001
        try:
            symbol = ts_code.split('.')[0]
            market = ts_code.split('.')[1]
            ak_symbol = f"{market.lower()}{symbol}" 
            
            df = ak.stock_zh_index_daily(symbol=ak_symbol)
            # date, open, high, low, close, volume
            df = df.rename(columns={
                'date': 'trade_date',
                'volume': 'vol'
            })
            df['ts_code'] = ts_code
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')
            # Filter date
            s_date = pd.to_datetime(start_date)
            e_date = pd.to_datetime(end_date)
            df['dt'] = pd.to_datetime(df['trade_date'])
            df = df[(df['dt'] >= s_date) & (df['dt'] <= e_date)]
            
            return df[['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'vol']]
        except Exception as e:
            logger.error(f"Akshare index daily error: {e}")
            return pd.DataFrame()

    def concept(self) -> pd.DataFrame:
        # 板块概念
        try:
            df = ak.stock_board_concept_name_em()
            # 排名, 板块名称, 板块代码, ...
            df = df.rename(columns={'板块代码': 'code', '板块名称': 'name'})
            df['src'] = 'akshare'
            return df[['code', 'name', 'src']]
        except Exception:
            return pd.DataFrame()
    
    def concept_detail(self, id: str) -> pd.DataFrame:
        # 概念成分
        try:
            # id 是板块名称还是代码？Akshare 通常用名称
            # 这里假设 id 传进来的是 name (因为 tushare 是 id, akshare 是 name)
            # 需要适配。
            pass 
        except Exception:
            pass
        return pd.DataFrame()

    def fina_indicator(self, ts_code: str) -> pd.DataFrame:
        return pd.DataFrame()

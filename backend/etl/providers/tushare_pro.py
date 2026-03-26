import tushare as ts
import pandas as pd
import time
from core.config import settings
from etl.providers.base import DataProvider
import logging

logger = logging.getLogger(__name__)

class TushareProvider(DataProvider):
    def __init__(self):
        token = settings.tushare_token
        self.pro = ts.pro_api(token)
        
        # Short token 需要特殊处理
        if settings.tushare_token_type == "short":
            self.pro._DataApi__token = token
            self.pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'
            self._is_short_token = True
        else:
            self._is_short_token = False
        
        self.last_call_time = 0
        self.min_interval = 0.5
        self._daily_limit_hit = set()  # 记录每日限额用完的接口

    def _rate_limited_call(self, func, **kwargs):
        # 获取接口名 (处理 partial 对象)
        if hasattr(func, '__name__'):
            func_name = func.__name__
        elif hasattr(func, 'func') and hasattr(func.func, '__name__'):
            func_name = func.func.__name__
        else:
            func_name = 'unknown'
        
        # 检查是否已达每日限额
        if func_name in self._daily_limit_hit:
            logger.warning(f"Tushare 接口 {func_name} 今日限额已用完，跳过")
            return pd.DataFrame()
        
        # Short token 无限流，直接调用
        if not self._is_short_token:
            elapsed = time.time() - self.last_call_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
        
        # 增加重试逻辑处理 Tushare 内部并发报错
        for attempt in range(3):
            try:
                res = func(**kwargs)
                self.last_call_time = time.time()
                return res
            except Exception as e:
                err_msg = str(e)
                # 检查每日限额已用完
                if "每天最多访问" in err_msg or "5000次" in err_msg:
                    logger.warning(f"Tushare 接口 {func_name} 今日限额已用完，明天恢复: {err_msg[:50]}")
                    self._daily_limit_hit.add(func_name)
                    return pd.DataFrame()
                if not self._is_short_token:
                    if "抱歉，您每分钟最多访问" in err_msg or "接口过快" in err_msg or "频繁" in err_msg:
                        logger.warning(f"Tushare 限流，等待中... (尝试 {attempt+1}/3)")
                        time.sleep(5 * (attempt + 1))
                        continue
                # 检查权限错误
                if "无权限" in err_msg or "auth" in err_msg.lower() or "权限" in err_msg:
                    logger.warning(f"Tushare 权限不足，跳过此接口: {err_msg}")
                    return pd.DataFrame()
                raise e
        return pd.DataFrame()

    @property
    def provider_name(self) -> str:
        return "tushare"

    @property
    def sync_mode(self) -> str:
        return "date"

    def health_check(self) -> bool:
        try:
            df = self._rate_limited_call(self.pro.daily, ts_code='000001.SZ', limit=1)
            return not df.empty
        except Exception as e:
            logger.error(f"Tushare health check failed: {e}")
            return False

    def stock_basic(self) -> pd.DataFrame:
        return self._rate_limited_call(self.pro.stock_basic, list_status='L', fields='ts_code,symbol,name,area,industry,market,list_date,fullname,enname,curr_type,list_status,is_hs')

    def daily(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        if trade_date:
            # 去除横杠兼容 tushare 格式
            d = trade_date.replace("-", "")
            return self._rate_limited_call(self.pro.daily, trade_date=d)
        elif ts_code:
            return self._rate_limited_call(self.pro.daily, ts_code=ts_code, start_date=start_date, end_date=end_date)
        return pd.DataFrame()

    def adj_factor(self, trade_date: str = None, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        if trade_date:
            d = trade_date.replace("-", "")
            return self._rate_limited_call(self.pro.adj_factor, trade_date=d)
        elif ts_code:
            return self._rate_limited_call(self.pro.adj_factor, ts_code=ts_code, start_date=start_date, end_date=end_date)
        return pd.DataFrame()

    def moneyflow(self, trade_date: str = None, ts_code: str = None) -> pd.DataFrame:
        if trade_date:
            d = trade_date.replace("-", "")
            return self._rate_limited_call(self.pro.moneyflow, trade_date=d)
        return pd.DataFrame()

    def index_daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        return self._rate_limited_call(self.pro.index_daily, ts_code=ts_code, start_date=start_date, end_date=end_date)

    def realtime_quote(self, ts_code: str = None, src: str = "dc") -> pd.DataFrame:
        """
        实时行情快照 (用于盘中预估)。

        Args:
            ts_code: 代码，支持股票/指数（如 000300.SH）
            src: 数据源，默认 dc
        """
        def _normalize_quote_df(df: pd.DataFrame) -> pd.DataFrame:
            if df is None or df.empty:
                return pd.DataFrame()
            rename_map = {c: str(c).lower() for c in df.columns}
            return df.rename(columns=rename_map)

        params = {"src": src}
        if ts_code:
            params["ts_code"] = ts_code

        # pro.realtime_quote 只支持 dc 源，sina 源直接使用 ts.realtime_quote
        if src == "sina":
            try:
                ts.set_token(settings.tushare_token)
                df = ts.realtime_quote(**params)
                if df is not None and not df.empty:
                    return _normalize_quote_df(df)
            except Exception as e:
                logger.warning(f"ts.realtime_quote(sina) 失败: {e}")
                return pd.DataFrame()

        # 优先尝试 pro 接口；若接口不可用，自动回退到 ts.realtime_quote
        try:
            df = self._rate_limited_call(self.pro.realtime_quote, **params)
            if df is not None and not df.empty:
                return _normalize_quote_df(df)
        except Exception as e:
            logger.warning(f"pro.realtime_quote 失败(src={src}): {e}")

        try:
            ts.set_token(settings.tushare_token)
            df = ts.realtime_quote(**params)
            if df is not None and not df.empty:
                return _normalize_quote_df(df)
        except Exception as e:
            logger.warning(f"ts.realtime_quote 失败(src={src}): {e}")

        # 东财源不稳定时，尝试新浪兜底
        if src != "sina":
            try:
                ts.set_token(settings.tushare_token)
                fb_params = {"src": "sina"}
                if ts_code:
                    fb_params["ts_code"] = ts_code
                df = ts.realtime_quote(**fb_params)
                if df is not None and not df.empty:
                    logger.info("realtime_quote 自动回退到 src=sina 成功")
                    return _normalize_quote_df(df)
            except Exception as e:
                logger.warning(f"ts.realtime_quote 回退 sina 失败: {e}")

        return pd.DataFrame()
    
    def concept(self) -> pd.DataFrame:
        return self._rate_limited_call(self.pro.concept, src='ts')
    
    def concept_detail(self, id: str) -> pd.DataFrame:
        return self._rate_limited_call(self.pro.concept_detail, id=id)
    
    def ths_index(self, exchange: str = None, type: str = None) -> pd.DataFrame:
        """同花顺概念指数 - 需要6000积分
        
        Args:
            exchange: 市场类型 A-a股 HK-港股 US-美股
            type: 指数类型 N-概念指数 I-行业指数 R-地域指数 S-特色指数 ST-风格指数 TH-主题指数 BB-宽基指数
        """
        params = {}
        if exchange:
            params['exchange'] = exchange
        if type:
            params['type'] = type
        return self._rate_limited_call(self.pro.ths_index, **params)
    
    def ths_member(self, ts_code: str = None, con_code: str = None) -> pd.DataFrame:
        """同花顺概念板块成分 - 需要6000积分
        
        Args:
            ts_code: 板块指数代码
            con_code: 股票代码
        """
        params = {}
        if ts_code:
            params['ts_code'] = ts_code
        if con_code:
            params['con_code'] = con_code
        return self._rate_limited_call(self.pro.ths_member, **params)

    def fina_indicator(self, ts_code: str) -> pd.DataFrame:
        return self._rate_limited_call(self.pro.fina_indicator, ts_code=ts_code)

    # === 以下接口可能需要更高权限 (2000积分可能无权限) ===
    # income, balancesheet, cashflow, express, fina_mainbz 需要pro权限
    # 如遇权限错误会自动跳过，不会中断程序

    def income(self, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """利润表数据 - 支持单只或批量(短token)
        
        Args:
            ts_code: 股票代码，支持逗号分隔批量查询(短token)
            start_date: 开始日期
            end_date: 结束日期
        """
        if self._is_short_token and ts_code and ',' in ts_code:
            return self.income_vip(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        params = {}
        if ts_code:
            params['ts_code'] = ts_code
        if start_date:
            params['start_date'] = start_date.replace("-", "")
        if end_date:
            params['end_date'] = end_date.replace("-", "")
        return self._rate_limited_call(self.pro.income, **params)

    def income_vip(self, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """VIP利润表数据 - 仅短token可用，支持批量"""
        if not self._is_short_token:
            logger.warning("income_vip 仅短token可用")
            return pd.DataFrame()
        
        params = {}
        if ts_code:
            params['ts_code'] = ts_code
        if start_date:
            params['start_date'] = start_date.replace("-", "")
        if end_date:
            params['end_date'] = end_date.replace("-", "")
        return self._rate_limited_call(self.pro.income_vip, **params)

    def balance_sheet(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """资产负债表数据"""
        params = {'ts_code': ts_code}
        if start_date:
            params['start_date'] = start_date.replace("-", "")
        if end_date:
            params['end_date'] = end_date.replace("-", "")
        return self._rate_limited_call(self.pro.balancesheet, **params)

    def cashflow(self, ts_code: str, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """现金流量表数据"""
        params = {'ts_code': ts_code}
        if start_date:
            params['start_date'] = start_date.replace("-", "")
        if end_date:
            params['end_date'] = end_date.replace("-", "")
        return self._rate_limited_call(self.pro.cashflow, **params)

    def express(self, ts_code: str = None, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """业绩快报数据"""
        params = {}
        if ts_code:
            params['ts_code'] = ts_code
        if start_date:
            params['start_date'] = start_date.replace("-", "")
        if end_date:
            params['end_date'] = end_date.replace("-", "")
        return self._rate_limited_call(self.pro.express, **params)

    def fina_mainbz(self, ts_code: str = None, type: str = '1') -> pd.DataFrame:
        """主营业务构成"""
        params = {'type': type}
        if ts_code:
            params['ts_code'] = ts_code
        return self._rate_limited_call(self.pro.fina_mainbz, **params)

    def trade_cal(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """ 获取交易日历 """
        params = {'exchange': 'SSE'}
        if start_date:
            params['start_date'] = start_date.replace("-", "")
        if end_date:
            params['end_date'] = end_date.replace("-", "")
        return self._rate_limited_call(self.pro.trade_cal, **params)

    def margin(self, trade_date: str = None, exchange_id: str = None) -> pd.DataFrame:
        """ 融资融券数据 - 按交易所汇总 """
        params = {}
        if trade_date:
            params['trade_date'] = trade_date.replace("-", "")
        if exchange_id:
            params['exchange_id'] = exchange_id
        return self._rate_limited_call(self.pro.margin, **params)

    def margin_detail(self, trade_date: str = None) -> pd.DataFrame:
        """ 融资融券明细数据 - 按股票 """
        params = {}
        if trade_date:
            params['trade_date'] = trade_date.replace("-", "")
        return self._rate_limited_call(self.pro.margin_detail, **params)

    def fx_daily(self) -> pd.DataFrame:
        """ 外汇/宏观数据 """
        return self._rate_limited_call(self.pro.fx_daily)

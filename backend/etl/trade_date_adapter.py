"""
交易日历适配器
"""
import akshare as ak
import pandas as pd
from datetime import date, datetime
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

class TradeDateAdapter:
    """交易日历适配器，封装akshare的交易日历接口"""
    
    _trade_calendar_cache: Optional[pd.DataFrame] = None
    _cache_date: Optional[date] = None
    
    @classmethod
    def get_trade_calendar(cls, refresh: bool = False) -> pd.DataFrame:
        """
        获取交易日历（带缓存）
        
        Args:
            refresh: 是否强制刷新缓存
            
        Returns:
            交易日历DataFrame，包含trade_date列
        """
        today = date.today()
        
        # 如果缓存存在且未过期（每天刷新一次），直接返回
        if not refresh and cls._trade_calendar_cache is not None:
            if cls._cache_date == today:
                return cls._trade_calendar_cache
        
        try:
            # 调用akshare接口
            trade_cal = ak.tool_trade_date_hist_sina()
            
            if trade_cal is None or trade_cal.empty:
                logger.warning("获取交易日历失败，返回空DataFrame")
                return pd.DataFrame(columns=['trade_date'])
            
            # 标准化列名
            if 'trade_date' not in trade_cal.columns:
                if len(trade_cal.columns) > 0:
                    trade_cal = trade_cal.rename(columns={trade_cal.columns[0]: 'trade_date'})
                else:
                    logger.error("交易日历数据格式异常")
                    return pd.DataFrame(columns=['trade_date'])
            
            # 转换日期格式
            if trade_cal['trade_date'].dtype == 'object':
                trade_cal['trade_date'] = pd.to_datetime(trade_cal['trade_date'], format='%Y%m%d').dt.date
            
            # 更新缓存
            cls._trade_calendar_cache = trade_cal
            cls._cache_date = today
            
            logger.debug(f"交易日历已更新，共 {len(trade_cal)} 个交易日")
            return trade_cal
            
        except Exception as e:
            logger.error(f"获取交易日历异常: {e}", exc_info=True)
            # 返回缓存数据（如果有）
            if cls._trade_calendar_cache is not None:
                logger.warning("使用缓存的交易日历数据")
                return cls._trade_calendar_cache
            return pd.DataFrame(columns=['trade_date'])
    
    @classmethod
    def is_trading_day(cls, check_date: Optional[date] = None) -> bool:
        """
        判断指定日期是否为交易日
        
        Args:
            check_date: 要检查的日期，默认为今天
            
        Returns:
            True表示是交易日，False表示非交易日
        """
        if check_date is None:
            check_date = date.today()
        
        # 先判断周末
        if check_date.weekday() >= 5:  # 周六、周日
            return False
        
        try:
            trade_cal = cls.get_trade_calendar()
            if trade_cal.empty:
                # 如果无法获取交易日历，至少排除周末
                return True
            
            return check_date in trade_cal['trade_date'].values
            
        except Exception as e:
            logger.warning(f"判断交易日失败: {e}，仅排除周末")
            return check_date.weekday() < 5
    
    @classmethod
    def get_last_trading_day(cls, check_date: Optional[date] = None) -> date:
        """
        获取指定日期之前的最后一个交易日
        
        Args:
            check_date: 基准日期，默认为今天
            
        Returns:
            最后一个交易日
        """
        if check_date is None:
            check_date = date.today()
        
        try:
            trade_cal = cls.get_trade_calendar()
            if trade_cal.empty:
                # 降级：向前查找最多7天
                from datetime import timedelta
                for i in range(1, 8):
                    prev_date = check_date - timedelta(days=i)
                    if prev_date.weekday() < 5:
                        return prev_date
                return check_date - timedelta(days=1)
            
            # 筛选小于check_date的交易日，按降序排列
            past_dates = trade_cal[trade_cal['trade_date'] < check_date]['trade_date'].sort_values(ascending=False)
            
            if len(past_dates) > 0:
                return past_dates.iloc[0].date() if hasattr(past_dates.iloc[0], 'date') else past_dates.iloc[0]
            else:
                # 如果没找到，返回最近的一个交易日
                all_dates = trade_cal['trade_date'].sort_values(ascending=False)
                if len(all_dates) > 0:
                    return all_dates.iloc[0].date() if hasattr(all_dates.iloc[0], 'date') else all_dates.iloc[0]
                
        except Exception as e:
            logger.error(f"获取上一个交易日失败: {e}", exc_info=True)
        
        # 降级处理
        from datetime import timedelta
        for i in range(1, 8):
            prev_date = check_date - timedelta(days=i)
            if prev_date.weekday() < 5:
                return prev_date
        
        return check_date - timedelta(days=1)
    
    @classmethod
    def get_trading_days_in_range(cls, start_date: date, end_date: date) -> List[date]:
        """
        获取指定日期范围内的所有交易日
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            交易日列表
        """
        try:
            trade_cal = cls.get_trade_calendar()
            if trade_cal.empty:
                return []
            
            mask = (trade_cal['trade_date'] >= start_date) & (trade_cal['trade_date'] <= end_date)
            trading_days = trade_cal[mask]['trade_date'].tolist()
            
            # 转换为date对象
            return [d.date() if hasattr(d, 'date') else d for d in trading_days]
            
        except Exception as e:
            logger.error(f"获取交易日范围失败: {e}", exc_info=True)
            return []
    
    @classmethod
    def get_next_trading_day(cls, check_date: Optional[date] = None) -> date:
        """
        获取指定日期之后的下一个交易日
        
        Args:
            check_date: 基准日期，默认为今天
            
        Returns:
            下一个交易日
        """
        from datetime import timedelta
        
        if check_date is None:
            check_date = date.today()
        
        try:
            trade_cal = cls.get_trade_calendar()
            if trade_cal.empty:
                # 降级：向后查找最多7天
                for i in range(1, 8):
                    next_date = check_date + timedelta(days=i)
                    if next_date.weekday() < 5:
                        return next_date
                return check_date + timedelta(days=1)
            
            # 筛选大于check_date的交易日，按升序排列
            future_dates = trade_cal[trade_cal['trade_date'] > check_date]['trade_date'].sort_values(ascending=True)
            
            if len(future_dates) > 0:
                result = future_dates.iloc[0]
                return result.date() if hasattr(result, 'date') else result
            else:
                # 如果没找到，向后查找最多7天
                for i in range(1, 8):
                    next_date = check_date + timedelta(days=i)
                    if next_date.weekday() < 5:
                        return next_date
                
        except Exception as e:
            logger.error(f"获取下一个交易日失败: {e}", exc_info=True)
        
        # 降级处理
        for i in range(1, 8):
            next_date = check_date + timedelta(days=i)
            if next_date.weekday() < 5:
                return next_date
        
        return check_date + timedelta(days=1)
    
    @classmethod
    def is_trading_hours(cls, check_time: Optional[datetime] = None) -> bool:
        """
        判断指定时间是否为交易时段
        
        交易时段：
        - 上午：9:30-11:30
        - 下午：13:00-15:00
        
        Args:
            check_time: 要检查的时间，默认为当前时间
            
        Returns:
            True表示是交易时段，False表示非交易时段
        """
        if check_time is None:
            check_time = datetime.now()
        
        # 判断是否为交易日
        if not cls.is_trading_day(check_time.date()):
            return False
        
        # 获取当前时间（小时和分钟）
        hour = check_time.hour
        minute = check_time.minute
        time_minutes = hour * 60 + minute
        
        # 上午交易时段：9:30-11:30 (570-690分钟)
        morning_start = 9 * 60 + 30  # 9:30
        morning_end = 11 * 60 + 30   # 11:30
        
        # 下午交易时段：13:00-15:00 (780-900分钟)
        afternoon_start = 13 * 60    # 13:00
        afternoon_end = 15 * 60       # 15:00
        
        # 判断是否在交易时段内
        is_morning = morning_start <= time_minutes <= morning_end
        is_afternoon = afternoon_start <= time_minutes <= afternoon_end
        
        return is_morning or is_afternoon
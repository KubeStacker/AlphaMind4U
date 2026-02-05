# /backend/core/calendar.py

import arrow
import chinese_calendar as calendar
from datetime import date, time

class TradingCalendar:
    """
    中国A股交易日历和时间校验器。
    
    - is_trading_day: 检查指定日期是否为交易日（排除周末和法定节假日）。
    - is_trading_time: 检查当前时间是否在交易时段内。
    """

    def is_trading_day(self, day: date) -> bool:
        """
        判断指定日期是否为A股交易日。
        
        Args:
            day (date): 需要检查的日期。
            
        Returns:
            bool: 如果是交易日则返回 True，否则返回 False。
        """
        # 1. 必须是工作日（周一至周五）
        # 2. 不能是法定节假日（通过 chinese_calendar 判断）
        return calendar.is_workday(day) and day.weekday() < 5

    def is_trading_time(self) -> bool:
        """
        判断当前时间是否处于A股的交易时段。
        
        交易时段:
        - 上午盘: 09:30:00 - 11:30:00
        - 下午盘: 13:00:00 - 15:00:00
        
        Returns:
            bool: 如果在交易时段内则返回 True，否则返回 False。
        """
        # 获取当前时间（使用 arrow 获取，时区为本地）
        now = arrow.now().time()
        
        # 定义交易时段
        morning_session_start = time(9, 30)
        morning_session_end = time(11, 30)
        afternoon_session_start = time(13, 0)
        afternoon_session_end = time(15, 0)
        
        # 检查当前时间是否在任一交易时段内
        is_morning_session = morning_session_start <= now <= morning_session_end
        is_afternoon_session = afternoon_session_start <= now <= afternoon_session_end
        
        return is_morning_session or is_afternoon_session

# 创建一个全局实例，方便其他模块使用
trading_calendar = TradingCalendar()

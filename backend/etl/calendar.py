import arrow
import chinese_calendar as local_calendar
from datetime import date, time, datetime
from db.connection import get_db_connection

class TradingCalendar:
    """
    中国A股交易日历和时间校验器。
    
    优先从数据库 trade_calendar (同步自 Tushare) 获取权威数据，
    如果数据库无记录，则回退到 chinese_calendar 本地计算。
    """

    def is_trading_day(self, day: date) -> bool:
        """
        判断指定日期是否为A股交易日。
        """
        try:
            with get_db_connection() as con:
                res = con.execute(
                    "SELECT is_open FROM trade_calendar WHERE exchange = 'SSE' AND cal_date = ?",
                    (day,)
                ).fetchone()
                if res is not None:
                    return bool(res[0])
        except Exception:
            pass

        # 回退逻辑：必须是工作日（周一至周五）且不能是法定节假日
        return local_calendar.is_workday(day) and day.weekday() < 5

    def get_last_trading_day(self, reference_date: date = None) -> date:
        """
        获取指定日期之前的最后一个交易日 (不包含指定日期)。
        """
        if reference_date is None:
            reference_date = arrow.now().date()
        
        try:
            with get_db_connection() as con:
                # 优先查询数据库中 pretrade_date 或直接查上一个 is_open=1 的日期
                res = con.execute(
                    "SELECT cal_date FROM trade_calendar WHERE exchange = 'SSE' AND is_open = 1 AND cal_date < ? ORDER BY cal_date DESC LIMIT 1",
                    (reference_date,)
                ).fetchone()
                if res:
                    return res[0]
        except Exception:
            pass

        # 回退逻辑
        current = arrow.get(reference_date).shift(days=-1)
        while not self.is_trading_day(current.date()):
            current = current.shift(days=-1)
        return current.date()

    def is_trading_time(self) -> bool:
        """
        判断当前时间是否处于A股的交易时段 (9:15 - 15:00, 北京时间)。
        """
        # 统一使用上海时区判断交易时间
        now_dt = arrow.now('Asia/Shanghai')
        
        # 首先必须是交易日
        if not self.is_trading_day(now_dt.date()):
            return False

        now_time = now_dt.time()
        
        # 用户定义交易时段: 9:15 - 15:00 (北京时间)
        start_time = time(9, 15)
        end_time = time(15, 0)
        
        return start_time <= now_time <= end_time

    def get_latest_sync_date(self) -> date:
        """
        获取当前可以同步的最晚日期。
        如果是交易日且在 15:30 之后，则返回今天，否则返回上一个交易日。
        """
        # 强制使用上海时区进行判定
        now = arrow.now('Asia/Shanghai')
        today = now.date()
        
        # 如果今天是交易日且已经收盘（15:30之后）
        if self.is_trading_day(today) and now.time() >= time(15, 30):
            return today
        
        # 否则返回上一个交易日
        return self.get_last_trading_day(today)

# 创建一个全局实例
trading_calendar = TradingCalendar()

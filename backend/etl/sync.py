import logging
import time
from etl.utils.factory import get_provider
from etl.tasks.stock_basic_task import StockBasicTask
from etl.tasks.daily_price_task import DailyPriceTask
from etl.tasks.moneyflow_task import MoneyFlowTask
from etl.tasks.concepts_task import ConceptsTask
from etl.tasks.market_index_task import MarketIndexTask
from etl.tasks.financials_task import FinancialsTask
from etl.tasks.calendar_task import CalendarTask
from etl.tasks.margin_task import MarginTask
from etl.tasks.fx_task import FxTask
from etl.utils.factors import factor_calculator
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

class SyncEngine:
    def __init__(self):
        self.provider = get_provider()
        self.stock_basic_task = StockBasicTask(self.provider)
        self.daily_price_task = DailyPriceTask(self.provider)
        self.moneyflow_task = MoneyFlowTask(self.provider)
        self.concepts_task = ConceptsTask(self.provider)
        self.market_index_task = MarketIndexTask(self.provider)
        self.financials_task = FinancialsTask(self.provider)
        self.calendar_task = CalendarTask(self.provider)
        self.margin_task = MarginTask(self.provider)
        self.fx_task = FxTask(self.provider)
        self.factor_calculator = factor_calculator

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_stock_basic(self):
        return self.stock_basic_task.sync()

    def sync_trade_cal(self, start_date: str = "2020-01-01", end_date: str = "2026-12-31"):
        self.calendar_task.sync(start_date=start_date, end_date=end_date)

    def sync_daily_price(self, years: int = 1, force: bool = False, calc_factors: bool = True):
        self.daily_price_task.sync(years=years, force=force, calc_factors=calc_factors)

    def sync_daily_by_date(self, trade_date: str, calc_factors: bool = True):
        """ 同步特定日期的数据 """
        self.daily_price_task._process_single_date(trade_date, calc_factors=calc_factors)

    def sync_moneyflow(self, years: int = 0, days: int = 3, force: bool = False):
        self.moneyflow_task.sync(years=years, days=days, force=force)

    def sync_daily_update(self):
        """ 每日收盘后更新任务 """
        logger.info("执行每日收盘数据更新...")
        
        # 1. 行情与资金流 (默认同步最近3天，防止漏数据)
        self.sync_daily_price(years=1)
        self.sync_moneyflow(days=3)
        
        # 2. 指数同步（覆盖情绪模型依赖指数）
        self.sync_core_indices(years=0, days=5)
        
        # 3. 情绪计算
        self.calculate_market_sentiment(days=30)
        
        logger.info("每日收盘数据更新完成")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_concepts(self):
        self.concepts_task.sync()

    def calculate_factors(self, trade_date: str):
        self.factor_calculator.calculate_daily(trade_date)

    def calculate_factors_batch(self, start_date_str: str, end_date_str: str):
        self.factor_calculator.calculate_batch(start_date_str, end_date_str)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_market_index(self, ts_code: str = '000001.SH', years: int = 0, days: int = 3):
        self.market_index_task.sync(ts_code=ts_code, years=years, days=days)

    def sync_core_indices(self, years: int = 0, days: int = 3):
        """
        同步情绪/回测依赖的核心指数。
        - 000001.SH: 上证指数
        - 000300.SH: 沪深300
        - 399001.SZ: 深证成指
        - 000688.SH: 科创50
        """
        for code in ("000001.SH", "000300.SH", "399001.SZ", "000688.SH"):
            self.sync_market_index(ts_code=code, years=years, days=days)

    def calculate_market_sentiment(self, days: int = 30):
        from strategy.sentiment import sentiment_analyst
        sentiment_analyst.calculate(days=days)

    def sync_financials(self, limit: int = 1000):
        """同步财务数据 - 调用新的季度利润表同步"""
        self.financials_task.sync_quarterly_income()

    def sync_margin(self, days: int = 90):
        """同步融资融券数据"""
        self.margin_task.sync(days=days)

    def sync_fx(self):
        """同步外汇/宏观数据"""
        self.fx_task.sync()

    def fill_missing_factors(self):
        logger.info("检查并补全缺失的因子数据...")
        from db.connection import get_db_connection
        with get_db_connection() as con:
            # 这里的逻辑：如果 factors 为空，或者虽然有因子但缺少关键的长周期因子 (high_250)，则认为需要重算
            query = """
            SELECT DISTINCT trade_date 
            FROM daily_price 
            WHERE factors IS NULL 
               OR factors = '{}' 
               OR factors = 'null'
               OR json_extract_path_text(factors, '$.high_250') IS NULL
            ORDER BY trade_date DESC
            """
            dates = con.execute(query).fetchall()
        
        dates = [d[0] for d in dates]
        if not dates:
            logger.info("所有行情因子的数据已完整。")
            return

        logger.info(f"发现 {len(dates)} 个交易日存在因子缺失或不完整，开始批量修复...")
        # 建议按月或按周分块，或者调用 calculate_factors_batch
        # 为了简单且稳妥，这里先逐日补全，如果是海量数据建议外部调用 batch
        for d in dates:
            try:
                d_str = d.strftime("%Y-%m-%d") if hasattr(d, 'strftime') else str(d)
                self.calculate_factors(d_str)
            except Exception as e:
                logger.error(f"计算 {d} 因子失败: {e}")

# Export singleton
sync_engine = SyncEngine()

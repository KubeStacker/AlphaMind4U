import logging
import time
from etl.factory import get_provider
from etl.tasks.stock_basic_task import StockBasicTask
from etl.tasks.daily_price_task import DailyPriceTask
from etl.tasks.moneyflow_task import MoneyFlowTask
from etl.tasks.concepts_task import ConceptsTask
from etl.tasks.market_index_task import MarketIndexTask
from etl.tasks.financials_task import FinancialsTask
from etl.factors import factor_calculator
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
        self.factor_calculator = factor_calculator

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_stock_basic(self):
        return self.stock_basic_task.sync()

    def sync_daily_price(self, years: int = 1, force: bool = False, calc_factors: bool = True):
        self.daily_price_task.sync(years=years, force=force, calc_factors=calc_factors)

    def sync_moneyflow(self, years: int = 1, force: bool = False):
        self.moneyflow_task.sync(years=years, force=force)

    def sync_daily_update(self):
        """ 每日收盘后更新任务 """
        logger.info("执行每日收盘数据更新...")
        
        # 1. 行情与资金流
        self.sync_daily_price(years=1)
        self.sync_moneyflow(years=1)
        
        # 2. 指数同步
        self.sync_market_index(years=1)
        
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
    def sync_market_index(self, ts_code: str = '000001.SH', years: int = 1):
        self.market_index_task.sync(ts_code=ts_code, years=years)

    def calculate_market_sentiment(self, days: int = 30):
        from strategy.sentiment import sentiment_analyst
        sentiment_analyst.calculate(days=days)

    def sync_financials(self, limit: int = 1000):
        self.financials_task.sync(limit=limit)

    def fill_missing_factors(self):
        logger.info("检查并补全缺失的因子数据...")
        from db.connection import get_db_connection
        with get_db_connection() as con:
            dates = con.execute("SELECT DISTINCT trade_date FROM daily_price WHERE factors IS NULL OR factors = '{}' OR factors = 'null'").fetchall()
        
        dates = [d[0] for d in dates]
        if not dates:
            logger.info("所有行情因子的数据已完整。")
            return

        logger.info(f"发现 {len(dates)} 个交易日存在因子缺失，开始计算...")
        for d in dates:
            try:
                d_str = d.strftime("%Y-%m-%d") if hasattr(d, 'strftime') else str(d)
                self.calculate_factors(d_str)
            except Exception as e:
                logger.error(f"计算 {d} 因子失败: {e}")

# Export singleton
sync_engine = SyncEngine()

import logging
import time
from etl.utils.factory import get_provider
from etl.tasks.stock_basic_task import StockBasicTask
from etl.tasks.daily_market_data_task import DailyMarketDataTask
from etl.tasks.capital_flow_task import CapitalFlowTask
from etl.tasks.concepts_task import ConceptsTask
from etl.tasks.market_index_task import MarketIndexTask
from etl.tasks.financials_task import FinancialsTask
from etl.tasks.calendar_task import CalendarTask
from etl.tasks.margin_trading_task import MarginTradingTask
from etl.tasks.forex_data_task import ForexDataTask
from etl.utils.factors import factor_calculator
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)

class SyncEngine:
    """数据同步引擎
    
    统一管理所有ETL任务的调度和执行
    """
    
    def __init__(self):
        self.provider = get_provider()
        self.stock_basic_task = StockBasicTask(self.provider)
        self.daily_market_data_task = DailyMarketDataTask(self.provider)
        self.capital_flow_task = CapitalFlowTask(self.provider)
        self.concepts_task = ConceptsTask(self.provider)
        self.market_index_task = MarketIndexTask(self.provider)
        self.financials_task = FinancialsTask(self.provider)
        self.calendar_task = CalendarTask(self.provider)
        self.margin_trading_task = MarginTradingTask(self.provider)
        self.forex_data_task = ForexDataTask(self.provider)
        self.factor_calculator = factor_calculator

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_stock_basic(self):
        """同步股票基础信息"""
        return self.stock_basic_task.sync()

    def sync_trade_calendar(self, start_date: str = "2020-01-01", end_date: str = "2026-12-31"):
        """同步交易日历
        
        Args:
            start_date: 开始日期
            end_date: 结束日期
        """
        self.calendar_task.sync(start_date=start_date, end_date=end_date)

    def sync_daily_market_data(self, years: int = 1, force: bool = False, calc_factors: bool = True):
        """同步每日市场数据
        
        Args:
            years: 同步的年数
            force: 是否强制重新同步
            calc_factors: 是否计算技术因子
        """
        self.daily_market_data_task.sync_daily_data(years=years, force=force, calc_factors=calc_factors)

    def sync_daily_data_by_date(self, trade_date: str, calc_factors: bool = True):
        """同步特定日期的市场数据
        
        Args:
            trade_date: 交易日期
            calc_factors: 是否计算技术因子
        """
        self.daily_market_data_task.fetch_and_save_daily_data(trade_date, calc_factors=calc_factors)

    def sync_capital_flow(self, years: int = 0, days: int = 3, force: bool = False):
        """同步资金流向数据
        
        Args:
            years: 同步的年数
            days: 同步的天数
            force: 是否强制重新同步
        """
        self.capital_flow_task.sync_capital_flow(years=years, days=days, force=force)

    def perform_daily_data_update(self):
        """执行每日收盘后数据更新任务"""
        logger.info("执行每日收盘数据更新...")
        
        # 1. 行情与资金流 (默认同步最近3天，防止漏数据)
        self.sync_daily_market_data(years=1)
        self.sync_capital_flow(days=3)
        
        # 2. 指数同步（覆盖情绪模型依赖指数）
        self.sync_core_market_indices(years=0, days=5)
        
        # 3. 验证数据完整性
        self._validate_daily_update()
        
        # 4. 情绪计算
        self.calculate_market_sentiment(days=30)
        
        logger.info("每日收盘数据更新完成")
    
    def _validate_daily_update(self):
        """验证每日更新的数据完整性"""
        from db.connection import fetch_df
        from etl.calendar import trading_calendar
        import arrow
        
        try:
            # 获取最近一个交易日
            latest_trading_day = trading_calendar.get_latest_sync_date()
            latest_str = latest_trading_day.strftime("%Y-%m-%d")
            
            # 检查行情数据完整性
            df_daily = fetch_df("""
                SELECT COUNT(*) as cnt 
                FROM daily_price 
                WHERE trade_date = ?
            """, [latest_trading_day])
            
            df_stocks = fetch_df("""
                SELECT COUNT(*) as cnt 
                FROM stock_basic 
                WHERE list_status = 'L'
            """)
            
            if not df_daily.empty and not df_stocks.empty:
                daily_count = int(df_daily.iloc[0]["cnt"])
                stock_count = int(df_stocks.iloc[0]["cnt"])
                
                # 如果数据完整度低于90%，记录警告
                if stock_count > 0 and daily_count < stock_count * 0.9:
                    logger.warning(f"数据完整性验证失败: {latest_str} 行情数据 {daily_count}/{stock_count} ({daily_count/stock_count*100:.1f}%)")
                else:
                    logger.info(f"数据完整性验证通过: {latest_str} 行情数据 {daily_count}/{stock_count} ({daily_count/stock_count*100:.1f}%)")
            
            # 检查资金流数据完整性
            df_moneyflow = fetch_df("""
                SELECT COUNT(*) as cnt 
                FROM stock_moneyflow 
                WHERE trade_date = ?
            """, [latest_trading_day])
            
            if not df_moneyflow.empty:
                moneyflow_count = int(df_moneyflow.iloc[0]["cnt"])
                if stock_count > 0 and moneyflow_count < stock_count * 0.8:
                    logger.warning(f"资金流数据不完整: {latest_str} {moneyflow_count}/{stock_count} ({moneyflow_count/stock_count*100:.1f}%)")
                else:
                    logger.info(f"资金流数据验证通过: {latest_str} {moneyflow_count}/{stock_count} ({moneyflow_count/stock_count*100:.1f}%)")
                    
        except Exception as e:
            logger.error(f"数据完整性验证失败: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_concept_classification(self):
        """同步概念分类数据"""
        self.concepts_task.sync()

    def calculate_technical_factors(self, trade_date: str):
        """计算指定日期的技术因子
        
        Args:
            trade_date: 交易日期
        """
        self.factor_calculator.calculate_daily(trade_date)

    def calculate_technical_factors_batch(self, start_date_str: str, end_date_str: str):
        """批量计算技术因子
        
        Args:
            start_date_str: 开始日期
            end_date_str: 结束日期
        """
        self.factor_calculator.calculate_batch(start_date_str, end_date_str)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_market_index(self, ts_code: str = '000001.SH', years: int = 0, days: int = 3):
        """同步市场指数数据
        
        Args:
            ts_code: 指数代码
            years: 同步的年数
            days: 同步的天数
        """
        self.market_index_task.sync(ts_code=ts_code, years=years, days=days)

    def sync_core_market_indices(self, years: int = 0, days: int = 3):
        """同步核心市场指数
        
        同步情绪/回测依赖的核心指数:
        - 000001.SH: 上证指数
        - 000300.SH: 沪深300
        - 399001.SZ: 深证成指
        - 000688.SH: 科创50
        
        Args:
            years: 同步的年数
            days: 同步的天数
        """
        for code in ("000001.SH", "000300.SH", "399001.SZ", "000688.SH"):
            self.sync_market_index(ts_code=code, years=years, days=days)

    def calculate_market_sentiment(self, days: int = 30):
        """计算市场情绪指标
        
        Args:
            days: 计算的天数
        """
        from strategy.sentiment import sentiment_analyst
        sentiment_analyst.calculate(days=days)

    def sync_financial_statements(self, limit: int = 1000):
        """同步财务报表数据（兼容旧接口）
        
        Args:
            limit: 同步限制数量
        """
        self.financials_task.sync_quarterly_income()

    def sync_quarterly_income_statement(self, ts_code: str = None, force: bool = False):
        """同步季度利润表数据
        
        Args:
            ts_code: 股票代码
            force: 是否强制重新同步
        """
        self.financials_task.sync_quarterly_income(ts_code=ts_code, force_sync=force)

    def sync_financial_indicators(self, ts_code: str = None):
        """同步财务指标数据
        
        Args:
            ts_code: 股票代码
        """
        self.financials_task.sync_fina_indicator(ts_code=ts_code)

    def sync_margin_trading_data(self, days: int = 90):
        """同步融资融券数据
        
        Args:
            days: 同步的天数
        """
        self.margin_trading_task.sync_margin_trading(days=days)

    def sync_forex_data(self):
        """同步外汇/宏观数据"""
        self.forex_data_task.sync_forex_data()

    def fill_missing_technical_factors(self):
        """补全缺失的技术因子数据"""
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
        # 建议按月或按周分块，或者调用 calculate_technical_factors_batch
        # 为了简单且稳妥，这里先逐日补全，如果是海量数据建议外部调用 batch
        for d in dates:
            try:
                d_str = d.strftime("%Y-%m-%d") if hasattr(d, 'strftime') else str(d)
                self.calculate_technical_factors(d_str)
            except Exception as e:
                logger.error(f"计算 {d} 因子失败: {e}")

# Export singleton
sync_engine = SyncEngine()

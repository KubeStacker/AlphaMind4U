import logging
from etl.utils.factory import get_provider
from etl.tasks.stock_basic_task import StockBasicTask
from etl.tasks.daily_market_data_task import DailyMarketDataTask
from etl.tasks.capital_flow_task import CapitalFlowTask
from etl.tasks.concepts_task import ConceptsTask
from etl.tasks.market_index_task import MarketIndexTask
from etl.tasks.financials_task import FinancialsTask
from etl.tasks.reference_data_task import ReferenceDataTask
from etl.tasks.calendar_task import CalendarTask
from etl.tasks.margin_trading_task import MarginTradingTask
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
        self.reference_data_task = ReferenceDataTask(self.provider)
        self.calendar_task = CalendarTask(self.provider)
        self.margin_trading_task = MarginTradingTask(self.provider)

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

    def sync_daily_market_data(self, years: int = 1, force: bool = False):
        """同步每日市场数据
        
        Args:
            years: 同步的年数
            force: 是否强制重新同步
        """
        self.daily_market_data_task.sync_daily_data(years=years, force=force)

    def sync_daily_data_by_date(self, trade_date: str):
        """同步特定日期的市场数据
        
        Args:
            trade_date: 交易日期
        """
        self.daily_market_data_task.fetch_and_save_daily_data(trade_date)

    def sync_capital_flow(self, years: int = 0, days: int = 3, force: bool = False):
        """同步资金流向数据
        
        Args:
            years: 同步的年数
            days: 同步的天数
            force: 是否强制重新同步
        """
        self.capital_flow_task.sync_capital_flow(years=years, days=days, force=force)

    def sync_daily_basic(self, years: int = 0, days: int = 3, force: bool = False):
        """同步日频基础指标。"""
        self.reference_data_task.sync_daily_basic(years=years, days=days, force=force)

    def sync_index_member_all(self):
        """同步申万行业层级归属。"""
        self.reference_data_task.sync_index_member_all()

    def sync_express_data(
        self,
        days: int = 120,
        start_date: str | None = None,
        end_date: str | None = None,
    ):
        """同步业绩快报数据。"""
        self.reference_data_task.sync_express(days=days, start_date=start_date, end_date=end_date)

    def perform_daily_data_update(self):
        """执行每日收盘后数据更新任务"""
        import time as _time
        logger.info("========== 每日数据更新开始 ==========")
        steps: list[dict] = []
        total_start = _time.time()

        def _run_step(label: str, fn, *args, **kwargs):
            start = _time.time()
            try:
                result = fn(*args, **kwargs)
                elapsed = _time.time() - start
                status = "[OK]"
                detail = str(result) if result else "完成"
            except Exception as e:
                elapsed = _time.time() - start
                status = "[FAIL]"
                detail = str(e)[:80]
                logger.warning("%s %s | %s | %.1fs", status, label, detail, elapsed)

            steps.append({"label": label, "status": status, "detail": detail, "elapsed": elapsed})
            logger.info("%s %-12s | %s | %5.1fs", status, label, detail, elapsed)

        # ---- 1. 行情与资金流 ----
        _run_step("业绩快报", self.sync_express_data, days=120)
        _run_step("业绩预告", self.sync_forecast_data, days=120)
        _run_step("日线行情", self.sync_daily_market_data, years=1)
        _run_step("资金流向", self.sync_capital_flow, days=3)
        _run_step("日频基础数据", self.sync_daily_basic, days=3)
        _run_step("沪深港通资金", self.sync_moneyflow_hsgt, days=10)
        _run_step("停复牌数据", self.sync_suspend_data, days=3)

        # ---- 2. 指数 ----
        _run_step("核心指数", self.sync_core_market_indices, years=0, days=5)
        _run_step("指数权重", self.sync_index_weight_data, days=90)

        # ---- 3. 融资融券 ----
        _run_step("融资融券", self.sync_margin_trading_data, days=3)

        # ---- 4. 验证 ----
        _run_step("完整性校验", self._validate_daily_update)

        total_elapsed = _time.time() - total_start
        ok_count = sum(1 for s in steps if s["status"] == "[OK]")
        fail_count = sum(1 for s in steps if s["status"] == "[FAIL]")
        logger.info(
            "========== 每日数据更新汇总 | 共 %d 项 成功:%d 失败:%d | 总耗时 %.1fs ==========",
            len(steps), ok_count, fail_count, total_elapsed,
        )

    def _get_latest_trade_date_str(self) -> str:
        from etl.calendar import trading_calendar

        latest = trading_calendar.get_latest_sync_date()
        return latest.strftime("%Y-%m-%d") if hasattr(latest, "strftime") else str(latest)
    
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
    def _sync_concept_classification_once(self):
        """同步概念分类数据主体。"""
        self.concepts_task.sync()

    def sync_concept_classification(self):
        """同步概念分类数据。"""
        self._sync_concept_classification_once()

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
        
        同步常用核心指数:
        - 000001.SH: 上证指数
        - 399006.SZ: 创业板指
        - 000300.SH: 沪深300
        - 399001.SZ: 深证成指
        - 000688.SH: 科创50
        
        Args:
            years: 同步的年数
            days: 同步的天数
        """
        for code in ("000001.SH", "399006.SZ", "000300.SH", "399001.SZ", "000688.SH"):
            self.sync_market_index(ts_code=code, years=years, days=days)

    def sync_financial_statements(self, limit: int = 1000):
        """同步财务报表数据（利润表、资产负债表、现金流量表）
        
        Args:
            limit: 同步限制数量
        """
        self.financials_task.sync_quarterly_income()
        self.financials_task.sync_balance_sheet()
        self.financials_task.sync_cashflow()

    def sync_quarterly_income_statement(self, ts_code: str = None, force: bool = False):
        """同步季度利润表数据"""
        self.financials_task.sync_quarterly_income(ts_code=ts_code, force_sync=force)

    def sync_balance_sheet(self, ts_code: str = None, force: bool = False):
        """同步资产负债表数据"""
        self.financials_task.sync_balance_sheet(ts_code=ts_code, force_sync=force)

    def sync_cashflow(self, ts_code: str = None, force: bool = False):
        """同步现金流量表数据"""
        self.financials_task.sync_cashflow(ts_code=ts_code, force_sync=force)

    def sync_financial_indicators(self, ts_code: str = None):
        """同步财务指标数据"""
        self.financials_task.sync_fina_indicator(ts_code=ts_code)

    def sync_dividend_data(self, ts_code: str = None):
        """同步分红送股数据"""
        self.financials_task.sync_dividend(ts_code=ts_code)

    def sync_forecast_data(
        self,
        days: int = 120,
        start_date: str | None = None,
        end_date: str | None = None,
        ts_code: str | None = None,
    ):
        """同步业绩预告数据"""
        self.financials_task.sync_forecast(
            days=days, start_date=start_date, end_date=end_date, ts_code=ts_code
        )

    def sync_suspend_data(self, years: int = 0, days: int = 3, force: bool = False):
        """同步每日停复牌数据"""
        self.daily_market_data_task.sync_suspend_data(years=years, days=days, force=force)

    def sync_namechange_data(self, ts_code: str = None):
        """同步股票更名历史数据"""
        self.reference_data_task.sync_namechange(ts_code=ts_code)

    def sync_index_weight_data(
        self,
        index_code: str = None,
        start_date: str = None,
        end_date: str = None,
        days: int = 90,
    ):
        """同步指数成分股权重数据"""
        self.reference_data_task.sync_index_weight(
            index_code=index_code, start_date=start_date, end_date=end_date, days=days
        )

    def sync_moneyflow_hsgt(
        self,
        days: int = 30,
        start_date: str = None,
        end_date: str = None,
    ):
        """同步沪深港通资金流向数据"""
        self.capital_flow_task.sync_moneyflow_hsgt(
            days=days, start_date=start_date, end_date=end_date
        )

    def sync_margin_trading_data(self, days: int = 90):
        """同步融资融券数据
        
        Args:
            days: 同步的天数
        """
        self.margin_trading_task.sync_margin_trading(days=days)

# Export singleton
sync_engine = SyncEngine()


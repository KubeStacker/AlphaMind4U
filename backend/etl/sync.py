# /backend/etl/sync.py

import logging
import arrow
import time
import tushare as ts
import pandas as pd
import numpy as np
import json
from functools import wraps
from datetime import date
from core.calendar import trading_calendar
from core.config import settings
from db.connection import get_db_connection, fetch_df, get_fresh_connection
from tenacity import retry, stop_after_attempt, wait_fixed

# Configure logging
logger = logging.getLogger(__name__)

class AShareSyncEngine:
    def __init__(self):
        token = settings.tushare_token
        self.pro = ts.pro_api(token)
        self.pro._DataApi__token = token
        self.pro._DataApi__http_url = 'http://lianghua.nanyangqiankun.top'

    def _get_last_sync_date(self, table: str, date_col: str) -> date | None:
        with get_db_connection() as con:
            result = con.execute(f"SELECT MAX({date_col}) FROM {table}").fetchone()
            return result[0] if result and result[0] else None

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_stock_basic(self):
        """ 同步股票基础信息 (实现 stock_basic 同步) """
        logger.info("正在同步股票基础信息...")
        df = self.pro.stock_basic(list_status='L', fields='ts_code,symbol,name,area,industry,market,list_date,fullname,enname,curr_type,list_status,is_hs')
        if not df.empty:
            df['list_date'] = pd.to_datetime(df['list_date']).dt.date
            with get_db_connection() as con:
                con.execute("INSERT INTO stock_basic SELECT * FROM df ON CONFLICT DO NOTHING")
        return len(df)


    def sync_daily_price(self, years: int = 1, force: bool = False, calc_factors: bool = True):
        """ 
        同步 A 股日线数据。
        - years: 同步多少年的数据
        - force: 是否强制重新同步所有日期 (慎用)
        - calc_factors: 是否同步计算因子 (全量同步时建议关闭，改用批量计算)
        """
        logger.info(f"开始同步最近 {years} 年的数据...")
        end_date = arrow.now()
        start_date = end_date.shift(years=-years)
        
        # 1. 获取目标范围内的所有交易日
        target_dates = {r.format("YYYY-MM-DD") for r in arrow.Arrow.range('day', start_date, end_date) 
                         if trading_calendar.is_trading_day(r.date())}
        
        # 2. 获取数据库中已有的且数据量充足的日期
        existing_query = """
        SELECT trade_date 
        FROM daily_price 
        WHERE trade_date BETWEEN ? AND ?
        GROUP BY trade_date 
        HAVING COUNT(*) > 1000
        """
        existing_dates = set()
        if not force:
            df_existing = fetch_df(existing_query, params=[start_date.date(), end_date.date()])
            if not df_existing.empty:
                existing_dates = {d.strftime("%Y-%m-%d") for d in df_existing['trade_date']}

        # 3. 计算需要补课的日期
        dates_to_sync = sorted(list(target_dates - existing_dates), reverse=True)
        
        if not dates_to_sync:
            logger.info("所有日期数据已完整，无需同步。")
            return

        logger.info(f"发现 {len(dates_to_sync)} 个日期需要同步数据...")
        
        for d_str in dates_to_sync:
            d_tushare = d_str.replace("-", "")
            logger.info(f"正在同步 {d_tushare} 的行情与复权因子...")
            try:
                # 尝试获取日线行情
                df_daily = self.pro.daily(trade_date=d_tushare)
                if df_daily.empty:
                    logger.warning(f"{d_tushare} 没有行情数据，可能尚未更新或接口受限")
                    time.sleep(1)
                    continue
                
                # 尝试获取复权因子
                df_adj = self.pro.adj_factor(trade_date=d_tushare)
                if df_adj.empty:
                    logger.warning(f"{d_tushare} 没有复权因子数据")
                    df = df_daily.copy()
                    df['adj_factor'] = 1.0
                else:
                    df = pd.merge(df_daily, df_adj[['ts_code', 'adj_factor']], on='ts_code', how='left')
                    df['adj_factor'] = df['adj_factor'].fillna(1.0)
                
                df['factors'] = '{}'
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
                
                cols = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount', 'factors', 'adj_factor']
                df_to_save = df[cols]
                with get_db_connection() as con:
                    con.execute("INSERT INTO daily_price SELECT * FROM df_to_save ON CONFLICT DO NOTHING")
                
                logger.info(f"已同步 {d_tushare} (跳过已存在记录)")

                # --- 优化：仅在 flag 开启时计算因子 ---
                if calc_factors:
                    try:
                        self.calculate_factors(d_str)
                    except Exception as fe:
                        logger.error(f"因子计算失败 {d_str}: {fe}")

                # 成功同步后，保持稳定节奏
                time.sleep(0.6) 
            except Exception as e:
                err_msg = str(e)
                logger.error(f"同步 {d_tushare} 失败: {err_msg}")
                if "每分钟内限制" in err_msg or "频次限制" in err_msg:
                    logger.warning("触发 Tushare 频次限制，等待 60 秒...")
                    time.sleep(60)
                else:
                    time.sleep(5)

    def calculate_factors_batch(self, start_date_str: str, end_date_str: str):
        """ 
        批量计算指定时间段内的因子。
        相比逐日计算，大幅减少 I/O 和重复计算，防止内存溢出。
        """
        logger.info(f"正在批量计算 {start_date_str} 至 {end_date_str} 的因子...")
        
        # 增加回溯时长以确保 250 日动量有足够数据 (450天)
        window_start = arrow.get(start_date_str).shift(days=-450).format("YYYY-MM-DD")
        
        update_query = """
        WITH RawData AS (
            SELECT 
                ts_code, 
                trade_date, 
                close,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20,
                (close - LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_20,
                (close - LAG(close, 50) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 50) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_50,
                (close - LAG(close, 120) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 120) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_120,
                (close - LAG(close, 250) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 250) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_250,
                COUNT(*) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as row_num
            FROM daily_price
            WHERE trade_date BETWEEN ? AND ?
        ),
        FilteredData AS (
            SELECT * FROM RawData WHERE trade_date BETWEEN ? AND ? AND row_num >= 20
        ),
        RPSData AS (
            SELECT 
                *,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_20) * 100 as rps_20,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_50) * 100 as rps_50,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_120) * 100 as rps_120,
                PERCENT_RANK() OVER (PARTITION BY trade_date ORDER BY mom_250) * 100 as rps_250
            FROM FilteredData
        ),
        FinalFactors AS (
            SELECT 
                ts_code,
                trade_date,
                json_object(
                    'ma5', CASE WHEN isnan(ma5) THEN NULL ELSE round(ma5, 2) END, 
                    'ma10', CASE WHEN isnan(ma10) THEN NULL ELSE round(ma10, 2) END, 
                    'ma20', CASE WHEN isnan(ma20) THEN NULL ELSE round(ma20, 2) END, 
                    'rps_20', CASE WHEN isnan(rps_20) THEN NULL ELSE round(rps_20, 1) END,
                    'rps_50', CASE WHEN isnan(rps_50) THEN NULL ELSE round(rps_50, 1) END,
                    'rps_120', CASE WHEN isnan(rps_120) THEN NULL ELSE round(rps_120, 1) END,
                    'rps_250', CASE WHEN isnan(rps_250) THEN NULL ELSE round(rps_250, 1) END
                ) as factors_json
            FROM RPSData
        )
        UPDATE daily_price
        SET factors = sub.factors_json
        FROM FinalFactors sub
        WHERE daily_price.ts_code = sub.ts_code AND daily_price.trade_date = sub.trade_date
        """
        
        try:
            con = get_fresh_connection()
            try:
                # Params: [WindowStart, EndDate, StartDate, EndDate]
                con.execute(update_query, [window_start, end_date_str, start_date_str, end_date_str])
                logger.info(f"已完成 {start_date_str} 至 {end_date_str} 的因子批量更新")
            finally:
                con.close()
        except Exception as e:
            logger.error(f"批量因子更新失败 {start_date_str}-{end_date_str}: {e}")

    def sync_moneyflow(self, years: int = 1, force: bool = False):
        """ 
        同步 A 股个股资金流向数据。
        """
        logger.info(f"开始同步最近 {years} 年的资金流向数据...")
        end_date = arrow.now()
        start_date = end_date.shift(years=-years)
        
        target_dates = {r.format("YYYY-MM-DD") for r in arrow.Arrow.range('day', start_date, end_date) 
                         if trading_calendar.is_trading_day(r.date())}
        
        existing_query = """
        SELECT trade_date FROM stock_moneyflow 
        WHERE trade_date BETWEEN ? AND ?
        GROUP BY trade_date HAVING COUNT(*) > 1000
        """
        existing_dates = set()
        if not force:
            df_existing = fetch_df(existing_query, params=[start_date.date(), end_date.date()])
            if not df_existing.empty:
                existing_dates = {d.strftime("%Y-%m-%d") for d in df_existing['trade_date']}

        dates_to_sync = sorted(list(target_dates - existing_dates), reverse=True)
        
        if not dates_to_sync:
            logger.info("资金流向数据已完整。")
            return

        for d_str in dates_to_sync:
            d_tushare = d_str.replace("-", "")
            logger.info(f"正在同步 {d_tushare} 的资金流向...")
            try:
                df = self.pro.moneyflow(trade_date=d_tushare)
                if not df.empty:
                    df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
                    with get_db_connection() as con:
                        con.execute("INSERT INTO stock_moneyflow SELECT * FROM df ON CONFLICT DO NOTHING")
                time.sleep(0.6)
            except Exception as e:
                logger.error(f"同步 {d_tushare} 资金流向失败: {e}")
                time.sleep(5)

    def sync_daily_update(self):
        """ 每日收盘后更新任务 (通常在 15:30 后执行) """
        logger.info("执行每日收盘数据更新...")
        
        # 1. 行情与资金流 (内部会自动补足最近几日缺失)
        self.sync_daily_price(years=1)
        self.sync_moneyflow(years=1)
        
        # 2. 指数同步
        self.sync_market_index(years=1)
        
        # 3. 情绪计算 (更新最近 30 天，确保平滑)
        self.calculate_market_sentiment(days=30)
        
        logger.info("每日收盘数据更新完成")


    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_concepts(self):
        """ 同步概念分类及股票关联明细 """
        logger.info("开始同步概念分类列表...")
        try:
            df_concept = self.pro.concept(src='ts')
            if not df_concept.empty:
                with get_db_connection() as con:
                    con.execute("INSERT INTO stock_concepts SELECT * FROM df_concept ON CONFLICT DO NOTHING")
                logger.info(f"成功同步 {len(df_concept)} 个概念分类")
                
                count = 0
                for index, row in df_concept.iterrows():
                    concept_id = row['code']
                    concept_name = row['name']
                    logger.info(f"正在同步概念股票关联: {concept_name} ({concept_id})")
                    
                    try:
                        df_detail = self.pro.concept_detail(id=concept_id)
                        if not df_detail.empty:
                            processed_detail = df_detail[['id', 'concept_name', 'ts_code', 'name']]
                            with get_db_connection() as con:
                                con.execute("INSERT INTO stock_concept_details SELECT * FROM processed_detail ON CONFLICT DO NOTHING")
                        
                        count += 1
                        # 动态休眠：每 10 个概念明细同步后增加长休眠
                        if count % 10 == 0:
                            time.sleep(2.0)
                        else:
                            time.sleep(0.6)
                    except Exception as e:
                        if "每分钟内限制" in str(e):
                            logger.warning("触发概念明细频次限制，等待 30 秒...")
                            time.sleep(30)
                        else:
                            logger.error(f"同步概念 {concept_name} 明细失败: {e}")
                            time.sleep(2)
        except Exception as e:
            logger.error(f"同步概念列表失败: {e}")

    def calculate_factors(self, trade_date: str):
        """ 
        计算全市场指定交易日的技术因子 (MA, RPS等) 并持久化到 daily_price.factors 字段。
        使用 SQL 窗口函数一次性完成计算和更新，效率提升百倍。
        """
        logger.info(f"正在通过 SQL 计算并更新 {trade_date} 的技术指标因子...")
        
        # 转换日期格式以确保兼容性
        try:
            target_date = arrow.get(trade_date).format("YYYY-MM-DD")
            # 增加回溯时长以确保 250 日动量有足够数据 (约 450 个自然日)
            start_date = arrow.get(trade_date).shift(days=-450).format("YYYY-MM-DD")
        except Exception as e:
            logger.error(f"日期格式解析失败 {trade_date}: {e}")
            return

        update_query = """
        WITH RawData AS (
            SELECT 
                ts_code, 
                trade_date, 
                close,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as ma5,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as ma10,
                AVG(close) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20,
                (close - LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 20) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_20,
                (close - LAG(close, 50) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 50) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_50,
                (close - LAG(close, 120) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 120) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_120,
                (close - LAG(close, 250) OVER (PARTITION BY ts_code ORDER BY trade_date)) / NULLIF(LAG(close, 250) OVER (PARTITION BY ts_code ORDER BY trade_date), 0) as mom_250,
                COUNT(*) OVER (PARTITION BY ts_code ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as row_num
            FROM daily_price
            WHERE trade_date BETWEEN ? AND ?
        ),
        FilteredData AS (
            SELECT * FROM RawData WHERE trade_date = ? AND row_num >= 20
        ),
        RPSData AS (
            SELECT 
                *,
                PERCENT_RANK() OVER (ORDER BY mom_20) * 100 as rps_20,
                PERCENT_RANK() OVER (ORDER BY mom_50) * 100 as rps_50,
                PERCENT_RANK() OVER (ORDER BY mom_120) * 100 as rps_120,
                PERCENT_RANK() OVER (ORDER BY mom_250) * 100 as rps_250
            FROM FilteredData
        ),
        FinalFactors AS (
            SELECT 
                ts_code,
                trade_date,
                json_object(
                    'ma5', CASE WHEN isnan(ma5) THEN NULL ELSE round(ma5, 2) END, 
                    'ma10', CASE WHEN isnan(ma10) THEN NULL ELSE round(ma10, 2) END, 
                    'ma20', CASE WHEN isnan(ma20) THEN NULL ELSE round(ma20, 2) END, 
                    'rps_20', CASE WHEN isnan(rps_20) THEN NULL ELSE round(rps_20, 1) END,
                    'rps_50', CASE WHEN isnan(rps_50) THEN NULL ELSE round(rps_50, 1) END,
                    'rps_120', CASE WHEN isnan(rps_120) THEN NULL ELSE round(rps_120, 1) END,
                    'rps_250', CASE WHEN isnan(rps_250) THEN NULL ELSE round(rps_250, 1) END
                ) as factors_json
            FROM RPSData
        )
        UPDATE daily_price
        SET factors = sub.factors_json
        FROM FinalFactors sub
        WHERE daily_price.ts_code = sub.ts_code AND daily_price.trade_date = sub.trade_date
        """
        
        try:
            # 使用独立连接以避免长时间锁定共享连接
            con = get_fresh_connection()
            try:
                con.execute(update_query, [start_date, target_date, target_date])
                logger.info(f"已成功完成 {target_date} 的因子 SQL 更新")
            finally:
                con.close()
        except Exception as e:
            logger.error(f"SQL 因子更新失败 {target_date}: {e}")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def sync_market_index(self, ts_code: str = '000001.SH', years: int = 1):
        """ 同步大盘指数 (默认上证指数) """
        logger.info(f"正在同步市场指数 {ts_code} ...")
        end_date = arrow.now()
        start_date = end_date.shift(years=-years).format("YYYYMMDD")
        end_date_str = end_date.format("YYYYMMDD")
        
        try:
            df = self.pro.index_daily(ts_code=ts_code, start_date=start_date, end_date=end_date_str)
            if not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d').dt.date
                # 确保字段匹配
                cols = ['trade_date', 'ts_code', 'open', 'high', 'low', 'close', 'pre_close', 'change', 'pct_chg', 'vol', 'amount']
                df_to_save = df[cols]
                with get_db_connection() as con:
                    con.execute("INSERT INTO market_index SELECT * FROM df_to_save ON CONFLICT DO NOTHING")
                logger.info(f"指数 {ts_code} 同步完成，共 {len(df)} 条记录")
        except Exception as e:
            logger.error(f"同步指数失败: {e}")

    def calculate_market_sentiment(self, days: int = 30):
        """ 
        委派给策略模块进行市场情绪分析
        """
        from strategy.sentiment import sentiment_analyst
        sentiment_analyst.calculate(days=days)


    def sync_financials(self, limit: int = 1000):
        """ 同步财务指标数据 """
        logger.info(f"开始同步前 {limit} 只股票的财务指标...")
        # 获取所有股票代码
        stocks = fetch_df("SELECT ts_code FROM stock_basic")['ts_code'].tolist()
        if not stocks: 
            logger.warning("未找到股票基本信息，请先同步 stock_basic")
            return
        
        count = 0
        # 为了演示和避免过度占用 Tushare 积分，默认同步 1000 只
        for ts_code in stocks[:limit]:
            try:
                # 检查是否已经同步过最近一年的财务数据（简单判断）
                df = self.pro.fina_indicator(ts_code=ts_code)
                if not df.empty:
                    df['ann_date'] = pd.to_datetime(df['ann_date']).dt.date
                    df['end_date'] = pd.to_datetime(df['end_date']).dt.date
                    
                    # 定义数据库表要求的完整字段列表 (17个字段)
                    fields = [
                        'ts_code','ann_date','end_date','eps','dt_eps','total_revenue_ps',
                        'revenue_ps','capital_rese_ps','surplus_rese_ps','undist_profit_ps',
                        'extra_item','profit_dedt','gross_margin','net_profit_margin','roe',
                        'roa','debt_to_assets'
                    ]
                    
                    # 使用 reindex 强制对齐列，缺失字段将填充 NaN (DuckDB 识别为 NULL)
                    processed = df.reindex(columns=fields)
                    
                    with get_db_connection() as con:
                        con.execute("INSERT INTO stock_financials SELECT * FROM processed ON CONFLICT DO NOTHING")
                
                count += 1
                # 动态休眠：每 5 次同步后增加长休眠以缓解 IO 压力
                if count % 5 == 0:
                    time.sleep(1.5)
                else:
                    time.sleep(0.4)
            except Exception as e:
                logger.error(f"同步 {ts_code} 财务数据失败: {e}")
                if "每分钟内限制" in str(e):
                    time.sleep(60)
                else:
                    time.sleep(2)

    def fill_missing_factors(self):
        """ 补全所有缺失的 daily_price.factors 字段 """
        logger.info("检查并补全缺失的因子数据...")
        with get_db_connection() as con:
            # Find dates with missing factors
            dates = con.execute("SELECT DISTINCT trade_date FROM daily_price WHERE factors IS NULL OR factors = '{}' OR factors = 'null'").fetchall()
        
        dates = [d[0] for d in dates]
        if not dates:
            logger.info("所有行情因子的数据已完整。")
            return

        logger.info(f"发现 {len(dates)} 个交易日存在因子缺失，开始计算...")
        for d in dates:
            try:
                # Ensure d is a valid date object or string format
                d_str = d.strftime("%Y-%m-%d") if hasattr(d, 'strftime') else str(d)
                self.calculate_factors(d_str)
            except Exception as e:
                logger.error(f"计算 {d} 因子失败: {e}")

# 导出同步引擎单例
sync_engine = AShareSyncEngine()
            
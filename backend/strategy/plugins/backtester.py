# /backend/strategy/plugins/backtester.py

import logging
import pandas as pd
import json
from datetime import datetime, timedelta
from db.connection import get_db_connection, fetch_df
from strategy.plugins.base import BaseStrategyPlugin

logger = logging.getLogger(__name__)

class BacktestPlugin(BaseStrategyPlugin):
    """
    回测验证插件 (Backtest Verifier)
    负责：
    1. 持久化当日推荐结果
    2. 计算推荐标的的后续收益率 (P5, P10)
    """
    
    @property
    def name(self):
        return "backtester"

    def run(self, **kwargs):
        """
        触发收益验证任务：
        1. 获取历史推荐但尚未计算收益的数据
        2. 更新 P5/P10 收益
        """
        logger.info("正在执行回测收益验证任务...")
        return self.verify_all_pending()

    def record_recommendations(self, date, strategy_name, recommendations):
        """
        保存推荐结果到数据库
        """
        if not recommendations:
            return
            
        data_to_save = []
        for rec in recommendations:
            data_to_save.append((
                date,
                rec['ts_code'],
                rec.get('name'),
                rec.get('score'),
                strategy_name,
                json.dumps(rec)
            ))
            
        with get_db_connection() as con:
            con.executemany("""
                INSERT INTO strategy_recommendations (recommend_date, ts_code, name, score, strategy_name, filters_used)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (recommend_date, ts_code, strategy_name) DO UPDATE SET
                    score = excluded.score,
                    filters_used = excluded.filters_used
            """, data_to_save)
        
        logger.info(f"已持久化 {len(data_to_save)} 条推荐记录，日期: {date}")

    def verify_all_pending(self):
        """
        扫描缺失收益率的记录并尝试计算
        """
        # 1. 查找缺失 p5 或 p10 的记录
        query = "SELECT DISTINCT recommend_date FROM strategy_recommendations WHERE p5_return IS NULL OR p10_return IS NULL"
        pending_dates_df = fetch_df(query)
        if pending_dates_df.empty:
            return {"status": "success", "message": "没有待验证的推荐记录"}
            
        updated_count = 0
        for target_date in pending_dates_df['recommend_date']:
            updated_count += self.calculate_returns_for_date(target_date)
            
        return {"status": "success", "message": f"收益验证完成，更新了 {updated_count} 条记录"}

    def calculate_returns_for_date(self, recommend_date):
        """
        计算特定日期推荐标的的后续收益
        """
        # 获取该日所有推荐标的
        rec_query = f"SELECT ts_code FROM strategy_recommendations WHERE recommend_date = '{recommend_date}'"
        recs = fetch_df(rec_query)
        if recs.empty: return 0
        
        ts_codes = recs['ts_code'].tolist()
        
        # 获取后续行情数据 (为了准确性，我们需要获取 recommend_date 之后的交易日)
        # 获取 recommend_date 之后的 15 个交易日
        date_query = f"SELECT DISTINCT trade_date FROM daily_price WHERE trade_date >= '{recommend_date}' ORDER BY trade_date ASC LIMIT 15"
        dates_df = fetch_df(date_query)
        if len(dates_df) < 2: return 0
        
        trading_dates = dates_df['trade_date'].tolist()
        # trading_dates[0] 是推荐日（或信号触发日），收益从下一个交易日开始算起，或者以当日收盘价为基准
        base_date = trading_dates[0]
        
        updates = []
        for ts_code in ts_codes:
            # 获取该股票在这段日期的价格
            price_query = f"SELECT trade_date, close FROM daily_price WHERE ts_code = '{ts_code}' AND trade_date IN {str(tuple(trading_dates)).replace(',)', ')')}"
            prices = fetch_df(price_query)
            if prices.empty: continue
            
            # 以基准日收盘价为分母
            base_price_row = prices[prices['trade_date'] == base_date]
            if base_price_row.empty: continue
            base_price = base_price_row['close'].iloc[0]
            
            # P5: 5个交易日后的收益 (如果存在)
            p5_ret = None
            if len(trading_dates) > 5:
                p5_date = trading_dates[5]
                p5_price_row = prices[prices['trade_date'] == p5_date]
                if not p5_price_row.empty:
                    p5_ret = round((p5_price_row['close'].iloc[0] / base_price - 1) * 100, 2)
            
            # P10: 10个交易日后的收益 (如果存在)
            p10_ret = None
            if len(trading_dates) > 10:
                p10_date = trading_dates[10]
                p10_price_row = prices[prices['trade_date'] == p10_date]
                if not p10_price_row.empty:
                    p10_ret = round((p10_price_row['close'].iloc[0] / base_price - 1) * 100, 2)
            
            if p5_ret is not None or p10_ret is not None:
                updates.append((p5_ret, p10_ret, recommend_date, ts_code))

        if updates:
            with get_db_connection() as con:
                con.executemany("""
                    UPDATE strategy_recommendations 
                    SET p5_return = ?, p10_return = ?
                    WHERE recommend_date = ? AND ts_code = ?
                """, updates)
                
        return len(updates)

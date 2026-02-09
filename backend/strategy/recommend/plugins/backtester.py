# /backend/strategy/recommend/plugins/backtester.py

import logging
import pandas as pd
import json
from datetime import datetime, timedelta
from db.connection import get_db_connection, fetch_df
from strategy.recommend.plugins.base import BaseStrategyPlugin

logger = logging.getLogger(__name__)

class BacktestPlugin(BaseStrategyPlugin):
    """
    回测验证插件 (Backtest Verifier)
    
    功能：
    1. 持久化当日的推荐结果，以便后续验证。
    2. 定期扫描历史推荐记录，计算 T+1, T+3, T+5, T+10 的实际收益率。
    3. 验证"市场情绪指标" (Market Sentiment) 的准确率 (胜率统计)。
    """
    
    @property
    def name(self):
        return "backtester"

    def run(self, **kwargs):
        """
        触发收益验证任务：
        1. 获取历史推荐但尚未计算收益的数据
        2. 更新 P5/P10 收益
        3. 验证市场情绪择时准确率
        """
        logger.info("正在执行回测收益验证任务...")
        stock_res = self.verify_all_pending()
        sentiment_res = self.verify_market_sentiment()
        return {
            "stock_verification": stock_res,
            "sentiment_verification": sentiment_res
        }

    def verify_market_sentiment(self):
        """
        验证市场情绪信号准确率 (Quantify Signal Accuracy)
        统计 BUY/SELL 信号发出后，指数在 T+1, T+3, T+5 的表现。
        
        胜率定义：
        - BUY 信号：T+5 收益 > 0
        - SELL 信号：T+5 收益 < 0 (成功逃顶)
        """
        logger.info("正在验证市场情绪信号准确率...")
        
        # 1. 获取所有历史信号 (Updated for V21 Labels)
        # 包含旧标签(兼容)和新标签
        query = """
        SELECT trade_date, label, score 
        FROM market_sentiment 
        WHERE 
           label LIKE '%进攻%' OR label LIKE '%抢筹%' OR label LIKE '%止盈%' OR label LIKE '%减仓%'
           OR label IN ('冰点反转', '良性分歧', '高潮退潮')
        ORDER BY trade_date ASC
        """
        signals = fetch_df(query)
        if signals.empty:
            return {"message": "无足够情绪信号数据"}

        # 2. 获取指数历史行情
        idx_query = "SELECT trade_date, close, pct_chg FROM market_index WHERE ts_code='000001.SH' ORDER BY trade_date ASC"
        idx_df = fetch_df(idx_query)
        if idx_df.empty: return {"message": "无指数数据"}
        
        # 构建日期索引
        idx_df['trade_date'] = pd.to_datetime(idx_df['trade_date']).dt.date
        date_to_idx = {row['trade_date']: i for i, row in idx_df.iterrows()}
        idx_closes = idx_df['close'].tolist()
        
        results = []
        
        for _, row in signals.iterrows():
            # Convert to date object
            s_ts = pd.to_datetime(row['trade_date'])
            s_date = s_ts.date() if hasattr(s_ts, 'date') else s_ts
            
            # Determine Signal Type
            lbl = row['label']
            if any(x in lbl for x in ['进攻', '抢筹', '建仓', '冰点', '分歧']):
                s_type = "BUY"
            elif any(x in lbl for x in ['止盈', '减仓', '退潮']):
                s_type = "SELL"
            else:
                s_type = "WATCH"
            
            if s_date not in date_to_idx: continue
            
            start_idx = date_to_idx[s_date]
            base_close = idx_closes[start_idx]
            
            # 计算未来收益
            returns = {}
            for day in [1, 3, 5, 10]:
                target_idx = start_idx + day
                if target_idx < len(idx_closes):
                    ret = (idx_closes[target_idx] / base_close - 1) * 100
                    returns[f'T+{day}'] = round(ret, 2)
                else:
                    returns[f'T+{day}'] = None
            
            results.append({
                "date": s_date,
                "type": s_type,
                "label": lbl,
                "returns": returns
            })
            
        # 3. 统计胜率 (以 T+5 为准)
        buy_wins = [r for r in results if r['type'] == 'BUY' and r['returns'].get('T+5') is not None and r['returns']['T+5'] > 0]
        buy_total = [r for r in results if r['type'] == 'BUY' and r['returns'].get('T+5') is not None]
        
        sell_wins = [r for r in results if r['type'] == 'SELL' and r['returns'].get('T+5') is not None and r['returns']['T+5'] < 0] # 卖出后下跌算赢
        sell_total = [r for r in results if r['type'] == 'SELL' and r['returns'].get('T+5') is not None]
        
        stats = {
            "buy_count": len(buy_total),
            "buy_win_rate": f"{len(buy_wins)/len(buy_total)*100:.1f}%" if buy_total else "N/A",
            "sell_count": len(sell_total),
            "sell_win_rate": f"{len(sell_wins)/len(sell_total)*100:.1f}%" if sell_total else "N/A"
        }
        
        logger.info(f"情绪信号回测结果: {json.dumps(stats, ensure_ascii=False)}")
        return stats

    def record_recommendations(self, date, strategy_name, recommendations):
        """
        保存推荐结果到数据库表 strategy_recommendations
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
        # 1. 查找缺失 p1, p5 或 p10 的记录 (优先看 p1 以便及时更新)
        query = "SELECT DISTINCT recommend_date FROM strategy_recommendations WHERE p1_return IS NULL OR p5_return IS NULL OR p10_return IS NULL"
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
        
        # 获取后续行情数据 (取15天以覆盖T+10)
        date_query = f"SELECT DISTINCT trade_date FROM daily_price WHERE trade_date >= '{recommend_date}' ORDER BY trade_date ASC LIMIT 15"
        dates_df = fetch_df(date_query)
        if len(dates_df) < 2: return 0
        
        trading_dates = dates_df['trade_date'].tolist()
        # date_strs for query
        date_strs = [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) for d in trading_dates]
        date_tuple_str = str(tuple(date_strs)).replace(',)', ')')
        
        base_date = trading_dates[0]
        
        updates = []
        for ts_code in ts_codes:
            # 获取该股票在这段日期的价格
            price_query = f"SELECT trade_date, close FROM daily_price WHERE ts_code = '{ts_code}' AND trade_date IN {date_tuple_str}"
            prices = fetch_df(price_query)
            if prices.empty: continue
            
            # 以基准日收盘价为分母
            base_price_row = prices[prices['trade_date'] == base_date]
            if base_price_row.empty: continue
            base_price = base_price_row['close'].iloc[0]
            
            # P1: 1个交易日后的收益
            p1_ret = None
            if len(trading_dates) > 1:
                p1_date = trading_dates[1]
                p1_price_row = prices[prices['trade_date'] == p1_date]
                if not p1_price_row.empty:
                    p1_ret = round((p1_price_row['close'].iloc[0] / base_price - 1) * 100, 2)

            # P3: 3个交易日后的收益
            p3_ret = None
            if len(trading_dates) > 3:
                p3_date = trading_dates[3]
                p3_price_row = prices[prices['trade_date'] == p3_date]
                if not p3_price_row.empty:
                    p3_ret = round((p3_price_row['close'].iloc[0] / base_price - 1) * 100, 2)

            # P5: 5个交易日后的收益
            p5_ret = None
            if len(trading_dates) > 5:
                p5_date = trading_dates[5]
                p5_price_row = prices[prices['trade_date'] == p5_date]
                if not p5_price_row.empty:
                    p5_ret = round((p5_price_row['close'].iloc[0] / base_price - 1) * 100, 2)
            
            # P10: 10个交易日后的收益
            p10_ret = None
            if len(trading_dates) > 10:
                p10_date = trading_dates[10]
                p10_price_row = prices[prices['trade_date'] == p10_date]
                if not p10_price_row.empty:
                    p10_ret = round((p10_price_row['close'].iloc[0] / base_price - 1) * 100, 2)
            
            if any(v is not None for v in [p1_ret, p3_ret, p5_ret, p10_ret]):
                updates.append((p1_ret, p3_ret, p5_ret, p10_ret, recommend_date, ts_code))

        if updates:
            with get_db_connection() as con:
                con.executemany("""
                    UPDATE strategy_recommendations 
                    SET p1_return = ?, p3_return = ?, p5_return = ?, p10_return = ?
                    WHERE recommend_date = ? AND ts_code = ?
                """, updates)
                
        return len(updates)
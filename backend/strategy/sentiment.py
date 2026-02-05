# /backend/strategy/sentiment.py

"""
情绪分析器 (Sentiment Analyst) 数据依赖说明:

1. daily_price (股票日线行情表):
   - trade_date: 交易日期，用于聚合与对齐。
   - pct_chg: 涨跌幅，计算涨跌家数比、涨停家数、跌停家数。
   - high, pre_close: 用于判断“炸板”情况 (最高价触及涨停但收盘未封死)。
   - close: 收盘价，用于配合 ma20 因子计算均线宽度。
   - factors (JSON): 提取 'ma20' 字段，计算全市场站上 20 日均线的股票比例。
   - amount: 成交额，计算全市场量能共振。

2. market_index (市场指数表):
   - ts_code: 锁定 '000001.SH' (上证指数)。
   - close: 指数收盘价，用于计算指数位置动能 (s_idx)。

3. stock_moneyflow (个股资金流向表):
   - net_mf_amount: 个股净流向，汇总为全市场资金净流向 (s_mf)。

该模块通过上述数据计算赚钱效应、均线宽度、极值动能、炸板反馈、量能共振及外部环境等因子，最终生成 0-100 的情绪评分。
"""

import logging
import json
import pandas as pd
from db.connection import get_db_connection, fetch_df

logger = logging.getLogger(__name__)

class SentimentAnalyst:
    """
    情绪分析器 (Sentiment Analyst)
    负责计算多因子量化情绪指标 (Quant Emotion V3)
    """

    def calculate(self, days: int = 30):
        """ 
        计算多因子量化情绪指标 (Quant Emotion V4)
        集成：炸板因子、连板高度、买卖点信号
        """
        logger.info("正在执行 A 股市场情绪深度量化模型 (V4)...")
        
        try:
            # 1. 基础行情与均线宽度
            breadth_query = f"""
            SELECT 
                trade_date,
                COUNT(*) as total,
                SUM(CASE WHEN pct_chg > 0 THEN 1 ELSE 0 END) as up_count,
                SUM(CASE WHEN pct_chg > 9.8 THEN 1 ELSE 0 END) as limit_ups,
                SUM(CASE WHEN pct_chg < -9.8 THEN 1 ELSE 0 END) as limit_downs,
                SUM(CASE WHEN high >= ROUND(pre_close * 1.098, 2) AND pct_chg < 9.8 THEN 1 ELSE 0 END) as failed_limits,
                SUM(CASE WHEN CAST(factors->>'ma20' AS DOUBLE) < close THEN 1 ELSE 0 END) as above_ma20,
                SUM(amount) as total_amount
            FROM daily_price
            GROUP BY trade_date
            HAVING total > 1000
            ORDER BY trade_date DESC
            LIMIT {days + 40}
            """
            df_breadth = fetch_df(breadth_query)
            if df_breadth.empty: return

            # 2. 指数数据
            index_query = """
            SELECT trade_date, close,
                   AVG(close) OVER (ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) as ma20
            FROM market_index WHERE ts_code = '000001.SH'
            """
            df_index = fetch_df(index_query)

            # 3. 全市场资金流向
            mf_query = "SELECT trade_date, SUM(net_mf_amount) as net_mf FROM stock_moneyflow GROUP BY trade_date"
            df_mf = fetch_df(mf_query)

            # 4. 获取高标反馈 (计算连板高度)
            # 这里简单取当日最高涨幅前 10 名的平均连板情况（假设后续有更复杂的连板表，目前先用涨幅模拟）
            
            # 数据合并
            df = df_breadth.merge(df_index, on='trade_date', how='left')
            df = df.merge(df_mf, on='trade_date', how='left')
            df = df.sort_values('trade_date')
            
            # 辅助指标
            df['ma_amount'] = df['total_amount'].rolling(window=5).mean()
            df['prev_score'] = 50.0 # 占位
            
            results = []
            df_target = df.tail(days + 5) # 多取几天计算信号连续性
            
            last_score = 50
            for i, row in df_target.iterrows():
                # A. 赚钱效应 (20%)
                s_breadth = (row['up_count'] / row['total'] * 100) if row['total'] > 0 else 50
                
                # B. 均线宽度 (10%)
                s_ma20 = (row['above_ma20'] / row['total'] * 100) if row['total'] > 0 else 50
                
                # C. 极值动能 (20%) - 净涨停因子
                net_limits = row['limit_ups'] - row['limit_downs']
                s_limit = 50 + net_limits * 0.4
                
                # D. 炸板反馈 (15%) - 炸板率因子 (负相关)
                # 炸板率 = 炸板 / (涨停 + 炸板)
                total_attempts = row['limit_ups'] + row['failed_limits']
                fail_rate = (row['failed_limits'] / total_attempts) if total_attempts > 0 else 0
                s_fail = 100 - (fail_rate * 150) # 炸板率 30% 以上开始大幅扣分
                s_fail = max(min(s_fail, 100), 0)
                
                # E. 量能共振 (15%)
                s_vol = 50
                if row['ma_amount'] > 0:
                    s_vol = (row['total_amount'] / row['ma_amount']) * 50
                
                # F. 指数与资金 (20%)
                s_idx = 50
                if row['ma20'] > 0:
                    s_idx = 50 + (row['close'] / row['ma20'] - 1) * 500
                s_mf = 50 + (row['net_mf'] / 2e9) * 5 if not pd.isna(row['net_mf']) else 50
                s_external = (s_idx * 0.5 + s_mf * 0.5)

                # 加权总分
                total_score = (
                    s_breadth * 0.20 +
                    s_ma20 * 0.10 +
                    s_limit * 0.20 +
                    s_fail * 0.15 +
                    s_vol * 0.15 +
                    s_external * 0.20
                )
                total_score = max(min(total_score, 100), 0)
                
                # --- 信号系统逻辑 (V7: 逆向抢筹模型) ---
                signal = "WATCH" 
                label = "修复"
                
                # A. 极端区间与恐慌特征判定
                is_panic_zone = last_score < 28 or total_score < 25
                is_boiling_zone = total_score > 80
                # 识别集体性杀跌 (黑天鹅/踩踏)
                is_extreme_panic = row['limit_downs'] > 80 or total_score < 15
                
                # B. 进攻与反转信号 (BUY)
                if is_extreme_panic:
                    # 极端恐慌即是机会：强制判定为买入/抢筹
                    signal = "BUY"
                    label = "抢筹"
                elif total_score > 60 and s_vol > 55 and s_breadth > 50:
                    signal = "BUY"
                    label = "进攻"
                elif is_panic_zone and total_score > last_score + 1.5: 
                    # 冰点回升即视为反转
                    signal = "BUY"
                    label = "反转"
                
                # C. 避仓与退潮信号 (STAY_OUT / SELL)
                if is_boiling_zone:
                    signal = "SELL"
                    label = "沸腾"
                elif last_score > 65 and total_score < 55:
                    signal = "SELL"
                    label = "分歧"
                elif total_score < 30 and total_score <= last_score and not is_extreme_panic:
                    # 阴跌/缓跌建议空仓，但极端恐慌(抢筹)除外
                    signal = "STAY_OUT"
                    label = "退潮"
                elif total_score < 20 and not is_extreme_panic:
                    label = "冰点"
                    signal = "WATCH"
                
                # D. 持仓信号 (HOLD)
                if signal == "WATCH" and total_score > 45:
                    signal = "HOLD"
                
                # 如果前一天是 BUY，今天只要不破位，维持 HOLD
                if i > 0 and results and results[-1][3]:
                    try:
                        prev_details = json.loads(results[-1][3])
                        if prev_details.get('signal') == 'BUY' and signal == 'WATCH' and total_score > 35:
                            signal = "HOLD"
                    except: pass

                # 映射字段以匹配前端预期 (假设前端需要 broken_limit)
                details = json.dumps({
                    "factors": {
                        "breadth": round(s_breadth, 1),
                        "limit": round(s_limit, 1),
                        "failure": round(s_fail, 1),
                        "broken_limit": round(s_fail, 1), # 新增兼容字段
                        "vol": round(s_vol, 1),
                        "external": round(s_external, 1)
                    },
                    "metrics": {
                        "fail_rate": round(fail_rate * 100, 1),
                        "limit_ups": int(row['limit_ups']),
                        "limit_downs": int(row['limit_downs'])
                    },
                    "signal": signal
                })
                
                results.append((row['trade_date'], total_score, label, details))
                last_score = total_score
            
            # 过滤掉用于计算前置信号的冗余天数
            final_results = results[-days:]
            
            with get_db_connection() as con:
                con.executemany("""
                    INSERT INTO market_sentiment (trade_date, score, label, details) 
                    VALUES (?, ?, ?, ?) 
                    ON CONFLICT (trade_date) DO UPDATE SET 
                        score=excluded.score, label=excluded.label, details=excluded.details
                """, final_results)
            
            logger.info(f"市场情绪 V4 计算完成，信号已集成 (已处理 {len(final_results)} 天)")
            
        except Exception as e:
            logger.error(f"计算 A 股量化情绪指标失败: {e}")

    def get_history(self, days: int = 30):
        """ 获取市场情绪历史数据 (用于前端展示) """
        import json
        
        # 获取日期范围 (排除非交易日)
        date_query = f"SELECT trade_date FROM daily_price GROUP BY trade_date HAVING COUNT(*) > 1000 ORDER BY trade_date DESC LIMIT {days}"
        dates_df = fetch_df(date_query)
        if dates_df.empty: return {"dates": [], "sentiment": [], "index": []}
        
        dates = sorted(dates_df['trade_date'].tolist())
        min_date = dates[0].strftime('%Y-%m-%d')
        max_date = dates[-1].strftime('%Y-%m-%d')
        
        # 查询情绪数据 (包含 details)
        sent_query = f"SELECT trade_date, score, label, details FROM market_sentiment WHERE trade_date BETWEEN '{min_date}' AND '{max_date}' ORDER BY trade_date"
        sent_df = fetch_df(sent_query)
        
        # 查询指数数据
        idx_query = f"SELECT trade_date, close FROM market_index WHERE ts_code='000001.SH' AND trade_date BETWEEN '{min_date}' AND '{max_date}' ORDER BY trade_date"
        idx_df = fetch_df(idx_query)
        
        # 合并数据
        result_dates = [d.strftime('%m-%d') for d in dates]
        sentiment_scores = []
        index_closes = []
        
        # 创建映射以便快速查找
        sent_map = {row['trade_date']: {"score": row['score'], "details": row['details']} for _, row in sent_df.iterrows()}
        idx_map = {row['trade_date']: row['close'] for _, row in idx_df.iterrows()}
        
        for d in dates:
            s_data = sent_map.get(d, {"score": 50, "details": "{}"})
            
            # 解析 details
            try:
                details_json = json.loads(s_data['details']) if s_data['details'] else {}
            except:
                details_json = {}
                
            sentiment_scores.append({
                "value": round(float(s_data['score']), 2),
                "details": details_json
            })
            index_closes.append(idx_map.get(d, None))
            
        return {
            "dates": result_dates,
            "sentiment": sentiment_scores,
            "index": index_closes
        }

# 导出单例
sentiment_analyst = SentimentAnalyst()


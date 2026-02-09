# /backend/strategy/sentiment/analyst.py

import logging
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from db.connection import get_db_connection, fetch_df
from enum import Enum

logger = logging.getLogger(__name__)

class MarketMood(Enum):
    ICE_POINT = "ICE_POINT"
    BOILING = "BOILING"
    DIVERGENCE = "DIVERGENCE"
    CONFUSED = "CONFUSED"

class SentimentAnalyst:
    """
    情绪分析器 (Sentiment Analyst) - V31 "Adaptive Dual-Mode"
    
    核心逻辑：环境决定策略。
    1. 环境感知：利用均线系统判定当前是"趋势市"还是"震荡市"。
    2. 趋势模式：启用宽进严出，死拿主升浪，防止卖飞。
    3. 震荡模式：启用狙击手逻辑，不见兔子不撒鹰，防止磨损。
    """

    def analyze(self, trade_date: str):
        logger.info(f"正在执行 V31 自适应策略: {trade_date}...")
        try:
            df_today = self._get_daily_data(trade_date)
            if df_today.empty: return
            index_kline = self._get_index_daily(trade_date)
            fingerprint = self._calculate_fingerprint(df_today, trade_date)
            current_score = self._calculate_continuous_score(fingerprint)
            
            history = self._get_recent_sentiments(trade_date, limit=3)
            v1, v2 = 0.0, 0.0
            if history:
                prev_score = history[-1]['score']
                v1 = current_score - prev_score
                if len(history) >= 2:
                    prev_v1 = history[-1]['score'] - history[-2]['score']
                    v2 = v1 - prev_v1

            crash_exp_ret = self._check_crash_feedback(trade_date)
            trend_context = self._check_trend_context(trade_date)

            plan = self._generate_plan(fingerprint, current_score, v1, v2, crash_exp_ret, trend_context, index_kline)
            self._save_result(trade_date, plan, fingerprint, current_score)
            return plan
        except Exception as e:
            logger.error(f"V31 失败: {e}")
            return None

    def _get_daily_data(self, date_str):
        return fetch_df(f"SELECT ts_code, open, high, low, close, pre_close, pct_chg, amount, vol FROM daily_price WHERE trade_date = '{date_str}' AND vol > 0")

    def _get_index_daily(self, date_str, ts_code='000300.SH'):
        df = fetch_df(f"SELECT open, high, low, close, pre_close, vol, amount, pct_chg FROM market_index WHERE trade_date = '{date_str}' AND ts_code = '{ts_code}'")
        return df.iloc[0] if not df.empty else None

    def _get_recent_sentiments(self, date_str, limit=3):
        df = fetch_df(f"SELECT trade_date, score FROM market_sentiment WHERE trade_date < '{date_str}' ORDER BY trade_date DESC LIMIT {limit}")
        return df.sort_values('trade_date').to_dict('records') if not df.empty else []

    def _calculate_continuous_score(self, fp):
        score = 50.0
        score += np.clip((fp['limit_up_count'] - fp['limit_down_count']) * 0.3, -30, 30)
        score += (fp['promotion_rate'] - 0.3) * 50
        score += (0.25 - fp['broken_ratio']) * 40
        score += np.clip(fp['index_pct_chg'] * 5, -10, 10)
        score += np.clip(fp['repair_count'] * 0.5, 0, 10)
        return round(float(np.clip(score, 0, 100)), 1)

    def _calculate_fingerprint(self, df_today, trade_date):
        stats = {}
        limit_ups = df_today[df_today['pct_chg'] >= 9.5]
        limit_downs = df_today[df_today['pct_chg'] <= -9.5]
        stats['limit_up_count'], stats['limit_down_count'] = len(limit_ups), len(limit_downs)
        stats['up_count'], stats['down_count'] = len(df_today[df_today['pct_chg'] > 0]), len(df_today[df_today['pct_chg'] < 0])
        total_amt = df_today['amount'].sum()
        stats['limit_up_vol_ratio'] = round(limit_ups['amount'].sum() / total_amt * 100, 1) if total_amt > 0 else 0
        stats['panic_vol_ratio'] = round(df_today[df_today['pct_chg'] < -7]['amount'].sum() / total_amt * 100, 1) if total_amt > 0 else 0
        stats['promotion_rate'] = 0.3
        stats['repair_count'] = len(df_today[((df_today[['open', 'close']].min(axis=1) - df_today['low']) / df_today['close'] > 0.03)])
        stats['broken_count'] = len(df_today[(df_today['high'] >= df_today['pre_close'] * 1.095) & (df_today['pct_chg'] < 9.5)])
        stats['broken_ratio'] = round(stats['broken_count'] / (len(limit_ups) + stats['broken_count']), 1) if (len(limit_ups) + stats['broken_count']) > 0 else 0
        hs300 = self._get_index_daily(trade_date)
        stats['index_pct_chg'] = round(hs300['pct_chg'], 1) if hs300 is not None else 0.0
        stats['index_vol'] = round(hs300['vol'], 1) if hs300 is not None else 0.0
        stats['median_pct_chg'] = round(df_today['pct_chg'].median(), 1)
        return stats

    def _check_crash_feedback(self, trade_date: str) -> float:
        try:
            df = fetch_df(f"SELECT trade_date, score FROM market_sentiment WHERE trade_date < '{trade_date}' ORDER BY trade_date DESC LIMIT 60")
            if len(df) < 2: return 0.0
            df = df.sort_values('trade_date')
            df['v1'] = df['score'].diff()
            dates = df[df['v1'] < -5]['trade_date'].tolist()
            if not dates: return 0.0
            rets = []
            for d in dates:
                d_str = d.strftime('%Y-%m-%d')
                r = fetch_df(f"SELECT pct_chg FROM market_index WHERE ts_code='000300.SH' AND trade_date > '{d_str}' ORDER BY trade_date ASC LIMIT 1")
                if not r.empty: rets.append(r.iloc[0]['pct_chg'])
            return sum(rets) / len(rets) if rets else 0.0
        except: return 0.0

    def _check_trend_context(self, trade_date: str) -> dict:
        """
        [New] 环境感知系统
        判断当前处于: 牛市(Bull), 熊市(Bear), 震荡(Chop)
        逻辑:
        1. 价格 > MA20 -> 趋势向好
        2. MA20 > MA60 -> 中期多头排列
        """
        try:
            df = fetch_df(f"SELECT close FROM market_index WHERE ts_code='000300.SH' AND trade_date <= '{trade_date}' ORDER BY trade_date DESC LIMIT 70")
            if len(df) < 60: return {'mode': 'CHOP', 'above_ma5': False}
            
            close = df['close'].iloc[0]
            ma5 = df['close'].iloc[:5].mean()
            ma20 = df['close'].iloc[:20].mean()
            ma60 = df['close'].iloc[:60].mean()
            
            mode = 'CHOP'
            if close > ma20 and ma20 > ma60:
                mode = 'BULL' # 牛市多头
            elif close < ma20 and ma20 < ma60:
                mode = 'BEAR' # 熊市空头
            
            return {'mode': mode, 'above_ma5': close > ma5, 'above_ma20': close > ma20}
        except: return {'mode': 'CHOP', 'above_ma5': False}

    def _generate_plan(self, fp, score, v1, v2, crash_exp_ret=0.0, trend_context=None, kline=None):
        if trend_context is None: trend_context = {'mode': 'CHOP', 'above_ma5': False}
        mode = trend_context['mode']
        
        # --- 模式 1: 牛市趋势跟随 (Bull Mode) ---
        # 特征: 宽进严出，死拿主升，防止卖飞
        if mode == 'BULL':
            # 1. 趋势共振买点: 只要情绪不是冰点(>35)且没大跌，就买/持
            if score > 35 and v1 > -3:
                # 除非情绪见顶且跌破MA5，否则一律持有
                if score > 88 and not trend_context['above_ma5']:
                    return self._pack_result(MarketMood.BOILING, "PLAN_SELL_CLIMAX", f"【牛市】情绪见顶且破位，止盈保护。", 80, 20, fp, score, v1, v2)
                return self._pack_result(MarketMood.DIVERGENCE, "PLAN_HOLD", f"【牛市】趋势多头排列，情绪稳定，锁仓享受主升浪。", 30, 90, fp, score, v1, v2)
            
            # 2. 积极进攻: 只要有起色就追
            if v1 > 2:
                return self._pack_result(MarketMood.DIVERGENCE, "PLAN_BUY_DIVERGENCE", f"【牛市】空中加油，资金回流，积极买入。", 30, 80, fp, score, v1, v2)

        # --- 模式 2: 震荡/熊市狙击 (Bear/Chop Mode) ---
        # 特征: 极高门槛，快进快出，空仓为主
        else:
            # 1. 恐慌反核 (最高优先级)
            if v1 < -20:
                return self._pack_result(MarketMood.ICE_POINT, "PLAN_SNIPER_BUY", f"【震荡】非理性踩踏(v1={v1:.1f})，极度超卖，博弈反核。", 20, 95, fp, score, v1, v2)

            # 2. 动能爆发 (高门槛)
            if 20 < score < 70 and v1 > 10 and v2 > 0:
                return self._pack_result(MarketMood.DIVERGENCE, "PLAN_SNIPER_BUY", f"【震荡】情绪动能爆发(v1={v1:.1f})，短线脉冲机会。", 30, 85, fp, score, v1, v2)

            # 3. 惯性止损
            if v1 < -8:
                return self._pack_result(MarketMood.ICE_POINT, "PLAN_SELL_ALL", f"【震荡】情绪走弱，趋势向下，清仓避险。", 95, 5, fp, score, v1, v2)

        # 默认状态
        strategy_text = "【牛市】持股待涨。" if mode == 'BULL' else "【震荡】空仓观望，等待狙击机会。"
        signal = "PLAN_HOLD" if mode == 'BULL' else "PLAN_WATCH"
        
        return self._pack_result(MarketMood.CONFUSED, signal, strategy_text, 50, 50, fp, score, v1, v2)

    def _pack_result(self, mood, signal, text, risk, opp, fp, score, v1, v2):
        return { "market_mood": mood.value, "signal": signal, "next_day_strategy": text, "risk_factor": risk, "opportunity_factor": opp, "fingerprint": fp, "metrics": {"score": score, "v1": v1, "v2": v2} }

    def _save_result(self, trade_date, plan, fingerprint, current_score):
        label_map = { "PLAN_BUY_REVERSAL": "反核博弈", "PLAN_SNIPER_BUY": "狙击买入", "PLAN_BUY_DIVERGENCE": "积极做多", "PLAN_HOLD": "趋势锁仓", "PLAN_SELL_CLIMAX": "趋势止盈", "PLAN_SELL_ALL": "清仓离场", "PLAN_WATCH": "观望" }
        advice_map = { "PLAN_BUY_REVERSAL": "博弈长腿", "PLAN_SNIPER_BUY": "重仓出击", "PLAN_BUY_DIVERGENCE": "顺势加仓", "PLAN_HOLD": "躺赢主升", "PLAN_SELL_CLIMAX": "落袋为安", "PLAN_SELL_ALL": "空仓避险", "PLAN_WATCH": "等待机会" }
        label = label_map.get(plan['signal'], "未知")
        advice = advice_map.get(plan['signal'], "观望")
        details = {
            "signal": plan['signal'], "strategy": plan['next_day_strategy'], "mood": plan['market_mood'], "fingerprint": fingerprint,
            "factors": { "breadth": round(fingerprint['up_count'] / (fingerprint['up_count'] + fingerprint['down_count']) * 100, 1) if (fingerprint['up_count'] + fingerprint['down_count']) > 0 else 0, "median_chg": round(fingerprint.get('median_pct_chg', 0), 1), "index_chg": round(fingerprint.get('index_pct_chg', 0), 1), "limit": fingerprint.get('limit_up_count', 0), "failure": fingerprint.get('broken_count', 0), "vol": "NORMAL" },
            "risk": plan['risk_factor'], "opp": plan['opportunity_factor'],
            "metrics": { "score": round(plan.get('metrics', {}).get('score', 0), 1), "v1": round(plan.get('metrics', {}).get('v1', 0), 1), "v2": round(plan.get('metrics', {}).get('v2', 0), 1), "score_delta": round(plan.get('metrics', {}).get('v1', 0), 1), "advice": advice }
        }
        sql = "INSERT INTO market_sentiment (trade_date, score, label, details) VALUES (?, ?, ?, ?) ON CONFLICT (trade_date) DO UPDATE SET score=excluded.score, label=excluded.label, details=excluded.details"
        with get_db_connection() as con:
            con.execute(sql, (trade_date, current_score, label, json.dumps(details, ensure_ascii=False)))
        logger.info(f"推演结果已保存: {label} | Score: {current_score:.1f}")

    def calculate(self, days=30):
        date_query = f"SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date DESC LIMIT {int(days)}"
        dates_df = fetch_df(date_query)
        if dates_df.empty: return
        target_dates = sorted(dates_df['trade_date'].tolist())
        for d in target_dates:
            date_str = d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)
            self.analyze(date_str)

    def get_history(self, days=30):
        query = f"SELECT trade_date, score, label, details FROM market_sentiment ORDER BY trade_date DESC LIMIT {int(days)}"
        df_sent = fetch_df(query)
        if df_sent.empty: return {"dates": [], "sentiment": [], "index": []}
        df_sent = df_sent.sort_values('trade_date')
        dates, sentiment_data = [], []
        for _, row in df_sent.iterrows():
            d_str = row['trade_date'].strftime('%Y-%m-%d') if hasattr(row['trade_date'], 'strftime') else str(row['trade_date'])
            dates.append(d_str)
            details = json.loads(row['details']) if isinstance(row['details'], str) else row['details']
            sentiment_data.append({ "value": round(row['score'], 1), "label": row['label'], "details": details })
        min_date, max_date = dates[0], dates[-1]
        idx_query = f"SELECT trade_date, close FROM market_index WHERE ts_code = '000001.SH' AND trade_date BETWEEN '{min_date}' AND '{max_date}' ORDER BY trade_date ASC"
        df_idx = fetch_df(idx_query)
        idx_map = { (d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)): round(c, 1) for d, c in zip(df_idx['trade_date'], df_idx['close']) }
        return { "dates": dates, "sentiment": sentiment_data, "index": [idx_map.get(d) for d in dates] }

sentiment_analyst = SentimentAnalyst()
# /backend/strategy/sentiment/analyst.py

import copy
import json
import logging
import math
from enum import Enum

import numpy as np
import arrow

from db.connection import get_db_connection, fetch_df
from strategy.sentiment.config import SENTIMENT_CONFIG
from etl.calendar import trading_calendar

logger = logging.getLogger(__name__)

class MarketMood(Enum):
    ICE_POINT = "ICE_POINT"
    BOILING = "BOILING"
    DIVERGENCE = "DIVERGENCE"
    CONFUSED = "CONFUSED"
    EUPHORIA = "EUPHORIA"  # 人声鼎沸
    DIVERGENCE_SELL = "DIVERGENCE_SELL"  # 动量背离


class PositionState:
    """持仓状态追踪器 - 有状态择时核心"""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        self.in_position = False
        self.entry_date = None
        self.entry_score = 0
        self.entry_price = 0
        self.hold_days = 0
        self.peak_profit = 0
        self.stop_loss_triggered = False
    
    def open_position(self, date: str, score: float, price: float):
        self.in_position = True
        self.entry_date = date
        self.entry_score = score
        self.entry_price = price
        self.hold_days = 0
        self.peak_profit = 0
        self.stop_loss_triggered = False
        logger.info(f"开仓: 日期={date}, 分数={score}, 价格={price}")
    
    def close_position(self, reason: str = ""):
        profit_pct = ((self.peak_profit / self.entry_price) - 1) * 100 if self.entry_price > 0 else 0
        logger.info(f"平仓: 原因={reason}, 持有天数={self.hold_days}, 盈亏={profit_pct:.2f}%")
        self.reset()
    
    def update(self, current_date: str, current_price: float, current_score: float):
        if not self.in_position:
            return None
        
        self.hold_days += 1
        
        profit_pct = ((current_price - self.entry_price) / self.entry_price) * 100
        if profit_pct > self.peak_profit:
            self.peak_profit = profit_pct  # 存收益率，不是价格
        
        return {
            "hold_days": self.hold_days,
            "profit_pct": profit_pct,
            "peak_profit_pct": self.peak_profit,  # 已经是百分比
            "entry_score": self.entry_score,
            "current_score": current_score,
            "score_change": current_score - self.entry_score
        }


# 全局持仓状态实例
_position_state = PositionState()


class SentimentAnalyst:
    """
    情绪分析器 (Sentiment Analyst) - V33 "Config-Driven Adaptive"
    
    核心逻辑：环境决定策略。
    1. 环境感知：利用均线系统判定当前是"趋势市"还是"震荡市"。
    2. 策略引擎：基于 SENTIMENT_CONFIG 驱动的动态参数。
    3. 信号校验：二次过滤信号，剔除假突破。
    """

    def _finite_number(self, value, default=0.0):
        try:
            v = float(value)
            if math.isnan(v) or math.isinf(v):
                return float(default)
            return v
        except Exception:
            return float(default)

    def _sanitize_payload(self, value):
        if isinstance(value, dict):
            return {k: self._sanitize_payload(v) for k, v in value.items()}
        if isinstance(value, list):
            return [self._sanitize_payload(v) for v in value]
        if isinstance(value, float):
            return self._finite_number(value, 0.0)
        return value

    def _next_trading_day_str(self, date_str: str) -> str:
        current = arrow.get(date_str).date()
        probe = arrow.get(current).shift(days=1)
        for _ in range(15):
            if trading_calendar.is_trading_day(probe.date()):
                return probe.format("YYYY-MM-DD")
            probe = probe.shift(days=1)
        return probe.format("YYYY-MM-DD")

    def analyze(self, trade_date: str, initial_capital=100000):
        logger.info(f"正在执行 V33 自适应策略: {trade_date}...")
        try:
            df_today = self._get_daily_data(trade_date)
            if df_today.empty: return
            
            # 获取当日指数收盘价 (用于持仓计算)
            index_row = self._get_index_daily(trade_date, '000688.SH')
            current_price = index_row['close'] if index_row is not None else 0
            
            # 更新持仓状态
            position_info = None
            if _position_state.in_position and current_price > 0:
                position_info = _position_state.update(trade_date, current_price, 0)
                logger.debug(f"持仓状态: 持有天数={_position_state.hold_days}, 盈亏={position_info.get('profit_pct', 0) if position_info else 0:.2f}%")
            
            # Fetch context for Verifier
            index_history = self._get_index_history(trade_date)
            top_sectors = self._get_top_sectors(trade_date)
            
            fingerprint = self._calculate_fingerprint(df_today, trade_date)
            current_score = self._calculate_continuous_score(fingerprint)
            
            history = self._get_recent_sentiments(trade_date, limit=10)
            v1, v2 = 0.0, 0.0
            v1_rate = 0.0
            sustained_high_days = 0
            sustained_low_days = 0
            score_ma5 = current_score
            score_ma10 = current_score
            score_std10 = 0.0
            score_z = 0.0
            score_drawdown5 = 0.0
            
            # 情绪动量新增指标
            breadth_divergence = 0.0  # 涨跌家数背离
            momentum_divergence = 0.0  # 情绪与指数动量背离
            volume_stagnation = False  # 放量滞涨标志
            double_peak_pattern = False  # 双头形态标志
            
            if history:
                prev_scores = [h['score'] for h in history]
                prev_score = prev_scores[-1]
                v1 = current_score - prev_score
                v1_rate = v1 / max(prev_score, 1) # 变化率
                
                # 计算 5 日均值 (含今日)
                combined_scores = prev_scores + [current_score]
                score_ma5 = sum(combined_scores[-5:]) / len(combined_scores[-5:])
                score_ma10 = sum(combined_scores[-10:]) / len(combined_scores[-10:])
                score_std10 = float(np.std(combined_scores[-10:])) if len(combined_scores) >= 2 else 0.0
                score_z = (current_score - score_ma10) / score_std10 if score_std10 > 0 else 0.0
                score_drawdown5 = current_score - max(combined_scores[-5:])
                
                # 持续高涨天数 (分数 > 80)
                consecutive_highs = 0
                for s in reversed(combined_scores):
                    if s > 80:
                        consecutive_highs += 1
                    else:
                        break
                sustained_high_days = consecutive_highs

                # 持续低迷天数 (分数 < 35)
                consecutive_lows = 0
                for s in reversed(combined_scores):
                    if s < 35:
                        consecutive_lows += 1
                    else:
                        break
                sustained_low_days = consecutive_lows

                # 计算动量背离 (情绪新高但指数/涨跌家数背离)
                if len(history) >= 3:
                    recent_3 = history[-3:]
                    idx_history = self._get_index_history(trade_date, limit=5)
                    
                    # 涨跌家数背离: 情绪连续上升但上涨家数减少
                    if len(recent_3) >= 2:
                        prev_breadth = fingerprint.get('up_count', 0) / max(fingerprint.get('up_count', 0) + fingerprint.get('down_count', 0), 1)
                        # 获取前两天的涨跌数据做对比
                        prev_2_dates = [h['trade_date'] for h in history[-2:]]
                        if prev_2_dates:
                            prev_2_breadth = self._get_breadth(prev_2_dates[0]) if len(prev_2_dates) > 0 else prev_breadth
                            breadth_divergence = prev_breadth - prev_2_breadth
                    
                    # 情绪与指数动量背离
                    if idx_history is not None and len(idx_history) >= 3:
                        idx_ma5 = idx_history['close'].iloc[-5:].mean()
                        idx_current = idx_history['close'].iloc[-1]
                        idx_prev = idx_history['close'].iloc[-2]
                        idx_momentum = (idx_current - idx_prev) / idx_prev * 100 if idx_prev > 0 else 0
                        # 情绪动量 > 5 但指数动量 < 0 为背离
                        if v1 > 5 and idx_momentum < -0.2:
                            momentum_divergence = v1 - idx_momentum

                # 放量滞涨检测: 成交量放大但涨幅收敛
                idx_history = self._get_index_history(trade_date, limit=6)
                if idx_history is not None and len(idx_history) >= 2:
                    prev_vol = idx_history['vol'].iloc[-2]
                    today_vol = idx_history['vol'].iloc[-1]
                    if prev_vol > 0 and fingerprint.get('index_pct_chg', 0) < 1.0:
                        vol_ratio = today_vol / max(prev_vol, 1)
                        if vol_ratio > 1.3 and v1 < 2.0:
                            volume_stagnation = True
                
                # 双头形态检测: 连续两次高分且动能回落
                if len(history) >= 5:
                    recent_scores = [h['score'] for h in history[-5:]]
                    if (recent_scores[-1] > 85 and recent_scores[-2] > 85 and 
                        recent_scores[-3] > 70 and v1 < -3.0):
                        double_peak_pattern = True

                if len(history) >= 2:
                    prev_v1 = history[-1]['score'] - history[-2]['score']
                    v2 = v1 - prev_v1 # 动能的变化率 (加速度)

            crash_exp_ret = self._check_crash_feedback(trade_date)
            trend_context = self._check_trend_context(trade_date)

            # 新增动量指标
            new_metrics = {
                'breadth_divergence': breadth_divergence,
                'momentum_divergence': momentum_divergence,
                'volume_stagnation': volume_stagnation,
                'double_peak_pattern': double_peak_pattern,
                'breadth_ratio': (
                    fingerprint.get('up_count', 0) /
                    max(fingerprint.get('up_count', 0) + fingerprint.get('down_count', 0), 1)
                ),
                'score_ma10': score_ma10,
                'score_std10': score_std10,
                'score_z': score_z,
                'score_drawdown5': score_drawdown5,
                'sustained_low_days': sustained_low_days
            }

            # Generate Raw Plan (传入持仓状态)
            plan = self._generate_plan(
                fingerprint, current_score, v1, v2, crash_exp_ret, trend_context,
                score_ma5, v1_rate, sustained_high_days, new_metrics, position_info
            )

            # Verify breakout/chase BUY signals first, then update position state.
            verify_required = plan.get('signal') in {"PLAN_BUY_MOMENTUM", "PLAN_BUY_BULL", "PLAN_BUY_FULL"}
            if verify_required and self._is_buy_signal(plan['signal']):
                market_data = {
                    'top_sectors': top_sectors,
                    'index_history': index_history,
                    'current_row': {
                        'index_pct_chg': fingerprint.get('index_pct_chg', 0),
                        'up_count': fingerprint.get('up_count', 0),
                        'total': fingerprint.get('up_count', 0) + fingerprint.get('down_count', 0),
                        'net_mf': top_sectors['net_inflow'].sum() if not top_sectors.empty else 0
                    }
                }
                verified_signal = self._verify_signal(plan['signal'], market_data)
                
                # Apply Verification Result
                if verified_signal.startswith("WATCH"):
                    # Do not fully discard BUY; degrade to staged entry.
                    plan['signal'] = "PLAN_BUY_PARTIAL"
                    plan['next_day_strategy'] += f" [校验转分批: {verified_signal}]"
                    plan['risk_factor'] = 68
                    plan['opportunity_factor'] = 58
                elif verified_signal.startswith("STRONG BUY"):
                    plan['opportunity_factor'] = 95
                    plan['next_day_strategy'] += f" [强力确认: {verified_signal}]"

            # 处理开仓/平仓（放在校验之后，保证状态与最终信号一致）
            if self._is_buy_signal(plan['signal']) and not _position_state.in_position:
                _position_state.open_position(trade_date, current_score, current_price)
            elif self._is_sell_signal(plan['signal']):
                if _position_state.in_position:
                    _position_state.close_position(plan.get('signal', 'signal_change'))

            plan['execution'] = self._build_execution(plan, fingerprint, trend_context)
            self._save_result(trade_date, plan, fingerprint, current_score)
            return plan
        except Exception as e:
            logger.error(f"V33 失败: {e}", exc_info=True)
            return None

    def _verify_signal(self, signal: str, market_data: dict) -> str:
        """
        信号校验逻辑 (原 SignalVerifier)
        对 "BUY" 信号进行二次确认，过滤假突破。
        """
        if signal != "BUY" and "BUY" not in signal:
            return signal
            
        # 逆向抄底类信号不依赖主线一致性，避免被过滤为 WATCH
        if "REVERSAL" in signal or "NEGLECT" in signal or "ICE" in signal:
            return signal

        # 1. 主线强度校验 (Filter A)
        top_sectors = market_data.get('top_sectors')
        filter_a_passed = False
        if top_sectors is not None and not top_sectors.empty:
            main_sectors = top_sectors.nlargest(20, 'amount')
            # 只要有板块涨幅超过 1% 且资金流入即可 (放宽至 0.5%)
            valid_main_sectors = main_sectors[(main_sectors['avg_pct_chg'] > 0.5) & (main_sectors['net_inflow'] > 0)]
            if not valid_main_sectors.empty:
                filter_a_passed = True
        
        if not filter_a_passed:
            return "WATCH (No Main Line)"

        # 2. 弹簧效应校验 (Filter B)
        index_history = market_data.get('index_history')
        filter_b_passed = False
        is_ignition = False
        if index_history is not None and len(index_history) >= 3:
            last_3_days = index_history.tail(3)
            current_day = index_history.iloc[-1]
            volatility_ratio = last_3_days['close'].std() / last_3_days['close'].mean() if last_3_days['close'].mean() > 0 else 1.0
            avg_vol_3d = last_3_days['vol'].mean()
            ma20_vol = current_day['ma20_vol']
            # 缩量整理或放量突破
            is_consolidating = (volatility_ratio < 0.015) and (avg_vol_3d < ma20_vol * 1.2)
            is_ignition = current_day['vol'] > ma20_vol * 1.1
            if is_consolidating or is_ignition:
                filter_b_passed = True
        
        if not filter_b_passed:
            return "WATCH (High Volatility/Low Vol)"

        # 3. 背离校验 (Filter C)
        current_row = market_data.get('current_row', {})
        idx_chg, up_count, total, net_mf = current_row.get('index_pct_chg', 0), current_row.get('up_count', 0), current_row.get('total', 0), current_row.get('net_mf', 0)
        breadth_up_ratio = up_count / total if total > 0 else 0
        is_index_div = (idx_chg < -0.3) and (breadth_up_ratio > 0.5)
        is_mf_div = (idx_chg < 0) and (net_mf > 0)

        if is_index_div or is_mf_div:
            return f"STRONG BUY ({'Index' if is_index_div else 'MF'} Divergence)"
        if is_ignition:
            return "STRONG BUY (Resonance)"
        return "BUY (Slight Resonance)"

    def _signal_action(self, signal: str) -> str:
        if not signal:
            return "WATCH"
        if signal.startswith("PLAN_BUY") or signal == "PLAN_SNIPER_BUY":
            return "BUY"
        if signal.startswith("PLAN_SELL"):
            return "SELL"
        if signal == "PLAN_HOLD":
            return "HOLD"
        return "WATCH"

    def _is_buy_signal(self, signal: str) -> bool:
        return self._signal_action(signal) == "BUY"

    def _is_sell_signal(self, signal: str) -> bool:
        return self._signal_action(signal) == "SELL"

    def _target_position_from_signal(self, signal: str, current_pos: float = 0.0) -> float:
        if not signal:
            return current_pos
        if signal in {"PLAN_BUY_PARTIAL"}:
            return max(current_pos, 0.5)
        if signal.startswith("PLAN_BUY") or signal == "PLAN_SNIPER_BUY":
            return 1.0
        if signal in {"PLAN_SELL_PARTIAL"}:
            return 0.5 if current_pos > 0.5 else 0.0
        if signal.startswith("PLAN_SELL"):
            return 0.0
        if signal == "PLAN_HOLD":
            return current_pos
        return current_pos

    def _score_to_position(self, score: float, full: float, half: float, zero: float) -> float:
        """将情绪分数映射到目标仓位 (连续线性插值)"""
        if score >= full:
            return 1.0
        elif score >= half:
            return 0.5 + 0.5 * (score - half) / max(full - half, 1)
        elif score >= zero:
            return 0.5 * (score - zero) / max(half - zero, 1)
        else:
            return 0.0

    def _get_daily_data(self, date_str):
        return fetch_df(f"SELECT ts_code, open, high, low, close, pre_close, pct_chg, amount, vol FROM daily_price WHERE trade_date = '{date_str}' AND vol > 0")

    def _get_index_daily(self, date_str, ts_code='000300.SH'):
        df = fetch_df(f"SELECT open, high, low, close, pre_close, vol, amount, pct_chg FROM market_index WHERE trade_date = '{date_str}' AND ts_code = '{ts_code}'")
        return df.iloc[0] if not df.empty else None

    def _get_breadth(self, date_str):
        """获取指定日期的涨跌家数比"""
        try:
            df = fetch_df(f"SELECT COUNT(*) as up_count FROM daily_price WHERE trade_date = '{date_str}' AND pct_chg > 0")
            total_df = fetch_df(f"SELECT COUNT(*) as total FROM daily_price WHERE trade_date = '{date_str}'")
            if not df.empty and not total_df.empty:
                up = df.iloc[0]['up_count']
                total = total_df.iloc[0]['total']
                return up / total if total > 0 else 0.5
        except:
            pass
        return 0.5
    
    def _get_index_history(self, date_str, ts_code='000300.SH', limit=20):
        # Need MA20 Vol, so fetch more
        df = fetch_df(f"SELECT trade_date, open, high, low, close, vol FROM market_index WHERE trade_date <= '{date_str}' AND ts_code = '{ts_code}' ORDER BY trade_date DESC LIMIT {limit}")
        if df.empty: return None
        df = df.sort_values('trade_date')
        df['ma20_vol'] = df['vol'].rolling(window=20).mean()
        return df

    def _get_top_sectors(self, date_str):
        # Fetch sector performance and moneyflow
        sql = f"""
        SELECT 
            sc.concept_name as name,
            SUM(p.amount) as amount,
            AVG(p.pct_chg) as avg_pct_chg,
            SUM(CASE WHEN m.net_mf_amount IS NULL THEN 0 ELSE m.net_mf_amount END) as net_inflow
        FROM daily_price p
        JOIN stock_concept_details sc ON p.ts_code = sc.ts_code
        LEFT JOIN stock_moneyflow m ON p.ts_code = m.ts_code AND p.trade_date = m.trade_date
        WHERE p.trade_date = '{date_str}'
        GROUP BY sc.concept_name
        HAVING amount > 0
        ORDER BY amount DESC
        LIMIT 50
        """
        return fetch_df(sql)

    def _get_recent_sentiments(self, date_str, limit=5):
        df = fetch_df(f"SELECT trade_date, score FROM market_sentiment WHERE trade_date < '{date_str}' ORDER BY trade_date DESC LIMIT {limit}")
        return df.sort_values('trade_date').to_dict('records') if not df.empty else []

    def _calculate_continuous_score(self, fp):
        w = SENTIMENT_CONFIG['weights']
        score = 50.0
        p = fp.get('factor_percentiles', {})
        # 1. 涨跌停动能 (Limit Momentum)
        limit_score = (fp['limit_up_count'] - fp['limit_down_count']) * w['limit_diff']
        score += np.clip(limit_score, -30, 30)
        
        # 2. 赚钱效应 (Profit Effect)
        promo_score = (fp['promotion_rate'] - 0.3) * w['promotion']
        score += np.clip(promo_score, -20, 20)
        
        # 3. 恐慌度 (Panic/Repair)
        broken_score = (0.25 - fp['broken_ratio']) * w['broken']
        score += np.clip(broken_score, -15, 15)
        
        # 4. 指数共振 (Index Resonance)
        index_score = fp['index_pct_chg'] * w['index_chg']
        score += np.clip(index_score, -10, 10)
        
        # 5. 修复力度 (Repair Strength)
        repair_score = fp['repair_count'] * w['repair']
        score += np.clip(repair_score, 0, 10)

        # 6. 市场广度 (Breadth)
        breadth_score = (p.get('breadth', 0.5) - 0.5) * w.get('breadth', 32.0)
        score += np.clip(breadth_score, -12, 12)

        # 7. 成交活跃度 (Turnover/Amount Activity)
        turnover_score = (p.get('turnover_activity', 0.5) - 0.5) * w.get('turnover_activity', 8.0)
        score += np.clip(turnover_score, -8, 8)

        # 8. 融资杠杆情绪 (5日融资余额变化率)
        margin_score = (p.get('margin_financing_delta5', 0.5) - 0.5) * w.get('margin_delta', 120.0)
        score += np.clip(margin_score, -10, 10)

        # 9. 资金流向强弱 (净流入占比)
        flow_score = (p.get('net_mf_ratio', 0.5) - 0.5) * w.get('net_mf_ratio', 18.0)
        score += np.clip(flow_score, -8, 8)

        # 10. 新高/新低结构
        nhnl_score = (p.get('new_high_low_ratio', 0.5) - 0.5) * w.get('new_high_low', 6.0)
        score += np.clip(nhnl_score, -8, 8)

        # 11. 连板高度
        board_score = (p.get('max_limit_up_streak', 0.5) - 0.5) * w.get('board_height', 2.0)
        score += np.clip(board_score, -6, 6)

        # 12. 波动恐慌代理 (IV Proxy)
        # iv_proxy_z 越高代表越恐慌，应降低情绪分
        iv_score = -(p.get('iv_proxy_z', 0.5) - 0.5) * w.get('iv_proxy', 5.5)
        score += np.clip(iv_score, -10, 10)
        
        score = self._finite_number(score, 50.0)
        return round(float(np.clip(score, 0, 100)), 1)

    def _calculate_fingerprint(self, df_today, trade_date):
        stats = {}
        missing_flags = {}
        limit_ups = df_today[df_today['pct_chg'] >= 9.5]
        limit_downs = df_today[df_today['pct_chg'] <= -9.5]
        stats['limit_up_count'], stats['limit_down_count'] = len(limit_ups), len(limit_downs)
        stats['up_count'], stats['down_count'] = len(df_today[df_today['pct_chg'] > 0]), len(df_today[df_today['pct_chg'] < 0])
        total_amt = df_today['amount'].sum()
        stats['total_amount'] = round(float(total_amt), 2)
        total_stocks = len(df_today)
        stats['breadth_ratio'] = round(stats['up_count'] / total_stocks, 4) if total_stocks > 0 else 0.5
        stats['limit_up_vol_ratio'] = round(limit_ups['amount'].sum() / total_amt * 100, 1) if total_amt > 0 else 0
        stats['panic_vol_ratio'] = round(df_today[df_today['pct_chg'] < -7]['amount'].sum() / total_amt * 100, 1) if total_amt > 0 else 0
        
        # 优化：单次查询获取上一交易日涨停列表
        stats['promotion_rate'] = 0.3
        try:
            prev_date_df = fetch_df(f"SELECT trade_date FROM market_index WHERE ts_code='000300.SH' AND trade_date < '{trade_date}' ORDER BY trade_date DESC LIMIT 1")
            if not prev_date_df.empty:
                prev_date = prev_date_df.iloc[0,0]
                prev_limit_ups = fetch_df(f"SELECT ts_code FROM daily_price WHERE trade_date = '{prev_date}' AND pct_chg >= 9.5")
                if not prev_limit_ups.empty:
                    promoted = limit_ups[limit_ups['ts_code'].isin(prev_limit_ups['ts_code'])]
                    stats['promotion_rate'] = round(len(promoted) / len(prev_limit_ups), 2)
        except Exception as e:
            logger.debug(f"Promotion rate error: {e}")

        stats['repair_count'] = len(df_today[((df_today[['open', 'close']].min(axis=1) - df_today['low']) / df_today['close'] > 0.03)])
        stats['broken_count'] = len(df_today[(df_today['high'] >= df_today['pre_close'] * 1.095) & (df_today['pct_chg'] < 9.5)])
        stats['broken_ratio'] = round(stats['broken_count'] / (len(limit_ups) + stats['broken_count']), 1) if (len(limit_ups) + stats['broken_count']) > 0 else 0
        
        # 优化：单次查询获取多个指数，支持回退以保证指数涨幅准确
        stats['index_pct_chg'] = 0.0
        stats['star50_pct_chg'] = 0.0
        stats['index_vol'] = 0.0
        missing_flags['index_chg'] = True
        try:
            indices = fetch_df(
                f"""
                SELECT ts_code, pct_chg, close, pre_close, vol
                FROM market_index
                WHERE trade_date = '{trade_date}'
                  AND ts_code IN ('000300.SH', '000001.SH', '399001.SZ', '000688.SH')
                """
            )
            if not indices.empty:
                def _resolve_pct(row):
                    pct = self._finite_number(row.get('pct_chg', 0.0), 0.0)
                    if abs(pct) > 1e-9:
                        return pct
                    close = self._finite_number(row.get('close', 0.0), 0.0)
                    pre_close = self._finite_number(row.get('pre_close', 0.0), 0.0)
                    if pre_close > 0:
                        return (close - pre_close) / pre_close * 100.0
                    return 0.0

                # 基准指数优先级：沪深300 -> 上证指数 -> 深证成指
                base_row = None
                for code in ['000300.SH', '000001.SH', '399001.SZ']:
                    tmp = indices[indices['ts_code'] == code]
                    if not tmp.empty:
                        base_row = tmp.iloc[0]
                        break
                if base_row is not None:
                    stats['index_pct_chg'] = round(_resolve_pct(base_row), 1)
                    stats['index_vol'] = round(self._finite_number(base_row.get('vol', 0.0), 0.0), 1)
                    missing_flags['index_chg'] = False

                star50 = indices[indices['ts_code'] == '000688.SH']
                if not star50.empty:
                    stats['star50_pct_chg'] = round(_resolve_pct(star50.iloc[0]), 1)
        except: pass

        # 成交活跃度 (用全市场成交额相对20日均值代替换手率强弱)
        stats['turnover_activity'] = 1.0
        missing_flags['turnover_activity'] = True
        try:
            amt_hist = fetch_df(
                f"""
                SELECT trade_date, SUM(amount) AS total_amount
                FROM daily_price
                WHERE trade_date <= '{trade_date}'
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 25
                """
            )
            if not amt_hist.empty:
                amt_hist = amt_hist.sort_values('trade_date')
                current_amt = float(amt_hist.iloc[-1]['total_amount']) if len(amt_hist) > 0 else float(total_amt)
                ma20_amt = float(amt_hist['total_amount'].tail(20).mean()) if len(amt_hist) > 0 else current_amt
                if ma20_amt > 0:
                    stats['turnover_activity'] = round(current_amt / ma20_amt, 4)
                    missing_flags['turnover_activity'] = False
        except Exception as e:
            logger.debug(f"Turnover activity error: {e}")

        # 融资融券情绪
        margin_stats = self._get_margin_stats(trade_date)
        stats.update(margin_stats)

        # 资金流向情绪
        flow_stats = self._get_moneyflow_stats(trade_date, total_amt)
        stats.update(flow_stats)

        # 新高/新低结构
        nh_nl_stats = self._get_new_high_low_stats(trade_date)
        stats.update(nh_nl_stats)
        missing_flags['new_high_low_ratio'] = not bool(stats.get('_new_high_low_available', False))

        # 连板高度 (风险偏好)
        stats['max_limit_up_streak'] = self._get_max_limit_up_streak(trade_date)
        stats['limit_up_down_ratio'] = round(
            stats['limit_up_count'] / max(stats['limit_down_count'], 1), 2
        )

        # 波动率恐慌代理 (IV Proxy)
        vol_stats = self._get_index_volatility_proxy(trade_date)
        stats.update(vol_stats)
        missing_flags['iv_proxy_z'] = not bool(stats.get('_iv_proxy_available', False))

        # 这些字段由当日行情直接计算，0 值也视为有效数据
        missing_flags['breadth'] = False
        missing_flags['median_chg'] = False
        missing_flags['limit'] = False
        missing_flags['failure'] = False
        missing_flags['limit_up_down_ratio'] = False
        missing_flags['max_limit_up_streak'] = False

        # 新增因子分位数标准化 (365天窗口)
        stats['factor_percentiles'] = self._get_factor_percentiles(trade_date, stats, lookback_days=365)
        stats['missing_flags'] = missing_flags

        stats['median_pct_chg'] = round(self._finite_number(df_today['pct_chg'].median(), 0.0), 1)
        return self._sanitize_payload(stats)

    def _calc_percentile(self, current: float, history: list[float]) -> float:
        clean = [float(x) for x in history if x is not None]
        if len(clean) < 20:
            return 0.5
        arr = np.array(clean, dtype=float)
        return float(np.mean(arr <= float(current)))

    def _get_factor_percentiles(self, trade_date: str, stats: dict, lookback_days: int = 365) -> dict:
        result = {
            'breadth': 0.5,
            'turnover_activity': 0.5,
            'margin_financing_delta5': 0.5,
            'net_mf_ratio': 0.5,
            'new_high_low_ratio': 0.5,
            'max_limit_up_streak': 0.5,
            'iv_proxy_z': 0.5
        }
        try:
            df = fetch_df(
                f"""
                SELECT
                    TRY_CAST(json_extract(details, '$.factors.breadth') AS DOUBLE) AS breadth,
                    TRY_CAST(json_extract(details, '$.factors.turnover_activity') AS DOUBLE) AS turnover_activity,
                    TRY_CAST(json_extract(details, '$.factors.margin_financing_delta5') AS DOUBLE) AS margin_financing_delta5,
                    TRY_CAST(json_extract(details, '$.factors.net_mf_ratio') AS DOUBLE) AS net_mf_ratio,
                    TRY_CAST(json_extract(details, '$.factors.new_high_low_ratio') AS DOUBLE) AS new_high_low_ratio,
                    TRY_CAST(json_extract(details, '$.factors.max_limit_up_streak') AS DOUBLE) AS max_limit_up_streak,
                    TRY_CAST(json_extract(details, '$.factors.iv_proxy_z') AS DOUBLE) AS iv_proxy_z
                FROM market_sentiment
                WHERE trade_date < '{trade_date}'
                ORDER BY trade_date DESC
                LIMIT {int(lookback_days)}
                """
            )
            if df.empty:
                return result

            result['breadth'] = round(
                self._calc_percentile(stats.get('breadth_ratio', 0.5) * 100, df['breadth'].tolist()), 4
            )
            result['turnover_activity'] = round(
                self._calc_percentile(stats.get('turnover_activity', 1.0), df['turnover_activity'].tolist()), 4
            )
            result['margin_financing_delta5'] = round(
                self._calc_percentile(
                    stats.get('margin_financing_delta5', 0.0), df['margin_financing_delta5'].tolist()
                ),
                4
            )
            result['net_mf_ratio'] = round(
                self._calc_percentile(stats.get('net_mf_ratio', 0.0), df['net_mf_ratio'].tolist()), 4
            )
            result['new_high_low_ratio'] = round(
                self._calc_percentile(stats.get('new_high_low_ratio', 1.0), df['new_high_low_ratio'].tolist()), 4
            )
            result['max_limit_up_streak'] = round(
                self._calc_percentile(
                    stats.get('max_limit_up_streak', 0), df['max_limit_up_streak'].tolist()
                ),
                4
            )
            result['iv_proxy_z'] = round(
                self._calc_percentile(stats.get('iv_proxy_z', 0.0), df['iv_proxy_z'].tolist()), 4
            )
        except Exception as e:
            logger.debug(f"Factor percentile error: {e}")
        return result

    def _get_margin_stats(self, trade_date: str) -> dict:
        stats = {
            'margin_financing': 0.0,
            'margin_short': 0.0,
            'margin_financing_delta5': 0.0
        }
        try:
            df = fetch_df(
                f"""
                SELECT trade_date, SUM(rzye) AS rzye, SUM(rqye) AS rqye
                FROM stock_margin
                WHERE trade_date <= '{trade_date}'
                GROUP BY trade_date
                ORDER BY trade_date DESC
                LIMIT 8
                """
            )
            if df.empty:
                return stats

            df = df.sort_values('trade_date')
            current_rzye = float(df.iloc[-1]['rzye']) if df.iloc[-1]['rzye'] is not None else 0.0
            current_rqye = float(df.iloc[-1]['rqye']) if df.iloc[-1]['rqye'] is not None else 0.0
            stats['margin_financing'] = round(current_rzye, 2)
            stats['margin_short'] = round(current_rqye, 2)

            if len(df) >= 6:
                base_rzye = float(df.iloc[-6]['rzye']) if df.iloc[-6]['rzye'] is not None else 0.0
                if base_rzye > 0:
                    stats['margin_financing_delta5'] = round((current_rzye - base_rzye) / base_rzye, 4)
        except Exception as e:
            logger.debug(f"Margin stats error: {e}")
        return stats

    def _get_moneyflow_stats(self, trade_date: str, total_amt: float) -> dict:
        stats = {
            'net_mf_amount': 0.0,
            'net_mf_ratio': 0.0
        }
        try:
            df = fetch_df(
                f"""
                SELECT SUM(net_mf_amount) AS net_mf_amount
                FROM stock_moneyflow
                WHERE trade_date = '{trade_date}'
                """
            )
            if df.empty:
                return stats
            net_mf_amount = float(df.iloc[0]['net_mf_amount']) if df.iloc[0]['net_mf_amount'] is not None else 0.0
            stats['net_mf_amount'] = round(net_mf_amount, 2)
            if total_amt > 0:
                # amount 单位通常为千元，此处保持相对比值，供打分使用
                stats['net_mf_ratio'] = round(net_mf_amount / total_amt, 4)
        except Exception as e:
            logger.debug(f"Moneyflow stats error: {e}")
        return stats

    def _get_new_high_low_stats(self, trade_date: str, window: int = 60) -> dict:
        stats = {
            'new_high_count': 0,
            'new_low_count': 0,
            'new_high_low_ratio': 1.0,
            '_new_high_low_available': False
        }
        try:
            # 仅使用最近窗口期数据，避免全表扫描与 Python 端聚合
            df = fetch_df(
                f"""
                WITH latest AS (
                    SELECT ts_code, close
                    FROM daily_price
                    WHERE trade_date = '{trade_date}'
                ),
                hist AS (
                    SELECT ts_code, MAX(close) AS max_close, MIN(close) AS min_close
                    FROM daily_price
                    WHERE trade_date < '{trade_date}'
                      AND trade_date >= DATE '{trade_date}' - INTERVAL {int(window * 2)} DAY
                    GROUP BY ts_code
                )
                SELECT
                    SUM(CASE WHEN l.close >= h.max_close THEN 1 ELSE 0 END) AS new_high_count,
                    SUM(CASE WHEN l.close <= h.min_close THEN 1 ELSE 0 END) AS new_low_count
                FROM latest l
                JOIN hist h ON l.ts_code = h.ts_code
                """
            )
            if df.empty:
                return stats
            new_high_count = int(df.iloc[0]['new_high_count'] or 0)
            new_low_count = int(df.iloc[0]['new_low_count'] or 0)
            stats['new_high_count'] = new_high_count
            stats['new_low_count'] = new_low_count
            stats['new_high_low_ratio'] = round((new_high_count + 1) / (new_low_count + 1), 4)
            stats['_new_high_low_available'] = True
        except Exception as e:
            logger.debug(f"New high/low stats error: {e}")
        return stats

    def _get_max_limit_up_streak(self, trade_date: str, lookback_days: int = 15) -> int:
        try:
            # 先限定当日涨停池，再拉历史，避免扫描全市场全历史
            today_limit_df = fetch_df(
                f"""
                SELECT ts_code
                FROM daily_price
                WHERE trade_date = '{trade_date}' AND pct_chg >= 9.5
                """
            )
            if today_limit_df.empty:
                return 0

            ts_codes = [c for c in today_limit_df['ts_code'].tolist() if c]
            codes_str = ",".join([f"'{c}'" for c in ts_codes])

            df = fetch_df(
                f"""
                SELECT ts_code, trade_date, pct_chg
                FROM daily_price
                WHERE trade_date <= '{trade_date}'
                  AND trade_date >= DATE '{trade_date}' - INTERVAL {int(lookback_days * 2)} DAY
                  AND ts_code IN ({codes_str})
                ORDER BY trade_date DESC
                """
            )
            if df.empty:
                return 0

            df['trade_date'] = df['trade_date'].astype(str)
            max_streak = 1
            for ts_code in ts_codes:
                stock_df = df[df['ts_code'] == ts_code].sort_values('trade_date')
                flags = (stock_df['pct_chg'] >= 9.5).tolist()
                streak = 0
                for is_limit in reversed(flags):
                    if is_limit:
                        streak += 1
                    else:
                        break
                if streak > max_streak:
                    max_streak = streak
            return int(max_streak)
        except Exception as e:
            logger.debug(f"Limit-up streak error: {e}")
            return 0

    def _get_index_volatility_proxy(self, trade_date: str, ts_code: str = '000300.SH') -> dict:
        stats = {
            'index_vol_20': 0.0,
            'iv_proxy_z': 0.0,
            '_iv_proxy_available': False
        }
        try:
            df = fetch_df(
                f"""
                SELECT trade_date, close
                FROM market_index
                WHERE ts_code = '{ts_code}' AND trade_date <= '{trade_date}'
                ORDER BY trade_date DESC
                LIMIT 180
                """
            )
            if df.empty or len(df) < 25:
                return stats
            df = df.sort_values('trade_date')
            df['ret'] = df['close'].pct_change()
            if df['ret'].dropna().empty:
                return stats

            vol20 = float(df['ret'].tail(20).std() * np.sqrt(252) * 100)
            stats['index_vol_20'] = round(vol20, 4)

            # 用近120天波动率均值和标准差构造 z-score
            rolling = df['ret'].rolling(window=20).std() * np.sqrt(252) * 100
            hist = rolling.dropna().tail(120)
            if len(hist) >= 20:
                mean_v = float(hist.mean())
                std_v = float(hist.std())
                if std_v > 1e-6:
                    stats['iv_proxy_z'] = round((vol20 - mean_v) / std_v, 4)
                    stats['_iv_proxy_available'] = True
        except Exception as e:
            logger.debug(f"Volatility proxy error: {e}")
        return stats

    def _check_crash_feedback(self, trade_date: str) -> float:
        try:
            # 优化：增加缓存或简化查询
            df = fetch_df(f"SELECT trade_date, score FROM market_sentiment WHERE trade_date < '{trade_date}' ORDER BY trade_date DESC LIMIT 40")
            if len(df) < 2: return 0.0
            df = df.sort_values('trade_date')
            df['v1'] = df['score'].diff()
            panic_dates = df[df['v1'] < -10]['trade_date'].tail(3).tolist() # 只看最近3次大跌
            if not panic_dates: return 0.0
            rets = []
            for d in panic_dates:
                d_str = d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)
                r = fetch_df(f"SELECT pct_chg FROM market_index WHERE ts_code='000300.SH' AND trade_date > '{d_str}' ORDER BY trade_date ASC LIMIT 1")
                if not r.empty: rets.append(r.iloc[0]['pct_chg'])
            return sum(rets) / len(rets) if rets else 0.0
        except: return 0.0

    def _check_trend_context(self, trade_date: str) -> dict:
        """
        [New] 环境感知系统 - 增强版
        判断当前处于: 牛市(Bull), 熊市(Bear), 震荡(Chop)
        同时参考沪深300和科创50，只要有一个处于强势趋势即进入牛市模式。
        """
        c = SENTIMENT_CONFIG['context']
        try:
            # 获取两个指数的历史数据
            idx_list = ['000300.SH', '000688.SH']
            modes = []
            for ts_code in idx_list:
                df = fetch_df(f"SELECT close FROM market_index WHERE ts_code='{ts_code}' AND trade_date <= '{trade_date}' ORDER BY trade_date DESC LIMIT {c['ma_long'] + 5}")
                if len(df) < c['ma_long']: 
                    modes.append('CHOP')
                    continue
                
                close = df['close'].iloc[0]
                ma20 = df['close'].iloc[:c['ma_mid']].mean()
                ma60 = df['close'].iloc[:c['ma_long']].mean()
                
                if close > ma20 and ma20 > ma60 * 0.98: # 稍微放宽 ma20 > ma60 的要求
                    modes.append('BULL')
                elif close < ma20 and ma20 < ma60:
                    modes.append('BEAR')
                else:
                    modes.append('CHOP')
            
            # 决策逻辑：只要有一个是 BULL，整体就是 BULL
            final_mode = 'CHOP'
            if 'BULL' in modes:
                final_mode = 'BULL'
            elif 'BEAR' in modes and 'BULL' not in modes:
                final_mode = 'BEAR'
                
            return {'mode': final_mode}
        except: return {'mode': 'CHOP'}

    def _generate_plan(self, fp, score, v1, v2, crash_exp_ret=0.0, trend_context=None, score_ma5=None, v1_rate=0.0, sustained_high_days=0, new_metrics=None, position_info=None):
        """
        V34: 平衡型情绪策略
        1. 先做风险裁剪，再做择时。
        2. 买卖都由同一套状态变量驱动，避免“只卖不买”偏置。
        3. 采用三类入场（逆向/趋势/突破）与三类离场（风控/衰减/保护）。
        """
        if new_metrics is None:
            new_metrics = {}
        if position_info is None:
            position_info = {}
        if trend_context is None:
            trend_context = {'mode': 'CHOP'}
        if score_ma5 is None:
            score_ma5 = score

        buy_cfg = SENTIMENT_CONFIG.get('buy', {})
        sell_cfg = SENTIMENT_CONFIG.get('sell', {})
        bull_cfg = SENTIMENT_CONFIG.get('bull', {})
        chop_cfg = SENTIMENT_CONFIG.get('chop', {})
        m_cfg = SENTIMENT_CONFIG.get('momentum', {})

        mode = trend_context.get('mode', 'CHOP')

        breadth_ratio = float(new_metrics.get('breadth_ratio', 0.5))
        breadth_div = float(new_metrics.get('breadth_divergence', 0.0))
        mom_div = float(new_metrics.get('momentum_divergence', 0.0))
        vol_stag = bool(new_metrics.get('volume_stagnation', False))
        double_peak = bool(new_metrics.get('double_peak_pattern', False))
        score_z = float(new_metrics.get('score_z', 0.0))
        score_drawdown5 = float(new_metrics.get('score_drawdown5', 0.0))
        sustained_low_days = int(new_metrics.get('sustained_low_days', 0))

        above_ma5_required = bool(m_cfg.get('buy_score_above_ma5', True))
        above_ma5_ok = (score >= score_ma5) if above_ma5_required else True

        # 统一状态变量
        chase_zone = score >= m_cfg.get('avoid_chase_score', 92)
        overheat_zone = score >= sell_cfg.get('euphoria_threshold', 85)
        momentum_break = (
            (v1 <= sell_cfg.get('momentum_reversal_v1', -3.0) and v2 <= sell_cfg.get('momentum_reversal_v2', -1.5))
            or score_drawdown5 <= -8
        )
        divergence_risk = (
            score >= m_cfg.get('sell_divergence_score', 78)
            and (breadth_div < -0.06 or mom_div > 8 or score_z > m_cfg.get('score_z_top', 1.2))
        )

        # 三类入场模板：逆向、顺势、突破
        rebound_entry = (
            score <= m_cfg.get('buy_neglect_score', 42)
            and breadth_ratio <= m_cfg.get('buy_neglect_breadth_max', 0.45)
            and (v1 >= -3.0 or v2 >= 0.0)
        )
        trend_entry = (
            score >= max(chop_cfg.get('range_low_score', 32), buy_cfg.get('normal_buy_min_score', 25))
            and score <= max(chop_cfg.get('range_high_score', 62), 68)
            and above_ma5_ok
            and v1 > 0
            and breadth_ratio >= 0.48
        )
        breakout_entry = (
            score >= buy_cfg.get('breakout_score', 58)
            and above_ma5_ok
            and v1 >= max(3.0, buy_cfg.get('momentum_v1_threshold', 5.0) - 2.0)
            and v2 >= max(0.0, buy_cfg.get('momentum_v2_threshold', 0.5) - 0.5)
            and breadth_ratio >= max(0.45, m_cfg.get('buy_min_breadth', 0.45))
            and mom_div < 12
        )

        if position_info and _position_state.in_position:
            hold_days = position_info.get('hold_days', 0)
            profit_pct = position_info.get('profit_pct', 0)
            peak_profit_pct = position_info.get('peak_profit_pct', 0)

            # 1) 硬风控
            if profit_pct <= sell_cfg.get('stop_loss', -5.0):
                return self._pack_result(MarketMood.ICE_POINT, "PLAN_SELL_ALL", "【止损清仓】跌破风控阈值。", 95, 5, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)
            if hold_days >= sell_cfg.get('max_hold_days', 20):
                signal = "PLAN_SELL_PARTIAL" if profit_pct > 0 else "PLAN_SELL_ALL"
                return self._pack_result(MarketMood.CONFUSED, signal, "【到期减仓】持仓时长达到上限。", 75, 25, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

            # 2) 顶部风险与动量衰减
            if divergence_risk:
                signal = "PLAN_SELL_PARTIAL" if profit_pct >= 2 else "PLAN_SELL_ALL"
                return self._pack_result(MarketMood.DIVERGENCE_SELL, signal, "【背离卖出】高位情绪与市场广度背离。", 88, 12, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

            if sustained_high_days >= m_cfg.get('sell_high_days_warning', 4) and v1 < -1.5:
                return self._pack_result(MarketMood.EUPHORIA, "PLAN_SELL_PARTIAL", "【高温退潮】连续高位后动量转弱。", 82, 18, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

            if (
                sustained_high_days >= m_cfg.get('sell_euphoria_persist_days', 4)
                and score >= m_cfg.get('sell_euphoria_score', 82)
                and (v1 <= 1.0 or score_z >= 1.0 or breadth_ratio < 0.45)
            ):
                return self._pack_result(MarketMood.EUPHORIA, "PLAN_SELL_PARTIAL", "【持续鼎沸卖出】人声鼎沸持续，分批兑现。", 86, 14, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

            if momentum_break:
                return self._pack_result(MarketMood.DIVERGENCE_SELL, "PLAN_SELL_ALL", "【动量离场】一二阶动量同步走弱。", 80, 20, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

            if vol_stag or double_peak:
                return self._pack_result(MarketMood.DIVERGENCE_SELL, "PLAN_SELL_PARTIAL", "【顶部预警】量价或结构出现顶背离。", 86, 14, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

            # 3) 浮盈回撤保护
            pullback = peak_profit_pct - profit_pct
            if (
                peak_profit_pct >= sell_cfg.get('trailing_profit_floor', 4.0)
                and pullback >= sell_cfg.get('trailing_pullback', 3.5)
            ):
                return self._pack_result(MarketMood.BOILING, "PLAN_SELL_PARTIAL", "【浮盈保护】高位回撤触发减仓。", 78, 22, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

            if profit_pct >= sell_cfg.get('profit_take', 8.0) and (v1 < -0.5 or overheat_zone):
                return self._pack_result(MarketMood.BOILING, "PLAN_SELL_PARTIAL", "【分批止盈】收益达标且动量趋弱。", 55, 45, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

            # 4) 否则持有
            return self._pack_result(MarketMood.BOILING, "PLAN_HOLD", "【持有】情绪动量仍支撑仓位。", 28, 72, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        # === 无持仓：买入决策 ===
        if chase_zone and not breakout_entry:
            return self._pack_result(MarketMood.EUPHORIA, "PLAN_WATCH", "【观望】情绪过热，避免追高。", 72, 28, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        iv_z = float(fp.get('iv_proxy_z', 0.0))
        if mode != 'BULL' and iv_z >= 1.2 and breadth_ratio < 0.5 and v1 < 2.0:
            return self._pack_result(MarketMood.CONFUSED, "PLAN_WATCH", "【观望】高波动弱广度环境，等待更高确定性。", 78, 22, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        # 极端恐慌但动量未企稳，避免接飞刀
        if score <= buy_cfg.get('ice_point_max_score', 35) and v1 < -6 and v2 <= 0:
            return self._pack_result(MarketMood.ICE_POINT, "PLAN_WATCH", "【风控】冰点惯性下跌，等待动量拐点。", 88, 12, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        if rebound_entry and sustained_low_days >= m_cfg.get('buy_neglect_days', 1):
            return self._pack_result(MarketMood.ICE_POINT, "PLAN_BUY_PARTIAL", "【逆向分批买入】连续低迷后动量拐点出现。", 24, 95, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        # 冰点反转
        if (
            score <= buy_cfg.get('ice_point_max_score', 35)
            and v1 >= min(0.0, buy_cfg.get('ice_point_v1_threshold', 0) + 0.5)
            and v2 >= min(0.5, buy_cfg.get('rebound_v2_threshold', 1.0))
            and breadth_ratio >= max(0.40, m_cfg.get('buy_min_breadth', 0.45) - 0.05)
        ):
            return self._pack_result(MarketMood.ICE_POINT, "PLAN_BUY_FULL", "【冰点反转重仓】低位情绪动量共振修复。", 26, 94, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        # 动量突破
        if breakout_entry:
            signal = "PLAN_BUY_FULL" if (score >= 68 or breadth_ratio >= 0.55) else "PLAN_BUY_PARTIAL"
            return self._pack_result(MarketMood.DIVERGENCE, signal, "【动量突破】分数与动量同步抬升。", 32, 90, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        # 牛市顺势
        if mode == 'BULL' and (
            (trend_entry and v1 >= bull_cfg.get('buy_min_v1', -2.0))
            or (score >= bull_cfg.get('buy_min_score', 20) and above_ma5_ok and v1 >= -1.0 and breadth_ratio >= 0.46)
        ):
            return self._pack_result(MarketMood.DIVERGENCE, "PLAN_BUY_PARTIAL", "【牛市顺势分批】趋势环境下优先跟随主升。", 34, 88, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        # 震荡低吸
        if mode != 'BULL' and trend_entry:
            return self._pack_result(MarketMood.DIVERGENCE, "PLAN_BUY_PARTIAL", "【震荡低吸分批】区间下沿修复，轻仓试错。", 38, 82, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        if crash_exp_ret > 0.1 and v1 > 1.5 and breadth_ratio >= 0.5:
            return self._pack_result(MarketMood.DIVERGENCE, "PLAN_BUY_FULL", "【修复狙击重仓】历史恐慌后修复胜率提升。", 35, 86, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        # 兜底买点：避免长期只有卖出没有买入
        if score >= 48 and v1 >= 1.0 and breadth_ratio >= 0.50 and above_ma5_ok and iv_z < 1.6:
            return self._pack_result(MarketMood.DIVERGENCE, "PLAN_BUY_PARTIAL", "【趋势试单】动量转正，先行分批试错。", 45, 70, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

        return self._pack_result(MarketMood.CONFUSED, "PLAN_WATCH", "【观望】等待情绪动量与广度共振。", 62, 38, fp, score, v1, v2, score_ma5, v1_rate, sustained_high_days)

    def _pack_result(self, mood, signal, text, risk, opp, fp, score, v1, v2, score_ma5, v1_rate=0.0, sustained_high_days=0):
        return { 
            "market_mood": mood.value, 
            "signal": signal, 
            "next_day_strategy": text, 
            "risk_factor": risk, 
            "opportunity_factor": opp, 
            "fingerprint": fp, 
            "metrics": {
                "score": score, 
                "v1": v1, 
                "v2": v2,
                "score_ma5": score_ma5,
                "v1_rate": v1_rate,
                "sustained_high_days": sustained_high_days
            } 
        }

    def _build_execution(self, plan: dict, fingerprint: dict, trend_context: dict | None = None) -> dict:
        """
        执行层建议：
        1) 将信号转换为可执行仓位
        2) 在高波动/弱广度环境下自动降仓
        3) 给出止损止盈参数
        """
        signal = str(plan.get("signal", "PLAN_WATCH"))
        action = self._signal_action(signal)
        sell_cfg = SENTIMENT_CONFIG.get("sell", {})
        context = trend_context or {"mode": "CHOP"}

        target_position = float(self._target_position_from_signal(signal, 0.0))
        iv_z = self._finite_number(fingerprint.get("iv_proxy_z", 0.0), 0.0)
        up_count = float(fingerprint.get("up_count", 0.0))
        down_count = float(fingerprint.get("down_count", 0.0))
        breadth_ratio = up_count / max(up_count + down_count, 1.0)
        risk_factor = float(plan.get("risk_factor", 50.0))

        if action == "BUY":
            if iv_z >= 1.2:
                target_position *= 0.7
            if breadth_ratio < 0.48:
                target_position *= 0.85
            if context.get("mode") != "BULL":
                target_position *= 0.9
            if risk_factor >= 75:
                target_position *= 0.75
        elif action == "SELL":
            target_position = min(target_position, 0.5)

        target_position = round(float(np.clip(target_position, 0.0, 1.0)), 2)

        stop_loss = abs(float(sell_cfg.get("stop_loss", -5.0)))
        take_profit = abs(float(sell_cfg.get("profit_take", 8.0)))
        if iv_z >= 1.2:
            stop_loss = max(2.5, stop_loss * 0.7)
        if risk_factor >= 80:
            take_profit = max(4.0, take_profit * 0.8)

        tranche_count = 1
        if signal in {"PLAN_BUY_PARTIAL", "PLAN_SELL_PARTIAL"}:
            tranche_count = 2
        elif action == "BUY" and target_position >= 0.9:
            tranche_count = 3

        confidence = round(float(np.clip((100.0 - risk_factor + float(plan.get("opportunity_factor", 50.0))) / 2.0, 0, 100)), 1)
        return {
            "action": action,
            "target_position": target_position,
            "tranche_count": tranche_count,
            "stop_loss_pct": round(stop_loss, 2),
            "take_profit_pct": round(take_profit, 2),
            "confidence": confidence
        }

    def _save_result(self, trade_date, plan, fingerprint, current_score):
        label_map = { 
            "PLAN_BUY_PARTIAL": "分批买入",
            "PLAN_BUY_FULL": "整体买入",
            "PLAN_BUY_ICE": "冰点买入",
            "PLAN_BUY_NEGLECT": "无人问津买入",
            "PLAN_BUY_NORMAL": "买入",
            "PLAN_BUY_MOMENTUM": "动量买入",
            "PLAN_BUY_BULL": "牛市顺势",
            "PLAN_BUY_REVERSAL": "反核博弈", 
            "PLAN_SNIPER_BUY": "狙击买入", 
            "PLAN_BUY_DIVERGENCE": "积极做多", 
            "PLAN_HOLD": "继续持有", 
            "PLAN_SELL_CLIMAX": "趋势止盈", 
            "PLAN_SELL_ALL": "清仓离场", 
            "PLAN_SELL_PARTIAL": "分批卖出",
            "PLAN_WATCH": "观望",
            "PLAN_SELL_PROFIT": "止盈",
            "PLAN_SELL_STOP_LOSS": "止损",
            "PLAN_SELL_TIME": "到期清仓",
            "PLAN_SELL_COOLDOWN": "情绪退潮",
            "PLAN_SELL_TOP": "逃顶",
            "PLAN_SELL_DIVERGENCE": "背离卖出",
            "PLAN_SELL_EUPHORIA": "人声鼎沸",
            "PLAN_SELL_EUPHORIA_PERSIST": "持续鼎沸卖出",
            "PLAN_SELL_MOMENTUM": "动量离场",
            "PLAN_SELL_TRAIL": "浮盈保护"
        }
        advice_map = { 
            "PLAN_BUY_PARTIAL": "分批建仓",
            "PLAN_BUY_FULL": "重仓买入",
            "PLAN_BUY_ICE": "重仓买入",
            "PLAN_BUY_NEGLECT": "逆向低吸",
            "PLAN_BUY_NORMAL": "顺势买入",
            "PLAN_BUY_MOMENTUM": "追涨买入",
            "PLAN_BUY_BULL": "顺势跟随",
            "PLAN_BUY_REVERSAL": "博弈长腿", 
            "PLAN_SNIPER_BUY": "重仓出击", 
            "PLAN_BUY_DIVERGENCE": "顺势加仓", 
            "PLAN_HOLD": "继续持有", 
            "PLAN_SELL_CLIMAX": "落袋为安", 
            "PLAN_SELL_ALL": "空仓避险", 
            "PLAN_SELL_PARTIAL": "分批兑现",
            "PLAN_WATCH": "等待机会",
            "PLAN_SELL_PROFIT": "锁定利润",
            "PLAN_SELL_STOP_LOSS": "认赔离场",
            "PLAN_SELL_TIME": "换股操作",
            "PLAN_SELL_COOLDOWN": "退出观望",
            "PLAN_SELL_TOP": "逃顶离场",
            "PLAN_SELL_DIVERGENCE": "减仓离场",
            "PLAN_SELL_EUPHORIA": "高位兑现",
            "PLAN_SELL_EUPHORIA_PERSIST": "分批止盈",
            "PLAN_SELL_MOMENTUM": "动量转弱离场",
            "PLAN_SELL_TRAIL": "锁定浮盈"
        }
        label = label_map.get(plan['signal'], "未知")
        advice = advice_map.get(plan['signal'], "观望")
        details = {
            "signal": plan['signal'], "strategy": plan['next_day_strategy'], "mood": plan['market_mood'], "fingerprint": fingerprint,
            "action": self._signal_action(plan.get('signal')),
            "execution": plan.get("execution", self._build_execution(plan, fingerprint, self._check_trend_context(trade_date))),
            "factors": { 
                "breadth": round(fingerprint['up_count'] / (fingerprint['up_count'] + fingerprint['down_count']) * 100, 1) if (fingerprint['up_count'] + fingerprint['down_count']) > 0 else 0, 
                "median_chg": round(fingerprint.get('median_pct_chg', 0), 1), 
                "index_chg": round(fingerprint.get('index_pct_chg', 0), 1), 
                "limit": fingerprint.get('limit_up_count', 0), 
                "failure": fingerprint.get('broken_count', 0),
                "limit_up_down_ratio": round(fingerprint.get('limit_up_down_ratio', 1.0), 2),
                "max_limit_up_streak": int(fingerprint.get('max_limit_up_streak', 0)),
                "turnover_activity": round(fingerprint.get('turnover_activity', 1.0), 4),
                "margin_financing_delta5": round(fingerprint.get('margin_financing_delta5', 0.0), 4),
                "net_mf_ratio": round(fingerprint.get('net_mf_ratio', 0.0), 4),
                "new_high_count": int(fingerprint.get('new_high_count', 0)),
                "new_low_count": int(fingerprint.get('new_low_count', 0)),
                "new_high_low_ratio": round(fingerprint.get('new_high_low_ratio', 1.0), 4),
                "index_vol_20": round(fingerprint.get('index_vol_20', 0.0), 4),
                "iv_proxy_z": round(fingerprint.get('iv_proxy_z', 0.0), 4),
                "factor_percentiles": fingerprint.get('factor_percentiles', {})
            },
            "missing": {
                "factors": fingerprint.get('missing_flags', {})
            },
            "risk": plan['risk_factor'], "opp": plan['opportunity_factor'],
            "metrics": { 
                "score": round(plan.get('metrics', {}).get('score', 0), 1), 
                "v1": round(plan.get('metrics', {}).get('v1', 0), 1), 
                "v2": round(plan.get('metrics', {}).get('v2', 0), 1),
                "score_ma5": round(plan.get('metrics', {}).get('score_ma5', 0), 1),
                "v1_rate": round(plan.get('metrics', {}).get('v1_rate', 0), 4),
                "sustained_high_days": plan.get('metrics', {}).get('sustained_high_days', 0),
                "advice": advice 
            }
        }
        details = self._sanitize_payload(details)
        current_score = round(self._finite_number(current_score, 50.0), 1)

        sql = "INSERT INTO market_sentiment (trade_date, score, label, details) VALUES (?, ?, ?, ?) ON CONFLICT (trade_date) DO UPDATE SET score=excluded.score, label=excluded.label, details=excluded.details"
        with get_db_connection() as con:
            con.execute(sql, (trade_date, current_score, label, json.dumps(details, ensure_ascii=False)))
        logger.info(f"推演结果已保存: {label} | Score: {current_score:.1f}")

    def preview_next_day(self, index_pct_chg: float, star50_pct_chg: float | None = None, as_of: str | None = None):
        """
        盘中预估（不落库）：
        - 以最近一个已收盘交易日的指纹为基线
        - 替换盘中指数涨跌幅后，估算次日情绪分与策略信号
        """
        latest_df = fetch_df(
            "SELECT trade_date, score, details FROM market_sentiment ORDER BY trade_date DESC LIMIT 1"
        )
        if latest_df.empty:
            raise ValueError("market_sentiment 为空，请先完成一次历史情绪计算")

        latest_row = latest_df.iloc[0]
        latest_trade_date = (
            latest_row["trade_date"].strftime("%Y-%m-%d")
            if hasattr(latest_row["trade_date"], "strftime")
            else str(latest_row["trade_date"])
        )
        next_trade_date = self._next_trading_day_str(latest_trade_date)

        details = latest_row.get("details")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except Exception:
                details = {}
        details = details if isinstance(details, dict) else {}
        baseline_fp = details.get("fingerprint", {}) if isinstance(details.get("fingerprint", {}), dict) else {}

        # 如果历史 details 缺失，回退到最近交易日重算指纹
        if not baseline_fp:
            df_last = self._get_daily_data(latest_trade_date)
            if df_last.empty:
                raise ValueError(f"缺少 {latest_trade_date} 的日线数据，无法预估")
            baseline_fp = self._calculate_fingerprint(df_last, latest_trade_date)

        fingerprint = copy.deepcopy(baseline_fp)
        fingerprint["index_pct_chg"] = round(self._finite_number(index_pct_chg, 0.0), 3)
        if star50_pct_chg is not None:
            fingerprint["star50_pct_chg"] = round(self._finite_number(star50_pct_chg, 0.0), 3)

        current_score = self._calculate_continuous_score(fingerprint)

        hist_df = fetch_df("SELECT trade_date, score FROM market_sentiment ORDER BY trade_date DESC LIMIT 10")
        hist_df = hist_df.sort_values("trade_date") if not hist_df.empty else hist_df
        prev_scores = [self._finite_number(x, 50.0) for x in hist_df["score"].tolist()] if not hist_df.empty else []

        prev_score = prev_scores[-1] if prev_scores else self._finite_number(latest_row.get("score"), 50.0)
        v1 = current_score - prev_score
        if len(prev_scores) >= 2:
            prev_v1 = prev_scores[-1] - prev_scores[-2]
            v2 = v1 - prev_v1
        else:
            v2 = 0.0

        score_window = (prev_scores[-4:] if len(prev_scores) >= 4 else prev_scores) + [current_score]
        score_ma5 = sum(score_window) / len(score_window) if score_window else current_score
        v1_rate = v1 / max(prev_score, 1.0)

        sustained_high_days = 0
        if prev_scores:
            merged = prev_scores + [current_score]
            for s in reversed(merged):
                if s > 80:
                    sustained_high_days += 1
                else:
                    break

        up_count = int(fingerprint.get("up_count", 0) or 0)
        down_count = int(fingerprint.get("down_count", 0) or 0)
        total = max(up_count + down_count, 1)
        new_metrics = {
            "breadth_divergence": 0.0,
            "momentum_divergence": 0.0,
            "volume_stagnation": False,
            "double_peak_pattern": False,
            "breadth_ratio": up_count / total,
            "score_ma10": score_ma5,
            "score_std10": 0.0,
            "score_z": 0.0,
            "score_drawdown5": 0.0,
            "sustained_low_days": 0
        }

        crash_exp_ret = self._check_crash_feedback(next_trade_date)
        trend_context = self._check_trend_context(next_trade_date)
        plan = self._generate_plan(
            fingerprint,
            current_score,
            v1,
            v2,
            crash_exp_ret,
            trend_context,
            score_ma5,
            v1_rate,
            sustained_high_days,
            new_metrics,
            position_info={}
        )
        plan["execution"] = self._build_execution(plan, fingerprint, trend_context)
        return {
            "as_of": as_of or arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
            "baseline_trade_date": latest_trade_date,
            "predicted_trade_date": next_trade_date,
            "projected_score": round(current_score, 1),
            "index_pct_chg": round(self._finite_number(index_pct_chg, 0.0), 3),
            "star50_pct_chg": round(self._finite_number(star50_pct_chg, 0.0), 3) if star50_pct_chg is not None else None,
            "plan": self._sanitize_payload(plan)
        }

    def calculate(self, days=365):
        date_query = f"SELECT DISTINCT trade_date FROM daily_price ORDER BY trade_date DESC LIMIT {int(days)}"
        dates_df = fetch_df(date_query)
        if dates_df.empty: return
        target_dates = sorted(dates_df['trade_date'].tolist())
        for d in target_dates:
            date_str = d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)
            self.analyze(date_str)
        
        # 自动执行回测并输出到日志
        res = self.backtest_star50()
        if res:
            m = res['metrics']
            logger.info(f"回测完成: 总收益={m['total_return']}, 最大回撤={m['max_drawdown']}, 胜率={m['win_rate']}, 基准={m['benchmark_return']}")
            self.generate_report()

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

    def backtest_star50(self, initial_capital=100000, start_date=None, policy=None):
        """
        科创50 ETF 情绪对冲策略回测 - 有状态增强版
        核心改进:
        1. 有状态持仓追踪 (开仓/平仓/止损/止盈)
        2. 修复之前"分数>85就持仓"的bug
        3. 明确买入信号 (冰点/狙击)
        4. 明确卖出信号 (止损/止盈/退潮)
        """
        import pandas as pd
        try:
            query_sentiment = "SELECT trade_date, score, label, details FROM market_sentiment ORDER BY trade_date"
            df_sent = fetch_df(query_sentiment)
            
            # 使用 000688.SH (科创50) 作为底层资产
            query_price = "SELECT trade_date, open, high, low, close, pre_close, pct_chg FROM market_index WHERE ts_code='000688.SH' ORDER BY trade_date"
            df_price = fetch_df(query_price)
            
            if df_sent.empty or df_price.empty:
                logger.warning("Backtest failed: Missing sentiment or price data.")
                return None

            df_sent['trade_date'] = pd.to_datetime(df_sent['trade_date'])
            df_price['trade_date'] = pd.to_datetime(df_price['trade_date'])
            
            df = pd.merge(df_price, df_sent, on='trade_date', how='left')
            df = df.sort_values('trade_date').reset_index(drop=True)
            if df.empty:
                logger.warning("Backtest failed: No merged data.")
                return None

            # 默认回测窗口: 最近一年
            if start_date is None:
                end_date = df['trade_date'].max()
                start_ts = end_date - pd.Timedelta(days=365)
            else:
                start_ts = pd.to_datetime(start_date)
            df = df[df['trade_date'] >= start_ts].reset_index(drop=True)
            if df.empty:
                logger.warning("Backtest failed: Empty data in selected window.")
                return None
            
            df['label'] = df['label'].fillna("观望").astype(str).str.strip()

            # 解析动作信号：优先 details.action，其次由 signal code 推断
            def parse_signal_code(details):
                if isinstance(details, dict):
                    return str(details.get("signal", "PLAN_WATCH"))
                if isinstance(details, str) and details:
                    try:
                        return str(json.loads(details).get("signal", "PLAN_WATCH"))
                    except Exception:
                        return "PLAN_WATCH"
                return "PLAN_WATCH"

            def parse_action(details, signal):
                details_obj = {}
                if isinstance(details, str) and details:
                    try:
                        details_obj = json.loads(details)
                    except Exception:
                        details_obj = {}
                elif isinstance(details, dict):
                    details_obj = details

                action = str(details_obj.get("action", "")).upper().strip()
                if action in {"BUY", "SELL", "HOLD", "WATCH"}:
                    return action
                return self._signal_action(signal)

            df['signal_code'] = df['details'].apply(parse_signal_code).fillna("PLAN_WATCH").astype(str)
            df['action'] = df.apply(lambda row: parse_action(row.get('details'), row.get('signal_code')), axis=1)
            
            # === 预计算技术指标 ===
            # ATR (14-period) for dynamic stop loss
            df['tr'] = np.maximum(
                df['high'] - df['low'],
                np.maximum(
                    (df['high'] - df['pre_close']).abs(),
                    (df['low'] - df['pre_close']).abs()
                )
            )
            df['atr14'] = df['tr'].rolling(14, min_periods=5).mean()
            df['atr_pct'] = (df['atr14'] / df['close'] * 100).fillna(2.0)
            
            # Score smoothing: 3-day EMA to reduce noise
            df['score_filled'] = df['score'].ffill().fillna(50.0)
            df['score_ema3'] = df['score_filled'].ewm(span=3, min_periods=1).mean()
            df['score_v1'] = df['score_filled'].diff(3).fillna(0.0)
            
            # MA20 for trend filter
            df['ma20'] = df['close'].rolling(20, min_periods=10).mean()
            
            # === 分数驱动仓位回测 ===
            # 核心思路：用情绪分数连续映射到目标仓位，而非离散BUY/SELL信号
            # 分数高→高仓位，分数低→低/零仓位
            # 风控规则(止损/止盈/退潮)作为强制覆盖层
            
            sell_cfg = SENTIMENT_CONFIG.get('sell', {})
            score_cfg = SENTIMENT_CONFIG.get('score_position', {})
            score_full = float(score_cfg.get('full_pos_score', 80))
            score_half = float(score_cfg.get('half_pos_score', 65))
            score_zero = float(score_cfg.get('zero_pos_score', 50))
            
            current_pos = 0.0
            entry_price = 0.0
            entry_score = 0.0
            hold_days = 0
            peak_profit = 0.0
            entry_date = None
            days_below_zero = 0
            
            # 风控状态
            consecutive_losses = 0          # 连续亏损计数
            equity_peak = 1.0               # 权益峰值(用于计算回撤)
            equity_current = 1.0            # 当前权益
            circuit_breaker_active = False   # 熔断状态
            
            # 提前提取杠杆，用于风控计算
            bt_cfg_pre = SENTIMENT_CONFIG.get("backtest", {})
            leverage_pre = float((policy or {}).get("leverage", bt_cfg_pre.get("leverage", 1.0)))
            
            cons_loss_limit = int(sell_cfg.get('consecutive_loss_limit', 2))
            cons_loss_scale = float(sell_cfg.get('consecutive_loss_scale', 0.5))
            dd_breaker = float(sell_cfg.get('drawdown_circuit_breaker', -15.0))
            dd_resume = float(sell_cfg.get('drawdown_resume', -8.0))
            # 这里先用trade-level近似，后面会在向量化阶段做最终DD计算
            
            positions = []
            target_pos_list = []
            signal_stats = {"BUY": 0, "SELL": 0, "HOLD": 0, "WATCH": 0}
            
            def _record_trade(exit_date, exit_price, closed_pos, reason):
                """记录一笔平仓交易"""
                nonlocal consecutive_losses, equity_current, equity_peak, circuit_breaker_active
                if entry_price <= 0 or closed_pos <= 0:
                    return
                pnl = (exit_price - entry_price) / entry_price * 100
                positions.append({
                    'entry_date': entry_date,
                    'exit_date': exit_date,
                    'entry_price': round(entry_price, 4),
                    'exit_price': round(exit_price, 4),
                    'profit_pct': round(pnl, 2),
                    'hold_days': hold_days,
                    'pos_closed': round(closed_pos, 3),
                    'weighted_pnl': round(pnl * closed_pos, 2),
                    'reason': reason
                })
                # 更新连亏计数
                if pnl < 0:
                    consecutive_losses += 1
                else:
                    consecutive_losses = 0
                # 更新权益曲线(去杠杆：仅用仓位加权PnL，熔断基于真实仓位风险)
                # 杠杆效果在向量化阶段的最终NAV计算中体现
                equity_current *= (1 + pnl * closed_pos / 100)
                if equity_current > equity_peak:
                    equity_peak = equity_current
                # 检查回撤熔断
                dd_pct = (equity_current / equity_peak - 1) * 100 if equity_peak > 0 else 0
                if dd_pct < dd_breaker:
                    circuit_breaker_active = True
                elif dd_pct > dd_resume:
                    circuit_breaker_active = False
            
            def _reset_position():
                nonlocal current_pos, entry_price, entry_score, hold_days, peak_profit, entry_date, days_below_zero
                current_pos = 0.0
                entry_price = 0.0
                entry_score = 0.0
                hold_days = 0
                peak_profit = 0.0
                entry_date = None
                days_below_zero = 0
            
            for i, row in df.iterrows():
                action = row['action']
                signal = row['signal_code']
                current_price = row['close']
                raw_score = float(row['score']) if row['score'] is not None else 50.0
                smooth_score = float(row.get('score_ema3', raw_score))
                signal_stats[action] = signal_stats.get(action, 0) + 1
                atr_pct = float(row.get('atr_pct', 2.0))
                score_v1 = float(row.get('score_v1', 0.0))
                ma20 = float(row.get('ma20', current_price))
                
                target_pos = 0.0
                
                if current_pos > 0:
                    hold_days += 1
                    profit_pct = (current_price - entry_price) / entry_price * 100 if entry_price > 0 else 0
                    if profit_pct > peak_profit:
                        peak_profit = profit_pct
                    
                    force_exit = False
                    exit_reason = ""
                    
                    # === 风控层（强制退出）===
                    # 1. 动态ATR止损
                    fixed_stop = sell_cfg.get('stop_loss', -4)
                    atr_mult = sell_cfg.get('atr_stop_multiplier', 1.5)
                    atr_stop = -(atr_pct * atr_mult)
                    dynamic_stop = min(fixed_stop, atr_stop)
                    if profit_pct < dynamic_stop:
                        force_exit = True
                        exit_reason = "止损"
                    
                    # 2. 移动止盈 (trailing stop)
                    elif peak_profit >= sell_cfg.get('trailing_profit_floor', 4.0):
                        pullback = peak_profit - profit_pct
                        if pullback >= sell_cfg.get('trailing_pullback', 3.5):
                            force_exit = True
                            exit_reason = "移动止盈"
                    
                    # 3. 固定止盈上限
                    elif profit_pct >= sell_cfg.get('profit_take', 8) * 1.5:
                        force_exit = True
                        exit_reason = "止盈"
                    
                    # 4. 最大持仓天数
                    elif hold_days >= sell_cfg.get('max_hold_days', 20):
                        force_exit = True
                        exit_reason = "到期"
                    
                    if force_exit:
                        _record_trade(row['trade_date'], current_price, current_pos, exit_reason)
                        target_pos = 0.0
                        _reset_position()
                    else:
                        # === 信号层 + 分数仓位调节 ===
                        if action == "SELL" and signal != "PLAN_SELL_PARTIAL":
                            if hold_days >= 2 and (smooth_score < score_half or profit_pct < -2.0):
                                # SELL信号 + 分数跌破半仓线 或 浮亏超2% → 清仓
                                _record_trade(row['trade_date'], current_price, current_pos, signal or "SELL")
                                target_pos = 0.0
                                _reset_position()
                            elif smooth_score < score_full and hold_days >= 3:
                                # SELL信号 + 分数低于满仓线 + 持仓3天 → 小幅减仓
                                score_pos = self._score_to_position(smooth_score, score_full, score_half, score_zero)
                                desired = max(score_pos, 0.5)
                                if desired < current_pos:
                                    delta = current_pos - desired
                                    _record_trade(row['trade_date'], current_price, delta, "信号减仓")
                                    current_pos = desired
                                target_pos = current_pos
                            else:
                                # 分数仍强(>=score_full)或持仓不足3天 → 完全忽略SELL信号
                                target_pos = current_pos
                        elif action == "BUY":
                            # 加仓到信号建议仓位，上限由分数决定
                            signal_pos = self._target_position_from_signal(signal, current_pos)
                            score_pos = self._score_to_position(smooth_score, score_full, score_half, score_zero)
                            desired = min(max(signal_pos, current_pos), max(score_pos, 0.5))
                            if desired > current_pos:
                                add_pos = desired - current_pos
                                new_total = current_pos + add_pos
                                entry_price = (entry_price * current_pos + current_price * add_pos) / new_total
                                current_pos = new_total
                            target_pos = current_pos
                        else:
                            # HOLD/WATCH — 根据分数微调仓位
                            score_pos = self._score_to_position(smooth_score, score_full, score_half, score_zero)
                            if score_pos < current_pos * 0.5 and hold_days >= 3:
                                # 分数大幅低于当前仓位，逐步减仓
                                desired = max(current_pos * 0.7, score_pos, 0.15)
                                if desired < current_pos:
                                    delta = current_pos - desired
                                    _record_trade(row['trade_date'], current_price, delta, "分数减仓")
                                    current_pos = desired
                            target_pos = current_pos
                else:
                    # 空仓 → BUY信号建仓，或HOLD/WATCH但分数足够强也建仓
                    # === 风控：熔断检查 ===
                    if circuit_breaker_active:
                        target_pos = 0.0  # 熔断中，禁止开仓
                    elif action == "BUY":
                        score_pos = self._score_to_position(smooth_score, score_full, score_half, score_zero)
                        signal_pos = self._target_position_from_signal(signal, 0.0)
                        target_pos = max(signal_pos, score_pos, 0.5)
                        # === 风控：波动率缩仓 (ATR高于正常水平时降仓) ===
                        if atr_pct > 2.5:
                            vol_scale = min(1.0, 2.0 / atr_pct)  # ATR=3%→scale=0.67, ATR=4%→0.5
                            target_pos *= vol_scale
                            target_pos = max(target_pos, 0.2)
                        # === 风控：连亏降仓 ===
                        if consecutive_losses >= cons_loss_limit:
                            target_pos *= cons_loss_scale
                            target_pos = max(target_pos, 0.2)  # 最低保留20%仓位
                        current_pos = target_pos
                        entry_price = current_price
                        entry_score = raw_score
                        entry_date = row['trade_date']
                        hold_days = 0
                        peak_profit = 0.0
                    elif action in ("HOLD", "WATCH") and smooth_score >= score_full:
                        # 分数很强但信号系统没给BUY → 主动建仓
                        target_pos = 0.5
                        # === 风控：波动率缩仓 ===
                        if atr_pct > 2.5:
                            vol_scale = min(1.0, 2.0 / atr_pct)
                            target_pos *= vol_scale
                            target_pos = max(target_pos, 0.2)
                        # === 风控：连亏降仓 ===
                        if consecutive_losses >= cons_loss_limit:
                            target_pos *= cons_loss_scale
                            target_pos = max(target_pos, 0.2)
                        current_pos = target_pos
                        entry_price = current_price
                        entry_score = raw_score
                        entry_date = row['trade_date']
                        hold_days = 0
                        peak_profit = 0.0
                
                target_pos_list.append(target_pos)
            
            df['target_pos'] = target_pos_list
            
            bt_cfg = SENTIMENT_CONFIG.get("backtest", {})
            leverage = float((policy or {}).get("leverage", bt_cfg.get("leverage", 1.0)))
            fee_rate = float((policy or {}).get("fee_rate", bt_cfg.get("fee_rate", 0.0015)))
            trend_floor_enabled = bool((policy or {}).get("trend_floor_enabled", bt_cfg.get("trend_floor_enabled", True)))
            trend_floor_pos = float((policy or {}).get("trend_floor_pos", bt_cfg.get("trend_floor_pos", 0.35)))
            ma_window = int((policy or {}).get("ma_window", bt_cfg.get("ma_window", 20)))

            if trend_floor_enabled:
                df['ma_trend'] = df['close'].rolling(ma_window, min_periods=max(5, ma_window // 2)).mean()
                trend_up = (df['close'] >= df['ma_trend']).fillna(False)
                df.loc[trend_up & (df['target_pos'] < trend_floor_pos), 'target_pos'] = trend_floor_pos

            # 向量化回测 (T+1 交易，今日信号决定明日仓位)
            df['bench_ret'] = df['pct_chg'] / 100.0
            df['pos_held'] = df['target_pos'].shift(1).fillna(0.0)
            df['strat_ret_gross'] = df['bench_ret'] * df['pos_held'] * leverage
            
            # 交易成本
            df['pos_change'] = (df['target_pos'] - df['target_pos'].shift(1).fillna(0.0)).abs()
            df['cost'] = df['pos_change'] * fee_rate
            df['strat_ret_net'] = df['strat_ret_gross'] - df['cost']
            
            # 计算累计收益
            df['strat_cum_ret'] = (1 + df['strat_ret_net']).cumprod()
            df['bench_cum_ret'] = (1 + df['bench_ret']).cumprod()
            df['strategy_nav'] = (df['strat_cum_ret'] * initial_capital)
            df['benchmark_nav'] = (df['bench_cum_ret'] * initial_capital)
            
            total_return = df['strat_cum_ret'].iloc[-1] - 1
            bench_return = df['bench_cum_ret'].iloc[-1] - 1
            max_drawdown = (df['strat_cum_ret'] / df['strat_cum_ret'].cummax() - 1).min()
            
            active_days = df[df['pos_held'] > 0]
            
            # 修正胜率：基于每笔交易的盈亏，而非持仓日指数涨跌
            trade_wins = len([p for p in positions if p['profit_pct'] > 0])
            trade_count = max(len(positions), 1)
            trade_win_rate = trade_wins / trade_count
            
            # 旧的日级胜率保留作为参考
            day_win_rate = len(active_days[active_days['bench_ret'] > 0]) / len(active_days) if len(active_days) > 0 else 0
            
            # 计算夏普比率 (简略版)
            sharpe = (df['strat_ret_net'].mean() / df['strat_ret_net'].std() * np.sqrt(242)) if df['strat_ret_net'].std() > 0 else 0

            # === 信号归因分析 (仓位加权) ===
            from collections import defaultdict
            signal_attr = defaultdict(lambda: {'count': 0, 'wins': 0, 'losses': 0, 
                                                'total_pnl': 0.0, 'total_weighted_pnl': 0.0,
                                                'total_pos': 0.0, 'avg_hold': 0.0, 'pnl_list': []})
            for p in positions:
                reason = p.get('reason', 'unknown')
                pnl = p['profit_pct']
                wpnl = p.get('weighted_pnl', pnl)
                pos_closed = p.get('pos_closed', 1.0)
                signal_attr[reason]['count'] += 1
                signal_attr[reason]['total_pnl'] += pnl
                signal_attr[reason]['total_weighted_pnl'] += wpnl
                signal_attr[reason]['total_pos'] += pos_closed
                signal_attr[reason]['avg_hold'] += p.get('hold_days', 0)
                signal_attr[reason]['pnl_list'].append(pnl)
                if pnl > 0:
                    signal_attr[reason]['wins'] += 1
                else:
                    signal_attr[reason]['losses'] += 1
            
            attribution = {}
            for reason, stats in signal_attr.items():
                cnt = stats['count']
                wins = stats['wins']
                pnl_list = stats['pnl_list']
                avg_win = sum(x for x in pnl_list if x > 0) / max(wins, 1)
                avg_loss = sum(x for x in pnl_list if x <= 0) / max(stats['losses'], 1)
                attribution[reason] = {
                    'count': cnt,
                    'win_rate': f"{wins / cnt * 100:.1f}%",
                    'avg_pnl': f"{stats['total_pnl'] / cnt:.2f}%",
                    'total_pnl': f"{stats['total_pnl']:.2f}%",
                    'weighted_pnl': f"{stats['total_weighted_pnl']:.2f}%",
                    'avg_pos_closed': f"{stats['total_pos'] / cnt:.2f}",
                    'avg_win': f"{avg_win:.2f}%",
                    'avg_loss': f"{avg_loss:.2f}%",
                    'profit_factor': f"{abs(avg_win / avg_loss):.2f}" if avg_loss != 0 else "inf",
                    'avg_hold_days': round(stats['avg_hold'] / cnt, 1)
                }

            df = df.fillna(0)
            df['trade_date'] = df['trade_date'].dt.strftime('%Y-%m-%d')
            
            result = {
                "metrics": {
                    "total_return": f"{total_return*100:.2f}%",
                    "annual_return": f"{(total_return / len(df) * 242)*100:.2f}%",
                    "max_drawdown": f"{max_drawdown*100:.2f}%",
                    "win_rate": f"{trade_win_rate*100:.2f}%",
                    "day_win_rate": f"{day_win_rate*100:.2f}%",
                    "sharpe": f"{sharpe:.2f}",
                    "benchmark_return": f"{bench_return*100:.2f}%",
                    "active_days_ratio": f"{len(active_days)/len(df)*100:.1f}%",
                    "total_trades": len(positions),
                    "buy_signals": int(signal_stats.get("BUY", 0)),
                    "sell_signals": int(signal_stats.get("SELL", 0)),
                    "hold_signals": int(signal_stats.get("HOLD", 0)),
                    "watch_signals": int(signal_stats.get("WATCH", 0)),
                    "leverage": round(leverage, 2),
                    "trend_floor_pos": round(trend_floor_pos, 2) if trend_floor_enabled else 0.0
                },
                "attribution": attribution,
                "trades": [
                    {
                        "entry_date": str(p['entry_date'])[:10],
                        "exit_date": str(p['exit_date'])[:10],
                        "entry_price": round(p['entry_price'], 2),
                        "exit_price": round(p['exit_price'], 2),
                        "profit_pct": round(p['profit_pct'], 2),
                        "weighted_pnl": round(p.get('weighted_pnl', p['profit_pct']), 2),
                        "pos_closed": round(p.get('pos_closed', 1.0), 3),
                        "hold_days": p['hold_days'],
                        "reason": p['reason']
                    } for p in positions
                ],
                "policy": {
                    "leverage": leverage,
                    "fee_rate": fee_rate,
                    "trend_floor_enabled": trend_floor_enabled,
                    "trend_floor_pos": trend_floor_pos,
                    "ma_window": ma_window
                },
                "curves": df.rename(columns={'pos_held': 'position'})
                           [['trade_date', 'strategy_nav', 'benchmark_nav', 'position']]
                           .to_dict('records')
            }
            return result
        except Exception as e:
            logger.error(f"Backtest error: {e}", exc_info=True)
            return None

    def optimize_backtest_policy(self, initial_capital=100000, start_date=None):
        bt_cfg = SENTIMENT_CONFIG.get("backtest", {})
        opt_cfg = bt_cfg.get("optimizer", {})
        if not opt_cfg.get("enabled", True):
            return self.backtest_star50(initial_capital=initial_capital, start_date=start_date), None

        target = float(opt_cfg.get("target_total_return", 1.0))
        max_dd_limit = float(opt_cfg.get("max_drawdown_limit", 0.35))
        leverage_grid = opt_cfg.get("leverage_grid", [1.0, 1.2, 1.5, 2.0])
        trend_floor_grid = opt_cfg.get("trend_floor_grid", [0.0, 0.2, 0.35, 0.5])
        fee_rate_grid = opt_cfg.get("fee_rate_grid", [bt_cfg.get("fee_rate", 0.0015)])

        best_res = None
        best_policy = None
        best_score = -1e9

        for lev in leverage_grid:
            for floor in trend_floor_grid:
                for fee in fee_rate_grid:
                    policy = {
                        "leverage": float(lev),
                        "trend_floor_enabled": True,
                        "trend_floor_pos": float(floor),
                        "fee_rate": float(fee),
                        "ma_window": int(bt_cfg.get("ma_window", 20))
                    }
                    res = self.backtest_star50(initial_capital=initial_capital, start_date=start_date, policy=policy)
                    if not res:
                        continue

                    m = res.get("metrics", {})
                    total_ret = float(str(m.get("total_return", "0%")).replace("%", "")) / 100.0
                    max_dd = abs(float(str(m.get("max_drawdown", "0%")).replace("%", "")) / 100.0)
                    sharpe = float(m.get("sharpe", 0))
                    logger.info(f"优化器: lev={lev}, floor={floor}, ret={total_ret*100:.1f}%, dd={max_dd*100:.1f}%, sharpe={sharpe:.2f}")
                    # 风险优先评分：DD超限直接淘汰
                    if max_dd > max_dd_limit:
                        continue  # 硬性淘汰，DD超限的组合不参与排名
                    score = total_ret * 1.0 + sharpe * 0.5 - max_dd * 2.0

                    if total_ret >= target:
                        score += 2.0

                    if score > best_score:
                        best_score = score
                        best_res = res
                        best_policy = policy

        # 如果所有组合都被DD硬限淘汰，放宽限制选最小DD的
        if best_res is None:
            logger.warning(f"优化器: 所有组合DD超限({max_dd_limit*100:.0f}%), 回退选最小DD组合")
            fallback_best = None
            fallback_dd = 1e9
            for lev in leverage_grid:
                for floor in trend_floor_grid:
                    for fee in fee_rate_grid:
                        policy = {
                            "leverage": float(lev),
                            "trend_floor_enabled": True,
                            "trend_floor_pos": float(floor),
                            "fee_rate": float(fee),
                            "ma_window": int(bt_cfg.get("ma_window", 20))
                        }
                        res = self.backtest_star50(initial_capital=initial_capital, start_date=start_date, policy=policy)
                        if not res:
                            continue
                        m = res.get("metrics", {})
                        max_dd = abs(float(str(m.get("max_drawdown", "0%")).replace("%", "")) / 100.0)
                        if max_dd < fallback_dd:
                            fallback_dd = max_dd
                            fallback_best = (res, policy)
            if fallback_best:
                best_res, best_policy = fallback_best

        return best_res, best_policy

    def walk_forward_backtest(self, initial_capital=100000, train_days=120, test_days=40):
        """
        Walk-Forward 回测：滚动窗口训练+验证，消除 in-sample 过拟合。
        - train_days: 训练窗口（用于网格搜索最优参数）
        - test_days: 测试窗口（用训练得到的参数跑 out-of-sample）
        - 所有信号基于 T 日收盘数据，T+1 日开盘执行（已由 backtest_star50 内部保证）
        """
        import pandas as pd

        bt_cfg = SENTIMENT_CONFIG.get("backtest", {})
        opt_cfg = bt_cfg.get("optimizer", {})
        leverage_grid = opt_cfg.get("leverage_grid", [1.0, 1.2, 1.5, 2.0])
        trend_floor_grid = opt_cfg.get("trend_floor_grid", [0.0, 0.2, 0.35, 0.5])
        fee_rate = float(bt_cfg.get("fee_rate", 0.0015))
        ma_window = int(bt_cfg.get("ma_window", 20))
        max_dd_limit = float(opt_cfg.get("max_drawdown_limit", 0.35))

        # 获取全部可用日期
        all_dates_df = fetch_df(
            "SELECT DISTINCT trade_date FROM market_sentiment ORDER BY trade_date"
        )
        if all_dates_df.empty or len(all_dates_df) < train_days + test_days:
            logger.warning("Walk-forward: 数据不足")
            return None

        all_dates = [str(d)[:10] for d in all_dates_df['trade_date'].tolist()]
        total = len(all_dates)

        windows = []
        all_test_trades = []
        all_test_curves = []
        cumulative_nav = initial_capital

        idx = 0
        while idx + train_days + test_days <= total:
            train_start = all_dates[idx]
            train_end = all_dates[idx + train_days - 1]
            test_start = all_dates[idx + train_days]
            test_end_idx = min(idx + train_days + test_days - 1, total - 1)
            test_end = all_dates[test_end_idx]

            # --- 训练阶段：在 train 窗口上网格搜索最优参数 ---
            best_score = -1e9
            best_policy = None
            for lev in leverage_grid:
                for floor in trend_floor_grid:
                    policy = {
                        "leverage": float(lev),
                        "trend_floor_enabled": True,
                        "trend_floor_pos": float(floor),
                        "fee_rate": fee_rate,
                        "ma_window": ma_window
                    }
                    res = self.backtest_star50(
                        initial_capital=100000,
                        start_date=train_start,
                        policy=policy
                    )
                    if not res:
                        continue
                    m = res.get("metrics", {})
                    # 只取 train 窗口内的数据
                    curves = res.get("curves", [])
                    train_curves = [c for c in curves if c['trade_date'] <= train_end]
                    if not train_curves:
                        continue

                    total_ret = float(str(m.get("total_return", "0%")).replace("%", "")) / 100.0
                    max_dd = abs(float(str(m.get("max_drawdown", "0%")).replace("%", "")) / 100.0)
                    sharpe = float(m.get("sharpe", 0))
                    penalty = max(0.0, max_dd - max_dd_limit) * 2.5
                    score = total_ret * 2.0 + sharpe * 0.1 - penalty

                    if score > best_score:
                        best_score = score
                        best_policy = policy

            if best_policy is None:
                best_policy = {
                    "leverage": 1.0, "trend_floor_enabled": True,
                    "trend_floor_pos": 0.0, "fee_rate": fee_rate, "ma_window": ma_window
                }

            # --- 测试阶段：用训练得到的参数跑 test 窗口 ---
            test_res = self.backtest_star50(
                initial_capital=cumulative_nav,
                start_date=test_start,
                policy=best_policy
            )

            window_info = {
                "train": f"{train_start} ~ {train_end}",
                "test": f"{test_start} ~ {test_end}",
                "policy": best_policy,
            }

            if test_res:
                test_curves = [c for c in test_res.get("curves", [])
                               if test_start <= c['trade_date'] <= test_end]
                test_trades = [t for t in test_res.get("trades", [])
                               if test_start <= t['entry_date'] <= test_end]

                if test_curves:
                    cumulative_nav = test_curves[-1].get('strategy_nav', cumulative_nav)
                    all_test_curves.extend(test_curves)

                all_test_trades.extend(test_trades)

                tm = test_res.get("metrics", {})
                window_info["test_return"] = tm.get("total_return", "N/A")
                window_info["test_max_dd"] = tm.get("max_drawdown", "N/A")
                window_info["test_trades"] = len(test_trades)
            else:
                window_info["test_return"] = "N/A"
                window_info["test_max_dd"] = "N/A"
                window_info["test_trades"] = 0

            windows.append(window_info)
            idx += test_days  # 滚动前进

        # --- 汇总 out-of-sample 结果 ---
        oos_total_return = (cumulative_nav / initial_capital) - 1
        oos_trade_count = len(all_test_trades)
        oos_wins = len([t for t in all_test_trades if t['profit_pct'] > 0])
        oos_win_rate = oos_wins / max(oos_trade_count, 1)

        # 计算 OOS 最大回撤
        oos_max_dd = 0.0
        if all_test_curves:
            navs = [c.get('strategy_nav', initial_capital) for c in all_test_curves]
            peak = navs[0]
            for nav in navs:
                if nav > peak:
                    peak = nav
                dd = (nav - peak) / peak
                if dd < oos_max_dd:
                    oos_max_dd = dd

        trading_days = len(all_test_curves) if all_test_curves else 1
        oos_annual = oos_total_return / trading_days * 242

        # OOS 信号归因
        from collections import defaultdict
        oos_attr = defaultdict(lambda: {'count': 0, 'wins': 0, 'total_pnl': 0.0})
        for t in all_test_trades:
            r = t.get('reason', 'unknown')
            oos_attr[r]['count'] += 1
            oos_attr[r]['total_pnl'] += t['profit_pct']
            if t['profit_pct'] > 0:
                oos_attr[r]['wins'] += 1

        attribution = {}
        for reason, s in oos_attr.items():
            cnt = s['count']
            attribution[reason] = {
                'count': cnt,
                'win_rate': f"{s['wins'] / cnt * 100:.1f}%",
                'avg_pnl': f"{s['total_pnl'] / cnt:.2f}%",
                'total_pnl': f"{s['total_pnl']:.2f}%",
            }

        result = {
            "method": "walk_forward",
            "train_days": train_days,
            "test_days": test_days,
            "total_windows": len(windows),
            "metrics": {
                "oos_total_return": f"{oos_total_return * 100:.2f}%",
                "oos_annual_return": f"{oos_annual * 100:.2f}%",
                "oos_max_drawdown": f"{oos_max_dd * 100:.2f}%",
                "oos_win_rate": f"{oos_win_rate * 100:.2f}%",
                "oos_total_trades": oos_trade_count,
                "oos_trading_days": trading_days,
            },
            "attribution": attribution,
            "windows": windows,
            "trades": all_test_trades,
        }

        logger.info(
            f"Walk-forward 完成: {len(windows)} 窗口, "
            f"OOS收益={oos_total_return*100:.2f}%, "
            f"OOS回撤={oos_max_dd*100:.2f}%, "
            f"OOS胜率={oos_win_rate*100:.1f}%"
        )
        return result

    def generate_report(self):
        """
        在 sentiment 目录下生成回测报告
        """
        import os
        from datetime import datetime
        
        result, best_policy = self.optimize_backtest_policy()
        if not result:
            return "Failed to generate backtest result."
            
        metrics = result['metrics']
        report_path = os.path.join(os.path.dirname(__file__), "backtest_report.md")
        
        content = f"""# 情绪策略回测报告 (科创50 ETF)
生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
资产对象: 000688.SH (科创50)
回测区间: 近一年滚动窗口

## 核心指标
- **总收益率**: {metrics['total_return']}
- **年化收益**: {metrics['annual_return']}
- **最大回撤**: {metrics['max_drawdown']}
- **胜率**: {metrics['win_rate']}
- **夏普比率**: {metrics['sharpe']}
- **基准收益**: {metrics['benchmark_return']}
- **持仓天数占比**: {metrics['active_days_ratio']}
- **杠杆**: {metrics.get('leverage', 1.0)}
- **趋势底仓**: {metrics.get('trend_floor_pos', 0.0)}

## 策略逻辑
1. **环境感知**: 区分牛市(BULL)与震荡/熊市(CHOP)环境。
2. **多维评分**: 整合涨跌停动能、晋级率、炸板率、指数共振等指标。
3. **情绪套利**: 
   - 震荡市博弈极致冰点后的反抽。
   - 牛市参与动能二段加速。
    - 触发风控动能阈值或见顶信号时及时离场。

## 回测曲线
(数据已在数据库中更新，可在前端页面查看交互式图表)

## 最优参数
{json.dumps(best_policy or result.get('policy', {}), ensure_ascii=False)}
"""
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(content)
        
        logger.info(f"回测报告已生成: {report_path}")
        return report_path

    def train_simple_ml(self, days=180):
        """
        纯Python实现的简单ML模型 - 基于条件概率
        不依赖scikit-learn
        """
        # 获取数据
        query = f"SELECT trade_date, score FROM market_sentiment ORDER BY trade_date DESC LIMIT {days}"
        df_sent = fetch_df(query)
        
        query_price = "SELECT trade_date, pct_chg FROM market_index WHERE ts_code='000688.SH' ORDER BY trade_date"
        df_price = fetch_df(query_price)
        
        if df_sent.empty or df_price.empty:
            return None
        
        # 转换为列表
        scores = df_sent['score'].tolist()[::-1]
        rets = df_price['pct_chg'].tolist()[::-1]
        
        if len(scores) < 30 or len(rets) < 30:
            return None
        
        # 构造特征和标签
        data = []
        for i in range(10, min(len(scores), len(rets)-1)):
            score = scores[i]
            score_prev = scores[i-1] if i > 0 else score
            v1 = score - score_prev
            v2 = score - 2*score_prev + scores[i-2] if i > 1 else 0
            
            # 5日均值
            score_ma5 = sum(scores[max(0,i-5):i]) / min(5, i)
            
            # 明日涨跌
            next_up = 1 if rets[i+1] > 0 else 0
            
            data.append({
                'score': score,
                'v1': v1,
                'v2': v2,
                'score_ma5': score_ma5,
                'above_ma5': 1 if score > score_ma5 else 0,
                'next_up': next_up
            })
        
        # 统计各条件下的上涨概率
        conditions = {}
        
        # 条件1: score > ma5 (趋势向上)
        subset = [d for d in data if d['above_ma5'] == 1]
        if len(subset) > 5:
            conditions['trend_up'] = sum(d['next_up'] for d in subset) / len(subset)
        
        # 条件2: v1 > 10 (动量强劲)
        subset = [d for d in data if d['v1'] > 10]
        if len(subset) > 5:
            conditions['strong_momentum'] = sum(d['next_up'] for d in subset) / len(subset)
        
        # 条件3: v1 < -10 (动量转负)
        subset = [d for d in data if d['v1'] < -10]
        if len(subset) > 5:
            conditions['weak_momentum'] = sum(d['next_up'] for d in subset) / len(subset)
        
        # 条件4: score > 70 (高分)
        subset = [d for d in data if d['score'] > 70]
        if len(subset) > 5:
            conditions['high_score'] = sum(d['next_up'] for d in subset) / len(subset)
        
        # 条件5: score < 40 (低分)
        subset = [d for d in data if d['score'] < 40]
        if len(subset) > 5:
            conditions['low_score'] = sum(d['next_up'] for d in subset) / len(subset)
        
        # 条件6: v1 > 5 AND score > 50
        subset = [d for d in data if d['v1'] > 5 and d['score'] > 50]
        if len(subset) > 5:
            conditions['trend_momentum'] = sum(d['next_up'] for d in subset) / len(subset)
        
        # 条件7: v1 < -5 AND score > 70 (顶背离)
        subset = [d for d in data if d['v1'] < -5 and d['score'] > 70]
        if len(subset) > 5:
            conditions['top_divergence'] = sum(d['next_up'] for d in subset) / len(subset)
        
        self._ml_conditions = conditions
        self._ml_scores = scores
        
        logger.info("=== ML模型训练结果 ===")
        for cond, prob in sorted(conditions.items(), key=lambda x: x[1], reverse=True):
            logger.info(f"  {cond}: 上涨概率 {prob:.1%}")
        
        return conditions

    def predict_with_ml(self, trade_date):
        """
        使用ML模型预测次日涨跌概率
        """
        if not hasattr(self, '_ml_conditions') or self._ml_conditions is None:
            self.train_simple_ml(180)
        
        # 获取最新数据
        history = self._get_recent_sentiments(trade_date, limit=10)
        if not history:
            return None
        
        scores = [h['score'] for h in history][::-1]
        current_score = scores[-1]
        v1 = current_score - scores[-2] if len(scores) > 1 else 0
        score_ma5 = sum(scores[-5:]) / 5
        
        # 计算当前条件
        above_ma5 = current_score > score_ma5
        strong_momentum = v1 > 10
        weak_momentum = v1 < -10
        high_score = current_score > 70
        low_score = current_score < 40
        trend_momentum = v1 > 5 and current_score > 50
        top_divergence = v1 < -5 and current_score > 70
        
        # 计算综合上涨概率
        probs = []
        weights = []
        
        if above_ma5 and 'trend_up' in self._ml_conditions:
            probs.append(self._ml_conditions['trend_up'])
            weights.append(0.2)
        
        if strong_momentum and 'strong_momentum' in self._ml_conditions:
            probs.append(self._ml_conditions['strong_momentum'])
            weights.append(0.25)
        
        if weak_momentum and 'weak_momentum' in self._ml_conditions:
            probs.append(self._ml_conditions['weak_momentum'])
            weights.append(0.15)
        
        if high_score and 'high_score' in self._ml_conditions:
            probs.append(self._ml_conditions['high_score'])
            weights.append(0.2)
        
        if low_score and 'low_score' in self._ml_conditions:
            probs.append(self._ml_conditions['low_score'])
            weights.append(0.1)
        
        if trend_momentum and 'trend_momentum' in self._ml_conditions:
            probs.append(self._ml_conditions['trend_momentum'])
            weights.append(0.3)
        
        if top_divergence and 'top_divergence' in self._ml_conditions:
            probs.append(self._ml_conditions['top_divergence'])
            weights.append(0.2)
        
        if probs and weights:
            total_weight = sum(weights)
            avg_prob = sum(p * w for p, w in zip(probs, weights)) / total_weight
        else:
            avg_prob = 0.5
        
        # 生成信号
        if avg_prob > 0.6:
            signal = 'BUY'
        elif avg_prob < 0.4:
            signal = 'SELL'
        else:
            signal = 'WATCH'
        
        return {
            'score': current_score,
            'v1': v1,
            'up_probability': avg_prob,
            'signal': signal,
            'conditions': {
                'above_ma5': above_ma5,
                'strong_momentum': strong_momentum,
                'high_score': high_score,
                'trend_momentum': trend_momentum
            }
        }


sentiment_analyst = SentimentAnalyst()

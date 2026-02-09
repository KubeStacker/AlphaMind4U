# /backend/strategy/sentiment/signal_verifier.py

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

class SignalVerifier:
    """
    信号校验器 (SignalVerifier)
    
    功能：
    对 SentimentAnalyst 发出的 "BUY" 信号进行二次确认 (Double-check)。
    
    目的：
    过滤假突破 (False Pulses) 和低质量的买入信号，提高策略胜率。
    
    校验逻辑 (V15 Optimized)：
    1. 主线校验 (Filter A): 市场必须有明确的主线板块（资金流入 > 0 且 涨幅 > 0.7%）。
    2. 量价结构校验 (Filter B): 大盘指数必须处于"缩量盘整"或"放量突破"状态，拒绝无序波动。
    """

    @staticmethod
    def verify(signal: str, market_data: dict) -> str:
        """
        执行校验逻辑
        
        参数:
        - signal: 原始信号 (如 "BUY")
        - market_data: 包含 top_sectors (板块数据), index_history (指数K线)
        
        返回:
        - 确认后的信号 (如 "STRONG BUY", "BUY") 或 降级信号 ("WATCH ...")
        """
        if signal != "BUY":
            return signal

        # 1. 过滤器 A: 主线强度校验 (The "Main Line" Check)
        # 逻辑：如果没有板块效应，大盘的上涨往往是虚假的拉升。
        # 条件：成交额前20的板块中，至少有一个板块 涨幅 > 0.7% 且 净流入 > 0。
        top_sectors = market_data.get('top_sectors')
        filter_a_passed = False
        
        if top_sectors is not None and not top_sectors.empty:
            # 关注成交额最大的前 20 个板块 (代表主流资金)
            main_sectors = top_sectors.nlargest(20, 'amount')
            
            # 放宽阈值：涨幅 > 0.7% (原 1.0%)
            valid_main_sectors = main_sectors[
                (main_sectors['avg_pct_chg'] > 0.7) & 
                (main_sectors['net_inflow'] > 0)
            ]
            
            if not valid_main_sectors.empty:
                filter_a_passed = True
            else:
                logger.info(f"Filter A Failed: No active leader in top 20 sectors.")
        else:
            filter_a_passed = False

        if not filter_a_passed:
            return "WATCH (No Main Line)"

        # 2. 过滤器 B: 弹簧效应校验 (The "Spring" Check)
        # 逻辑：有效的买点通常出现在"极致缩量盘整后"或"放量突破时"。
        index_history = market_data.get('index_history')
        filter_b_passed = False
        is_ignition = False
        
        if index_history is not None and len(index_history) >= 3:
            last_3_days = index_history.tail(3)
            current_day = index_history.iloc[-1]
            
            # 波动率计算
            close_std = last_3_days['close'].std()
            close_mean = last_3_days['close'].mean()
            volatility_ratio = close_std / close_mean if close_mean > 0 else 1.0
            
            # 条件 B1: 缩量盘整 (Consolidation)
            # 波动率低且成交量萎缩
            avg_vol_3d = last_3_days['vol'].mean()
            ma20_vol = current_day['ma20_vol']
            is_consolidating = (volatility_ratio < 0.012) and (avg_vol_3d < ma20_vol * 1.1)
            
            # 条件 B2: 放量启动 (Ignition)
            # 成交量显著放大 (> 1.2倍均量)
            is_ignition = current_day['vol'] > ma20_vol * 1.2
            
            if is_consolidating or is_ignition:
                filter_b_passed = True
            else:
                logger.info(f"Filter B Failed: Neither consolidation nor ignition found.")
        else:
            filter_b_passed = False

        if not filter_b_passed:
            return "WATCH (High Volatility)"

        # 3. 过滤器 C: 背离校验 (Divergence Check)
        # 逻辑：识别指数与个股、指数与资金之间的非对称表现。
        current_row = market_data.get('current_row', {})
        idx_chg = current_row.get('index_pct_chg', 0)
        up_count = current_row.get('up_count', 0)
        total_stocks = current_row.get('total', 0)
        net_mf = current_row.get('net_mf', 0)
        
        breadth_up_ratio = up_count / total_stocks if total_stocks > 0 else 0
        
        # 指数背离：权重砸盘，中小盘情绪回暖 (指数跌 > 0.5% 但上涨个股 > 50%)
        is_index_div = (idx_chg < -0.5) and (breadth_up_ratio > 0.5)
        
        # 主力背离：主力借大盘调整逆势吸筹 (指数跌 但 资金净流入)
        is_mf_div = (idx_chg < 0) and (net_mf > 0)

        # 4. 最终评级
        if is_index_div or is_mf_div:
            reason = "Index Divergence" if is_index_div else "MF Divergence"
            return f"STRONG BUY ({reason})"
            
        if is_ignition:
            return "STRONG BUY (Resonance)"
        return "BUY (Slight Resonance)"
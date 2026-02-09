import pandas as pd
import numpy as np
import json
import os
import logging
from db.connection import fetch_df

logger = logging.getLogger(__name__)

def run_backtest():
    """
    V31 自适应双模回测 (Adaptive Dual-Mode Backtest)
    
    逻辑映射：
    1. 进攻信号(买入/做多) -> 满仓 (1.0)
    2. 撤退信号(止盈/清仓) -> 空仓 (0.0)
    3. 牛市持仓(趋势锁仓) -> 满仓 (1.0) - 牛市默认在车上
    4. 熊市观望(观望)     -> 空仓 (0.0) - 熊市默认空仓
    """
    try:
        initial_capital = 100000
        
        query_sentiment = "SELECT trade_date, label FROM market_sentiment ORDER BY trade_date"
        df_sent = fetch_df(query_sentiment)
        
        query_price = "SELECT trade_date, open, close, pre_close, pct_chg FROM market_index WHERE ts_code='000688.SH' ORDER BY trade_date"
        df_price = fetch_df(query_price)
        
        if df_sent.empty or df_price.empty:
            return None

        df_sent['trade_date'] = pd.to_datetime(df_sent['trade_date'])
        df_price['trade_date'] = pd.to_datetime(df_price['trade_date'])
        
        df = pd.merge(df_price, df_sent, on='trade_date', how='left')
        df = df.dropna(subset=['close', 'pct_chg'])
        df = df.sort_values('trade_date').reset_index(drop=True)
        df = df[df['trade_date'] >= '2025-01-01'].reset_index(drop=True)
        if df.empty: return None
            
        # V31 Label Mapping
        # 1. 明确的多头信号
        long_keywords = ["反核博弈", "狙击买入", "积极做多", "趋势锁仓"]
        # 2. 明确的空头信号
        short_keywords = ["趋势止盈", "清仓离场", "观望"]
        
        df['label'] = df['label'].astype(str).str.strip()
        
        target_positions = []
        for _, row in df.iterrows():
            label = row['label']
            if any(kw in label for kw in long_keywords):
                target_positions.append(1.0)
            else:
                target_positions.append(0.0)
            
        df['target_pos'] = target_positions
        
        # Vectorized Backtest
        df['bench_ret'] = df['pct_chg'] / 100.0
        df['pos_held'] = df['target_pos'].shift(1).fillna(0.0)
        df['strat_ret_gross'] = df['bench_ret'] * df['pos_held']
        df['pos_change'] = (df['target_pos'] - df['target_pos'].shift(1).fillna(0.0)).abs()
        df['cost'] = df['pos_change'] * 0.0015
        df['strat_ret_net'] = df['strat_ret_gross'] - df['cost']
        
        df['strategy_nav'] = (1 + df['strat_ret_net']).cumprod()
        df['benchmark_nav'] = (1 + df['bench_ret']).cumprod()
        df['strategy_value'] = (df['strategy_nav'] * initial_capital).round(2)
        df['benchmark_value'] = (df['benchmark_nav'] * initial_capital).round(2)
        
        total_return = df['strategy_nav'].iloc[-1] - 1
        bench_return = df['benchmark_nav'].iloc[-1] - 1
        max_drawdown = (df['strategy_nav'] / df['strategy_nav'].cummax() - 1).min()
        active_days = df[df['pos_held'] > 0]
        win_rate = len(active_days[active_days['bench_ret'] > 0]) / len(active_days) if len(active_days) > 0 else 0

        df = df.fillna(0)
        result = {
            "metrics": {
                "total_return": f"{total_return*100:.1f}%",
                "max_drawdown": f"{max_drawdown*100:.1f}%",
                "win_rate": f"{win_rate*100:.1f}%",
                "benchmark_return": f"{bench_return*100:.1f}%",
                "final_value": f"¥{df['strategy_value'].iloc[-1]:,.2f}"
            },
            "curves": df.assign(trade_date=df['trade_date'].dt.strftime('%Y-%m-%d'))
                       .rename(columns={'strategy_value': 'strategy_nav', 'benchmark_value': 'benchmark_nav', 'pos_held': 'position'})
                       [['trade_date', 'strategy_nav', 'benchmark_nav', 'position']]
                       .to_dict('records')
        }
        return result
    except Exception as e:
        logger.error(f"Backtest error: {e}")
        return None

if __name__ == "__main__":
    run_backtest()

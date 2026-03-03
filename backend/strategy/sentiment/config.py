# /backend/strategy/sentiment/config.py

"""
Market Sentiment Strategy Configurations (V33)
包含情绪分值计算、多维动能阈值以及不同市场环境下的交易指令参数。
"""

SENTIMENT_CONFIG = {
    # --- 评分因子权重 (Continuous Score Weights) ---
    "weights": {
        "limit_diff": 0.25,     
        "promotion": 75.0,      # 高赚钱效应权重
        "broken": 20.0,         
        "index_chg": 12.0,      # 强化指数共振
        "repair": 1.5,
        "breadth": 32.0,        # 市场广度 (上涨家数占比)
        "turnover_activity": 8.0,  # 成交活跃度(成交额/20日均值)
        "margin_delta": 120.0,  # 融资余额5日变化率
        "net_mf_ratio": 18.0,   # 资金净流入占比
        "new_high_low": 6.0,    # 新高/新低结构强弱
        "board_height": 2.0,    # 连板高度
        "iv_proxy": 5.5         # 波动恐慌代理 (指数历史波动z)
    },

    # --- 环境感知阈值 (Market Context) ---
    "context": {
        "ma_short": 5,
        "ma_mid": 10,           
        "ma_long": 25           
    },

    # --- 风控参数 (Risk Control) ---
    "risk": {
        "crash_exp_ret_threshold": -1.0,  
        "panic_v1_threshold": -10.0       
    },

    # --- 简化版买入信号参数 (更宽松) ---
    "buy": {
        "ice_point_max_score": 35,          # 冰点买入阈值(提高)
        "ice_point_v1_threshold": 0,        # 降低要求
        "normal_buy_min_score": 25,        # 正常买入最低分
        "rebound_v2_threshold": 1.0,       # 冰点反转需要加速度转正
        "breakout_score": 58,              # 趋势突破分数阈值
        "momentum_v1_threshold": 5.0,      # 动量买入一阶导阈值
        "momentum_v2_threshold": 0.5,      # 动量买入二阶导阈值
    },

    # --- 卖出信号参数 ---
    "sell": {
        "euphoria_threshold": 85,          # 人声鼎沸
        "profit_take": 8.0,               # 止盈线(简化)
        "stop_loss": -5.0,               # 止损
        "cooldown_score_drop": 20,         # 情绪回落多少分就卖
        "max_hold_days": 20,              # 最大持仓天数
        "momentum_reversal_v1": -3.0,      # 动量反转触发减仓/离场
        "momentum_reversal_v2": -1.5,      # 加速度恶化阈值
        "trailing_profit_floor": 4.0,      # 浮盈保护启动阈值
        "trailing_pullback": 3.5,          # 从峰值回撤超过该值离场
    },

    # --- 牛市模式参数 ---
    "bull": {
        "buy_min_score": 20,              # 牛市买入门槛更低
        "buy_min_v1": -2.0,               # 牛市允许轻微回撤时继续参与
    },

    # --- 震荡/熊市模式参数 ---
    "chop": {
        "buy_min_score": 25,
        "range_high_score": 62,           # 震荡上沿分数
        "range_low_score": 32,            # 震荡下沿分数
    },

    # --- 情绪动量控制参数 ---
    "momentum": {
        "buy_score_above_ma5": True,      # 买入需站上情绪MA5
        "buy_min_breadth": 0.45,          # 买入时最小上涨家数占比
        "buy_neglect_days": 1,            # 连续低迷天数(无人问津)阈值
        "buy_neglect_score": 42,          # 无人问津分数上限
        "buy_neglect_breadth_max": 0.45,  # 无人问津期市场广度上限
        "sell_divergence_score": 78,      # 背离卖出触发分数
        "sell_high_days_warning": 4,      # 高位持续天数预警
        "sell_euphoria_persist_days": 4,  # 持续鼎沸天数阈值
        "sell_euphoria_score": 82,        # 鼎沸分数阈值
        "avoid_chase_score": 92,          # 禁止追高阈值
        "score_z_top": 1.2,               # 情绪Z分数高位阈值
    },

    # --- 回测与优化参数 ---
    "backtest": {
        "default_days": 365,
        "fee_rate": 0.0015,
        "leverage": 1.0,
        "trend_floor_enabled": True,      # 牛市趋势下维持底仓
        "trend_floor_pos": 0.35,          # 默认趋势底仓
        "ma_window": 20,                  # 趋势判定均线
        "optimizer": {
            "enabled": True,
            "target_total_return": 1.0,   # 100%
            "leverage_grid": [1.0, 1.2, 1.5, 2.0, 2.5],
            "trend_floor_grid": [0.0, 0.2, 0.35, 0.5, 0.7],
            "fee_rate_grid": [0.0015],
            "max_drawdown_limit": 0.35
        }
    }
}

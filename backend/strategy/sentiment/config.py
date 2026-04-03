# /backend/strategy/sentiment/config.py

"""
Market Sentiment Strategy Configurations - 简化版
只保留情绪分值计算权重。
"""

SENTIMENT_CONFIG = {
    # --- 评分因子权重 ---
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
    "live_monitor": {
        "dashboard_refresh_seconds": 60,
        "closed_refresh_seconds": 300,
        "live_cache_seconds": 45,
        "macro_refresh_seconds": 600,
        "cnbc_ten_year_warn_high": 4.38,
        "cnbc_ten_year_risk_high": 4.40,
        "cnbc_ten_year_warn_low": 4.33,
        "cnbc_ten_year_support_low": 4.30,
        "pizza_spike_warn": 150.0,
        "pizza_spike_risk": 300.0,
    },
}

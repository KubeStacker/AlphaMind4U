# /backend/strategy/sentiment/config.py

"""
Market Sentiment Strategy Configurations
"""

SENTIMENT_CONFIG = {
    # --- 标签阈值（统一配置源） ---
    "labels": [
        {"min_score": 85, "label": "沸腾", "position": "≤30%", "strategy": "减仓防守，不追高"},
        {"min_score": 70, "label": "高热", "position": "50-70%", "strategy": "持有龙头，不开新仓"},
        {"min_score": 55, "label": "修复", "position": "50-70%", "strategy": "试探建仓，跟随主线"},
        {"min_score": 42, "label": "拉锯", "position": "30-50%", "strategy": "控制仓位，做T为主"},
        {"min_score": 25, "label": "低温", "position": "≤30%", "strategy": "防守为主，等待信号"},
        {"min_score": 0,  "label": "冰点", "position": "0-10%", "strategy": "空仓等待反转"},
    ],

    # --- 评分因子权重 ---
    # 注：这些是缩放系数而非归一化权重。每个因子先计算原始值，
    # 乘以系数后直接加到50分基准上，最后clip到0-100。
    "weights": {
        "limit_diff": 0.25,         # 涨跌停差值（个数 → 分数）
        "promotion": 75.0,          # 涨停晋级率（比例 → 分数）
        "broken": 20.0,             # 炸板数（个数 → 分数，反向）
        "index_chg": 12.0,          # 指数共振（涨跌幅 → 分数）
        "repair": 1.5,              # 修复力度（个数 → 分数）
        "breadth": 32.0,            # 市场广度（百分位 → 分数）
        "turnover_activity": 8.0,   # 成交活跃度（百分位 → 分数）
        "margin_delta": 120.0,      # 融资余额5日变化率（百分位 → 分数）
        "net_mf_ratio": 18.0,       # 资金净流入占比（百分位 → 分数）
        "new_high_low": 6.0,        # 新高/新低结构（百分位 → 分数）
        "board_height": 2.0,        # 连板高度（百分位 → 分数）
        "iv_proxy": 5.5,            # 波动恐慌代理（百分位 → 分数，反向）
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


def score_to_label(score: float) -> str:
    """统一标签映射函数，前后端共用"""
    if score >= 85:
        return "沸腾"
    if score >= 70:
        return "高热"
    if score >= 55:
        return "修复"
    if score >= 42:
        return "拉锯"
    if score >= 25:
        return "低温"
    return "冰点"


def get_label_info(score: float) -> dict:
    """获取标签完整信息（含仓位建议和操作策略）"""
    for item in reversed(SENTIMENT_CONFIG["labels"]):
        if score >= item["min_score"]:
            return {
                "label": item["label"],
                "position": item["position"],
                "strategy": item["strategy"],
            }
    return SENTIMENT_CONFIG["labels"][0]

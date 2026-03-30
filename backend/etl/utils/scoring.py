# /backend/etl/utils/scoring.py

"""
主线龙头评分模块

核心逻辑：
选股 = 主线板块 × 板块内强势 × 突破形态 × 资金确认 × 盈亏比

一票否决条件：
1. 不属于主线板块
2. 盈亏比 < 2:1
3. 大盘趋势向下且情绪冰点
"""

import logging
import json
from typing import Dict, Any, List, Optional, Tuple
from .dynamic_thresholds import get_dynamic_thresholds

logger = logging.getLogger(__name__)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _is_beijing_stock(ts_code: str) -> bool:
    return str(ts_code or "").upper().endswith(".BJ")


def calc_mainline_leader_score(
    stock: Dict[str, Any],
    market_env: Dict[str, Any],
    sector_stocks: List[Dict[str, Any]]
) -> Tuple[float, str, Dict[str, float]]:
    """
    主线龙头综合评分
    
    Args:
        stock: 股票数据，包含factors, moneyflow等
        market_env: 市场环境数据
        sector_stocks: 同板块其他股票数据
    
    Returns:
        (score, reason, scores): 评分、推荐理由、各维度分数
    """
    
    # 一票否决检查
    reject_reason = check_reject_conditions(stock, market_env, sector_stocks)
    if reject_reason:
        return 0, reject_reason, {}
    
    # 各因子评分
    scores = {}
    
    # 1. 板块共振
    scores['sector_resonance'] = calc_sector_resonance(stock, sector_stocks)
    
    # 2. 趋势先行
    scores['trend_leadership'] = calc_trend_leadership_score(stock, sector_stocks)

    # 3. 形态突破
    scores['breakout'] = calc_breakout_score(stock)
    
    # 4. 资金确认
    scores['capital_flow'] = calc_flow_score(stock)
    
    # 5. 板块内地位
    scores['sector_rank'] = calc_sector_rank_score(stock, sector_stocks)
    
    # 6. 热度与量价配合
    scores['volume_match'] = calc_volume_match_score(stock)

    # 7. 题材命中
    scores['theme_fit'] = calc_theme_fit_score(stock)
    
    # 8. 市场环境
    scores['market_env'] = calc_market_env_score(market_env)
    
    # 权重
    weights = {
        'sector_resonance': 0.16,
        'trend_leadership': 0.22,
        'breakout': 0.10,
        'capital_flow': 0.20,
        'sector_rank': 0.14,
        'volume_match': 0.12,
        'theme_fit': 0.10,
        'market_env': 0.06,
    }
    
    # 计算总分
    total = sum(scores.get(k, 0) * weights[k] for k in weights)
    
    # 生成推荐理由
    reason = generate_reason(stock, scores, total)
    
    return round(total, 1), reason, scores


def check_reject_conditions(
    stock: Dict[str, Any],
    market_env: Dict[str, Any],
    sector_stocks: List[Dict[str, Any]]
) -> Optional[str]:
    """
    一票否决条件检查
    
    Returns:
        拒绝原因，如果通过则返回None
    """
    
    if _is_beijing_stock(stock.get('ts_code', '')):
        return "北交所标的不参与主线龙头评分"

    # 1. 非主线板块
    if not stock.get('is_mainline', False):
        return "非主线板块"
    
    # 2. 盈亏比不足
    risk_reward = calc_risk_reward(stock)
    trend_signature = (
        int(stock.get('recent_active_days', 0)) >= 2
        or int(stock.get('strong_streak', 0)) >= 2
        or int(stock.get('active_days', 0)) >= 3
        or int(stock.get('limit_ups_10d', 0)) >= 1
    )
    if risk_reward < 1.0:
        return f"盈亏比不足 ({risk_reward:.1f} < 1.0)"
    if risk_reward < 1.4 and not trend_signature:
        return f"盈亏比不足 ({risk_reward:.1f} < 1.4)"
    
    # 3. 市场环境恶劣
    if market_env.get('trend') == 'down' and market_env.get('sentiment', 50) < 30:
        return "市场环境恶劣 (下跌趋势+情绪冰点)"
    
    # 4. 板块样本不足
    if len(sector_stocks) < 5:
        return "板块样本不足"
    
    return None


def calc_sector_resonance(stock: Dict[str, Any], sector_stocks: List[Dict[str, Any]]) -> float:
    """
    板块共振评分
    
    板块共振 = 板块内多只股票同时走强
    """
    if not sector_stocks:
        return 0
    
    total_count = len(sector_stocks)
    
    # 板块内涨幅>5%的股票占比
    strong_count = len([s for s in sector_stocks if s.get('pct_chg', 0) > 5])
    strong_ratio = strong_count / total_count
    
    # 板块内资金净流入股票占比
    inflow_count = len([s for s in sector_stocks if s.get('net_mf_amount', 0) > 0])
    inflow_ratio = inflow_count / total_count
    
    # 板块平均涨幅
    avg_pct = sum(s.get('pct_chg', 0) for s in sector_stocks) / total_count
    
    # 计算分数
    score = (strong_ratio * 40 + inflow_ratio * 40 + min(max(avg_pct, 0), 5) * 4)
    
    return min(max(score, 0), 100)


def calc_trend_leadership_score(stock: Dict[str, Any], sector_stocks: List[Dict[str, Any]]) -> float:
    """
    趋势先行评分

    核心不是只看单日涨幅，而是看近阶段是否持续走强、是否先于同板块启动。
    """
    active_days = int(stock.get('active_days', 0))
    recent_active_days = int(stock.get('recent_active_days', 0))
    strong_streak = int(stock.get('strong_streak', 0))
    pioneer_score = _safe_float(stock.get('trend_pioneer_score', 0.0), 0.0)
    latest_pct = _safe_float(stock.get('pct_chg', 0.0), 0.0)
    limit_ups_10d = int(stock.get('limit_ups_10d', 0))

    score = 0.0

    if active_days >= 5:
        score += 28
    elif active_days >= 4:
        score += 22
    elif active_days >= 3:
        score += 16
    elif active_days >= 2:
        score += 10
    elif active_days >= 1:
        score += 5

    if recent_active_days >= 3:
        score += 24
    elif recent_active_days >= 2:
        score += 18
    elif recent_active_days >= 1:
        score += 10

    if strong_streak >= 3:
        score += 18
    elif strong_streak >= 2:
        score += 12
    elif strong_streak >= 1:
        score += 6

    if limit_ups_10d >= 2:
        score += 14
    elif limit_ups_10d >= 1:
        score += 8

    score += min(max(pioneer_score, 0.0), 100.0) * 0.16
    score += min(max(latest_pct, 0.0), 12.0) * 0.8

    return min(max(score, 0), 100)


def calc_breakout_score(stock: Dict[str, Any]) -> float:
    """
    形态突破评分
    
    检查：
    1. 价格突破MA20/60
    2. MACD金叉
    3. 成交量放大确认
    """
    score = 0
    factors = stock.get('factors', {})
    close = stock.get('close', 0)
    volume_ratio = stock.get('volume_ratio', 1.0)
    
    ma20 = factors.get('ma20', 0)
    ma60 = factors.get('ma60', 0)
    ma5 = factors.get('ma5', 0)
    ma10 = factors.get('ma10', 0)
    
    # 1. 价格突破均线
    if close > ma5 > ma10 > ma20 > 0:
        score += 45  # 短中期趋势共振
    elif close > ma60 > 0:
        score += 40  # 站上MA60
    elif close > ma20 > 0:
        score += 25  # 站上MA20
    
    # 2. 量价配合 (放量确认突破)
    if volume_ratio >= 1.5:
        score += 30  # 放量
    elif volume_ratio >= 1.2:
        score += 20  # 温和放量
    
    # 3. MACD状态 (从stock数据中获取，如果有的话)
    macd_signal = stock.get('macd_signal', '')
    if macd_signal == '金叉':
        score += 30
    elif macd_signal == 'DIF在上':
        score += 15
    
    return min(score, 100)


def calc_flow_score(stock: Dict[str, Any]) -> float:
    """
    资金流向评分
    
    检查：
    1. 连续净流入天数
    2. 累计净流入金额
    3. 大单占比
    """
    score = 0
    
    # 连续净流入天数
    continuous_days = int(stock.get('flow_positive_streak', stock.get('flow_continuous_days', 0)))
    if continuous_days >= 5:
        score += 40
    elif continuous_days >= 3:
        score += 30
    elif continuous_days >= 1:
        score += 15
    
    positive_days = int(stock.get('positive_flow_days', 0))
    if positive_days >= 6:
        score += 18
    elif positive_days >= 4:
        score += 12
    elif positive_days >= 2:
        score += 6

    # 累计净流入金额 (万元)
    total_inflow = _safe_float(stock.get('flow_total_inflow', 0), 0.0)
    if total_inflow > 100000:  # >1亿
        score += 25
    elif total_inflow > 50000:  # >5000万
        score += 18
    elif total_inflow > 10000:  # >1000万
        score += 10

    inflow_ratio = _safe_float(stock.get('flow_inflow_ratio', 0.0), 0.0)
    if inflow_ratio >= 0.04:
        score += 20
    elif inflow_ratio >= 0.02:
        score += 14
    elif inflow_ratio > 0:
        score += 8

    latest_net = _safe_float(stock.get('latest_net_mf_amount', stock.get('net_mf_amount', 0.0)), 0.0)
    if latest_net > 0:
        score += 8
    
    # 大单占比
    big_order_ratio = _safe_float(stock.get('big_order_ratio', 0), 0.0)
    if big_order_ratio > 0.4:
        score += 14
    elif big_order_ratio > 0.25:
        score += 9
    elif big_order_ratio > 0.1:
        score += 5
    
    return min(score, 100)


def calc_sector_position_value(stock: Dict[str, Any]) -> float:
    """
    板块内龙头位置值

    用趋势先行、资金、热度、题材命中构建复合位置，避免退化成单日涨幅榜。
    """
    trend = calc_trend_leadership_score(stock, [])
    flow = calc_flow_score(stock)
    heat = calc_volume_match_score(stock)
    theme = calc_theme_fit_score(stock)
    breakout = calc_breakout_score(stock)

    latest_pct = min(max(_safe_float(stock.get('pct_chg', 0.0), 0.0), 0.0), 12.0)
    latest_bonus = latest_pct * 1.2

    return (
        trend * 0.34
        + flow * 0.24
        + heat * 0.18
        + theme * 0.14
        + breakout * 0.10
        + latest_bonus
    )


def calc_sector_rank_score(stock: Dict[str, Any], sector_stocks: List[Dict[str, Any]]) -> float:
    """
    板块内排名评分
    
    在所属板块中的涨幅/资金排名
    """
    if not sector_stocks:
        return 50  # 默认中性分
    
    # 按复合龙头位置排序，而不是按单日涨幅排序
    sorted_by_position = sorted(
        sector_stocks,
        key=lambda item: (
            calc_sector_position_value(item),
            _safe_float(item.get('pct_chg', 0), 0.0),
            _safe_float(item.get('amount', 0), 0.0),
        ),
        reverse=True,
    )
    
    # 找到当前股票排名
    stock_code = stock.get('ts_code', '')
    rank = next((i + 1 for i, s in enumerate(sorted_by_position) if s.get('ts_code') == stock_code), len(sorted_by_position))
    
    total = len(sorted_by_position)
    rank_ratio = rank / total
    
    # 排名越靠前分数越高
    if rank_ratio <= 0.1:  # 前10%
        return 100
    elif rank_ratio <= 0.2:  # 前20%
        return 85
    elif rank_ratio <= 0.3:  # 前30%
        return 70
    elif rank_ratio <= 0.5:  # 前50%
        return 50
    else:
        return 30


def calc_volume_match_score(stock: Dict[str, Any]) -> float:
    """
    热度与量价配合评分

    检查：
    1. 涨幅配合成交量
    2. 换手率适中
    """
    score = 0
    pct_chg = stock.get('pct_chg', 0)
    volume_ratio = stock.get('volume_ratio', 1.0)
    turnover_rate = stock.get('turnover_rate', 0)
    amount_rank_pct = _safe_float(stock.get('amount_rank_pct', 0.0), 0.0)
    total_amount_rank_pct = _safe_float(stock.get('total_amount_rank_pct', 0.0), 0.0)
    recent_active_days = int(stock.get('recent_active_days', 0))
    
    # 获取动态阈值
    total_mv = stock.get('total_mv', 0)
    thresholds = get_dynamic_thresholds(total_mv)
    
    # 1. 量价配合
    if pct_chg > 0 and volume_ratio > 1.2:
        score += 50  # 放量上涨
    elif pct_chg > 0 and volume_ratio > 1.0:
        score += 35  # 温和上涨
    elif pct_chg < 0 and volume_ratio < 0.8:
        score += 25  # 缩量下跌 (相对健康)
    
    # 2. 换手率适中
    turnover_min = thresholds.get('turnover_rate_min', 2.0)
    if turnover_rate >= turnover_min:
        score += 25  # 满足最低换手率
    
    # 3. 成交额充足
    amount = stock.get('amount', 0)
    amount_min = thresholds.get('amount_min', 50000)
    if amount >= amount_min:
        score += 25

    if amount_rank_pct >= 0.9:
        score += 18
    elif amount_rank_pct >= 0.75:
        score += 12
    elif amount_rank_pct >= 0.6:
        score += 6

    if total_amount_rank_pct >= 0.9:
        score += 10
    elif total_amount_rank_pct >= 0.75:
        score += 6

    if recent_active_days >= 2:
        score += 8
    
    return min(score, 100)


def calc_theme_fit_score(stock: Dict[str, Any]) -> float:
    """
    题材命中评分

    更强调是否真正命中当前主线的细分驱动，而不是只挂一个宽泛行业。
    """
    hit_count = int(stock.get('theme_hit_count', 0))
    hit_names = stock.get('theme_hit_names') or []
    score = 0.0

    if hit_count >= 3:
        score += 80
    elif hit_count == 2:
        score += 65
    elif hit_count == 1:
        score += 45
    else:
        score += 20 if stock.get('mapped_sector') else 0

    if hit_names:
        score += min(len(hit_names), 3) * 6

    return min(score, 100)


def calc_market_env_score(market_env: Dict[str, Any]) -> float:
    """
    市场环境评分
    
    检查：
    1. 大盘趋势
    2. 市场情绪
    """
    score = 50  # 基础分
    
    # 大盘趋势
    trend = market_env.get('trend', 'neutral')
    if trend == 'up':
        score += 30
    elif trend == 'down':
        score -= 30
    
    # 市场情绪
    sentiment = market_env.get('sentiment', 50)
    if 40 <= sentiment <= 80:
        score += 20  # 情绪适中
    elif sentiment > 85:
        score -= 20  # 过热
    elif sentiment < 30:
        score -= 30  # 过冷
    
    return min(max(score, 0), 100)


def calc_risk_reward(stock: Dict[str, Any]) -> float:
    """
    盈亏比计算
    
    盈亏比 = (目标价 - 买入价) / (买入价 - 止损价)
    """
    close = stock.get('close', 0)
    factors = stock.get('factors', {})
    high_250 = factors.get('high_250', close * 1.1)
    ma20 = factors.get('ma20', close * 0.95)
    
    if close <= 0:
        return 0
    
    # 止损价：MA20下方2%或当前价下方5%
    stop_loss = min(ma20 * 0.98, close * 0.95)
    
    # 目标价：前高或上涨10%
    target = min(high_250, close * 1.10)
    
    # 风险和收益
    risk = close - stop_loss
    reward = target - close
    
    if risk <= 0:
        return 0
    
    return round(reward / risk, 2)


def calc_entry_stop_target(stock: Dict[str, Any]) -> Dict[str, Any]:
    """
    计算买入区间、止损价、目标价
    """
    close = stock.get('close', 0)
    factors = stock.get('factors', {})
    high_250 = factors.get('high_250', close * 1.1)
    ma20 = factors.get('ma20', close * 0.95)
    ma60 = factors.get('ma60', close * 0.90)
    
    # 买入区间：当前价附近
    entry_low = round(close * 0.99, 2)
    entry_high = round(close * 1.01, 2)
    
    # 止损价：MA20下方2%或当前价下方5%
    stop_loss = round(min(ma20 * 0.98, close * 0.95), 2)
    
    # 目标价：前高或上涨10%
    target = round(min(high_250, close * 1.10), 2)
    
    # 盈亏比
    risk = close - stop_loss
    reward = target - close
    risk_reward = round(reward / risk, 2) if risk > 0 else 0
    
    # 最大亏损和目标收益百分比
    max_loss_pct = round((stop_loss - close) / close * 100, 2)
    target_gain_pct = round((target - close) / close * 100, 2)
    
    return {
        'entry_zone': [entry_low, entry_high],
        'stop_loss': stop_loss,
        'target': target,
        'risk_reward': risk_reward,
        'max_loss_pct': max_loss_pct,
        'target_gain_pct': target_gain_pct
    }


def generate_reason(stock: Dict[str, Any], scores: Dict[str, float], total: float) -> str:
    """
    生成推荐理由（精简版，用于快速展示）
    """
    reasons = []
    
    if scores.get('trend_leadership', 0) >= 70:
        reasons.append("趋势先行")

    if scores.get('sector_resonance', 0) >= 70:
        reasons.append("板块共振")
    
    if scores.get('breakout', 0) >= 70:
        reasons.append("突破形态")
    
    if scores.get('capital_flow', 0) >= 70:
        reasons.append("资金追踪")
    
    if scores.get('sector_rank', 0) >= 70:
        reasons.append("龙头地位")
    
    if scores.get('volume_match', 0) >= 70:
        reasons.append("热度充足")

    if scores.get('theme_fit', 0) >= 60:
        reasons.append("题材命中")
    
    risk_reward = calc_risk_reward(stock)
    if risk_reward >= 2.5:
        reasons.append(f"盈亏比{risk_reward}")
    
    if not reasons:
        reasons.append("综合评分")
    
    return "+".join(reasons)


def generate_detailed_reason(stock: Dict[str, Any], scores: Dict[str, float], total: float, sector_rank: int) -> Dict[str, Any]:
    """
    生成详细的入选原因说明
    
    Returns:
        {
            "summary": "一句话总结",
            "details": ["详细原因列表"],
            "tier": "龙一/龙二/龙三/跟风",
            "tier_label": "梯队标签",
            "strategy": "推荐操作策略",
            "advantages": ["核心优势"],
        }
    """
    details = []
    advantages = []
    name = stock.get('name', '')
    
    # ---- 趋势先行维度 ----
    active_days = int(stock.get('active_days', 0))
    recent_active_days = int(stock.get('recent_active_days', 0))
    strong_streak = int(stock.get('strong_streak', 0))
    limit_ups_10d = int(stock.get('limit_ups_10d', 0))
    pioneer_score = _safe_float(stock.get('trend_pioneer_score', 0.0), 0.0)
    avg_pct_10d = _safe_float(stock.get('avg_pct', 0.0), 0.0)
    max_pct_10d = _safe_float(stock.get('max_pct', 0.0), 0.0)
    
    trend_parts = []
    if scores.get('trend_leadership', 0) >= 50:
        if active_days >= 5:
            trend_parts.append(f"10日内{active_days}日强势")
        elif active_days >= 3:
            trend_parts.append(f"近10日{active_days}日走强")
        if strong_streak >= 3:
            trend_parts.append(f"连续{strong_streak}日强势")
        elif strong_streak >= 2:
            trend_parts.append(f"已连续{strong_streak}日")
        if recent_active_days >= 2:
            trend_parts.append(f"近3日{recent_active_days}日活跃")
        if limit_ups_10d >= 2:
            trend_parts.append(f"10日内{limit_ups_10d}次涨停")
        elif limit_ups_10d >= 1:
            trend_parts.append("含涨停板")
        if pioneer_score >= 70:
            trend_parts.append("先于板块启动")
        elif pioneer_score >= 50:
            trend_parts.append("较早启动")
        if trend_parts:
            details.append("趋势先行：" + "，".join(trend_parts))
            if strong_streak >= 3 or (active_days >= 4 and recent_active_days >= 2):
                advantages.append("趋势最稳")

    # ---- 资金追踪维度 ----
    continuous_days = int(stock.get('flow_positive_streak', stock.get('flow_continuous_days', 0)))
    positive_flow_days = int(stock.get('positive_flow_days', 0))
    total_inflow = _safe_float(stock.get('flow_total_inflow', 0), 0.0)
    latest_net = _safe_float(stock.get('latest_net_mf_amount', stock.get('net_mf_amount', 0.0)), 0.0)
    
    flow_parts = []
    if scores.get('capital_flow', 0) >= 40:
        if continuous_days >= 3:
            flow_parts.append(f"连续{continuous_days}日资金净流入")
        if positive_flow_days >= 5:
            flow_parts.append(f"10日内{positive_flow_days}日获资金流入")
        if total_inflow > 50000:
            flow_parts.append(f"累计流入{total_inflow/10000:.1f}亿")
        elif total_inflow > 10000:
            flow_parts.append(f"累计流入{total_inflow:.0f}万")
        if latest_net > 0:
            flow_parts.append("当日资金净流入")
        if flow_parts:
            details.append("资金追踪：" + "，".join(flow_parts))
            if continuous_days >= 3 and total_inflow > 50000:
                advantages.append("资金持续涌入")

    # ---- 板块内地位维度 ----
    if sector_rank == 1:
        details.append(f"板块内综合排名第1（龙一地位）")
        advantages.append("板块龙头")
    elif sector_rank <= 3:
        details.append(f"板块内综合排名第{sector_rank}")
    
    # ---- 涨幅维度 ----
    pct_chg = _safe_float(stock.get('pct_chg', 0), 0)
    if pct_chg > 0:
        if limit_ups_10d >= 1:
            details.append(f"10日均涨{avg_pct_10d:.1f}%，最高单日{max_pct_10d:.1f}%，含涨停")
        elif avg_pct_10d > 3:
            details.append(f"10日均涨{avg_pct_10d:.1f}%，最高单日{max_pct_10d:.1f}%")
    
    # ---- 量价配合维度 ----
    volume_ratio = _safe_float(stock.get('volume_ratio', 1.0), 1.0)
    turnover_rate = _safe_float(stock.get('turnover_rate', 0), 0)
    if volume_ratio >= 1.5 and pct_chg > 3:
        details.append(f"放量上涨（量比{volume_ratio:.1f}，换手{turnover_rate:.1f}%）")
    elif volume_ratio >= 1.2 and pct_chg > 0:
        details.append(f"温和放量（量比{volume_ratio:.1f}）")
    
    # ---- 题材命中维度 ----
    hit_names = stock.get('theme_hit_names') or []
    if hit_names:
        details.append(f"题材命中：{'/'.join(hit_names[:3])}")
    
    # ---- 梯队判定 ----
    tier, tier_label, strategy = _determine_tier(stock, scores, sector_rank, total)
    
    # ---- 一句话总结 ----
    top_reasons = []
    if advantages:
        top_reasons = advantages[:2]
    if details:
        # 提取各维度名称
        for d in details[:3]:
            if "：" in d:
                dim_name = d.split("：")[0]
                if dim_name not in top_reasons:
                    top_reasons.append(dim_name)
    summary = f"{name}（{tier}）：" + ("、".join(top_reasons[:2]) if top_reasons else f"综合评分{total:.0f}分")
    
    return {
        "summary": summary,
        "details": details,
        "tier": tier,
        "tier_label": tier_label,
        "strategy": strategy,
        "advantages": advantages,
    }


def _determine_tier(stock: Dict[str, Any], scores: Dict[str, float], sector_rank: int, total_score: float) -> Tuple[str, str, str]:
    """
    判断股票在板块中的梯队地位
    
    梯队规则：
    - 龙一：板块内排名前3 + 趋势最强 + 资金最强 + 高分
    - 龙二：板块内排名前5 + 趋势或资金突出 + 较高分
    - 龙三：板块内排名前10 + 综合评分达标
    - 跟风：板块内排名前20 + 有基本板块效应
    - 边缘：其他
    
    Returns:
        (tier, tier_label, strategy)
        tier: 龙一/龙二/龙三/跟风
        tier_label: 梯队标签描述
        strategy: 推荐操作策略
    """
    trend_score = scores.get('trend_leadership', 0)
    capital_score = scores.get('capital_flow', 0)
    sector_score = scores.get('sector_rank', 0)
    strong_streak = int(stock.get('strong_streak', 0))
    limit_ups_10d = int(stock.get('limit_ups_10d', 0))
    active_days = int(stock.get('active_days', 0))
    continuous_days = int(stock.get('flow_positive_streak', stock.get('flow_continuous_days', 0)))
    pioneer_score = _safe_float(stock.get('trend_pioneer_score', 0.0), 0.0)
    recent_active_days = int(stock.get('recent_active_days', 0))
    
    # 龙一条件：排名靠前 + 趋势最强 + 资金最强 + 领涨特质
    is_pioneer = pioneer_score >= 50 and (strong_streak >= 2 or limit_ups_10d >= 1)
    is_strongest_trend = trend_score >= 70 and active_days >= 4
    is_strongest_flow = capital_score >= 60 and continuous_days >= 2
    is_top_rank = sector_rank <= 3
    
    if is_top_rank and is_strongest_trend and is_strongest_flow and total_score >= 72:
        tier = "龙一"
        tier_label = "板块核心龙头"
        strategy = "核心标的地位确立，可在回调至5日线附近主动低吸，或突破前高后追涨；若板块持续活跃可加仓"
    elif sector_rank <= 5 and (is_strongest_trend or is_strongest_flow) and total_score >= 65:
        tier = "龙二"
        tier_label = "板块次龙头"
        strategy = "强势跟随龙一，适合在板块整体走强时介入；若龙一涨停可考虑低吸补涨"
    elif sector_rank <= 10 and total_score >= 60 and trend_score >= 55 and (active_days >= 3 or recent_active_days >= 2):
        tier = "龙三"
        tier_label = "板块补涨潜力"
        strategy = "存在补涨预期，适合板块全面走强时低吸布局；不宜追高，等缩量回调机会"
    elif sector_rank <= 20 and total_score >= 58 and trend_score >= 40:
        tier = "跟风"
        tier_label = "板块跟风"
        strategy = "跟随板块大势，波动较大；建议观望或小仓位试探，若板块转弱优先离场"
    else:
        tier = "边缘"
        tier_label = "板块边缘"
        strategy = "板块效应有限，不建议参与"
    
    return tier, tier_label, strategy


def get_tier_info(sector_rank: int, scores: Dict[str, float], stock: Dict[str, Any], total_score: float) -> Dict[str, Any]:
    """
    获取梯队信息（供API直接调用）
    """
    tier, tier_label, strategy = _determine_tier(stock, scores, sector_rank, total_score)
    return {
        "tier": tier,
        "tier_label": tier_label,
        "strategy": strategy,
    }


def get_signal_level(score: float) -> Dict[str, str]:
    """
    根据分数返回信号等级
    """
    if score >= 80:
        return {'level': '强势', 'action': '可买入', 'color': 'red'}
    elif score >= 60:
        return {'level': '中性偏多', 'action': '可持有', 'color': 'orange'}
    elif score >= 40:
        return {'level': '中性', 'action': '观望', 'color': 'gray'}
    elif score >= 20:
        return {'level': '偏弱', 'action': '减仓', 'color': 'blue'}
    else:
        return {'level': '弱势', 'action': '回避', 'color': 'green'}

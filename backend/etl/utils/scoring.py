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


def calc_mainline_leader_score(
    stock: Dict[str, Any],
    market_env: Dict[str, Any],
    sector_stocks: List[Dict[str, Any]]
) -> Tuple[float, str]:
    """
    主线龙头综合评分
    
    Args:
        stock: 股票数据，包含factors, moneyflow等
        market_env: 市场环境数据
        sector_stocks: 同板块其他股票数据
    
    Returns:
        (score, reason): 评分和推荐理由
    """
    
    # 一票否决检查
    reject_reason = check_reject_conditions(stock, market_env, sector_stocks)
    if reject_reason:
        return 0, reject_reason
    
    # 各因子评分
    scores = {}
    
    # 1. 板块共振 (20%)
    scores['sector_resonance'] = calc_sector_resonance(stock, sector_stocks)
    
    # 2. 形态突破 (20%)
    scores['breakout'] = calc_breakout_score(stock)
    
    # 3. 资金确认 (20%)
    scores['capital_flow'] = calc_flow_score(stock)
    
    # 4. 板块内排名 (15%)
    scores['sector_rank'] = calc_sector_rank_score(stock, sector_stocks)
    
    # 5. 量价配合 (15%)
    scores['volume_match'] = calc_volume_match_score(stock)
    
    # 6. 市场环境 (10%)
    scores['market_env'] = calc_market_env_score(market_env)
    
    # 权重
    weights = {
        'sector_resonance': 0.20,
        'breakout': 0.20,
        'capital_flow': 0.20,
        'sector_rank': 0.15,
        'volume_match': 0.15,
        'market_env': 0.10,
    }
    
    # 计算总分
    total = sum(scores.get(k, 0) * weights[k] for k in weights)
    
    # 生成推荐理由
    reason = generate_reason(stock, scores, total)
    
    return round(total, 1), reason


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
    
    # 1. 非主线板块
    if not stock.get('is_mainline', False):
        return "非主线板块"
    
    # 2. 盈亏比不足
    risk_reward = calc_risk_reward(stock)
    if risk_reward < 2.0:
        return f"盈亏比不足 ({risk_reward:.1f} < 2.0)"
    
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
    
    # 1. 价格突破均线
    if close > ma60 > 0:
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
    continuous_days = stock.get('flow_continuous_days', 0)
    if continuous_days >= 5:
        score += 40
    elif continuous_days >= 3:
        score += 30
    elif continuous_days >= 1:
        score += 15
    
    # 累计净流入金额 (万元)
    total_inflow = stock.get('flow_total_inflow', 0)
    if total_inflow > 100000:  # >1亿
        score += 35
    elif total_inflow > 50000:  # >5000万
        score += 25
    elif total_inflow > 10000:  # >1000万
        score += 15
    
    # 大单占比
    big_order_ratio = stock.get('big_order_ratio', 0)
    if big_order_ratio > 0.4:
        score += 25
    elif big_order_ratio > 0.25:
        score += 15
    elif big_order_ratio > 0.1:
        score += 8
    
    return min(score, 100)


def calc_sector_rank_score(stock: Dict[str, Any], sector_stocks: List[Dict[str, Any]]) -> float:
    """
    板块内排名评分
    
    在所属板块中的涨幅/资金排名
    """
    if not sector_stocks:
        return 50  # 默认中性分
    
    # 按涨幅排序
    sorted_by_pct = sorted(sector_stocks, key=lambda x: x.get('pct_chg', 0), reverse=True)
    
    # 找到当前股票排名
    stock_code = stock.get('ts_code', '')
    rank = next((i + 1 for i, s in enumerate(sorted_by_pct) if s.get('ts_code') == stock_code), len(sorted_by_pct))
    
    total = len(sorted_by_pct)
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
    量价配合评分
    
    检查：
    1. 涨幅配合成交量
    2. 换手率适中
    """
    score = 0
    pct_chg = stock.get('pct_chg', 0)
    volume_ratio = stock.get('volume_ratio', 1.0)
    turnover_rate = stock.get('turnover_rate', 0)
    
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
    生成推荐理由
    """
    reasons = []
    
    if scores.get('sector_resonance', 0) >= 70:
        reasons.append("板块共振")
    
    if scores.get('breakout', 0) >= 70:
        reasons.append("突破形态")
    
    if scores.get('capital_flow', 0) >= 70:
        reasons.append("资金确认")
    
    if scores.get('sector_rank', 0) >= 70:
        reasons.append("板块强势")
    
    if scores.get('volume_match', 0) >= 70:
        reasons.append("量价配合")
    
    risk_reward = calc_risk_reward(stock)
    if risk_reward >= 2.5:
        reasons.append(f"盈亏比{risk_reward}")
    
    if not reasons:
        reasons.append("综合评分")
    
    return "+".join(reasons)


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

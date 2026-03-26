# /backend/etl/utils/dynamic_thresholds.py

"""
动态阈值模块

根据市值、行业等因素动态调整选股条件的阈值

核心逻辑：
- 小盘股：换手率要求高，流动性敏感
- 中盘股：适中要求
- 大盘股：换手率要求低，机构主导
"""

from typing import Dict, Any


def get_dynamic_thresholds(total_mv: float) -> Dict[str, float]:
    """
    根据总市值返回动态阈值
    
    Args:
        total_mv: 总市值（万元）
    
    Returns:
        动态阈字典
    """
    if total_mv < 500000:  # <50亿
        return {
            'turnover_rate_min': 8.0,      # 换手率最低8%
            'turnover_rate_max': 25.0,     # 换手率最高25%（过高可能炒作）
            'volume_ratio_min': 1.8,       # 量比最低1.8
            'amount_min': 30000,           # 成交额最低3000万
            'market_cap_label': '小盘',
        }
    elif total_mv < 1000000:  # 50-100亿
        return {
            'turnover_rate_min': 5.0,
            'turnover_rate_max': 20.0,
            'volume_ratio_min': 1.5,
            'amount_min': 50000,           # 成交额最低5000万
            'market_cap_label': '小盘',
        }
    elif total_mv < 3000000:  # 100-300亿
        return {
            'turnover_rate_min': 3.0,
            'turnover_rate_max': 15.0,
            'volume_ratio_min': 1.3,
            'amount_min': 80000,           # 成交额最低8000万
            'market_cap_label': '中盘',
        }
    elif total_mv < 5000000:  # 300-500亿
        return {
            'turnover_rate_min': 2.0,
            'turnover_rate_max': 10.0,
            'volume_ratio_min': 1.2,
            'amount_min': 100000,          # 成交额最低1亿
            'market_cap_label': '中盘',
        }
    elif total_mv < 10000000:  # 500-1000亿
        return {
            'turnover_rate_min': 1.5,
            'turnover_rate_max': 8.0,
            'volume_ratio_min': 1.1,
            'amount_min': 200000,          # 成交额最低2亿
            'market_cap_label': '大盘',
        }
    else:  # >1000亿
        return {
            'turnover_rate_min': 0.8,
            'turnover_rate_max': 5.0,
            'volume_ratio_min': 1.0,
            'amount_min': 300000,          # 成交额最低3亿
            'market_cap_label': '大盘',
        }


def get_sector_thresholds(sector_type: str) -> Dict[str, float]:
    """
    根据板块类型返回动态阈值
    
    Args:
        sector_type: 板块类型 (tech/finance/consumer/energy/material/industrial)
    
    Returns:
        动态阈字典
    """
    thresholds = {
        'tech': {  # 科技板块
            'turnover_rate_multiplier': 1.3,   # 换手率要求更高
            'volume_ratio_multiplier': 1.2,
            'description': '科技股流动性好，换手率要求较高'
        },
        'finance': {  # 金融板块
            'turnover_rate_multiplier': 0.7,   # 换手率要求更低
            'volume_ratio_multiplier': 0.8,
            'description': '金融股权重大，机构主导，换手率较低'
        },
        'consumer': {  # 消费板块
            'turnover_rate_multiplier': 1.0,
            'volume_ratio_multiplier': 1.0,
            'description': '消费股换手率适中'
        },
        'energy': {  # 能源板块
            'turnover_rate_multiplier': 0.9,
            'volume_ratio_multiplier': 0.9,
            'description': '能源股波动大，但换手率适中'
        },
        'material': {  # 材料板块
            'turnover_rate_multiplier': 1.0,
            'volume_ratio_multiplier': 1.0,
            'description': '材料股换手率适中'
        },
        'industrial': {  # 工业板块
            'turnover_rate_multiplier': 1.0,
            'volume_ratio_multiplier': 1.0,
            'description': '工业股换手率适中'
        },
    }
    
    return thresholds.get(sector_type, thresholds['industrial'])


def adjust_thresholds_by_sector(
    base_thresholds: Dict[str, float],
    sector_type: str
) -> Dict[str, float]:
    """
    根据板块类型调整阈值
    
    Args:
        base_thresholds: 基础阈值
        sector_type: 板块类型
    
    Returns:
        调整后的阈值
    """
    sector_adjust = get_sector_thresholds(sector_type)
    
    adjusted = base_thresholds.copy()
    
    # 调整换手率
    adjusted['turnover_rate_min'] = base_thresholds['turnover_rate_min'] * sector_adjust['turnover_rate_multiplier']
    adjusted['turnover_rate_max'] = base_thresholds['turnover_rate_max'] * sector_adjust['turnover_rate_multiplier']
    
    # 调整量比
    adjusted['volume_ratio_min'] = base_thresholds['volume_ratio_min'] * sector_adjust['volume_ratio_multiplier']
    
    # 添加板块信息
    adjusted['sector_type'] = sector_type
    adjusted['sector_description'] = sector_adjust['description']
    
    return adjusted


def get_market_env_thresholds(market_trend: str, sentiment: float) -> Dict[str, Any]:
    """
    根据市场环境返回调整系数
    
    Args:
        market_trend: 市场趋势 (up/down/neutral)
        sentiment: 市场情绪 (0-100)
    
    Returns:
        市场环境调整系数
    """
    adjustments = {
        'score_multiplier': 1.0,      # 评分乘数
        'volume_ratio_adjust': 0.0,   # 量比调整
        'turnover_adjust': 0.0,       # 换手率调整
        'suggestion': '正常操作',
    }
    
    if market_trend == 'up':
        if sentiment >= 80:
            # 上涨趋势+情绪高涨
            adjustments['score_multiplier'] = 0.9  # 降低评分要求（更宽松）
            adjustments['volume_ratio_adjust'] = -0.2  # 降低量比要求
            adjustments['suggestion'] = '市场强势，可积极操作，但注意过热风险'
        elif sentiment >= 50:
            # 上涨趋势+情绪适中
            adjustments['score_multiplier'] = 1.0
            adjustments['suggestion'] = '市场健康上涨，正常操作'
        else:
            # 上涨趋势+情绪偏低
            adjustments['score_multiplier'] = 1.1  # 提高评分要求（更严格）
            adjustments['suggestion'] = '市场上涨但情绪谨慎，精选个股'
    
    elif market_trend == 'down':
        if sentiment <= 30:
            # 下跌趋势+情绪冰点
            adjustments['score_multiplier'] = 1.3  # 大幅提高评分要求
            adjustments['volume_ratio_adjust'] = 0.3  # 提高量比要求
            adjustments['suggestion'] = '市场弱势+情绪冰点，等待企稳'
        elif sentiment <= 50:
            # 下跌趋势+情绪偏低
            adjustments['score_multiplier'] = 1.2
            adjustments['suggestion'] = '市场下跌，谨慎操作，只做最强主线'
        else:
            # 下跌趋势+情绪偏高（背离）
            adjustments['score_multiplier'] = 1.1
            adjustments['suggestion'] = '情绪与趋势背离，注意风险'
    
    else:  # neutral
        if sentiment >= 70:
            adjustments['score_multiplier'] = 0.95
            adjustments['suggestion'] = '震荡市中情绪偏高，精选个股'
        elif sentiment <= 40:
            adjustments['score_multiplier'] = 1.1
            adjustments['suggestion'] = '震荡市中情绪偏低，观望为主'
        else:
            adjustments['suggestion'] = '震荡市，正常操作'
    
    return adjustments


def check_volume_quality(
    vol: float,
    vol_ma5: float,
    turnover_rate: float,
    total_mv: float,
    sector_type: str = 'industrial'
) -> Dict[str, Any]:
    """
    检查成交量质量
    
    Args:
        vol: 当日成交量
        vol_ma5: 5日平均成交量
        turnover_rate: 换手率
        total_mv: 总市值
        sector_type: 板块类型
    
    Returns:
        成交量质量检查结果
    """
    base_thresholds = get_dynamic_thresholds(total_mv)
    thresholds = adjust_thresholds_by_sector(base_thresholds, sector_type)
    
    # 计算量比
    volume_ratio = vol / vol_ma5 if vol_ma5 > 0 else 1.0
    
    result = {
        'volume_ratio': round(volume_ratio, 2),
        'turnover_rate': round(turnover_rate, 2),
        'thresholds': thresholds,
        'checks': {},
        'pass': True,
        'reasons': [],
    }
    
    # 检查量比
    volume_ratio_min = thresholds.get('volume_ratio_min', 1.2)
    if volume_ratio >= volume_ratio_min:
        result['checks']['volume_ratio'] = True
    else:
        result['checks']['volume_ratio'] = False
        result['pass'] = False
        result['reasons'].append(f'量比不足 ({volume_ratio:.2f} < {volume_ratio_min})')
    
    # 检查换手率
    turnover_min = thresholds.get('turnover_rate_min', 2.0)
    turnover_max = thresholds.get('turnover_rate_max', 15.0)
    
    if turnover_rate >= turnover_min:
        result['checks']['turnover_min'] = True
    else:
        result['checks']['turnover_min'] = False
        result['pass'] = False
        result['reasons'].append(f'换手率不足 ({turnover_rate:.2f}% < {turnover_min}%)')
    
    if turnover_rate <= turnover_max:
        result['checks']['turnover_max'] = True
    else:
        result['checks']['turnover_max'] = False
        result['pass'] = False
        result['reasons'].append(f'换手率过高 ({turnover_rate:.2f}% > {turnover_max}%)，可能炒作')
    
    return result

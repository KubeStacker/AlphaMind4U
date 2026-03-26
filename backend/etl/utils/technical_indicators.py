# /backend/etl/utils/technical_indicators.py

"""
技术指标计算模块

提供常用技术指标的向量化计算：
- 均线系统: MA5/10/20/60, 均线排列状态
- MACD: DIF, DEA, MACD柱, 金叉/死叉信号
- RSI: RSI6/12/24, 超买超卖区间
- KDJ: K, D, J值
- 布林带: 上轨/中轨/下轨, 带宽
- 成交量指标: 量比, 换手率, 放量/缩量
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Optional


def calculate_ma(df: pd.DataFrame) -> pd.DataFrame:
    """计算均线系统
    
    Args:
        df: 包含 'close' 列的DataFrame，按日期升序排列
    
    Returns:
        添加了 ma5, ma10, ma20, ma60 列的DataFrame
    """
    df = df.copy()
    df['ma5'] = df['close'].rolling(window=5, min_periods=1).mean()
    df['ma10'] = df['close'].rolling(window=10, min_periods=1).mean()
    df['ma20'] = df['close'].rolling(window=20, min_periods=1).mean()
    df['ma60'] = df['close'].rolling(window=60, min_periods=1).mean()
    return df


def get_ma_status(row: pd.Series) -> str:
    """判断均线排列状态
    
    Args:
        row: 包含 ma5, ma10, ma20, ma60 的Series
    
    Returns:
        均线状态: '多头排列', '空头排列', '缠绕'
    """
    ma5 = row.get('ma5', 0)
    ma10 = row.get('ma10', 0)
    ma20 = row.get('ma20', 0)
    ma60 = row.get('ma60', 0)
    
    if pd.isna(ma5) or pd.isna(ma10) or pd.isna(ma20) or pd.isna(ma60):
        return '数据不足'
    
    # 多头排列: MA5 > MA10 > MA20 > MA60
    if ma5 > ma10 > ma20 > ma60:
        return '多头排列'
    # 空头排列: MA5 < MA10 < MA20 < MA60
    elif ma5 < ma10 < ma20 < ma60:
        return '空头排列'
    else:
        return '缠绕'


def calculate_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    """计算MACD指标
    
    Args:
        df: 包含 'close' 列的DataFrame
        fast: 快线周期，默认12
        slow: 慢线周期，默认26
        signal: 信号线周期，默认9
    
    Returns:
        添加了 macd_dif, macd_dea, macd_bar 列的DataFrame
    """
    df = df.copy()
    
    # 计算EMA
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    
    # DIF = 快线EMA - 慢线EMA
    df['macd_dif'] = ema_fast - ema_slow
    
    # DEA = DIF的EMA
    df['macd_dea'] = df['macd_dif'].ewm(span=signal, adjust=False).mean()
    
    # MACD柱 = (DIF - DEA) * 2
    df['macd_bar'] = (df['macd_dif'] - df['macd_dea']) * 2
    
    return df


def get_macd_signal(row: pd.Series, prev_row: Optional[pd.Series] = None) -> str:
    """判断MACD信号
    
    Args:
        row: 当前行，包含 macd_dif, macd_dea
        prev_row: 前一行（可选，用于判断金叉死叉）
    
    Returns:
        MACD信号: '金叉', '死叉', '无信号'
    """
    dif = row.get('macd_dif', 0)
    dea = row.get('macd_dea', 0)
    
    if pd.isna(dif) or pd.isna(dea):
        return '数据不足'
    
    if prev_row is not None:
        prev_dif = prev_row.get('macd_dif', 0)
        prev_dea = prev_row.get('macd_dea', 0)
        
        if not pd.isna(prev_dif) and not pd.isna(prev_dea):
            # 金叉: DIF从下方穿越DEA
            if prev_dif <= prev_dea and dif > dea:
                return '金叉'
            # 死叉: DIF从上方穿越DEA
            elif prev_dif >= prev_dea and dif < dea:
                return '死叉'
    
    # 当前状态
    if dif > dea:
        return 'DIF在上'
    elif dif < dea:
        return 'DIF在下'
    else:
        return '无信号'


def calculate_rsi(df: pd.DataFrame, periods: list = [6, 12, 24]) -> pd.DataFrame:
    """计算RSI指标
    
    Args:
        df: 包含 'close' 列的DataFrame
        periods: RSI周期列表，默认[6, 12, 24]
    
    Returns:
        添加了 rsi6, rsi12, rsi24 列的DataFrame
    """
    df = df.copy()
    
    for period in periods:
        # 计算价格变化
        delta = df['close'].diff()
        
        # 分离上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # 计算平均上涨和平均下跌
        avg_gain = gain.rolling(window=period, min_periods=1).mean()
        avg_loss = loss.rolling(window=period, min_periods=1).mean()
        
        # 计算RS和RSI
        rs = avg_gain / avg_loss.replace(0, np.nan)
        rsi = 100 - (100 / (1 + rs))
        
        df[f'rsi{period}'] = rsi.fillna(50)  # 填充NaN为中性值50
    
    return df


def get_rsi_status(rsi_value: float) -> str:
    """判断RSI状态
    
    Args:
        rsi_value: RSI值
    
    Returns:
        RSI状态: '超买', '超卖', '中性'
    """
    if pd.isna(rsi_value):
        return '数据不足'
    if rsi_value >= 80:
        return '超买'
    elif rsi_value <= 20:
        return '超卖'
    elif rsi_value >= 70:
        return '偏强'
    elif rsi_value <= 30:
        return '偏弱'
    else:
        return '中性'


def calculate_kdj(df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
    """计算KDJ指标
    
    Args:
        df: 包含 'high', 'low', 'close' 列的DataFrame
        n: RSV周期，默认9
        m1: K值平滑因子，默认3
        m2: D值平滑因子，默认3
    
    Returns:
        添加了 kdj_k, kdj_d, kdj_j 列的DataFrame
    """
    df = df.copy()
    
    # 计算N日最高价和最低价
    low_n = df['low'].rolling(window=n, min_periods=1).min()
    high_n = df['high'].rolling(window=n, min_periods=1).max()
    
    # 计算RSV (未成熟随机值)
    rsv = (df['close'] - low_n) / (high_n - low_n).replace(0, np.nan) * 100
    rsv = rsv.fillna(50)
    
    # 计算K值 (RSV的M1日移动平均)
    df['kdj_k'] = rsv.ewm(span=m1, adjust=False).mean()
    
    # 计算D值 (K值的M2日移动平均)
    df['kdj_d'] = df['kdj_k'].ewm(span=m2, adjust=False).mean()
    
    # 计算J值 (3K - 2D)
    df['kdj_j'] = 3 * df['kdj_k'] - 2 * df['kdj_d']
    
    return df


def calculate_bollinger(df: pd.DataFrame, period: int = 20, std_dev: float = 2.0) -> pd.DataFrame:
    """计算布林带
    
    Args:
        df: 包含 'close' 列的DataFrame
        period: 中轨周期，默认20
        std_dev: 标准差倍数，默认2.0
    
    Returns:
        添加了 boll_upper, boll_mid, boll_lower 列的DataFrame
    """
    df = df.copy()
    
    # 中轨 = MA20
    df['boll_mid'] = df['close'].rolling(window=period, min_periods=1).mean()
    
    # 标准差
    std = df['close'].rolling(window=period, min_periods=1).std()
    
    # 上轨 = 中轨 + 2倍标准差
    df['boll_upper'] = df['boll_mid'] + std_dev * std
    
    # 下轨 = 中轨 - 2倍标准差
    df['boll_lower'] = df['boll_mid'] - std_dev * std
    
    return df


def get_bollinger_position(close: float, upper: float, mid: float, lower: float) -> str:
    """判断价格在布林带中的位置
    
    Args:
        close: 收盘价
        upper: 上轨
        mid: 中轨
        lower: 下轨
    
    Returns:
        布林带位置: '上轨附近', '中轨上方', '中轨下方', '下轨附近'
    """
    if pd.isna(close) or pd.isna(upper) or pd.isna(mid) or pd.isna(lower):
        return '数据不足'
    
    band_width = upper - lower
    if band_width <= 0:
        return '数据异常'
    
    position = (close - lower) / band_width
    
    if position >= 0.8:
        return '上轨附近'
    elif position >= 0.5:
        return '中轨上方'
    elif position >= 0.2:
        return '中轨下方'
    else:
        return '下轨附近'


def calculate_volume_indicators(df: pd.DataFrame, vol_periods: list = [5, 10, 20]) -> pd.DataFrame:
    """计算成交量指标
    
    Args:
        df: 包含 'vol', 'amount' 列的DataFrame
        vol_periods: 成交量均线周期列表
    
    Returns:
        添加了成交量相关指标的DataFrame
    """
    df = df.copy()
    
    # 成交量均线
    for period in vol_periods:
        df[f'vol_ma{period}'] = df['vol'].rolling(window=period, min_periods=1).mean()
    
    # 量比 = 当日成交量 / 过去5日平均成交量
    df['volume_ratio'] = df['vol'] / df['vol_ma5'].replace(0, np.nan)
    df['volume_ratio'] = df['volume_ratio'].fillna(1.0)
    
    return df


def get_volume_status(row: pd.Series) -> str:
    """判断成交量状态
    
    Args:
        row: 包含 vol, vol_ma5 的Series
    
    Returns:
        成交量状态: '放量', '温和', '缩量', '极度缩量'
    """
    vol = row.get('vol', 0)
    vol_ma5 = row.get('vol_ma5', 0)
    
    if pd.isna(vol) or pd.isna(vol_ma5) or vol_ma5 <= 0:
        return '数据不足'
    
    ratio = vol / vol_ma5
    
    if ratio >= 2.0:
        return '放量'
    elif ratio >= 1.2:
        return '温和放量'
    elif ratio >= 0.8:
        return '温和'
    elif ratio >= 0.5:
        return '缩量'
    else:
        return '极度缩量'


def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """计算所有技术指标
    
    Args:
        df: 包含 'open', 'high', 'low', 'close', 'vol', 'amount' 列的DataFrame
            必须按日期升序排列
    
    Returns:
        添加了所有技术指标的DataFrame
    """
    # 均线系统
    df = calculate_ma(df)
    
    # MACD
    df = calculate_macd(df)
    
    # RSI
    df = calculate_rsi(df)
    
    # KDJ
    df = calculate_kdj(df)
    
    # 布林带
    df = calculate_bollinger(df)
    
    # 成交量指标
    df = calculate_volume_indicators(df)
    
    return df


def get_indicators_summary(df: pd.DataFrame) -> Dict[str, Any]:
    """获取技术指标摘要（最新一行）
    
    Args:
        df: 包含所有技术指标的DataFrame，按日期升序排列
    
    Returns:
        技术指标摘要字典
    """
    if df.empty:
        return {}
    
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else None
    
    # 均线状态
    ma_status = get_ma_status(latest)
    
    # MACD信号
    macd_signal = get_macd_signal(latest, prev)
    
    # RSI状态
    rsi_status = get_rsi_status(latest.get('rsi6', 50))
    
    # 布林带位置
    boll_position = get_bollinger_position(
        latest.get('close', 0),
        latest.get('boll_upper', 0),
        latest.get('boll_mid', 0),
        latest.get('boll_lower', 0)
    )
    
    # 成交量状态
    volume_status = get_volume_status(latest)
    
    # 生成技术信号列表
    signals = []
    
    # 均线信号
    if ma_status == '多头排列':
        signals.append({
            'type': '趋势',
            'desc': '均线多头排列',
            'strength': '强'
        })
    
    # MACD信号
    if macd_signal == '金叉':
        signals.append({
            'type': '动量',
            'desc': 'MACD金叉',
            'strength': '中'
        })
    elif macd_signal == '死叉':
        signals.append({
            'type': '动量',
            'desc': 'MACD死叉',
            'strength': '中'
        })
    
    # RSI信号
    if rsi_status == '超买':
        signals.append({
            'type': '超买超卖',
            'desc': 'RSI超买区域',
            'strength': '警告'
        })
    elif rsi_status == '超卖':
        signals.append({
            'type': '超买超卖',
            'desc': 'RSI超卖区域',
            'strength': '机会'
        })
    
    # 布林带信号
    if boll_position == '上轨附近':
        signals.append({
            'type': '波动',
            'desc': '触及布林上轨',
            'strength': '注意'
        })
    elif boll_position == '下轨附近':
        signals.append({
            'type': '波动',
            'desc': '触及布林下轨',
            'strength': '关注'
        })
    
    # 成交量信号
    if volume_status == '放量':
        price_change = latest.get('pct_chg', 0)
        if price_change > 0:
            signals.append({
                'type': '量价',
                'desc': '放量上涨',
                'strength': '强'
            })
        elif price_change < 0:
            signals.append({
                'type': '量价',
                'desc': '放量下跌',
                'strength': '警告'
            })
    
    return {
        'trade_date': str(latest.get('trade_date', '')),
        'close': float(latest.get('close', 0)),
        'ma': {
            'ma5': round(float(latest.get('ma5', 0)), 2),
            'ma10': round(float(latest.get('ma10', 0)), 2),
            'ma20': round(float(latest.get('ma20', 0)), 2),
            'ma60': round(float(latest.get('ma60', 0)), 2),
            'status': ma_status
        },
        'macd': {
            'dif': round(float(latest.get('macd_dif', 0)), 4),
            'dea': round(float(latest.get('macd_dea', 0)), 4),
            'bar': round(float(latest.get('macd_bar', 0)), 4),
            'signal': macd_signal
        },
        'rsi': {
            'rsi6': round(float(latest.get('rsi6', 50)), 1),
            'rsi12': round(float(latest.get('rsi12', 50)), 1),
            'rsi24': round(float(latest.get('rsi24', 50)), 1),
            'status': rsi_status
        },
        'kdj': {
            'k': round(float(latest.get('kdj_k', 50)), 1),
            'd': round(float(latest.get('kdj_d', 50)), 1),
            'j': round(float(latest.get('kdj_j', 50)), 1)
        },
        'bollinger': {
            'upper': round(float(latest.get('boll_upper', 0)), 2),
            'mid': round(float(latest.get('boll_mid', 0)), 2),
            'lower': round(float(latest.get('boll_lower', 0)), 2),
            'position': boll_position
        },
        'volume': {
            'vol': float(latest.get('vol', 0)),
            'vol_ma5': round(float(latest.get('vol_ma5', 0)), 0),
            'volume_ratio': round(float(latest.get('volume_ratio', 1)), 2),
            'status': volume_status
        },
        'signals': signals
    }

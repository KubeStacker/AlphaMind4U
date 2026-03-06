"""
K线形态识别工具库 (K-line Pattern Recognition Utility) v2.0

高性能、向量化的K线形态识别，适用于A股市场技术分析。
所有计算采用 Pandas/Numpy 向量化操作，禁止 for 循环遍历行。

输入: DataFrame 含 open, high, low, close, vol(或volume) 列，按时间升序
输出: 原 DataFrame 新增信号列，值为 0.0~1.0 置信度（1.0=标准形态, 0.5=疑似）

注意: 严格避免"未来函数"——所有判断仅使用当前及历史数据。
"""

import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

# ============================================================================
# 私有辅助: 基础K线特征（向量化）
# ============================================================================

def _ensure_volume_col(df: pd.DataFrame) -> pd.DataFrame:
    """统一 volume 列名: vol -> volume"""
    if 'volume' not in df.columns and 'vol' in df.columns:
        df = df.rename(columns={'vol': 'volume'})
    return df


def _body_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算K线实体和影线基础特征（向量化），新增列:
      body_top, body_bottom, body_size, upper_shadow, lower_shadow,
      total_range, body_ratio, is_bullish
    """
    df = df.copy()
    df = _ensure_volume_col(df)

    df['body_top'] = np.maximum(df['open'], df['close'])
    df['body_bottom'] = np.minimum(df['open'], df['close'])
    df['body_size'] = df['body_top'] - df['body_bottom']
    df['upper_shadow'] = df['high'] - df['body_top']
    df['lower_shadow'] = df['body_bottom'] - df['low']
    df['total_range'] = df['high'] - df['low']
    # 实体占总长比例，避免除零
    df['body_ratio'] = np.where(
        df['total_range'] > 1e-9,
        df['body_size'] / df['total_range'],
        0.0
    )
    df['is_bullish'] = (df['close'] >= df['open']).astype(int)
    return df


def _volume_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算量能辅助特征（向量化），新增列:
      vol_ma5, vol_ma10, vol_ratio_5（当日量/5日均量）
    """
    df['vol_ma5'] = df['volume'].rolling(5, min_periods=1).mean()
    df['vol_ma10'] = df['volume'].rolling(10, min_periods=1).mean()
    df['vol_ratio_5'] = np.where(
        df['vol_ma5'] > 1e-9,
        df['volume'] / df['vol_ma5'],
        1.0
    )
    return df


def _ma_features(df: pd.DataFrame) -> pd.DataFrame:
    """计算均线特征，新增 ma5, ma10, ma20, ma60"""
    for p in [5, 10, 20, 60]:
        col = f'ma{p}'
        if col not in df.columns:
            df[col] = df['close'].rolling(p, min_periods=p).mean()
    return df


def _trend_context(df: pd.DataFrame, lookback: int = 5) -> pd.DataFrame:
    """
    判断近期趋势方向（向量化），新增列:
      trend_up: 近N日收盘均值 > 前N日收盘均值 (上涨趋势)
      trend_down: 反之
    """
    recent_ma = df['close'].rolling(lookback, min_periods=lookback).mean()
    prev_ma = recent_ma.shift(lookback)
    df['trend_up'] = (recent_ma > prev_ma).astype(int)
    df['trend_down'] = (recent_ma < prev_ma).astype(int)
    return df


# ============================================================================
# 公开API: 形态识别函数（全部向量化）
# ============================================================================

def detect_hammer_hanging(df: pd.DataFrame) -> pd.DataFrame:
    """
    锤子线 (Hammer) 与上吊线 (Hanging Man)

    定义:
      - 实体极小: body_ratio < 0.30
      - 下影线极长: lower_shadow > 2 * body_size
      - 上影线极短: upper_shadow < 0.1 * total_range
    区分:
      - 下跌趋势末端 -> BULLISH_HAMMER (看涨)
      - 上涨趋势末端 -> HANGING_MAN (看跌)

    置信度:
      1.0 = 标准形态 (下影>3倍实体, 趋势明确)
      0.5 = 疑似形态 (基本满足但不完美)
    """
    df = _body_features(df)
    df = _trend_context(df)

    small_body = df['body_ratio'] < 0.30
    long_lower = df['lower_shadow'] > 2 * df['body_size']
    tiny_upper = df['upper_shadow'] < 0.1 * df['total_range']
    base = small_body & long_lower & tiny_upper

    # 置信度: 下影越长越可靠
    confidence = np.where(
        base,
        np.where(df['lower_shadow'] > 3 * df['body_size'], 1.0, 0.5),
        0.0
    )

    df['BULLISH_HAMMER'] = np.where(base & (df['trend_down'] == 1), confidence, 0.0)
    df['HANGING_MAN'] = np.where(base & (df['trend_up'] == 1), confidence, 0.0)
    return df


def detect_engulfing(df: pd.DataFrame) -> pd.DataFrame:
    """
    吞没形态 (Engulfing)

    看涨吞没: 前一根阴线 + 当前阳线实体完全覆盖前阴线实体
    看跌吞没: 前一根阳线 + 当前阴线实体完全覆盖前阳线实体

    置信度:
      1.0 = 当前实体 > 1.5倍前一根实体
      0.5 = 刚好覆盖
    """
    df = _body_features(df)

    prev_bullish = df['is_bullish'].shift(1)
    prev_top = df['body_top'].shift(1)
    prev_bottom = df['body_bottom'].shift(1)
    prev_body = df['body_size'].shift(1)

    # 看涨吞没: 前阴 + 当前阳 + 当前实体包裹前实体
    bull_base = (
        (prev_bullish == 0) &
        (df['is_bullish'] == 1) &
        (df['body_top'] > prev_top) &
        (df['body_bottom'] < prev_bottom)
    )
    bull_conf = np.where(
        bull_base,
        np.where(df['body_size'] > 1.5 * prev_body, 1.0, 0.5),
        0.0
    )
    df['BULLISH_ENGULFING'] = bull_conf

    # 看跌吞没: 前阳 + 当前阴 + 当前实体包裹前实体
    bear_base = (
        (prev_bullish == 1) &
        (df['is_bullish'] == 0) &
        (df['body_top'] > prev_top) &
        (df['body_bottom'] < prev_bottom)
    )
    bear_conf = np.where(
        bear_base,
        np.where(df['body_size'] > 1.5 * prev_body, 1.0, 0.5),
        0.0
    )
    df['BEARISH_ENGULFING'] = bear_conf
    return df


def detect_doji(df: pd.DataFrame) -> pd.DataFrame:
    """
    十字星 (Doji)

    定义: 实体极小 body_ratio < 0.10 且 total_range > 0
    置信度: body_ratio 越小越标准
    """
    df = _body_features(df)

    has_range = df['total_range'] > 1e-9
    is_doji = (df['body_ratio'] < 0.10) & has_range

    df['DOJI'] = np.where(
        is_doji,
        np.where(df['body_ratio'] < 0.05, 1.0, 0.5),
        0.0
    )
    return df


def detect_morning_evening_star(df: pd.DataFrame) -> pd.DataFrame:
    """
    启明星 (Morning Star) 与黄昏星 (Evening Star) — 三根K线组合

    启明星: 大阴线 -> 十字星(实体小) -> 大阳线, 第3根收盘>第1根实体中点
    黄昏星: 大阳线 -> 十字星(实体小) -> 大阴线, 第3根收盘<第1根实体中点

    置信度:
      1.0 = 中间K线跳空 (gap)
      0.5 = 无跳空但满足基本条件
    """
    df = _body_features(df)

    # 第1根(shift 2), 第2根(shift 1), 第3根(当前)
    s2_bullish = df['is_bullish'].shift(2)
    s2_body_ratio = df['body_ratio'].shift(2)
    s2_body_mid = ((df['body_top'].shift(2) + df['body_bottom'].shift(2)) / 2)
    s1_body_ratio = df['body_ratio'].shift(1)
    s1_body_bottom = df['body_bottom'].shift(1)
    s1_body_top = df['body_top'].shift(1)
    s2_body_bottom = df['body_bottom'].shift(2)
    s2_body_top = df['body_top'].shift(2)

    big_body = 0.50  # 大K线实体占比阈值
    small_body = 0.25  # 十字星/小K线阈值

    # 启明星
    morning_base = (
        (s2_bullish == 0) &                    # 第1根阴线
        (s2_body_ratio > big_body) &            # 第1根大实体
        (s1_body_ratio < small_body) &          # 第2根小实体
        (df['is_bullish'] == 1) &               # 第3根阳线
        (df['body_ratio'] > big_body) &         # 第3根大实体
        (df['close'] > s2_body_mid)             # 第3根收盘>第1根中点
    )
    # 跳空加分
    morning_gap = s1_body_top < s2_body_bottom
    df['MORNING_STAR'] = np.where(
        morning_base,
        np.where(morning_gap, 1.0, 0.5),
        0.0
    )

    # 黄昏星
    evening_base = (
        (s2_bullish == 1) &
        (s2_body_ratio > big_body) &
        (s1_body_ratio < small_body) &
        (df['is_bullish'] == 0) &
        (df['body_ratio'] > big_body) &
        (df['close'] < s2_body_mid)
    )
    evening_gap = s1_body_bottom > s2_body_top
    df['EVENING_STAR'] = np.where(
        evening_base,
        np.where(evening_gap, 1.0, 0.5),
        0.0
    )
    return df


def detect_three_white_soldiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    红三兵 (Three White Soldiers)

    定义:
      - 连续三根阳线, 收盘价逐日创新高
      - 实体逐日增大（或至少持平）
      - 量能递增

    置信度:
      1.0 = 标准红三兵 (量增价升, 无长上影)
      0.7 = 第三根出现长上影或巨量 (衰竭警告)
      0.5 = 基本满足但量能不配合
    """
    df = _body_features(df)
    df = _ensure_volume_col(df)
    df = _volume_features(df)

    # 三根K线
    bull_0 = df['is_bullish'].shift(2) == 1
    bull_1 = df['is_bullish'].shift(1) == 1
    bull_2 = df['is_bullish'] == 1

    close_rise = (
        (df['close'] > df['close'].shift(1)) &
        (df['close'].shift(1) > df['close'].shift(2))
    )
    body_grow = (
        (df['body_size'] >= df['body_size'].shift(1) * 0.9) &
        (df['body_size'].shift(1) >= df['body_size'].shift(2) * 0.9)
    )
    vol_grow = (
        (df['volume'] >= df['volume'].shift(1) * 0.9) &
        (df['volume'].shift(1) >= df['volume'].shift(2) * 0.9)
    )

    base = bull_0 & bull_1 & bull_2 & close_rise & body_grow

    # 衰竭检测: 第3根长上影 或 巨量(>3倍5日均量)
    exhaustion = (
        (df['upper_shadow'] > df['body_size'] * 0.8) |
        (df['vol_ratio_5'] > 3.0)
    )

    conf = np.where(
        base,
        np.where(exhaustion, 0.7, np.where(vol_grow, 1.0, 0.5)),
        0.0
    )
    df['THREE_WHITE_SOLDIERS'] = conf
    return df


def detect_three_black_crows(df: pd.DataFrame) -> pd.DataFrame:
    """
    三只乌鸦 (Three Black Crows)

    定义: 连续三根阴线, 收盘价逐日创新低, 实体逐日增大
    置信度: 1.0=标准, 0.5=基本满足
    """
    df = _body_features(df)

    bear_0 = df['is_bullish'].shift(2) == 0
    bear_1 = df['is_bullish'].shift(1) == 0
    bear_2 = df['is_bullish'] == 0

    close_fall = (
        (df['close'] < df['close'].shift(1)) &
        (df['close'].shift(1) < df['close'].shift(2))
    )
    body_grow = (
        (df['body_size'] >= df['body_size'].shift(1) * 0.9) &
        (df['body_size'].shift(1) >= df['body_size'].shift(2) * 0.9)
    )

    base = bear_0 & bear_1 & bear_2 & close_fall

    df['THREE_BLACK_CROWS'] = np.where(
        base,
        np.where(body_grow, 1.0, 0.5),
        0.0
    )
    return df


def detect_rising_three_methods(df: pd.DataFrame) -> pd.DataFrame:
    """
    上升三法 (Rising Three Methods) — 简化5根K线版本

    定义:
      K1: 大阳线
      K2~K4: 小阴线缩量回调, 不跌破K1开盘价
      K5: 大阳线, 收盘创新高

    置信度: 1.0=量能完美配合, 0.5=基本满足
    """
    df = _body_features(df)
    df = _ensure_volume_col(df)

    # K1(shift 4), K2(shift 3), K3(shift 2), K4(shift 1), K5(当前)
    k1_bull = df['is_bullish'].shift(4) == 1
    k1_big = df['body_ratio'].shift(4) > 0.6
    k1_open = df['open'].shift(4)

    # 中间三根小K线, 不跌破K1开盘价
    mid_small = (
        (df['body_ratio'].shift(3) < 0.5) &
        (df['body_ratio'].shift(2) < 0.5) &
        (df['body_ratio'].shift(1) < 0.5)
    )
    mid_hold = (
        (df['low'].shift(3) >= k1_open) &
        (df['low'].shift(2) >= k1_open) &
        (df['low'].shift(1) >= k1_open)
    )
    # 中间缩量
    mid_shrink = (
        (df['volume'].shift(1) < df['volume'].shift(4)) &
        (df['volume'].shift(2) < df['volume'].shift(4))
    )

    k5_bull = df['is_bullish'] == 1
    k5_big = df['body_ratio'] > 0.6
    k5_new_high = df['close'] > df['high'].shift(4)

    base = k1_bull & k1_big & mid_small & mid_hold & k5_bull & k5_big & k5_new_high

    df['RISING_THREE_METHODS'] = np.where(
        base,
        np.where(mid_shrink, 1.0, 0.5),
        0.0
    )
    return df


def detect_falling_three_methods(df: pd.DataFrame) -> pd.DataFrame:
    """
    下降三法 (Falling Three Methods)

    定义:
      K1: 大阴线
      K2~K4: 小阳线缩量反弹, 不突破K1开盘价
      K5: 大阴线, 收盘创新低
    """
    df = _body_features(df)
    df = _ensure_volume_col(df)

    k1_bear = df['is_bullish'].shift(4) == 0
    k1_big = df['body_ratio'].shift(4) > 0.6
    k1_open = df['open'].shift(4)

    mid_small = (
        (df['body_ratio'].shift(3) < 0.5) &
        (df['body_ratio'].shift(2) < 0.5) &
        (df['body_ratio'].shift(1) < 0.5)
    )
    mid_hold = (
        (df['high'].shift(3) <= k1_open) &
        (df['high'].shift(2) <= k1_open) &
        (df['high'].shift(1) <= k1_open)
    )

    k5_bear = df['is_bullish'] == 0
    k5_big = df['body_ratio'] > 0.6
    k5_new_low = df['close'] < df['low'].shift(4)

    base = k1_bear & k1_big & mid_small & mid_hold & k5_bear & k5_big & k5_new_low

    df['FALLING_THREE_METHODS'] = np.where(base, 1.0, 0.0)
    return df


def detect_immortal_guide(df: pd.DataFrame) -> pd.DataFrame:
    """
    仙人指路 (Immortal Points the Way)

    定义:
      - 上涨初期或中途 (ma20向上)
      - 长上影线: upper_shadow > 2 * body_size
      - 量能放大: 当日量 > 5日均量 * 1.5
      - 收盘不跌（小阳或平盘）

    置信度:
      1.0 = 标准 (长上影+放量+趋势向上)
      0.5 = 基本满足
    注意: 不使用次日数据，避免未来函数。
    """
    df = _body_features(df)
    df = _ensure_volume_col(df)
    df = _volume_features(df)
    df = _ma_features(df)

    long_upper = df['upper_shadow'] > 2 * df['body_size']
    small_body = df['body_ratio'] < 0.40
    not_bearish = df['close'] >= df['open'] * 0.99  # 收盘不大跌
    vol_expand = df['vol_ratio_5'] > 1.5
    ma20_up = df['ma20'] > df['ma20'].shift(5)

    base = long_upper & small_body & not_bearish & ma20_up

    df['IMMORTAL_GUIDE'] = np.where(
        base,
        np.where(vol_expand, 1.0, 0.5),
        0.0
    )
    return df


def detect_old_duck_head(df: pd.DataFrame) -> pd.DataFrame:
    """
    老鸭头 (Old Duck Head) — 均线系统形态

    逻辑 (向量化近似):
      1. 鸭颈: 历史上 ma5 金叉 ma10 且均在 ma60 之上
      2. 鸭头: 之后 ma5 死叉 ma10 (回调)
      3. 鸭嘴不张: 回调期间 close 不破 ma60
      4. 鸭嘴闭合(买入信号): ma5 再次金叉 ma10, 且放量

    实现: 在30日窗口内寻找"金叉->死叉->再金叉"序列
    """
    df = _body_features(df)
    df = _ensure_volume_col(df)
    df = _ma_features(df)
    df = _volume_features(df)

    # ma5 与 ma10 的交叉信号
    ma5_above_10 = (df['ma5'] > df['ma10']).astype(int)
    cross = ma5_above_10.diff()
    golden_cross = cross == 1   # 金叉: 0->1
    death_cross = cross == -1   # 死叉: 1->0

    # 当前发生金叉
    current_golden = golden_cross

    # 回溯30日内存在过死叉
    has_recent_death = death_cross.rolling(30, min_periods=1).sum() > 0

    # 回溯30日内、死叉之前还有过金叉 (即完整序列)
    has_prior_golden = golden_cross.shift(1).rolling(30, min_periods=1).sum() > 0

    # 均线多头: ma5 在 ma60 之上
    above_ma60 = df['ma5'] > df['ma60']

    # 回调不破 ma60: 近15日最低价 > ma60
    low_15 = df['low'].rolling(15, min_periods=1).min()
    not_break_60 = low_15 >= df['ma60'] * 0.98  # 容忍2%误差

    # 放量确认
    vol_confirm = df['vol_ratio_5'] > 1.2

    signal = (
        current_golden &
        has_recent_death &
        has_prior_golden &
        above_ma60 &
        not_break_60 &
        vol_confirm
    )

    df['OLD_DUCK_HEAD'] = np.where(signal, 1.0, 0.0)
    return df


def detect_volume_price_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    量价关系形态（向量化）

    ACCUMULATION (量价齐升):
      价格上涨 + 量能同步放大 (连续3日)
    DIVERGENCE (量价背离):
      价格创20日新高 + 量能未创20日新高
    EXTREMELY_LOW_VOLUME (地量地价):
      量能 < 20日均量的40%, 预示变盘
    """
    df = _body_features(df)
    df = _ensure_volume_col(df)
    df = _volume_features(df)

    # --- 量价齐升 ---
    price_up_3 = (
        (df['close'] > df['close'].shift(1)) &
        (df['close'].shift(1) > df['close'].shift(2))
    )
    vol_up_3 = (
        (df['volume'] > df['volume'].shift(1)) &
        (df['volume'].shift(1) > df['volume'].shift(2))
    )
    df['ACCUMULATION'] = np.where(price_up_3 & vol_up_3, 1.0, 0.0)

    # --- 量价背离 ---
    high_20 = df['close'].rolling(20, min_periods=20).max()
    vol_high_20 = df['volume'].rolling(20, min_periods=20).max()
    price_at_high = df['close'] >= high_20
    vol_not_high = df['volume'] < vol_high_20 * 0.8
    df['DIVERGENCE'] = np.where(price_at_high & vol_not_high, 1.0, 0.0)

    # --- 地量地价 ---
    vol_ma20 = df['volume'].rolling(20, min_periods=20).mean()
    extremely_low = df['volume'] < vol_ma20 * 0.4
    df['EXTREMELY_LOW_VOLUME'] = np.where(extremely_low, 1.0, 0.0)

    return df


# ============================================================================
# 主入口: 一次性识别所有形态
# ============================================================================

def detect_all_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    一次性运行所有形态识别，返回含全部信号列的 DataFrame。

    参数:
        df: 含 open, high, low, close, vol/volume 列, 按时间升序
    返回:
        原 df + 所有形态信号列 (BULLISH_HAMMER, HANGING_MAN, ...)
    """
    if df is None or df.empty or len(df) < 5:
        return df

    df = _ensure_volume_col(df)

    # 按顺序调用各识别函数
    df = detect_hammer_hanging(df)
    df = detect_engulfing(df)
    df = detect_doji(df)
    df = detect_morning_evening_star(df)
    df = detect_three_white_soldiers(df)
    df = detect_three_black_crows(df)
    df = detect_rising_three_methods(df)
    df = detect_falling_three_methods(df)
    df = detect_immortal_guide(df)
    df = detect_old_duck_head(df)
    df = detect_volume_price_patterns(df)

    return df


# ============================================================================
# 信号列名 -> 中文名映射
# ============================================================================

PATTERN_CN_MAP = {
    'BULLISH_HAMMER': '锤子线',
    'HANGING_MAN': '上吊线',
    'BULLISH_ENGULFING': '看涨吞没',
    'BEARISH_ENGULFING': '看跌吞没',
    'DOJI': '十字星',
    'MORNING_STAR': '启明星',
    'EVENING_STAR': '黄昏星',
    'THREE_WHITE_SOLDIERS': '红三兵',
    'THREE_BLACK_CROWS': '三只乌鸦',
    'RISING_THREE_METHODS': '上升三法',
    'FALLING_THREE_METHODS': '下降三法',
    'IMMORTAL_GUIDE': '仙人指路',
    'OLD_DUCK_HEAD': '老鸭头',
    'ACCUMULATION': '量价齐升',
    'DIVERGENCE': '量价背离',
    'EXTREMELY_LOW_VOLUME': '地量地价',
}

BULLISH_PATTERNS = {
    'BULLISH_HAMMER', 'BULLISH_ENGULFING', 'MORNING_STAR',
    'THREE_WHITE_SOLDIERS', 'RISING_THREE_METHODS',
    'IMMORTAL_GUIDE', 'OLD_DUCK_HEAD', 'ACCUMULATION',
}
BEARISH_PATTERNS = {
    'HANGING_MAN', 'BEARISH_ENGULFING', 'EVENING_STAR',
    'THREE_BLACK_CROWS', 'FALLING_THREE_METHODS', 'DIVERGENCE',
}

ALL_PATTERN_COLS = list(PATTERN_CN_MAP.keys())


def get_latest_signals(df: pd.DataFrame, min_confidence: float = 0.5) -> list:
    """
    从已计算信号的 DataFrame 中，提取最后一行的触发信号。

    返回: [{"pattern": "红三兵", "code": "THREE_WHITE_SOLDIERS",
             "confidence": 1.0, "direction": "bullish"}, ...]
    """
    if df is None or df.empty:
        return []

    last = df.iloc[-1]
    signals = []
    for col in ALL_PATTERN_COLS:
        if col not in df.columns:
            continue
        val = last.get(col, 0.0)
        if pd.isna(val) or val < min_confidence:
            continue
        direction = 'bullish' if col in BULLISH_PATTERNS else (
            'bearish' if col in BEARISH_PATTERNS else 'neutral'
        )
        signals.append({
            'pattern': PATTERN_CN_MAP[col],
            'code': col,
            'confidence': round(float(val), 2),
            'direction': direction,
        })

    # 按置信度降序
    signals.sort(key=lambda x: -x['confidence'])
    return signals

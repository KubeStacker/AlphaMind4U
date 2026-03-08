"""
K线形态识别工具库 (K-line Pattern Recognition Utility) v3.0

目标:
1. 对经典K线形态做更严格的结构化识别，减少纯阈值误判。
2. 在识别时加入趋势、位置、量能、波动上下文。
3. 提供历史日线回测/训练能力，用未来收益校准各形态有效性。

输入: DataFrame 含 open, high, low, close, vol(或volume) 列，按时间升序
输出: 原 DataFrame 新增信号列，值为 0.0~1.0 置信度

注意: 严格避免“未来函数”——所有实时识别判断仅使用当前及历史数据。
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

EPS = 1e-9
DEFAULT_CALIBRATION_PATH = Path(__file__).with_name("kline_pattern_calibration.json")
DEFAULT_CONF_BUCKETS = (0.50, 0.65, 0.80, 1.01)
CORE_FEATURE_COLS = {
    "body_top",
    "body_bottom",
    "body_size",
    "upper_shadow",
    "lower_shadow",
    "total_range",
    "body_ratio",
    "is_bullish",
    "prev_close",
    "atr14",
    "close_pos",
    "vol_ratio_5",
    "ma5",
    "ma10",
    "ma20",
    "ma60",
    "ret_5",
    "ret_10",
    "trend_up",
    "trend_down",
    "close_rank_20",
}


# ============================================================================
# 私有辅助: 基础K线特征（向量化）
# ============================================================================

def _ensure_volume_col(df: pd.DataFrame) -> pd.DataFrame:
    """统一 volume 列名: vol -> volume"""
    if "volume" not in df.columns and "vol" in df.columns:
        df = df.rename(columns={"vol": "volume"})
    return df


def _safe_divide(numerator: Any, denominator: Any, default: float = 0.0) -> np.ndarray:
    """安全除法，避免 0/0 与 inf。"""
    num = np.asarray(numerator, dtype=float)
    den = np.asarray(denominator, dtype=float)
    out = np.full_like(num, float(default), dtype=float)
    mask = np.abs(den) > EPS
    out[mask] = num[mask] / den[mask]
    return out


def _clip01(values: Any) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    return np.clip(np.nan_to_num(arr, nan=0.0, posinf=1.0, neginf=0.0), 0.0, 1.0)


def _compose_confidence(base_mask: Any, *components: Any) -> np.ndarray:
    """
    置信度组合器:
      - 不触发 base -> 0.0
      - 触发 base -> 0.5~1.0
    """
    base_arr = np.asarray(base_mask, dtype=bool)
    if not components:
        return np.where(base_arr, 1.0, 0.0)

    stacked = np.vstack([_clip01(c) for c in components])
    mean_score = stacked.mean(axis=0)
    return np.where(base_arr, np.clip(0.5 + 0.5 * mean_score, 0.0, 1.0), 0.0)


def _bucket_confidence(value: float, edges: Sequence[float] = DEFAULT_CONF_BUCKETS) -> str:
    for start, end in zip(edges[:-1], edges[1:]):
        if start <= value < end:
            return f"{int(start * 100):02d}_{int(min(end, 1.0) * 100):02d}"
    return f"{int(edges[-2] * 100):02d}_{int(min(edges[-1], 1.0) * 100):02d}"


def _sorted_input(df: pd.DataFrame) -> pd.DataFrame:
    if "trade_date" in df.columns:
        return df.sort_values("trade_date").reset_index(drop=True)
    return df.reset_index(drop=True)


def _body_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算K线实体和影线基础特征（向量化）。
    """
    df = _ensure_volume_col(df)
    if not CORE_FEATURE_COLS.issubset(df.columns):
        df = df.copy()

    base_cols = ["open", "high", "low", "close"]
    if "volume" in df.columns:
        base_cols.append("volume")

    for col in base_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["body_top"] = np.maximum(df["open"], df["close"])
    df["body_bottom"] = np.minimum(df["open"], df["close"])
    df["body_size"] = df["body_top"] - df["body_bottom"]
    df["upper_shadow"] = df["high"] - df["body_top"]
    df["lower_shadow"] = df["body_bottom"] - df["low"]
    df["total_range"] = df["high"] - df["low"]
    df["body_ratio"] = _safe_divide(df["body_size"], df["total_range"], 0.0)
    df["upper_shadow_ratio"] = _safe_divide(df["upper_shadow"], df["total_range"], 0.0)
    df["lower_shadow_ratio"] = _safe_divide(df["lower_shadow"], df["total_range"], 0.0)
    df["is_bullish"] = (df["close"] >= df["open"]).astype(int)

    df["prev_open"] = df["open"].shift(1)
    df["prev_close"] = df["close"].shift(1)
    df["prev_high"] = df["high"].shift(1)
    df["prev_low"] = df["low"].shift(1)
    df["prev_body_top"] = df["body_top"].shift(1)
    df["prev_body_bottom"] = df["body_bottom"].shift(1)
    df["prev_body_size"] = df["body_size"].shift(1)
    df["prev_body_ratio"] = df["body_ratio"].shift(1)
    df["prev_is_bullish"] = df["is_bullish"].shift(1)

    tr = np.maximum.reduce(
        [
            df["total_range"].to_numpy(),
            np.abs(df["high"] - df["prev_close"]).to_numpy(),
            np.abs(df["low"] - df["prev_close"]).to_numpy(),
        ]
    )
    df["true_range"] = tr
    df["atr14"] = pd.Series(tr, index=df.index).rolling(14, min_periods=5).mean()
    df["body_ma10"] = df["body_size"].rolling(10, min_periods=3).mean()
    df["range_ma10"] = df["total_range"].rolling(10, min_periods=3).mean()
    df["body_to_atr"] = _safe_divide(df["body_size"], df["atr14"], 0.0)
    df["range_to_atr"] = _safe_divide(df["total_range"], df["atr14"], 0.0)
    df["body_vs_avg"] = _safe_divide(df["body_size"], df["body_ma10"], 0.0)
    df["range_vs_avg"] = _safe_divide(df["total_range"], df["range_ma10"], 0.0)
    df["close_pos"] = _safe_divide(df["close"] - df["low"], df["total_range"], 0.5)
    df["open_pos"] = _safe_divide(df["open"] - df["low"], df["total_range"], 0.5)
    df["gap_up_body"] = (df["body_bottom"] > df["prev_body_top"]).astype(int)
    df["gap_down_body"] = (df["body_top"] < df["prev_body_bottom"]).astype(int)
    return df


def _volume_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算量能辅助特征（向量化）。
    """
    df = _ensure_volume_col(df)
    if "volume" not in df.columns:
        df = df.copy()
        df["volume"] = 0.0

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    df["vol_ma5"] = df["volume"].rolling(5, min_periods=1).mean()
    df["vol_ma10"] = df["volume"].rolling(10, min_periods=1).mean()
    df["vol_ma20"] = df["volume"].rolling(20, min_periods=3).mean()
    df["vol_ratio_5"] = _safe_divide(df["volume"], df["vol_ma5"], 1.0)
    df["vol_ratio_10"] = _safe_divide(df["volume"], df["vol_ma10"], 1.0)
    df["vol_ratio_20"] = _safe_divide(df["volume"], df["vol_ma20"], 1.0)
    df["vol_trend"] = _safe_divide(df["vol_ma5"], df["vol_ma20"], 1.0)
    return df


def _ma_features(df: pd.DataFrame) -> pd.DataFrame:
    """计算均线与均线斜率特征。"""
    for p in [5, 10, 20, 60]:
        col = f"ma{p}"
        if col not in df.columns:
            df[col] = df["close"].rolling(p, min_periods=p).mean()

    df["ma5_slope_3"] = _safe_divide(df["ma5"] - df["ma5"].shift(3), df["ma5"].shift(3), 0.0)
    df["ma10_slope_5"] = _safe_divide(df["ma10"] - df["ma10"].shift(5), df["ma10"].shift(5), 0.0)
    df["ma20_slope_5"] = _safe_divide(df["ma20"] - df["ma20"].shift(5), df["ma20"].shift(5), 0.0)
    return df


def _trend_context(df: pd.DataFrame) -> pd.DataFrame:
    """
    趋势与位置上下文:
      - trend_up / trend_down
      - ret_3/5/10/20
      - close_rank_20（处于20日区间中的位置）
    """
    df = _ma_features(df)
    df["ret_3"] = _safe_divide(df["close"] - df["close"].shift(3), df["close"].shift(3), 0.0)
    df["ret_5"] = _safe_divide(df["close"] - df["close"].shift(5), df["close"].shift(5), 0.0)
    df["ret_10"] = _safe_divide(df["close"] - df["close"].shift(10), df["close"].shift(10), 0.0)
    df["ret_20"] = _safe_divide(df["close"] - df["close"].shift(20), df["close"].shift(20), 0.0)

    df["high_20"] = df["high"].rolling(20, min_periods=5).max()
    df["low_20"] = df["low"].rolling(20, min_periods=5).min()
    df["close_rank_20"] = _safe_divide(df["close"] - df["low_20"], df["high_20"] - df["low_20"], 0.5)
    df["near_low_20"] = (df["close_rank_20"] <= 0.25).astype(int)
    df["near_high_20"] = (df["close_rank_20"] >= 0.75).astype(int)

    ma_up = (df["ma5"] > df["ma10"]) & (df["ma10"] > df["ma20"])
    ma_down = (df["ma5"] < df["ma10"]) & (df["ma10"] < df["ma20"])

    df["trend_up"] = (
        (ma_up & (df["ma5_slope_3"] > 0.01))
        | ((df["ret_10"] > 0.06) & (df["close"] > df["ma20"]) & (df["close_rank_20"] > 0.6))
    ).astype(int)
    df["trend_down"] = (
        (ma_down & (df["ma5_slope_3"] < -0.01))
        | ((df["ret_10"] < -0.06) & (df["close"] < df["ma20"]) & (df["close_rank_20"] < 0.4))
    ).astype(int)
    return df


def _prepare_core_features(df: pd.DataFrame) -> pd.DataFrame:
    df = _body_features(df)
    df = _volume_features(df)
    df = _trend_context(df)
    return df


# ============================================================================
# 公开API: 形态识别函数（全部向量化）
# ============================================================================

def detect_hammer_hanging(df: pd.DataFrame) -> pd.DataFrame:
    """
    锤子线 / 上吊线:
      - 小实体
      - 长下影
      - 收盘接近当日高位
      - 需放在趋势末端理解
    """
    df = _prepare_core_features(df)

    small_body = (df["body_ratio"] <= 0.35) & (df["body_vs_avg"] <= 0.95)
    long_lower = df["lower_shadow"] >= np.maximum(df["body_size"] * 2.2, df["total_range"] * 0.45)
    tiny_upper = df["upper_shadow"] <= np.maximum(df["body_size"] * 0.35, df["total_range"] * 0.18)
    close_near_high = df["close_pos"] >= 0.58
    valid_range = df["range_to_atr"] >= 0.75
    base = small_body & long_lower & tiny_upper & close_near_high & valid_range

    bull_context = (df["trend_down"] == 1) | ((df["ret_5"] < -0.03) & (df["near_low_20"] == 1))
    bear_context = (df["trend_up"] == 1) | ((df["ret_5"] > 0.04) & (df["near_high_20"] == 1))

    base_quality = _compose_confidence(
        base,
        1.0 - _clip01(df["body_ratio"] / 0.35),
        _clip01(_safe_divide(df["lower_shadow"], np.maximum(df["body_size"], df["atr14"] * 0.08), 0.0) / 4.0),
        _clip01(df["close_pos"]),
        _clip01(_safe_divide(df["range_to_atr"], 1.4, 0.0)),
    )
    vol_boost = _clip01(_safe_divide(df["vol_ratio_5"], 1.4, 0.0))

    df["BULLISH_HAMMER"] = np.where(
        bull_context,
        np.maximum(0.0, np.minimum(1.0, base_quality + 0.08 * vol_boost)),
        0.0,
    )
    df["HANGING_MAN"] = np.where(
        bear_context,
        np.maximum(0.0, np.minimum(1.0, base_quality + 0.04 * _clip01(df["close_rank_20"]))),
        0.0,
    )
    return df


def detect_shooting_star(df: pd.DataFrame) -> pd.DataFrame:
    """
    射击之星:
      - 小实体位于下方
      - 长上影
      - 上涨趋势后更有效
    """
    df = _prepare_core_features(df)

    small_body = (df["body_ratio"] <= 0.35) & (df["body_vs_avg"] <= 1.0)
    long_upper = df["upper_shadow"] >= np.maximum(df["body_size"] * 2.5, df["total_range"] * 0.45)
    tiny_lower = df["lower_shadow"] <= np.maximum(df["body_size"] * 0.3, df["total_range"] * 0.16)
    body_low = df["close_pos"] <= 0.45
    valid_range = df["range_to_atr"] >= 0.85
    context = (df["trend_up"] == 1) | ((df["ret_5"] > 0.05) & (df["near_high_20"] == 1))
    base = small_body & long_upper & tiny_lower & body_low & valid_range & context

    df["SHOOTING_STAR"] = _compose_confidence(
        base,
        1.0 - _clip01(df["body_ratio"] / 0.35),
        _clip01(_safe_divide(df["upper_shadow"], np.maximum(df["body_size"], df["atr14"] * 0.08), 0.0) / 4.5),
        1.0 - _clip01(df["close_pos"]),
        _clip01(df["close_rank_20"]),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.3, 0.0)),
    )
    return df


def detect_engulfing(df: pd.DataFrame) -> pd.DataFrame:
    """
    吞没形态:
      - 当前实体完整包裹前一日实体
      - 更强调前后趋势与实体扩张
    """
    df = _prepare_core_features(df)

    prev_body = df["prev_body_size"].fillna(0.0)
    current_expand = df["body_size"] >= np.maximum(prev_body * 1.08, df["body_ma10"] * 0.85)
    prev_meaningful = df["prev_body_ratio"] >= 0.3

    bull_base = (
        (df["prev_is_bullish"] == 0)
        & (df["is_bullish"] == 1)
        & (df["body_top"] >= df["prev_body_top"])
        & (df["body_bottom"] <= df["prev_body_bottom"])
        & current_expand
        & prev_meaningful
    )
    bull_context = (df["trend_down"] == 1) | ((df["ret_5"] < -0.03) & (df["near_low_20"] == 1))
    df["BULLISH_ENGULFING"] = _compose_confidence(
        bull_base & bull_context,
        _clip01(_safe_divide(df["body_size"], np.maximum(prev_body, df["body_ma10"] * 0.5), 0.0) / 2.0),
        _clip01(df["close_pos"]),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.35, 0.0)),
        np.where(df["close"] > df["prev_high"], 1.0, 0.55),
        np.where(bull_context, 1.0, 0.0),
    )

    bear_base = (
        (df["prev_is_bullish"] == 1)
        & (df["is_bullish"] == 0)
        & (df["body_top"] >= df["prev_body_top"])
        & (df["body_bottom"] <= df["prev_body_bottom"])
        & current_expand
        & prev_meaningful
    )
    bear_context = (df["trend_up"] == 1) | ((df["ret_5"] > 0.04) & (df["near_high_20"] == 1))
    df["BEARISH_ENGULFING"] = _compose_confidence(
        bear_base & bear_context,
        _clip01(_safe_divide(df["body_size"], np.maximum(prev_body, df["body_ma10"] * 0.5), 0.0) / 2.0),
        1.0 - _clip01(df["close_pos"]),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.25, 0.0)),
        np.where(df["close"] < df["prev_low"], 1.0, 0.55),
        np.where(bear_context, 1.0, 0.0),
    )
    return df


def detect_piercing_dark_cloud(df: pd.DataFrame) -> pd.DataFrame:
    """
    曙光初现 / 乌云盖顶:
      - 与吞没类似，但第2根只需穿透前一根实体中部。
    """
    df = _prepare_core_features(df)

    prev_mid = (df["prev_body_top"] + df["prev_body_bottom"]) / 2
    prev_big = (df["prev_body_ratio"] >= 0.5) & (df["prev_body_size"] >= df["body_ma10"] * 0.9)

    piercing_base = (
        (df["prev_is_bullish"] == 0)
        & (df["is_bullish"] == 1)
        & (df["open"] <= df["prev_low"] * 1.01)
        & (df["close"] > prev_mid)
        & (df["close"] < df["prev_body_top"])
        & prev_big
    )
    piercing_context = (df["trend_down"] == 1) | ((df["ret_5"] < -0.04) & (df["near_low_20"] == 1))
    df["PIERCING_LINE"] = _compose_confidence(
        piercing_base & piercing_context,
        _clip01(_safe_divide(df["close"] - prev_mid, df["prev_body_top"] - prev_mid, 0.0)),
        _clip01(df["close_pos"]),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.25, 0.0)),
        np.where(df["gap_down_body"] == 1, 1.0, 0.55),
    )

    dark_base = (
        (df["prev_is_bullish"] == 1)
        & (df["is_bullish"] == 0)
        & (df["open"] >= df["prev_high"] * 0.99)
        & (df["close"] < prev_mid)
        & (df["close"] > df["prev_body_bottom"])
        & prev_big
    )
    dark_context = (df["trend_up"] == 1) | ((df["ret_5"] > 0.04) & (df["near_high_20"] == 1))
    df["DARK_CLOUD_COVER"] = _compose_confidence(
        dark_base & dark_context,
        _clip01(_safe_divide(prev_mid - df["close"], prev_mid - df["prev_body_bottom"], 0.0)),
        1.0 - _clip01(df["close_pos"]),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.2, 0.0)),
        np.where(df["gap_up_body"] == 1, 1.0, 0.55),
    )
    return df


def detect_doji(df: pd.DataFrame) -> pd.DataFrame:
    """
    十字星:
      - 实体极小
      - 当日波动不能过于失真
    """
    df = _prepare_core_features(df)

    small_body = df["body_ratio"] <= 0.08
    tiny_body_vs_avg = df["body_vs_avg"] <= 0.35
    enough_range = (df["total_range"] > EPS) & (df["range_vs_avg"] >= 0.65)
    balanced_shadow = np.abs(df["upper_shadow"] - df["lower_shadow"]) <= np.maximum(df["total_range"] * 0.45, df["atr14"] * 0.4)
    base = small_body & tiny_body_vs_avg & enough_range

    df["DOJI"] = _compose_confidence(
        base,
        1.0 - _clip01(df["body_ratio"] / 0.08),
        1.0 - _clip01(df["body_vs_avg"] / 0.35),
        _clip01(_safe_divide(df["range_vs_avg"], 1.4, 0.0)),
        np.where(balanced_shadow, 1.0, 0.5),
    )
    return df


def detect_morning_evening_star(df: pd.DataFrame) -> pd.DataFrame:
    """
    启明星 / 黄昏星:
      - 三根K线组合
      - 第3根需有效收复/跌破第1根实体中位
    """
    df = _prepare_core_features(df)

    first_bull = df["is_bullish"].shift(2)
    first_ratio = df["body_ratio"].shift(2)
    first_top = df["body_top"].shift(2)
    first_bottom = df["body_bottom"].shift(2)
    first_mid = (first_top + first_bottom) / 2

    second_ratio = df["body_ratio"].shift(1)
    second_top = df["body_top"].shift(1)
    second_bottom = df["body_bottom"].shift(1)

    first_big = first_ratio >= 0.5
    second_small = second_ratio <= 0.28
    third_big = df["body_ratio"] >= 0.5
    third_expand = df["body_vs_avg"] >= 1.0

    morning_context = ((df["trend_down"].shift(2) == 1) | (df["ret_5"].shift(2) < -0.04))
    morning_base = (
        (first_bull == 0)
        & first_big
        & second_small
        & (df["is_bullish"] == 1)
        & third_big
        & third_expand
        & (df["close"] > first_mid)
        & morning_context
    )
    morning_gap = (second_top < first_bottom) | (df["open"] > second_top)
    df["MORNING_STAR"] = _compose_confidence(
        morning_base,
        _clip01(_safe_divide(df["close"] - first_mid, first_top - first_mid, 0.0)),
        _clip01(_safe_divide(df["body_size"], df["body_ma10"], 0.0) / 1.6),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.35, 0.0)),
        np.where(morning_gap, 1.0, 0.55),
    )

    evening_context = ((df["trend_up"].shift(2) == 1) | (df["ret_5"].shift(2) > 0.04))
    evening_base = (
        (first_bull == 1)
        & first_big
        & second_small
        & (df["is_bullish"] == 0)
        & third_big
        & third_expand
        & (df["close"] < first_mid)
        & evening_context
    )
    evening_gap = (second_bottom > first_top) | (df["open"] < second_bottom)
    df["EVENING_STAR"] = _compose_confidence(
        evening_base,
        _clip01(_safe_divide(first_mid - df["close"], first_mid - first_bottom, 0.0)),
        _clip01(_safe_divide(df["body_size"], df["body_ma10"], 0.0) / 1.5),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.2, 0.0)),
        np.where(evening_gap, 1.0, 0.55),
    )
    return df


def detect_three_white_soldiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    红三兵:
      - 连续三根阳线，收盘逐日抬高
      - 开盘尽量位于前一根实体内部
      - 上影不能太长
      - 更适合出现在底部或整理后的启动阶段
    """
    df = _prepare_core_features(df)

    bull_0 = df["is_bullish"].shift(2) == 1
    bull_1 = df["is_bullish"].shift(1) == 1
    bull_2 = df["is_bullish"] == 1

    close_rise = (df["close"] > df["close"].shift(1)) & (df["close"].shift(1) > df["close"].shift(2))
    opens_inside = (
        (df["open"].shift(1) >= df["body_bottom"].shift(2))
        & (df["open"].shift(1) <= df["body_top"].shift(2))
        & (df["open"] >= df["body_bottom"].shift(1))
        & (df["open"] <= df["body_top"].shift(1))
    )
    short_upper = (
        (df["upper_shadow_ratio"] <= 0.22)
        & (df["upper_shadow_ratio"].shift(1) <= 0.22)
        & (df["upper_shadow_ratio"].shift(2) <= 0.22)
    )
    body_strong = (
        (df["body_ratio"] >= 0.45)
        & (df["body_ratio"].shift(1) >= 0.45)
        & (df["body_ratio"].shift(2) >= 0.45)
    )
    start_context = (df["trend_down"].shift(2) == 1) | (df["ret_10"].shift(2) <= 0.02)
    vol_confirm = (df["vol_ratio_5"] >= 1.0) & (df["volume"] >= df["volume"].shift(1) * 0.9)
    base = bull_0 & bull_1 & bull_2 & close_rise & opens_inside & short_upper & body_strong & start_context

    df["THREE_WHITE_SOLDIERS"] = _compose_confidence(
        base,
        _clip01(df["close_pos"]),
        _clip01(df["close_pos"].shift(1)),
        _clip01(df["close_pos"].shift(2)),
        np.where(vol_confirm, 1.0, 0.45),
        _clip01(_safe_divide(df["body_size"], df["body_ma10"], 0.0) / 1.6),
    )
    return df


def detect_three_black_crows(df: pd.DataFrame) -> pd.DataFrame:
    """
    三只乌鸦:
      - 连续三根阴线，收盘逐日降低
      - 更适合出现在高位或持续拉升之后
    """
    df = _prepare_core_features(df)

    bear_0 = df["is_bullish"].shift(2) == 0
    bear_1 = df["is_bullish"].shift(1) == 0
    bear_2 = df["is_bullish"] == 0

    close_fall = (df["close"] < df["close"].shift(1)) & (df["close"].shift(1) < df["close"].shift(2))
    opens_inside = (
        (df["open"].shift(1) >= df["body_bottom"].shift(2))
        & (df["open"].shift(1) <= df["body_top"].shift(2))
        & (df["open"] >= df["body_bottom"].shift(1))
        & (df["open"] <= df["body_top"].shift(1))
    )
    short_lower = (
        (df["lower_shadow_ratio"] <= 0.22)
        & (df["lower_shadow_ratio"].shift(1) <= 0.22)
        & (df["lower_shadow_ratio"].shift(2) <= 0.22)
    )
    body_strong = (
        (df["body_ratio"] >= 0.45)
        & (df["body_ratio"].shift(1) >= 0.45)
        & (df["body_ratio"].shift(2) >= 0.45)
    )
    start_context = (df["trend_up"].shift(2) == 1) | (df["ret_10"].shift(2) >= 0.05)
    vol_confirm = (df["vol_ratio_5"] >= 0.95) & (df["volume"] >= df["volume"].shift(1) * 0.9)
    base = bear_0 & bear_1 & bear_2 & close_fall & opens_inside & short_lower & body_strong & start_context

    df["THREE_BLACK_CROWS"] = _compose_confidence(
        base,
        1.0 - _clip01(df["close_pos"]),
        1.0 - _clip01(df["close_pos"].shift(1)),
        1.0 - _clip01(df["close_pos"].shift(2)),
        np.where(vol_confirm, 1.0, 0.45),
        _clip01(_safe_divide(df["body_size"], df["body_ma10"], 0.0) / 1.5),
    )
    return df


def detect_rising_three_methods(df: pd.DataFrame) -> pd.DataFrame:
    """
    上升三法:
      - 大阳 -> 3根小阴/小实体回撤 -> 再次大阳突破
      - 中间调整不能破坏主趋势
    """
    df = _prepare_core_features(df)

    k1_bull = df["is_bullish"].shift(4) == 1
    k1_big = (df["body_ratio"].shift(4) >= 0.55) & (df["body_vs_avg"].shift(4) >= 1.0)
    k1_top = df["body_top"].shift(4)
    k1_bottom = df["body_bottom"].shift(4)

    mid_small = (
        (df["body_ratio"].shift(3) <= 0.38)
        & (df["body_ratio"].shift(2) <= 0.38)
        & (df["body_ratio"].shift(1) <= 0.38)
    )
    mid_inside = (
        (df["high"].shift(3) <= k1_top * 1.01)
        & (df["low"].shift(3) >= k1_bottom * 0.99)
        & (df["high"].shift(2) <= k1_top * 1.01)
        & (df["low"].shift(2) >= k1_bottom * 0.99)
        & (df["high"].shift(1) <= k1_top * 1.01)
        & (df["low"].shift(1) >= k1_bottom * 0.99)
    )
    mid_volume_shrink = (
        (df["volume"].shift(1) <= df["volume"].shift(4))
        & (df["volume"].shift(2) <= df["volume"].shift(4))
        & (df["volume"].shift(3) <= df["volume"].shift(4))
    )

    k5_bull = df["is_bullish"] == 1
    k5_big = (df["body_ratio"] >= 0.55) & (df["body_vs_avg"] >= 1.0)
    k5_break = df["close"] > df["high"].shift(4)
    trend_ok = (df["trend_up"].shift(4) == 1) | ((df["ma20_slope_5"] > 0) & (df["close"] > df["ma20"]))
    base = k1_bull & k1_big & mid_small & mid_inside & k5_bull & k5_big & k5_break & trend_ok

    df["RISING_THREE_METHODS"] = _compose_confidence(
        base,
        _clip01(_safe_divide(df["close"] - df["high"].shift(4), np.maximum(df["atr14"], df["range_ma10"]), 0.0)),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.25, 0.0)),
        np.where(mid_volume_shrink, 1.0, 0.5),
        _clip01(df["close_pos"]),
    )
    return df


def detect_falling_three_methods(df: pd.DataFrame) -> pd.DataFrame:
    """
    下降三法:
      - 大阴 -> 3根小阳/小实体反抽 -> 再次大阴破位
    """
    df = _prepare_core_features(df)

    k1_bear = df["is_bullish"].shift(4) == 0
    k1_big = (df["body_ratio"].shift(4) >= 0.55) & (df["body_vs_avg"].shift(4) >= 1.0)
    k1_top = df["body_top"].shift(4)
    k1_bottom = df["body_bottom"].shift(4)

    mid_small = (
        (df["body_ratio"].shift(3) <= 0.38)
        & (df["body_ratio"].shift(2) <= 0.38)
        & (df["body_ratio"].shift(1) <= 0.38)
    )
    mid_inside = (
        (df["high"].shift(3) <= k1_top * 1.01)
        & (df["low"].shift(3) >= k1_bottom * 0.99)
        & (df["high"].shift(2) <= k1_top * 1.01)
        & (df["low"].shift(2) >= k1_bottom * 0.99)
        & (df["high"].shift(1) <= k1_top * 1.01)
        & (df["low"].shift(1) >= k1_bottom * 0.99)
    )
    mid_volume_shrink = (
        (df["volume"].shift(1) <= df["volume"].shift(4))
        & (df["volume"].shift(2) <= df["volume"].shift(4))
        & (df["volume"].shift(3) <= df["volume"].shift(4))
    )

    k5_bear = df["is_bullish"] == 0
    k5_big = (df["body_ratio"] >= 0.55) & (df["body_vs_avg"] >= 1.0)
    k5_break = df["close"] < df["low"].shift(4)
    trend_ok = (df["trend_down"].shift(4) == 1) | ((df["ma20_slope_5"] < 0) & (df["close"] < df["ma20"]))
    base = k1_bear & k1_big & mid_small & mid_inside & k5_bear & k5_big & k5_break & trend_ok

    df["FALLING_THREE_METHODS"] = _compose_confidence(
        base,
        _clip01(_safe_divide(df["low"].shift(4) - df["close"], np.maximum(df["atr14"], df["range_ma10"]), 0.0)),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.15, 0.0)),
        np.where(mid_volume_shrink, 1.0, 0.5),
        1.0 - _clip01(df["close_pos"]),
    )
    return df


def detect_immortal_guide(df: pd.DataFrame) -> pd.DataFrame:
    """
    仙人指路:
      - 处于上涨中段或突破前夕
      - 长上影、实体较小、放量但收盘不能太弱
    """
    df = _prepare_core_features(df)

    trend_ok = (df["ma20_slope_5"] > 0.01) & (df["close"] >= df["ma20"] * 0.98)
    long_upper = df["upper_shadow"] >= np.maximum(df["body_size"] * 2.2, df["total_range"] * 0.42)
    small_body = df["body_ratio"] <= 0.40
    close_not_weak = df["close_pos"] >= 0.45
    vol_expand = df["vol_ratio_5"] >= 1.25
    base = trend_ok & long_upper & small_body & close_not_weak

    df["IMMORTAL_GUIDE"] = _compose_confidence(
        base,
        _clip01(_safe_divide(df["upper_shadow"], np.maximum(df["body_size"], df["atr14"] * 0.08), 0.0) / 4.0),
        _clip01(df["close_pos"]),
        np.where(vol_expand, 1.0, 0.5),
        _clip01(df["close_rank_20"]),
    )
    return df


def detect_old_duck_head(df: pd.DataFrame) -> pd.DataFrame:
    """
    老鸭头:
      - 30~40日内存在金叉 -> 死叉 -> 再金叉
      - 调整不破60日线
      - 再次放量突破时确认
    """
    df = _prepare_core_features(df)

    ma5_above_10 = (df["ma5"] > df["ma10"]).astype(int)
    cross = ma5_above_10.diff()
    golden_cross = cross == 1
    death_cross = cross == -1

    current_golden = golden_cross
    has_recent_death = death_cross.rolling(30, min_periods=1).sum() > 0
    has_prior_golden = golden_cross.shift(1).rolling(40, min_periods=1).sum() > 0
    above_ma60 = (df["ma5"] > df["ma60"]) & (df["ma10"] > df["ma60"])
    ma60_up = df["ma60"] > df["ma60"].shift(5)
    retrace_hold = df["low"].rolling(15, min_periods=5).min() >= df["ma60"] * 0.98
    close_break = df["close"] >= df["high"].rolling(20, min_periods=5).max().shift(1) * 0.98
    vol_confirm = df["vol_ratio_5"] >= 1.15

    signal = current_golden & has_recent_death & has_prior_golden & above_ma60 & ma60_up & retrace_hold
    df["OLD_DUCK_HEAD"] = _compose_confidence(
        signal,
        np.where(close_break, 1.0, 0.55),
        np.where(vol_confirm, 1.0, 0.55),
        _clip01(_safe_divide(df["ma20_slope_5"], 0.04, 0.0)),
        _clip01(df["close_rank_20"]),
    )
    return df


def detect_volume_price_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    量价关系形态:
      - ACCUMULATION: 趋势上行中的连续价量共振
      - DIVERGENCE: 高位价强量弱
      - EXTREMELY_LOW_VOLUME: 地量窗口
    """
    df = _prepare_core_features(df)

    price_up_3 = (df["close"] > df["close"].shift(1)) & (df["close"].shift(1) > df["close"].shift(2))
    vol_up_3 = (df["volume"] >= df["volume"].shift(1) * 0.95) & (df["volume"].shift(1) >= df["volume"].shift(2) * 0.95)
    acc_context = (df["close"] > df["ma20"]) & (df["ma20_slope_5"] > 0)
    acc_base = price_up_3 & vol_up_3 & acc_context
    df["ACCUMULATION"] = _compose_confidence(
        acc_base,
        _clip01(_safe_divide(df["ret_3"], 0.08, 0.0)),
        _clip01(_safe_divide(df["vol_ratio_5"], 1.5, 0.0)),
        _clip01(df["close_rank_20"]),
    )

    price_at_high = df["close"] >= df["high_20"] * 0.985
    vol_not_high = df["vol_ratio_20"] < 0.9
    weak_close = df["close_pos"] < 0.65
    divergence_base = price_at_high & vol_not_high & weak_close & ((df["trend_up"] == 1) | (df["ret_10"] > 0.08))
    df["DIVERGENCE"] = _compose_confidence(
        divergence_base,
        _clip01(1.0 - _safe_divide(df["vol_ratio_20"], 0.9, 0.0)),
        _clip01(df["close_rank_20"]),
        1.0 - _clip01(df["close_pos"]),
    )

    extremely_low = (df["vol_ratio_20"] <= 0.45) & (df["vol_ratio_5"] <= 0.65) & (df["range_vs_avg"] <= 0.75)
    df["EXTREMELY_LOW_VOLUME"] = _compose_confidence(
        extremely_low,
        1.0 - _clip01(_safe_divide(df["vol_ratio_20"], 0.45, 0.0)),
        1.0 - _clip01(_safe_divide(df["range_vs_avg"], 0.75, 0.0)),
    )

    return df


# ============================================================================
# 主入口: 一次性识别所有形态
# ============================================================================

def detect_all_patterns(df: pd.DataFrame) -> pd.DataFrame:
    """
    一次性运行所有形态识别，返回含全部信号列的 DataFrame。
    """
    if df is None or df.empty or len(df) < 5:
        return df

    df = _sorted_input(df)
    df = _prepare_core_features(df)
    df = detect_hammer_hanging(df)
    df = detect_shooting_star(df)
    df = detect_engulfing(df)
    df = detect_piercing_dark_cloud(df)
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
    "BULLISH_HAMMER": "锤子线",
    "HANGING_MAN": "上吊线",
    "SHOOTING_STAR": "射击之星",
    "BULLISH_ENGULFING": "看涨吞没",
    "BEARISH_ENGULFING": "看跌吞没",
    "PIERCING_LINE": "曙光初现",
    "DARK_CLOUD_COVER": "乌云盖顶",
    "DOJI": "十字星",
    "MORNING_STAR": "启明星",
    "EVENING_STAR": "黄昏星",
    "THREE_WHITE_SOLDIERS": "红三兵",
    "THREE_BLACK_CROWS": "三只乌鸦",
    "RISING_THREE_METHODS": "上升三法",
    "FALLING_THREE_METHODS": "下降三法",
    "IMMORTAL_GUIDE": "仙人指路",
    "OLD_DUCK_HEAD": "老鸭头",
    "ACCUMULATION": "量价齐升",
    "DIVERGENCE": "量价背离",
    "EXTREMELY_LOW_VOLUME": "地量地价",
}

BULLISH_PATTERNS = {
    "BULLISH_HAMMER",
    "BULLISH_ENGULFING",
    "PIERCING_LINE",
    "MORNING_STAR",
    "THREE_WHITE_SOLDIERS",
    "RISING_THREE_METHODS",
    "IMMORTAL_GUIDE",
    "OLD_DUCK_HEAD",
    "ACCUMULATION",
}

BEARISH_PATTERNS = {
    "HANGING_MAN",
    "SHOOTING_STAR",
    "BEARISH_ENGULFING",
    "DARK_CLOUD_COVER",
    "EVENING_STAR",
    "THREE_BLACK_CROWS",
    "FALLING_THREE_METHODS",
    "DIVERGENCE",
}

ALL_PATTERN_COLS = list(PATTERN_CN_MAP.keys())


# ============================================================================
# 历史回测 / 训练 / 校准
# ============================================================================

def _iter_symbol_frames(
    df: pd.DataFrame,
    group_col: str | None = "ts_code",
    date_col: str = "trade_date",
) -> Iterable[pd.DataFrame]:
    if df is None or df.empty:
        return []

    work = df.copy()
    if date_col in work.columns:
        work = work.sort_values([c for c in [group_col, date_col] if c and c in work.columns])

    if group_col and group_col in work.columns:
        return (g.reset_index(drop=True) for _, g in work.groupby(group_col, sort=False))
    return [work.reset_index(drop=True)]


def _build_pattern_eval_panel(
    df: pd.DataFrame,
    group_col: str | None = "ts_code",
    date_col: str = "trade_date",
    horizons: Sequence[int] = (3, 5, 10),
) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for frame in _iter_symbol_frames(df, group_col=group_col, date_col=date_col):
        if len(frame) < max(30, max(horizons) + 5):
            continue
        enriched = detect_all_patterns(frame)
        for horizon in horizons:
            future_close = enriched["close"].shift(-int(horizon))
            enriched[f"fwd_ret_{int(horizon)}d"] = _safe_divide(future_close - enriched["close"], enriched["close"], np.nan)
        keep_cols = [c for c in [group_col, date_col, "close"] if c and c in enriched.columns]
        keep_cols += ALL_PATTERN_COLS + [f"fwd_ret_{int(h)}d" for h in horizons]
        parts.append(enriched[keep_cols].copy())

    if not parts:
        cols = [c for c in [group_col, date_col, "close"] if c]
        cols += ALL_PATTERN_COLS + [f"fwd_ret_{int(h)}d" for h in horizons]
        return pd.DataFrame(columns=cols)
    return pd.concat(parts, ignore_index=True)


def _directional_hits(pattern_code: str, returns: pd.Series, threshold: float) -> pd.Series:
    if pattern_code in BULLISH_PATTERNS:
        return returns > threshold
    if pattern_code in BEARISH_PATTERNS:
        return returns < -threshold
    return pd.Series(dtype=bool)


def _directional_edge(pattern_code: str, returns: pd.Series) -> pd.Series:
    if pattern_code in BULLISH_PATTERNS:
        return returns
    if pattern_code in BEARISH_PATTERNS:
        return -returns
    return returns.abs()


def evaluate_pattern_performance(
    df: pd.DataFrame,
    group_col: str | None = "ts_code",
    date_col: str = "trade_date",
    horizons: Sequence[int] = (3, 5, 10),
    min_confidence: float = 0.55,
    positive_return_threshold: float = 0.0,
) -> pd.DataFrame:
    """
    使用历史日线数据评估每种形态的未来表现。

    返回字段示例:
      pattern, sample_count, avg_confidence, hit_rate_5d, avg_ret_5d, directional_edge_5d
    """
    panel = _build_pattern_eval_panel(df, group_col=group_col, date_col=date_col, horizons=horizons)
    if panel.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    primary_horizon = int(horizons[0])

    for pattern_code in ALL_PATTERN_COLS:
        if pattern_code not in panel.columns:
            continue
        sig = panel.loc[panel[pattern_code] >= float(min_confidence)].copy()
        if sig.empty:
            continue

        row: dict[str, Any] = {
            "pattern_code": pattern_code,
            "pattern": PATTERN_CN_MAP[pattern_code],
            "direction": (
                "bullish" if pattern_code in BULLISH_PATTERNS else
                "bearish" if pattern_code in BEARISH_PATTERNS else
                "neutral"
            ),
            "sample_count": int(len(sig)),
            "avg_confidence": round(float(sig[pattern_code].mean()), 4),
        }

        for horizon in horizons:
            col = f"fwd_ret_{int(horizon)}d"
            rets = pd.to_numeric(sig[col], errors="coerce").dropna()
            row[f"sample_{int(horizon)}d"] = int(len(rets))
            if rets.empty:
                row[f"hit_rate_{int(horizon)}d"] = None
                row[f"avg_ret_{int(horizon)}d"] = None
                row[f"directional_edge_{int(horizon)}d"] = None
                row[f"ret_std_{int(horizon)}d"] = None
                continue

            hits = _directional_hits(pattern_code, rets, float(positive_return_threshold))
            edge = _directional_edge(pattern_code, rets)
            row[f"hit_rate_{int(horizon)}d"] = None if hits.empty else round(float(hits.mean()), 4)
            row[f"avg_ret_{int(horizon)}d"] = round(float(rets.mean()), 4)
            row[f"directional_edge_{int(horizon)}d"] = round(float(edge.mean()), 4)
            row[f"ret_std_{int(horizon)}d"] = round(float(rets.std(ddof=0)), 4)

        rows.append(row)

    result = pd.DataFrame(rows)
    if result.empty:
        return result

    sort_col = f"directional_edge_{primary_horizon}d"
    if sort_col not in result.columns:
        return result.sort_values(["sample_count", "avg_confidence"], ascending=[False, False]).reset_index(drop=True)

    return result.sort_values(
        [sort_col, "sample_count", "avg_confidence"],
        ascending=[False, False, False],
        na_position="last",
    ).reset_index(drop=True)


def train_pattern_calibration(
    df: pd.DataFrame,
    group_col: str | None = "ts_code",
    date_col: str = "trade_date",
    horizons: Sequence[int] = (3, 5, 10),
    min_confidence: float = 0.55,
    positive_return_threshold: float = 0.0,
    prior_strength: float = 24.0,
    conf_buckets: Sequence[float] = DEFAULT_CONF_BUCKETS,
) -> dict[str, Any]:
    """
    用历史样本训练形态校准表。

    训练结果可被 get_latest_signals(..., calibration=...) 直接消费。
    """
    panel = _build_pattern_eval_panel(df, group_col=group_col, date_col=date_col, horizons=horizons)
    calibration: dict[str, Any] = {
        "meta": {
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "horizons": [int(h) for h in horizons],
            "min_confidence": float(min_confidence),
            "positive_return_threshold": float(positive_return_threshold),
            "prior_strength": float(prior_strength),
            "confidence_buckets": list(conf_buckets),
            "sample_rows": int(len(panel)),
        },
        "patterns": {},
    }
    if panel.empty:
        return calibration

    direction_priors: dict[tuple[str, int], float] = {}
    for direction in ("bullish", "bearish"):
        direction_codes = [
            c for c in ALL_PATTERN_COLS
            if (direction == "bullish" and c in BULLISH_PATTERNS)
            or (direction == "bearish" and c in BEARISH_PATTERNS)
        ]
        for horizon in horizons:
            samples: list[pd.Series] = []
            for code in direction_codes:
                ser = panel.loc[panel[code] >= float(min_confidence), f"fwd_ret_{int(horizon)}d"]
                ser = pd.to_numeric(ser, errors="coerce").dropna()
                if not ser.empty:
                    samples.append(ser)
            if not samples:
                direction_priors[(direction, int(horizon))] = 0.5
                continue
            merged = pd.concat(samples, ignore_index=True)
            hits = (
                merged > float(positive_return_threshold)
                if direction == "bullish"
                else merged < -float(positive_return_threshold)
            )
            direction_priors[(direction, int(horizon))] = float(hits.mean()) if not hits.empty else 0.5

    for pattern_code in ALL_PATTERN_COLS:
        sig = panel.loc[panel[pattern_code] >= float(min_confidence)].copy()
        if sig.empty:
            continue

        direction = (
            "bullish" if pattern_code in BULLISH_PATTERNS else
            "bearish" if pattern_code in BEARISH_PATTERNS else
            "neutral"
        )
        entry: dict[str, Any] = {
            "pattern": PATTERN_CN_MAP[pattern_code],
            "direction": direction,
            "sample_count": int(len(sig)),
            "avg_confidence": round(float(sig[pattern_code].mean()), 4),
        }

        conf_bucket = pd.cut(
            sig[pattern_code],
            bins=list(conf_buckets),
            labels=[_bucket_confidence(v, conf_buckets) for v in conf_buckets[:-1]],
            right=False,
            include_lowest=True,
        )
        sig["_conf_bucket"] = conf_bucket.astype(str)

        for horizon in horizons:
            col = f"fwd_ret_{int(horizon)}d"
            data = sig[[pattern_code, "_conf_bucket", col]].copy()
            data[col] = pd.to_numeric(data[col], errors="coerce")
            data = data.dropna(subset=[col])
            if data.empty:
                entry[f"{int(horizon)}d"] = {
                    "sample_count": 0,
                    "hit_rate": None,
                    "bayes_hit_rate": None,
                    "avg_ret": None,
                    "directional_edge": None,
                    "buckets": {},
                }
                continue

            if direction == "neutral":
                hits = pd.Series(dtype=bool)
                prior_hit = 0.5
            else:
                hits = _directional_hits(pattern_code, data[col], float(positive_return_threshold))
                prior_hit = direction_priors.get((direction, int(horizon)), 0.5)

            avg_ret = float(data[col].mean())
            edge = float(_directional_edge(pattern_code, data[col]).mean())
            sample_count = int(len(data))
            raw_hit = None if hits.empty else float(hits.mean())
            bayes_hit = None
            if raw_hit is not None:
                bayes_hit = float(
                    (hits.sum() + float(prior_strength) * prior_hit) / (sample_count + float(prior_strength))
                )

            bucket_stats: dict[str, Any] = {}
            for bucket_name, bucket_df in data.groupby("_conf_bucket", dropna=False):
                if bucket_name in ("nan", "None", ""):
                    continue
                sample_bucket = int(len(bucket_df))
                if sample_bucket <= 0:
                    continue

                if direction == "neutral":
                    bucket_hit = None
                    bucket_bayes = None
                else:
                    bucket_hits = _directional_hits(pattern_code, bucket_df[col], float(positive_return_threshold))
                    bucket_hit = float(bucket_hits.mean()) if not bucket_hits.empty else None
                    bucket_bayes = None
                    if bucket_hit is not None:
                        bucket_bayes = float(
                            (bucket_hits.sum() + float(prior_strength) * prior_hit)
                            / (sample_bucket + float(prior_strength))
                        )

                bucket_stats[str(bucket_name)] = {
                    "sample_count": sample_bucket,
                    "hit_rate": None if bucket_hit is None else round(bucket_hit, 4),
                    "bayes_hit_rate": None if bucket_bayes is None else round(bucket_bayes, 4),
                    "avg_ret": round(float(bucket_df[col].mean()), 4),
                    "directional_edge": round(float(_directional_edge(pattern_code, bucket_df[col]).mean()), 4),
                }

            entry[f"{int(horizon)}d"] = {
                "sample_count": sample_count,
                "hit_rate": None if raw_hit is None else round(raw_hit, 4),
                "bayes_hit_rate": None if bayes_hit is None else round(bayes_hit, 4),
                "avg_ret": round(avg_ret, 4),
                "directional_edge": round(edge, 4),
                "buckets": bucket_stats,
            }

        calibration["patterns"][pattern_code] = entry

    return calibration


def save_pattern_calibration(calibration: dict[str, Any], path: str | Path | None = None) -> str:
    import os
    target = Path(path or DEFAULT_CALIBRATION_PATH)
    target.parent.mkdir(parents=True, exist_ok=True)
    temp_path = target.with_suffix(".tmp")
    temp_path.write_text(json.dumps(calibration, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(temp_path, target)
    return str(target)


def build_combined_training_stats(
    df: pd.DataFrame,
    group_col: str | None = "ts_code",
    date_col: str = "trade_date",
    horizons: Sequence[int] = (3, 5, 10),
    min_confidence: float = 0.55,
    positive_return_threshold: float = 0.0,
    prior_strength: float = 24.0,
    conf_buckets: Sequence[float] = DEFAULT_CONF_BUCKETS,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """
    单次遍历版本：同时生成校准表和评估摘要，避免重复计算。
    """
    panel = _build_pattern_eval_panel(df, group_col=group_col, date_col=date_col, horizons=horizons)
    
    # 1. 生成校准表
    calibration: dict[str, Any] = {
        "meta": {
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "horizons": [int(h) for h in horizons],
            "min_confidence": float(min_confidence),
            "positive_return_threshold": float(positive_return_threshold),
            "prior_strength": float(prior_strength),
            "confidence_buckets": list(conf_buckets),
            "sample_rows": int(len(panel)),
        },
        "patterns": {},
    }
    
    if panel.empty:
        return calibration, pd.DataFrame()

    # 先计算 priors
    direction_priors: dict[tuple[str, int], float] = {}
    for direction in ("bullish", "bearish"):
        direction_codes = [
            c for c in ALL_PATTERN_COLS
            if (direction == "bullish" and c in BULLISH_PATTERNS)
            or (direction == "bearish" and c in BEARISH_PATTERNS)
        ]
        for horizon in horizons:
            samples: list[pd.Series] = []
            for code in direction_codes:
                if code in panel.columns:
                    ser = panel.loc[panel[code] >= float(min_confidence), f"fwd_ret_{int(horizon)}d"]
                    ser = pd.to_numeric(ser, errors="coerce").dropna()
                    if not ser.empty:
                        samples.append(ser)
            if not samples:
                direction_priors[(direction, int(horizon))] = 0.5
                continue
            merged = pd.concat(samples, ignore_index=True)
            hits = (
                merged > float(positive_return_threshold)
                if direction == "bullish"
                else merged < -float(positive_return_threshold)
            )
            direction_priors[(direction, int(horizon))] = float(hits.mean()) if not hits.empty else 0.5

    # 循环各个形态计算统计
    rows_eval: list[dict[str, Any]] = []
    primary_horizon = int(horizons[0])

    for pattern_code in ALL_PATTERN_COLS:
        if pattern_code not in panel.columns:
            continue
        sig = panel.loc[panel[pattern_code] >= float(min_confidence)].copy()
        if sig.empty:
            continue

        direction = (
            "bullish" if pattern_code in BULLISH_PATTERNS else
            "bearish" if pattern_code in BEARISH_PATTERNS else
            "neutral"
        )
        
        # 校准表条目
        entry: dict[str, Any] = {
            "pattern": PATTERN_CN_MAP[pattern_code],
            "direction": direction,
            "sample_count": int(len(sig)),
            "avg_confidence": round(float(sig[pattern_code].mean()), 4),
        }
        
        # 评估摘要条目
        row_eval: dict[str, Any] = {
            "pattern_code": pattern_code,
            "pattern": PATTERN_CN_MAP[pattern_code],
            "direction": direction,
            "sample_count": int(len(sig)),
            "avg_confidence": round(float(sig[pattern_code].mean()), 4),
        }

        conf_bucket = pd.cut(
            sig[pattern_code],
            bins=list(conf_buckets),
            labels=[_bucket_confidence(v, conf_buckets) for v in conf_buckets[:-1]],
            right=False,
            include_lowest=True,
        )
        sig["_conf_bucket"] = conf_bucket.astype(str)

        for horizon in horizons:
            col = f"fwd_ret_{int(horizon)}d"
            data = sig[[pattern_code, "_conf_bucket", col]].copy()
            data[col] = pd.to_numeric(data[col], errors="coerce")
            data = data.dropna(subset=[col])
            
            if data.empty:
                entry[f"{int(horizon)}d"] = {
                    "sample_count": 0,
                    "hit_rate": None,
                    "bayes_hit_rate": None,
                    "avg_ret": None,
                    "directional_edge": None,
                    "buckets": {},
                }
                continue

            hits = _directional_hits(pattern_code, data[col], float(positive_return_threshold))
            edge = _directional_edge(pattern_code, data[col])
            prior_hit = direction_priors.get((direction, int(horizon)), 0.5) if direction != "neutral" else 0.5
            
            avg_ret = float(data[col].mean())
            avg_edge = float(edge.mean())
            sample_count = int(len(data))
            raw_hit = None if hits.empty else float(hits.mean())
            bayes_hit = float((hits.sum() + float(prior_strength) * prior_hit) / (sample_count + float(prior_strength))) if raw_hit is not None else None

            # 填充校准表
            bucket_stats: dict[str, Any] = {}
            for bucket_name, bucket_df in data.groupby("_conf_bucket", dropna=False):
                if bucket_name in ("nan", "None", ""): continue
                sb = int(len(bucket_df))
                bhits = _directional_hits(pattern_code, bucket_df[col], float(positive_return_threshold))
                bhit = float(bhits.mean()) if not bhits.empty else None
                bbayes = float((bhits.sum() + float(prior_strength) * prior_hit) / (sb + float(prior_strength))) if bhit is not None else None
                
                bucket_stats[str(bucket_name)] = {
                    "sample_count": sb,
                    "hit_rate": round(bhit, 4) if bhit is not None else None,
                    "bayes_hit_rate": round(bbayes, 4) if bbayes is not None else None,
                    "avg_ret": round(float(bucket_df[col].mean()), 4),
                    "directional_edge": round(float(_directional_edge(pattern_code, bucket_df[col]).mean()), 4),
                }

            entry[f"{int(horizon)}d"] = {
                "sample_count": sample_count,
                "hit_rate": round(raw_hit, 4) if raw_hit is not None else None,
                "bayes_hit_rate": round(bayes_hit, 4) if bayes_hit is not None else None,
                "avg_ret": round(avg_ret, 4),
                "directional_edge": round(avg_edge, 4),
                "buckets": bucket_stats,
            }
            
            # 填充评估摘要
            row_eval[f"sample_{int(horizon)}d"] = sample_count
            row_eval[f"hit_rate_{int(horizon)}d"] = round(raw_hit, 4) if raw_hit is not None else None
            row_eval[f"avg_ret_{int(horizon)}d"] = round(avg_ret, 4)
            row_eval[f"directional_edge_{int(horizon)}d"] = round(avg_edge, 4)

        calibration["patterns"][pattern_code] = entry
        rows_eval.append(row_eval)

    summary = pd.DataFrame(rows_eval)
    if not summary.empty:
        sort_col = f"directional_edge_{primary_horizon}d"
        summary = summary.sort_values([sort_col, "sample_count"], ascending=[False, False], na_position="last").reset_index(drop=True)
    
    return calibration, summary


def load_pattern_calibration(path: str | Path | None = None) -> dict[str, Any]:
    target = Path(path or DEFAULT_CALIBRATION_PATH)
    if not target.exists():
        return {}
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("读取K线形态校准文件失败: %s", exc)
        return {}


def _lookup_calibration(
    calibration: dict[str, Any],
    pattern_code: str,
    confidence: float,
    horizon: int = 5,
) -> dict[str, Any]:
    if not calibration or "patterns" not in calibration:
        return {}

    pattern_stats = calibration.get("patterns", {}).get(pattern_code, {})
    horizon_stats = pattern_stats.get(f"{int(horizon)}d", {})
    if not horizon_stats:
        return {}

    bucket = _bucket_confidence(float(confidence), calibration.get("meta", {}).get("confidence_buckets", DEFAULT_CONF_BUCKETS))
    bucket_stats = horizon_stats.get("buckets", {}).get(bucket, {})
    return bucket_stats or horizon_stats


def get_latest_signals(
    df: pd.DataFrame,
    min_confidence: float = 0.5,
    calibration: dict[str, Any] | None = None,
    calibration_horizon: int = 5,
) -> list[dict[str, Any]]:
    """
    从已计算信号的 DataFrame 中，提取最后一行的触发信号。

    返回:
      [{
        "pattern": "红三兵",
        "code": "THREE_WHITE_SOLDIERS",
        "confidence": 0.87,
        "raw_confidence": 0.82,
        "direction": "bullish",
        "historical_hit_rate": 0.61,
        "historical_avg_ret": 0.028
      }, ...]
    """
    if df is None or df.empty:
        return []

    last = df.iloc[-1]
    signals: list[dict[str, Any]] = []

    for col in ALL_PATTERN_COLS:
        if col not in df.columns:
            continue
        raw_val = last.get(col, 0.0)
        if pd.isna(raw_val) or float(raw_val) < float(min_confidence):
            continue

        direction = (
            "bullish" if col in BULLISH_PATTERNS else
            "bearish" if col in BEARISH_PATTERNS else
            "neutral"
        )
        final_conf = float(raw_val)
        signal: dict[str, Any] = {
            "pattern": PATTERN_CN_MAP[col],
            "code": col,
            "confidence": round(final_conf, 2),
            "raw_confidence": round(float(raw_val), 2),
            "direction": direction,
        }

        if calibration:
            stats = _lookup_calibration(calibration, col, float(raw_val), horizon=int(calibration_horizon))
            hist_hit = stats.get("bayes_hit_rate", stats.get("hit_rate"))
            hist_ret = stats.get("avg_ret")
            if hist_hit is not None:
                final_conf = np.clip(0.75 * float(raw_val) + 0.25 * float(hist_hit), 0.0, 1.0)
                signal["confidence"] = round(float(final_conf), 2)
                signal["historical_hit_rate"] = round(float(hist_hit), 4)
            if hist_ret is not None:
                signal["historical_avg_ret"] = round(float(hist_ret), 4)

        signals.append(signal)

    signals.sort(key=lambda x: (-float(x["confidence"]), -float(x.get("historical_hit_rate", 0.0))))
    return signals

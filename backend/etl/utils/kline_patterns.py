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
import re
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd
from db.connection import fetch_df
from strategy.mainline.config import CATEGORY_WEIGHTS, CONCEPT_MAPPING

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

COMMENTARY_SUBTHEME_RULES = (
    {
        "label": "半导体材料",
        "sector": "半导体",
        "concept_keywords": (
            "半导体", "芯片", "集成电路", "光刻胶", "先进封装", "封装测试",
            "存储芯片", "晶圆", "光芯片", "chiplet", "eda",
        ),
        "industry_keywords": ("化工", "材料", "新材料", "电子化学", "化学制品"),
        "reason": "交易和产业归因更接近国产半导体材料转型方向。"
    },
    {
        "label": "半导体设备",
        "sector": "半导体",
        "concept_keywords": (
            "半导体", "芯片", "集成电路", "光刻", "刻蚀", "检测设备", "先进封装",
        ),
        "industry_keywords": ("专用设备", "自动化设备", "机械设备", "仪器仪表"),
        "reason": "概念落在半导体链条，交易上更按设备/工艺升级理解。"
    },
)


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
    if "body_to_atr" in df.columns:
        return df

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
    if "vol_trend" in df.columns:
        return df

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
    if "ma20_slope_5" in df.columns:
        return df

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
    if "trend_up" in df.columns and "close_rank_20" in df.columns:
        return df

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
    if "trend_up" in df.columns and "body_to_atr" in df.columns:
        return df
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


# ============================================================================
# 兼容层: PatternRecognizer / get_professional_commentary
# ============================================================================

_PATTERN_CALIBRATION_CACHE: dict | None = None
_PATTERN_CALIBRATION_MTIME: float | None = None


def _get_pattern_calibration() -> dict:
    global _PATTERN_CALIBRATION_CACHE, _PATTERN_CALIBRATION_MTIME

    target = Path(DEFAULT_CALIBRATION_PATH)
    current_mtime = target.stat().st_mtime if target.exists() else None
    should_reload = (
        _PATTERN_CALIBRATION_CACHE is None
        or current_mtime != _PATTERN_CALIBRATION_MTIME
    )

    if should_reload:
        _PATTERN_CALIBRATION_CACHE = load_pattern_calibration()
        _PATTERN_CALIBRATION_MTIME = current_mtime
    return _PATTERN_CALIBRATION_CACHE or {}


class PatternRecognizer:
    """
    股票技术形态识别器（兼容旧接口）

    用法:
        recognizer = PatternRecognizer(df)
        patterns = recognizer.recognize()  # -> ["红三兵", "仙人指路", ...]
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy() if df is not None else pd.DataFrame()
        self.signals = []

        if len(self.df) < 5:
            return

        # 统一 volume 列名
        if 'volume' not in self.df.columns and 'vol' in self.df.columns:
            self.df = self.df.rename(columns={'vol': 'volume'})

        # 确保基本均线存在
        for ma in [5, 10, 20, 60]:
            col = f'ma{ma}'
            if col not in self.df.columns:
                self.df[col] = self.df['close'].rolling(ma, min_periods=ma).mean()

    def recognize(self, min_confidence: float = 0.5) -> list:
        """
        识别最新一个交易日的形态，返回中文名列表。

        参数:
            min_confidence: 最低置信度阈值 (0.0~1.0)
        返回:
            ["红三兵", "仙人指路", ...]
        """
        if len(self.df) < 5:
            return []

        try:
            result_df = detect_all_patterns(self.df)
            self.signals = get_latest_signals(
                result_df,
                min_confidence=min_confidence,
                calibration=_get_pattern_calibration(),
            )
            return [s['pattern'] for s in self.signals]
        except Exception as e:
            logger.error(f"形态识别异常: {e}", exc_info=True)
            return []

    def recognize_detailed(self, min_confidence: float = 0.5) -> list:
        """
        返回详细信号列表 (含置信度和方向)。

        返回:
            [{"pattern": "红三兵", "code": "THREE_WHITE_SOLDIERS",
              "confidence": 1.0, "direction": "bullish"}, ...]
        """
        if len(self.df) < 5:
            return []

        try:
            result_df = detect_all_patterns(self.df)
            self.signals = get_latest_signals(
                result_df,
                min_confidence=min_confidence,
                calibration=_get_pattern_calibration(),
            )
            return self.signals
        except Exception as e:
            logger.error(f"形态识别异常: {e}", exc_info=True)
            return []


def _safe_number(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        number = float(value)
        if np.isnan(number) or np.isinf(number):
            return default
        return number
    except (TypeError, ValueError):
        return default


def _nearest_price_level(candidates: Iterable[Any], close: float, direction: str) -> float | None:
    values: list[float] = []
    for candidate in candidates:
        number = _safe_number(candidate)
        if number is None:
            continue
        if direction == "below" and number <= close:
            values.append(number)
        elif direction == "above" and number >= close:
            values.append(number)
    if not values:
        return None
    return max(values) if direction == "below" else min(values)


def _decision_bucket(score: int) -> tuple[str, str, str, str]:
    if score >= 78:
        return "强势偏多", "关注", "高", "趋势跟随"
    if score >= 64:
        return "偏多", "试错", "中高", "回踩低吸"
    if score >= 45:
        return "中性", "观望", "中", "等待确认"
    if score >= 30:
        return "偏空", "减仓", "中", "防守减仓"
    return "弱势", "回避", "高", "风险回避"


def _position_text(action: str, confidence: str) -> str:
    if action == "关注":
        return "可按 50%-70% 的跟踪仓位执行，优先分批处理。"
    if action == "试错":
        return "建议 20%-35% 轻仓试错，确认后再加仓。"
    if action == "观望":
        return "以观察为主，不追价，等待放量突破或回踩企稳。"
    if action == "减仓":
        return "以降低仓位为主，保留少量跟踪仓等待修复。"
    return "空仓或极轻仓处理，优先控制回撤。"


def _clean_theme_token(value: Any) -> str:
    if value is None:
        return ""
    cleaned = str(value).strip().replace("_THS", "")
    for token in ("概念股", "概念", "题材", "板块", "指数", "产业链", "同花顺"):
        cleaned = cleaned.replace(token, "")
    cleaned = re.sub(r"[\s/,_\-]+", "", cleaned)
    return cleaned.upper()


def _match_theme_keyword(values: Sequence[str], keywords: Sequence[str]) -> list[str]:
    matched: list[str] = []
    cleaned_keywords = [_clean_theme_token(keyword) for keyword in keywords if _clean_theme_token(keyword)]
    for raw in values:
        cleaned = _clean_theme_token(raw)
        if not cleaned:
            continue
        if any(keyword in cleaned or cleaned in keyword for keyword in cleaned_keywords):
            matched.append(str(raw))
    return matched


def _infer_primary_sector(industry: str, concepts: Sequence[str]) -> tuple[str, list[str]]:
    tokens = [industry] + list(concepts or [])
    scores: dict[str, float] = {}
    matched: dict[str, list[str]] = {}

    for sector, keywords in CONCEPT_MAPPING.items():
        sector_score = 0.0
        hits: list[str] = []
        sector_keywords = [sector, *(keywords or [])]
        cleaned_keywords = [_clean_theme_token(keyword) for keyword in sector_keywords if _clean_theme_token(keyword)]
        for raw in tokens:
            cleaned = _clean_theme_token(raw)
            if not cleaned:
                continue
            for keyword, original_keyword in zip(cleaned_keywords, sector_keywords):
                if keyword in cleaned or cleaned in keyword:
                    sector_score += len(keyword) * float(CATEGORY_WEIGHTS.get(sector, 1.0))
                    if raw not in hits:
                        hits.append(str(raw))
                    break
        if sector_score > 0:
            scores[sector] = sector_score
            matched[sector] = hits

    if not scores:
        return "", []

    sector = max(scores.items(), key=lambda item: item[1])[0]
    return sector, matched.get(sector, [])


def _classify_commentary_theme(industry: str, concepts: Sequence[str]) -> dict:
    concepts = [str(item).strip() for item in (concepts or []) if str(item).strip()]
    cleaned_industry = _clean_theme_token(industry)
    cleaned_concepts = [_clean_theme_token(item) for item in concepts if _clean_theme_token(item)]

    for rule in COMMENTARY_SUBTHEME_RULES:
        concept_hits = _match_theme_keyword(concepts, rule["concept_keywords"])
        if not concept_hits:
            continue
        if rule.get("industry_keywords"):
            cleaned_industry_hits = [
                keyword for keyword in rule["industry_keywords"]
                if _clean_theme_token(keyword) and _clean_theme_token(keyword) in cleaned_industry
            ]
            if not cleaned_industry_hits:
                continue

        return {
            "sector": rule["sector"],
            "primary_label": rule["label"],
            "matched_concepts": concept_hits[:4],
            "legacy_industry": industry,
            "reason": (
                f"基础行业归在{industry or '待确认'}，"
                f"但概念命中{'、'.join(concept_hits[:3])}，{rule['reason']}"
            ),
            "is_reclassified": True,
        }

    sector, hits = _infer_primary_sector(industry, concepts)
    if sector:
        preferred_label = hits[0] if hits and hits[0] != industry else sector
        return {
            "sector": sector,
            "primary_label": preferred_label,
            "matched_concepts": hits[:4],
            "legacy_industry": industry,
            "reason": (
                f"综合基础行业{industry or '待确认'}与概念标签{'、'.join(hits[:3]) or '暂无'}，"
                f"当前更按{sector}方向理解。"
            ),
            "is_reclassified": bool(industry and preferred_label and preferred_label != industry),
        }

    fallback_label = industry or "待确认"
    return {
        "sector": fallback_label,
        "primary_label": fallback_label,
        "matched_concepts": concepts[:4],
        "legacy_industry": industry,
        "reason": "缺少足够概念标签时，先按基础行业定位。",
        "is_reclassified": False,
    }


@lru_cache(maxsize=1024)
def _load_commentary_context(ts_code: str) -> dict:
    code = str(ts_code or "").strip().upper()
    if not code:
        return {}

    basic_df = fetch_df(
        """
        SELECT ts_code, name, industry
        FROM stock_basic
        WHERE ts_code = ?
        """,
        params=[code],
    )
    concept_df = fetch_df(
        """
        SELECT concept_name
        FROM stock_concept_details
        WHERE ts_code = ?
          AND concept_name IS NOT NULL
        ORDER BY concept_name
        """,
        params=[code],
    )

    stock_name = ""
    industry = ""
    if not basic_df.empty:
        stock_name = str(basic_df.iloc[0].get("name") or "").strip()
        industry = str(basic_df.iloc[0].get("industry") or "").strip()

    concepts = []
    if not concept_df.empty:
        concepts = [
            str(item).strip()
            for item in concept_df["concept_name"].tolist()
            if str(item).strip()
        ]

    classification = _classify_commentary_theme(industry, concepts)
    return {
        "ts_code": code,
        "stock_name": stock_name,
        "industry": industry,
        "concepts": concepts,
        "classification": classification,
    }


def _resolve_commentary_context(context: dict | None) -> dict:
    if not context:
        return {}

    resolved = dict(context)
    ts_code = str(resolved.get("ts_code") or "").strip().upper()
    if ts_code and not resolved.get("classification"):
        cached = _load_commentary_context(ts_code)
        if cached:
            cached.update({k: v for k, v in resolved.items() if v not in (None, "", [], {})})
            return cached
    return resolved


LEVEL_FAMILY_TEXT = {
    "anchor": "短线锚点",
    "trend": "均线成本",
    "range": "区间极值",
    "pivot": "摆动拐点",
    "volume": "成交密集区",
}


def _describe_level_source(source: str, level_type: str) -> tuple[str, str, str, str]:
    normalized = str(source or "").upper()
    if normalized.startswith("MA"):
        window = re.sub(r"[^0-9]", "", normalized) or normalized.replace("MA", "")
        basis = f"{window}日均线"
        definition = f"近{window}个交易日的平均成本线"
        breach_rule = "收盘跌破说明均线支撑削弱" if level_type == "support" else "放量站上才算均线压力化解"
        return basis, definition, breach_rule, "trend"

    if normalized == "PRE_CLOSE":
        basis = "昨收价"
        definition = "上一交易日收盘价，代表短线多空分界"
        breach_rule = "跌回昨收下方说明隔夜优势消失" if level_type == "support" else "重新站回昨收上方才算修复日内弱势"
        return basis, definition, breach_rule, "anchor"

    if normalized == "OPEN":
        basis = "开盘价"
        definition = "当日开盘成本区，适合作为盘中强弱分界"
        breach_rule = "跌破开盘价说明日内承接转弱" if level_type == "support" else "重新站上开盘价才算日内回稳"
        return basis, definition, breach_rule, "anchor"

    if normalized.startswith("LOW_"):
        window = re.sub(r"[^0-9]", "", normalized) or normalized.replace("LOW_", "")
        basis = f"近{window}日最低价"
        definition = f"近{window}日回撤低点"
        breach_rule = "跌破意味着该时间窗防守区被击穿"
        return basis, definition, breach_rule, "range"

    if normalized.startswith("HIGH_"):
        window = re.sub(r"[^0-9]", "", normalized) or normalized.replace("HIGH_", "")
        basis = f"近{window}日最高价"
        definition = f"近{window}日上方阻力区"
        breach_rule = "放量站上才算有效突破"
        return basis, definition, breach_rule, "range"

    if normalized.startswith("PIVOT_LOW"):
        basis = "近期确认摆动低点"
        definition = "局部波段拐点，对应最近一次有效回踩结构"
        breach_rule = "收盘跌破说明最近一轮摆动低点失守"
        return basis, definition, breach_rule, "pivot"

    if normalized.startswith("PIVOT_HIGH"):
        basis = "近期确认摆动高点"
        definition = "局部波段拐点，对应最近一次冲高受阻结构"
        breach_rule = "放量站上说明最近一轮摆动高点被突破"
        return basis, definition, breach_rule, "pivot"

    if normalized.startswith("VP_SUPPORT") or normalized.startswith("VP_RESISTANCE"):
        basis = "近60日成交密集区"
        definition = "按典型价格与成交量聚合后的成本峰值，类似简化筹码峰"
        breach_rule = "跌破说明密集成交区承接被击穿" if level_type == "support" else "放量站上说明上方密集成交区开始松动"
        return basis, definition, breach_rule, "volume"

    basis = normalized or ("支撑位" if level_type == "support" else "压力位")
    definition = "历史量价形成的重要价格带"
    breach_rule = "跌破说明支撑失效" if level_type == "support" else "放量站上才算压力化解"
    return basis, definition, breach_rule, "range"


def _latest_atr_value(df: pd.DataFrame, window: int = 14) -> float | None:
    if "atr14" in df.columns:
        atr_series = pd.to_numeric(df["atr14"], errors="coerce").dropna()
        if not atr_series.empty:
            value = _safe_number(atr_series.iloc[-1])
            if value is not None and value > 0:
                return value

    if not {"high", "low", "close"}.issubset(df.columns):
        return None

    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    prev_close = close.shift(1).fillna(close)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr_series = tr.rolling(window, min_periods=min(5, window)).mean().dropna()
    if atr_series.empty:
        return None
    return _safe_number(atr_series.iloc[-1])


def _compute_level_bands(df: pd.DataFrame, close: float, atr_value: float | None) -> tuple[float, float, float, float, float]:
    if close <= 0:
        return 0.03, 0.05, 0.02, 0.0, 0.0

    swing_pct = 0.0
    if {"high", "low"}.issubset(df.columns):
        recent = df.tail(min(len(df), 10))
        swing = (
            (pd.to_numeric(recent["high"], errors="coerce") - pd.to_numeric(recent["low"], errors="coerce"))
            / pd.to_numeric(recent["close"], errors="coerce").replace(0, np.nan)
        ) * 100.0
        swing = swing.replace([np.inf, -np.inf], np.nan).dropna()
        if not swing.empty:
            swing_pct = float(swing.median())

    atr_pct = (float(atr_value) / close * 100.0) if atr_value and atr_value > 0 else 0.0
    volatility_pct = max(atr_pct, swing_pct, 1.0)

    cluster_band = max(close * float(np.clip(volatility_pct * 0.45 / 100.0, 0.0055, 0.022)), 0.03)
    selection_gap = max(close * float(np.clip(volatility_pct * 0.90 / 100.0, 0.011, 0.040)), 0.05)
    touch_band = max(close * float(np.clip(volatility_pct * 0.35 / 100.0, 0.0045, 0.018)), 0.02)
    return cluster_band, selection_gap, touch_band, atr_pct, swing_pct


def _count_level_touches(
    df: pd.DataFrame,
    price: float,
    level_type: str,
    touch_band: float,
    volume_col: str,
) -> tuple[int, int | None, float]:
    if df.empty:
        return 0, None, 1.0

    recent = df.tail(min(len(df), 30)).reset_index(drop=True)
    close_series = pd.to_numeric(recent.get("close"), errors="coerce")
    low_series = pd.to_numeric(recent.get("low"), errors="coerce")
    high_series = pd.to_numeric(recent.get("high"), errors="coerce")
    if level_type == "support":
        ref_mask = (low_series >= price - touch_band * 1.35) & (low_series <= price + touch_band)
    else:
        ref_mask = (high_series <= price + touch_band * 1.35) & (high_series >= price - touch_band)
    close_mask = (close_series - price).abs() <= touch_band * 0.8
    touch_mask = (ref_mask.fillna(False) | close_mask.fillna(False))

    positions = np.flatnonzero(touch_mask.to_numpy())
    recency_days = int(len(recent) - 1 - positions[-1]) if positions.size else None

    volume_ratio = 1.0
    if volume_col and volume_col in recent.columns:
        volume_series = pd.to_numeric(recent[volume_col], errors="coerce").fillna(0.0)
        baseline = _safe_number(volume_series.replace(0, np.nan).mean(), 0.0) or 0.0
        if baseline > 0 and positions.size:
            touch_volume = _safe_number(volume_series.loc[touch_mask].replace(0, np.nan).mean(), baseline) or baseline
            volume_ratio = touch_volume / baseline if baseline else 1.0

    return int(touch_mask.sum()), recency_days, float(np.clip(volume_ratio, 0.6, 2.2))


def _build_level_candidate(
    df: pd.DataFrame,
    level_type: str,
    source: str,
    price: float | None,
    close: float,
    base_weight: float,
    touch_band: float,
    volume_col: str,
    extra_strength: float = 0.0,
) -> dict | None:
    value = _safe_number(price)
    if value is None or close <= 0:
        return None
    if level_type == "support" and value > close * 1.002:
        return None
    if level_type == "resistance" and value < close * 0.998:
        return None

    distance_pct = abs(close - value) / close * 100.0
    if distance_pct > 18.0:
        return None

    basis, definition, breach_rule, family = _describe_level_source(source, level_type)
    touch_count, recency_days, touch_volume_ratio = _count_level_touches(
        df,
        value,
        level_type=level_type,
        touch_band=touch_band,
        volume_col=volume_col,
    )

    return {
        "type": level_type,
        "source": source,
        "family": family,
        "price": round(value, 2),
        "distance_pct": round(distance_pct, 2),
        "weight": round(float(base_weight) + float(extra_strength or 0.0), 3),
        "basis": basis,
        "definition": definition,
        "breach_rule": breach_rule,
        "touch_count": int(touch_count),
        "recency_days": recency_days,
        "touch_volume_ratio": round(touch_volume_ratio, 2),
    }


def _collect_pivot_level_candidates(
    df: pd.DataFrame,
    close: float,
    level_type: str,
    touch_band: float,
    volume_col: str,
) -> list[dict]:
    if len(df) < 7 or not {"close", "high", "low"}.issubset(df.columns):
        return []

    window = df.tail(min(len(df), 60)).reset_index(drop=True)
    price_series = pd.to_numeric(window["low" if level_type == "support" else "high"], errors="coerce")
    close_series = pd.to_numeric(window["close"], errors="coerce")
    candidates: list[dict] = []
    min_move_pct = max(0.8, touch_band / max(close, EPS) * 100.0)

    for idx in range(2, len(window) - 2):
        value = _safe_number(price_series.iloc[idx])
        if value is None:
            continue

        neighborhood = price_series.iloc[idx - 2: idx + 3]
        if neighborhood.isna().any():
            continue

        if level_type == "support":
            if value > float(neighborhood.min()) + EPS:
                continue
            after = close_series.iloc[idx + 1: min(len(window), idx + 6)]
            move_away_pct = ((after.max() - value) / max(value, EPS) * 100.0) if not after.empty else 0.0
            source = f"PIVOT_LOW_{len(window) - 1 - idx}"
        else:
            if value < float(neighborhood.max()) - EPS:
                continue
            after = close_series.iloc[idx + 1: min(len(window), idx + 6)]
            move_away_pct = ((value - after.min()) / max(value, EPS) * 100.0) if not after.empty else 0.0
            source = f"PIVOT_HIGH_{len(window) - 1 - idx}"

        if move_away_pct < min_move_pct:
            continue

        age = len(window) - 1 - idx
        if age > 45:
            continue

        extra_strength = min(move_away_pct / 6.0, 0.55) + max(0.0, 0.18 - age * 0.004)
        candidate = _build_level_candidate(
            df,
            level_type=level_type,
            source=source,
            price=value,
            close=close,
            base_weight=1.02,
            touch_band=touch_band,
            volume_col=volume_col,
            extra_strength=extra_strength,
        )
        if candidate:
            candidates.append(candidate)

    selected: list[dict] = []
    for item in sorted(
        candidates,
        key=lambda candidate: (
            candidate.get("recency_days") if candidate.get("recency_days") is not None else 999,
            candidate["distance_pct"],
        ),
    ):
        if any(abs(float(item["price"]) - float(prev["price"])) <= touch_band for prev in selected):
            continue
        selected.append(item)
        if len(selected) >= 4:
            break
    return selected


def _collect_volume_profile_level_candidates(
    df: pd.DataFrame,
    close: float,
    level_type: str,
    cluster_band: float,
    touch_band: float,
    volume_col: str,
) -> list[dict]:
    if len(df) < 12 or not volume_col or volume_col not in df.columns or not {"close", "high", "low"}.issubset(df.columns):
        return []

    window = df.tail(min(len(df), 60)).copy()
    typical_price = (
        pd.to_numeric(window["high"], errors="coerce")
        + pd.to_numeric(window["low"], errors="coerce")
        + pd.to_numeric(window["close"], errors="coerce")
    ) / 3.0
    volume = pd.to_numeric(window[volume_col], errors="coerce").fillna(0.0)
    mask = typical_price.notna() & volume.gt(0)
    if int(mask.sum()) < 8:
        return []

    bins = int(np.clip(np.sqrt(mask.sum()) * 2.2, 12, 24))
    weights, edges = np.histogram(typical_price.loc[mask], bins=bins, weights=np.maximum(volume.loc[mask], 1.0))
    if not np.any(weights > 0):
        return []

    centers = (edges[:-1] + edges[1:]) / 2.0
    nonzero_weights = weights[weights > 0]
    weight_threshold = float(np.percentile(nonzero_weights, 60)) if len(nonzero_weights) >= 4 else float(nonzero_weights.mean())

    raw_nodes: list[dict[str, float]] = []
    for idx, weight in enumerate(weights):
        if weight <= 0:
            continue

        center = float(centers[idx])
        if level_type == "support" and center > close + cluster_band * 0.35:
            continue
        if level_type == "resistance" and center < close - cluster_band * 0.35:
            continue

        neighbor_weight = max(
            float(weights[idx - 1]) if idx > 0 else 0.0,
            float(weights[idx + 1]) if idx + 1 < len(weights) else 0.0,
        )
        prominence = float(weight / max(neighbor_weight, 1.0))
        if float(weight) < weight_threshold and prominence < 1.15:
            continue

        raw_nodes.append(
            {
                "price": center,
                "strength": float(weight / max(nonzero_weights.mean(), 1.0)),
                "prominence": prominence,
            }
        )

    selected_nodes: list[dict[str, float]] = []
    for node in sorted(raw_nodes, key=lambda item: (-item["strength"], abs(item["price"] - close))):
        if any(abs(node["price"] - prev["price"]) <= cluster_band for prev in selected_nodes):
            continue
        selected_nodes.append(node)
        if len(selected_nodes) >= 3:
            break

    candidates: list[dict] = []
    prefix = "VP_SUPPORT" if level_type == "support" else "VP_RESISTANCE"
    for idx, node in enumerate(selected_nodes, start=1):
        extra_strength = min(
            max(node["strength"] - 1.0, 0.0) * 0.28 + max(node["prominence"] - 1.0, 0.0) * 0.18,
            0.75,
        )
        candidate = _build_level_candidate(
            df,
            level_type=level_type,
            source=f"{prefix}_{idx}",
            price=node["price"],
            close=close,
            base_weight=1.18,
            touch_band=touch_band,
            volume_col=volume_col,
            extra_strength=extra_strength,
        )
        if candidate:
            candidates.append(candidate)
    return candidates


def _merge_level_clusters(candidates: Sequence[dict], cluster_band: float) -> list[dict]:
    if not candidates:
        return []

    ordered = sorted(candidates, key=lambda item: float(item["price"]))
    merged: list[dict] = []
    for item in ordered:
        existing = next(
            (
                level for level in merged
                if abs(float(level["price"]) - float(item["price"])) <= cluster_band
            ),
            None,
        )
        if existing is None:
            cloned = dict(item)
            cloned["sources"] = [item["source"]]
            cloned["basis_list"] = [item["basis"]]
            cloned["definitions"] = [item["definition"]]
            cloned["breach_rules"] = [item["breach_rule"]]
            cloned["families"] = [item["family"]]
            cloned["dominant_basis"] = item["basis"]
            cloned["dominant_weight"] = float(item["weight"])
            cloned["dominant_family"] = item["family"]
            merged.append(cloned)
            continue

        current_weight = float(existing["weight"])
        incoming_weight = float(item["weight"])
        total_weight = current_weight + incoming_weight
        if total_weight > 0:
            existing["price"] = round(
                (float(existing["price"]) * current_weight + float(item["price"]) * incoming_weight) / total_weight,
                2,
            )
        existing["weight"] = round(total_weight, 3)
        existing["distance_pct"] = min(float(existing["distance_pct"]), float(item["distance_pct"]))
        existing["touch_count"] = max(int(existing.get("touch_count") or 0), int(item.get("touch_count") or 0))
        existing["touch_volume_ratio"] = max(float(existing.get("touch_volume_ratio") or 1.0), float(item.get("touch_volume_ratio") or 1.0))

        recencies = [value for value in (existing.get("recency_days"), item.get("recency_days")) if value is not None]
        existing["recency_days"] = min(recencies) if recencies else None

        for key, field in (
            ("sources", "source"),
            ("basis_list", "basis"),
            ("definitions", "definition"),
            ("breach_rules", "breach_rule"),
            ("families", "family"),
        ):
            values = set(existing.get(key, []))
            values.add(item[field])
            existing[key] = sorted(values)

        if incoming_weight > float(existing.get("dominant_weight", 0.0)):
            existing["dominant_weight"] = incoming_weight
            existing["dominant_basis"] = item["basis"]
            existing["dominant_family"] = item["family"]

    for item in merged:
        recency_days = item.get("recency_days")
        recency_score = 0.2 if recency_days is None else max(0.0, 1.0 - min(float(recency_days), 25.0) / 22.0)
        touch_score = min(float(item.get("touch_count") or 0) / 4.0, 1.6)
        volume_bonus = max(0.0, min(float(item.get("touch_volume_ratio") or 1.0), 2.2) - 0.9)
        distance_score = max(0.0, 1.0 - min(float(item["distance_pct"]), 14.0) / 12.0)
        family_bonus = min(0.24 * len(item.get("families", [])), 0.72)
        item["strength_score"] = round(
            float(item["weight"]) + touch_score * 0.55 + recency_score * 0.40 + volume_bonus * 0.35 + family_bonus,
            3,
        )
        item["selection_score"] = round(float(item["strength_score"]) + distance_score * 0.95, 3)
    return merged


def _select_key_levels(
    close: float,
    candidates: Sequence[dict],
    level_type: str,
    top_n: int = 2,
    cluster_band: float | None = None,
    selection_gap: float | None = None,
) -> list[dict]:
    if not candidates or close <= 0:
        return []

    cluster_band = cluster_band or max(close * 0.006, 0.03)
    selection_gap = selection_gap or max(close * 0.012, 0.05)
    clusters = _merge_level_clusters(candidates, cluster_band=cluster_band)
    if not clusters:
        return []

    ranked = sorted(
        clusters,
        key=lambda item: (
            -float(item["selection_score"]),
            float(item["distance_pct"]),
            -float(item["price"]) if level_type == "support" else float(item["price"]),
        ),
    )

    picked: list[dict] = []
    for item in ranked:
        if any(abs(float(item["price"]) - float(prev["price"])) < selection_gap for prev in picked):
            continue
        picked.append(item)
        if len(picked) >= max(1, int(top_n)):
            break

    if not picked:
        nearest = sorted(clusters, key=lambda item: (float(item["distance_pct"]), -float(item["weight"])))
        picked = nearest[:1]

    ordered = sorted(picked, key=lambda item: float(item["price"]), reverse=level_type == "support")
    selected: list[dict] = []
    for idx, item in enumerate(ordered[:max(1, int(top_n))], start=1):
        basis_text = "、".join(item.get("basis_list", [])[:3]) or str(item.get("dominant_basis") or "")
        family_text = "、".join(
            LEVEL_FAMILY_TEXT.get(family, family) for family in item.get("families", [])
        )
        breach_rules = [str(rule).strip() for rule in item.get("breach_rules", []) if str(rule).strip()]
        breach_rule = breach_rules[0] if breach_rules else ("跌破说明支撑失效" if level_type == "support" else "放量站上才算压力化解")
        direction_text = "下方" if level_type == "support" else "上方"
        role_text = (
            "首要防守位" if level_type == "support" and idx == 1 else
            "次级缓冲位" if level_type == "support" else
            "首个突破确认位" if idx == 1 else
            "第二道阻力位"
        )
        family_suffix = f"（{family_text}）" if family_text else ""
        selected.append(
            {
                "label": f"{'支撑' if level_type == 'support' else '压力'}{idx}",
                "price": round(float(item["price"]), 2),
                "type": level_type,
                "source": item.get("dominant_basis") or basis_text,
                "sources": item.get("sources", []),
                "families": item.get("families", []),
                "dominant_family": item.get("dominant_family"),
                "distance_pct": round(float(item["distance_pct"]), 2),
                "strength_score": round(float(item.get("strength_score") or 0.0), 2),
                "definition": " / ".join(item.get("definitions", [])[:2]),
                "note": (
                    f"来源共振：{basis_text}{family_suffix}；近30日触碰 {int(item.get('touch_count') or 0)} 次，"
                    f"距现价{direction_text}约 {float(item['distance_pct']):.2f}%，按 ATR/振幅带去重后保留为{role_text}。{breach_rule}。"
                ),
            }
        )
    return selected


def build_structural_price_levels_legacy(df: pd.DataFrame, top_n: int = 2) -> dict[str, Any]:
    if df is None or df.empty or "close" not in df.columns:
        return {
            "support_levels": [],
            "resistance_levels": [],
            "level_methodology": [],
            "atr14": None,
            "atr_pct": None,
            "cluster_band": None,
            "selection_gap": None,
        }

    work = _ensure_volume_col(df.copy())
    for col in ("open", "high", "low", "close", "volume", "amount"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    for ma in (5, 10, 20, 60):
        col = f"ma{ma}"
        if col not in work.columns:
            work[col] = work["close"].rolling(ma, min_periods=1).mean()
        else:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    close = _safe_number(work.iloc[-1].get("close"))
    if close is None or close <= 0:
        return {
            "support_levels": [],
            "resistance_levels": [],
            "level_methodology": [],
            "atr14": None,
            "atr_pct": None,
            "cluster_band": None,
            "selection_gap": None,
        }

    atr_value = _latest_atr_value(work)
    cluster_band, selection_gap, touch_band, atr_pct, swing_pct = _compute_level_bands(work, close, atr_value)
    volume_col = "volume" if "volume" in work.columns else ("amount" if "amount" in work.columns else "")
    prev_close = _safe_number(work.iloc[-2]["close"] if len(work) >= 2 else close, close) or close
    open_price = _safe_number(work.iloc[-1].get("open"), close) or close

    last_5 = work.tail(5)
    last_10 = work.tail(10)
    last_20 = work.tail(20)
    last_60 = work.tail(60) if len(work) >= 60 else work

    support_specs = [
        ("PRE_CLOSE", prev_close, 0.72),
        ("OPEN", open_price, 0.66),
        ("MA5", _safe_number(work.iloc[-1].get("ma5")), 0.76),
        ("MA10", _safe_number(work.iloc[-1].get("ma10")), 0.92),
        ("MA20", _safe_number(work.iloc[-1].get("ma20")), 1.06),
        ("MA60", _safe_number(work.iloc[-1].get("ma60")), 1.14),
        ("LOW_5", _safe_number(last_5["low"].min() if "low" in last_5.columns else None), 0.78),
        ("LOW_10", _safe_number(last_10["low"].min() if "low" in last_10.columns else None), 0.92),
        ("LOW_20", _safe_number(last_20["low"].min() if "low" in last_20.columns else None), 1.06),
        ("LOW_60", _safe_number(last_60["low"].min() if "low" in last_60.columns else None), 1.12),
    ]
    resistance_specs = [
        ("PRE_CLOSE", prev_close, 0.72),
        ("OPEN", open_price, 0.66),
        ("MA5", _safe_number(work.iloc[-1].get("ma5")), 0.76),
        ("MA10", _safe_number(work.iloc[-1].get("ma10")), 0.92),
        ("MA20", _safe_number(work.iloc[-1].get("ma20")), 1.06),
        ("MA60", _safe_number(work.iloc[-1].get("ma60")), 1.14),
        ("HIGH_5", _safe_number(last_5["high"].max() if "high" in last_5.columns else None), 0.78),
        ("HIGH_10", _safe_number(last_10["high"].max() if "high" in last_10.columns else None), 0.92),
        ("HIGH_20", _safe_number(last_20["high"].max() if "high" in last_20.columns else None), 1.06),
        ("HIGH_60", _safe_number(last_60["high"].max() if "high" in last_60.columns else None), 1.12),
    ]

    support_candidates: list[dict] = []
    for source, price, weight in support_specs:
        candidate = _build_level_candidate(
            work,
            level_type="support",
            source=source,
            price=price,
            close=close,
            base_weight=weight,
            touch_band=touch_band,
            volume_col=volume_col,
        )
        if candidate:
            support_candidates.append(candidate)

    resistance_candidates: list[dict] = []
    for source, price, weight in resistance_specs:
        candidate = _build_level_candidate(
            work,
            level_type="resistance",
            source=source,
            price=price,
            close=close,
            base_weight=weight,
            touch_band=touch_band,
            volume_col=volume_col,
        )
        if candidate:
            resistance_candidates.append(candidate)

    support_candidates.extend(
        _collect_pivot_level_candidates(
            work,
            close=close,
            level_type="support",
            touch_band=touch_band,
            volume_col=volume_col,
        )
    )
    resistance_candidates.extend(
        _collect_pivot_level_candidates(
            work,
            close=close,
            level_type="resistance",
            touch_band=touch_band,
            volume_col=volume_col,
        )
    )
    support_candidates.extend(
        _collect_volume_profile_level_candidates(
            work,
            close=close,
            level_type="support",
            cluster_band=cluster_band,
            touch_band=touch_band,
            volume_col=volume_col,
        )
    )
    resistance_candidates.extend(
        _collect_volume_profile_level_candidates(
            work,
            close=close,
            level_type="resistance",
            cluster_band=cluster_band,
            touch_band=touch_band,
            volume_col=volume_col,
        )
    )

    support_levels = _select_key_levels(
        close,
        support_candidates,
        "support",
        top_n=top_n,
        cluster_band=cluster_band,
        selection_gap=selection_gap,
    )
    resistance_levels = _select_key_levels(
        close,
        resistance_candidates,
        "resistance",
        top_n=top_n,
        cluster_band=cluster_band,
        selection_gap=selection_gap,
    )

    level_methodology = [
        "算法参考 qlib 常见的 rolling-window 特征工程思路：同时评估 MA5/10/20/60、近 5/10/20/60 日高低点、已确认摆动高低点和近 60 日成交密集区，不再只按离现价最近取值。",
        "去重规则：先用 ATR14 与近 10 日真实振幅生成动态价格带，把过近候选合并为同一价位区，再要求相邻支撑/压力至少间隔一个波动单位，避免两个点位几乎重合。",
        "排序规则：每个价位按来源共振、近 30 日触碰次数、触碰时成交量、最近性和距现价远近综合打分，优先保留更贴近当前博弈区的结构位。",
    ]
    if atr_pct > 0 or swing_pct > 0:
        level_methodology.append(
            f"当前波动刻度：ATR14 约 {atr_pct:.2f}% ，近10日中位振幅约 {swing_pct:.2f}% ，本次聚类带约 {cluster_band / close * 100.0:.2f}% 。"
        )

    return {
        "support_levels": support_levels,
        "resistance_levels": resistance_levels,
        "level_methodology": level_methodology,
        "atr14": round(float(atr_value), 2) if atr_value is not None else None,
        "atr_pct": round(float(atr_pct), 2) if atr_pct else None,
        "cluster_band": round(float(cluster_band), 2),
        "selection_gap": round(float(selection_gap), 2),
    }


LEVEL_BOARD_PROFILES: dict[str, dict[str, Any]] = {
    "default": {
        "label": "主板/其他",
        "cluster_pct_mult": 0.34,
        "cluster_pct_floor": 0.0048,
        "cluster_pct_cap": 0.0160,
        "cluster_atr_min": 0.22,
        "cluster_atr_max": 0.52,
        "selection_pct_mult": 0.66,
        "selection_pct_floor": 0.0085,
        "selection_pct_cap": 0.0240,
        "selection_atr_min": 0.68,
        "selection_atr_max": 1.55,
        "touch_pct_mult": 0.24,
        "touch_pct_floor": 0.0038,
        "touch_pct_cap": 0.0120,
        "touch_atr_min": 0.16,
        "touch_atr_max": 0.38,
        "max_candidate_pct": 10.5,
        "max_candidate_atr_scale": 3.2,
        "primary_target_atr": 1.00,
        "primary_tolerance_atr": 0.92,
        "min_primary_atr": 0.42,
        "max_primary_atr": 2.45,
        "secondary_gap_target_atr": 1.15,
        "min_secondary_gap_atr": 0.55,
        "max_secondary_gap_atr": 2.45,
        "secondary_max_atr": 4.40,
        "touch_tolerance_atr": 0.20,
        "break_tolerance_atr": 0.34,
        "reaction_target_atr": 0.62,
        "volatility_floor_pct": 0.9,
        "family_weight": {
            "anchor": 0.84,
            "trend": 1.04,
            "range": 1.00,
            "pivot": 1.10,
            "volume": 1.12,
        },
        "source_weight": {
            "PRE_CLOSE": 0.76,
            "OPEN": 0.72,
            "MA5": 0.86,
            "MA10": 0.96,
            "MA20": 1.08,
            "MA60": 1.12,
            "LOW_5": 0.84,
            "LOW_10": 0.94,
            "LOW_20": 1.06,
            "LOW_60": 1.10,
            "HIGH_5": 0.84,
            "HIGH_10": 0.94,
            "HIGH_20": 1.06,
            "HIGH_60": 1.10,
        },
        "pivot_weight": 1.10,
        "volume_profile_weight": 1.12,
    },
    "growth": {
        "label": "创业/科创",
        "cluster_pct_mult": 0.30,
        "cluster_pct_floor": 0.0056,
        "cluster_pct_cap": 0.0175,
        "cluster_atr_min": 0.18,
        "cluster_atr_max": 0.46,
        "selection_pct_mult": 0.58,
        "selection_pct_floor": 0.0090,
        "selection_pct_cap": 0.0260,
        "selection_atr_min": 0.78,
        "selection_atr_max": 1.80,
        "touch_pct_mult": 0.21,
        "touch_pct_floor": 0.0043,
        "touch_pct_cap": 0.0130,
        "touch_atr_min": 0.14,
        "touch_atr_max": 0.32,
        "max_candidate_pct": 13.5,
        "max_candidate_atr_scale": 4.0,
        "primary_target_atr": 1.30,
        "primary_tolerance_atr": 1.05,
        "min_primary_atr": 0.55,
        "max_primary_atr": 3.10,
        "secondary_gap_target_atr": 1.45,
        "min_secondary_gap_atr": 0.75,
        "max_secondary_gap_atr": 3.05,
        "secondary_max_atr": 5.30,
        "touch_tolerance_atr": 0.22,
        "break_tolerance_atr": 0.40,
        "reaction_target_atr": 0.78,
        "volatility_floor_pct": 1.1,
        "family_weight": {
            "anchor": 0.78,
            "trend": 1.05,
            "range": 1.00,
            "pivot": 1.14,
            "volume": 1.18,
        },
        "source_weight": {
            "PRE_CLOSE": 0.70,
            "OPEN": 0.66,
            "MA5": 0.82,
            "MA10": 0.94,
            "MA20": 1.10,
            "MA60": 1.12,
            "LOW_5": 0.82,
            "LOW_10": 0.92,
            "LOW_20": 1.08,
            "LOW_60": 1.10,
            "HIGH_5": 0.82,
            "HIGH_10": 0.92,
            "HIGH_20": 1.08,
            "HIGH_60": 1.10,
        },
        "pivot_weight": 1.14,
        "volume_profile_weight": 1.18,
    },
    "gem": {
        "label": "创业板",
        "cluster_pct_mult": 0.30,
        "cluster_pct_floor": 0.0056,
        "cluster_pct_cap": 0.0178,
        "cluster_atr_min": 0.18,
        "cluster_atr_max": 0.46,
        "selection_pct_mult": 0.59,
        "selection_pct_floor": 0.0092,
        "selection_pct_cap": 0.0265,
        "selection_atr_min": 0.78,
        "selection_atr_max": 1.82,
        "touch_pct_mult": 0.21,
        "touch_pct_floor": 0.0043,
        "touch_pct_cap": 0.0130,
        "touch_atr_min": 0.14,
        "touch_atr_max": 0.32,
        "max_candidate_pct": 13.8,
        "max_candidate_atr_scale": 4.0,
        "primary_target_atr": 1.32,
        "primary_tolerance_atr": 1.05,
        "min_primary_atr": 0.56,
        "max_primary_atr": 3.15,
        "secondary_gap_target_atr": 1.46,
        "min_secondary_gap_atr": 0.76,
        "max_secondary_gap_atr": 3.08,
        "secondary_max_atr": 5.40,
        "touch_tolerance_atr": 0.22,
        "break_tolerance_atr": 0.40,
        "reaction_target_atr": 0.78,
        "volatility_floor_pct": 1.1,
        "family_weight": {
            "anchor": 0.78,
            "trend": 1.05,
            "range": 1.00,
            "pivot": 1.15,
            "volume": 1.18,
        },
        "source_weight": {
            "PRE_CLOSE": 0.70,
            "OPEN": 0.66,
            "MA5": 0.82,
            "MA10": 0.94,
            "MA20": 1.10,
            "MA60": 1.12,
            "LOW_5": 0.82,
            "LOW_10": 0.92,
            "LOW_20": 1.08,
            "LOW_60": 1.10,
            "HIGH_5": 0.82,
            "HIGH_10": 0.92,
            "HIGH_20": 1.08,
            "HIGH_60": 1.10,
        },
        "pivot_weight": 1.15,
        "volume_profile_weight": 1.18,
    },
    "star": {
        "label": "科创板",
        "cluster_pct_mult": 0.28,
        "cluster_pct_floor": 0.0052,
        "cluster_pct_cap": 0.0165,
        "cluster_atr_min": 0.17,
        "cluster_atr_max": 0.42,
        "selection_pct_mult": 0.54,
        "selection_pct_floor": 0.0085,
        "selection_pct_cap": 0.0240,
        "selection_atr_min": 0.66,
        "selection_atr_max": 1.55,
        "touch_pct_mult": 0.20,
        "touch_pct_floor": 0.0040,
        "touch_pct_cap": 0.0125,
        "touch_atr_min": 0.14,
        "touch_atr_max": 0.30,
        "max_candidate_pct": 12.8,
        "max_candidate_atr_scale": 3.7,
        "primary_target_atr": 1.08,
        "primary_tolerance_atr": 0.90,
        "min_primary_atr": 0.48,
        "max_primary_atr": 2.75,
        "secondary_gap_target_atr": 1.28,
        "min_secondary_gap_atr": 0.64,
        "max_secondary_gap_atr": 2.65,
        "secondary_max_atr": 4.80,
        "touch_tolerance_atr": 0.22,
        "break_tolerance_atr": 0.40,
        "reaction_target_atr": 0.78,
        "volatility_floor_pct": 1.0,
        "family_weight": {
            "anchor": 0.74,
            "trend": 1.10,
            "range": 0.96,
            "pivot": 1.06,
            "volume": 1.20,
        },
        "source_weight": {
            "PRE_CLOSE": 0.68,
            "OPEN": 0.62,
            "MA5": 0.78,
            "MA10": 0.94,
            "MA20": 1.12,
            "MA60": 1.16,
            "LOW_5": 0.76,
            "LOW_10": 0.90,
            "LOW_20": 1.00,
            "LOW_60": 1.08,
            "HIGH_5": 0.76,
            "HIGH_10": 0.90,
            "HIGH_20": 1.00,
            "HIGH_60": 1.08,
        },
        "pivot_weight": 1.08,
        "volume_profile_weight": 1.22,
    },
}

LEVEL_BACKTEST_BOARD_SCOPES = {
    "growth": {
        "label": "创业板+科创板",
        "where_sql": "b.market IN ('创业板', '科创板')",
    },
    "gem": {
        "label": "创业板",
        "where_sql": "b.market = '创业板'",
    },
    "star": {
        "label": "科创板",
        "where_sql": "b.market = '科创板'",
    },
    "all": {
        "label": "全市场",
        "where_sql": "1 = 1",
    },
}


def _infer_level_market_board(
    df: pd.DataFrame | None = None,
    ts_code: str | None = None,
    market: str | None = None,
) -> str:
    market_text = str(market or "").strip()
    if market_text == "创业板":
        return "gem"
    if market_text == "科创板":
        return "star"

    code = str(ts_code or "").strip().upper()
    if not code and df is not None and not df.empty:
        if "ts_code" in df.columns:
            code = str(df.iloc[-1].get("ts_code") or "").strip().upper()
        elif "symbol" in df.columns:
            code = str(df.iloc[-1].get("symbol") or "").strip().upper()

    symbol = code.split(".", 1)[0]
    if symbol.startswith(("300", "301")):
        return "gem"
    if symbol.startswith("688"):
        return "star"
    return "default"


def _resolve_level_board_profile(market_board: str | None = None) -> dict[str, Any]:
    key = str(market_board or "default").strip().lower()
    profile = dict(LEVEL_BOARD_PROFILES.get(key, LEVEL_BOARD_PROFILES["default"]))
    profile["key"] = key if key in LEVEL_BOARD_PROFILES else "default"
    return profile


def _infer_level_regime(df: pd.DataFrame) -> dict[str, Any]:
    work = _ensure_volume_col(df.copy())
    for ma in (20, 60):
        col = f"ma{ma}"
        if col not in work.columns:
            work[col] = pd.to_numeric(work["close"], errors="coerce").rolling(ma, min_periods=1).mean()
        else:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    close = _safe_number(work.iloc[-1].get("close"), 0.0) or 0.0
    ma20 = _safe_number(work.iloc[-1].get("ma20"), close) or close
    ma60 = _safe_number(work.iloc[-1].get("ma60"), ma20) or ma20
    ma20_prev = _safe_number(work.iloc[-5].get("ma20"), ma20) if len(work) >= 5 else ma20
    slope20 = ((ma20 - ma20_prev) / max(abs(ma20_prev), EPS) * 100.0) if ma20_prev else 0.0

    trend_up = close >= ma20 and ma20 >= ma60 * 0.995 and slope20 >= 0
    trend_down = close <= ma20 and ma20 <= ma60 * 1.005 and slope20 <= 0
    return {
        "trend_up": bool(trend_up),
        "trend_down": bool(trend_down),
        "slope20": round(float(slope20), 3),
    }


def _resolve_source_weight(source: str, family: str, market_profile: dict[str, Any]) -> float:
    normalized = str(source or "").upper()
    source_weight = market_profile.get("source_weight", {})
    if normalized in source_weight:
        return float(source_weight[normalized])
    if normalized.startswith("PIVOT_"):
        return float(market_profile.get("pivot_weight", market_profile.get("family_weight", {}).get("pivot", 1.10)))
    if normalized.startswith("VP_"):
        return float(market_profile.get("volume_profile_weight", market_profile.get("family_weight", {}).get("volume", 1.12)))
    return float(market_profile.get("family_weight", {}).get(family, 1.0))


def _resolve_trend_weight(
    level_type: str,
    family: str,
    source: str,
    regime: dict[str, Any],
) -> float:
    normalized = str(source or "").upper()
    if regime.get("trend_up"):
        if level_type == "support":
            if family in {"trend", "pivot", "volume"}:
                return 1.10
            if family == "anchor" or normalized in {"MA5", "PRE_CLOSE", "OPEN"}:
                return 0.82
        if level_type == "resistance" and family == "anchor":
            return 0.84
    elif regime.get("trend_down"):
        if level_type == "support":
            if family == "anchor" or normalized in {"MA5", "PRE_CLOSE", "OPEN"}:
                return 0.68
            if family in {"trend", "range"}:
                return 0.88
            if family in {"pivot", "volume"}:
                return 0.92
        else:
            if family in {"trend", "pivot", "volume"}:
                return 1.10
            if family == "anchor":
                return 0.84
    elif family == "anchor":
        return 0.90
    return 1.0


def _compute_level_bands_adaptive(
    df: pd.DataFrame,
    close: float,
    atr_value: float | None,
    market_profile: dict[str, Any],
) -> tuple[float, float, float, float, float, float]:
    swing_pct = 0.0
    if {"high", "low"}.issubset(df.columns):
        recent = df.tail(min(len(df), 10))
        swing = (
            (pd.to_numeric(recent["high"], errors="coerce") - pd.to_numeric(recent["low"], errors="coerce"))
            / pd.to_numeric(recent["close"], errors="coerce").replace(0, np.nan)
        ) * 100.0
        swing = swing.replace([np.inf, -np.inf], np.nan).dropna()
        if not swing.empty:
            swing_pct = float(swing.median())

    atr_pct = (float(atr_value) / close * 100.0) if atr_value and atr_value > 0 and close > 0 else 0.0
    volatility_pct = max(atr_pct, swing_pct, float(market_profile.get("volatility_floor_pct", 1.0)))

    cluster_band = max(
        close
        * float(
            np.clip(
                volatility_pct * float(market_profile["cluster_pct_mult"]) / 100.0,
                float(market_profile["cluster_pct_floor"]),
                float(market_profile["cluster_pct_cap"]),
            )
        ),
        0.03,
    )
    selection_gap = max(
        close
        * float(
            np.clip(
                volatility_pct * float(market_profile["selection_pct_mult"]) / 100.0,
                float(market_profile["selection_pct_floor"]),
                float(market_profile["selection_pct_cap"]),
            )
        ),
        0.05,
    )
    touch_band = max(
        close
        * float(
            np.clip(
                volatility_pct * float(market_profile["touch_pct_mult"]) / 100.0,
                float(market_profile["touch_pct_floor"]),
                float(market_profile["touch_pct_cap"]),
            )
        ),
        0.02,
    )

    if atr_value and atr_value > 0:
        cluster_band = float(
            np.clip(
                cluster_band,
                atr_value * float(market_profile["cluster_atr_min"]),
                atr_value * float(market_profile["cluster_atr_max"]),
            )
        )
        selection_gap = float(
            np.clip(
                selection_gap,
                atr_value * float(market_profile["selection_atr_min"]),
                atr_value * float(market_profile["selection_atr_max"]),
            )
        )
        touch_band = float(
            np.clip(
                touch_band,
                atr_value * float(market_profile["touch_atr_min"]),
                atr_value * float(market_profile["touch_atr_max"]),
            )
        )

    return cluster_band, selection_gap, touch_band, atr_pct, swing_pct, volatility_pct


def _adaptive_distance_limit_pct(
    atr_pct: float,
    volatility_pct: float,
    market_profile: dict[str, Any],
) -> float:
    base = max(
        atr_pct * float(market_profile.get("max_candidate_atr_scale", 3.2)),
        volatility_pct * 1.7,
        float(market_profile.get("volatility_floor_pct", 1.0)) * 4.0,
    )
    return float(np.clip(base, 5.5, float(market_profile["max_candidate_pct"])))


def _build_level_candidate_adaptive(
    df: pd.DataFrame,
    level_type: str,
    source: str,
    price: float | None,
    close: float,
    base_weight: float,
    touch_band: float,
    volume_col: str,
    market_profile: dict[str, Any],
    regime: dict[str, Any],
    atr_value: float | None,
    atr_pct: float,
    volatility_pct: float,
    extra_strength: float = 0.0,
) -> dict | None:
    candidate = _build_level_candidate(
        df,
        level_type=level_type,
        source=source,
        price=price,
        close=close,
        base_weight=base_weight,
        touch_band=touch_band,
        volume_col=volume_col,
        extra_strength=extra_strength,
    )
    if not candidate:
        return None

    max_distance_pct = _adaptive_distance_limit_pct(atr_pct, volatility_pct, market_profile)
    if float(candidate["distance_pct"]) > max_distance_pct:
        return None

    family = str(candidate.get("family") or "")
    source_weight = _resolve_source_weight(source, family, market_profile)
    trend_weight = _resolve_trend_weight(level_type, family, source, regime)
    adjusted_weight = float(candidate["weight"]) * source_weight * trend_weight

    distance_atr = None
    if atr_value and atr_value > 0:
        distance_atr = abs(close - float(candidate["price"])) / atr_value
        if distance_atr < float(market_profile["min_primary_atr"]) * 0.35:
            adjusted_weight *= 0.76
        if distance_atr > float(market_profile["secondary_max_atr"]) * 1.12:
            return None

    candidate["distance_atr"] = round(float(distance_atr), 2) if distance_atr is not None else None
    candidate["weight"] = round(float(adjusted_weight), 3)
    candidate["market_board"] = market_profile.get("key")
    return candidate


def _distance_fit_bonus(distance_atr: float | None, market_profile: dict[str, Any]) -> float:
    if distance_atr is None:
        return 0.0
    if distance_atr < float(market_profile["min_primary_atr"]) * 0.45:
        return -0.85
    if distance_atr > float(market_profile["secondary_max_atr"]) * 1.08:
        return -0.85
    target = float(market_profile["primary_target_atr"])
    tolerance = max(float(market_profile["primary_tolerance_atr"]), 0.4)
    deviation = abs(distance_atr - target)
    score = 1.0 - deviation / tolerance
    return float(np.clip(score, -0.75, 1.0) * 0.92)


def _spacing_fit_bonus(spacing_atr: float | None, market_profile: dict[str, Any]) -> float:
    if spacing_atr is None:
        return 0.0
    target = float(market_profile["secondary_gap_target_atr"])
    tolerance = max(
        (float(market_profile["max_secondary_gap_atr"]) - float(market_profile["min_secondary_gap_atr"])) / 1.9,
        0.55,
    )
    deviation = abs(spacing_atr - target)
    score = 1.0 - deviation / tolerance
    return float(np.clip(score, -0.70, 1.0) * 0.75)


def _select_key_levels_adaptive(
    close: float,
    candidates: Sequence[dict],
    level_type: str,
    market_profile: dict[str, Any],
    atr_value: float | None,
    top_n: int = 2,
    cluster_band: float | None = None,
    selection_gap: float | None = None,
) -> list[dict]:
    if not candidates or close <= 0:
        return []

    cluster_band = cluster_band or max(close * 0.006, 0.03)
    selection_gap = selection_gap or max(close * 0.012, 0.05)
    clusters = _merge_level_clusters(candidates, cluster_band=cluster_band)
    if not clusters:
        return []

    for item in clusters:
        distance_atr = None
        if atr_value and atr_value > 0:
            distance_atr = abs(close - float(item["price"])) / atr_value
        item["distance_atr"] = round(float(distance_atr), 2) if distance_atr is not None else None
        item["actionable_score"] = round(
            float(item["selection_score"]) + _distance_fit_bonus(distance_atr, market_profile),
            3,
        )

    ranked = sorted(
        clusters,
        key=lambda item: (
            -float(item["actionable_score"]),
            abs((item.get("distance_atr") or 99.0) - float(market_profile["primary_target_atr"])),
            float(item["distance_pct"]),
            -float(item["price"]) if level_type == "support" else float(item["price"]),
        ),
    )

    picked: list[dict] = []
    secondary_candidates: list[dict] = []
    atr_gap_base = atr_value if atr_value and atr_value > 0 else max(close * 0.01, 0.01)

    for item in ranked:
        if not picked:
            distance_atr = item.get("distance_atr")
            if distance_atr is not None and distance_atr > float(market_profile["max_primary_atr"]) * 1.15:
                continue
            picked.append(item)
            if len(picked) >= max(1, int(top_n)):
                break
            continue

        if level_type == "support" and float(item["price"]) >= float(picked[-1]["price"]):
            continue
        if level_type == "resistance" and float(item["price"]) <= float(picked[-1]["price"]):
            continue

        if any(abs(float(item["price"]) - float(prev["price"])) < selection_gap for prev in picked):
            continue

        spacing_atr = abs(float(item["price"]) - float(picked[-1]["price"])) / atr_gap_base
        if spacing_atr < float(market_profile["min_secondary_gap_atr"]):
            continue

        item["actionable_score"] = round(
            float(item["actionable_score"]) + _spacing_fit_bonus(spacing_atr, market_profile),
            3,
        )
        if spacing_atr > float(market_profile["max_secondary_gap_atr"]):
            secondary_candidates.append(item)
            continue

        picked.append(item)
        if len(picked) >= max(1, int(top_n)):
            break

    if picked and len(picked) < max(1, int(top_n)) and secondary_candidates:
        secondary_candidates = sorted(
            secondary_candidates,
            key=lambda item: (
                -float(item["actionable_score"]),
                float(item["distance_pct"]),
            ),
        )
        picked.append(secondary_candidates[0])

    if not picked:
        picked = ranked[:1]

    ordered = sorted(picked, key=lambda item: float(item["price"]), reverse=level_type == "support")
    selected: list[dict] = []
    for idx, item in enumerate(ordered[:max(1, int(top_n))], start=1):
        basis_text = "、".join(item.get("basis_list", [])[:3]) or str(item.get("dominant_basis") or "")
        family_text = "、".join(
            LEVEL_FAMILY_TEXT.get(family, family) for family in item.get("families", [])
        )
        breach_rules = [str(rule).strip() for rule in item.get("breach_rules", []) if str(rule).strip()]
        breach_rule = breach_rules[0] if breach_rules else (
            "跌破说明支撑失效" if level_type == "support" else "放量站上才算压力化解"
        )
        direction_text = "下方" if level_type == "support" else "上方"
        role_text = (
            "首要防守位" if level_type == "support" and idx == 1 else
            "次级缓冲位" if level_type == "support" else
            "首个突破确认位" if idx == 1 else
            "第二道阻力位"
        )
        family_suffix = f"（{family_text}）" if family_text else ""
        distance_atr = item.get("distance_atr")
        board_label = market_profile.get("label", "当前板块")
        distance_atr_text = f" / {distance_atr:.2f} ATR" if distance_atr is not None else ""
        selected.append(
            {
                "label": f"{'支撑' if level_type == 'support' else '压力'}{idx}",
                "price": round(float(item["price"]), 2),
                "type": level_type,
                "source": item.get("dominant_basis") or basis_text,
                "sources": item.get("sources", []),
                "families": item.get("families", []),
                "dominant_family": item.get("dominant_family"),
                "distance_pct": round(float(item["distance_pct"]), 2),
                "distance_atr": round(float(distance_atr), 2) if distance_atr is not None else None,
                "strength_score": round(float(item.get("strength_score") or 0.0), 2),
                "definition": " / ".join(item.get("definitions", [])[:2]),
                "note": (
                    f"来源共振：{basis_text}{family_suffix}；近30日触碰 {int(item.get('touch_count') or 0)} 次，"
                    f"距现价{direction_text}约 {float(item['distance_pct']):.2f}%{distance_atr_text}，"
                    f"并按 {board_label} 波动画像约束首位/二位间距后保留为{role_text}。{breach_rule}。"
                ),
            }
        )
    return selected


def build_structural_price_levels(
    df: pd.DataFrame,
    top_n: int = 2,
    market_board: str | None = None,
    profile: str = "adaptive",
) -> dict[str, Any]:
    normalized_market_board = str(market_board or "").strip().lower()
    inferred_board = (
        normalized_market_board
        if normalized_market_board in LEVEL_BOARD_PROFILES
        else _infer_level_market_board(df, market=market_board)
    )
    if str(profile or "adaptive").lower() in {"legacy", "baseline"}:
        legacy = build_structural_price_levels_legacy(df, top_n=top_n)
        legacy["market_board"] = inferred_board
        return legacy

    if df is None or df.empty or "close" not in df.columns:
        return {
            "support_levels": [],
            "resistance_levels": [],
            "level_methodology": [],
            "atr14": None,
            "atr_pct": None,
            "cluster_band": None,
            "selection_gap": None,
            "market_board": inferred_board,
        }

    work = _ensure_volume_col(df.copy())
    for col in ("open", "high", "low", "close", "volume", "amount"):
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    for ma in (5, 10, 20, 60):
        col = f"ma{ma}"
        if col not in work.columns:
            work[col] = work["close"].rolling(ma, min_periods=1).mean()
        else:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    close = _safe_number(work.iloc[-1].get("close"))
    if close is None or close <= 0:
        return build_structural_price_levels_legacy(df, top_n=top_n)

    if normalized_market_board in LEVEL_BOARD_PROFILES:
        inferred_market_board = normalized_market_board
    else:
        inferred_market_board = _infer_level_market_board(work, market=market_board)
    market_profile = _resolve_level_board_profile(inferred_market_board)
    regime = _infer_level_regime(work)
    atr_value = _latest_atr_value(work)
    (
        cluster_band,
        selection_gap,
        touch_band,
        atr_pct,
        swing_pct,
        volatility_pct,
    ) = _compute_level_bands_adaptive(work, close, atr_value, market_profile)
    volume_col = "volume" if "volume" in work.columns else ("amount" if "amount" in work.columns else "")
    prev_close = _safe_number(work.iloc[-2]["close"] if len(work) >= 2 else close, close) or close
    open_price = _safe_number(work.iloc[-1].get("open"), close) or close

    last_5 = work.tail(5)
    last_10 = work.tail(10)
    last_20 = work.tail(20)
    last_60 = work.tail(60) if len(work) >= 60 else work

    support_specs = [
        ("PRE_CLOSE", prev_close, 0.72),
        ("OPEN", open_price, 0.66),
        ("MA5", _safe_number(work.iloc[-1].get("ma5")), 0.76),
        ("MA10", _safe_number(work.iloc[-1].get("ma10")), 0.92),
        ("MA20", _safe_number(work.iloc[-1].get("ma20")), 1.06),
        ("MA60", _safe_number(work.iloc[-1].get("ma60")), 1.14),
        ("LOW_5", _safe_number(last_5["low"].min() if "low" in last_5.columns else None), 0.78),
        ("LOW_10", _safe_number(last_10["low"].min() if "low" in last_10.columns else None), 0.92),
        ("LOW_20", _safe_number(last_20["low"].min() if "low" in last_20.columns else None), 1.06),
        ("LOW_60", _safe_number(last_60["low"].min() if "low" in last_60.columns else None), 1.12),
    ]
    resistance_specs = [
        ("PRE_CLOSE", prev_close, 0.72),
        ("OPEN", open_price, 0.66),
        ("MA5", _safe_number(work.iloc[-1].get("ma5")), 0.76),
        ("MA10", _safe_number(work.iloc[-1].get("ma10")), 0.92),
        ("MA20", _safe_number(work.iloc[-1].get("ma20")), 1.06),
        ("MA60", _safe_number(work.iloc[-1].get("ma60")), 1.14),
        ("HIGH_5", _safe_number(last_5["high"].max() if "high" in last_5.columns else None), 0.78),
        ("HIGH_10", _safe_number(last_10["high"].max() if "high" in last_10.columns else None), 0.92),
        ("HIGH_20", _safe_number(last_20["high"].max() if "high" in last_20.columns else None), 1.06),
        ("HIGH_60", _safe_number(last_60["high"].max() if "high" in last_60.columns else None), 1.12),
    ]

    support_candidates: list[dict] = []
    for source, price, weight in support_specs:
        candidate = _build_level_candidate_adaptive(
            work,
            level_type="support",
            source=source,
            price=price,
            close=close,
            base_weight=weight,
            touch_band=touch_band,
            volume_col=volume_col,
            market_profile=market_profile,
            regime=regime,
            atr_value=atr_value,
            atr_pct=atr_pct,
            volatility_pct=volatility_pct,
        )
        if candidate:
            support_candidates.append(candidate)

    resistance_candidates: list[dict] = []
    for source, price, weight in resistance_specs:
        candidate = _build_level_candidate_adaptive(
            work,
            level_type="resistance",
            source=source,
            price=price,
            close=close,
            base_weight=weight,
            touch_band=touch_band,
            volume_col=volume_col,
            market_profile=market_profile,
            regime=regime,
            atr_value=atr_value,
            atr_pct=atr_pct,
            volatility_pct=volatility_pct,
        )
        if candidate:
            resistance_candidates.append(candidate)

    for candidate in _collect_pivot_level_candidates(
        work,
        close=close,
        level_type="support",
        touch_band=touch_band,
        volume_col=volume_col,
    ):
        adaptive_candidate = _build_level_candidate_adaptive(
            work,
            level_type="support",
            source=str(candidate["source"]),
            price=_safe_number(candidate["price"]),
            close=close,
            base_weight=float(candidate["weight"]),
            touch_band=touch_band,
            volume_col=volume_col,
            market_profile=market_profile,
            regime=regime,
            atr_value=atr_value,
            atr_pct=atr_pct,
            volatility_pct=volatility_pct,
        )
        if adaptive_candidate:
            support_candidates.append(adaptive_candidate)
    for candidate in _collect_pivot_level_candidates(
        work,
        close=close,
        level_type="resistance",
        touch_band=touch_band,
        volume_col=volume_col,
    ):
        adaptive_candidate = _build_level_candidate_adaptive(
            work,
            level_type="resistance",
            source=str(candidate["source"]),
            price=_safe_number(candidate["price"]),
            close=close,
            base_weight=float(candidate["weight"]),
            touch_band=touch_band,
            volume_col=volume_col,
            market_profile=market_profile,
            regime=regime,
            atr_value=atr_value,
            atr_pct=atr_pct,
            volatility_pct=volatility_pct,
        )
        if adaptive_candidate:
            resistance_candidates.append(adaptive_candidate)
    for candidate in _collect_volume_profile_level_candidates(
        work,
        close=close,
        level_type="support",
        cluster_band=cluster_band,
        touch_band=touch_band,
        volume_col=volume_col,
    ):
        adaptive_candidate = _build_level_candidate_adaptive(
            work,
            level_type="support",
            source=str(candidate["source"]),
            price=_safe_number(candidate["price"]),
            close=close,
            base_weight=float(candidate["weight"]),
            touch_band=touch_band,
            volume_col=volume_col,
            market_profile=market_profile,
            regime=regime,
            atr_value=atr_value,
            atr_pct=atr_pct,
            volatility_pct=volatility_pct,
        )
        if adaptive_candidate:
            support_candidates.append(adaptive_candidate)
    for candidate in _collect_volume_profile_level_candidates(
        work,
        close=close,
        level_type="resistance",
        cluster_band=cluster_band,
        touch_band=touch_band,
        volume_col=volume_col,
    ):
        adaptive_candidate = _build_level_candidate_adaptive(
            work,
            level_type="resistance",
            source=str(candidate["source"]),
            price=_safe_number(candidate["price"]),
            close=close,
            base_weight=float(candidate["weight"]),
            touch_band=touch_band,
            volume_col=volume_col,
            market_profile=market_profile,
            regime=regime,
            atr_value=atr_value,
            atr_pct=atr_pct,
            volatility_pct=volatility_pct,
        )
        if adaptive_candidate:
            resistance_candidates.append(adaptive_candidate)

    support_levels = _select_key_levels_adaptive(
        close,
        support_candidates,
        "support",
        market_profile=market_profile,
        atr_value=atr_value,
        top_n=top_n,
        cluster_band=cluster_band,
        selection_gap=selection_gap,
    )
    resistance_levels = _select_key_levels_adaptive(
        close,
        resistance_candidates,
        "resistance",
        market_profile=market_profile,
        atr_value=atr_value,
        top_n=top_n,
        cluster_band=cluster_band,
        selection_gap=selection_gap,
    )

    if not support_levels and not resistance_levels:
        legacy = build_structural_price_levels_legacy(df, top_n=top_n)
        legacy["market_board"] = inferred_market_board
        legacy["level_methodology"] = list(legacy.get("level_methodology") or [])
        legacy["level_methodology"].append("自适应画像未产出有效结构位，本次回退 legacy 结果。")
        return legacy

    level_methodology = [
        "算法仍同时评估 MA5/10/20/60、近 5/10/20/60 日高低点、已确认摆动高低点与近 60 日成交密集区，但不再只看离现价最近。",
        f"自适应校准：按 {market_profile['label']} 波动画像，把聚类带、首要点位距离和二级间距统一换算到 ATR/振幅刻度，减少跨度过大或过小。",
        "排序规则：候选价位先按来源共振、触碰次数、量能与最近性打分，再叠加“离现价是否处在可交易 ATR 区间”的约束，优先保留更像回踩/回落观察位的结构点。",
    ]
    if atr_pct > 0 or swing_pct > 0:
        level_methodology.append(
            f"当前波动刻度：ATR14 约 {atr_pct:.2f}% ，近10日中位振幅约 {swing_pct:.2f}% ，"
            f"本次聚类带约 {cluster_band / close * 100.0:.2f}% ，相邻点位最小间距约 {selection_gap / close * 100.0:.2f}% 。"
        )

    return {
        "support_levels": support_levels,
        "resistance_levels": resistance_levels,
        "level_methodology": level_methodology,
        "atr14": round(float(atr_value), 2) if atr_value is not None else None,
        "atr_pct": round(float(atr_pct), 2) if atr_pct else None,
        "cluster_band": round(float(cluster_band), 2),
        "selection_gap": round(float(selection_gap), 2),
        "market_board": inferred_market_board,
    }


def _resolve_backtest_board_scope(board: str) -> tuple[str, dict[str, str]]:
    key = str(board or "growth").strip().lower()
    if key in {"gem_star", "growth", "growth_boards"}:
        key = "growth"
    scope = LEVEL_BACKTEST_BOARD_SCOPES.get(key, LEVEL_BACKTEST_BOARD_SCOPES["growth"])
    return key, scope


def _select_level_backtest_universe(
    board: str = "growth",
    sample_size: int = 60,
    liquidity_days: int = 40,
) -> list[dict[str, Any]]:
    board_key, scope = _resolve_backtest_board_scope(board)
    sample_size = max(10, min(int(sample_size or 60), 240))
    liquidity_days = max(20, min(int(liquidity_days or 40), 90))
    min_trade_days = max(10, int(liquidity_days * 0.7))

    df = fetch_df(
        f"""
        WITH recent_dates AS (
            SELECT trade_date
            FROM daily_price
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT ?
        )
        SELECT
            d.ts_code,
            MAX(b.name) AS name,
            MAX(b.market) AS market,
            AVG(COALESCE(d.amount, 0)) AS avg_amount,
            COUNT(*) AS trade_days
        FROM daily_price d
        JOIN stock_basic b ON d.ts_code = b.ts_code
        WHERE d.trade_date IN (SELECT trade_date FROM recent_dates)
          AND {scope['where_sql']}
        GROUP BY d.ts_code
        HAVING COUNT(*) >= ?
        ORDER BY avg_amount DESC, ts_code
        LIMIT ?
        """,
        params=[liquidity_days, min_trade_days, sample_size],
    )
    if df.empty:
        return []
    records = df.to_dict("records")
    for item in records:
        item["avg_amount"] = round(float(item.get("avg_amount") or 0.0), 2)
        item["trade_days"] = int(item.get("trade_days") or 0)
        item["board_scope"] = board_key
    return records


def _select_level_backtest_targets(
    target_codes: Sequence[str],
    liquidity_days: int = 40,
) -> list[dict[str, Any]]:
    codes = [str(code).strip().upper() for code in (target_codes or []) if str(code).strip()]
    if not codes:
        return []

    placeholders = ",".join(["?"] * len(codes))
    liquidity_days = max(20, min(int(liquidity_days or 40), 90))
    order_cases = " ".join([f"WHEN ? THEN {idx}" for idx, _ in enumerate(codes)])
    params = [liquidity_days, *codes, *codes]
    df = fetch_df(
        f"""
        WITH recent_dates AS (
            SELECT trade_date
            FROM daily_price
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT ?
        )
        SELECT
            d.ts_code,
            MAX(b.name) AS name,
            MAX(b.market) AS market,
            AVG(COALESCE(d.amount, 0)) AS avg_amount,
            COUNT(*) AS trade_days
        FROM daily_price d
        JOIN stock_basic b ON d.ts_code = b.ts_code
        WHERE d.ts_code IN ({placeholders})
          AND d.trade_date IN (SELECT trade_date FROM recent_dates)
        GROUP BY d.ts_code
        ORDER BY CASE d.ts_code {order_cases} ELSE {len(codes)} END
        """,
        params=params,
    )
    if df.empty:
        return []

    records = df.to_dict("records")
    for item in records:
        item["avg_amount"] = round(float(item.get("avg_amount") or 0.0), 2)
        item["trade_days"] = int(item.get("trade_days") or 0)
        item["board_scope"] = _infer_level_market_board(
            ts_code=item.get("ts_code"),
            market=item.get("market"),
        )
    return records


def _load_level_backtest_history(
    ts_codes: Sequence[str],
    rows_per_stock: int,
) -> dict[str, pd.DataFrame]:
    codes = [str(code).strip().upper() for code in ts_codes if str(code).strip()]
    if not codes:
        return {}

    placeholders = ",".join(["?"] * len(codes))
    df = fetch_df(
        f"""
        SELECT *
        FROM (
            SELECT
                d.ts_code,
                b.market,
                d.trade_date,
                d.open,
                d.high,
                d.low,
                d.close,
                d.pre_close,
                d.vol AS volume,
                d.amount,
                d.pct_chg,
                ROW_NUMBER() OVER (PARTITION BY d.ts_code ORDER BY d.trade_date DESC) AS rn
            FROM daily_price d
            JOIN stock_basic b ON d.ts_code = b.ts_code
            WHERE d.ts_code IN ({placeholders})
        ) ranked
        WHERE rn <= ?
        ORDER BY ts_code, trade_date
        """,
        params=[*codes, max(80, int(rows_per_stock))],
    )
    if df.empty:
        return {}

    history_map: dict[str, pd.DataFrame] = {}
    for ts_code, group in df.groupby("ts_code"):
        work = group.drop(columns=["rn"], errors="ignore").sort_values("trade_date").reset_index(drop=True)
        history_map[str(ts_code)] = _ensure_volume_col(work)
    return history_map


def _evaluate_level_reaction(
    future_df: pd.DataFrame,
    price: float | None,
    level_type: str,
    atr_value: float | None,
    market_profile: dict[str, Any],
) -> dict[str, Any]:
    value = _safe_number(price)
    if value is None or future_df is None or future_df.empty:
        return {"status": "missing", "touch_index": None}

    atr_ref = atr_value if atr_value and atr_value > 0 else max(value * 0.01, 0.01)
    touch_band = max(
        atr_ref * float(market_profile["touch_tolerance_atr"]),
        value * 0.0026,
    )
    break_band = max(
        atr_ref * float(market_profile["break_tolerance_atr"]),
        value * 0.0042,
    )
    reaction_band = max(
        atr_ref * float(market_profile["reaction_target_atr"]),
        value * 0.0065,
    )

    highs = pd.to_numeric(future_df.get("high"), errors="coerce")
    lows = pd.to_numeric(future_df.get("low"), errors="coerce")
    closes = pd.to_numeric(future_df.get("close"), errors="coerce")

    if level_type == "support":
        touch_positions = np.flatnonzero((lows <= value + touch_band).fillna(False).to_numpy())
        if not touch_positions.size:
            return {"status": "untouched", "touch_index": None}
        touch_idx = int(touch_positions[0])
        after = future_df.iloc[touch_idx:].reset_index(drop=True)
        after_highs = pd.to_numeric(after.get("high"), errors="coerce")
        after_lows = pd.to_numeric(after.get("low"), errors="coerce")
        after_closes = pd.to_numeric(after.get("close"), errors="coerce")
        success_positions = np.flatnonzero(
            ((after_highs >= value + reaction_band) | (after_closes >= value + reaction_band * 0.82))
            .fillna(False)
            .to_numpy()
        )
        fail_positions = np.flatnonzero(
            ((after_closes <= value - break_band) | (after_lows <= value - break_band * 1.15))
            .fillna(False)
            .to_numpy()
        )
    else:
        touch_positions = np.flatnonzero((highs >= value - touch_band).fillna(False).to_numpy())
        if not touch_positions.size:
            return {"status": "untouched", "touch_index": None}
        touch_idx = int(touch_positions[0])
        after = future_df.iloc[touch_idx:].reset_index(drop=True)
        after_highs = pd.to_numeric(after.get("high"), errors="coerce")
        after_lows = pd.to_numeric(after.get("low"), errors="coerce")
        after_closes = pd.to_numeric(after.get("close"), errors="coerce")
        success_positions = np.flatnonzero(
            ((after_lows <= value - reaction_band) | (after_closes <= value - reaction_band * 0.82))
            .fillna(False)
            .to_numpy()
        )
        fail_positions = np.flatnonzero(
            ((after_closes >= value + break_band) | (after_highs >= value + break_band * 1.15))
            .fillna(False)
            .to_numpy()
        )

    success_idx = int(success_positions[0]) if success_positions.size else None
    fail_idx = int(fail_positions[0]) if fail_positions.size else None
    if success_idx is not None and (fail_idx is None or success_idx <= fail_idx):
        return {"status": "success", "touch_index": touch_idx, "resolve_index": success_idx}
    if fail_idx is not None:
        return {"status": "fail", "touch_index": touch_idx, "resolve_index": fail_idx}
    return {"status": "touched_open", "touch_index": touch_idx}


def _append_metric(values: list[float], value: float | None) -> None:
    number = _safe_number(value)
    if number is None:
        return
    values.append(float(number))


def _update_family_stats(bucket: dict[str, Any], family: str | None, status: str) -> None:
    family_key = str(family or "unknown")
    stats = bucket["family_stats"].setdefault(
        family_key,
        {"cases": 0, "touched": 0, "success": 0, "fail": 0},
    )
    stats["cases"] += 1
    if status in {"success", "fail", "touched_open"}:
        stats["touched"] += 1
    if status == "success":
        stats["success"] += 1
    elif status == "fail":
        stats["fail"] += 1


def _summarize_family_stats(family_stats: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family, stats in family_stats.items():
        touched = int(stats.get("touched", 0))
        success = int(stats.get("success", 0))
        rows.append(
            {
                "family": family,
                "cases": int(stats.get("cases", 0)),
                "touched": touched,
                "success": success,
                "fail": int(stats.get("fail", 0)),
                "reaction_rate": round(success / touched, 4) if touched else None,
            }
        )
    rows.sort(
        key=lambda item: (
            -(item.get("reaction_rate") or -1.0),
            -int(item.get("touched") or 0),
            str(item.get("family") or ""),
        )
    )
    return rows[:4]


def _summarize_backtest_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    touched = int(bucket["touched"])
    cases = int(bucket["cases"])
    success = int(bucket["success"])
    fail = int(bucket["fail"])
    median_distance_atr = (
        round(float(np.median(bucket["distance_atr"])), 3) if bucket["distance_atr"] else None
    )
    median_gap_atr = round(float(np.median(bucket["gap_atr"])), 3) if bucket["gap_atr"] else None
    return {
        "cases": cases,
        "missing": int(bucket["missing"]),
        "touched": touched,
        "success": success,
        "fail": fail,
        "open": int(bucket["touched_open"]),
        "untouched": int(bucket["untouched"]),
        "touch_rate": round(touched / cases, 4) if cases else None,
        "reaction_rate": round(success / touched, 4) if touched else None,
        "fail_rate": round(fail / touched, 4) if touched else None,
        "primary_tight_ratio": round(bucket["primary_tight"] / cases, 4) if cases else None,
        "primary_wide_ratio": round(bucket["primary_wide"] / cases, 4) if cases else None,
        "gap_tight_ratio": round(bucket["gap_tight"] / cases, 4) if cases else None,
        "gap_wide_ratio": round(bucket["gap_wide"] / cases, 4) if cases else None,
        "median_distance_atr": median_distance_atr,
        "median_gap_atr": median_gap_atr,
        "top_families": _summarize_family_stats(bucket["family_stats"]),
    }


def _run_level_backtest_profile(
    history_map: dict[str, pd.DataFrame],
    profile_name: str,
    lookback_days: int,
    eval_days: int,
    horizon: int,
) -> dict[str, Any]:
    lookback_days = max(60, min(int(lookback_days or 120), 240))
    eval_days = max(20, min(int(eval_days or 60), 160))
    horizon = max(3, min(int(horizon or 7), 15))

    result = {
        "profile": profile_name,
        "support": {
            "cases": 0,
            "missing": 0,
            "touched": 0,
            "success": 0,
            "fail": 0,
            "touched_open": 0,
            "untouched": 0,
            "primary_tight": 0,
            "primary_wide": 0,
            "gap_tight": 0,
            "gap_wide": 0,
            "distance_atr": [],
            "gap_atr": [],
            "family_stats": {},
        },
        "resistance": {
            "cases": 0,
            "missing": 0,
            "touched": 0,
            "success": 0,
            "fail": 0,
            "touched_open": 0,
            "untouched": 0,
            "primary_tight": 0,
            "primary_wide": 0,
            "gap_tight": 0,
            "gap_wide": 0,
            "distance_atr": [],
            "gap_atr": [],
            "family_stats": {},
        },
        "window_count": 0,
        "stock_count": 0,
        "per_stock": {},
    }

    for ts_code, history_df in history_map.items():
        if history_df is None or history_df.empty or len(history_df) < lookback_days + horizon + 5:
            continue

        stock_bucket = {
            "support": {
                "cases": 0,
                "missing": 0,
                "touched": 0,
                "success": 0,
                "fail": 0,
                "touched_open": 0,
                "untouched": 0,
                "primary_tight": 0,
                "primary_wide": 0,
                "gap_tight": 0,
                "gap_wide": 0,
                "distance_atr": [],
                "gap_atr": [],
                "family_stats": {},
            },
            "resistance": {
                "cases": 0,
                "missing": 0,
                "touched": 0,
                "success": 0,
                "fail": 0,
                "touched_open": 0,
                "untouched": 0,
                "primary_tight": 0,
                "primary_wide": 0,
                "gap_tight": 0,
                "gap_wide": 0,
                "distance_atr": [],
                "gap_atr": [],
                "family_stats": {},
            },
            "window_count": 0,
        }
        work = history_df.sort_values("trade_date").reset_index(drop=True)
        start_idx = max(lookback_days, len(work) - eval_days - horizon)
        end_idx = len(work) - horizon
        if end_idx <= start_idx:
            continue

        used_this_stock = False
        for idx in range(start_idx, end_idx):
            window_df = work.iloc[max(0, idx - lookback_days + 1): idx + 1].reset_index(drop=True)
            future_df = work.iloc[idx + 1: idx + 1 + horizon].reset_index(drop=True)
            if len(window_df) < 60 or future_df.empty:
                continue

            bundle = build_structural_price_levels(
                window_df,
                top_n=2,
                market_board=_infer_level_market_board(window_df, ts_code=ts_code, market=window_df.iloc[-1].get("market")),
                profile=profile_name,
            )
            atr_value = _safe_number(bundle.get("atr14")) or _latest_atr_value(window_df)
            market_profile = _resolve_level_board_profile(
                bundle.get("market_board")
                or _infer_level_market_board(window_df, ts_code=ts_code, market=window_df.iloc[-1].get("market"))
            )

            for level_type in ("support", "resistance"):
                bucket = result[level_type]
                stock_side = stock_bucket[level_type]
                bucket["cases"] += 1
                stock_side["cases"] += 1
                levels = list(bundle.get(f"{level_type}_levels") or [])
                primary = levels[0] if levels else None
                secondary = levels[1] if len(levels) > 1 else None
                if not primary:
                    bucket["missing"] += 1
                    stock_side["missing"] += 1
                    continue

                distance_atr = _safe_number(primary.get("distance_atr"))
                if distance_atr is None and atr_value and atr_value > 0:
                    distance_atr = abs(float(primary["price"]) - float(window_df.iloc[-1]["close"])) / atr_value
                _append_metric(bucket["distance_atr"], distance_atr)
                _append_metric(stock_side["distance_atr"], distance_atr)

                if distance_atr is not None:
                    if distance_atr < float(market_profile["min_primary_atr"]):
                        bucket["primary_tight"] += 1
                        stock_side["primary_tight"] += 1
                    elif distance_atr > float(market_profile["max_primary_atr"]):
                        bucket["primary_wide"] += 1
                        stock_side["primary_wide"] += 1

                if secondary and atr_value and atr_value > 0:
                    gap_atr = abs(float(primary["price"]) - float(secondary["price"])) / atr_value
                    _append_metric(bucket["gap_atr"], gap_atr)
                    _append_metric(stock_side["gap_atr"], gap_atr)
                    if gap_atr < float(market_profile["min_secondary_gap_atr"]):
                        bucket["gap_tight"] += 1
                        stock_side["gap_tight"] += 1
                    elif gap_atr > float(market_profile["max_secondary_gap_atr"]):
                        bucket["gap_wide"] += 1
                        stock_side["gap_wide"] += 1

                outcome = _evaluate_level_reaction(
                    future_df,
                    price=_safe_number(primary.get("price")),
                    level_type=level_type,
                    atr_value=atr_value,
                    market_profile=market_profile,
                )
                status = str(outcome.get("status") or "missing")
                if status == "success":
                    bucket["touched"] += 1
                    bucket["success"] += 1
                    stock_side["touched"] += 1
                    stock_side["success"] += 1
                elif status == "fail":
                    bucket["touched"] += 1
                    bucket["fail"] += 1
                    stock_side["touched"] += 1
                    stock_side["fail"] += 1
                elif status == "touched_open":
                    bucket["touched"] += 1
                    bucket["touched_open"] += 1
                    stock_side["touched"] += 1
                    stock_side["touched_open"] += 1
                elif status == "untouched":
                    bucket["untouched"] += 1
                    stock_side["untouched"] += 1
                else:
                    bucket["missing"] += 1
                    stock_side["missing"] += 1

                _update_family_stats(bucket, primary.get("dominant_family"), status)
                _update_family_stats(stock_side, primary.get("dominant_family"), status)

            result["window_count"] += 1
            stock_bucket["window_count"] += 1
            used_this_stock = True

        if used_this_stock:
            result["stock_count"] += 1
            result["per_stock"][str(ts_code)] = {
                "window_count": int(stock_bucket["window_count"]),
                "support": _summarize_backtest_bucket(stock_bucket["support"]),
                "resistance": _summarize_backtest_bucket(stock_bucket["resistance"]),
            }

    support_summary = _summarize_backtest_bucket(result["support"])
    resistance_summary = _summarize_backtest_bucket(result["resistance"])
    reaction_scores = [
        item for item in [support_summary.get("reaction_rate"), resistance_summary.get("reaction_rate")]
        if item is not None
    ]
    span_penalties = [
        item
        for item in [
            support_summary.get("primary_tight_ratio"),
            support_summary.get("primary_wide_ratio"),
            support_summary.get("gap_tight_ratio"),
            support_summary.get("gap_wide_ratio"),
            resistance_summary.get("primary_tight_ratio"),
            resistance_summary.get("primary_wide_ratio"),
            resistance_summary.get("gap_tight_ratio"),
            resistance_summary.get("gap_wide_ratio"),
        ]
        if item is not None
    ]
    composite_score = None
    if reaction_scores:
        composite_score = float(np.mean(reaction_scores))
        if span_penalties:
            composite_score -= float(np.mean(span_penalties)) * 0.35
        composite_score = round(composite_score, 4)

    return {
        "profile": profile_name,
        "window_count": int(result["window_count"]),
        "stock_count": int(result["stock_count"]),
        "support": support_summary,
        "resistance": resistance_summary,
        "composite_score": composite_score,
        "per_stock": result["per_stock"],
    }


def backtest_structural_price_levels(
    board: str = "growth",
    sample_size: int = 60,
    lookback_days: int = 120,
    eval_days: int = 60,
    horizon: int = 7,
    include_legacy: bool = True,
    target_codes: Sequence[str] | None = None,
) -> dict[str, Any]:
    board_key, scope = _resolve_backtest_board_scope(board)
    explicit_codes = [str(code).strip().upper() for code in (target_codes or []) if str(code).strip()]
    universe = (
        _select_level_backtest_targets(explicit_codes)
        if explicit_codes
        else _select_level_backtest_universe(board=board_key, sample_size=sample_size)
    )
    codes = [str(item.get("ts_code") or "").strip().upper() for item in universe if item.get("ts_code")]
    if not codes:
        return {
            "scope": {
                "board": board_key,
                "label": scope["label"],
                "sample_size": 0,
                "lookback_days": lookback_days,
                "eval_days": eval_days,
                "horizon": horizon,
                "explicit_codes": explicit_codes,
            },
            "profiles": {},
            "comparison": {},
        }

    rows_per_stock = max(lookback_days + eval_days + horizon + 20, 120)
    history_map = _load_level_backtest_history(codes, rows_per_stock=rows_per_stock)
    profiles: dict[str, Any] = {}
    if include_legacy:
        profiles["legacy"] = _run_level_backtest_profile(
            history_map,
            profile_name="legacy",
            lookback_days=lookback_days,
            eval_days=eval_days,
            horizon=horizon,
        )
    profiles["adaptive"] = _run_level_backtest_profile(
        history_map,
        profile_name="adaptive",
        lookback_days=lookback_days,
        eval_days=eval_days,
        horizon=horizon,
    )

    comparison: dict[str, Any] = {}
    if "legacy" in profiles and "adaptive" in profiles:
        legacy = profiles["legacy"]
        adaptive = profiles["adaptive"]
        comparison = {
            "winner": (
                "adaptive"
                if (adaptive.get("composite_score") or -999) >= (legacy.get("composite_score") or -999)
                else "legacy"
            ),
            "composite_uplift": round(
                float((adaptive.get("composite_score") or 0.0) - (legacy.get("composite_score") or 0.0)),
                4,
            ),
            "support_reaction_uplift": round(
                float(
                    (adaptive.get("support", {}).get("reaction_rate") or 0.0)
                    - (legacy.get("support", {}).get("reaction_rate") or 0.0)
                ),
                4,
            ),
            "resistance_reaction_uplift": round(
                float(
                    (adaptive.get("resistance", {}).get("reaction_rate") or 0.0)
                    - (legacy.get("resistance", {}).get("reaction_rate") or 0.0)
                ),
                4,
            ),
            "primary_span_wide_reduction": round(
                float(
                    (
                        (legacy.get("support", {}).get("primary_wide_ratio") or 0.0)
                        + (legacy.get("resistance", {}).get("primary_wide_ratio") or 0.0)
                    )
                    - (
                        (adaptive.get("support", {}).get("primary_wide_ratio") or 0.0)
                        + (adaptive.get("resistance", {}).get("primary_wide_ratio") or 0.0)
                    )
                ),
                4,
            ),
            "secondary_gap_anomaly_reduction": round(
                float(
                    (
                        (legacy.get("support", {}).get("gap_tight_ratio") or 0.0)
                        + (legacy.get("support", {}).get("gap_wide_ratio") or 0.0)
                        + (legacy.get("resistance", {}).get("gap_tight_ratio") or 0.0)
                        + (legacy.get("resistance", {}).get("gap_wide_ratio") or 0.0)
                    )
                    - (
                        (adaptive.get("support", {}).get("gap_tight_ratio") or 0.0)
                        + (adaptive.get("support", {}).get("gap_wide_ratio") or 0.0)
                        + (adaptive.get("resistance", {}).get("gap_tight_ratio") or 0.0)
                        + (adaptive.get("resistance", {}).get("gap_wide_ratio") or 0.0)
                    )
                ),
                4,
            ),
        }

    latest_trade_date = None
    if history_map:
        latest_dates = [
            str(df.iloc[-1].get("trade_date"))[:10]
            for df in history_map.values()
            if df is not None and not df.empty and df.iloc[-1].get("trade_date") is not None
        ]
        if latest_dates:
            latest_trade_date = max(latest_dates)

    return {
        "scope": {
            "board": board_key,
            "label": scope["label"],
            "sample_size": len(history_map),
            "lookback_days": max(60, min(int(lookback_days or 120), 240)),
            "eval_days": max(20, min(int(eval_days or 60), 160)),
            "horizon": max(3, min(int(horizon or 7), 15)),
            "latest_trade_date": latest_trade_date,
            "explicit_codes": explicit_codes or None,
        },
        "universe": {
            "sample_codes": [item["ts_code"] for item in universe[:12]],
            "top_samples": universe[:8],
        },
        "profiles": profiles,
        "comparison": comparison,
    }


def get_professional_commentary(df: pd.DataFrame, patterns: list, context: dict | None = None) -> str:
    """
    根据形态和最近行情，给出专业的点评分析（机构/游资视角）。
    返回: 简洁的汇总字符串 (兼容旧接口)
    """
    detail = get_professional_commentary_detailed(df, patterns, context=context)
    return detail.get("summary", "暂无明显信号，观望为主。")


def get_professional_commentary_detailed(
    df: pd.DataFrame,
    patterns: list,
    context: dict | None = None,
) -> dict:
    """
    根据形态和最近行情，给出专业的点评分析（机构/游资视角）。
    返回详细的结构化数据，供前端展示。
    """
    if df is None or df.empty:
        return {
            "summary": "暂无数据分析",
            "decision": {},
            "trade_plan": {},
            "key_levels": [],
            "level_methodology": [],
            "observation_points": [],
            "classification": {},
            "institution": [],
            "hotmoney": [],
            "patterns": [],
            "risk_alert": [],
            "technical": {},
        }

    df = df.copy()
    commentary_context = _resolve_commentary_context(context)
    classification = commentary_context.get("classification") or {}
    if "volume" not in df.columns and "vol" in df.columns:
        df = df.rename(columns={"vol": "volume"})
    for ma in (5, 10, 20, 60):
        col = f"ma{ma}"
        if col not in df.columns:
            df[col] = df["close"].rolling(ma, min_periods=1).mean()

    last = df.iloc[-1]
    last_5 = df.tail(5)
    last_10 = df.tail(10)
    last_20 = df.tail(20)
    last_60 = df.tail(60) if len(df) >= 60 else df

    # 统一列名
    vol_col = 'volume' if 'volume' in df.columns else 'vol'
    pct_col = 'pct_chg' if 'pct_chg' in df.columns else None

    close = last['close']
    open_price = last.get('open', close)
    high = last.get('high', close)
    low = last.get('low', close)
    pct_today = last.get(pct_col, 0) if pct_col else 0
    vol_today = last[vol_col]

    # 均线数据
    ma5 = last.get('ma5', None)
    ma10 = last.get('ma10', None)
    ma20 = last.get('ma20', None)
    ma60 = last.get('ma60', None)

    prev_close = _safe_number(df.iloc[-2]["close"] if len(df) >= 2 else close, close) or close

    # 量能分析
    vol_5_avg = _safe_number(last_5[vol_col].mean(), 0.0) or 0.0
    vol_20_avg = _safe_number(last_20[vol_col].mean() if len(last_20) >= 20 else vol_5_avg, 0.0) or 0.0
    volume_ratio = _safe_number(last.get("volume_ratio"))
    if volume_ratio is None or volume_ratio <= 0:
        volume_ratio = round(vol_today / vol_5_avg, 2) if vol_5_avg > 0 else None
    volume_ratio_20 = round(vol_today / vol_20_avg, 2) if vol_20_avg > 0 else None
    vol_ratio_5 = ((volume_ratio or 1.0) - 1.0) if volume_ratio is not None else 0.0
    vol_ratio_20 = ((volume_ratio_20 or 1.0) - 1.0) if volume_ratio_20 is not None else 0.0

    # 涨跌幅统计
    pct_5_avg = last_5[pct_col].mean() if pct_col and len(last_5) >= 5 else 0
    pct_10_sum = last_10[pct_col].sum() if pct_col and len(last_10) >= 10 else 0
    pct_20_sum = last_20[pct_col].sum() if pct_col and len(last_20) >= 20 else 0

    # 振幅分析
    if 'high' in df.columns and 'low' in df.columns:
        amplitude_today = ((last['high'] - last['low']) / last['low'] * 100) if last['low'] > 0 else 0
        amplitude_5_avg = ((last_5['high'] - last_5['low']) / last_5['low'] * 100).mean() if len(last_5) >= 5 else 0
    else:
        amplitude_today = 0
        amplitude_5_avg = 0

    # 换手率（如果有）
    turnover = _safe_number(last.get('turnover_rate'))

    # 融资数据（如果有）
    rzye = _safe_number(last.get('rzye'))
    rzmre = _safe_number(last.get('rzmre'))
    rzche = _safe_number(last.get('rzche'))

    # 资金流向
    amount_today = _safe_number(last.get('amount'))
    amount_5_avg = _safe_number(last_5['amount'].mean(), 0.0) if 'amount' in last_5.columns else 0.0
    amount_ratio = ((amount_today / amount_5_avg) - 1.0) if amount_today and amount_5_avg else 0.0
    net_mf_amount = _safe_number(last.get("net_mf_amount"), 0.0) or 0.0
    net_mf_ratio = _safe_number(last.get("net_mf_ratio"))
    big_order_ratio = _safe_number(last.get("big_order_ratio"))
    factor_score = _safe_number(last.get("factor_score"))
    trend_factor = _safe_number(last.get("trend_score"))
    flow_factor = _safe_number(last.get("flow_score"))
    quality_factor = _safe_number(last.get("quality_score"))
    value_factor = _safe_number(last.get("value_score"))
    event_factor = _safe_number(last.get("event_score"))
    rps_20 = _safe_number(last.get("rps_20"))
    rps_50 = _safe_number(last.get("rps_50"))
    rps_120 = _safe_number(last.get("rps_120"))
    recent_net_flows = [
        _safe_number(item, 0.0) or 0.0
        for item in (last_5["net_mf_amount"].tolist() if "net_mf_amount" in last_5.columns else [])
    ]
    flow_3_sum = float(sum(recent_net_flows[-3:])) if recent_net_flows else 0.0
    positive_flow_days_3 = int(sum(1 for item in recent_net_flows[-3:] if item > 0))
    negative_flow_days_3 = int(sum(1 for item in recent_net_flows[-3:] if item < 0))
    latest_snapshot = commentary_context.get("realtime_snapshot") or {}
    quote_time = str(latest_snapshot.get("quote_time") or "").strip()
    quote_mode = "realtime" if quote_time else "close"

    institution_view = []
    hotmoney_view = []
    pattern_view = []
    risk_alert = []

    # === 1. 机构视角分析 ===
    
    # 趋势结构（核心）
    if ma20 is not None and ma60 is not None and not (pd.isna(ma20) or pd.isna(ma60)):
        if close > ma20 > ma60 and ma20 > ma60 * 1.02:
            if ma5 is not None and ma10 is not None and close > ma5 > ma10:
                institution_view.append({
                    "type": "trend",
                    "level": "strong",
                    "title": "上升趋势确立",
                    "desc": "均线多头排列，短期中期趋势共振向上，机构资金配置窗口打开"
                })
            else:
                institution_view.append({
                    "type": "trend",
                    "level": "medium",
                    "title": "中期趋势向好",
                    "desc": "价格站上20/60日均线，中期上升趋势初步形成"
                })
        elif close < ma20 < ma60 and ma20 < ma60 * 0.98:
            institution_view.append({
                "type": "trend",
                "level": "bearish",
                "title": "下降趋势中",
                "desc": "均线空头排列，趋势未扭转，机构资金维持观望"
            })
        elif close > ma20 and ma20 < ma60:
            institution_view.append({
                "type": "trend",
                "level": "neutral",
                "title": "突破关键均线",
                "desc": "价格突破20日线，但中期均线仍在下行，需观察量能持续性"
            })
        elif close < ma20 and ma20 > ma60:
            institution_view.append({
                "type": "trend",
                "level": "warning",
                "title": "反弹遇阻",
                "desc": "价格跌破20日线，短期反弹结束，需警惕进一步回落"
            })
        else:
            institution_view.append({
                "type": "trend",
                "level": "neutral",
                "title": "横盘整理",
                "desc": "均线粘连缠绕，趋势方向不明，等待突破信号"
            })

    # 均线支撑/压力判断
    if ma5 is not None and ma10 is not None and ma20 is not None:
        if close > ma5 * 1.05:
            if close > ma20 * 1.05:
                institution_view.append({
                    "type": "support",
                    "level": "strong",
                    "title": "强势站上均线",
                    "desc": f"价格距20日均线{((close/ma20-1)*100):.1f}%，上涨动能充沛"
                })
            else:
                institution_view.append({
                    "type": "support",
                    "level": "medium",
                    "title": "依托5日均线上涨",
                    "desc": "短期走势健康，回调不破5日均线可继续持有"
                })
        elif close < ma5 * 0.95:
            institution_view.append({
                "type": "support",
                "level": "warning",
                "title": "跌破短期均线",
                "desc": "价格失守5日均线，短期走势转弱，支撑位下移至10日均线"
            })

    # 量能结构（机构关注持续性）
    if vol_ratio_20 > 1.0:
        institution_view.append({
            "type": "volume",
            "level": "strong",
            "title": "量能显著放大",
            "desc": f"成交量较20日均量放大{(vol_ratio_20*100):.0f}%，增量资金入场明显，关注持续性"
        })
    elif vol_ratio_20 > 0.3:
        institution_view.append({
            "type": "volume",
            "level": "medium",
            "title": "量能温和放大",
            "desc": "成交量稳步缓慢放大，增量资金入场，趋势可持续"
        })
    elif vol_ratio_20 < -0.4:
        institution_view.append({
            "type": "volume",
            "level": "weak",
            "title": "量能萎缩",
            "desc": "成交量持续萎缩，市场参与度不足，需等待催化剂"
        })

    # 量价配合分析
    if vol_ratio_5 > 0.5 and pct_today > 2:
        institution_view.append({
            "type": "volume_price",
            "level": "strong",
            "title": "价量齐升",
            "desc": "上涨伴随放量，量价配合健康，资金持续流入"
        })
    elif vol_ratio_5 < -0.3 and abs(pct_today) < 1:
        institution_view.append({
            "type": "volume_price",
            "level": "neutral",
            "title": "缩量横盘",
            "desc": "波动率降低，观望情绪浓厚，等待方向选择"
        })

    # 融资融券（机构杠杆资金）
    if rzye is not None and rzmre is not None and not pd.isna(rzye) and not pd.isna(rzmre):
        rz_ratio = rzmre / rzye if rzye > 0 else 0
        if rz_ratio > 0.03:
            institution_view.append({
                "type": "margin",
                "level": "strong",
                "title": "融资活跃",
                "desc": f"融资买入占比{rz_ratio*100:.1f}%，杠杆资金强烈看多，后市看涨"
            })
        elif rz_ratio > 0.015:
            institution_view.append({
                "type": "margin",
                "level": "medium",
                "title": "融资买入回升",
                "desc": "融资情绪回暖，杠杆资金参与度提升"
            })
        elif rz_ratio < 0.005:
            institution_view.append({
                "type": "margin",
                "level": "weak",
                "title": "融资观望",
                "desc": "融资买入低迷，杠杆资金谨慎观望"
            })
        
        # 融资余额变化
        if rzche is not None and not pd.isna(rzche):
            if rzche > 0:
                institution_view.append({
                    "type": "margin",
                    "level": "medium",
                    "title": "融资余额增加",
                    "desc": "融资偿还额小于买入额，杠杆资金净流入"
                })
            elif rzche < -rzye * 0.01:
                institution_view.append({
                    "type": "margin",
                    "level": "warning",
                    "title": "融资偿还加速",
                    "desc": "杠杆资金出现偿还压力，需警惕抛压"
                })

    # 波动率（机构风控）
    if amplitude_today > amplitude_5_avg * 1.5 and amplitude_today > 5:
        risk_alert.append({
            "type": "volatility",
            "level": "high",
            "title": "波动率上升",
            "desc": f"日内振幅{amplitude_today:.1f}%，较5日均值放大{(amplitude_today/amplitude_5_avg-1)*100:.0f}%，需控制仓位"
        })

    # 20日累计涨跌幅
    if pct_20_sum > 30:
        risk_alert.append({
            "type": "accumulate",
            "level": "medium",
            "title": "短期涨幅较大",
            "desc": f"20日累计上涨{pct_20_sum:.1f}%，注意短期回调风险"
        })
    elif pct_20_sum < -20:
        risk_alert.append({
            "type": "accumulate",
            "level": "low",
            "title": "短期超跌",
            "desc": f"20日累计下跌{abs(pct_20_sum):.1f}%，存在超跌反弹机会"
        })

    # === 2. 游资视角分析 ===
    
    # 短期爆发力
    if pct_today >= 9.5:
        hotmoney_view.append({
            "type": "momentum",
            "level": "extreme",
            "title": "涨停板",
            "desc": "强势涨停，情绪达到高潮，关注次日溢价和封板强度"
        })
    elif pct_today >= 7:
        hotmoney_view.append({
            "type": "momentum",
            "level": "strong",
            "title": "大涨突破",
            "desc": "涨幅超7%，短线资金抢筹明显，关注能否封板"
        })
    elif pct_today >= 5:
        hotmoney_view.append({
            "type": "momentum",
            "level": "medium",
            "title": "强势上涨",
            "desc": "大阳线突破，短线资金积极入场"
        })
    elif pct_today >= 3:
        hotmoney_view.append({
            "type": "momentum",
            "level": "light",
            "title": "日内强势",
            "desc": "走势强于大盘，有短线资金关注"
        })
    elif pct_today <= -9.5:
        hotmoney_view.append({
            "type": "momentum",
            "level": "extreme",
            "title": "跌停板",
            "desc": "恐慌性跌停，短线资金踩踏离场，短期回避"
        })
    elif pct_today <= -7:
        hotmoney_view.append({
            "type": "momentum",
            "level": "strong",
            "title": "大跌",
            "desc": "跌幅超7%，恐慌盘涌出，短线风险极大"
        })
    elif pct_today <= -5:
        hotmoney_view.append({
            "type": "momentum",
            "level": "medium",
            "title": "下跌调整",
            "desc": "大阴线杀跌，短线情绪转弱，观望为主"
        })

    # 换手率（游资最关注）
    if turnover is not None and not pd.isna(turnover):
        if turnover > 30:
            hotmoney_view.append({
                "type": "turnover",
                "level": "extreme",
                "title": "极高换手",
                "desc": f"换手率{turnover:.1f}%，筹码充分换手，注意游资对倒出货风险"
            })
        elif turnover > 20:
            hotmoney_view.append({
                "type": "turnover",
                "level": "strong",
                "title": "高换手",
                "desc": f"换手率{turnover:.1f}%，筹码高度活跃，游资接力积极"
            })
        elif turnover > 10:
            hotmoney_view.append({
                "type": "turnover",
                "level": "medium",
                "title": "活跃换手",
                "desc": f"换手率{turnover:.1f}%，资金进出活跃，适合短线操作"
            })
        elif turnover > 5:
            hotmoney_view.append({
                "type": "turnover",
                "level": "light",
                "title": "温和换手",
                "desc": f"换手率{turnover:.1f}%，流动性适中"
            })
        elif turnover < 2:
            hotmoney_view.append({
                "type": "turnover",
                "level": "weak",
                "title": "低换手",
                "desc": f"换手率{turnover:.1f}%，筹码锁定，缺乏流动性，观望"
            })

    # 量价配合（游资看爆发）
    if vol_ratio_5 > 2.0 and pct_today > 5:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "strong",
            "title": "放量启动",
            "desc": "放量大涨，游资抢筹信号，可能进入主升浪"
        })
    elif vol_ratio_5 > 1.5 and pct_today > 3:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "medium",
            "title": "量价齐升",
            "desc": "量价配合良好，短线保持强势"
        })
    elif vol_ratio_5 > 1.0 and pct_today < -3:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "warning",
            "title": "放量下跌",
            "desc": "放量下跌，游资出逃，短线风险加大"
        })
    elif vol_ratio_5 > 0.5 and pct_today < -5:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "danger",
            "title": "恐慌抛售",
            "desc": "放量暴跌，恐慌盘涌出，短期回避"
        })
    elif vol_ratio_5 < -0.3 and abs(pct_today) < 1.5:
        hotmoney_view.append({
            "type": "volume_price",
            "level": "neutral",
            "title": "缩量横盘",
            "desc": "缩量盘整，游资观望，等待方向选择"
        })

    # 连板高度（如果10日累计涨幅大）
    if pct_10_sum > 40:
        hotmoney_view.append({
            "type": "continue_rise",
            "level": "danger",
            "title": "高位风险",
            "desc": f"10日累计涨幅{pct_10_sum:.0f}%，高位分歧加大，注意补跌风险"
        })
    elif pct_10_sum > 30:
        hotmoney_view.append({
            "type": "continue_rise",
            "level": "warning",
            "title": "注意分歧",
            "desc": f"10日累计涨幅{pct_10_sum:.0f}%，获利盘丰厚，注意短线回调"
        })
    elif pct_10_sum > 20:
        hotmoney_view.append({
            "type": "continue_rise",
            "level": "medium",
            "title": "已有涨幅",
            "desc": f"10日累计涨幅{pct_10_sum:.0f}%，短线可逐步兑现收益"
        })

    # 日内分型（上下影线）
    up_shadow = 0.0
    down_shadow = 0.0
    if high > low:
        up_shadow = (high - max(close, open_price)) / (high - low) * 100 if high > low else 0
        down_shadow = (min(close, open_price) - low) / (high - low) * 100 if high > low else 0
        
        if up_shadow > 60 and pct_today > 2:
            hotmoney_view.append({
                "type": "candle",
                "level": "warning",
                "title": "长上影",
                "desc": f"上影线占比{up_shadow:.0f}%，上方抛压重，警惕冲高回落"
            })
        elif down_shadow > 60 and pct_today < -2:
            hotmoney_view.append({
                "type": "candle",
                "level": "medium",
                "title": "长下影",
                "desc": f"下影线占比{down_shadow:.0f}%，有资金抄底，关注反弹力度"
            })

    # === 3. 形态信号 ===
    if patterns:
        p_set = set(patterns)

        # 强势形态
        if p_set & {'老鸭头'}:
            pattern_view.append({
                "type": "bullish",
                "level": "strong",
                "pattern": "老鸭头",
                "desc": "主力洗盘结束，控盘度高，经典拉升形态，后市看涨"
            })
        if p_set & {'仙人指路'}:
            pattern_view.append({
                "type": "bullish",
                "level": "medium",
                "pattern": "仙人指路",
                "desc": "上影线试盘，主力试探上方抛压，后市看涨"
            })
        if p_set & {'红三兵'}:
            pattern_view.append({
                "type": "bullish",
                "level": "strong",
                "pattern": "红三兵",
                "desc": "多头稳步推进，底部逐步抬升，趋势向好"
            })
        if p_set & {'曙光初现'}:
            pattern_view.append({
                "type": "bullish",
                "level": "medium",
                "pattern": "曙光初现",
                "desc": "空头衰竭后资金回补，属于经典止跌转强信号"
            })
        if p_set & {'放量突破'}:
            pattern_view.append({
                "type": "bullish",
                "level": "strong",
                "pattern": "放量突破",
                "desc": "突破关键压力位，量能配合，有效性强"
            })
        if p_set & {'量价齐升'}:
            pattern_view.append({
                "type": "bullish",
                "level": "medium",
                "pattern": "量价齐升",
                "desc": "资金持续流入，健康上涨态势"
            })
        if p_set & {'出水芙蓉'}:
            pattern_view.append({
                "type": "bullish",
                "level": "strong",
                "pattern": "出水芙蓉",
                "desc": "一阳穿多线，短期转强信号"
            })
        if p_set & {'多方炮'}:
            pattern_view.append({
                "type": "bullish",
                "level": "medium",
                "pattern": "多方炮",
                "desc": "两阳夹一阴，多头进攻形态，后市看涨"
            })

        # 反转形态
        if p_set & {'锤子线', '启明星', '早晨之星', '曙光初现', '看涨吞没'}:
            pattern_view.append({
                "type": "reversal_bull",
                "level": "medium",
                "pattern": "底部反转",
                "desc": "出现底部反转信号，空头动能衰竭，多头开始反攻"
            })
        if p_set & {'三只乌鸦', '黄昏星', '顶分型', '乌云盖顶', '看跌吞没'}:
            pattern_view.append({
                "type": "reversal_bear",
                "level": "medium",
                "pattern": "顶部反转",
                "desc": "出现顶部反转信号，多头力竭，注意减仓"
            })
        if p_set & {'射击之星', '吊颈线'}:
            pattern_view.append({
                "type": "reversal_bear",
                "level": "light",
                "pattern": "射击之星",
                "desc": "上影线较长，警惕短期回调"
            })
        if p_set & {'乌云盖顶'}:
            pattern_view.append({
                "type": "warning",
                "level": "warning",
                "pattern": "乌云盖顶",
                "desc": "强势区被空头反压，若次日继续走弱宜减仓控制风险"
            })

        # 警示形态
        if p_set & {'量价背离'}:
            risk_alert.append({
                "type": "warning",
                "level": "medium",
                "title": "量价背离",
                "desc": "价格上涨但成交量萎缩，主力可能借势出货"
            })
            pattern_view.append({
                "type": "warning",
                "level": "warning",
                "pattern": "量价背离",
                "desc": "价涨量缩，需警惕量价配合失效"
            })
        if p_set & {'地量地价'}:
            pattern_view.append({
                "type": "special",
                "level": "neutral",
                "pattern": "地量地价",
                "desc": "成交极度萎缩，变盘窗口临近"
            })
        if p_set & {'天量天价'}:
            risk_alert.append({
                "type": "warning",
                "level": "high",
                "title": "天量天价",
                "desc": "成交量创近期新高，需警惕短期头部"
            })
            pattern_view.append({
                "type": "warning",
                "level": "warning",
                "pattern": "天量天价",
                "desc": "放量滞涨信号，注意风险"
            })

    # === 4. 决策层：趋势/量价/资金/实时位置主导，形态仅作辅助参考 ===
    trend_score = 0.0
    if ma20 is not None and ma60 is not None and not (pd.isna(ma20) or pd.isna(ma60)):
        if close > ma20 > ma60:
            trend_score += 18
        elif close < ma20 < ma60:
            trend_score -= 18
        elif close > ma20:
            trend_score += 8
        else:
            trend_score -= 8
    if ma5 is not None and ma10 is not None and not (pd.isna(ma5) or pd.isna(ma10)):
        if close > ma5 > ma10:
            trend_score += 10
        elif close < ma5 < ma10:
            trend_score -= 10
    if trend_factor is not None:
        trend_score += float(np.clip((trend_factor - 50.0) * 0.24, -12.0, 12.0))

    momentum_score = 0.0
    if pct_today >= 7:
        momentum_score += 12
    elif pct_today >= 3:
        momentum_score += 7
    elif pct_today <= -7:
        momentum_score -= 12
    elif pct_today <= -3:
        momentum_score -= 7

    if pct_10_sum >= 18:
        momentum_score += 8
    elif pct_10_sum >= 8:
        momentum_score += 4
    elif pct_10_sum <= -18:
        momentum_score -= 8
    elif pct_10_sum <= -8:
        momentum_score -= 4

    if rps_20 is not None:
        if rps_20 >= 85:
            momentum_score += 6
        elif rps_20 >= 70:
            momentum_score += 3
        elif rps_20 <= 20:
            momentum_score -= 6
        elif rps_20 <= 35:
            momentum_score -= 3
    if rps_50 is not None:
        if rps_50 >= 80:
            momentum_score += 3
        elif rps_50 <= 25:
            momentum_score -= 3

    volume_score = 0.0
    if volume_ratio is not None:
        if volume_ratio >= 1.6 and pct_today >= 0:
            volume_score += 8
        elif volume_ratio >= 1.2 and pct_today >= 0:
            volume_score += 4
        elif volume_ratio <= 0.8 and abs(pct_today) < 1.5:
            volume_score -= 2
    if volume_ratio_20 is not None:
        if volume_ratio_20 >= 1.8 and pct_today >= 0:
            volume_score += 5
        elif volume_ratio_20 <= 0.75:
            volume_score -= 4
    if turnover is not None:
        if turnover >= 10 and pct_today >= 0:
            volume_score += 4
        elif turnover <= 2 and close < (ma20 or close):
            volume_score -= 3
    if amount_ratio >= 0.5 and pct_today >= 0:
        volume_score += 3

    capital_score = 0.0
    if flow_factor is not None:
        capital_score += float(np.clip((flow_factor - 50.0) * 0.18, -8.0, 8.0))
    if factor_score is not None:
        capital_score += float(np.clip((factor_score - 50.0) * 0.16, -8.0, 8.0))
    if net_mf_amount > 0:
        capital_score += 6 if positive_flow_days_3 >= 2 else 3
    elif net_mf_amount < 0:
        capital_score -= 6 if negative_flow_days_3 >= 2 else 3
    if big_order_ratio is not None:
        if big_order_ratio >= 0.2:
            capital_score += 2
        elif big_order_ratio <= -0.2:
            capital_score -= 2

    pattern_score = 0.0
    for item in pattern_view:
        p_type = str(item.get("type", ""))
        if p_type in {"bullish", "reversal_bull"}:
            pattern_score += 0.8
        elif p_type in {"reversal_bear", "warning"}:
            pattern_score -= 0.8
    pattern_score = float(np.clip(pattern_score, -2.0, 2.0))

    risk_penalty = 0.0
    for item in risk_alert:
        risk_penalty += {"high": 12, "medium": 8, "low": 4}.get(str(item.get("level", "low")), 4)
    if amplitude_today > 5 and amplitude_today > amplitude_5_avg * 1.5:
        risk_penalty += 4
    if up_shadow > 55 and pct_today > 2:
        risk_penalty += 3
    if close < (ma20 or close) and net_mf_amount < 0 and (volume_ratio or 1.0) >= 1.2:
        risk_penalty += 4

    score = int(
        np.clip(
            50 + trend_score + momentum_score + volume_score + capital_score + pattern_score - risk_penalty,
            0,
            100,
        )
    )
    bias, action, confidence, style = _decision_bucket(score)

    low_20 = _safe_number(last_20["low"].min() if "low" in last_20.columns else None)

    level_bundle = build_structural_price_levels(
        df,
        top_n=2,
        market_board=_infer_level_market_board(
            df,
            ts_code=commentary_context.get("ts_code"),
            market=commentary_context.get("market"),
        ),
    )
    support_levels = list(level_bundle.get("support_levels") or [])
    resistance_levels = list(level_bundle.get("resistance_levels") or [])

    support_1 = support_levels[0]["price"] if support_levels else None
    support_2 = support_levels[1]["price"] if len(support_levels) > 1 else None
    resistance_1 = resistance_levels[0]["price"] if resistance_levels else None
    resistance_2 = resistance_levels[1]["price"] if len(resistance_levels) > 1 else None
    stop_level = support_2 or support_1 or low_20 or ma20 or prev_close

    def fmt_level(value: float | None) -> str:
        return f"{value:.2f}" if value is not None else "待确认"

    def distance_pct(level: float | None) -> float | None:
        if level is None or close <= 0:
            return None
        return round(abs(close - level) / close * 100, 2)

    dist_support = distance_pct(support_1)
    dist_resistance = distance_pct(resistance_1)
    near_band_pct = min(
        2.2,
        max(
            1.0,
            ((_safe_number(level_bundle.get("selection_gap"), close * 0.015) or (close * 0.015)) / close) * 100.0,
        ),
    )
    near_support = dist_support is not None and dist_support <= near_band_pct
    near_resistance = dist_resistance is not None and dist_resistance <= near_band_pct

    if near_support and near_resistance:
        zone_label = "窄区间"
    elif near_support:
        zone_label = "贴近支撑"
    elif near_resistance:
        zone_label = "逼近压力"
    else:
        zone_label = "区间中段"

    if action in {"关注", "试错"}:
        signal_color = "buy"
        signal_label = "买入"
    elif action in {"减仓", "回避", "持币"}:
        signal_color = "sell"
        signal_label = "卖出"
    else:
        signal_color = "watch"
        signal_label = "观望"

    volume_trigger = 1.2 if score >= 60 else 1.3
    snapshot_text = f"基于 {quote_time} 最新快照" if quote_mode == "realtime" else "基于最近收盘数据"

    if action in {"关注", "试错"}:
        if near_support:
            current_action_text = (
                f"现价贴近支撑1 {fmt_level(support_1)}，优先等回踩不破再试错，不追离支撑过远的拉升。"
            )
            entry_text = (
                f"回踩 {fmt_level(support_1)} 附近不破，且量比回到 {volume_trigger:.1f} 以上，可先小仓试错。"
            )
        elif near_resistance:
            current_action_text = (
                f"现价逼近压力1 {fmt_level(resistance_1)}，此处不追高，只有放量站上后才考虑跟进。"
            )
            entry_text = (
                f"只有放量站上 {fmt_level(resistance_1)}，并维持在该位上方，才把观察升级为跟进。"
            )
        else:
            current_action_text = (
                f"现价位于支撑 {fmt_level(support_1)} 与压力 {fmt_level(resistance_1)} 之间，优先等回踩支撑或突破压力再动作。"
            )
            entry_text = (
                f"回踩 {fmt_level(support_1)} 不破可试错；若放量站上 {fmt_level(resistance_1)}，可顺势跟进。"
            )
        add_text = f"加仓只在站稳 {fmt_level(resistance_1)} 且资金没有重新转弱时执行。"
        reduce_text = f"若跌破 {fmt_level(support_1)} 或主力重新转净流出，先把仓位降下来。"
        invalid_text = f"若收盘有效跌破 {fmt_level(stop_level)}，当前转强逻辑失效。"
    elif action == "观望":
        if near_support:
            current_action_text = (
                f"现价靠近支撑1 {fmt_level(support_1)}，但趋势和资金还没完全共振，先看是否止跌企稳。"
            )
        elif near_resistance:
            current_action_text = (
                f"现价靠近压力1 {fmt_level(resistance_1)}，未放量前按反弹看待，暂不追价。"
            )
        else:
            current_action_text = (
                f"现价位于区间中部，盈亏比一般，等靠近支撑或突破压力后再决策。"
            )
        entry_text = f"等待放量站稳 {fmt_level(resistance_1)}，或回踩 {fmt_level(support_1)} 后再确认。"
        add_text = "确认前不主动加仓，避免在区间中部来回追价。"
        reduce_text = f"若再次失守 {fmt_level(stop_level)}，偏弱结构会进一步强化。"
        invalid_text = f"未收复 {fmt_level(resistance_1)} 前，不把反弹视为趋势反转。"
    else:
        if near_resistance:
            current_action_text = (
                f"反抽已接近压力1 {fmt_level(resistance_1)}，更适合借反弹减仓，而不是去抢修复。"
            )
        elif near_support:
            current_action_text = (
                f"现价贴近支撑1 {fmt_level(support_1)} 但承接仍弱，支撑若失守要优先执行风控。"
            )
        else:
            current_action_text = "趋势和资金仍偏弱，当前以防守处理为主，不做主动开仓。"
        entry_text = "不主动开新仓，先把风险和仓位顺序放在前面。"
        add_text = "未重新转强前不考虑加仓。"
        reduce_text = f"若反抽 {fmt_level(resistance_1)} 仍无量，优先继续减仓。"
        invalid_text = (
            f"只有重新站稳 {fmt_level(resistance_1)} 且量比回到 {volume_trigger:.1f} 以上，弱势判断才算修复。"
        )

    risk_brief = "等待确认"
    if risk_alert:
        risk_brief = risk_alert[0]["title"]
    elif net_mf_amount < 0:
        risk_brief = "资金承接偏弱"
    elif volume_ratio is not None and volume_ratio < 0.9:
        risk_brief = "量能不足"

    decision_summary = (
        f"{bias}，当前以{style}为主，建议 {action}。"
        f"支撑 {fmt_level(support_1)}、压力 {fmt_level(resistance_1)}。"
    )

    key_levels = [*support_levels, *resistance_levels]
    for item in key_levels:
        if item.get("type") == "support":
            item["trigger"] = (
                f"回踩 {fmt_level(item.get('price'))} 不破且量比不低于 {volume_trigger:.1f}，才考虑承接；跌破则防守失败。"
            )
            item["note"] = f"{item['note']} 操作：{item['trigger']}"
        else:
            item["trigger"] = (
                f"只有放量站上 {fmt_level(item.get('price'))} 并维持住，才算压力化解；无量靠近更适合减仓或继续观望。"
            )
            item["note"] = f"{item['note']} 操作：{item['trigger']}"

    level_methodology = list(level_bundle.get("level_methodology") or [])
    level_methodology.append("信号定义：交易结论以趋势、量价、主力资金、量比/换手和实时位置主导，K 线形态只做辅助参考。")

    signal_reasons: list[dict[str, Any]] = []

    def add_reason(kind: str, title: str, desc: str, weight: int) -> None:
        text = str(desc or "").strip()
        if not text:
            return
        signal_reasons.append({
            "kind": kind,
            "title": title,
            "desc": text,
            "weight": abs(int(weight)),
        })

    if ma20 is not None and ma60 is not None and not (pd.isna(ma20) or pd.isna(ma60)):
        if close > ma20 > ma60:
            add_reason("buy", "趋势结构偏强", f"现价站上 MA20/MA60，趋势评分 {trend_factor:.1f}。" if trend_factor is not None else "现价站上 MA20/MA60，中期趋势偏强。", 10)
        elif close < ma20 < ma60:
            add_reason("sell", "趋势结构偏弱", f"现价位于 MA20/MA60 下方，趋势评分 {trend_factor:.1f}。" if trend_factor is not None else "现价位于 MA20/MA60 下方，主趋势仍偏弱。", 10)
        else:
            add_reason("watch", "趋势仍待确认", "价格虽然出现修复，但均线尚未形成顺向共振。", 6)

    if volume_ratio is not None:
        if volume_ratio >= 1.5 and pct_today >= 0:
            add_reason("buy", "量比有效放大", f"量比 {volume_ratio:.2f}，说明跟风盘和承接同时在放大。", 8)
        elif volume_ratio <= 0.85 and abs(pct_today) < 1.5:
            add_reason("watch", "量能不足", f"量比仅 {volume_ratio:.2f}，没有形成能推动突破的增量成交。", 7)

    if turnover is not None:
        if turnover >= 10 and pct_today >= 0:
            add_reason("buy", "换手支持上攻", f"换手率 {turnover:.1f}% ，筹码交换充分，后续更容易走趋势。", 6)
        elif turnover <= 2 and signal_color != "buy":
            add_reason("watch", "换手偏低", f"换手率 {turnover:.1f}% ，当前更像存量博弈，信号确认度有限。", 4)

    if net_mf_amount > 0:
        flow_desc = f"主力净流入 {net_mf_amount:.2f} 万元"
        if positive_flow_days_3 >= 2:
            flow_desc += f"，近 3 日有 {positive_flow_days_3} 天维持净流入。"
        add_reason("buy", "资金承接偏强", flow_desc, 8)
    elif net_mf_amount < 0:
        flow_desc = f"主力净流出 {abs(net_mf_amount):.2f} 万元"
        if negative_flow_days_3 >= 2:
            flow_desc += f"，近 3 日有 {negative_flow_days_3} 天偏弱。"
        add_reason("sell", "资金承接偏弱", flow_desc, 8)

    if factor_score is not None:
        if factor_score >= 65:
            add_reason("buy", "综合因子偏强", f"综合因子分 {factor_score:.1f}，趋势/流动性/事件因子整体占优。", 7)
        elif factor_score <= 40:
            add_reason("sell", "综合因子偏弱", f"综合因子分 {factor_score:.1f}，尚未形成高胜率配置结构。", 7)

    if near_support:
        add_reason(
            "watch" if signal_color == "watch" else signal_color,
            "位置靠近支撑",
            f"现价距支撑1 {fmt_level(support_1)} 仅 {dist_support:.2f}% ，盈亏比会优于在区间中部追价。",
            7,
        )
    elif near_resistance:
        add_reason(
            "sell" if signal_color == "sell" else "watch",
            "位置逼近压力",
            f"现价距压力1 {fmt_level(resistance_1)} 仅 {dist_resistance:.2f}% ，需要放量确认后才算突破。",
            7,
        )
    else:
        add_reason("watch", "位置处于中段", f"当前位于支撑 {fmt_level(support_1)} 与压力 {fmt_level(resistance_1)} 之间，先等更优位置。", 5)

    if risk_alert:
        add_reason("sell" if signal_color == "sell" else "watch", risk_alert[0]["title"], risk_alert[0]["desc"], 7)

    if pattern_view:
        add_reason(
            "watch",
            f"形态参考：{pattern_view[0]['pattern']}",
            "形态信号只作为辅助观察，不参与关键点位和仓位主判断。",
            2,
        )

    add_reason("watch", "快照口径", f"{snapshot_text}，当前判定位置为“{zone_label}”。", 3)
    signal_reasons = sorted(signal_reasons, key=lambda item: (-item["weight"], item["title"]))
    observation_points = [f"{item['title']}：{item['desc']}" for item in signal_reasons[:5]]
    signal_reasons = [{k: v for k, v in item.items() if k != "weight"} for item in signal_reasons[:5]]

    action_signal = {
        "color": signal_color,
        "label": signal_label,
        "headline": current_action_text,
        "zone": zone_label,
        "trigger": entry_text,
        "fallback": invalid_text,
        "snapshot": snapshot_text,
    }

    summary_parts = [
        f"【结论】{decision_summary}",
        f"【动作】{current_action_text}",
        f"【风险】{risk_brief}，跌破 {fmt_level(stop_level)} 需收缩仓位。",
    ]
    summary_parts = [item for item in summary_parts if item]

    institution_order = {"strong": 0, "medium": 1, "light": 2, "neutral": 3, "weak": 4, "warning": 5, "bearish": 6}
    hotmoney_order = {"extreme": 0, "strong": 1, "medium": 2, "light": 3, "neutral": 4, "weak": 5, "warning": 6, "danger": 7}
    pattern_order = {"bullish": 0, "reversal_bull": 1, "special": 2, "warning": 3, "reversal_bear": 4}

    institution_sorted = sorted(institution_view, key=lambda x: institution_order.get(x.get("level", "neutral"), 3))
    hotmoney_sorted = sorted(hotmoney_view, key=lambda x: hotmoney_order.get(x.get("level", "neutral"), 4))
    pattern_sorted = sorted(pattern_view, key=lambda x: pattern_order.get(x.get("type", "special"), 2))

    intraday_context = {
        "mode": quote_mode,
        "quote_time": quote_time or None,
        "snapshot": snapshot_text,
        "zone": zone_label,
        "distance_to_support_1": dist_support,
        "distance_to_resistance_1": dist_resistance,
        "status": current_action_text,
    }

    return {
        "summary": " | ".join(summary_parts),
        "decision": {
            "score": score,
            "bias": bias,
            "action": action,
            "confidence": confidence,
            "style": style,
            "summary": decision_summary,
        },
        "trade_plan": {
            "current": current_action_text,
            "entry": entry_text,
            "add": add_text,
            "reduce": reduce_text,
            "invalid": invalid_text,
            "position": _position_text(action, confidence),
        },
        "action_signal": action_signal,
        "signal_reasons": signal_reasons,
        "intraday_context": intraday_context,
        "key_levels": key_levels,
        "level_methodology": level_methodology,
        "observation_points": observation_points,
        "classification": classification,
        "institution": institution_sorted,
        "hotmoney": hotmoney_sorted,
        "patterns": pattern_sorted,
        "risk_alert": risk_alert,
        "technical": {
            "close": close,
            "open": open_price,
            "pre_close": prev_close,
            "pct_today": pct_today,
            "volume": vol_today,
            "volume_ratio": volume_ratio,
            "vol_ratio_5": vol_ratio_5,
            "vol_ratio_20": vol_ratio_20,
            "turnover": turnover,
            "amount": amount_today,
            "amount_ratio": amount_ratio,
            "net_mf_amount": net_mf_amount,
            "net_mf_ratio": net_mf_ratio,
            "big_order_ratio": big_order_ratio,
            "amplitude": amplitude_today,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma60": ma60,
            "factor_score": factor_score,
            "trend_factor": trend_factor,
            "flow_factor": flow_factor,
            "quality_factor": quality_factor,
            "value_factor": value_factor,
            "event_factor": event_factor,
            "rps_20": rps_20,
            "rps_50": rps_50,
            "rps_120": rps_120,
            "score": score,
            "zone": zone_label,
            "quote_time": quote_time or None,
            "support_1": support_1,
            "support_2": support_2,
            "resistance_1": resistance_1,
            "resistance_2": resistance_2,
        }
    }

# /backend/api/routes/ai.py

import json
import logging
import math
import re
from typing import Any, Optional

import httpx
import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from db.connection import get_db_connection, fetch_df
from .users import get_current_user_id

logger = logging.getLogger(__name__)
router = APIRouter(tags=["AI"])

BASE_ANALYSIS_SYSTEM_PROMPT = (
    "你是A股交易分析助手。下面提供的是数据库中的客观行情、资金、持仓与基础指标摘要，"
    "不代表程序已经给出交易结论。你的任务是基于这些事实自行完成分析，"
    "输出简洁、可执行、面向交易的判断。"
    "识别形态时，要同时结合形态骨架、量价因子、板块共振与不同板块的波动系数，"
    "不要把主板和创业板/科创板放在同一个波动标准下比较。"
    "不要把输入中的字段名、程序标签或程序评分直接机械改写成结论，"
    "要先基于事实再做推断；若证据不足，明确写“等待确认”。"
    "禁止横向推荐其他股票，禁止空喊看好，禁止把程序中间结论当成最终答案。"
)

DEFAULT_ANALYSIS_USER_PROMPT = """以下资料包含标的自身的客观行情、资金、持仓、多维形态坐标与最相关板块共振，不带入主观结论。请基于这些事实自行输出聚焦交易决策的 Markdown，总字数控制在 320-520 字。

输出格式固定为：
## 交易结论
- 观点：偏多 / 中性 / 偏空
- 建议：低吸试错 / 回踩接回 / 持有 / 冲高减仓 / 观望 / 回避
- 时效：盘中 / 1-3日 / 1-2周
- 置信度：高 / 中 / 低

## 核心依据
- 最多 3 条，只写最关键证据，优先从价格位置、量价、资金、持仓盈亏中挑最有用的

## 买卖点
- 买点：
- 卖点：
- 追价条件：
- 止损 / 失效：
- 仓位建议：

## 持仓应对
- 空仓：
- 持仓：

## 后续观察
- 量价确认：
- 失效信号：
- 需要补充的数据：

## 风险
- 最多 2 条，直接写风险触发点

要求：
1. 结论先行，但所有判断都必须基于下面给出的客观数据，不要把任何标签或字段名直接当结论复述。
2. 优先从多维形态坐标、价格位置、成交量、资金流、板块共振和持仓成本这些事实推导交易计划。
3. 如果关键价格位、趋势延续、量能确认不足，不要勉强下结论，直接写“等待确认”。
4. 不做横向推荐或题材扩写，不写成长篇研报，不复述无关常识。
5. 可以做推断，但要尽量让推断与下方事实一一对应；若三套形态都不满足，明确写“当前不属于高确定性形态”。

### 标的概览
{stock_snapshot}

### 资金与杠杆
{capital_flow_snapshot}

### 板块与概念共振
{sector_context}

### 持仓信息
{holding_context}

### 多维形态坐标
{pattern_factor_snapshot}

### 客观补充数据
{commentary_snapshot}
"""

PATTERN_RESONANCE_PRESET = {
    "key": "multidimensional_pattern_resonance",
    "name": "多维形态共振",
    "description": "把板块波动系数、形态骨架、量价因子与板块共振合并到同一套分析模板。",
    "content": """Role: 资深量化交易专家 & K线形态识别引擎
Task: 扫描以下行情、形态坐标与板块共振数据，识别三大核心策略，并给出交易判断。

核心规则：
1. 先识别标的所属板块的涨停阈值与波动系数 K。主板默认 K=1.0，创业板/科创板默认 K=2.0。
2. 先看形态骨架，再看缩量、空间、乖离、MACD、趋势对齐和板块共振，不能只凭单一 K 线下结论。
3. 头7龙回头允许 Day5-Day8 的模糊匹配；单阳不破重视长阳后的横盘承接；大眼睛/空中加油重视 MA5/MA20 收敛、MACD 与放量反包。
4. 评分按 100 分制理解：形态匹配度 40，量价与结构因子 20，板块热度 20，长期趋势对齐 20。
5. 如果三套形态都不满足，不要硬贴标签，直接说明“不属于高确定性形态”。

输出格式固定为：
## 交易结论
- 观点：偏多 / 中性 / 偏空
- 建议：低吸试错 / 回踩接回 / 持有 / 冲高减仓 / 观望 / 回避
- 时效：盘中 / 1-3日 / 1-2周
- 置信度：高 / 中 / 低

## 形态识别
- 第一候选：
- 第二候选：
- 放弃理由：

## 核心依据
- 最多 4 条，只写最关键证据，优先引用板块系数、阶段窗口、缩量、空间、MACD、板块共振与关键均线。

## 买卖点
- 买点：
- 卖点：
- 追价条件：
- 止损 / 失效：
- 仓位建议：

## 风险
- 最多 2 条，直接写触发点。

### 标的概览
{stock_snapshot}

### 资金与杠杆
{capital_flow_snapshot}

### 板块与概念共振
{sector_context}

### 多维形态坐标
{pattern_factor_snapshot}

### 持仓信息
{holding_context}

### 客观补充数据
{commentary_snapshot}
""",
}
PROMPT_PRESETS = [PATTERN_RESONANCE_PRESET]

TEMPLATE_CUTOFF_MARKERS = ("请分析并给出：", "请给出详细分析：")
TEMPLATE_REMOVAL_KEYWORDS = ("关联个股", "同行业", "技术壁垒", "业绩预期向上")
TEMPLATE_EXECUTION_SUFFIX = """

---
请将上面资料压缩为交易结论，并严格遵守：
1. 只输出 `交易结论`、`核心依据`、`买卖点`、`持仓应对`、`后续观察`、`风险` 六部分；
2. 结论先行，总字数控制在 320-520 字；
3. 不做横向推荐或题材联想，不展开成长篇研报；
4. 所有判断必须基于提供的客观数据，不要把程序字段或标签直接翻译成结论；
5. 买卖点、追价条件、止损/失效和仓位建议要尽量明确；数据不足时明确写“等待确认”。
"""
TEMPLATE_EQUIVALENT_KEYS = {
    "stock_snapshot": {"stock_snapshot", "stock_basic", "price_data"},
    "capital_flow_snapshot": {"capital_flow_snapshot", "money_flow", "margin_data"},
    "sector_context": {"sector_context"},
    "holding_context": {"holding_context", "holding"},
    "pattern_factor_snapshot": {"pattern_factor_snapshot"},
    "commentary_snapshot": {"commentary_snapshot"},
}
MANDATORY_TEMPLATE_BLOCKS = [
    ("标的概览", "stock_snapshot"),
    ("资金与杠杆", "capital_flow_snapshot"),
    ("板块与概念共振", "sector_context"),
    ("持仓信息", "holding_context"),
    ("多维形态坐标", "pattern_factor_snapshot"),
    ("客观补充数据", "commentary_snapshot"),
]
FORBIDDEN_TEMPLATE_PLACEHOLDERS = (
    "{theme_context}",
    "{market_context}",
    "{market_sentiment}",
    "{mainline}",
)
FORBIDDEN_TEMPLATE_SECTION_TITLES = ("市场环境",)

class UserAIConfig(BaseModel):
    model_provider: str = "openai"
    model_name: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    system_prompt: Optional[str] = None
    max_tokens: int = 1200
    temperature: float = 0.35

class UserAIConfigUpdate(BaseModel):
    model_provider: Optional[str] = None
    model_name: Optional[str] = None
    api_key: Optional[str] = None
    base_url: Optional[str] = None
    system_prompt: Optional[str] = None
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None

class PromptTemplateCreate(BaseModel):
    name: str
    content: str
    is_default: bool = False

class PromptTemplateUpdate(BaseModel):
    name: Optional[str] = None
    content: Optional[str] = None
    is_default: Optional[bool] = None


AI_PROVIDER_DEFAULTS = {
    "deepseek": {
        "model_provider": "deepseek",
        "model_name": None,
        "api_key": None,
        "base_url": None,
        "system_prompt": None,
        "max_tokens": 1200,
        "temperature": 0.35,
    },
    "openai": {
        "model_provider": "openai",
        "model_name": None,
        "api_key": None,
        "base_url": None,
        "system_prompt": None,
        "max_tokens": 1200,
        "temperature": 0.35,
    },
    "gemini": {
        "model_provider": "gemini",
        "model_name": None,
        "api_key": None,
        "base_url": None,
        "system_prompt": None,
        "max_tokens": 1200,
        "temperature": 0.35,
    },
}
AI_PROVIDER_CONFIG_FIELDS = (
    "model_name",
    "api_key",
    "base_url",
    "system_prompt",
    "max_tokens",
    "temperature",
)


def _model_payload(model: BaseModel) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump(exclude_unset=True)
    return model.dict(exclude_unset=True)


def _clone_provider_defaults(provider: str) -> dict[str, Any]:
    defaults = AI_PROVIDER_DEFAULTS.get(provider, AI_PROVIDER_DEFAULTS["openai"])
    cloned = dict(defaults)
    cloned["model_provider"] = provider
    return cloned


def _apply_provider_values(target: dict[str, Any], values: dict[str, Any]) -> dict[str, Any]:
    for field in ("model_provider", *AI_PROVIDER_CONFIG_FIELDS):
        if field in values:
            target[field] = values[field]
    return target


def _load_user_ai_config_bundle(con, user_id: int) -> dict[str, Any]:
    provider_configs = {
        provider: _clone_provider_defaults(provider)
        for provider in AI_PROVIDER_DEFAULTS
    }

    base_row = con.execute(
        """
        SELECT model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature, selected_template_id
        FROM user_ai_config
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchone()

    current_provider = "openai"
    selected_template_id = None
    if base_row:
        current_provider = str(base_row[0] or "openai")
        if current_provider not in provider_configs:
            provider_configs[current_provider] = _clone_provider_defaults(current_provider)
        _apply_provider_values(
            provider_configs[current_provider],
            {
                "model_provider": current_provider,
                "model_name": base_row[1],
                "api_key": base_row[2],
                "base_url": base_row[3],
                "system_prompt": base_row[4],
                "max_tokens": base_row[5],
                "temperature": base_row[6],
            },
        )
        selected_template_id = base_row[7]

    provider_rows = con.execute(
        """
        SELECT provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature
        FROM user_ai_provider_configs
        WHERE user_id = ?
        """,
        (user_id,),
    ).fetchall()

    for row in provider_rows:
        provider = str(row[0] or "openai")
        if provider not in provider_configs:
            provider_configs[provider] = _clone_provider_defaults(provider)
        _apply_provider_values(
            provider_configs[provider],
            {
                "model_provider": provider,
                "model_name": row[1],
                "api_key": row[2],
                "base_url": row[3],
                "system_prompt": row[4],
                "max_tokens": row[5],
                "temperature": row[6],
            },
        )

    if current_provider not in provider_configs:
        provider_configs[current_provider] = _clone_provider_defaults(current_provider)

    current_config = dict(provider_configs[current_provider])
    current_config["selected_template_id"] = selected_template_id
    current_config["provider_configs"] = provider_configs
    return current_config


def _upsert_provider_ai_config(con, user_id: int, provider: str, values: dict[str, Any]) -> None:
    current_bundle = _load_user_ai_config_bundle(con, user_id)
    provider_configs = current_bundle.get("provider_configs", {})
    merged_values = dict(provider_configs.get(provider) or _clone_provider_defaults(provider))
    for field in AI_PROVIDER_CONFIG_FIELDS:
        if field in values:
            merged_values[field] = values[field]

    exists = con.execute(
        "SELECT 1 FROM user_ai_provider_configs WHERE user_id = ? AND provider = ?",
        (user_id, provider),
    ).fetchone()

    params = (
        merged_values.get("model_name"),
        merged_values.get("api_key"),
        merged_values.get("base_url"),
        merged_values.get("system_prompt"),
        merged_values.get("max_tokens"),
        merged_values.get("temperature"),
        user_id,
        provider,
    )
    if exists:
        con.execute(
            """
            UPDATE user_ai_provider_configs
            SET model_name = ?, api_key = ?, base_url = ?, system_prompt = ?,
                max_tokens = ?, temperature = ?, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND provider = ?
            """,
            params,
        )
    else:
        con.execute(
            """
            INSERT INTO user_ai_provider_configs (
                model_name, api_key, base_url, system_prompt, max_tokens, temperature, user_id, provider
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            params,
        )

    selected_template_id = current_bundle.get("selected_template_id")
    legacy_exists = con.execute(
        "SELECT 1 FROM user_ai_config WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    legacy_params = (
        provider,
        merged_values.get("model_name"),
        merged_values.get("api_key"),
        merged_values.get("base_url"),
        merged_values.get("system_prompt"),
        merged_values.get("max_tokens"),
        merged_values.get("temperature"),
        selected_template_id,
        user_id,
    )
    if legacy_exists:
        con.execute(
            """
            UPDATE user_ai_config
            SET model_provider = ?, model_name = ?, api_key = ?, base_url = ?, system_prompt = ?,
                max_tokens = ?, temperature = ?, selected_template_id = ?, updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ?
            """,
            legacy_params,
        )
    else:
        con.execute(
            """
            INSERT INTO user_ai_config (
                model_provider, model_name, api_key, base_url, system_prompt,
                max_tokens, temperature, selected_template_id, user_id
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            legacy_params,
        )

class AIAnalyzeRequest(BaseModel):
    ts_code: str
    template_id: Optional[int] = None
    force_refresh: Optional[bool] = False

class SelectTemplateRequest(BaseModel):
    template_id: Optional[int] = None


def _safe_float(value, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        if value is None:
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except (TypeError, ValueError):
        return default


def _fmt_price(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number:.2f}"


def _fmt_pct(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number:+.2f}%"


def _fmt_ratio_pct(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number * 100:.1f}%"


def _fmt_wan(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number:.2f}万"


def _fmt_lot_volume(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    if number is None:
        return "暂无"
    if abs(number) >= 10000:
        return f"{number / 10000:.2f}万手"
    return f"{number:.0f}手"


def _fmt_yi(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number / 100000000:.2f}亿"


def _fmt_thousand_yuan_to_yi(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number / 100000:.2f}亿"


def _prepare_analysis_df(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy().sort_values("trade_date").reset_index(drop=True)
    if "volume" not in work.columns and "vol" in work.columns:
        work = work.rename(columns={"vol": "volume"})
    for column in (
        "open",
        "high",
        "low",
        "close",
        "volume",
        "amount",
        "pct_chg",
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "vol_ma5",
        "turnover_rate",
        "volume_ratio",
        "net_mf_amount",
        "net_mf_ratio",
        "big_order_ratio",
        "rps_20",
        "rps_50",
        "rps_120",
        "trend_score",
        "quality_score",
        "flow_score",
        "value_score",
        "event_score",
        "factor_score",
    ):
        if column in work.columns:
            work[column] = pd.to_numeric(work[column], errors="coerce")
    for ma in (5, 10, 20, 60, 120, 250):
        col = f"ma{ma}"
        if col not in work.columns:
            work[col] = work["close"].rolling(ma, min_periods=1).mean()
    if "vol_ma20" not in work.columns:
        work["vol_ma20"] = work["volume"].rolling(20, min_periods=1).mean()
    if "volume_ratio" not in work.columns and "vol_ma5" in work.columns:
        base = pd.to_numeric(work["vol_ma5"], errors="coerce")
        work["volume_ratio"] = (work["volume"] / base).where(base > 0)
    return work


def _infer_market_profile(ts_code: str) -> dict[str, Any]:
    code = str(ts_code or "").strip().upper()
    prefix = code[:3]
    if prefix in {"300", "688"}:
        return {
            "board_key": "growth_board",
            "board_label": "创业板/科创板",
            "limit_up_pct": 19.8,
            "k": 2.0,
        }
    return {
        "board_key": "main_board",
        "board_label": "主板",
        "limit_up_pct": 9.8,
        "k": 1.0,
    }


def _compute_macd(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    close = pd.to_numeric(work["close"], errors="coerce")
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    hist = (dif - dea) * 2
    work["macd_dif"] = dif
    work["macd_dea"] = dea
    work["macd_hist"] = hist
    return work


def _calc_amplitude_pct(row: pd.Series) -> float | None:
    high = _safe_float(row.get("high"), None)
    low = _safe_float(row.get("low"), None)
    if high is None or low is None or low <= 0:
        return None
    return (high / low - 1.0) * 100.0


def _find_recent_limit_up_index(df: pd.DataFrame, limit_up_pct: float, lookback: int = 40) -> int | None:
    if df.empty:
        return None
    start = max(0, len(df) - lookback)
    for idx in range(len(df) - 2, start - 1, -1):
        row = df.iloc[idx]
        pct_chg = _safe_float(row.get("pct_chg"), None)
        high = _safe_float(row.get("high"), None)
        close = _safe_float(row.get("close"), None)
        if pct_chg is None or high is None or close is None:
            continue
        if pct_chg >= limit_up_pct and close >= high * 0.998:
            return idx
    return None


def _find_recent_long_bull_index(df: pd.DataFrame, threshold_pct: float, lookback: int = 30) -> int | None:
    if df.empty:
        return None
    start = max(0, len(df) - lookback)
    for idx in range(len(df) - 2, start - 1, -1):
        row = df.iloc[idx]
        pct_chg = _safe_float(row.get("pct_chg"), None)
        open_price = _safe_float(row.get("open"), None)
        close = _safe_float(row.get("close"), None)
        volume = _safe_float(row.get("volume"), None)
        if pct_chg is None or open_price is None or close is None or volume is None:
            continue
        prev_slice = df.iloc[max(0, idx - 5):idx]
        prev_vol_ma5 = _safe_float(prev_slice["volume"].mean(), None) if not prev_slice.empty else None
        if (
            pct_chg > threshold_pct
            and close > open_price
            and prev_vol_ma5
            and prev_vol_ma5 > 0
            and volume >= prev_vol_ma5 * 2.0
        ):
            return idx
    return None


def _score_from_rank(rank: int | None, total: int | None) -> tuple[int, str]:
    if not rank or not total:
        return 6, "板块热度暂无可靠排名"
    if rank <= 3:
        return 20, f"板块热度位于前 {rank}/{total}"
    if rank <= 5:
        return 16, f"板块热度位于前 5（当前 {rank}/{total}）"
    if rank <= 10:
        return 11, f"板块热度中上（当前 {rank}/{total}）"
    return 6, f"板块热度一般（当前 {rank}/{total}）"


def _score_trend_alignment(latest: pd.Series) -> tuple[int, str]:
    close = _safe_float(latest.get("close"), None)
    ma250 = _safe_float(latest.get("ma250"), None)
    ma120 = _safe_float(latest.get("ma120"), None)
    ma60 = _safe_float(latest.get("ma60"), None)
    if close is None:
        return 0, "长期趋势数据不足"
    if ma250 and close >= ma250:
        return 20, "收盘位于 MA250 上方"
    if ma120 and close >= ma120:
        return 14, "收盘位于 MA120 上方"
    if ma60 and close >= ma60:
        return 10, "收盘仍在 MA60 上方"
    return 4, "收盘仍在中长期均线下方"


def _format_strategy_result(
    name: str,
    total_score: int,
    shape_score: int,
    factor_score: int,
    sector_score: int,
    trend_score: int,
    details: list[str],
) -> str:
    if total_score >= 75:
        verdict = "高匹配"
    elif total_score >= 60:
        verdict = "观察"
    else:
        verdict = "弱匹配"
    detail_text = "；".join(item for item in details if item)
    return (
        f"- {name}：{verdict}，总分 {total_score}/100"
        f"（形态 {shape_score} / 因子 {factor_score} / 板块 {sector_score} / 趋势 {trend_score}）。"
        f"{detail_text}"
    )


def _evaluate_dragon_return(
    df: pd.DataFrame,
    profile: dict[str, Any],
    sector_score: int,
    trend_score: int,
) -> str:
    base_idx = _find_recent_limit_up_index(df, profile["limit_up_pct"])
    if base_idx is None:
        return _format_strategy_result(
            "头7龙回头",
            sector_score + trend_score,
            0,
            0,
            sector_score,
            trend_score,
            ["近 40 日未识别到可作为 Day0 的封板基准"],
        )

    latest = df.iloc[-1]
    base = df.iloc[base_idx]
    gap = len(df) - 1 - base_idx
    k = float(profile["k"])
    body_low = min(_safe_float(base.get("open"), 0.0) or 0.0, _safe_float(base.get("close"), 0.0) or 0.0)
    body_high = max(_safe_float(base.get("open"), 0.0) or 0.0, _safe_float(base.get("close"), 0.0) or 0.0)
    latest_close = _safe_float(latest.get("close"), 0.0) or 0.0
    latest_low = _safe_float(latest.get("low"), latest_close) or latest_close
    base_low = _safe_float(base.get("low"), latest_low) or latest_low
    base_high = _safe_float(base.get("high"), latest_close) or latest_close
    max_high_after = _safe_float(df.iloc[base_idx + 1:]["high"].max(), base_high) or base_high
    latest_volume = _safe_float(latest.get("volume"), None)
    base_volume = _safe_float(base.get("volume"), None)
    v_shrink = (latest_volume / base_volume) if latest_volume is not None and base_volume and base_volume > 0 else None

    shape_score = 0
    factor_score = 0
    details = [f"Day0={str(base.get('trade_date'))[:10]}，当前位于第 {gap} 个交易日"]

    if 5 <= gap <= 8:
        shape_score += 14
    elif 4 <= gap <= 9:
        shape_score += 10
    elif gap <= 12:
        shape_score += 6

    if max_high_after <= base_high * 1.10:
        shape_score += 8
        details.append("调整期未突破 Day0 高点 110%")
    else:
        details.append("调整期曾明显突破 Day0 高点 110%")

    if body_low <= latest_close <= body_high:
        shape_score += 10
        details.append("收盘回到 Day0 阳线实体区间")
    elif latest_close >= body_low * (1 - 0.02 * k) and latest_close <= body_high * (1 + 0.02 * k):
        shape_score += 6
        details.append("收盘接近 Day0 实体区间")
    else:
        details.append("收盘已偏离 Day0 实体区间")

    if latest_low >= base_low * (1 - 0.03 * k):
        shape_score += 8
        details.append("低点未跌破 Day0 低点防线")
    else:
        details.append("低点已跌破 Day0 低点防线")

    if v_shrink is not None:
        if v_shrink <= 0.4:
            factor_score += 12
            details.append(f"缩量因子 {v_shrink:.2f}")
        elif v_shrink <= 0.6:
            factor_score += 8
            details.append(f"缩量因子 {v_shrink:.2f}")
        elif v_shrink <= 0.8:
            factor_score += 4
            details.append(f"缩量因子 {v_shrink:.2f}")

    ma10 = _safe_float(latest.get("ma10"), None)
    ma20 = _safe_float(latest.get("ma20"), None)
    near_ma10 = ma10 is not None and ma10 > 0 and abs(latest_close / ma10 - 1.0) <= 0.02 * k
    near_ma20 = ma20 is not None and ma20 > 0 and abs(latest_close / ma20 - 1.0) <= 0.02 * k
    if near_ma10 or near_ma20:
        factor_score += 8
        details.append("价格贴近 MA10/MA20 支撑")

    total_score = int(min(100, shape_score + factor_score + sector_score + trend_score))
    return _format_strategy_result("头7龙回头", total_score, shape_score, factor_score, sector_score, trend_score, details)


def _evaluate_indestructible_yang(
    df: pd.DataFrame,
    profile: dict[str, Any],
    sector_score: int,
    trend_score: int,
) -> str:
    base_idx = _find_recent_long_bull_index(df, 6.0 * float(profile["k"]))
    if base_idx is None:
        return _format_strategy_result(
            "单阳不破",
            sector_score + trend_score,
            0,
            0,
            sector_score,
            trend_score,
            ["近 30 日未识别到放量长阳基准日"],
        )

    latest = df.iloc[-1]
    base = df.iloc[base_idx]
    gap = len(df) - 1 - base_idx
    k = float(profile["k"])
    post_df = df.iloc[base_idx + 1:].copy()
    base_open = _safe_float(base.get("open"), 0.0) or 0.0
    base_volume = _safe_float(base.get("volume"), None)
    latest_volume = _safe_float(latest.get("volume"), None)
    latest_close = _safe_float(latest.get("close"), 0.0) or 0.0

    shape_score = 0
    factor_score = 0
    details = [f"基准长阳={str(base.get('trade_date'))[:10]}，当前维持 {gap} 个交易日"]

    if 6 <= gap <= 10:
        shape_score += 14
    elif 5 <= gap <= 12:
        shape_score += 9

    if not post_df.empty:
        guard_holds = bool((pd.to_numeric(post_df["low"], errors="coerce") >= base_open).all())
        if guard_holds:
            shape_score += 14
            details.append("调整期低点始终未破基准日开盘价")
        else:
            details.append("调整期曾跌破基准日开盘价")

        mid_close = pd.to_numeric(post_df["close"], errors="coerce").mean()
        if latest_close > 0 and mid_close and abs(latest_close / mid_close - 1.0) <= 0.03 * k:
            shape_score += 12
            details.append("价格重心仍以横向整理为主")

        amp_series = post_df.apply(_calc_amplitude_pct, axis=1).dropna()
        if not amp_series.empty:
            amp_now = float(amp_series.tail(3).mean())
            if amp_now <= 4.0 * k:
                factor_score += 12
                details.append(f"近 3 日振幅收敛至 {amp_now:.2f}%")
            elif amp_now <= 5.0 * k:
                factor_score += 8
                details.append(f"近 3 日振幅回落至 {amp_now:.2f}%")

    if latest_volume is not None and base_volume and base_volume > 0:
        shrink = latest_volume / base_volume
        if shrink <= 0.6:
            factor_score += 8
            details.append(f"相对基准长阳缩量至 {shrink:.2f}")
        elif shrink <= 0.8:
            factor_score += 4
            details.append(f"相对基准长阳缩量至 {shrink:.2f}")

    total_score = int(min(100, shape_score + factor_score + sector_score + trend_score))
    return _format_strategy_result("单阳不破", total_score, shape_score, factor_score, sector_score, trend_score, details)


def _evaluate_golden_eye(
    df: pd.DataFrame,
    profile: dict[str, Any],
    sector_score: int,
    trend_score: int,
) -> str:
    work = _compute_macd(df)
    latest = work.iloc[-1]
    ma5 = pd.to_numeric(work["ma5"], errors="coerce")
    ma20 = pd.to_numeric(work["ma20"], errors="coerce")
    cross = (ma5 > ma20).astype(int).diff().fillna(0)
    golden_indexes = cross[cross > 0].index.tolist()
    recent_golden = golden_indexes[-1] if golden_indexes else None
    latest_close = _safe_float(latest.get("close"), 0.0) or 0.0
    k = float(profile["k"])

    shape_score = 0
    factor_score = 0
    details = []

    if recent_golden is not None:
        days_since = len(work) - 1 - recent_golden
        details.append(f"最近一次 MA5 上穿 MA20 距今 {days_since} 日")
        if days_since <= 15:
            shape_score += 10
        ma20_now = _safe_float(latest.get("ma20"), None)
        ma20_then = _safe_float(work.iloc[max(0, recent_golden - 3)].get("ma20"), None)
        if ma20_now is not None and ma20_then is not None and ma20_now >= ma20_then:
            shape_score += 10
            details.append("MA20 仍保持抬升")

    ma5_now = _safe_float(latest.get("ma5"), None)
    ma20_now = _safe_float(latest.get("ma20"), None)
    if ma5_now is not None and ma20_now is not None and ma20_now > 0:
        gap_pct = abs(ma5_now - ma20_now) / ma20_now * 100.0
        if ma5_now >= ma20_now and gap_pct <= 1.5 * k:
            shape_score += 16
            details.append(f"MA5 与 MA20 距离 {gap_pct:.2f}%")
        elif gap_pct <= 2.2 * k:
            shape_score += 8
            details.append(f"MA5 与 MA20 接近收敛，距离 {gap_pct:.2f}%")

    if len(work) >= 3:
        prev_1 = work.iloc[-2]
        prev_2 = work.iloc[-3]
        latest_open = _safe_float(latest.get("open"), None)
        prev_open = _safe_float(prev_1.get("open"), None)
        prev_close = _safe_float(prev_1.get("close"), None)
        latest_volume = _safe_float(latest.get("volume"), None)
        vol_ma5 = _safe_float(latest.get("vol_ma5"), None)
        if (
            latest_open is not None
            and prev_open is not None
            and prev_close is not None
            and latest_close > latest_open
            and latest_close >= max(prev_open, prev_close)
            and latest_open <= min(prev_open, prev_close)
            and latest_volume is not None
            and vol_ma5
            and latest_volume >= vol_ma5 * 1.2
        ):
            shape_score += 14
            details.append("出现放量反包阳线")

    dif = _safe_float(latest.get("macd_dif"), None)
    dea = _safe_float(latest.get("macd_dea"), None)
    rps20 = _safe_float(latest.get("rps_20"), None)
    if dif is not None and dea is not None:
        if dif >= 0 and dif >= dea:
            factor_score += 12
            details.append(f"MACD 位于零轴上方并保持金叉（DIF {dif:.3f} / DEA {dea:.3f}）")
        elif dif >= dea:
            factor_score += 8
            details.append(f"MACD 刚转强（DIF {dif:.3f} / DEA {dea:.3f}）")
    if rps20 is not None:
        if rps20 >= 85:
            factor_score += 8
            details.append(f"RPS20 {rps20:.1f}，相对强势明显")
        elif rps20 >= 70:
            factor_score += 5
            details.append(f"RPS20 {rps20:.1f}，个股强于多数标的")

    total_score = int(min(100, shape_score + factor_score + sector_score + trend_score))
    return _format_strategy_result("大眼睛/空中加油", total_score, shape_score, factor_score, sector_score, trend_score, details)


def _build_pattern_factor_snapshot(
    df: pd.DataFrame,
    ts_code: str,
    sector_meta: Optional[dict[str, Any]] = None,
) -> str:
    if df.empty:
        return "暂无形态坐标数据。"

    work = _compute_macd(df)
    latest = work.iloc[-1]
    profile = _infer_market_profile(ts_code)
    sector_meta = sector_meta or {}
    sector_score, sector_text = _score_from_rank(
        sector_meta.get("sector_heat_rank"),
        sector_meta.get("sector_heat_total"),
    )
    trend_score, trend_text = _score_trend_alignment(latest)

    close = _safe_float(latest.get("close"), None)
    ma20 = _safe_float(latest.get("ma20"), None)
    ma120 = _safe_float(latest.get("ma120"), None)
    ma250 = _safe_float(latest.get("ma250"), None)
    bias_20 = ((close / ma20) - 1.0) * 100.0 if close is not None and ma20 else None
    high_120 = _safe_float(work["high"].tail(120).max() if "high" in work.columns else None, None)
    space_120 = ((high_120 / close) - 1.0) * 100.0 if close and high_120 and close > 0 else None
    dif = _safe_float(latest.get("macd_dif"), None)
    dea = _safe_float(latest.get("macd_dea"), None)
    rps20 = _safe_float(latest.get("rps_20"), None)
    focus_tags = [str(item).strip() for item in (sector_meta.get("focus_tags") or []) if str(item).strip()]
    related_theme = " / ".join(focus_tags[:3]) or (sector_meta.get("display_name") or sector_meta.get("mapped_sector") or "暂无")

    lines = [
        (
            f"- 板块系数：{profile['board_label']}，涨停阈值 {profile['limit_up_pct']:.1f}% ，"
            f"K={profile['k']:.1f}。"
        ),
        (
            f"- 核心因子：Bias20 {_fmt_pct(bias_20)}，Space(距120日高点) {_fmt_pct(space_120)}，"
            f"MA120 {_fmt_price(ma120)}，MA250 {_fmt_price(ma250)}。"
        ),
        (
            f"- 共振坐标：相关方向 {related_theme}；{sector_text}；{trend_text}；"
            f"MACD DIF/DEA {_fmt_price(dif)} / {_fmt_price(dea)}；RPS20 "
            f"{f'{rps20:.1f}' if rps20 is not None else '暂无'}。"
        ),
        _evaluate_dragon_return(work, profile, sector_score, trend_score),
        _evaluate_indestructible_yang(work, profile, sector_score, trend_score),
        _evaluate_golden_eye(work, profile, sector_score, trend_score),
    ]
    return "\n".join(lines)


def _build_price_snapshot(df: pd.DataFrame) -> tuple[str, dict]:
    if df.empty:
        return "暂无行情摘要", {}

    last = df.iloc[-1]
    last_5 = df.tail(5)
    last_10 = df.tail(10)
    last_20 = df.tail(20)

    close = _safe_float(last.get("close"), 0.0) or 0.0
    pct_today = _safe_float(last.get("pct_chg"), 0.0) or 0.0
    volume = _safe_float(last.get("volume"), 0.0) or 0.0
    amount = _safe_float(last.get("amount"), 0.0) or 0.0
    ma5 = _safe_float(last.get("ma5"), close)
    ma10 = _safe_float(last.get("ma10"), close)
    ma20 = _safe_float(last.get("ma20"), close)
    ma60 = _safe_float(last.get("ma60"), close)
    vol_ma5 = _safe_float(last_5["volume"].mean(), 0.0) or 0.0
    vol_ma20 = _safe_float(last_20["volume"].mean(), 0.0) or 0.0
    vol_ratio_5 = (volume / vol_ma5) if vol_ma5 > 0 else 1.0
    vol_ratio_20 = (volume / vol_ma20) if vol_ma20 > 0 else 1.0

    ret_5 = ((close / df.iloc[-5]["close"]) - 1) * 100 if len(df) >= 5 and df.iloc[-5]["close"] else None
    ret_10 = ((close / df.iloc[-10]["close"]) - 1) * 100 if len(df) >= 10 and df.iloc[-10]["close"] else None
    ret_20 = ((close / df.iloc[-20]["close"]) - 1) * 100 if len(df) >= 20 and df.iloc[-20]["close"] else None

    high_5 = _safe_float(last_5["high"].max() if "high" in last_5.columns else None, None)
    high_10 = _safe_float(last_10["high"].max() if "high" in last_10.columns else None, None)
    high_20 = _safe_float(last_20["high"].max() if "high" in last_20.columns else None, None)
    low_5 = _safe_float(last_5["low"].min() if "low" in last_5.columns else None, None)
    low_10 = _safe_float(last_10["low"].min() if "low" in last_10.columns else None, None)
    low_20 = _safe_float(last_20["low"].min() if "low" in last_20.columns else None, None)

    lines = [
        (
            f"- 最新交易日数据：开 {_fmt_price(last.get('open'))}，高 {_fmt_price(last.get('high'))}，"
            f"低 {_fmt_price(last.get('low'))}，收 {_fmt_price(close)}，当日涨跌 {_fmt_pct(pct_today)}。"
        ),
        f"- 近 5/10/20 日区间收益：{_fmt_pct(ret_5)} / {_fmt_pct(ret_10)} / {_fmt_pct(ret_20)}。",
        (
            f"- 均线数据：MA5/10/20/60 为 {_fmt_price(ma5)} / {_fmt_price(ma10)} / "
            f"{_fmt_price(ma20)} / {_fmt_price(ma60)}；收盘相对 MA5/10/20/60 的偏离为 "
            f"{_fmt_pct((close / ma5 - 1) * 100 if ma5 else None)} / "
            f"{_fmt_pct((close / ma10 - 1) * 100 if ma10 else None)} / "
            f"{_fmt_pct((close / ma20 - 1) * 100 if ma20 else None)} / "
            f"{_fmt_pct((close / ma60 - 1) * 100 if ma60 else None)}。"
        ),
        (
            f"- 成交数据：成交量 {_fmt_lot_volume(volume)}，成交额 {_fmt_thousand_yuan_to_yi(amount)}，"
            f"相对 5 日均量 {vol_ratio_5:.2f} 倍，相对 20 日均量 {vol_ratio_20:.2f} 倍。"
        ),
        (
            f"- 近 5/10/20 日高低点：高 {_fmt_price(high_5)} / {_fmt_price(high_10)} / {_fmt_price(high_20)}；"
            f"低 {_fmt_price(low_5)} / {_fmt_price(low_10)} / {_fmt_price(low_20)}。"
        ),
    ]
    return "\n".join(lines), {
        "close": close,
    }


def _build_money_flow_snapshot(df: pd.DataFrame) -> str:
    if df.empty:
        return "- 主力资金：暂无。"

    work = df.sort_values("trade_date").reset_index(drop=True)
    last = work.iloc[-1]
    tail_3 = work.tail(3)
    tail_5 = work.tail(5)
    net_1 = _safe_float(last.get("net_mf_amount"), 0.0) or 0.0
    net_3 = _safe_float(tail_3["net_mf_amount"].sum(), 0.0) or 0.0
    net_5 = _safe_float(tail_5["net_mf_amount"].sum(), 0.0) or 0.0
    ratio = _safe_float(last.get("net_mf_ratio"), None)
    positive_days = int((tail_5["net_mf_amount"] > 0).sum())
    return (
        f"- 主力净流入：1 日 {_fmt_wan(net_1)}，3 日 {_fmt_wan(net_3)}，5 日 {_fmt_wan(net_5)}。"
        f" 最近 5 日净流入天数 {positive_days}/5，最新净占比 {(_fmt_pct(ratio) if ratio is not None else '暂无')}。"
    )


def _build_margin_snapshot(df: pd.DataFrame) -> str:
    if df.empty or "rzye" not in df.columns:
        return "- 融资杠杆：暂无。"

    work = df.sort_values("trade_date").reset_index(drop=True)
    latest = _safe_float(work.iloc[-1].get("rzye"), None)
    change_5 = None
    change_10 = None
    if len(work) >= 5:
        base_5 = _safe_float(work.iloc[-5].get("rzye"), None)
        if base_5 and base_5 > 0 and latest is not None:
            change_5 = (latest / base_5 - 1) * 100
    if len(work) >= 10:
        base_10 = _safe_float(work.iloc[-10].get("rzye"), None)
        if base_10 and base_10 > 0 and latest is not None:
            change_10 = (latest / base_10 - 1) * 100
    return (
        f"- 融资余额最新 {_fmt_yi(latest)}，5 日变化 {_fmt_pct(change_5)}，10 日变化 {_fmt_pct(change_10)}。"
    )


def _build_holding_snapshot(holding_row, last_close: Optional[float]) -> str:
    if not holding_row:
        return "空仓，优先等确认信号再决策。"

    shares = int(_safe_float(holding_row[0], 0.0) or 0.0)
    avg_cost = _safe_float(holding_row[1], None)
    pnl_pct = None
    if avg_cost and avg_cost > 0 and last_close:
        pnl_pct = (last_close / avg_cost - 1) * 100
    return (
        f"持仓 {shares} 股，成本 {_fmt_price(avg_cost)}，按最新价估算浮盈亏 {_fmt_pct(pnl_pct)}。"
    )


def _parse_json_text(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _build_market_snapshot(
    sentiment_row,
    mainline_rows,
    market_env: Optional[dict[str, Any]] = None,
    top_mainline: Optional[dict[str, Any]] = None,
) -> tuple[str, str, str]:
    market_env = market_env or {}
    details = _parse_json_text(sentiment_row[3] if sentiment_row and len(sentiment_row) > 3 else None)
    factor_details = details.get("factors") or {}
    sentiment_score = _safe_float(sentiment_row[1], _safe_float(market_env.get("sentiment"), 50.0)) or 50.0
    sentiment_label = sentiment_row[2] if sentiment_row else "未知"
    v1 = _safe_float(details.get("v1"), None)
    breadth = _safe_float(factor_details.get("breadth"), None)
    median_chg = _safe_float(factor_details.get("median_chg"), None)
    index_chg = _safe_float(
        factor_details.get("index_chg"),
        _safe_float(market_env.get("index_pct_chg"), None),
    )
    market_line = (
        f"- 市场情绪数据：标签 {sentiment_label or '未知'}，分数 {sentiment_score:.1f}，"
        f"动量 {(_fmt_pct(v1) if v1 is not None else '暂无')}。"
    )

    detail_parts = []
    if index_chg is not None:
        detail_parts.append(f"沪深300 {_fmt_pct(index_chg)}")
    if breadth is not None:
        detail_parts.append(f"市场广度 {breadth:.1f}%")
    if median_chg is not None:
        detail_parts.append(f"中位涨跌 {median_chg:+.1f}%")
    market_detail_line = f"- 交易背景：{'；'.join(detail_parts)}。" if detail_parts else "- 交易背景：暂无。"

    top_display = ""
    top_score = None
    top_driver = ""
    if top_mainline:
        top_display = top_mainline.get("display_name") or top_mainline.get("name") or ""
        top_score = _safe_float(top_mainline.get("score"), None)
        top_driver = top_mainline.get("driver_summary") or ""

    if not top_display and mainline_rows:
        top_display = str(mainline_rows[0][1] or "")
        top_score = _safe_float(mainline_rows[0][2], None)

    top_mainlines = [f"{row[1]}({_safe_float(row[2], 0.0):.1f})" for row in mainline_rows or []]
    if top_display:
        mainline_text = f"- 主线榜单最新方向：{top_display}"
        if top_score is not None:
            mainline_text += f"（{top_score:.1f}）"
        if top_mainlines:
            mainline_text += f"；同日榜单前列还包括 {'、'.join(top_mainlines[:3])}"
        mainline_text += "。"
    else:
        mainline_text = f"- 主线榜单最新方向：{'、'.join(top_mainlines[:3]) if top_mainlines else '暂无'}。"

    return market_line, mainline_text, "\n".join([market_line, market_detail_line, mainline_text])


def _build_sector_snapshot(
    mainline_analysis: Optional[dict[str, Any]],
    review_map: Optional[dict[str, Any]],
    market_env: Optional[dict[str, Any]],
    top_mainline: Optional[dict[str, Any]],
) -> str:
    if not mainline_analysis or mainline_analysis.get("status") != "success":
        return "暂无板块关联摘要。"

    review_map = review_map or {}
    mapped_sector = str(mainline_analysis.get("mapped_sector") or mainline_analysis.get("sector") or "").strip()
    review_item = review_map.get(mapped_sector, {}) if mapped_sector else {}
    display_name = review_item.get("display_name") or mapped_sector or "待确认"
    focus_tags = review_item.get("focus_tags") or []
    driver_summary = review_item.get("driver_summary") or ""

    is_mainline = bool(mainline_analysis.get("is_mainline"))
    sector_rank = mainline_analysis.get("sector_rank")
    sector_total = mainline_analysis.get("sector_total")
    analysis = mainline_analysis.get("analysis") or {}
    signal = mainline_analysis.get("signal") or {}
    factor_scores = analysis.get("factor_scores") or {}
    sector_resonance = analysis.get("sector_resonance") or {}
    breakout = analysis.get("breakout") or {}
    capital_flow = analysis.get("capital_flow") or {}
    risk_reward = analysis.get("risk_reward") or {}

    rank_text = f"{int(sector_rank)}/{int(sector_total)}" if sector_rank and sector_total else "暂无"

    lines = [
        (
            f"- 板块/主线字段：mapped_sector={mapped_sector or '暂无'}，display_name={display_name or '暂无'}，"
            f"is_mainline={str(is_mainline).lower()}，sector_rank={rank_text}。"
        )
    ]
    if focus_tags:
        focus_text = " / ".join(str(item).strip() for item in focus_tags[:3] if str(item).strip())
        if focus_text:
            lines.append(f"- 相关标签：{focus_text}。")
    if driver_summary:
        lines.append(f"- 主线摘要字段：{driver_summary}。")

    metric_parts = []
    if sector_resonance.get("score") is not None:
        metric_parts.append(f"板块共振 {float(sector_resonance['score']):.1f}")
    if breakout.get("score") is not None:
        metric_parts.append(f"突破 {float(breakout['score']):.1f}")
    if capital_flow.get("score") is not None:
        metric_parts.append(f"资金 {float(capital_flow['score']):.1f}")
    if factor_scores.get("sector_rank") is not None:
        metric_parts.append(f"龙头地位 {float(factor_scores['sector_rank']):.1f}")
    if signal.get("score") is not None:
        metric_parts.append(f"综合信号 {float(signal['score']):.1f}")
    if metric_parts:
        lines.append(f"- 板块相关评分字段：{'，'.join(metric_parts)}。")

    risk_parts = []
    entry_zone = risk_reward.get("entry_zone") or []
    if isinstance(entry_zone, list) and len(entry_zone) == 2:
        risk_parts.append(f"entry_zone {_fmt_price(entry_zone[0])}-{_fmt_price(entry_zone[1])}")
    if risk_reward.get("stop_loss") is not None:
        risk_parts.append(f"stop_loss {_fmt_price(risk_reward.get('stop_loss'))}")
    if risk_reward.get("target") is not None:
        risk_parts.append(f"target {_fmt_price(risk_reward.get('target'))}")
    if risk_reward.get("risk_reward") is not None:
        risk_parts.append(f"risk_reward {float(risk_reward['risk_reward']):.2f}")
    if risk_parts:
        lines.append(f"- 风险收益字段：{'，'.join(risk_parts)}。")
    return "\n".join(lines)


def _build_mainline_analysis_fallback(
    ts_code: str,
    trade_date: str,
    review_map: Optional[dict[str, Any]],
    mainline_rows,
) -> Optional[dict[str, Any]]:
    try:
        from strategy.mainline.analyst import mainline_analyst
        from .stocks import get_sector_stocks

        norm_code = str(ts_code or "").strip().upper()
        stock_map_df = mainline_analyst.get_stock_mainline_map(ts_codes=[norm_code])
        mapped_sector = (
            str(stock_map_df.iloc[0]["mapped_name"]).strip()
            if stock_map_df is not None and not stock_map_df.empty and stock_map_df.iloc[0].get("mapped_name")
            else ""
        )
        if not mapped_sector:
            return {
                "status": "success",
                "mapped_sector": "",
                "is_mainline": False,
                "sector_rank": None,
                "sector_total": 0,
                "analysis": {},
                "signal": {},
            }

        review_item = (review_map or {}).get(mapped_sector, {})
        focus_tags = review_item.get("focus_tags", []) or []
        sector_stocks = get_sector_stocks(mapped_sector, trade_date, focus_tags=focus_tags) or []
        sector_total = len(sector_stocks)

        sector_rank = None
        matched_stock = {}
        if sector_stocks:
            ranked_stocks = sorted(
                sector_stocks,
                key=lambda item: (
                    _safe_float(item.get("trend_pioneer_score"), 0.0) or 0.0,
                    _safe_float(item.get("active_days"), 0.0) or 0.0,
                    _safe_float(item.get("pct_chg"), 0.0) or 0.0,
                    _safe_float(item.get("amount"), 0.0) or 0.0,
                ),
                reverse=True,
            )
            sector_rank = next(
                (idx + 1 for idx, item in enumerate(ranked_stocks) if str(item.get("ts_code") or "").upper() == norm_code),
                sector_total,
            )
            matched_stock = next(
                (item for item in sector_stocks if str(item.get("ts_code") or "").upper() == norm_code),
                {},
            )

        related_row = next((row for row in (mainline_rows or []) if str(row[1] or "").strip() == mapped_sector), None)
        sector_score = _safe_float(
            review_item.get("latest_score"),
            _safe_float(related_row[2], None) if related_row else None,
        )
        breakout_score = min(
            95.0,
            max(
                20.0,
                45.0
                + max(_safe_float(matched_stock.get("pct_chg"), 0.0) or 0.0, 0.0) * 4.0
                + max((_safe_float(matched_stock.get("volume_ratio"), 1.0) or 1.0) - 1.0, 0.0) * 18.0,
            ),
        )
        capital_flow_score = min(
            95.0,
            max(
                20.0,
                42.0
                + max(_safe_float(matched_stock.get("flow_positive_streak"), 0.0) or 0.0, 0.0) * 8.0
                + max((_safe_float(matched_stock.get("flow_inflow_ratio"), 0.0) or 0.0) * 200.0, 0.0),
            ),
        )
        signal_candidates = [value for value in [sector_score, breakout_score, capital_flow_score] if value is not None]
        signal_score = round(sum(signal_candidates) / len(signal_candidates), 1) if signal_candidates else None

        return {
            "status": "success",
            "mapped_sector": mapped_sector,
            "is_mainline": mapped_sector in {str(row[1] or "").strip() for row in (mainline_rows or [])},
            "sector_rank": sector_rank,
            "sector_total": sector_total,
            "analysis": {
                "factor_scores": {},
                "sector_resonance": {"score": round(sector_score, 1)} if sector_score is not None else {},
                "breakout": {"score": round(breakout_score, 1)},
                "capital_flow": {"score": round(capital_flow_score, 1)},
                "risk_reward": {},
            },
            "signal": {"score": signal_score} if signal_score is not None else {},
        }
    except Exception as exc:
        logger.warning("主线分析 fallback 失败: %s", exc)
        return None


def _build_commentary_snapshot(df: pd.DataFrame) -> str:
    if df.empty:
        return "暂无额外客观数据。"

    last = df.iloc[-1]
    parts = []

    turnover_rate = _safe_float(last.get("turnover_rate"), None)
    volume_ratio = _safe_float(last.get("volume_ratio"), None)
    net_mf_ratio = _safe_float(last.get("net_mf_ratio"), None)
    big_order_ratio = _safe_float(last.get("big_order_ratio"), None)
    flow_parts = []
    if turnover_rate is not None:
        flow_parts.append(f"换手率 {turnover_rate:.2f}%")
    if volume_ratio is not None:
        flow_parts.append(f"量比 {volume_ratio:.2f}")
    if net_mf_ratio is not None:
        flow_parts.append(f"主力净额占比 {_fmt_pct(net_mf_ratio)}")
    if big_order_ratio is not None:
        flow_parts.append(f"大单占比 {_fmt_pct(big_order_ratio)}")
    if flow_parts:
        parts.append(f"- 交易活跃度字段：{'，'.join(flow_parts)}。")

    return "\n".join(parts) if parts else "暂无额外客观数据。"


def _strip_forbidden_template_sections(content: str) -> str:
    sanitized = content
    for title in FORBIDDEN_TEMPLATE_SECTION_TITLES:
        sanitized = re.sub(
            rf"(?ms)^###\s*{re.escape(title)}\s*\n.*?(?=^###\s|\Z)",
            "",
            sanitized,
        )

    kept_lines = []
    for line in sanitized.splitlines():
        if any(token in line for token in FORBIDDEN_TEMPLATE_PLACEHOLDERS):
            continue
        kept_lines.append(line)

    sanitized = "\n".join(kept_lines)
    sanitized = re.sub(r"\n{3,}", "\n\n", sanitized)
    return sanitized.strip()


def _parse_ai_response_json(response: httpx.Response, model_provider: str, model: str) -> dict[str, Any]:
    try:
        result = response.json()
    except Exception as exc:
        content_type = str(response.headers.get("content-type") or "unknown")
        snippet = str(getattr(response, "text", "") or "").strip().replace("\n", " ")[:200]
        logger.error(
            "AI服务返回非JSON: provider=%s model=%s content_type=%s body=%s",
            model_provider,
            model,
            content_type,
            snippet or "<empty>",
        )
        raise HTTPException(
            status_code=502,
            detail=(
                f"AI服务返回了非 JSON 响应（provider={model_provider}, "
                f"model={model}, content_type={content_type}）"
            ),
        ) from exc

    if not isinstance(result, dict):
        logger.error(
            "AI服务返回JSON根节点异常: provider=%s model=%s type=%s",
            model_provider,
            model,
            type(result).__name__,
        )
        raise HTTPException(status_code=502, detail="AI服务返回格式异常")
    return result


def _extract_ai_analysis_text(result: dict[str, Any], model_provider: str) -> str:
    try:
        if model_provider == "gemini":
            analysis = result["candidates"][0]["content"]["parts"][0]["text"]
        else:
            analysis = result["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        logger.error(
            "AI服务返回格式异常: provider=%s keys=%s",
            model_provider,
            list(result.keys())[:10],
        )
        raise HTTPException(status_code=502, detail="AI服务返回格式异常") from exc

    analysis_text = str(analysis or "").strip()
    if not analysis_text:
        raise HTTPException(status_code=502, detail="AI服务返回空结果")
    return analysis_text


def _sanitize_template_content(content: Optional[str]) -> Optional[str]:
    if not content:
        return content

    sanitized = str(content)
    sanitized = sanitized.replace(
        "你是A股量化分析师，请对以下股票进行全面深入的分析。",
        "请基于以下资料做交易判断。"
    )
    sanitized = sanitized.replace(
        "你是一个专业的A股交易分析师。请根据以下信息对股票进行分析并给出投资建议。",
        "请基于以下资料做交易判断。"
    )
    for marker in TEMPLATE_CUTOFF_MARKERS:
        if marker in sanitized:
            sanitized = sanitized.split(marker, 1)[0].rstrip()
            break

    lines = []
    for line in sanitized.splitlines():
        if any(keyword in line for keyword in TEMPLATE_REMOVAL_KEYWORDS):
            continue
        lines.append(line)

    sanitized = "\n".join(lines).strip()
    sanitized = _strip_forbidden_template_sections(sanitized)
    sanitized = _ensure_template_blocks(sanitized)
    return f"{sanitized}{TEMPLATE_EXECUTION_SUFFIX}" if sanitized else DEFAULT_ANALYSIS_USER_PROMPT


def _ensure_template_blocks(content: str) -> str:
    if not content:
        return content

    existing_keys = {placeholder for placeholder in TEMPLATE_EQUIVALENT_KEYS if f"{{{placeholder}}}" in content}
    for canonical_key, aliases in TEMPLATE_EQUIVALENT_KEYS.items():
        if any(f"{{{alias}}}" in content for alias in aliases):
            existing_keys.add(canonical_key)

    missing_blocks = []
    for title, canonical_key in MANDATORY_TEMPLATE_BLOCKS:
        if canonical_key in existing_keys:
            continue
        missing_blocks.append(f"### {title}\n{{{canonical_key}}}")

    if not missing_blocks:
        return content
    return f"{content.rstrip()}\n\n" + "\n\n".join(missing_blocks)

@router.get("/users/me/ai-config")
async def get_my_ai_config(request: Request):
    """获取当前用户的AI配置"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            return _load_user_ai_config_bundle(con, user_id)
    except Exception as e:
        logger.error(f"获取AI配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/ai-config")
async def update_my_ai_config(request: Request, config: UserAIConfigUpdate):
    """更新当前用户的AI配置"""
    user_id = await get_current_user_id(request)
    try:
        payload = _model_payload(config)
        if not payload:
            return {"message": "没有需要更新的字段"}

        provider = str(payload.get("model_provider") or "openai")
        with get_db_connection() as con:
            _upsert_provider_ai_config(con, user_id, provider, payload)

        return {"message": "AI配置已更新"}
    except Exception as e:
        logger.error(f"更新AI配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users/me/prompt-templates")
async def get_prompt_templates(request: Request):
    """获取当前用户的提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            rows = con.execute(
                "SELECT id, name, content, is_default, created_at, updated_at FROM user_prompt_templates WHERE user_id = ? ORDER BY is_default DESC, created_at DESC",
                (user_id,)
            ).fetchall()
        return [{"id": r[0], "name": r[1], "content": r[2], "is_default": r[3], "created_at": r[4], "updated_at": r[5]} for r in rows]
    except Exception as e:
        logger.error(f"获取提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/users/me/prompt-presets")
async def get_prompt_presets(request: Request):
    """获取系统内置提示词预设"""
    await get_current_user_id(request)
    return PROMPT_PRESETS

@router.post("/users/me/prompt-templates")
async def create_prompt_template(request: Request, template: PromptTemplateCreate):
    """创建提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            if template.is_default:
                con.execute("UPDATE user_prompt_templates SET is_default = FALSE WHERE user_id = ?", (user_id,))
            max_id = con.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM user_prompt_templates").fetchone()[0]
            con.execute(
                "INSERT INTO user_prompt_templates (id, user_id, name, content, is_default) VALUES (?, ?, ?, ?, ?)",
                (max_id, user_id, template.name, template.content, template.is_default)
            )
        return {"message": "模板创建成功"}
    except Exception as e:
        logger.error(f"创建提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/prompt-templates/{template_id}")
async def update_prompt_template(request: Request, template_id: int, template: PromptTemplateUpdate):
    """更新提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_prompt_templates WHERE id = ? AND user_id = ?", (template_id, user_id)).fetchone()
            if not exists:
                raise HTTPException(status_code=404, detail="模板不存在")
            
            if template.is_default:
                con.execute("UPDATE user_prompt_templates SET is_default = FALSE WHERE user_id = ?", (user_id,))
            
            updates = []
            params = []
            if template.name is not None:
                updates.append("name = ?")
                params.append(template.name)
            if template.content is not None:
                updates.append("content = ?")
                params.append(template.content)
            if template.is_default is not None:
                updates.append("is_default = ?")
                params.append(template.is_default)
            
            if updates:
                updates.append("updated_at = CURRENT_TIMESTAMP")
                params.append(template_id)
                con.execute(f"UPDATE user_prompt_templates SET {', '.join(updates)} WHERE id = ?", params)
        return {"message": "模板更新成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/users/me/prompt-templates/{template_id}")
async def delete_prompt_template(request: Request, template_id: int):
    """删除提示词模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            con.execute("DELETE FROM user_prompt_templates WHERE id = ? AND user_id = ?", (template_id, user_id))
        return {"message": "模板已删除"}
    except Exception as e:
        logger.error(f"删除提示词模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/users/me/selected-template")
async def get_selected_template(request: Request):
    """获取当前选中的模板ID"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            row = con.execute(
                "SELECT selected_template_id FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        return {"selected_template_id": row[0] if row else None}
    except Exception as e:
        logger.error(f"获取选中模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/selected-template")
async def select_template(request: Request, body: SelectTemplateRequest):
    """设置选中的模板"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_ai_config WHERE user_id = ?", (user_id,)).fetchone()
            if exists:
                con.execute("UPDATE user_ai_config SET selected_template_id = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?", 
                    (body.template_id, user_id))
            else:
                con.execute("INSERT INTO user_ai_config (user_id, selected_template_id) VALUES (?, ?)", 
                    (user_id, body.template_id))
        return {"message": "模板已选中"}
    except Exception as e:
        logger.error(f"选择模板失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/stock/analyze")
async def analyze_stock_with_ai(request: Request, body: AIAnalyzeRequest):
    """使用AI分析股票"""
    user_id = await get_current_user_id(request)
    try:
        prices_df = fetch_df(
            """
            SELECT
                d.trade_date,
                d.open,
                d.high,
                d.low,
                d.close,
                d.vol,
                d.amount,
                d.pct_chg,
                COALESCE(m.net_mf_amount, f.net_mf_amount) AS net_mf_amount,
                COALESCE(m.net_mf_ratio, f.net_mf_ratio) AS net_mf_ratio,
                f.ma5,
                f.ma10,
                f.ma20,
                f.ma60,
                f.vol_ma5,
                f.turnover_rate,
                f.volume_ratio,
                f.big_order_ratio,
                f.rps_20,
                f.rps_50,
                f.rps_120,
                f.trend_score,
                f.quality_score,
                f.flow_score,
                f.value_score,
                f.event_score,
                f.factor_score
            FROM daily_price d
            LEFT JOIN stock_moneyflow m
              ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
            LEFT JOIN stock_factor_daily f
              ON d.ts_code = f.ts_code AND d.trade_date = f.trade_date
            WHERE d.ts_code = ?
            ORDER BY d.trade_date DESC
            LIMIT 60
            """,
            (body.ts_code,),
        )
        if prices_df.empty:
            raise HTTPException(status_code=400, detail=f"未找到股票 {body.ts_code} 的行情数据")

        latest_trade_date = str(prices_df.iloc[0]["trade_date"])
        analysis_df = _prepare_analysis_df(prices_df)
        
        # 检查缓存（如果不是强制刷新）
        if not body.force_refresh:
            with get_db_connection() as con:
                cache = con.execute(
                    "SELECT analysis_result, created_at FROM ai_analysis_cache WHERE user_id = ? AND ts_code = ? AND trade_date = ? ORDER BY created_at DESC LIMIT 1",
                    (user_id, body.ts_code, latest_trade_date)
                ).fetchone()
            
            if cache:
                logger.info(f"返回缓存的分析结果: {body.ts_code} {latest_trade_date}")
                return {
                    "analysis": cache[0],
                    "ts_code": body.ts_code,
                    "trade_date": latest_trade_date,
                    "from_cache": True
                }
        
        # 获取用户AI配置
        with get_db_connection() as con:
            config = con.execute(
                "SELECT model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        
        if not config or not config[2]:
            raise HTTPException(status_code=400, detail="请先在设置中配置API Key")
        
        model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature = config
        
        # 获取模板
        template_id = body.template_id
        if not template_id:
            with get_db_connection() as con:
                tpl = con.execute(
                    "SELECT content FROM user_prompt_templates WHERE user_id = ? AND is_default = TRUE",
                    (user_id,)
                ).fetchone()
                template_content = tpl[0] if tpl else None
        else:
            with get_db_connection() as con:
                tpl = con.execute(
                    "SELECT content FROM user_prompt_templates WHERE id = ? AND user_id = ?",
                    (template_id, user_id)
                ).fetchone()
                template_content = tpl[0] if tpl else None
        
        # 获取股票基本信息
        stock_basic = None
        with get_db_connection() as con:
            basic = con.execute(
                "SELECT ts_code, name, industry, market FROM stock_basic WHERE ts_code = ?",
                (body.ts_code,)
            ).fetchone()
            if basic:
                stock_basic = {"ts_code": basic[0], "name": basic[1], "industry": basic[2], "market": basic[3]}

        money_flow_df = fetch_df(
            """
            SELECT trade_date, net_mf_amount, net_mf_ratio
            FROM stock_moneyflow
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT 10
            """,
            (body.ts_code,),
        )

        margin_df = fetch_df(
            """
            SELECT trade_date, rzye
            FROM stock_margin
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT 10
            """,
            (body.ts_code,),
        )
        
        # 获取持仓信息
        holding_row = None
        with get_db_connection() as con:
            h = con.execute(
                "SELECT shares, avg_cost FROM user_holdings WHERE user_id = ? AND ts_code = ?",
                (user_id, body.ts_code)
            ).fetchone()
            if h:
                holding_row = h
        # 判断是否在开盘时间段
        from etl.calendar import trading_calendar
        is_trading_time = trading_calendar.is_trading_time()
        
        # 获取实时行情数据（如果是开盘时间段）
        realtime_data = None
        if is_trading_time:
            try:
                from etl.sync import sync_engine
                rt_df = sync_engine.provider.realtime_quote(ts_code=body.ts_code, src="sina")
                if rt_df is not None and not rt_df.empty:
                    rt = rt_df.iloc[0]
                    current_price = rt.get('price', 0)
                    current_vol = rt.get('vol', 0)
                    current_amount = rt.get('amount', 0)
                    current_pct_chg = rt.get('pct_chg', 0)
                    realtime_data = (
                        f"- 盘中实时：最新价 {_fmt_price(current_price)}，涨跌 {_fmt_pct(current_pct_chg)}，"
                        f"成交量 {_fmt_wan(current_vol)}，成交额 {_fmt_wan(current_amount)}。"
                    )
                    logger.info(f"获取实时行情成功: {body.ts_code} price={current_price}")
            except Exception as e:
                logger.warning(f"获取实时行情失败: {e}")

        price_snapshot, price_metrics = _build_price_snapshot(analysis_df)
        money_flow_snapshot = _build_money_flow_snapshot(money_flow_df)
        margin_snapshot = _build_margin_snapshot(margin_df)
        holding_snapshot = _build_holding_snapshot(holding_row, price_metrics.get("close"))
        commentary_snapshot = _build_commentary_snapshot(analysis_df)

        # 格式化数据（用于日志）
        stock_name = stock_basic.get("name", "") if stock_basic else ""
        ts_code = stock_basic.get("ts_code", body.ts_code) if stock_basic else body.ts_code
        industry = stock_basic.get("industry", "") if stock_basic else ""
        market = stock_basic.get("market", "") if stock_basic else ""

        stock_basic_text = (
            f"{stock_name}({ts_code}) | 行业:{industry or '暂无'} | 市场:{market or '暂无'}"
        )
        stock_snapshot_parts = [f"- 标的：{stock_basic_text}。", price_snapshot]
        if realtime_data:
            stock_snapshot_parts.append(realtime_data)
        stock_snapshot = "\n".join(part for part in stock_snapshot_parts if part)
        capital_flow_snapshot = "\n".join(part for part in [money_flow_snapshot, margin_snapshot] if part)
        analysis_snapshot = "\n".join(
            [
                stock_snapshot,
                capital_flow_snapshot,
                f"- 持仓：{holding_snapshot}",
                commentary_snapshot,
            ]
        )

        # 构建提示词
        replacements = {
            "stock_name": stock_name or body.ts_code,
            "ts_code": ts_code,
            "industry": industry or "暂无",
            "market": market or "暂无",
            "stock_basic": stock_basic_text,
            "realtime_section": realtime_data or "",
            "price_data": price_snapshot,
            "money_flow": money_flow_snapshot,
            "margin_data": margin_snapshot,
            "holding": holding_snapshot,
            "market_sentiment": "",
            "mainline": "",
            "stock_snapshot": stock_snapshot,
            "capital_flow_snapshot": capital_flow_snapshot,
            "sector_context": "",
            "theme_context": "",
            "market_context": "",
            "holding_context": holding_snapshot,
            "commentary_snapshot": commentary_snapshot,
            "analysis_snapshot": analysis_snapshot,
            "related_section": "",
        }
        if template_content:
            prompt = _sanitize_template_content(template_content)
            for key, value in replacements.items():
                replacement = value or "暂无"
                if key in {
                    "realtime_section",
                    "related_section",
                    "market_sentiment",
                    "mainline",
                    "sector_context",
                    "theme_context",
                    "market_context",
                } and not value:
                    replacement = ""
                prompt = prompt.replace(f"{{{key}}}", replacement)
        else:
            prompt = DEFAULT_ANALYSIS_USER_PROMPT
            for key, value in replacements.items():
                replacement = value or "暂无"
                if key in {
                    "realtime_section",
                    "related_section",
                    "market_sentiment",
                    "mainline",
                    "sector_context",
                    "theme_context",
                    "market_context",
                } and not value:
                    replacement = ""
                prompt = prompt.replace(f"{{{key}}}", replacement)
        
        # 记录数据状态以便调试
        logger.info(f"AI分析数据准备: stock_basic={'有' if stock_basic else '无'}, "
                    f"price_rows={len(analysis_df)}, "
                    f"money_flow_rows={len(money_flow_df)}, "
                    f"margin_rows={len(margin_df)}, "
                    f"holding={'有' if holding_row else '无'}, "
                    f"is_trading_time={is_trading_time}, "
                    f"realtime_data={'有' if realtime_data else '无'}")
        
        # 记录提示词长度和部分内容
        logger.info(f"AI分析提示词长度: {len(prompt)}")
        if len(prompt) > 1000:
            logger.info(f"AI分析提示词前1000字符: {prompt[:1000]}")
            logger.debug(f"AI分析提示词完整内容: {prompt}")
        else:
            logger.info(f"AI分析提示词: {prompt}")
        
        # 调用AI
        model = model_name or "deepseek-chat"
        response_max_tokens = max(300, min(int(max_tokens or 1200), 1200))
        effective_temperature = float(temperature) if temperature is not None else 0.35
        effective_system_prompt = BASE_ANALYSIS_SYSTEM_PROMPT
        if system_prompt:
            effective_system_prompt = f"{effective_system_prompt}\n\n用户补充要求：\n{system_prompt}"
        logger.info(f"AI provider: {model_provider}, model: {model}")
        if model_provider == "deepseek":
            if not base_url:
                base_url = "https://api.deepseek.com/v1"
            base_url = base_url.rstrip('/')
            url = f"{base_url}/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": effective_system_prompt},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": response_max_tokens,
                "temperature": effective_temperature
            }
        elif model_provider == "gemini":
            gemini_model = model or "gemini-2.5-flash"
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent"
            headers = {"Content-Type": "application/json", "x-goog-api-key": api_key}
            payload = {
                "contents": [
                    {"role": "user", "parts": [{"text": f"{effective_system_prompt}\n\n{prompt}"}]}
                ],
                "generationConfig": {
                    "maxOutputTokens": response_max_tokens,
                    "temperature": effective_temperature
                }
            }
        else:  # openai
            if not base_url:
                base_url = "https://api.openai.com/v1"
            base_url = base_url.rstrip('/')
            url = f"{base_url}/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            payload = {
                "model": model or "gpt-4",
                "messages": [
                    {"role": "system", "content": effective_system_prompt},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": response_max_tokens,
                "temperature": effective_temperature
            }
        
        logger.info(f"AI分析请求: {body.ts_code}, 模型: {model}, 交易日: {latest_trade_date}")
        
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code != 200:
                error_detail = resp.text
                logger.error(f"AI API error: {error_detail}")
                raise HTTPException(status_code=502, detail=f"AI服务调用失败: {error_detail}")
            result = _parse_ai_response_json(resp, model_provider=model_provider, model=model)
            analysis = _extract_ai_analysis_text(result, model_provider=model_provider)
        
        # 保存到缓存
        with get_db_connection() as con:
            # 先删除旧缓存
            con.execute(
                "DELETE FROM ai_analysis_cache WHERE user_id = ? AND ts_code = ? AND trade_date = ?",
                (user_id, body.ts_code, latest_trade_date)
            )
            # 生成新id
            max_id = con.execute("SELECT COALESCE(MAX(id), 0) FROM ai_analysis_cache").fetchone()[0]
            new_id = max_id + 1
            # 插入新缓存
            con.execute(
                "INSERT INTO ai_analysis_cache (id, user_id, ts_code, trade_date, analysis_result, model_name) VALUES (?, ?, ?, ?, ?, ?)",
                (new_id, user_id, body.ts_code, latest_trade_date, analysis, model)
            )
        
        logger.info(f"AI分析完成并缓存: {body.ts_code} {latest_trade_date}")
        
        return {
            "analysis": analysis,
            "ts_code": body.ts_code,
            "trade_date": latest_trade_date,
            "from_cache": False
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        logger.error(f"AI分析失败: {e}")
        logger.error(f"AI分析失败详情: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=str(e))

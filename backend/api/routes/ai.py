# /backend/api/routes/ai.py

import json
import logging
import math
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
    "你是A股交易分析助手。下面提供的是数据库中的客观行情、资金、板块、持仓与因子数据摘要，"
    "不代表程序已经给出交易结论。你的任务是基于这些事实自行完成分析，"
    "输出简洁、可执行、面向交易的判断。"
    "不要把输入中的标签、评分、字段名或程序映射结果直接机械改写成结论，"
    "要先基于事实再做推断；若证据不足，明确写“等待确认”。"
    "禁止横向推荐其他股票，禁止空喊看好，禁止把程序中间结论当成最终答案。"
)

DEFAULT_ANALYSIS_USER_PROMPT = """以下资料尽量只包含客观数据与字段摘要，不包含程序化买卖结论。请基于这些事实自行输出聚焦交易决策的 Markdown，总字数控制在 320-520 字。

输出格式固定为：
## 交易结论
- 观点：偏多 / 中性 / 偏空
- 建议：低吸试错 / 回踩接回 / 持有 / 冲高减仓 / 观望 / 回避
- 时效：盘中 / 1-3日 / 1-2周
- 置信度：高 / 中 / 低

## 核心依据
- 最多 3 条，只写最关键证据，优先从趋势、量价、资金、主线、市场中挑最有用的

## 买卖点
- 买点：
- 卖点：
- 追价条件：
- 止损 / 失效：
- 仓位建议：

## 持仓应对
- 空仓：
- 持仓：

## 相关主线与市场
- 最相关主线：
- 市场配合：
- 后续观察：

## 风险
- 最多 2 条，直接写风险触发点

要求：
1. 结论先行，但所有判断都必须基于下面给出的客观数据，不要把任何标签或字段名直接当结论复述。
2. 优先从价格位置、成交量、资金流、主线强弱、市场环境、持仓成本这些事实推导交易计划。
3. 如果关键价格位、趋势延续、量能确认不足，不要勉强下结论，直接写“等待确认”。
4. 不做横向推荐或题材扩写，不写成长篇研报，不复述无关常识。
5. 可以做推断，但要尽量让推断与下方事实一一对应。

### 标的概览
{stock_snapshot}

### 资金与杠杆
{capital_flow_snapshot}

### 相关主线
{sector_context}

### 市场环境
{market_context}

### 持仓信息
{holding_context}

### 客观补充数据
{commentary_snapshot}
"""

TEMPLATE_CUTOFF_MARKERS = ("请分析并给出：", "请给出详细分析：")
TEMPLATE_REMOVAL_KEYWORDS = ("关联个股", "同行业", "技术壁垒", "业绩预期向上")
TEMPLATE_EXECUTION_SUFFIX = """

---
请将上面资料压缩为交易结论，并严格遵守：
1. 只输出 `交易结论`、`核心依据`、`买卖点`、`持仓应对`、`相关主线与市场`、`风险` 六部分；
2. 结论先行，总字数控制在 320-520 字；
3. 不做横向推荐或题材联想，不展开成长篇研报；
4. 所有判断必须基于提供的客观数据，不要把程序字段或标签直接翻译成结论；
5. 买卖点、追价条件、止损/失效和仓位建议要尽量明确；数据不足时明确写“等待确认”。
"""
TEMPLATE_EQUIVALENT_KEYS = {
    "stock_snapshot": {"stock_snapshot", "stock_basic", "price_data"},
    "capital_flow_snapshot": {"capital_flow_snapshot", "money_flow", "margin_data"},
    "sector_context": {"sector_context", "theme_context"},
    "market_context": {"market_context", "market_sentiment", "mainline"},
    "holding_context": {"holding_context", "holding"},
    "commentary_snapshot": {"commentary_snapshot"},
}
MANDATORY_TEMPLATE_BLOCKS = [
    ("标的概览", "stock_snapshot"),
    ("资金与杠杆", "capital_flow_snapshot"),
    ("相关主线", "sector_context"),
    ("市场环境", "market_context"),
    ("持仓信息", "holding_context"),
    ("客观补充数据", "commentary_snapshot"),
]

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


def _fmt_yi(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number / 100000000:.2f}亿"


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
    for ma in (5, 10, 20, 60):
        col = f"ma{ma}"
        if col not in work.columns:
            work[col] = work["close"].rolling(ma, min_periods=1).mean()
    if "volume_ratio" not in work.columns and "vol_ma5" in work.columns:
        base = pd.to_numeric(work["vol_ma5"], errors="coerce")
        work["volume_ratio"] = (work["volume"] / base).where(base > 0)
    return work


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
            f"- 成交数据：成交量 {volume:.0f}，成交额 {_fmt_yi(amount)}，"
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

    rps_parts = []
    for key in ("rps_20", "rps_50", "rps_120"):
        value = _safe_float(last.get(key), None)
        if value is not None:
            rps_parts.append(f"{key.upper()} {value:.1f}")
    if rps_parts:
        parts.append(f"- 强度字段：{'，'.join(rps_parts)}。")

    factor_parts = []
    for key, label in (
        ("trend_score", "trend_score"),
        ("quality_score", "quality_score"),
        ("flow_score", "flow_score"),
        ("value_score", "value_score"),
        ("event_score", "event_score"),
        ("factor_score", "factor_score"),
    ):
        value = _safe_float(last.get(key), None)
        if value is not None:
            factor_parts.append(f"{label} {value:.1f}")
    if factor_parts:
        parts.append(f"- 因子字段：{'，'.join(factor_parts)}。")

    return "\n".join(parts) if parts else "暂无额外客观数据。"


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

        market_sentiment_row = None
        with get_db_connection() as con:
            market_sentiment_row = con.execute(
                """
                SELECT trade_date, score, label, details
                FROM market_sentiment
                ORDER BY trade_date DESC
                LIMIT 1
                """
            ).fetchone()

        with get_db_connection() as con:
            latest_mainline_date = con.execute(
                "SELECT MAX(trade_date) FROM mainline_scores"
            ).fetchone()
            if latest_mainline_date and latest_mainline_date[0]:
                mainline_rows = con.execute(
                    """
                    SELECT trade_date, mapped_name, score
                    FROM mainline_scores
                    WHERE trade_date = ?
                    ORDER BY score DESC
                    LIMIT 3
                    """,
                    (latest_mainline_date[0],),
                ).fetchall()
            else:
                mainline_rows = []

        from strategy.mainline.analyst import mainline_analyst
        from .stocks import get_market_environment, get_stock_mainline_analysis

        mainline_history = mainline_analyst.get_history(days=10) or {}
        review_mainlines = (
            (((mainline_history.get("analysis") or {}).get("review_10d") or {}).get("mainlines") or [])
        )
        review_map = {item.get("name", ""): item for item in review_mainlines if item.get("name")}
        top_mainline = ((mainline_history.get("analysis") or {}).get("top_mainline") or {})
        market_env = get_market_environment(str(latest_trade_date)[:10])
        mainline_analysis = None
        try:
            mainline_analysis = get_stock_mainline_analysis(body.ts_code)
        except HTTPException as exc:
            logger.warning("个股主线分析失败，使用 fallback: %s", exc.detail)
        except Exception as exc:
            logger.warning("个股主线分析异常，使用 fallback: %s", exc)
        if not mainline_analysis or mainline_analysis.get("status") != "success":
            mainline_analysis = _build_mainline_analysis_fallback(
                ts_code=body.ts_code,
                trade_date=str(latest_trade_date)[:10],
                review_map=review_map,
                mainline_rows=mainline_rows,
            )

        price_snapshot, price_metrics = _build_price_snapshot(analysis_df)
        money_flow_snapshot = _build_money_flow_snapshot(money_flow_df)
        margin_snapshot = _build_margin_snapshot(margin_df)
        holding_snapshot = _build_holding_snapshot(holding_row, price_metrics.get("close"))
        market_sentiment_snapshot, mainline_snapshot, market_context = _build_market_snapshot(
            market_sentiment_row,
            mainline_rows,
            market_env=market_env,
            top_mainline=top_mainline,
        )
        sector_context = _build_sector_snapshot(
            mainline_analysis=mainline_analysis,
            review_map=review_map,
            market_env=market_env,
            top_mainline=top_mainline,
        )
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
        capital_flow_snapshot = "\n".join([money_flow_snapshot, margin_snapshot])
        analysis_snapshot = "\n".join(
            [
                stock_snapshot,
                capital_flow_snapshot,
                sector_context,
                market_context,
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
            "market_sentiment": market_sentiment_snapshot,
            "mainline": mainline_snapshot,
            "stock_snapshot": stock_snapshot,
            "capital_flow_snapshot": capital_flow_snapshot,
            "sector_context": sector_context,
            "theme_context": sector_context,
            "market_context": market_context,
            "holding_context": holding_snapshot,
            "commentary_snapshot": commentary_snapshot,
            "analysis_snapshot": analysis_snapshot,
            "related_section": "",
        }
        if template_content:
            prompt = _sanitize_template_content(template_content)
            for key, value in replacements.items():
                replacement = value or "暂无"
                if key in {"realtime_section", "related_section"} and not value:
                    replacement = ""
                prompt = prompt.replace(f"{{{key}}}", replacement)
        else:
            prompt = DEFAULT_ANALYSIS_USER_PROMPT
            for key, value in replacements.items():
                replacement = value or "暂无"
                if key in {"realtime_section", "related_section"} and not value:
                    replacement = ""
                prompt = prompt.replace(f"{{{key}}}", replacement)
        
        # 记录数据状态以便调试
        logger.info(f"AI分析数据准备: stock_basic={'有' if stock_basic else '无'}, "
                    f"price_rows={len(analysis_df)}, "
                    f"money_flow_rows={len(money_flow_df)}, "
                    f"margin_rows={len(margin_df)}, "
                    f"holding={'有' if holding_row else '无'}, "
                    f"is_trading_time={is_trading_time}, "
                    f"realtime_data={'有' if realtime_data else '无'}, "
                    f"sector_context={'有' if sector_context else '无'}")
        
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
            result = resp.json()
            if model_provider == "gemini":
                analysis = result["candidates"][0]["content"]["parts"][0]["text"]
            else:
                analysis = result["choices"][0]["message"]["content"]
        
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

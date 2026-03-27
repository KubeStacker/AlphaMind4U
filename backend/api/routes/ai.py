# /backend/api/routes/ai.py

import logging
import math
from typing import Optional

import httpx
import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from db.connection import get_db_connection, fetch_df
from .users import get_current_user_id

logger = logging.getLogger(__name__)
router = APIRouter(tags=["AI"])

BASE_ANALYSIS_SYSTEM_PROMPT = (
    "你是A股交易分析助手，偏短中线执行，不写成研究报告。"
    "回答必须结论先行、短、准、可执行，优先给方向、关键价位、触发条件、失效条件和仓位建议。"
    "禁止做横向推荐、题材联想或泛泛而谈。"
)

DEFAULT_ANALYSIS_USER_PROMPT = """请只基于以下资料输出简洁 Markdown，总字数控制在 260-420 字。

输出格式固定为：
## 结论
- 观点：偏多 / 中性 / 偏空
- 建议：买入试错 / 持有 / 观望 / 减仓 / 回避
- 时效：1-3日 或 1-2周
- 置信度：高 / 中 / 低

## 核心依据
- 最多 3 条，只写最关键证据

## 关键价位与动作
- 支撑位：
- 压力位：
- 触发条件：
- 失效条件：

## 持仓应对
- 空仓：
- 持仓：

## 风险
- 最多 2 条，直接写风险触发点

要求：
1. 结论先行，不复述大段原始数据。
2. 不做横向推荐或题材联想。
3. 只有在数据支持时才给出价位和动作；不确定就写“等待确认”。
4. 避免空话、套话和长免责声明。

### 标的概览
{stock_snapshot}

### 资金与杠杆
{capital_flow_snapshot}

### 市场环境
{market_context}

### 持仓信息
{holding_context}

### 量化点评
{commentary_snapshot}
"""

TEMPLATE_CUTOFF_MARKERS = ("请分析并给出：", "请给出详细分析：")
TEMPLATE_REMOVAL_KEYWORDS = ("关联个股", "同行业", "技术壁垒", "业绩预期向上")
TEMPLATE_EXECUTION_SUFFIX = """

---
请将上面资料压缩为短线交易结论，并严格遵守：
1. 只输出 `结论`、`核心依据`、`关键价位与动作`、`持仓应对`、`风险` 五部分；
2. 结论先行，总字数控制在 260-420 字；
3. 不做横向推荐或题材联想，不展开成长篇研报；
4. 关键价位、触发条件、失效条件要尽量明确；数据不足时明确写“等待确认”。
"""

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


def _fmt_wan(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number / 10000:.2f}万"


def _fmt_yi(value: Optional[float]) -> str:
    number = _safe_float(value, None)
    return "暂无" if number is None else f"{number / 100000000:.2f}亿"


def _prepare_analysis_df(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy().sort_values("trade_date").reset_index(drop=True)
    if "volume" not in work.columns and "vol" in work.columns:
        work = work.rename(columns={"vol": "volume"})
    for ma in (5, 10, 20, 60):
        col = f"ma{ma}"
        if col not in work.columns:
            work[col] = work["close"].rolling(ma, min_periods=1).mean()
    return work


def _describe_trend(last_row: pd.Series) -> str:
    close = _safe_float(last_row.get("close"), 0.0) or 0.0
    ma5 = _safe_float(last_row.get("ma5"), close) or close
    ma10 = _safe_float(last_row.get("ma10"), close) or close
    ma20 = _safe_float(last_row.get("ma20"), close) or close
    ma60 = _safe_float(last_row.get("ma60"), close) or close

    if close > ma5 > ma10 > ma20 > ma60:
        return "多头趋势，短中期共振向上"
    if close > ma5 > ma10 > ma20:
        return "短中期偏强，仍在上行通道"
    if close > ma20 > ma60:
        return "中期转强，但仍需量能确认"
    if close < ma5 < ma10 < ma20 < ma60:
        return "空头趋势，弱势未扭转"
    if close < ma20 < ma60:
        return "中期偏弱，反弹更多看确认"
    return "区间震荡，方向尚未明确"


def _nearest_level(candidates: list[Optional[float]], close: float, direction: str) -> Optional[float]:
    values = []
    for candidate in candidates:
        number = _safe_float(candidate, None)
        if number is None:
            continue
        if direction == "below" and number <= close:
            values.append(number)
        if direction == "above" and number >= close:
            values.append(number)
    if not values:
        return None
    return max(values) if direction == "below" else min(values)


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

    support = _nearest_level([ma5, ma10, ma20, low_5, low_10, low_20], close, "below")
    resistance = _nearest_level([high_5, high_10, high_20, ma60], close, "above")

    lines = [
        f"- 最新收盘 {_fmt_price(close)}，当日 {_fmt_pct(pct_today)}，趋势：{_describe_trend(last)}。",
        f"- 近 5/10/20 日区间收益：{_fmt_pct(ret_5)} / {_fmt_pct(ret_10)} / {_fmt_pct(ret_20)}。",
        f"- 当日成交额 {_fmt_yi(amount)}，量能为 5 日均量 {vol_ratio_5:.2f} 倍、20 日均量 {vol_ratio_20:.2f} 倍。",
        f"- 参考位：支撑 {_fmt_price(support)}，压力 {_fmt_price(resistance)}，MA5/10/20/60 为 {_fmt_price(ma5)} / {_fmt_price(ma10)} / {_fmt_price(ma20)} / {_fmt_price(ma60)}。",
    ]
    return "\n".join(lines), {
        "close": close,
        "support": support,
        "resistance": resistance,
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


def _build_market_snapshot(sentiment_row, mainline_rows) -> tuple[str, str, str]:
    market_sentiment_snapshot = "- 市场情绪：暂无。"
    if sentiment_row:
        market_sentiment_snapshot = (
            f"- 市场情绪：{sentiment_row[2] or '未知'}，分数 {_safe_float(sentiment_row[1], 0.0):.1f}，日期 {str(sentiment_row[0])[:10]}。"
        )

    top_mainlines = []
    for row in mainline_rows or []:
        top_mainlines.append(f"{row[1]}({_safe_float(row[2], 0.0):.1f})")
    mainline_snapshot = f"- 主线热点：{'、'.join(top_mainlines) if top_mainlines else '暂无'}。"

    return market_sentiment_snapshot, mainline_snapshot, "\n".join([market_sentiment_snapshot, mainline_snapshot])


def _build_commentary_snapshot(detail: dict) -> str:
    if not detail:
        return "暂无量化点评。"

    decision = detail.get("decision") or {}
    trade_plan = detail.get("trade_plan") or {}
    levels = detail.get("key_levels") or []
    level_text = "、".join(
        f"{item.get('label')} {_fmt_price(item.get('price'))}" for item in levels[:3] if item.get("price") is not None
    )
    parts = []
    if decision.get("summary"):
        parts.append(f"- 结论：{decision['summary']}")
    if trade_plan.get("entry"):
        parts.append(f"- 执行：{trade_plan['entry']}")
    if trade_plan.get("invalid"):
        parts.append(f"- 失效：{trade_plan['invalid']}")
    if level_text:
        parts.append(f"- 关键位：{level_text}")
    return "\n".join(parts) if parts else (detail.get("summary") or "暂无量化点评。")


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
    return f"{sanitized}{TEMPLATE_EXECUTION_SUFFIX}" if sanitized else DEFAULT_ANALYSIS_USER_PROMPT

@router.get("/users/me/ai-config")
async def get_my_ai_config(request: Request):
    """获取当前用户的AI配置"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            row = con.execute(
                "SELECT model_provider, model_name, api_key, base_url, system_prompt, max_tokens, temperature FROM user_ai_config WHERE user_id = ?",
                (user_id,)
            ).fetchone()
        if row:
            return {
                "model_provider": row[0],
                "model_name": row[1],
                "api_key": row[2],
                "base_url": row[3],
                "system_prompt": row[4],
                "max_tokens": row[5],
                "temperature": row[6]
            }
        return {
            "model_provider": "openai",
            "model_name": None,
            "api_key": None,
            "base_url": None,
            "system_prompt": None,
            "max_tokens": 1200,
            "temperature": 0.35
        }
    except Exception as e:
        logger.error(f"获取AI配置失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/ai-config")
async def update_my_ai_config(request: Request, config: UserAIConfigUpdate):
    """更新当前用户的AI配置"""
    user_id = await get_current_user_id(request)
    try:
        updates = []
        params = []
        
        if config.model_provider is not None:
            updates.append("model_provider = ?")
            params.append(config.model_provider)
        if config.model_name is not None:
            updates.append("model_name = ?")
            params.append(config.model_name)
        if config.api_key is not None:
            updates.append("api_key = ?")
            params.append(config.api_key)
        if config.base_url is not None:
            updates.append("base_url = ?")
            params.append(config.base_url)
        if config.system_prompt is not None:
            updates.append("system_prompt = ?")
            params.append(config.system_prompt)
        if config.max_tokens is not None:
            updates.append("max_tokens = ?")
            params.append(config.max_tokens)
        if config.temperature is not None:
            updates.append("temperature = ?")
            params.append(config.temperature)
        
        if not updates:
            return {"message": "没有需要更新的字段"}
        
        updates.append("updated_at = CURRENT_TIMESTAMP")
        
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_ai_config WHERE user_id = ?", (user_id,)).fetchone()
            if exists:
                sql = f"UPDATE user_ai_config SET {', '.join(updates)} WHERE user_id = ?"
                params.append(user_id)
                con.execute(sql, params)
            else:
                cols = [u.split(' = ')[0] for u in updates if 'CURRENT_TIMESTAMP' not in u]
                vals_placeholders = ', '.join(['?' for _ in cols])
                insert_sql = f"INSERT INTO user_ai_config (user_id, {', '.join(cols)}) VALUES (?, {vals_placeholders})"
                insert_params = [user_id] + params
                con.execute(insert_sql, insert_params)
        
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
            SELECT trade_date, open, high, low, close, vol, amount, pct_chg
            FROM daily_price
            WHERE ts_code = ?
            ORDER BY trade_date DESC
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

        from etl.utils.kline_patterns import PatternRecognizer, get_professional_commentary_detailed

        latest_patterns = PatternRecognizer(analysis_df).recognize()
        commentary_detail = get_professional_commentary_detailed(analysis_df, latest_patterns)
        price_snapshot, price_metrics = _build_price_snapshot(analysis_df)
        money_flow_snapshot = _build_money_flow_snapshot(money_flow_df)
        margin_snapshot = _build_margin_snapshot(margin_df)
        holding_snapshot = _build_holding_snapshot(holding_row, price_metrics.get("close"))
        market_sentiment_snapshot, mainline_snapshot, market_context = _build_market_snapshot(
            market_sentiment_row, mainline_rows
        )
        commentary_snapshot = _build_commentary_snapshot(commentary_detail)

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
                    f"patterns={len(latest_patterns)}")
        
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

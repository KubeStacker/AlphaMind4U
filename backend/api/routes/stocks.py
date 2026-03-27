# /backend/api/routes/stocks.py

import json
import logging
import math
import threading
import time
from collections import OrderedDict
from typing import Any, Dict, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from db.connection import get_db_connection, fetch_df
from etl.calendar import trading_calendar
from etl.sync import sync_engine
from .users import get_current_user_id

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Stocks"])

_ANALYSIS_CACHE_LOCK = threading.Lock()
_ANALYSIS_CACHE: OrderedDict[str, tuple[float, dict[str, Any]]] = OrderedDict()
_ANALYSIS_CACHE_TTL_SECONDS = 900
_ANALYSIS_CACHE_MAX_ENTRIES = 128

# --- 通用工具函数 ---

def _normalize_ts_code(code: str) -> str:
    """标准化股票代码格式"""
    if not code:
        return ""
    code = str(code).upper().strip()
    if "." in code:
        return code
    # 简单启发式补齐
    if code.startswith("6"):
        return f"{code}.SH"
    if code.startswith("0") or code.startswith("3"):
        return f"{code}.SZ"
    if code.startswith("8") or code.startswith("4"):
        return f"{code}.BJ"
    return code

def _safe_float(v, default=None):
    """安全转换为浮点数"""
    try:
        if v is None:
            return default
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except:
        return default

def _compact_watch_analysis(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "summary": payload.get("summary", ""),
        "history": payload.get("history", []),
        "suggestion": payload.get("suggestion", "观望"),
    }

def _empty_watch_analysis(include_detail: bool = False) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "summary": "数据不足",
        "history": [],
        "suggestion": "观望",
    }
    if include_detail:
        payload["detail"] = {}
    return payload

def _prepare_watch_df(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "volume" not in work.columns and "vol" in work.columns:
        work = work.rename(columns={"vol": "volume"})

    for ma in (5, 10, 20, 60):
        col = f"ma{ma}"
        if col not in work.columns:
            work[col] = work["close"].rolling(ma, min_periods=1).mean()

    if "volume" in work.columns:
        work["volume_ma5"] = work["volume"].rolling(5, min_periods=1).mean()
    else:
        work["volume_ma5"] = 0.0
    return work

def _derive_watch_suggestion(row: pd.Series) -> str:
    pct_today = _safe_float(row.get("pct_chg"), 0.0) or 0.0
    close = _safe_float(row.get("close"), 0.0) or 0.0
    ma5 = _safe_float(row.get("ma5"), close) or close
    ma20 = _safe_float(row.get("ma20"), close) or close
    volume = _safe_float(row.get("volume"), 0.0) or 0.0
    volume_ma5 = _safe_float(row.get("volume_ma5"), 0.0) or 0.0
    vol_ratio_5 = (volume / volume_ma5) if volume_ma5 > 0 else 1.0

    if pct_today >= 5 or (close > ma5 > ma20 and vol_ratio_5 >= 1.2):
        return "关注"
    if pct_today > 0 or close >= ma5 >= ma20 * 0.98:
        return "试错"
    if pct_today <= -5 or close < ma5 < ma20:
        return "减仓"
    return "观望"

def _derive_watch_tone(row: pd.Series) -> str:
    pct_today = _safe_float(row.get("pct_chg"), 0.0) or 0.0
    close = _safe_float(row.get("close"), 0.0) or 0.0
    ma5 = _safe_float(row.get("ma5"), close) or close
    ma20 = _safe_float(row.get("ma20"), close) or close

    if pct_today >= 7:
        return "爆发"
    if pct_today <= -7:
        return "杀跌"
    if close > ma5 > ma20:
        return "看多(强)"
    if close > ma20:
        return "看多"
    if close < ma5 < ma20:
        return "看空"
    return "中性"

def _build_watch_history(df: pd.DataFrame, lookback: int = 10) -> list[dict[str, Any]]:
    if df.empty:
        return []

    history = []
    for _, row in df.tail(lookback).iterrows():
        history.append({
            "date": str(row.get("trade_date", ""))[:10],
            "suggestion": _derive_watch_suggestion(row),
            "tone": _derive_watch_tone(row),
            "patterns": [],
        })
    return history

def _build_watch_analysis(ts_code: str) -> Dict[str, Any]:
    """为自选股生成结构化分析结果。"""
    try:
        df = fetch_df(
            """
            SELECT trade_date, open, high, low, close, vol, amount, pct_chg
            FROM daily_price
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT 75
            """,
            (ts_code,),
        )
        if df.empty or len(df) < 20:
            return _empty_watch_analysis(include_detail=True)

        df = _prepare_watch_df(df.iloc[::-1].reset_index(drop=True))

        from etl.utils.kline_patterns import PatternRecognizer, get_professional_commentary_detailed

        latest_recognizer = PatternRecognizer(df)
        latest_patterns = latest_recognizer.recognize()
        latest_detail = get_professional_commentary_detailed(df, latest_patterns)

        history = _build_watch_history(df)
        latest_row = df.iloc[-1]
        decision = latest_detail.get("decision") or {}
        suggestion = decision.get("action") or (
            history[-1]["suggestion"] if history else _derive_watch_suggestion(latest_row)
        )

        return {
            "summary": latest_detail.get("summary", ""),
            "history": history,
            "suggestion": suggestion,
            "detail": latest_detail,
        }
    except Exception as e:
        logger.warning(f"分析股票 {ts_code} 失败: {e}", exc_info=True)
        return {
            "summary": "分析失败",
            "history": [],
            "suggestion": "观望",
            "detail": {},
        }

def _get_watch_analysis(ts_code: str, force_refresh: bool = False) -> Dict[str, Any]:
    now = time.time()

    with _ANALYSIS_CACHE_LOCK:
        cached = _ANALYSIS_CACHE.get(ts_code)
        if (
            cached
            and not force_refresh
            and now - cached[0] < _ANALYSIS_CACHE_TTL_SECONDS
        ):
            _ANALYSIS_CACHE.move_to_end(ts_code)
            return cached[1]

    analysis = _build_watch_analysis(ts_code)

    with _ANALYSIS_CACHE_LOCK:
        _ANALYSIS_CACHE[ts_code] = (now, analysis)
        _ANALYSIS_CACHE.move_to_end(ts_code)
        while len(_ANALYSIS_CACHE) > _ANALYSIS_CACHE_MAX_ENTRIES:
            _ANALYSIS_CACHE.popitem(last=False)

    return analysis

# --- 数据模型 ---

class WatchlistStock(BaseModel):
    ts_code: str
    name: Optional[str] = None
    remark: Optional[str] = None

class HoldingUpdate(BaseModel):
    shares: float
    avg_cost: Optional[float] = None

# ========== 自选股管理 ==========

@router.get("/watchlist")
def list_watchlist():
    """获取自选股列表"""
    try:
        df = fetch_df("SELECT * FROM watchlist ORDER BY created_at DESC")
        return {"status": "success", "data": df.to_dict('records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/watchlist")
def add_to_watchlist(stock: WatchlistStock):
    """添加股票到自选"""
    try:
        ts_code = _normalize_ts_code(stock.ts_code)
        if not ts_code:
            raise HTTPException(status_code=400, detail="无效股票代码")
        
        # 尝试从stock_basic获取名称
        if not stock.name:
            basic = fetch_df("SELECT name FROM stock_basic WHERE ts_code = ?", (ts_code,))
            if not basic.empty:
                stock.name = basic.iloc[0]['name']

        with get_db_connection() as con:
            con.execute(
                "INSERT OR REPLACE INTO watchlist (ts_code, name, remark) VALUES (?, ?, ?)",
                (ts_code, stock.name, stock.remark)
            )
        return {"status": "success", "message": f"已添加 {ts_code}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/watchlist/{ts_code}")
def remove_from_watchlist(ts_code: str):
    """从自选删除股票"""
    try:
        norm_code = _normalize_ts_code(ts_code)
        with get_db_connection() as con:
            con.execute("DELETE FROM watchlist WHERE ts_code = ?", (norm_code,))
        return {"status": "success", "message": f"已从自选删除 {norm_code}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/watchlist/realtime")
def get_watchlist_realtime(
    codes: Optional[str] = None,
    src: str = "sina",
    include_analysis: bool = True,
    analysis_depth: str = "full",
):
    """
    获取自选股实时行情（盘中刷新）。
    - 盘中：获取实时行情
    - 盘后：获取最近交易日收盘数据
    - 如果未指定codes，则从数据库加载
    - include_analysis: 是否包含技术分析
    - analysis_depth: full 返回完整分析，compact 返回列表所需摘要
    """
    analysis_depth = str(analysis_depth or "full").lower()
    if analysis_depth not in {"full", "compact"}:
        raise HTTPException(status_code=400, detail="analysis_depth 仅支持 full 或 compact")

    if codes:
        raw_codes = [c.strip() for c in codes.split(",") if c.strip()]
        norm_codes = [_normalize_ts_code(c) for c in raw_codes]
        norm_codes = [c for c in norm_codes if c]
    else:
        db_watchlist = fetch_df("SELECT ts_code FROM watchlist")
        raw_codes = db_watchlist['ts_code'].tolist() if not db_watchlist.empty else []
        norm_codes = [_normalize_ts_code(c) for c in raw_codes]
        norm_codes = [c for c in norm_codes if c]

    if len(norm_codes) > 50:
        norm_codes = norm_codes[:50]
        logger.warning(f"自选股数量超过50只，已截断到50只")

    if not norm_codes:
        return {"status": "success", "is_trading_time": False, "message": "自选股为空", "data": []}

    is_trading = trading_calendar.is_trading_time()
    rows = []

    if is_trading:
        quote_df = sync_engine.provider.realtime_quote(ts_code=",".join(norm_codes), src=src or "sina")
        if quote_df is not None and not quote_df.empty:
            col_map = {str(c).lower(): c for c in quote_df.columns}
            price_col = col_map.get("price") or col_map.get("current") or col_map.get("close")
            pre_close_col = col_map.get("pre_close") or col_map.get("yclose")
            pct_col = col_map.get("pct_chg") or col_map.get("pct_change") or col_map.get("changepercent")
            name_col = col_map.get("name")
            vol_col = col_map.get("vol") or col_map.get("volume")
            amount_col = col_map.get("amount") or col_map.get("turnover")

            for _, row in quote_df.iterrows():
                ts_code = _normalize_ts_code(str(row.get(col_map.get("ts_code", ""), "")))
                if not ts_code:
                    continue

                price = _safe_float(row.get(price_col)) if price_col else None
                pre_close = _safe_float(row.get(pre_close_col)) if pre_close_col else None
                pct = _safe_float(row.get(pct_col)) if pct_col else None
                if price is not None and pre_close not in (None, 0) and pct is None:
                    pct = (price - pre_close) / pre_close * 100.0

                analyze_result = {}
                if include_analysis:
                    full_analysis = _get_watch_analysis(ts_code)
                    analyze_result = (
                        full_analysis
                        if analysis_depth == "full"
                        else _compact_watch_analysis(full_analysis)
                    )

                rows.append({
                    "ts_code": ts_code,
                    "name": str(row.get(name_col, "")) if name_col else "",
                    "price": price,
                    "pre_close": pre_close,
                    "pct": pct,
                    "vol": _safe_float(row.get(vol_col)) if vol_col else None,
                    "amount": _safe_float(row.get(amount_col)) if amount_col else None,
                    "analyze": analyze_result
                })
    
    processed_codes = {r['ts_code'] for r in rows}
    remaining_codes = [c for c in norm_codes if c not in processed_codes]

    if remaining_codes:
        placeholders = ",".join(["?"] * len(remaining_codes))
        static_df = fetch_df(f"""
            SELECT ts_code, close as price, pre_close, pct_chg as pct, vol, amount, trade_date
            FROM daily_price
            WHERE (ts_code, trade_date) IN (
                SELECT ts_code, MAX(trade_date)
                FROM daily_price
                WHERE ts_code IN ({placeholders})
                GROUP BY ts_code
            )
        """, tuple(remaining_codes))

        names_df = fetch_df(
            f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
            tuple(remaining_codes),
        )
        name_map = dict(zip(names_df['ts_code'], names_df['name']))

        for _, row in static_df.iterrows():
            tc = row['ts_code']
            analyze_result = {}
            if include_analysis:
                full_analysis = _get_watch_analysis(tc)
                analyze_result = (
                    full_analysis
                    if analysis_depth == "full"
                    else _compact_watch_analysis(full_analysis)
                )
            rows.append({
                "ts_code": tc,
                "name": name_map.get(tc, tc),
                "price": row['price'],
                "pre_close": row['pre_close'],
                "pct": row['pct'],
                "vol": row['vol'],
                "amount": row['amount'],
                "analyze": analyze_result
            })

    idx_map = {c: i for i, c in enumerate(norm_codes)}
    rows.sort(key=lambda x: idx_map.get(x.get("ts_code"), 999))

    return {
        "status": "success",
        "refresh_mode": "realtime" if is_trading else "static",
        "is_trading_time": is_trading,
        "message": "实时刷新中" if is_trading else "非交易时段，已展示最近收盘数据",
        "data": rows,
    }

@router.get("/watchlist/{ts_code}/analysis")
def get_watchlist_analysis(ts_code: str, force_refresh: bool = False):
    """获取单只自选股的深度分析，供详情弹窗按需加载。"""
    try:
        norm_code = _normalize_ts_code(ts_code)
        if not norm_code:
            raise HTTPException(status_code=400, detail="无效股票代码")
        return {
            "status": "success",
            "ts_code": norm_code,
            "data": _get_watch_analysis(norm_code, force_refresh=force_refresh),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== 股票搜索 ==========

@router.get("/stock/search")
def search_stocks(q: str = "", limit: int = 10):
    """搜索股票，支持代码、名称、拼音首字母；q为空时返回所有股票（用于前端缓存）"""
    try:
        q = q.strip() if q else ""
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="limit 必须为整数")
        limit = max(1, min(limit, 5000))

        # 空查询：返回所有股票（用于前端缓存）
        if not q:
            query = "SELECT ts_code, name, pinyin, pinyin_abbr FROM stock_basic ORDER BY ts_code LIMIT ?"
            df = fetch_df(query, (limit,))
            result = df.to_dict('records') if not df.empty else []
            return {"status": "success", "data": result}

        # 判断输入类型：纯数字优先匹配代码，中文匹配名称，英文匹配代码或拼音
        is_digit = q.isdigit()
        is_chinese = any('\u4e00' <= c <= '\u9fff' for c in q)
        q_upper = q.upper()
        q_lower = q.lower()
        
        if is_digit:
            # 纯数字输入：优先匹配股票代码（如600000、000001）
            query = """
                SELECT ts_code, name, pinyin, pinyin_abbr FROM stock_basic 
                WHERE ts_code LIKE ? OR symbol LIKE ?
                ORDER BY 
                    CASE WHEN symbol = ? THEN 0
                         WHEN symbol LIKE ? THEN 1
                         ELSE 2 END,
                    ts_code
                LIMIT ?
            """
            prefix = f"{q}%"
            params = (prefix, prefix, q, f"{q}%", limit)
        elif is_chinese:
            # 中文输入：匹配名称
            contains_pattern = f"%{q}%"
            prefix_pattern = f"{q}%"
            query = """
                SELECT ts_code, name, pinyin, pinyin_abbr FROM stock_basic 
                WHERE name LIKE ?
                   OR name LIKE ?
                ORDER BY 
                    CASE WHEN name = ? THEN 0
                         WHEN name LIKE ? THEN 1
                         ELSE 2 END,
                    ts_code
                LIMIT ?
            """
            params = (
                prefix_pattern,
                contains_pattern,
                q,
                prefix_pattern,
                limit,
            )
        else:
            # 英文输入：匹配代码或拼音首字母
            contains_pattern = f"%{q_upper}%"
            prefix_pattern = f"{q_upper}%"
            pinyin_pattern = f"%{q_lower}%"
            pinyin_prefix = f"{q_lower}%"
            query = """
                SELECT ts_code, name, pinyin, pinyin_abbr FROM stock_basic 
                WHERE UPPER(ts_code) LIKE ?
                   OR UPPER(ts_code) LIKE ?
                   OR pinyin_abbr LIKE ?
                   OR pinyin_abbr LIKE ?
                   OR pinyin LIKE ?
                   OR pinyin LIKE ?
                ORDER BY 
                    CASE WHEN UPPER(ts_code) = ? THEN 0
                         WHEN UPPER(ts_code) LIKE ? THEN 1
                         WHEN pinyin_abbr LIKE ? THEN 2
                         WHEN pinyin_abbr LIKE ? THEN 3
                         WHEN pinyin LIKE ? THEN 4
                         ELSE 5 END,
                    ts_code
                LIMIT ?
            """
            params = (
                prefix_pattern,
                contains_pattern,
                pinyin_prefix,
                pinyin_pattern,
                pinyin_prefix,
                pinyin_pattern,
                q_upper,
                prefix_pattern,
                pinyin_prefix,
                pinyin_pattern,
                pinyin_prefix,
                limit,
            )

        df = fetch_df(query, params)
        
        result = []
        if not df.empty:
            result = df.to_dict('records')
        
        return {"status": "success", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== K线数据 ==========

@router.get("/stock/{ts_code}/kline")
def get_stock_kline(ts_code: str, limit: int = 200):
    """获取股票日K线数据，包含均线、指标及融资融券"""
    try:
        norm_code = _normalize_ts_code(ts_code)
        # 获取行情
        df = fetch_df(
            """
            SELECT trade_date, open, high, low, close, vol, amount, factors
            FROM daily_price
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (norm_code, limit),
        )
        if df.empty:
            return {"status": "success", "data": []}
        
        df = df.iloc[::-1].reset_index(drop=True)
        
        # 获取两融数据
        margin_df = fetch_df(
            """
            SELECT trade_date, rzye, rzmre, rqye
            FROM stock_margin
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (norm_code, limit * 2),
        )
        
        # 合并
        if not margin_df.empty:
            df = df.merge(margin_df, on='trade_date', how='left')
        
        # 获取主力资金数据
        moneyflow_df = fetch_df(
            """
            SELECT trade_date, net_mf_vol, net_mf_amount
            FROM stock_moneyflow
            WHERE ts_code = ?
            ORDER BY trade_date DESC
            LIMIT ?
            """,
            (norm_code, limit * 2),
        )
        
        # 合并主力资金
        if not moneyflow_df.empty:
            df = df.merge(moneyflow_df, on='trade_date', how='left')
        
        # 处理factors（均线），并处理NaN值
        result = []
        for _, row in df.iterrows():
            item = row.to_dict()
            if row.factors:
                try:
                    factors = json.loads(row.factors) if isinstance(row.factors, str) else row.factors
                    item.update(factors)
                except:
                    pass
            # 将NaN / Inf转换为None (JSON null)
            for k, v in item.items():
                if isinstance(v, float) and not math.isfinite(v):
                    item[k] = None
            result.append(item)

        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== 持仓管理 ==========

@router.get("/users/me/holdings")
async def get_holdings(request: Request):
    """获取当前用户的持仓（含盈亏计算）"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            # 获取持仓基本信息
            rows = con.execute("""
                SELECT h.ts_code, h.shares, h.avg_cost, h.updated_at,
                       b.name, p.close as current_price
                FROM user_holdings h
                LEFT JOIN stock_basic b ON h.ts_code = b.ts_code
                LEFT JOIN (
                    SELECT ts_code, close,
                           ROW_NUMBER() OVER (PARTITION BY ts_code ORDER BY trade_date DESC) as rn
                    FROM daily_price
                ) p ON h.ts_code = p.ts_code AND p.rn = 1
                WHERE h.user_id = ?
            """, (user_id,)).fetchall()
        
        holdings = []
        total_market_value = 0
        
        for r in rows:
            ts_code, shares, avg_cost, updated_at, name, current_price = r
            shares = float(shares) if shares else 0
            avg_cost = float(avg_cost) if avg_cost else 0
            current_price = float(current_price) if current_price else 0
            
            # 计算市值和盈亏
            market_value = shares * current_price if current_price else 0
            cost_value = shares * avg_cost
            profit_loss = market_value - cost_value
            profit_loss_pct = (profit_loss / cost_value * 100) if cost_value > 0 else 0
            
            total_market_value += market_value
            
            holdings.append({
                "ts_code": ts_code,
                "name": name or ts_code,
                "shares": shares,
                "avg_cost": avg_cost,
                "current_price": current_price,
                "market_value": round(market_value, 2),
                "cost_value": round(cost_value, 2),
                "profit_loss": round(profit_loss, 2),
                "profit_loss_pct": round(profit_loss_pct, 2),
                "updated_at": str(updated_at) if updated_at else None
            })
        
        # 计算持仓占比
        for h in holdings:
            h["weight_pct"] = round(h["market_value"] / total_market_value * 100, 2) if total_market_value > 0 else 0
        
        return {
            "holdings": holdings,
            "summary": {
                "total_market_value": round(total_market_value, 2),
                "total_cost_value": round(sum(h["cost_value"] for h in holdings), 2),
                "total_profit_loss": round(sum(h["profit_loss"] for h in holdings), 2),
                "total_profit_loss_pct": round(
                    sum(h["profit_loss"] for h in holdings) / sum(h["cost_value"] for h in holdings) * 100
                    if sum(h["cost_value"] for h in holdings) > 0 else 0, 2
                ),
                "stock_count": len(holdings)
            }
        }
    except Exception as e:
        logger.error(f"获取持仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/users/me/holdings/{ts_code}")
async def update_holding(request: Request, ts_code: str, holding: HoldingUpdate):
    """更新持仓"""
    user_id = await get_current_user_id(request)
    try:
        norm_code = _normalize_ts_code(ts_code)
        with get_db_connection() as con:
            exists = con.execute("SELECT 1 FROM user_holdings WHERE user_id = ? AND ts_code = ?", (user_id, norm_code)).fetchone()
            if exists:
                con.execute(
                    "UPDATE user_holdings SET shares = ?, avg_cost = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ? AND ts_code = ?",
                    (holding.shares, holding.avg_cost or 0, user_id, norm_code)
                )
            else:
                con.execute(
                    "INSERT INTO user_holdings (user_id, ts_code, shares, avg_cost) VALUES (?, ?, ?, ?)",
                    (user_id, norm_code, holding.shares, holding.avg_cost or 0)
                )
        return {"message": "持仓已更新"}
    except Exception as e:
        logger.error(f"更新持仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/users/me/holdings/{ts_code}")
async def delete_holding(request: Request, ts_code: str):
    """删除持仓"""
    user_id = await get_current_user_id(request)
    try:
        norm_code = _normalize_ts_code(ts_code)
        with get_db_connection() as con:
            con.execute("DELETE FROM user_holdings WHERE user_id = ? AND ts_code = ?", (user_id, norm_code))
        return {"message": "持仓已删除"}
    except Exception as e:
        logger.error(f"删除持仓失败: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ========== 技术指标 ==========

@router.get("/stock/{ts_code}/indicators")
def get_stock_indicators(ts_code: str, limit: int = 100):
    """获取股票技术指标（均线、MACD、RSI、KDJ、布林带、成交量）
    
    Args:
        ts_code: 股票代码
        limit: 获取最近N天数据，默认100天
    
    Returns:
        技术指标数据，包含最新指标摘要和历史数据
    """
    try:
        from etl.utils.technical_indicators import calculate_all_indicators, get_indicators_summary
        
        norm_code = _normalize_ts_code(ts_code)
        
        # 获取行情数据
        df = fetch_df(
            f"""
            SELECT trade_date, open, high, low, close, vol, amount, pct_chg
            FROM daily_price
            WHERE ts_code = '{norm_code}'
            ORDER BY trade_date DESC
            LIMIT {limit + 60}
            """
        )
        
        if df.empty or len(df) < 20:
            return {
                "status": "success",
                "ts_code": norm_code,
                "message": "数据不足，无法计算技术指标",
                "summary": {},
                "history": []
            }
        
        # 转为正序
        df = df.iloc[::-1].reset_index(drop=True)
        
        # 计算所有技术指标
        df = calculate_all_indicators(df)
        
        # 获取最新指标摘要
        summary = get_indicators_summary(df)
        
        # 获取历史数据（最近limit天）
        history_df = df.tail(limit).copy()
        
        # 转换为JSON格式
        history = []
        for _, row in history_df.iterrows():
            item = {
                "trade_date": str(row.get('trade_date', ''))[:10],
                "open": round(float(row.get('open', 0)), 2),
                "high": round(float(row.get('high', 0)), 2),
                "low": round(float(row.get('low', 0)), 2),
                "close": round(float(row.get('close', 0)), 2),
                "pct_chg": round(float(row.get('pct_chg', 0)), 2),
                "vol": float(row.get('vol', 0)),
                "amount": float(row.get('amount', 0)),
                # 均线
                "ma5": round(float(row.get('ma5', 0)), 2) if not pd.isna(row.get('ma5')) else None,
                "ma10": round(float(row.get('ma10', 0)), 2) if not pd.isna(row.get('ma10')) else None,
                "ma20": round(float(row.get('ma20', 0)), 2) if not pd.isna(row.get('ma20')) else None,
                "ma60": round(float(row.get('ma60', 0)), 2) if not pd.isna(row.get('ma60')) else None,
                # MACD
                "macd_dif": round(float(row.get('macd_dif', 0)), 4) if not pd.isna(row.get('macd_dif')) else None,
                "macd_dea": round(float(row.get('macd_dea', 0)), 4) if not pd.isna(row.get('macd_dea')) else None,
                "macd_bar": round(float(row.get('macd_bar', 0)), 4) if not pd.isna(row.get('macd_bar')) else None,
                # RSI
                "rsi6": round(float(row.get('rsi6', 50)), 1) if not pd.isna(row.get('rsi6')) else None,
                "rsi12": round(float(row.get('rsi12', 50)), 1) if not pd.isna(row.get('rsi12')) else None,
                "rsi24": round(float(row.get('rsi24', 50)), 1) if not pd.isna(row.get('rsi24')) else None,
                # KDJ
                "kdj_k": round(float(row.get('kdj_k', 50)), 1) if not pd.isna(row.get('kdj_k')) else None,
                "kdj_d": round(float(row.get('kdj_d', 50)), 1) if not pd.isna(row.get('kdj_d')) else None,
                "kdj_j": round(float(row.get('kdj_j', 50)), 1) if not pd.isna(row.get('kdj_j')) else None,
                # 布林带
                "boll_upper": round(float(row.get('boll_upper', 0)), 2) if not pd.isna(row.get('boll_upper')) else None,
                "boll_mid": round(float(row.get('boll_mid', 0)), 2) if not pd.isna(row.get('boll_mid')) else None,
                "boll_lower": round(float(row.get('boll_lower', 0)), 2) if not pd.isna(row.get('boll_lower')) else None,
                # 成交量
                "vol_ma5": round(float(row.get('vol_ma5', 0)), 0) if not pd.isna(row.get('vol_ma5')) else None,
                "volume_ratio": round(float(row.get('volume_ratio', 1)), 2) if not pd.isna(row.get('volume_ratio')) else None,
            }
            history.append(item)
        
        return {
            "status": "success",
            "ts_code": norm_code,
            "summary": summary,
            "history": history
        }
    except Exception as e:
        logger.error(f"获取技术指标失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# ========== 主线龙头选股 ==========

@router.get("/mainline/leaders")
def get_mainline_leaders(
    limit: int = 20,
    min_score: int = 60,
    sector: Optional[str] = None
):
    """
    主线龙头推荐
    
    选股逻辑：
    1. 先选主线板块（板块效应）
    2. 再选板块内强势股（龙头梯队）
    3. 结合形态突破、资金确认、盈亏比
    
    Args:
        limit: 每个板块返回的龙头数量
        min_score: 最低评分筛选
        sector: 指定板块筛选
    
    Returns:
        主线板块及龙头股推荐列表
    """
    try:
        from strategy.mainline.analyst import mainline_analyst
        from etl.utils.scoring import (
            calc_mainline_leader_score,
            calc_entry_stop_target,
            get_signal_level
        )
        import json
        
        # 获取最新交易日
        date_df = fetch_df("""
            SELECT trade_date FROM daily_price 
            GROUP BY trade_date HAVING COUNT(*) > 1000 
            ORDER BY trade_date DESC LIMIT 1
        """)
        
        if date_df.empty:
            return {"status": "success", "message": "无数据", "data": []}
        
        trade_date = date_df.iloc[0]['trade_date']
        trade_date_str = trade_date.strftime('%Y-%m-%d') if hasattr(trade_date, 'strftime') else str(trade_date)
        
        # 获取主线板块分析 (使用get_history获取实时数据)
        mainline_history = mainline_analyst.get_history(days=10)
        
        if not mainline_history or not mainline_history.get('series'):
            return {"status": "success", "message": "无主线板块", "mainlines": []}
        
        review_10d = ((mainline_history.get('analysis') or {}).get('review_10d') or {})

        # 优先使用最近10日复盘后的持续主线，避免单日噪声题材进入龙头推荐
        mainline_result = []
        for item in review_10d.get('mainlines', []) or []:
            mainline_result.append({
                'name': item.get('name', ''),
                'score': item.get('latest_score', 0),
                'limit_ups': item.get('max_limit_ups', 0),
                'breadth': item.get('latest_breadth', 0),
                'stock_count': item.get('stock_count', 0),
                'top_stocks': item.get('leaders', []),
                'active_days': item.get('active_days', 0),
                'consecutive_days': item.get('consecutive_days', 0),
            })

        if not mainline_result:
            for series in mainline_history.get('series', []):
                if series.get('data'):
                    latest = series['data'][-1] if series['data'] else {}
                    mainline_result.append({
                        'name': series.get('name', ''),
                        'score': latest.get('value', 0),
                        'limit_ups': latest.get('limit_ups', 0),
                        'breadth': latest.get('breadth', 0),
                        'stock_count': latest.get('stock_count', 0),
                        'top_stocks': latest.get('top_stocks', []),
                        'active_days': 0,
                        'consecutive_days': 0,
                    })
        
        # 按分数排序，取前5
        mainline_result.sort(key=lambda x: x.get('score', 0), reverse=True)
        mainline_result = mainline_result[:5]
        
        if not mainline_result:
            return {"status": "success", "message": "无主线板块", "mainlines": []}
        
        # 获取市场环境
        market_env = get_market_environment(trade_date_str)
        
        # 构建主线板块数据
        mainlines_data = []
        
        for mainline in mainline_result:
            sector_name = mainline.get('name', '')
            
            # 板块筛选
            if sector and sector not in sector_name:
                continue
            
            # 获取板块内股票
            sector_stocks = get_sector_stocks(sector_name, trade_date_str)
            
            if len(sector_stocks) < 5:
                continue
            
            # 计算每只股票的龙头评分
            leaders = []
            
            for stock in sector_stocks:
                # 计算综合评分
                score, reason = calc_mainline_leader_score(stock, market_env, sector_stocks)
                
                if score < min_score:
                    continue
                
                # 计算买入区间、止损、目标价
                entry_stop_target = calc_entry_stop_target(stock)
                
                # 获取信号等级
                signal = get_signal_level(score)
                
                # 获取板块内排名
                sorted_by_pct = sorted(sector_stocks, key=lambda x: x.get('pct_chg', 0), reverse=True)
                sector_rank = next((i + 1 for i, s in enumerate(sorted_by_pct) if s.get('ts_code') == stock['ts_code']), len(sorted_by_pct))
                
                leaders.append({
                    'ts_code': stock.get('ts_code'),
                    'name': stock.get('name', ''),
                    'score': score,
                    'reason': reason,
                    'sector_rank': sector_rank,
                    'sector_total': len(sector_stocks),
                    'close': stock.get('close', 0),
                    'pct_chg': stock.get('pct_chg', 0),
                    'volume_ratio': stock.get('volume_ratio', 1.0),
                    'turnover_rate': stock.get('turnover_rate', 0),
                    'net_mf_amount': stock.get('net_mf_amount', 0),
                    'entry_zone': entry_stop_target.get('entry_zone'),
                    'stop_loss': entry_stop_target.get('stop_loss'),
                    'target': entry_stop_target.get('target'),
                    'risk_reward': entry_stop_target.get('risk_reward'),
                    'max_loss_pct': entry_stop_target.get('max_loss_pct'),
                    'target_gain_pct': entry_stop_target.get('target_gain_pct'),
                    'signal': signal,
                })
            
            # 按评分排序
            leaders.sort(key=lambda x: x['score'], reverse=True)
            leaders = leaders[:limit]

            if not leaders:
                leaders = build_recent_leader_fallback(
                    sector_stocks=sector_stocks,
                    review_leaders=mainline.get('top_stocks', []),
                    limit=limit,
                )

            # 计算板块共振度
            resonance = calc_sector_resonance_simple(sector_stocks)

            mainlines_data.append({
                'sector': sector_name,
                'strength': mainline.get('score', 0),
                'limit_ups': mainline.get('limit_ups', 0),
                'stock_count': mainline.get('stock_count', 0),
                'active_days': mainline.get('active_days', 0),
                'consecutive_days': mainline.get('consecutive_days', 0),
                'resonance': resonance,
                'leaders': leaders,
            })
        
        return {
            "status": "success",
            "trade_date": trade_date_str,
            "market_env": market_env,
            "mainlines": mainlines_data
        }
        
    except Exception as e:
        logger.error(f"获取主线龙头失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stock/{ts_code}/mainline_analysis")
def get_stock_mainline_analysis(ts_code: str):
    """
    个股主线分析
    
    分析个股是否属于主线板块、板块内地位、突破形态、资金状况
    
    Args:
        ts_code: 股票代码
    
    Returns:
        个股主线分析结果
    """
    try:
        from strategy.mainline.analyst import mainline_analyst
        from etl.utils.scoring import (
            calc_sector_resonance,
            calc_breakout_score,
            calc_flow_score,
            calc_risk_reward,
            calc_entry_stop_target,
            get_signal_level,
            calc_mainline_leader_score
        )
        import json
        
        norm_code = _normalize_ts_code(ts_code)
        
        # 获取最新交易日
        date_df = fetch_df("""
            SELECT trade_date FROM daily_price 
            GROUP BY trade_date HAVING COUNT(*) > 1000 
            ORDER BY trade_date DESC LIMIT 1
        """)
        
        if date_df.empty:
            return {"status": "success", "message": "无数据", "analysis": {}}
        
        trade_date = date_df.iloc[0]['trade_date']
        trade_date_str = trade_date.strftime('%Y-%m-%d') if hasattr(trade_date, 'strftime') else str(trade_date)
        
        # 获取股票数据
        stock_df = fetch_df(f"""
            SELECT d.ts_code, d.close, d.pct_chg, d.vol, d.amount, d.factors,
                   b.name, b.industry
            FROM daily_price d
            LEFT JOIN stock_basic b ON d.ts_code = b.ts_code
            WHERE d.ts_code = '{norm_code}' AND d.trade_date = '{trade_date_str}'
        """)
        
        if stock_df.empty:
            return {"status": "success", "message": "股票数据不存在", "analysis": {}}
        
        stock_row = stock_df.iloc[0]
        
        # 解析factors
        factors = {}
        try:
            if stock_row.get('factors'):
                factors = json.loads(stock_row['factors']) if isinstance(stock_row['factors'], str) else stock_row['factors']
        except:
            pass
        
        # 获取所属板块
        sector_df = fetch_df(f"""
            SELECT concept_name FROM stock_concept_details
            WHERE ts_code = '{norm_code}'
        """)
        
        sectors = sector_df['concept_name'].tolist() if not sector_df.empty else []
        
        # 获取主线板块
        mainline_result = mainline_analyst.analyze(days=3, limit=10, trade_date=trade_date_str)
        stock_map_df = mainline_analyst.get_stock_mainline_map(ts_codes=[norm_code])
        mapped_sector = (
            stock_map_df.iloc[0]['mapped_name']
            if stock_map_df is not None and not stock_map_df.empty
            else ''
        )

        # 判断是否属于主线板块
        mainline_sectors = [m.get('name', '') for m in mainline_result] if mainline_result else []
        is_mainline = bool(mapped_sector) and mapped_sector in mainline_sectors

        # 找到所属主线板块
        belong_sector = next(
            (ml for ml in (mainline_result or []) if ml.get('name', '') == mapped_sector),
            None
        )

        # 获取板块内其他股票
        sector_stocks = []
        if belong_sector:
            sector_stocks = get_sector_stocks(belong_sector.get('name', ''), trade_date_str)
        
        # 获取资金流向数据
        flow_df = fetch_df(f"""
            SELECT trade_date, net_mf_amount
            FROM stock_moneyflow
            WHERE ts_code = '{norm_code}'
            ORDER BY trade_date DESC
            LIMIT 5
        """)
        
        # 计算连续流入天数
        flow_continuous_days = 0
        if not flow_df.empty:
            for _, row in flow_df.iterrows():
                if row.get('net_mf_amount', 0) > 0:
                    flow_continuous_days += 1
                else:
                    break
        
        # 构建股票数据
        stock_data = {
            'ts_code': norm_code,
            'name': stock_row.get('name', ''),
            'close': float(stock_row.get('close', 0)),
            'pct_chg': float(stock_row.get('pct_chg', 0)),
            'vol': float(stock_row.get('vol', 0)),
            'amount': float(stock_row.get('amount', 0)),
            'factors': factors,
            'is_mainline': is_mainline,
            'sectors': sectors,
            'mapped_sector': mapped_sector,
            'flow_continuous_days': flow_continuous_days,
            'flow_total_inflow': float(flow_df['net_mf_amount'].sum()) if not flow_df.empty else 0,
            'big_order_ratio': 0.3,  # 需要从详细资金数据计算
            'volume_ratio': 1.0,  # 需要计算
            'turnover_rate': 0,  # 需要计算
            'total_mv': factors.get('total_mv', 0),
        }
        
        # 计算量比
        vol_ma5 = factors.get('vol_ma5', stock_data['vol'])
        if vol_ma5 > 0:
            stock_data['volume_ratio'] = round(stock_data['vol'] / vol_ma5, 2)
        
        # 获取市场环境
        market_env = get_market_environment(trade_date_str)
        
        # 计算综合评分
        score, reason = calc_mainline_leader_score(stock_data, market_env, sector_stocks)
        
        # 计算各因子评分
        from etl.utils.scoring import (
            calc_sector_resonance as calc_resonance,
            calc_breakout_score,
            calc_flow_score as calc_flow,
            calc_sector_rank_score,
            calc_volume_match_score
        )
        
        factor_scores = {
            'sector_resonance': calc_resonance(stock_data, sector_stocks),
            'breakout': calc_breakout_score(stock_data),
            'capital_flow': calc_flow(stock_data),
            'sector_rank': calc_sector_rank_score(stock_data, sector_stocks),
            'volume_match': calc_volume_match_score(stock_data),
        }
        
        # 计算买入区间、止损、目标价
        entry_stop_target = calc_entry_stop_target(stock_data)
        
        # 获取信号等级
        signal = get_signal_level(score)
        
        # 板块内排名
        sector_rank = None
        sector_total = len(sector_stocks)
        if sector_stocks:
            sorted_by_pct = sorted(sector_stocks, key=lambda x: x.get('pct_chg', 0), reverse=True)
            sector_rank = next((i + 1 for i, s in enumerate(sorted_by_pct) if s.get('ts_code') == norm_code), sector_total)
        
        return {
            "status": "success",
            "ts_code": norm_code,
            "name": stock_data['name'],
            "industry": stock_row.get('industry', ''),
            "trade_date": trade_date_str,
            "is_mainline": is_mainline,
            "sector": belong_sector.get('name', '') if belong_sector else '',
            "mapped_sector": mapped_sector,
            "sectors": sectors,
            "sector_rank": sector_rank,
            "sector_total": sector_total,
            "analysis": {
                "factor_scores": factor_scores,
                "sector_resonance": {
                    "score": factor_scores['sector_resonance'],
                    "strong_ratio": len([s for s in sector_stocks if s.get('pct_chg', 0) > 5]) / len(sector_stocks) if sector_stocks else 0,
                    "inflow_ratio": len([s for s in sector_stocks if s.get('net_mf_amount', 0) > 0]) / len(sector_stocks) if sector_stocks else 0,
                },
                "breakout": {
                    "score": factor_scores['breakout'],
                    "status": "已突破" if factor_scores['breakout'] >= 70 else "未突破",
                    "detail": generate_breakout_detail(stock_data),
                },
                "capital_flow": {
                    "score": factor_scores['capital_flow'],
                    "continuous_days": flow_continuous_days,
                    "total_inflow": stock_data['flow_total_inflow'],
                },
                "risk_reward": entry_stop_target,
            },
            "signal": {
                "score": score,
                "reason": reason,
                **signal
            }
        }
        
    except Exception as e:
        logger.error(f"获取个股主线分析失败: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


def get_market_environment(trade_date: str) -> Dict[str, Any]:
    """
    获取市场环境数据
    """
    try:
        # 获取沪深300数据
        index_df = fetch_df(f"""
            SELECT close, pct_chg FROM market_index
            WHERE ts_code = '000300.SH' AND trade_date <= '{trade_date}'
            ORDER BY trade_date DESC LIMIT 25
        """)
        
        if index_df.empty:
            return {'trend': 'neutral', 'sentiment': 50, 'suggestion': '数据不足'}
        
        # 计算趋势
        if len(index_df) >= 20:
            ma20 = index_df['close'].head(20).mean()
            current = index_df.iloc[0]['close']
            trend = 'up' if current > ma20 else 'down'
        else:
            trend = 'neutral'
        
        # 获取市场情绪
        sentiment_df = fetch_df(f"""
            SELECT score FROM market_sentiment
            WHERE trade_date <= '{trade_date}'
            ORDER BY trade_date DESC LIMIT 1
        """)
        
        sentiment = float(sentiment_df.iloc[0]['score']) if not sentiment_df.empty else 50
        
        # 生成建议
        if trend == 'up' and sentiment >= 50:
            suggestion = '市场健康上涨，可积极操作'
        elif trend == 'up' and sentiment < 50:
            suggestion = '市场上涨但情绪谨慎，精选个股'
        elif trend == 'down' and sentiment <= 30:
            suggestion = '市场弱势+情绪冰点，等待企稳'
        elif trend == 'down':
            suggestion = '市场下跌，谨慎操作，只做最强主线'
        else:
            suggestion = '震荡市，正常操作'
        
        return {
            'trend': trend,
            'sentiment': round(sentiment, 1),
            'index_pct_chg': round(float(index_df.iloc[0].get('pct_chg', 0)), 2),
            'suggestion': suggestion
        }
    except Exception as e:
        logger.warning(f"获取市场环境失败: {e}")
        return {'trend': 'neutral', 'sentiment': 50, 'suggestion': '数据异常'}


def get_sector_stocks(sector_name: str, trade_date: str) -> list:
    """
    获取板块内股票数据
    """
    try:
        from strategy.mainline.analyst import mainline_analyst

        stock_map = mainline_analyst.get_stock_mainline_map()
        if stock_map.empty:
            return []

        sector_codes = (
            stock_map[stock_map['mapped_name'] == sector_name]['ts_code']
            .dropna()
            .drop_duplicates()
            .tolist()
        )
        if not sector_codes:
            return []

        placeholders = ",".join(["?"] * len(sector_codes))
        stocks_df = fetch_df(
            f"""
            SELECT d.ts_code, b.name, d.close, d.pct_chg, d.vol, d.amount, d.factors,
                   COALESCE(m.net_mf_amount, 0) AS net_mf_amount
            FROM daily_price d
            LEFT JOIN stock_basic b ON d.ts_code = b.ts_code
            LEFT JOIN stock_moneyflow m ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
            WHERE d.trade_date = ?
              AND d.vol > 0
              AND d.ts_code IN ({placeholders})
            """,
            params=[trade_date, *sector_codes],
        )
        
        if stocks_df.empty:
            return []
        
        result = []
        for _, row in stocks_df.iterrows():
            factors = {}
            try:
                if row.get('factors'):
                    factors = json.loads(row['factors']) if isinstance(row['factors'], str) else row['factors']
            except:
                pass

            volume_ratio = 1.0
            vol_ma5 = _safe_float(factors.get('vol_ma5'))
            if vol_ma5 and vol_ma5 > 0:
                volume_ratio = round(float(row.get('vol', 0)) / vol_ma5, 2)
            
            result.append({
                'ts_code': row['ts_code'],
                'name': row.get('name', ''),
                'close': float(row.get('close', 0)),
                'pct_chg': float(row.get('pct_chg', 0)),
                'vol': float(row.get('vol', 0)),
                'amount': float(row.get('amount', 0)),
                'factors': factors,
                'net_mf_amount': float(row.get('net_mf_amount', 0)),
                'volume_ratio': volume_ratio,
                'turnover_rate': _safe_float(factors.get('turnover_rate'), 0) or 0,
                'flow_continuous_days': 1 if _safe_float(row.get('net_mf_amount'), 0) > 0 else 0,
                'flow_total_inflow': float(row.get('net_mf_amount', 0)),
                'big_order_ratio': _safe_float(factors.get('big_order_ratio'), 0.0) or 0.0,
                'is_mainline': True,
                'sector': sector_name,
            })
        
        return result
    except Exception as e:
        logger.warning(f"获取板块股票失败: {e}")
        return []


def build_recent_leader_fallback(sector_stocks: list, review_leaders: list, limit: int = 3) -> list:
    """
    当最新交易日因市场环境过弱导致推荐分数不足时，回退到最近10日主线龙头池。
    """
    if not sector_stocks or not review_leaders:
        return []

    stock_map = {item.get('ts_code'): item for item in sector_stocks if item.get('ts_code')}
    fallback = []
    for leader in review_leaders:
        ts_code = leader.get('ts_code')
        stock = stock_map.get(ts_code)
        if not stock:
            continue
        fallback.append({
            'ts_code': ts_code,
            'name': stock.get('name', leader.get('name', '')),
            'score': round(float(leader.get('leader_score', 0)), 1),
            'reason': (
                f"近10日活跃{leader.get('active_days', 0)}天，"
                f"最近3日走强{leader.get('recent_active_days', 0)}天，"
                f"最高涨幅{leader.get('max_pct', 0)}%"
            ),
            'sector_rank': 0,
            'sector_total': len(sector_stocks),
            'close': stock.get('close', 0),
            'pct_chg': stock.get('pct_chg', leader.get('latest_pct', 0)),
            'volume_ratio': stock.get('volume_ratio', 1.0),
            'turnover_rate': stock.get('turnover_rate', 0),
            'net_mf_amount': stock.get('net_mf_amount', 0),
            'entry_zone': None,
            'stop_loss': None,
            'target': None,
            'risk_reward': None,
            'max_loss_pct': None,
            'target_gain_pct': None,
            'signal': '观察',
        })

    return fallback[:max(1, int(limit))]


def calc_sector_resonance_simple(sector_stocks: list) -> float:
    """
    计算板块共振度（简化版）
    """
    if not sector_stocks:
        return 0
    
    total = len(sector_stocks)
    strong_count = len([s for s in sector_stocks if s.get('pct_chg', 0) > 5])
    inflow_count = len([s for s in sector_stocks if s.get('net_mf_amount', 0) > 0])
    
    strong_ratio = strong_count / total
    inflow_ratio = inflow_count / total
    
    return round((strong_ratio * 50 + inflow_ratio * 50), 1)


def generate_breakout_detail(stock_data: dict) -> str:
    """
    生成突破形态描述
    """
    factors = stock_data.get('factors', {})
    close = stock_data.get('close', 0)
    ma20 = factors.get('ma20', 0)
    ma60 = factors.get('ma60', 0)
    
    details = []
    
    if close > ma60 > 0:
        details.append('站上MA60')
    elif close > ma20 > 0:
        details.append('站上MA20')
    
    volume_ratio = stock_data.get('volume_ratio', 1.0)
    if volume_ratio >= 1.5:
        details.append('放量')
    elif volume_ratio >= 1.2:
        details.append('温和放量')
    
    return '，'.join(details) if details else '无明显突破'

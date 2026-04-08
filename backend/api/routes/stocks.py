# /backend/api/routes/stocks.py

import base64
import json
import logging
import math
import re
import threading
import time
import unicodedata
from collections import OrderedDict
from datetime import date, datetime
from typing import Any, Dict, Optional

import arrow
import httpx
import pandas as pd
from fastapi import APIRouter, File, HTTPException, Request, UploadFile
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
_ANALYSIS_CACHE_MAX_ENTRIES = 256
_STOCK_BASIC_LOOKUP_LOCK = threading.Lock()
_STOCK_BASIC_LOOKUP_TTL_SECONDS = 600
_STOCK_BASIC_LOOKUP_CACHE: dict[str, Any] = {
    "loaded_at": 0.0,
    "rows": [],
    "by_ts_code": {},
    "by_symbol": {},
    "by_exact_name": {},
    "by_norm_name": {},
    "by_pinyin": {},
    "by_pinyin_abbr": {},
}

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


def _normalize_lookup_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or ""))
    text = text.replace("\u3000", " ")
    text = text.replace("（", "(").replace("）", ")")
    return text.strip().upper()


def _normalize_stock_name_key(value: Any) -> str:
    text = _normalize_lookup_text(value)
    if not text:
        return ""
    text = text.replace("股份有限公司", "").replace("有限公司", "")
    text = text.replace("*", "")
    text = re.sub(r"\([^)]*\)", "", text)
    text = re.sub(r"[\s·•ㆍ･・/,_\\-]+", "", text)
    text = re.sub(r"[()\[\]{}【】<>《》]", "", text)
    return text.strip()


def _append_unique_text(bucket: list[str], seen: set[str], value: Any) -> None:
    text = _normalize_lookup_text(value)
    if not text or text in seen:
        return
    seen.add(text)
    bucket.append(text)


def _extract_stock_symbol_candidates(*values: Any) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()

    for value in values:
        text = _normalize_lookup_text(value)
        if not text:
            continue

        stripped = re.sub(r"[^0-9A-Z.]", "", text)
        if stripped:
            cleaned = stripped.replace("SH", "").replace("SZ", "").replace("BJ", "").replace(".", "")
            if len(cleaned) == 6 and cleaned.isdigit() and cleaned not in seen:
                seen.add(cleaned)
                candidates.append(cleaned)

        for symbol in re.findall(r"(?<!\d)(\d{6})(?!\d)", text):
            if symbol in seen:
                continue
            seen.add(symbol)
            candidates.append(symbol)

    return candidates


def _build_stock_name_candidates(raw_name: Any) -> list[str]:
    base = _normalize_lookup_text(raw_name)
    if not base:
        return []

    candidates: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        _append_unique_text(candidates, seen, value)

    add(base)
    stripped_brackets = re.sub(r"\([^)]*\)", "", base).strip()
    add(stripped_brackets)

    for source in tuple(candidates):
        trimmed = source
        for prefix in ("XD", "XR", "DR", "N", "C"):
            if trimmed.startswith(prefix) and len(trimmed) > len(prefix) + 1:
                add(trimmed[len(prefix):])
        if trimmed.startswith("*ST") and len(trimmed) > 3:
            add(trimmed[1:])
        if trimmed.endswith("A股") and len(trimmed) > 2:
            add(trimmed[:-2])
        if trimmed.endswith("B股") and len(trimmed) > 2:
            add(trimmed[:-2])
        if trimmed.endswith("A") and len(trimmed) > 2:
            add(trimmed[:-1])
        if trimmed.endswith("B") and len(trimmed) > 2:
            add(trimmed[:-1])

    return candidates


def _pick_unique_stock_record(records: Any) -> Optional[dict[str, Any]]:
    if not isinstance(records, list) or len(records) != 1:
        return None
    record = records[0]
    return record if isinstance(record, dict) else None


def _load_stock_basic_lookup() -> dict[str, Any]:
    now = time.time()
    with _STOCK_BASIC_LOOKUP_LOCK:
        if now - float(_STOCK_BASIC_LOOKUP_CACHE.get("loaded_at") or 0) < _STOCK_BASIC_LOOKUP_TTL_SECONDS:
            return _STOCK_BASIC_LOOKUP_CACHE

    df = fetch_df(
        """
        SELECT ts_code, symbol, name, pinyin, pinyin_abbr
        FROM stock_basic
        """,
    )

    lookup: dict[str, Any] = {
        "loaded_at": now,
        "rows": [],
        "by_ts_code": {},
        "by_symbol": {},
        "by_exact_name": {},
        "by_norm_name": {},
        "by_pinyin": {},
        "by_pinyin_abbr": {},
    }

    if not df.empty:
        for raw_row in df.to_dict("records"):
            ts_code = _normalize_ts_code(raw_row.get("ts_code") or "")
            name = str(raw_row.get("name") or "").strip()
            symbol = str(raw_row.get("symbol") or "").strip().upper()
            pinyin = _normalize_lookup_text(raw_row.get("pinyin"))
            pinyin_abbr = _normalize_lookup_text(raw_row.get("pinyin_abbr"))
            exact_name = _normalize_lookup_text(name)
            norm_name = _normalize_stock_name_key(name)

            record = {
                "ts_code": ts_code,
                "name": name,
                "symbol": symbol,
                "exact_name": exact_name,
                "norm_name": norm_name,
                "pinyin": pinyin,
                "pinyin_abbr": pinyin_abbr,
            }
            lookup["rows"].append(record)

            if ts_code and ts_code not in lookup["by_ts_code"]:
                lookup["by_ts_code"][ts_code] = record
            if symbol and symbol not in lookup["by_symbol"]:
                lookup["by_symbol"][symbol] = record
            if exact_name:
                lookup["by_exact_name"].setdefault(exact_name, []).append(record)
            if norm_name:
                lookup["by_norm_name"].setdefault(norm_name, []).append(record)
            if pinyin:
                lookup["by_pinyin"].setdefault(pinyin, []).append(record)
            if pinyin_abbr:
                lookup["by_pinyin_abbr"].setdefault(pinyin_abbr, []).append(record)

    with _STOCK_BASIC_LOOKUP_LOCK:
        _STOCK_BASIC_LOOKUP_CACHE.clear()
        _STOCK_BASIC_LOOKUP_CACHE.update(lookup)
        return _STOCK_BASIC_LOOKUP_CACHE


def _is_beijing_stock(code: Any) -> bool:
    return str(code or "").upper().endswith(".BJ")

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


def _clean_theme_token(value: Any) -> str:
    cleaned = str(value or "").strip()
    if not cleaned:
        return ""
    cleaned = cleaned.replace("_THS", "")
    cleaned = cleaned.replace("（", "(").replace("）", ")")
    for token in ("概念股", "概念", "题材", "板块", "指数", "产业链", "同花顺"):
        cleaned = cleaned.replace(token, "")
    cleaned = re.sub(r"[\s/,_\\-]+", "", cleaned)
    return cleaned.strip().upper()


def _count_true_streak(flags: list[bool]) -> int:
    streak = 0
    for flag in reversed(flags):
        if flag:
            streak += 1
        else:
            break
    return streak


def _fetch_recent_trade_dates(trade_date: str, limit: int = 10) -> list[str]:
    date_df = fetch_df(
        """
        SELECT trade_date
        FROM daily_price
        WHERE trade_date <= ?
        GROUP BY trade_date
        HAVING COUNT(*) > 1000
        ORDER BY trade_date DESC
        LIMIT ?
        """,
        params=[trade_date, max(1, int(limit))],
    )
    if date_df.empty:
        return []
    return [
        pd.to_datetime(item).strftime("%Y-%m-%d")
        for item in sorted(pd.to_datetime(date_df["trade_date"]).tolist())
    ]


def _load_sector_recent_metrics(sector_codes: list[str], trade_date: str, lookback: int = 10) -> dict[str, dict[str, Any]]:
    codes = [code for code in sector_codes if code and not _is_beijing_stock(code)]
    if not codes:
        return {}

    recent_dates = _fetch_recent_trade_dates(trade_date, limit=lookback)
    if not recent_dates:
        return {}

    date_placeholders = ",".join(["?"] * len(recent_dates))
    code_placeholders = ",".join(["?"] * len(codes))
    history_df = fetch_df(
        f"""
        SELECT d.trade_date, d.ts_code, d.pct_chg, d.amount,
               COALESCE(m.net_mf_amount, 0) AS net_mf_amount
        FROM daily_price d
        LEFT JOIN stock_moneyflow m
          ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
        WHERE d.trade_date IN ({date_placeholders})
          AND d.ts_code IN ({code_placeholders})
        ORDER BY d.ts_code, d.trade_date
        """,
        params=[*recent_dates, *codes],
    )
    if history_df.empty:
        return {}

    metrics: dict[str, dict[str, Any]] = {}
    review_days = max(1, len(recent_dates))
    recent_window = min(3, review_days)
    for ts_code, rows in history_df.groupby("ts_code"):
        rows = rows.sort_values("trade_date").reset_index(drop=True)
        pct_list = [_safe_float(item, 0.0) or 0.0 for item in rows["pct_chg"].tolist()]
        amount_list = [_safe_float(item, 0.0) or 0.0 for item in rows["amount"].tolist()]
        flow_list = [_safe_float(item, 0.0) or 0.0 for item in rows["net_mf_amount"].tolist()]

        strong_flags = [pct >= 3.0 for pct in pct_list]
        positive_flow_flags = [flow > 0 for flow in flow_list]
        active_days = int(sum(strong_flags))
        recent_active_days = int(sum(strong_flags[-recent_window:])) if strong_flags else 0
        strong_streak = _count_true_streak(strong_flags)
        positive_flow_days = int(sum(positive_flow_flags))
        positive_flow_streak = _count_true_streak(positive_flow_flags)
        total_amount = float(sum(amount_list))
        total_positive_inflow = float(sum(max(flow, 0.0) for flow in flow_list))
        total_net_inflow = float(sum(flow_list))
        first_strong_idx = next((idx for idx, flag in enumerate(strong_flags) if flag), None)
        trend_pioneer_score = 0.0
        if first_strong_idx is not None:
            trend_pioneer_score = round((review_days - first_strong_idx) / review_days * 100, 2)

        metrics[str(ts_code)] = {
            "active_days": active_days,
            "recent_active_days": recent_active_days,
            "strong_streak": strong_streak,
            "limit_ups_10d": int(sum(1 for pct in pct_list if pct >= 9.5)),
            "positive_flow_days": positive_flow_days,
            "flow_positive_streak": positive_flow_streak,
            "flow_total_inflow": total_positive_inflow,
            "flow_net_total": total_net_inflow,
            "flow_inflow_ratio": round(total_net_inflow / total_amount, 4) if total_amount > 0 else 0.0,
            "latest_net_mf_amount": float(flow_list[-1]) if flow_list else 0.0,
            "total_amount_10d": total_amount,
            "latest_amount": float(amount_list[-1]) if amount_list else 0.0,
            "trend_pioneer_score": trend_pioneer_score,
            "avg_pct": round(sum(pct_list) / max(len(pct_list), 1), 2),
            "max_pct": round(max(pct_list), 2) if pct_list else 0.0,
        }

    return metrics


def _load_sector_theme_hits(
    sector_codes: list[str],
    sector_name: str,
    focus_tags: Optional[list[str]] = None,
) -> dict[str, dict[str, Any]]:
    codes = [code for code in sector_codes if code and not _is_beijing_stock(code)]
    if not codes:
        return {}

    theme_terms = [sector_name, *(focus_tags or [])]
    keyword_map: dict[str, str] = {}
    for item in theme_terms:
        cleaned = _clean_theme_token(item)
        if cleaned and cleaned not in keyword_map:
            keyword_map[cleaned] = str(item)
    if not keyword_map:
        return {}

    placeholders = ",".join(["?"] * len(codes))
    concept_df = fetch_df(
        f"""
        SELECT ts_code, concept_name
        FROM stock_concept_details
        WHERE ts_code IN ({placeholders})
          AND concept_name IS NOT NULL
        """,
        params=codes,
    )
    industry_df = fetch_df(
        f"""
        SELECT ts_code, industry
        FROM stock_basic
        WHERE ts_code IN ({placeholders})
        """,
        params=codes,
    )

    candidate_map: dict[str, list[str]] = {code: [] for code in codes}
    if not concept_df.empty:
        for _, row in concept_df.iterrows():
            code = str(row.get("ts_code") or "").strip()
            concept_name = str(row.get("concept_name") or "").strip()
            if code and concept_name:
                candidate_map.setdefault(code, []).append(concept_name)
    if not industry_df.empty:
        for _, row in industry_df.iterrows():
            code = str(row.get("ts_code") or "").strip()
            industry_name = str(row.get("industry") or "").strip()
            if code and industry_name:
                candidate_map.setdefault(code, []).append(industry_name)

    result: dict[str, dict[str, Any]] = {}
    for code, raw_terms in candidate_map.items():
        hits: list[str] = []
        seen_hits: set[str] = set()
        for raw_term in raw_terms:
            cleaned_term = _clean_theme_token(raw_term)
            if not cleaned_term:
                continue
            for keyword, display_name in keyword_map.items():
                if keyword in cleaned_term or cleaned_term in keyword:
                    if display_name not in seen_hits:
                        hits.append(display_name)
                        seen_hits.add(display_name)
                    break
        result[code] = {
            "theme_hit_count": len(hits),
            "theme_hit_names": hits[:3],
        }

    return result


def _sanitize_json_value(val: Any) -> Any:
    if isinstance(val, dict):
        return {k: _sanitize_json_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_sanitize_json_value(v) for v in val]
    if isinstance(val, tuple):
        return [_sanitize_json_value(v) for v in val]
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    try:
        if pd.isna(val):
            return None
    except TypeError:
        pass
    return val


def _normalize_trade_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")

    raw = str(value).strip()
    if not raw:
        return None

    candidates = [raw]
    digits = "".join(ch for ch in raw if ch.isdigit())
    if len(digits) >= 8:
        candidates.append(digits[:8])

    for candidate in candidates:
        for fmt in ("YYYY-MM-DD", "YYYYMMDD", "YYYY-MM-DD HH:mm:ss", "YYYYMMDD HH:mm:ss"):
            try:
                return arrow.get(candidate, fmt).format("YYYY-MM-DD")
            except Exception:
                continue

    try:
        return arrow.get(raw).format("YYYY-MM-DD")
    except Exception:
        return raw[:10] if len(raw) >= 10 else None


def _today_trade_date() -> str:
    return arrow.now("Asia/Shanghai").format("YYYY-MM-DD")


def _can_try_live_snapshot(latest_trade_date: Any = None) -> bool:
    now = arrow.now("Asia/Shanghai")
    if not trading_calendar.is_trading_day(now.date()):
        return False
    return _normalize_trade_date(latest_trade_date) != now.format("YYYY-MM-DD")


def _extract_live_quote_snapshot(
    raw_row: Any,
    expected_ts_code: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if raw_row is None:
        return None

    if isinstance(raw_row, pd.Series):
        row_map = {str(k).lower(): raw_row[k] for k in raw_row.index}
    elif isinstance(raw_row, dict):
        row_map = {str(k).lower(): v for k, v in raw_row.items()}
    else:
        return None

    ts_code = _normalize_ts_code(row_map.get("ts_code"))
    if expected_ts_code and ts_code and ts_code != expected_ts_code:
        return None

    trade_date = _normalize_trade_date(row_map.get("trade_date") or row_map.get("date"))
    price = _safe_float(row_map.get("price") or row_map.get("current") or row_map.get("close"))
    if price is None:
        return None

    pre_close = _safe_float(row_map.get("pre_close") or row_map.get("yclose"))
    pct = _safe_float(
        row_map.get("pct_chg") or row_map.get("pct_change") or row_map.get("changepercent")
    )
    if price is not None and pre_close not in (None, 0) and pct is None:
        pct = (price - pre_close) / pre_close * 100.0

    open_price = _safe_float(row_map.get("open"), price)
    high_price = _safe_float(row_map.get("high"))
    low_price = _safe_float(row_map.get("low"))
    baseline = [v for v in (open_price, price, pre_close) if v is not None]
    if high_price is None and baseline:
        high_price = max(baseline)
    if low_price is None and baseline:
        low_price = min(baseline)

    volume = _safe_float(row_map.get("vol") or row_map.get("volume"))
    amount = _safe_float(row_map.get("amount") or row_map.get("turnover"))

    return {
        "ts_code": ts_code or expected_ts_code,
        "name": str(row_map.get("name") or "").strip(),
        "trade_date": trade_date,
        "quote_time": str(row_map.get("time") or "").strip() or None,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": price,
        "pre_close": pre_close,
        "pct": pct,
        "volume_shares": volume,
        "vol_lot": (volume / 100.0) if volume is not None else None,
        "amount_yuan": amount,
        "amount_k": (amount / 1000.0) if amount is not None else None,
    }


def _fetch_live_snapshot(ts_code: str, latest_trade_date: Any = None, src: str = "sina") -> Optional[Dict[str, Any]]:
    if not _can_try_live_snapshot(latest_trade_date):
        return None

    try:
        quote_df = sync_engine.provider.realtime_quote(ts_code=ts_code, src=src or "sina")
    except Exception as exc:
        logger.warning("获取 %s 实时快照失败: %s", ts_code, exc)
        return None

    if quote_df is None or quote_df.empty:
        return None

    today = _today_trade_date()
    for _, quote_row in quote_df.iterrows():
        snapshot = _extract_live_quote_snapshot(quote_row, expected_ts_code=ts_code)
        if snapshot and snapshot.get("trade_date") == today:
            return snapshot
    return None


def _merge_live_snapshot_into_df(df: pd.DataFrame, snapshot: Optional[Dict[str, Any]]) -> pd.DataFrame:
    if snapshot is None or not snapshot.get("trade_date"):
        return df

    base = df.copy()
    if base.empty:
        base = pd.DataFrame(columns=["trade_date", "open", "high", "low", "close", "pre_close", "vol", "amount", "pct_chg"])

    if "trade_date" in base.columns:
        base["trade_date"] = base["trade_date"].map(_normalize_trade_date)

    for col in ("open", "high", "low", "close", "pre_close", "vol", "amount", "pct_chg"):
        if col not in base.columns:
            base[col] = None
        base[col] = pd.to_numeric(base[col], errors="coerce")

    last_close = _safe_float(base.iloc[-1].get("close")) if not base.empty else None
    existing_same_day = None
    if "trade_date" in base.columns:
        same_day = base.loc[base["trade_date"] == snapshot["trade_date"]]
        if not same_day.empty:
            existing_same_day = same_day.iloc[-1].to_dict()

    if existing_same_day is not None:
        snapshot_row = dict(existing_same_day)
    elif not base.empty:
        snapshot_row = base.iloc[-1].to_dict()
    else:
        snapshot_row = {col: None for col in base.columns}
    snapshot_row.update({
        "trade_date": snapshot["trade_date"],
        "open": snapshot.get("open", last_close),
        "high": snapshot.get("high"),
        "low": snapshot.get("low"),
        "close": snapshot.get("close"),
        "pre_close": snapshot.get("pre_close", last_close),
        "vol": snapshot.get("vol_lot"),
        "amount": snapshot.get("amount_k"),
        "pct_chg": snapshot.get("pct"),
    })

    if snapshot_row["high"] is None:
        candidates = [v for v in (snapshot_row["open"], snapshot_row["close"], snapshot.get("pre_close"), last_close) if v is not None]
        snapshot_row["high"] = max(candidates) if candidates else snapshot_row["close"]
    if snapshot_row["low"] is None:
        candidates = [v for v in (snapshot_row["open"], snapshot_row["close"], snapshot.get("pre_close"), last_close) if v is not None]
        snapshot_row["low"] = min(candidates) if candidates else snapshot_row["close"]

    base = base.loc[base["trade_date"] != snapshot["trade_date"]].copy()
    base = pd.concat([base, pd.DataFrame([snapshot_row])], ignore_index=True)
    base["_sort_trade_date"] = pd.to_datetime(base["trade_date"], errors="coerce")
    base = base.sort_values("_sort_trade_date").drop(columns="_sort_trade_date").reset_index(drop=True)

    prepared = _prepare_watch_df(base)
    for col in ("ma5", "ma10", "ma20", "ma60", "volume_ma5"):
        if col in prepared.columns:
            base[col] = prepared[col]

    return base

def _extract_watch_conclusion(payload: Dict[str, Any]) -> str:
    detail = payload.get("detail") or {}
    decision = detail.get("decision") or {}
    conclusion = str(decision.get("summary") or "").strip()
    if conclusion:
        return conclusion

    summary = str(payload.get("summary") or "").strip()
    if summary:
        for segment in summary.split("|"):
            cleaned = segment.strip()
            if cleaned.startswith("【结论】"):
                return cleaned.replace("【结论】", "", 1).strip()
        return summary

    suggestion = str(payload.get("suggestion") or "").strip()
    return f"建议 {suggestion}" if suggestion else "暂无结论"


def _compact_watch_analysis(payload: Dict[str, Any]) -> Dict[str, Any]:
    detail = payload.get("detail") or {}
    return {
        "conclusion": _extract_watch_conclusion(payload),
        "summary": payload.get("summary", ""),
        "suggestion": payload.get("suggestion", "观望"),
        "detail": {
            "_compact": True,
            "decision": detail.get("decision") or {},
            "trade_plan": detail.get("trade_plan") or {},
            "key_levels": (detail.get("key_levels") or [])[:4],
            "action_signal": detail.get("action_signal") or {},
            "signal_reasons": (detail.get("signal_reasons") or [])[:3],
            "intraday_context": detail.get("intraday_context") or {},
            "technical": detail.get("technical") or {},
        },
    }


def _extract_watch_technical_metric(payload: Dict[str, Any], key: str, digits: int = 2) -> Optional[float]:
    detail = payload.get("detail") or {}
    technical = detail.get("technical") or {}
    value = _safe_float(technical.get(key))
    if value is None:
        return None
    return round(float(value), digits)

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


def _parse_factor_payload(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _expand_watch_factor_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "factors" not in df.columns:
        return df

    work = df.copy()
    factor_rows = work["factors"].apply(_parse_factor_payload)
    factor_fields = (
        "ma5",
        "ma10",
        "ma20",
        "ma60",
        "vol_ma5",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "trend_score",
        "flow_score",
        "factor_score",
        "quality_score",
        "value_score",
        "event_score",
        "big_order_ratio",
        "net_mf_ratio",
        "rps_20",
        "rps_50",
        "rps_120",
        "rps_250",
    )

    for field in factor_fields:
        values = factor_rows.apply(lambda item: _safe_float(item.get(field)) if isinstance(item, dict) else None)
        if field in work.columns:
            existing = pd.to_numeric(work[field], errors="coerce")
            work[field] = existing.where(existing.notna(), values)
        else:
            work[field] = values

    if "volume_ma5" not in work.columns or work["volume_ma5"].isna().all():
        work["volume_ma5"] = pd.to_numeric(work.get("vol_ma5"), errors="coerce")
    else:
        current = pd.to_numeric(work["volume_ma5"], errors="coerce")
        fallback = pd.to_numeric(work.get("vol_ma5"), errors="coerce")
        work["volume_ma5"] = current.where(current.notna(), fallback)

    return work


def _fetch_watch_history_map(ts_codes: list[str], limit: int = 75) -> dict[str, pd.DataFrame]:
    codes = [_normalize_ts_code(code) for code in (ts_codes or []) if _normalize_ts_code(code)]
    if not codes:
        return {}

    placeholders = ",".join(["?"] * len(codes))
    df = fetch_df(
        f"""
        SELECT *
        FROM (
            SELECT
                d.ts_code,
                d.trade_date,
                d.open,
                d.high,
                d.low,
                d.close,
                d.pre_close,
                d.vol,
                d.amount,
                d.pct_chg,
                d.factors,
                COALESCE(m.net_mf_amount, 0) AS net_mf_amount,
                m.net_mf_ratio,
                ROW_NUMBER() OVER (PARTITION BY d.ts_code ORDER BY d.trade_date DESC) AS rn
            FROM daily_price d
            LEFT JOIN stock_moneyflow m
              ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
            WHERE d.ts_code IN ({placeholders})
        ) ranked
        WHERE rn <= ?
        ORDER BY ts_code, trade_date
        """,
        (*codes, max(20, int(limit))),
    )
    if df.empty:
        return {}

    df = _expand_watch_factor_columns(df)
    history_map: dict[str, pd.DataFrame] = {}
    for raw_code, group in df.groupby("ts_code", sort=False):
        ts_code = _normalize_ts_code(raw_code)
        if not ts_code:
            continue
        history = group.drop(columns=["rn"], errors="ignore").sort_values("trade_date").reset_index(drop=True)
        history_map[ts_code] = _prepare_watch_df(history)
    return history_map


def _build_compact_watch_analysis(
    ts_code: str,
    history_df: Optional[pd.DataFrame],
    realtime_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if history_df is None or history_df.empty:
        return _compact_watch_analysis(_empty_watch_analysis(include_detail=False))

    work = history_df.copy()
    if realtime_snapshot:
        work = _merge_live_snapshot_into_df(work, realtime_snapshot)
    work = _prepare_watch_df(work)
    merged = _sanitize_json_value(work.iloc[-1].to_dict()) if not work.empty else {}
    close = _safe_float(merged.get("close"))
    if close is None or close <= 0:
        return _compact_watch_analysis(_empty_watch_analysis(include_detail=False))

    open_price = _safe_float(merged.get("open"), close) or close
    high_price = _safe_float(merged.get("high"), max(open_price, close)) or max(open_price, close)
    low_price = _safe_float(merged.get("low"), min(open_price, close)) or min(open_price, close)
    pre_close = _safe_float(merged.get("pre_close"), close) or close
    pct_today = _safe_float(merged.get("pct_chg"))
    if pct_today is None and pre_close:
        pct_today = (close - pre_close) / pre_close * 100.0
    pct_today = pct_today or 0.0

    volume = _safe_float(merged.get("volume") or merged.get("vol"), 0.0) or 0.0
    amount = _safe_float(merged.get("amount"), 0.0) or 0.0
    ma5 = _safe_float(merged.get("ma5"), close) or close
    ma10 = _safe_float(merged.get("ma10"), close) or close
    ma20 = _safe_float(merged.get("ma20"), close) or close
    ma60 = _safe_float(merged.get("ma60"), close) or close
    volume_ma5 = _safe_float(merged.get("volume_ma5") or merged.get("vol_ma5"), 0.0) or 0.0
    volume_ratio = _safe_float(merged.get("volume_ratio"))
    if volume_ratio is None and volume_ma5 > 0 and volume > 0:
        volume_ratio = volume / volume_ma5
    if volume_ratio is None:
        volume_ratio = 1.0
    turnover = _safe_float(merged.get("turnover_rate") or merged.get("turnover_rate_f"))
    net_mf_amount = _safe_float(merged.get("net_mf_amount"), 0.0) or 0.0
    net_mf_ratio = _safe_float(merged.get("net_mf_ratio"))
    factor_score = _safe_float(merged.get("factor_score"))
    trend_factor = _safe_float(merged.get("trend_score"))
    flow_factor = _safe_float(merged.get("flow_score"))
    quality_factor = _safe_float(merged.get("quality_score"))
    big_order_ratio = _safe_float(merged.get("big_order_ratio"))

    from etl.utils.kline_patterns import build_structural_price_levels

    level_bundle = build_structural_price_levels(work, top_n=2)
    support_levels = list(level_bundle.get("support_levels") or [])
    resistance_levels = list(level_bundle.get("resistance_levels") or [])
    level_gap = _safe_float(level_bundle.get("selection_gap"), close * 0.015) or (close * 0.015)

    support_1 = support_levels[0]["price"] if support_levels else round(max(0.01, close - level_gap), 2)
    support_2 = support_levels[1]["price"] if len(support_levels) > 1 else round(max(0.01, support_1 - level_gap), 2)
    resistance_1 = resistance_levels[0]["price"] if resistance_levels else round(close + level_gap, 2)
    resistance_2 = resistance_levels[1]["price"] if len(resistance_levels) > 1 else round(resistance_1 + level_gap, 2)

    dist_support = round(max(0.0, (close - support_1) / close * 100.0), 2) if close else None
    dist_resistance = round(max(0.0, (resistance_1 - close) / close * 100.0), 2) if close else None
    near_band_pct = min(2.0, max(1.0, level_gap / close * 100.0)) if close else 1.2
    near_support = dist_support is not None and dist_support <= near_band_pct
    near_resistance = dist_resistance is not None and dist_resistance <= near_band_pct
    if near_support:
        zone_label = "支撑附近"
    elif near_resistance:
        zone_label = "压力附近"
    else:
        zone_label = "区间中段"

    score = 50.0
    if close >= ma20:
        score += 8
    else:
        score -= 8
    if close >= ma60:
        score += 6
    else:
        score -= 4
    if close >= ma5:
        score += 4
    else:
        score -= 4
    if pct_today >= 2:
        score += 6
    elif pct_today <= -2:
        score -= 8
    if volume_ratio >= 1.5:
        score += 8
    elif volume_ratio <= 0.85:
        score -= 5
    if turnover is not None:
        if turnover >= 8:
            score += 4
        elif turnover <= 2:
            score -= 2
    if net_mf_amount > 0:
        score += 6
    elif net_mf_amount < 0:
        score -= 6
    if factor_score is not None:
        if factor_score >= 65:
            score += 8
        elif factor_score <= 40:
            score -= 8
    if near_support:
        score += 3
    if near_resistance:
        score -= 3
    score = max(8.0, min(92.0, score))

    if score >= 78 and volume_ratio >= 1.5 and pct_today >= 2 and net_mf_amount > 0:
        action = "主动进攻"
        signal_color = "buy"
        signal_label = "红色买入"
    elif score >= 64:
        action = "试错" if near_support else "关注"
        signal_color = "buy"
        signal_label = "红色买入"
    elif score <= 38 or (close < ma20 and net_mf_amount < 0):
        action = "减仓"
        signal_color = "sell"
        signal_label = "绿色卖出"
    else:
        action = "观望"
        signal_color = "watch"
        signal_label = "白色观望"

    signal_reasons: list[dict[str, Any]] = []

    def add_reason(kind: str, title: str, desc: str, weight: int) -> None:
        text = str(desc or "").strip()
        if not text:
            return
        signal_reasons.append({
            "kind": kind,
            "title": title,
            "desc": text,
            "weight": weight,
        })

    if close > ma20 > ma60:
        add_reason("buy", "趋势结构偏强", f"现价站上 MA20/MA60，上行结构还在。", 10)
    elif close < ma20 < ma60:
        add_reason("sell", "趋势结构偏弱", f"现价位于 MA20/MA60 下方，中期结构仍弱。", 10)
    else:
        add_reason("watch", "趋势仍待确认", "价格与中期均线未形成同向共振。", 6)

    if volume_ratio >= 1.5:
        add_reason("buy", "量比明显放大", f"当前量比 {volume_ratio:.2f}，增量资金活跃度更高。", 9)
    elif volume_ratio <= 0.85:
        add_reason("watch", "量能偏弱", f"当前量比 {volume_ratio:.2f}，突破确认度不足。", 7)

    if net_mf_amount > 0:
        add_reason("buy", "主力承接偏强", f"主力净流入 {net_mf_amount:.2f} 万元。", 8)
    elif net_mf_amount < 0:
        add_reason("sell", "主力承接偏弱", f"主力净流出 {abs(net_mf_amount):.2f} 万元。", 8)

    if factor_score is not None:
        if factor_score >= 65:
            add_reason("buy", "综合因子占优", f"综合因子分 {factor_score:.1f}。", 7)
        elif factor_score <= 40:
            add_reason("sell", "综合因子偏弱", f"综合因子分 {factor_score:.1f}。", 7)

    if near_support:
        add_reason("watch" if signal_color == "watch" else signal_color, "位置靠近支撑", f"现价距支撑1 {support_1:.2f} 仅 {dist_support:.2f}%。", 7)
    elif near_resistance:
        add_reason("sell" if signal_color == "sell" else "watch", "位置逼近压力", f"现价距压力1 {resistance_1:.2f} 仅 {dist_resistance:.2f}%。", 7)
    else:
        add_reason("watch", "位置处于中段", f"当前处于支撑 {support_1:.2f} 与压力 {resistance_1:.2f} 之间。", 5)

    signal_reasons = sorted(signal_reasons, key=lambda item: (-item["weight"], item["title"]))
    signal_reasons = [{k: v for k, v in item.items() if k != "weight"} for item in signal_reasons[:3]]

    snapshot_text = (
        f"{merged.get('trade_date') or _today_trade_date()} {merged.get('quote_time') or ''}".strip()
        if realtime_snapshot else
        f"{merged.get('trade_date') or '-'} 收盘快照"
    )
    if signal_color == "buy":
        current_action_text = action
        entry_text = f"回踩 {support_1:.2f} 不破可跟踪；或放量站上 {resistance_1:.2f} 再确认。"
        invalid_text = f"跌破 {support_1:.2f} 且承接不足，买点失效。"
        reduce_text = f"逼近 {resistance_1:.2f} 仍无放量时，不追高。"
    elif signal_color == "sell":
        current_action_text = action
        entry_text = f"只有重新放量站回 {resistance_1:.2f} 上方，才考虑撤销防守。"
        invalid_text = f"继续跌破 {support_1:.2f}，弱势延续。"
        reduce_text = f"靠近 {resistance_1:.2f} 但不能突破时，优先减仓。"
    else:
        current_action_text = action
        entry_text = f"靠近 {support_1:.2f} 看承接，或放量突破 {resistance_1:.2f} 再跟。"
        invalid_text = f"若跌破 {support_1:.2f}，则转为防守；若冲高不过 {resistance_1:.2f}，继续等。"
        reduce_text = f"未放量前靠近 {resistance_1:.2f} 不追价。"

    def build_level_entry(label: str, price: float, base_note: str, trigger: str) -> Dict[str, Any]:
        note = str(base_note or "").strip()
        if note:
            note = f"{note} 操作：{trigger}"
        else:
            note = trigger
        return {"label": label, "price": price, "note": note, "trigger": trigger}

    key_levels = [
        build_level_entry(
            "支撑1",
            support_1,
            support_levels[0].get("note", "") if support_levels else "首要防守位，跌破则先看弱。",
            f"回踩 {support_1:.2f} 不破再看承接。",
        ),
        build_level_entry(
            "支撑2",
            support_2,
            support_levels[1].get("note", "") if len(support_levels) > 1 else "次级缓冲位，失守说明结构继续转弱。",
            f"仅在 {support_2:.2f} 附近止跌时考虑二次观察。",
        ),
        build_level_entry(
            "压力1",
            resistance_1,
            resistance_levels[0].get("note", "") if resistance_levels else "首个突破确认位，放量站上才算有效。",
            f"放量站上 {resistance_1:.2f} 才算压力化解。",
        ),
        build_level_entry(
            "压力2",
            resistance_2,
            resistance_levels[1].get("note", "") if len(resistance_levels) > 1 else "上方第二道抛压位。",
            f"逼近 {resistance_2:.2f} 时看是否继续放量。",
        ),
    ]

    detail = {
        "_compact": True,
        "decision": {
            "score": round(score, 1),
            "bias": "bullish" if signal_color == "buy" else "bearish" if signal_color == "sell" else "neutral",
            "action": action,
            "confidence": "high" if score >= 72 or score <= 32 else "medium",
            "style": "realtime_compact",
            "summary": "",
        },
        "trade_plan": {
            "current": current_action_text,
            "entry": entry_text,
            "add": f"只有站稳 {resistance_1:.2f} 且量比维持在 1.2 以上，再考虑加仓。",
            "reduce": reduce_text,
            "invalid": invalid_text,
            "position": "轻仓试探" if signal_color == "buy" else "控制仓位" if signal_color == "sell" else "维持观察",
        },
        "action_signal": {
            "color": signal_color,
            "label": signal_label,
            "headline": current_action_text,
            "zone": zone_label,
            "trigger": entry_text,
            "fallback": invalid_text,
            "snapshot": snapshot_text,
        },
        "signal_reasons": signal_reasons,
        "intraday_context": {
            "mode": "realtime" if realtime_snapshot else "static",
            "quote_time": merged.get("quote_time"),
            "snapshot": snapshot_text,
            "zone": zone_label,
            "distance_to_support_1": dist_support,
            "distance_to_resistance_1": dist_resistance,
            "status": current_action_text,
        },
        "key_levels": key_levels,
        "technical": {
            "close": round(close, 2),
            "open": round(open_price, 2),
            "pre_close": round(pre_close, 2),
            "pct_today": round(pct_today, 2),
            "volume": volume,
            "volume_ratio": round(volume_ratio, 2),
            "turnover": round(turnover, 2) if turnover is not None else None,
            "amount": amount,
            "net_mf_amount": round(net_mf_amount, 2),
            "net_mf_ratio": round(net_mf_ratio, 2) if net_mf_ratio is not None else None,
            "big_order_ratio": round(big_order_ratio, 2) if big_order_ratio is not None else None,
            "ma5": round(ma5, 2),
            "ma10": round(ma10, 2),
            "ma20": round(ma20, 2),
            "ma60": round(ma60, 2),
            "factor_score": round(factor_score, 1) if factor_score is not None else None,
            "trend_factor": round(trend_factor, 1) if trend_factor is not None else None,
            "flow_factor": round(flow_factor, 1) if flow_factor is not None else None,
            "quality_factor": round(quality_factor, 1) if quality_factor is not None else None,
        },
    }

    return {
        "conclusion": detail["decision"]["summary"].strip(),
        "summary": f"【结论】{detail['decision']['summary'].strip()} | 【动作】{current_action_text}",
        "suggestion": action,
        "detail": detail,
    }

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

def _build_watch_analysis(
    ts_code: str,
    realtime_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """为自选股生成结构化分析结果。"""
    try:
        df = fetch_df(
            """
            SELECT d.trade_date, d.open, d.high, d.low, d.close, d.vol, d.amount, d.pct_chg, d.factors,
                   COALESCE(m.net_mf_amount, 0) AS net_mf_amount,
                   m.net_mf_ratio,
                   g.rzye,
                   g.rzmre,
                   g.rzche
            FROM daily_price d
            LEFT JOIN stock_moneyflow m
              ON d.ts_code = m.ts_code AND d.trade_date = m.trade_date
            LEFT JOIN stock_margin g
              ON d.ts_code = g.ts_code AND d.trade_date = g.trade_date
            WHERE d.ts_code = ?
            ORDER BY d.trade_date DESC
            LIMIT 75
            """,
            (ts_code,),
        )
        if df.empty:
            return _empty_watch_analysis(include_detail=True)

        df = df.iloc[::-1].reset_index(drop=True)
        df = _expand_watch_factor_columns(df)
        latest_trade_date = df.iloc[-1]["trade_date"] if not df.empty else None
        live_snapshot = realtime_snapshot or _fetch_live_snapshot(ts_code, latest_trade_date=latest_trade_date)
        if live_snapshot:
            df = _merge_live_snapshot_into_df(df, live_snapshot)

        if len(df) < 20:
            return _empty_watch_analysis(include_detail=True)

        df = _prepare_watch_df(df)

        from etl.utils.kline_patterns import PatternRecognizer, get_professional_commentary_detailed

        latest_recognizer = PatternRecognizer(df)
        latest_patterns = latest_recognizer.recognize()
        latest_detail = get_professional_commentary_detailed(
            df,
            latest_patterns,
            context={"ts_code": ts_code, "realtime_snapshot": live_snapshot},
        )

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

def _get_watch_analysis(
    ts_code: str,
    force_refresh: bool = False,
    realtime_snapshot: Optional[Dict[str, Any]] = None,
    allow_live_fetch: bool = True,
) -> Dict[str, Any]:
    live_snapshot = realtime_snapshot
    if live_snapshot is None and allow_live_fetch:
        live_snapshot = _fetch_live_snapshot(ts_code)

    cache_key = ts_code
    snapshot_trade_date = _normalize_trade_date(
        live_snapshot.get("trade_date") if live_snapshot else None
    )
    cache_ttl = _ANALYSIS_CACHE_TTL_SECONDS
    if snapshot_trade_date:
        cache_key = f"{ts_code}@{snapshot_trade_date}"
        cache_ttl = 45
        quote_time = str(live_snapshot.get("quote_time") or "").strip() if live_snapshot else ""
        live_price = _safe_float(live_snapshot.get("close")) if live_snapshot else None
        if quote_time:
            cache_key = f"{cache_key}@{quote_time}"
        elif live_price is not None:
            cache_key = f"{cache_key}@{live_price:.2f}"

    now = time.time()

    with _ANALYSIS_CACHE_LOCK:
        cached = _ANALYSIS_CACHE.get(cache_key)
        if (
            cached
            and not force_refresh
            and now - cached[0] < cache_ttl
        ):
            _ANALYSIS_CACHE.move_to_end(cache_key)
            return cached[1]

    analysis = _build_watch_analysis(ts_code, realtime_snapshot=live_snapshot)

    with _ANALYSIS_CACHE_LOCK:
        _ANALYSIS_CACHE[cache_key] = (now, analysis)
        _ANALYSIS_CACHE.move_to_end(cache_key)
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


class HoldingBatchItem(BaseModel):
    ts_code: str
    shares: float = Field(gt=0)
    avg_cost: Optional[float] = Field(default=None, ge=0)
    name: Optional[str] = None


class HoldingsBatchUpdateRequest(BaseModel):
    items: list[HoldingBatchItem] = Field(default_factory=list)
    replace_missing: bool = False
    sync_watchlist: bool = True

# ========== 自选股管理 ==========

def _fetch_user_watchlist_df(user_id: int) -> pd.DataFrame:
    return fetch_df(
        """
        SELECT ts_code, name, remark, sort_order, created_at
        FROM watchlist
        WHERE user_id = ?
        ORDER BY sort_order, created_at DESC
        """,
        (user_id,),
    )


def _fetch_user_watchlist_codes(user_id: int) -> list[str]:
    df = fetch_df(
        """
        SELECT ts_code
        FROM watchlist
        WHERE user_id = ?
        ORDER BY sort_order, created_at DESC
        """,
        (user_id,),
    )
    if df.empty:
        return []
    return [_normalize_ts_code(code) for code in df["ts_code"].tolist() if code]


def _ensure_watchlist_membership(user_id: int, ts_code: str) -> None:
    with get_db_connection() as con:
        row = con.execute(
            "SELECT 1 FROM watchlist WHERE user_id = ? AND ts_code = ?",
            (user_id, ts_code),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="当前用户自选中不存在该股票")


def _load_user_ai_config(
    user_id: int,
    provider: Optional[str] = None,
) -> tuple[str, Optional[str], Optional[str], Optional[str], Optional[int]]:
    target_provider = str(provider or "").strip().lower()
    with get_db_connection() as con:
        if target_provider:
            provider_row = con.execute(
                """
                SELECT model_name, api_key, base_url, max_tokens
                FROM user_ai_provider_configs
                WHERE user_id = ? AND provider = ?
                """,
                (user_id, target_provider),
            ).fetchone()
            if provider_row and provider_row[1]:
                return (
                    target_provider,
                    provider_row[0],
                    provider_row[1],
                    provider_row[2],
                    provider_row[3],
                )

        row = con.execute(
            """
            SELECT model_provider, model_name, api_key, base_url, max_tokens
            FROM user_ai_config
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()

    if target_provider:
        legacy_provider = str(row[0] or "openai").lower() if row else ""
        if row and legacy_provider == target_provider and row[2]:
            return target_provider, row[1], row[2], row[3], row[4]
        raise HTTPException(
            status_code=400,
            detail=f"请先在设置中配置 {target_provider.upper()} 的 API Key，持仓截图识别固定使用 OpenAI 模型。",
        )

    if not row or not row[2]:
        raise HTTPException(status_code=400, detail="请先在设置中配置可用的 AI API Key")
    return row[0] or "openai", row[1], row[2], row[3], row[4]


def _extract_json_payload(raw_text: str) -> Any:
    text = str(raw_text or "").strip()
    if not text:
        raise HTTPException(status_code=502, detail="图片识别返回为空")

    fenced = re.search(r"```(?:json)?\s*(.*?)```", text, re.S | re.I)
    candidates = [fenced.group(1).strip()] if fenced else []
    candidates.append(text)

    object_match = re.search(r"\{.*\}", text, re.S)
    if object_match:
        candidates.append(object_match.group(0).strip())

    for candidate in candidates:
        if not candidate:
            continue
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            continue

    raise HTTPException(status_code=502, detail="图片识别结果不是有效 JSON")


def _extract_text_from_ai_content(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()

    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            text = _extract_text_from_ai_content(item)
            if text:
                parts.append(text)
        return "\n".join(parts).strip()

    if isinstance(content, dict):
        text_value = content.get("text")
        if isinstance(text_value, str) and text_value.strip():
            return text_value.strip()
        if isinstance(text_value, dict):
            nested_value = text_value.get("value")
            if isinstance(nested_value, str) and nested_value.strip():
                return nested_value.strip()

        for key in ("content", "output_text", "value"):
            nested = content.get(key)
            if nested is None:
                continue
            text = _extract_text_from_ai_content(nested)
            if text:
                return text

        function_payload = content.get("function")
        if isinstance(function_payload, dict):
            arguments = function_payload.get("arguments")
            if isinstance(arguments, str) and arguments.strip():
                return arguments.strip()

    return ""


def _extract_ai_response_text(result: dict[str, Any]) -> str:
    choices = result.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue

            message = choice.get("message")
            if isinstance(message, dict):
                content = _extract_text_from_ai_content(message.get("content"))
                if content:
                    return content

                for tool_call in message.get("tool_calls") or []:
                    if not isinstance(tool_call, dict):
                        continue
                    function_payload = tool_call.get("function")
                    if not isinstance(function_payload, dict):
                        continue
                    arguments = function_payload.get("arguments")
                    if isinstance(arguments, str) and arguments.strip():
                        return arguments.strip()

            choice_text = _extract_text_from_ai_content(choice.get("text"))
            if choice_text:
                return choice_text

    output_text = _extract_text_from_ai_content(result.get("output_text"))
    if output_text:
        return output_text

    output = result.get("output")
    if isinstance(output, list):
        for item in output:
            if not isinstance(item, dict):
                continue
            content = _extract_text_from_ai_content(item.get("content"))
            if content:
                return content

    return ""


def _extract_text_from_chunk_delta(delta: Any) -> str:
    if not isinstance(delta, dict):
        return ""

    content = _extract_text_from_ai_content(delta.get("content"))
    if content:
        return content

    for key in ("reasoning_content", "reasoning", "text"):
        value = delta.get(key)
        text = _extract_text_from_ai_content(value)
        if text:
            return text

    tool_calls = delta.get("tool_calls")
    if isinstance(tool_calls, list):
        parts: list[str] = []
        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                continue
            function_payload = tool_call.get("function")
            if not isinstance(function_payload, dict):
                continue
            arguments = function_payload.get("arguments")
            if isinstance(arguments, str) and arguments:
                parts.append(arguments)
        if parts:
            return "".join(parts)

    return ""


def _extract_ai_response_text_from_sse(raw_text: str) -> str:
    chunks: list[str] = []
    fallback_payloads: list[str] = []

    for raw_line in str(raw_text or "").splitlines():
        line = raw_line.strip()
        if not line or not line.startswith("data:"):
            continue

        data = line[5:].strip()
        if not data or data == "[DONE]":
            continue

        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            continue

        if not isinstance(payload, dict):
            continue

        direct_text = _extract_ai_response_text(payload)
        if direct_text:
            fallback_payloads.append(direct_text)

        choices = payload.get("choices")
        if not isinstance(choices, list):
            continue

        for choice in choices:
            if not isinstance(choice, dict):
                continue
            delta = choice.get("delta")
            chunk_text = _extract_text_from_chunk_delta(delta)
            if chunk_text:
                chunks.append(chunk_text)

    if chunks:
        return "".join(chunks).strip()
    if fallback_payloads:
        return "\n".join(part for part in fallback_payloads if part).strip()
    return ""


async def _parse_holdings_with_ai_vision(
    user_id: int,
    content: bytes,
    content_type: str,
) -> dict[str, Any]:
    _, model_name, api_key, base_url, max_tokens = _load_user_ai_config(user_id, provider="openai")
    provider = "openai"
    if not base_url:
        base_url = "https://api.openai.com/v1"
    base_url = str(base_url).rstrip("/")
    model = model_name or "gpt-4.1-mini"

    data_uri = f"data:{content_type};base64,{base64.b64encode(content).decode('utf-8')}"
    prompt = (
        "请识别这张券商持仓截图里的 A 股持仓明细，只返回 JSON 对象，不要输出 Markdown 或解释。\n"
        "格式固定为：\n"
        "{\n"
        '  "holdings": [\n'
        '    {"ts_code": "600519", "name": "贵州茅台", "shares": 100, "avg_cost": 1688.88}\n'
        "  ],\n"
        '  "notes": ["无法确认的内容"]\n'
        "}\n"
        "要求：\n"
        "1. 只保留当前持仓股票，不要包含总资产、可用资金、盈亏汇总、现金、基金或港美股。\n"
        "2. ts_code 尽量输出 6 位数字；如果看不到代码，可仅填 name。\n"
        "3. shares 必须是数字股数；avg_cost 看不清可填 null。\n"
        "4. 无法确认的行不要猜，写入 notes。\n"
        "5. 如果图片不是持仓页，返回空 holdings，并在 notes 里说明。"
    )

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "你是 A 股券商持仓截图结构化助手，只输出严格 JSON。",
            },
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": data_uri}},
                ],
            },
        ],
        "max_tokens": max(500, min(int(max_tokens or 1200), 1200)),
        "temperature": 0,
        "stream": False,
        "response_format": {"type": "json_object"},
    }

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                f"{base_url}/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
            )
    except httpx.HTTPError as exc:
        logger.error("持仓图片 %s 调用异常: %s", provider, exc)
        raise HTTPException(
            status_code=502,
            detail=f"{provider.upper()} 图片识别调用失败: {type(exc).__name__}",
        ) from exc
    if resp.status_code != 200:
        logger.error("持仓图片 AI 兜底识别失败: %s", resp.text)
        upstream_message = resp.text
        try:
            error_payload = resp.json()
        except json.JSONDecodeError:
            error_payload = None
        if isinstance(error_payload, dict) and isinstance(error_payload.get("error"), dict):
            upstream_message = str(error_payload["error"].get("message") or upstream_message).strip()
        invalid_image_markers = (
            "does not represent a valid image",
            "invalid image",
            "image data",
            "invalid_value",
        )
        if any(marker in upstream_message.lower() for marker in invalid_image_markers):
            raise HTTPException(
                status_code=400,
                detail=(
                    f"当前 OpenAI 图片识别所用 base_url={base_url} 返回无效图片错误。"
                    "这通常不是截图本身坏了，而是当前代理/兼容网关不支持 base64 图片输入。"
                    "请在设置里把 OpenAI base_url 改成官方 OpenAI，或改成明确支持多模态图片输入的兼容端点。"
                ),
            )
        raise HTTPException(status_code=502, detail=f"AI 图片识别调用失败: {upstream_message}")

    raw_body = resp.text or ""
    content_type = str(resp.headers.get("content-type") or "").lower()
    if "text/event-stream" in content_type or raw_body.lstrip().startswith("data:"):
        raw_content = _extract_ai_response_text_from_sse(raw_body)
        if not raw_content:
            body_preview = raw_body.strip()[:500]
            logger.error(
                "持仓图片 AI 返回 SSE 但未提取到内容: content_type=%s body=%s",
                resp.headers.get("content-type"),
                body_preview,
            )
            raise HTTPException(
                status_code=502,
                detail="AI 图片识别返回了流式响应，但未提取到有效内容，请检查当前 OpenAI 模型或代理是否支持图像输入。",
            )
        parsed = _extract_json_payload(raw_content)
        return {
            "backend": f"ai_multimodal_{provider}",
            "holdings": parsed.get("holdings") if isinstance(parsed, dict) else [],
            "notes": parsed.get("notes") if isinstance(parsed, dict) else [],
        }

    try:
        result = resp.json()
    except json.JSONDecodeError as exc:
        body_preview = raw_body.strip()[:500]
        logger.error(
            "持仓图片 AI 返回非 JSON: content_type=%s body=%s",
            resp.headers.get("content-type"),
            body_preview,
        )
        raise HTTPException(
            status_code=502,
            detail="AI 图片识别返回了非 JSON 响应，请检查当前 OpenAI 模型或代理是否支持图像输入。",
        ) from exc

    if isinstance(result, dict) and isinstance(result.get("error"), dict):
        error_message = str(result["error"].get("message") or "上游模型返回错误").strip()
        raise HTTPException(status_code=502, detail=f"AI 图片识别调用失败: {error_message}")

    raw_content = _extract_ai_response_text(result)
    if not raw_content:
        logger.error(
            "持仓图片 AI 返回空内容: keys=%s body=%s",
            list(result.keys()) if isinstance(result, dict) else type(result).__name__,
            json.dumps(result, ensure_ascii=False)[:800] if isinstance(result, dict) else str(result)[:800],
        )
        raise HTTPException(
            status_code=502,
            detail="AI 图片识别返回空内容，请检查当前 OpenAI 模型或代理是否支持图像输入。",
        )

    parsed = _extract_json_payload(raw_content)
    return {
        "backend": f"ai_multimodal_{provider}",
        "holdings": parsed.get("holdings") if isinstance(parsed, dict) else [],
        "notes": parsed.get("notes") if isinstance(parsed, dict) else [],
    }


def _resolve_stock_identity(raw_code: Any = None, raw_name: Any = None) -> tuple[Optional[str], Optional[str], str]:
    lookup = _load_stock_basic_lookup()

    for symbol in _extract_stock_symbol_candidates(raw_code, raw_name):
        norm_code = _normalize_ts_code(symbol)
        record = lookup["by_ts_code"].get(norm_code) if norm_code else None
        if record:
            return str(record["ts_code"]), str(record["name"]), "ts_code"

        record = lookup["by_symbol"].get(symbol)
        if record:
            return str(record["ts_code"]), str(record["name"]), "symbol"

    raw_name_text = str(raw_name or "").strip()
    name_candidates = _build_stock_name_candidates(raw_name)
    for candidate in name_candidates:
        record = _pick_unique_stock_record(lookup["by_exact_name"].get(candidate))
        if record:
            return str(record["ts_code"]), str(record["name"]), "name"

    for candidate in name_candidates:
        norm_key = _normalize_stock_name_key(candidate)
        if not norm_key:
            continue
        record = _pick_unique_stock_record(lookup["by_norm_name"].get(norm_key))
        if record:
            return str(record["ts_code"]), str(record["name"]), "name_normalized"

    for candidate in name_candidates:
        norm_key = _normalize_stock_name_key(candidate)
        if len(norm_key) < 3:
            continue
        fuzzy_matches = [
            row
            for row in lookup["rows"]
            if row.get("norm_name") and (norm_key in row["norm_name"] or row["norm_name"] in norm_key)
        ]
        record = _pick_unique_stock_record(fuzzy_matches)
        if record:
            return str(record["ts_code"]), str(record["name"]), "name_fuzzy"

    ascii_name = _normalize_lookup_text(raw_name)
    if ascii_name and ascii_name.isascii():
        record = _pick_unique_stock_record(lookup["by_pinyin"].get(ascii_name))
        if record:
            return str(record["ts_code"]), str(record["name"]), "pinyin"
        record = _pick_unique_stock_record(lookup["by_pinyin_abbr"].get(ascii_name))
        if record:
            return str(record["ts_code"]), str(record["name"]), "pinyin_abbr"

    return None, raw_name_text or None, "unmatched"


def _prepare_imported_holding_rows(raw_holdings: Any, raw_notes: Any = None) -> tuple[list[dict[str, Any]], list[str]]:
    items = raw_holdings if isinstance(raw_holdings, list) else []
    notes = [str(note).strip() for note in (raw_notes or []) if str(note).strip()]
    prepared_rows: list[dict[str, Any]] = []

    for idx, raw_item in enumerate(items):
        if not isinstance(raw_item, dict):
            notes.append(f"第 {idx + 1} 项不是有效对象，已跳过。")
            continue

        shares = _safe_float(raw_item.get("shares"), None)
        avg_cost = _safe_float(raw_item.get("avg_cost"), None)
        source_code = str(raw_item.get("ts_code") or raw_item.get("code") or raw_item.get("symbol") or "").strip()
        source_name = str(raw_item.get("name") or raw_item.get("stock_name") or "").strip()

        if shares is None or shares <= 0:
            notes.append(f"{source_name or source_code or f'第 {idx + 1} 项'} 持仓数量无效，已跳过。")
            continue

        ts_code, resolved_name, matched_by = _resolve_stock_identity(source_code, source_name)
        status = "matched" if ts_code else "unmatched"
        warning = None
        if avg_cost is not None and avg_cost < 0:
            avg_cost = None
            warning = "成本价无效，已清空。"
        if status != "matched":
            warning = warning or "图片已识别，但该项未自动匹配到 stock_basic；应用前请手工确认代码。"

        prepared_rows.append({
            "ts_code": ts_code,
            "name": resolved_name or source_name or source_code or f"第 {idx + 1} 项",
            "shares": int(round(shares)),
            "avg_cost": round(float(avg_cost), 4) if avg_cost is not None else None,
            "source_code": source_code or None,
            "source_name": source_name or None,
            "matched_by": matched_by,
            "status": status,
            "warning": warning,
        })

    return prepared_rows, notes


def _sync_watchlist_entries(
    con,
    user_id: int,
    items: list[dict[str, Any]],
) -> int:
    valid_items = [item for item in items if item.get("ts_code")]
    if not valid_items:
        return 0

    existing_rows = con.execute(
        "SELECT ts_code FROM watchlist WHERE user_id = ?",
        (user_id,),
    ).fetchall()
    existing_codes = {str(row[0]) for row in existing_rows}
    current_max_sort = con.execute(
        "SELECT COALESCE(MAX(sort_order), 0) FROM watchlist WHERE user_id = ?",
        (user_id,),
    ).fetchone()[0] or 0

    insert_params: list[tuple[Any, ...]] = []
    for item in valid_items:
        ts_code = str(item["ts_code"])
        if ts_code in existing_codes:
            continue
        current_max_sort += 1
        insert_params.append(
            (user_id, ts_code, item.get("name") or ts_code, "持仓同步", current_max_sort)
        )
        existing_codes.add(ts_code)
    if insert_params:
        con.executemany(
            """
            INSERT INTO watchlist (user_id, ts_code, name, remark, sort_order)
            VALUES (?, ?, ?, ?, ?)
            """,
            insert_params,
        )

    return len(insert_params)


@router.get("/watchlist")
async def list_watchlist(request: Request):
    """获取自选股列表"""
    user_id = await get_current_user_id(request)
    try:
        df = _fetch_user_watchlist_df(user_id)
        records = [_sanitize_json_value(row) for row in df.to_dict("records")]
        return {"status": "success", "data": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/watchlist")
async def add_to_watchlist(stock: WatchlistStock, request: Request):
    """添加股票到自选"""
    user_id = await get_current_user_id(request)
    try:
        ts_code = _normalize_ts_code(stock.ts_code)
        if not ts_code:
            raise HTTPException(status_code=400, detail="无效股票代码")
        
        basic = fetch_df("SELECT name FROM stock_basic WHERE ts_code = ?", (ts_code,))
        if basic.empty:
            raise HTTPException(status_code=400, detail="股票代码不存在")
        if not stock.name:
            stock.name = basic.iloc[0]["name"]

        with get_db_connection() as con:
            con.execute(
                """
                INSERT OR REPLACE INTO watchlist (user_id, ts_code, name, remark, sort_order)
                VALUES (?, ?, ?, ?, COALESCE((SELECT MAX(sort_order) FROM watchlist WHERE user_id = ?), 0) + 1)
                """,
                (user_id, ts_code, stock.name, stock.remark, user_id)
            )
        return {"status": "success", "message": f"已添加 {ts_code}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/watchlist/{ts_code}")
async def remove_from_watchlist(ts_code: str, request: Request):
    """从自选删除股票"""
    user_id = await get_current_user_id(request)
    try:
        norm_code = _normalize_ts_code(ts_code)
        with get_db_connection() as con:
            con.execute(
                "DELETE FROM watchlist WHERE user_id = ? AND ts_code = ?",
                (user_id, norm_code),
            )
        return {"status": "success", "message": f"已从自选删除 {norm_code}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class WatchlistReorder(BaseModel):
    codes: list[str]

@router.put("/watchlist/reorder")
async def reorder_watchlist(body: WatchlistReorder, request: Request):
    """调整自选股排序"""
    user_id = await get_current_user_id(request)
    try:
        with get_db_connection() as con:
            for idx, code in enumerate(body.codes):
                norm = _normalize_ts_code(code)
                con.execute(
                    "UPDATE watchlist SET sort_order = ? WHERE user_id = ? AND ts_code = ?",
                    (idx + 1, user_id, norm),
                )
        return {"status": "success", "message": "排序已更新"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/watchlist/levels/backtest")
async def get_watchlist_level_backtest(
    board: str = "growth",
    codes: Optional[str] = None,
    sample_size: int = 60,
    lookback_days: int = 120,
    eval_days: int = 60,
    horizon: int = 7,
    include_legacy: bool = True,
):
    """
    Watchlist 点位回测诊断。
    默认聚焦创业板/科创板的高流动性样本，对比 adaptive 与 legacy 点位。
    """
    try:
        from etl.utils.kline_patterns import backtest_structural_price_levels

        target_codes = [
            _normalize_ts_code(code)
            for code in str(codes or "").split(",")
            if str(code).strip()
        ] if codes else None
        payload = backtest_structural_price_levels(
            board=board,
            sample_size=sample_size,
            lookback_days=lookback_days,
            eval_days=eval_days,
            horizon=horizon,
            include_legacy=include_legacy,
            target_codes=target_codes,
        )
        return {"status": "success", **payload}
    except Exception as e:
        logger.warning("Watchlist 点位回测失败: %s", e, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/watchlist/realtime")
async def get_watchlist_realtime(
    request: Request,
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

    user_id = await get_current_user_id(request)

    user_watchlist_codes = _fetch_user_watchlist_codes(user_id)

    if codes:
        raw_codes = [c.strip() for c in codes.split(",") if c.strip()]
        norm_codes = [_normalize_ts_code(c) for c in raw_codes]
        allowed_codes = set(user_watchlist_codes)
        norm_codes = [c for c in norm_codes if c and c in allowed_codes]
    else:
        norm_codes = [c for c in user_watchlist_codes if c]

    max_codes = 80 if analysis_depth == "compact" else 50
    if len(norm_codes) > max_codes:
        norm_codes = norm_codes[:max_codes]
        logger.warning(f"自选股数量超过{max_codes}只，已截断到{max_codes}只")

    if not norm_codes:
        return {"status": "success", "is_trading_time": False, "message": "自选股为空", "data": []}

    watchlist_df = _fetch_user_watchlist_df(user_id)
    watchlist_name_map: dict[str, str] = {}
    if not watchlist_df.empty:
        for _, row in watchlist_df.iterrows():
            watch_code = _normalize_ts_code(row.get("ts_code"))
            if watch_code:
                watch_name = _sanitize_json_value(row.get("name"))
                watchlist_name_map[watch_code] = watch_name or watch_code

    tradable_codes = set()
    basic_name_map: dict[str, str] = {}
    placeholders = ",".join(["?"] * len(norm_codes))
    basic_df = fetch_df(
        f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
        tuple(norm_codes),
    )
    if not basic_df.empty:
        for _, row in basic_df.iterrows():
            basic_code = _normalize_ts_code(row.get("ts_code"))
            if basic_code:
                tradable_codes.add(basic_code)
                basic_name = _sanitize_json_value(row.get("name"))
                basic_name_map[basic_code] = basic_name or basic_code

    quote_candidate_codes = [c for c in norm_codes if c in tradable_codes]
    display_name_map = {**watchlist_name_map, **basic_name_map}
    history_map = _fetch_watch_history_map(norm_codes) if analysis_depth == "compact" else {}

    is_trading = trading_calendar.is_trading_time()
    live_quote_day = trading_calendar.is_trading_day(arrow.now("Asia/Shanghai").date())
    rows = []
    snapshot_time = None

    if live_quote_day and quote_candidate_codes:
        quote_df = sync_engine.provider.realtime_quote(
            ts_code=",".join(quote_candidate_codes),
            src=src or "sina",
        )
        if quote_df is not None and not quote_df.empty:
            today_trade_date = _today_trade_date()
            for _, quote_row in quote_df.iterrows():
                snapshot = _extract_live_quote_snapshot(quote_row)
                ts_code = snapshot.get("ts_code") if snapshot else ""
                if not ts_code or snapshot.get("trade_date") != today_trade_date:
                    continue

                analyze_result = {}
                if include_analysis:
                    if analysis_depth == "compact":
                        analyze_result = _build_compact_watch_analysis(
                            ts_code,
                            history_map.get(ts_code),
                            realtime_snapshot=snapshot,
                        )
                    else:
                        full_analysis = _get_watch_analysis(
                            ts_code,
                            realtime_snapshot=snapshot,
                            allow_live_fetch=False,
                        )
                        analyze_result = full_analysis

                if snapshot.get("quote_time"):
                    snapshot_time = max(snapshot_time or snapshot["quote_time"], snapshot["quote_time"])

                rows.append(_sanitize_json_value({
                    "ts_code": ts_code,
                    "name": snapshot.get("name") or ts_code,
                    "trade_date": snapshot.get("trade_date"),
                    "quote_time": snapshot.get("quote_time"),
                    "price": snapshot.get("close"),
                    "pre_close": snapshot.get("pre_close"),
                    "pct": snapshot.get("pct"),
                    "vol": snapshot.get("volume_shares"),
                    "amount": snapshot.get("amount_yuan"),
                    "volume_ratio": _extract_watch_technical_metric(analyze_result, "volume_ratio"),
                    "turnover_rate": _extract_watch_technical_metric(analyze_result, "turnover"),
                    "analyze": analyze_result
                }))
    
    processed_codes = {r['ts_code'] for r in rows}
    remaining_codes = [c for c in norm_codes if c not in processed_codes]

    if remaining_codes and analysis_depth == "compact":
        for tc in remaining_codes:
            history_df = history_map.get(tc)
            if history_df is None or history_df.empty:
                continue
            latest_row = history_df.iloc[-1]
            analyze_result = (
                _build_compact_watch_analysis(tc, history_df, realtime_snapshot=None)
                if include_analysis else {}
            )
            rows.append(_sanitize_json_value({
                "ts_code": tc,
                "name": display_name_map.get(tc, tc),
                "trade_date": _normalize_trade_date(latest_row.get("trade_date")),
                "quote_time": None,
                "price": latest_row.get("close"),
                "pre_close": latest_row.get("pre_close"),
                "pct": latest_row.get("pct_chg"),
                "vol": latest_row.get("volume", latest_row.get("vol")),
                "amount": latest_row.get("amount"),
                "volume_ratio": _extract_watch_technical_metric(analyze_result, "volume_ratio"),
                "turnover_rate": _extract_watch_technical_metric(analyze_result, "turnover"),
                "analyze": analyze_result
            }))
    elif remaining_codes:
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
                full_analysis = _get_watch_analysis(tc, allow_live_fetch=False)
                analyze_result = (
                    full_analysis
                    if analysis_depth == "full"
                    else _compact_watch_analysis(full_analysis)
                )
            rows.append(_sanitize_json_value({
                "ts_code": tc,
                "name": display_name_map.get(tc) or name_map.get(tc, tc),
                "trade_date": _normalize_trade_date(row.get("trade_date")),
                "quote_time": None,
                "price": row['price'],
                "pre_close": row['pre_close'],
                "pct": row['pct'],
                "vol": row['vol'],
                "amount": row['amount'],
                "volume_ratio": _extract_watch_technical_metric(analyze_result, "volume_ratio"),
                "turnover_rate": _extract_watch_technical_metric(analyze_result, "turnover"),
                "analyze": analyze_result
            }))

    rendered_codes = {r["ts_code"] for r in rows}
    missing_codes = [c for c in norm_codes if c not in rendered_codes]
    for tc in missing_codes:
        if analysis_depth == "full":
            analyze_result = _empty_watch_analysis(include_detail=True) if include_analysis else {}
        else:
            analyze_result = (
                _compact_watch_analysis(_empty_watch_analysis(include_detail=False))
                if include_analysis
                else {}
            )
        rows.append(_sanitize_json_value({
            "ts_code": tc,
            "name": display_name_map.get(tc, tc),
            "trade_date": None,
            "quote_time": None,
            "price": None,
            "pre_close": None,
            "pct": None,
            "vol": None,
            "amount": None,
            "volume_ratio": _extract_watch_technical_metric(analyze_result, "volume_ratio"),
            "turnover_rate": _extract_watch_technical_metric(analyze_result, "turnover"),
            "analyze": analyze_result,
        }))

    idx_map = {c: i for i, c in enumerate(norm_codes)}
    rows.sort(key=lambda x: idx_map.get(x.get("ts_code"), 999))

    if rows and len(processed_codes) > 0:
        snapshot_label = _today_trade_date()
        if snapshot_time:
            snapshot_label = f"{snapshot_label} {snapshot_time}"
        message = (
            f"实时刷新中，已展示 {snapshot_label} 快照"
            if is_trading
            else f"已展示 {snapshot_label} 最新快照"
        )
    elif live_quote_day:
        message = "实时行情暂不可用，已回退最近收盘数据"
    else:
        message = "非交易时段，已展示最近收盘数据"

    return {
        "status": "success",
        "refresh_mode": "realtime" if rows and len(processed_codes) > 0 else "static",
        "is_trading_time": is_trading,
        "message": message,
        "snapshot_trade_date": _today_trade_date() if rows and len(processed_codes) > 0 else None,
        "snapshot_time": snapshot_time,
        "data": rows,
    }

@router.get("/watchlist/{ts_code}/analysis")
async def get_watchlist_analysis(ts_code: str, request: Request, force_refresh: bool = False):
    """获取单只自选股的深度分析，供详情弹窗按需加载。"""
    try:
        user_id = await get_current_user_id(request)
        norm_code = _normalize_ts_code(ts_code)
        if not norm_code:
            raise HTTPException(status_code=400, detail="无效股票代码")
        _ensure_watchlist_membership(user_id, norm_code)
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
        limit = max(1, min(limit, 10000))

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

        latest_trade_date = df.iloc[-1]["trade_date"] if not df.empty else None
        live_snapshot = _fetch_live_snapshot(norm_code, latest_trade_date=latest_trade_date)
        if live_snapshot:
            df = _merge_live_snapshot_into_df(df, live_snapshot)
        
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
            result.append(_sanitize_json_value(item))

        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ========== 持仓管理 ==========

@router.post("/users/me/holdings/parse-image")
async def parse_holdings_from_image(
    request: Request,
    file: UploadFile = File(...),
):
    """识别持仓截图，返回可供预览和批量应用的结构化结果。"""
    user_id = await get_current_user_id(request)

    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="仅支持上传图片文件")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="图片内容为空")
    if len(content) > 8 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="图片过大，请控制在 8MB 以内")

    try:
        ai_result = await _parse_holdings_with_ai_vision(
            user_id=user_id,
            content=content,
            content_type=file.content_type,
        )
        raw_holdings = ai_result.get("holdings") or []
        notes = [
            str(note).strip()
            for note in (ai_result.get("notes") or [])
            if str(note).strip()
        ]
        prepared_rows, notes = _prepare_imported_holding_rows(
            raw_holdings,
            notes,
        )

        matched_rows = [item for item in prepared_rows if item.get("status") == "matched"]
        unmatched_rows = [item for item in prepared_rows if item.get("status") != "matched"]
        if prepared_rows:
            notes.append(
                f"已识别 {len(prepared_rows)} 条持仓，其中 {len(matched_rows)} 条已自动匹配代码，"
                f"{len(unmatched_rows)} 条待确认。待确认不代表识别失败。"
            )

        return {
            "status": "success",
            "data": {
                "items": prepared_rows,
                "matched_items": matched_rows,
                "unmatched_items": unmatched_rows,
                "recognition_backend": ai_result.get("backend") or "ai_multimodal",
                "used_ai_fallback": False,
                "local_ocr_available": False,
                "notes": notes,
                "summary": {
                    "total_items": len(prepared_rows),
                    "matched_count": len(matched_rows),
                    "unmatched_count": len(unmatched_rows),
                },
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("识别持仓截图失败")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/users/me/holdings/batch")
async def batch_update_holdings(request: Request, body: HoldingsBatchUpdateRequest):
    """批量更新当前用户持仓，并可自动同步到自选。"""
    user_id = await get_current_user_id(request)

    if not body.items:
        raise HTTPException(status_code=400, detail="至少需要一条持仓记录")

    deduped: dict[str, dict[str, Any]] = {}
    for item in body.items:
        norm_code = _normalize_ts_code(item.ts_code)
        if not norm_code:
            raise HTTPException(status_code=400, detail=f"无效股票代码: {item.ts_code}")
        deduped[norm_code] = {
            "ts_code": norm_code,
            "name": item.name,
            "shares": int(round(float(item.shares))),
            "avg_cost": round(float(item.avg_cost or 0), 4),
        }

    codes = list(deduped.keys())
    placeholders = ",".join(["?"] * len(codes))
    valid_df = fetch_df(
        f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})",
        tuple(codes),
    )
    valid_map = {
        str(row["ts_code"]): str(row["name"])
        for _, row in valid_df.iterrows()
    } if not valid_df.empty else {}

    missing_codes = [code for code in codes if code not in valid_map]
    if missing_codes:
        raise HTTPException(status_code=400, detail=f"以下代码不在 stock_basic 中: {', '.join(missing_codes)}")

    applied_items = []
    for code, item in deduped.items():
        applied_items.append({
            "ts_code": code,
            "name": valid_map.get(code) or item.get("name") or code,
            "shares": item["shares"],
            "avg_cost": item["avg_cost"],
        })

    deleted_count = 0
    watchlist_added = 0
    try:
        with get_db_connection() as con:
            existing_codes = {
                str(row[0])
                for row in con.execute(
                    "SELECT ts_code FROM user_holdings WHERE user_id = ?",
                    (user_id,),
                ).fetchall()
            }
            remaining_existing_codes = set(existing_codes)

            if body.replace_missing:
                keep_codes = tuple(codes)
                deleted_count = len(existing_codes - set(keep_codes))
                if keep_codes:
                    keep_placeholders = ",".join(["?"] * len(keep_codes))
                    delete_sql = (
                        f"DELETE FROM user_holdings WHERE user_id = ? "
                        f"AND ts_code NOT IN ({keep_placeholders})"
                    )
                    con.execute(delete_sql, (user_id, *keep_codes))
                else:
                    con.execute("DELETE FROM user_holdings WHERE user_id = ?", (user_id,))
                    deleted_count = len(existing_codes)
                remaining_existing_codes = existing_codes & set(keep_codes)

            update_params: list[tuple[Any, ...]] = []
            insert_params: list[tuple[Any, ...]] = []
            for item in applied_items:
                ts_code = item["ts_code"]
                if ts_code in remaining_existing_codes:
                    update_params.append((item["shares"], item["avg_cost"], user_id, ts_code))
                else:
                    insert_params.append((user_id, ts_code, item["shares"], item["avg_cost"]))

            if update_params:
                con.executemany(
                    """
                    UPDATE user_holdings
                    SET shares = ?, avg_cost = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ? AND ts_code = ?
                    """,
                    update_params,
                )
            if insert_params:
                con.executemany(
                    """
                    INSERT INTO user_holdings (user_id, ts_code, shares, avg_cost)
                    VALUES (?, ?, ?, ?)
                    """,
                    insert_params,
                )

            if body.sync_watchlist:
                watchlist_added = _sync_watchlist_entries(con, user_id, applied_items)

        return {
            "status": "success",
            "message": "持仓已批量更新",
            "data": {
                "items": applied_items,
                "summary": {
                    "updated_count": len(applied_items),
                    "deleted_count": deleted_count,
                    "watchlist_added": watchlist_added,
                    "replace_missing": body.replace_missing,
                    "sync_watchlist": body.sync_watchlist,
                },
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("批量更新持仓失败")
        raise HTTPException(status_code=500, detail=str(e))

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
            get_signal_level,
            calc_sector_position_value,
            generate_detailed_reason,
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
                'display_name': item.get('display_name') or item.get('name', ''),
                'score': item.get('latest_score', 0),
                'limit_ups': item.get('max_limit_ups', 0),
                'breadth': item.get('latest_breadth', 0),
                'stock_count': item.get('stock_count', 0),
                'top_stocks': item.get('leaders', []),
                'active_days': item.get('active_days', 0),
                'consecutive_days': item.get('consecutive_days', 0),
                'focus_tags': item.get('focus_tags', []),
                'driver_summary': item.get('driver_summary', ''),
                'driver_details': item.get('driver_details', []),
            })

        if not mainline_result:
            for series in mainline_history.get('series', []):
                if series.get('data'):
                    latest = series['data'][-1] if series['data'] else {}
                    mainline_result.append({
                        'name': series.get('name', ''),
                        'display_name': series.get('name', ''),
                        'score': latest.get('value', 0),
                        'limit_ups': latest.get('limit_ups', 0),
                        'breadth': latest.get('breadth', 0),
                        'stock_count': latest.get('stock_count', 0),
                        'top_stocks': latest.get('top_stocks', []),
                        'active_days': 0,
                        'consecutive_days': 0,
                        'focus_tags': [],
                        'driver_summary': '',
                        'driver_details': [],
                    })
        
        # 按分数排序，取前5
        mainline_result.sort(key=lambda x: x.get('score', 0), reverse=True)
        mainline_result = mainline_result[:5]
        
        if not mainline_result:
            return {"status": "success", "message": "无主线板块", "mainlines": []}

        # 获取市场环境
        market_env = get_market_environment(trade_date_str)
        stock_map_df = mainline_analyst.get_stock_mainline_map()

        # 构建主线板块数据
        mainlines_data = []
        
        for mainline in mainline_result:
            sector_name = mainline.get('name', '')
            
            # 板块筛选
            if sector and sector not in sector_name:
                continue
            
            # 获取板块内股票
            sector_stocks = get_sector_stocks(
                sector_name,
                trade_date_str,
                stock_map_df=stock_map_df,
                focus_tags=mainline.get('focus_tags', []),
            )
            
            if len(sector_stocks) < 5:
                continue

            ranked_sector_stocks = sorted(
                sector_stocks,
                key=lambda item: (
                    calc_sector_position_value(item),
                    _safe_float(item.get('pct_chg'), 0) or 0,
                    _safe_float(item.get('amount'), 0) or 0,
                ),
                reverse=True,
            )
            sector_rank_map = {
                item.get('ts_code'): idx + 1
                for idx, item in enumerate(ranked_sector_stocks)
                if item.get('ts_code')
            }
            
            # 计算每只股票的龙头评分
            leaders = []
            
            for stock in sector_stocks:
                # 计算综合评分
                score, reason, factor_scores = calc_mainline_leader_score(stock, market_env, sector_stocks)
                
                if score < min_score:
                    continue
                
                # 计算买入区间、止损、目标价
                entry_stop_target = calc_entry_stop_target(stock)
                
                # 获取信号等级
                signal = get_signal_level(score)
                
                # 获取板块内排名
                sector_rank = sector_rank_map.get(stock['ts_code'], len(sector_stocks))
                
                # 生成详细的入选原因
                detailed = generate_detailed_reason(stock, factor_scores, score, sector_rank)
                
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
                    # 梯队信息
                    'tier': detailed.get('tier', ''),
                    'tier_label': detailed.get('tier_label', ''),
                    'strategy': detailed.get('strategy', ''),
                    'detailed_reason': detailed.get('summary', ''),
                    'reason_details': detailed.get('details', []),
                    'advantages': detailed.get('advantages', []),
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
                'display_sector': mainline.get('display_name') or sector_name,
                'strength': mainline.get('score', 0),
                'limit_ups': mainline.get('limit_ups', 0),
                'stock_count': mainline.get('stock_count', 0),
                'active_days': mainline.get('active_days', 0),
                'consecutive_days': mainline.get('consecutive_days', 0),
                'resonance': resonance,
                'focus_tags': mainline.get('focus_tags', []),
                'driver_summary': mainline.get('driver_summary', ''),
                'driver_details': mainline.get('driver_details', []),
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
            calc_mainline_leader_score,
            calc_sector_position_value,
            calc_trend_leadership_score,
            calc_theme_fit_score,
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
        history_payload = mainline_analyst.get_history(days=10) or {}
        review_mainlines = (((history_payload.get('analysis') or {}).get('review_10d') or {}).get('mainlines') or [])
        review_map = {item.get('name', ''): item for item in review_mainlines if item.get('name')}
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
        if mapped_sector and review_map.get(mapped_sector):
            belong_sector = review_map.get(mapped_sector)

        # 获取板块内其他股票
        sector_stocks = []
        focus_tags = []
        if belong_sector:
            focus_tags = belong_sector.get('focus_tags', []) or []
        elif mapped_sector and review_map.get(mapped_sector):
            focus_tags = review_map.get(mapped_sector, {}).get('focus_tags', []) or []

        if mapped_sector:
            sector_stocks = get_sector_stocks(
                mapped_sector,
                trade_date_str,
                focus_tags=focus_tags,
            )
        sector_snapshot = next(
            (item for item in sector_stocks if item.get('ts_code') == norm_code),
            {},
        )
        
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
        for field in (
            'industry',
            'latest_net_mf_amount',
            'flow_positive_streak',
            'positive_flow_days',
            'flow_inflow_ratio',
            'active_days',
            'recent_active_days',
            'strong_streak',
            'limit_ups_10d',
            'trend_pioneer_score',
            'total_amount_10d',
            'theme_hit_count',
            'theme_hit_names',
            'amount_rank_pct',
            'total_amount_rank_pct',
            'volume_ratio',
            'turnover_rate',
            'big_order_ratio',
        ):
            if field in sector_snapshot and sector_snapshot.get(field) not in (None, ""):
                stock_data[field] = sector_snapshot.get(field)
        if sector_snapshot.get('flow_total_inflow') not in (None, ""):
            stock_data['flow_total_inflow'] = float(sector_snapshot.get('flow_total_inflow', 0) or 0)
        
        # 优先使用因子层写回的真实 volume_ratio，缺失时再回退到 vol/vol_ma5
        factor_volume_ratio = _safe_float(factors.get('volume_ratio'))
        if factor_volume_ratio and factor_volume_ratio > 0:
            stock_data['volume_ratio'] = round(factor_volume_ratio, 2)
        else:
            vol_ma5 = _safe_float(factors.get('vol_ma5'), stock_data['vol']) or 0
            if vol_ma5 > 0:
                stock_data['volume_ratio'] = round(stock_data['vol'] / vol_ma5, 2)
        
        # 获取市场环境
        market_env = get_market_environment(trade_date_str)
        
        # 计算综合评分
        score, reason, factor_scores = calc_mainline_leader_score(stock_data, market_env, sector_stocks)
        
        # 计算买入区间、止损、目标价
        entry_stop_target = calc_entry_stop_target(stock_data)
        
        # 获取信号等级
        signal = get_signal_level(score)
        
        # 板块内排名
        sector_rank = None
        sector_total = len(sector_stocks)
        if sector_stocks:
            sorted_by_position = sorted(
                sector_stocks,
                key=lambda item: (
                    calc_sector_position_value(item),
                    _safe_float(item.get('pct_chg'), 0) or 0,
                    _safe_float(item.get('amount'), 0) or 0,
                ),
                reverse=True,
            )
            sector_rank = next(
                (i + 1 for i, s in enumerate(sorted_by_position) if s.get('ts_code') == norm_code),
                sector_total,
            )
        
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


def get_sector_stocks(
    sector_name: str,
    trade_date: str,
    stock_map_df: pd.DataFrame | None = None,
    focus_tags: Optional[list[str]] = None,
) -> list:
    """
    获取板块内股票数据
    """
    try:
        from strategy.mainline.analyst import mainline_analyst

        stock_map = stock_map_df.copy() if stock_map_df is not None else mainline_analyst.get_stock_mainline_map()
        if stock_map.empty:
            return []

        sector_codes = (
            stock_map[stock_map['mapped_name'] == sector_name]['ts_code']
            .dropna()
            .drop_duplicates()
            .tolist()
        )
        sector_codes = [code for code in sector_codes if not _is_beijing_stock(code)]
        if not sector_codes:
            return []

        recent_metrics = _load_sector_recent_metrics(sector_codes, trade_date, lookback=10)
        theme_hits = _load_sector_theme_hits(sector_codes, sector_name, focus_tags=focus_tags)

        placeholders = ",".join(["?"] * len(sector_codes))
        stocks_df = fetch_df(
            f"""
            SELECT d.ts_code, b.name, b.industry, d.close, d.pct_chg, d.vol, d.amount, d.factors,
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
            ts_code = str(row.get('ts_code') or '').strip()
            if _is_beijing_stock(ts_code):
                continue
            factors = {}
            try:
                if row.get('factors'):
                    factors = json.loads(row['factors']) if isinstance(row['factors'], str) else row['factors']
            except:
                pass

            volume_ratio = _safe_float(factors.get('volume_ratio'), 1.0) or 1.0
            vol_ma5 = _safe_float(factors.get('vol_ma5'))
            if (not volume_ratio or volume_ratio <= 0) and vol_ma5 and vol_ma5 > 0:
                volume_ratio = round(float(row.get('vol', 0)) / vol_ma5, 2)

            recent = recent_metrics.get(ts_code, {})
            theme_meta = theme_hits.get(ts_code, {})
            
            result.append({
                'ts_code': ts_code,
                'name': row.get('name', ''),
                'industry': row.get('industry', ''),
                'close': float(row.get('close', 0)),
                'pct_chg': float(row.get('pct_chg', 0)),
                'vol': float(row.get('vol', 0)),
                'amount': float(row.get('amount', 0)),
                'factors': factors,
                'net_mf_amount': float(row.get('net_mf_amount', 0)),
                'latest_net_mf_amount': _safe_float(recent.get('latest_net_mf_amount'), float(row.get('net_mf_amount', 0)) or 0.0) or 0.0,
                'volume_ratio': volume_ratio,
                'turnover_rate': _safe_float(factors.get('turnover_rate'), 0) or 0,
                'flow_continuous_days': int(recent.get('flow_positive_streak', 1 if _safe_float(row.get('net_mf_amount'), 0) > 0 else 0)),
                'flow_positive_streak': int(recent.get('flow_positive_streak', 0)),
                'positive_flow_days': int(recent.get('positive_flow_days', 0)),
                'flow_total_inflow': float(recent.get('flow_total_inflow', row.get('net_mf_amount', 0) or 0)),
                'flow_inflow_ratio': float(recent.get('flow_inflow_ratio', 0.0)),
                'big_order_ratio': _safe_float(factors.get('big_order_ratio'), 0.0) or 0.0,
                'active_days': int(recent.get('active_days', 0)),
                'recent_active_days': int(recent.get('recent_active_days', 0)),
                'strong_streak': int(recent.get('strong_streak', 0)),
                'limit_ups_10d': int(recent.get('limit_ups_10d', 0)),
                'trend_pioneer_score': float(recent.get('trend_pioneer_score', 0.0)),
                'avg_pct': float(recent.get('avg_pct', 0.0)),
                'max_pct': float(recent.get('max_pct', 0.0)),
                'total_amount_10d': float(recent.get('total_amount_10d', row.get('amount', 0) or 0)),
                'theme_hit_count': int(theme_meta.get('theme_hit_count', 0)),
                'theme_hit_names': theme_meta.get('theme_hit_names', []),
                'is_mainline': True,
                'sector': sector_name,
                'mapped_sector': sector_name,
            })

        if not result:
            return []

        latest_amounts = [max(_safe_float(item.get('amount'), 0) or 0, 0.0) for item in result]
        total_amounts = [max(_safe_float(item.get('total_amount_10d'), 0) or 0, 0.0) for item in result]
        latest_sorted = sorted(latest_amounts)
        total_sorted = sorted(total_amounts)
        latest_total = max(1, len(latest_sorted))
        total_total = max(1, len(total_sorted))

        for item in result:
            latest_amount = max(_safe_float(item.get('amount'), 0) or 0, 0.0)
            total_amount = max(_safe_float(item.get('total_amount_10d'), 0) or 0, 0.0)
            latest_rank = sum(1 for value in latest_sorted if value <= latest_amount)
            total_rank = sum(1 for value in total_sorted if value <= total_amount)
            item['amount_rank_pct'] = round(latest_rank / latest_total, 4)
            item['total_amount_rank_pct'] = round(total_rank / total_total, 4)
        
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

    stock_map = {
        item.get('ts_code'): item
        for item in sector_stocks
        if item.get('ts_code') and not _is_beijing_stock(item.get('ts_code'))
    }
    fallback = []
    for leader in review_leaders:
        ts_code = leader.get('ts_code')
        if _is_beijing_stock(ts_code):
            continue
        stock = stock_map.get(ts_code)
        if not stock:
            continue
        leader_reason = str(leader.get('leader_reason') or '').strip()
        if not leader_reason:
            reason_parts = []
            if leader.get('active_days'):
                reason_parts.append(f"近10日强势{leader.get('active_days', 0)}天")
            if leader.get('recent_active_days'):
                reason_parts.append(f"最近3日走强{leader.get('recent_active_days', 0)}天")
            if leader.get('positive_flow_days'):
                reason_parts.append(f"资金净流入{leader.get('positive_flow_days', 0)}天")
            theme_hit_names = leader.get('theme_hit_names') or []
            if theme_hit_names:
                reason_parts.append(f"题材命中{' / '.join(theme_hit_names[:2])}")
            if not reason_parts:
                reason_parts.append(f"最高涨幅{leader.get('max_pct', 0)}%")
            leader_reason = "，".join(reason_parts)
        fallback.append({
            'ts_code': ts_code,
            'name': stock.get('name', leader.get('name', '')),
            'score': round(float(leader.get('leader_score', 0)), 1),
            'reason': leader_reason,
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

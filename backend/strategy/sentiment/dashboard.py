import json
import logging
import math
from typing import Any, Optional

from db.connection import fetch_df
from etl.calendar import trading_calendar
from strategy.sentiment import sentiment_analyst
from strategy.sentiment.live_monitor import live_sentiment_monitor

logger = logging.getLogger(__name__)


def _normalize_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return raw[:10]


def _sanitize_json_value(val: Any) -> Any:
    if val is None:
        return 0
    if isinstance(val, float):
        if math.isnan(val) or math.isinf(val):
            return 0.0
        return val
    if isinstance(val, dict):
        return {k: _sanitize_json_value(v) for k, v in val.items()}
    if isinstance(val, list):
        return [_sanitize_json_value(v) for v in val]
    return val


def _latest_available_close_date() -> tuple[Optional[str], Optional[str]]:
    expected_trade_date = _normalize_date(trading_calendar.get_latest_sync_date())
    if not expected_trade_date:
        return None, None

    df = fetch_df(
        """
        SELECT MAX(trade_date) AS trade_date
        FROM daily_price
        WHERE trade_date <= ?
        """,
        [expected_trade_date],
    )
    if df.empty:
        return None, expected_trade_date
    latest_trade_date = _normalize_date(df.iloc[0].get("trade_date"))
    return latest_trade_date, expected_trade_date


def _ensure_sentiment_upto_date(target_trade_date: Optional[str]) -> None:
    if not target_trade_date:
        return

    latest_sentiment_df = fetch_df(
        "SELECT MAX(trade_date) AS trade_date FROM market_sentiment"
    )
    latest_sentiment_date = None
    if not latest_sentiment_df.empty:
        latest_sentiment_date = _normalize_date(latest_sentiment_df.iloc[0].get("trade_date"))

    if latest_sentiment_date and latest_sentiment_date >= target_trade_date:
        return

    params: list[Any] = [target_trade_date]
    date_filter = ""
    if latest_sentiment_date:
        date_filter = "AND trade_date > ?"
        params.append(latest_sentiment_date)

    missing_df = fetch_df(
        f"""
        SELECT DISTINCT trade_date
        FROM daily_price
        WHERE trade_date <= ?
          {date_filter}
        ORDER BY trade_date
        """,
        params,
    )
    if missing_df.empty:
        return

    for trade_date in missing_df["trade_date"].tolist():
        date_str = _normalize_date(trade_date)
        if not date_str:
            continue
        try:
            sentiment_analyst.analyze(date_str)
        except Exception as exc:
            logger.warning("补算市场情绪失败 %s: %s", date_str, exc)


def build_market_sentiment_payload(
    days: int = 365,
    force_macro_refresh: bool = False,
) -> dict[str, Any]:
    latest_trade_date, expected_trade_date = _latest_available_close_date()
    _ensure_sentiment_upto_date(latest_trade_date)

    df = fetch_df(
        f"""
        SELECT trade_date, score, label, details
        FROM market_sentiment
        ORDER BY trade_date DESC
        LIMIT {max(1, int(days))}
        """
    )
    if df.empty:
        live_payload = live_sentiment_monitor.build_live_overlay(
            latest_trade_date=latest_trade_date,
            latest_sentiment=None,
            force_macro_refresh=force_macro_refresh,
            expected_trade_date=expected_trade_date,
        )
        return {
            "status": "success",
            "data": {
                "dates": [],
                "sentiment": [],
                "index": [],
                "live": live_payload,
                "automation": live_payload.get("automation") or {},
            },
        }

    records = df.iloc[::-1].to_dict("records")
    dates = [_normalize_date(row.get("trade_date")) for row in records]
    dates = [d for d in dates if d]
    if not dates:
        return {
            "status": "success",
            "data": {"dates": [], "sentiment": [], "index": []},
        }

    min_date = dates[0]
    max_date = dates[-1]

    daily_stats_df = fetch_df(
        """
        SELECT
            trade_date,
            COUNT(*) FILTER (WHERE pct_chg >= 9.5) AS limit_up_count,
            COUNT(*) FILTER (WHERE pct_chg <= -9.5) AS limit_down_count,
            COUNT(*) FILTER (WHERE high >= pre_close * 1.095 AND pct_chg < 9.5) AS broken_count
        FROM daily_price
        WHERE trade_date BETWEEN ? AND ?
        GROUP BY trade_date
        ORDER BY trade_date
        """,
        (min_date, max_date),
    )
    daily_stats_map: dict[str, dict[str, int]] = {}
    if not daily_stats_df.empty:
        for _, row in daily_stats_df.iterrows():
            trade_date = _normalize_date(row.get("trade_date"))
            if not trade_date:
                continue
            daily_stats_map[trade_date] = {
                "limit": int(row.get("limit_up_count") or 0),
                "limit_down": int(row.get("limit_down_count") or 0),
                "failure": int(row.get("broken_count") or 0),
            }

    sentiment = []
    for row in records:
        trade_date = _normalize_date(row.get("trade_date"))
        if not trade_date:
            continue
        details = row.get("details")
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except Exception:
                details = {}
        details = details or {}
        factors = details.setdefault("factors", {})
        stats_row = daily_stats_map.get(trade_date)
        if stats_row:
            factors.update(stats_row)
        sentiment.append(
            {
                "trade_date": trade_date,
                "value": _sanitize_json_value(row.get("score")),
                "label": row.get("label") or "观望",
                "details": _sanitize_json_value(details),
            }
        )

    index_df = fetch_df(
        """
        SELECT trade_date, close
        FROM market_index
        WHERE ts_code = '000001.SH'
          AND trade_date BETWEEN ? AND ?
        ORDER BY trade_date
        """,
        (min_date, max_date),
    )
    index_map: dict[str, Any] = {}
    if not index_df.empty:
        for _, row in index_df.iterrows():
            trade_date = _normalize_date(row.get("trade_date"))
            if trade_date:
                index_map[trade_date] = _sanitize_json_value(row.get("close"))

    index = []
    last_index = 0.0
    for trade_date in dates:
        current_index = _sanitize_json_value(index_map.get(trade_date, last_index))
        last_index = current_index
        index.append(current_index)

    latest_sentiment = sentiment[-1] if sentiment else None
    live_payload = live_sentiment_monitor.build_live_overlay(
        latest_trade_date=max_date,
        latest_sentiment=latest_sentiment,
        force_macro_refresh=force_macro_refresh,
        expected_trade_date=expected_trade_date,
    )
    return {
        "status": "success",
        "data": {
            "dates": dates,
            "sentiment": sentiment,
            "index": index,
            "live": live_payload,
            "automation": live_payload.get("automation") or {},
        },
    }

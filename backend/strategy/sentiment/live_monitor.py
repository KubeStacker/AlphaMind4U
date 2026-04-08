import html
import logging
import math
import re
import threading
import time
from typing import Any, Optional
from urllib.request import ProxyHandler, Request, build_opener

import arrow
import httpx
import pandas as pd

from db.connection import fetch_df
from etl.calendar import trading_calendar
from strategy.sentiment.config import SENTIMENT_CONFIG, score_to_label

logger = logging.getLogger(__name__)


class LiveSentimentMonitor:
    CNBC_QUOTE_URL = "https://quote.cnbc.com/quote-html-webservice/quote.htm"
    CNBC_QUOTE_PAGE = "https://www.cnbc.com/quotes/US.10"
    FRED_TEN_YEAR_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"
    PIZZA_URL = "https://www.pizzint.watch/"

    def __init__(self):
        live_cfg = SENTIMENT_CONFIG.get("live_monitor", {})
        self.dashboard_refresh_seconds = int(live_cfg.get("dashboard_refresh_seconds", 60))
        self.closed_refresh_seconds = int(live_cfg.get("closed_refresh_seconds", 300))
        self.live_cache_seconds = int(live_cfg.get("live_cache_seconds", 45))
        self.macro_refresh_seconds = int(live_cfg.get("macro_refresh_seconds", 600))
        self.ten_year_warn_high = float(live_cfg.get("cnbc_ten_year_warn_high", 4.38))
        self.ten_year_risk_high = float(live_cfg.get("cnbc_ten_year_risk_high", 4.40))
        self.ten_year_warn_low = float(live_cfg.get("cnbc_ten_year_warn_low", 4.33))
        self.ten_year_support_low = float(live_cfg.get("cnbc_ten_year_support_low", 4.30))
        self.pizza_spike_warn = float(live_cfg.get("pizza_spike_warn", 150.0))
        self.pizza_spike_risk = float(live_cfg.get("pizza_spike_risk", 300.0))
        self.primary_index_weights = {
            "000001.SH": 0.55,
            "399006.SZ": 0.45,
        }

        self._macro_lock = threading.RLock()
        self._live_lock = threading.RLock()
        self._macro_cache: dict[str, Any] = {}
        self._macro_fetched_at = 0.0
        self._live_cache: dict[str, Any] = {}
        self._live_fetched_at = 0.0

    def refresh_macro_signals(self, force: bool = False) -> dict[str, Any]:
        with self._macro_lock:
            now_ts = time.time()
            if (
                not force
                and self._macro_cache
                and now_ts - self._macro_fetched_at < self.macro_refresh_seconds
            ):
                return self._macro_cache

            ten_year = self._fetch_ten_year_signal()
            pizza = self._fetch_pizza_index()
            snapshot = {
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "refresh_seconds": self.macro_refresh_seconds,
                "ten_year_yield": ten_year,
                "pizza_index": pizza,
            }
            self._macro_cache = snapshot
            self._macro_fetched_at = now_ts
            return snapshot

    def build_live_overlay(
        self,
        latest_trade_date: Optional[str],
        latest_sentiment: Optional[dict[str, Any]],
        force_macro_refresh: bool = False,
        expected_trade_date: Optional[str] = None,
    ) -> dict[str, Any]:
        now = arrow.now("Asia/Shanghai")
        market_status = "TRADING" if trading_calendar.is_trading_time() else "CLOSED"
        macro = self.refresh_macro_signals(force=force_macro_refresh)
        if market_status == "TRADING":
            indices = self._get_live_indices(force=True)
        else:
            indices = self._get_close_indices(latest_trade_date=latest_trade_date)

        base_score = self._safe_float(
            (latest_sentiment or {}).get("value"),
            self._safe_float((latest_sentiment or {}).get("score"), 50.0),
        )
        base_label = str((latest_sentiment or {}).get("label") or self._score_to_label(base_score))
        expected_trade_date = expected_trade_date or latest_trade_date
        is_latest_close_loaded = (
            bool(latest_trade_date)
            and bool(expected_trade_date)
            and str(latest_trade_date)[:10] == str(expected_trade_date)[:10]
        )

        weighted_pct = self._calc_weighted_index_move(indices)
        score_notes: list[str] = []
        score_delta = 0.0
        risk_score = 45.0
        index_note = self._build_index_note(indices=indices, weighted_pct=weighted_pct)
        if index_note:
            score_notes.append(index_note)
        if market_status == "TRADING" and indices.get("available"):
            score_delta = self._clip(weighted_pct * 7.5, -10.0, 10.0)
            risk_score += self._clip(-weighted_pct * 10.0, -14.0, 14.0)

        if market_status != "TRADING":
            if latest_trade_date:
                if is_latest_close_loaded:
                    score_notes.append(f"收盘情绪已切到 {latest_trade_date} 最新基线。")
                else:
                    score_notes.append(
                        f"收盘后目标基线应为 {expected_trade_date}，当前库内只到 {latest_trade_date}。"
                    )
            else:
                score_notes.append("收盘基线暂缺，先按默认中性情绪处理。")

        ten_year_payload = macro.get("ten_year_yield") or {}
        pizza_payload = macro.get("pizza_index") or {}

        ten_year_score_adj, ten_year_risk, ten_year_note = self._assess_ten_year_signal(
            ten_year_payload,
            weighted_pct,
        )
        if ten_year_note:
            score_notes.append(ten_year_note)

        pizza_score_adj, pizza_risk, pizza_note = self._assess_pizza_signal(pizza_payload)
        if pizza_note:
            score_notes.append(pizza_note)

        live_score = round(self._clip(base_score + score_delta + ten_year_score_adj + pizza_score_adj, 0.0, 100.0), 1)
        live_label = self._score_to_label(live_score)
        risk_score = round(self._clip(risk_score + ten_year_risk + pizza_risk, 0.0, 100.0), 1)
        risk_prediction = self._build_risk_prediction(
            risk_score=risk_score,
            weighted_pct=weighted_pct,
            index_mode=str(indices.get("mode") or "unknown"),
            ten_year_payload=ten_year_payload,
            pizza_payload=pizza_payload,
        )

        sse_quote = (indices.get("quotes") or {}).get("000001.SH") or {}
        cyb_quote = (indices.get("quotes") or {}).get("399006.SZ") or {}
        overlay_summary = "；".join(score_notes) if score_notes else "盘中暂无可用实时叠加，先按收盘情绪处理。"
        return {
            "available": True,
            "market_status": market_status,
            "is_trading_day": trading_calendar.is_trading_day(now.date()),
            "board_time": now.format("YYYY-MM-DD HH:mm:ss"),
            "trade_date": latest_trade_date,
            "score": live_score,
            "label": live_label,
            "base_score": round(base_score, 1),
            "base_label": base_label,
            "delta": round(live_score - base_score, 1),
            "overlay_summary": overlay_summary,
            "indices": indices,
            "sse_snapshot": sse_quote,
            "cyb_snapshot": cyb_quote,
            "external_signals": macro,
            "risk_prediction": risk_prediction,
            "data_freshness": {
                "expected_trade_date": expected_trade_date,
                "baseline_trade_date": latest_trade_date,
                "is_latest_close_loaded": is_latest_close_loaded,
            },
            "automation": {
                "dashboard_refresh_seconds": (
                    self.dashboard_refresh_seconds
                    if market_status == "TRADING"
                    else self.closed_refresh_seconds
                ),
                "macro_refresh_seconds": self.macro_refresh_seconds,
            },
        }

    def _get_live_indices(self, force: bool = False) -> dict[str, Any]:
        with self._live_lock:
            now_ts = time.time()
            if (
                not force
                and self._live_cache
                and now_ts - self._live_fetched_at < self.live_cache_seconds
            ):
                return self._live_cache

            today = arrow.now("Asia/Shanghai").format("YYYY-MM-DD")
            payload = {
                "available": False,
                "mode": "live",
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "quotes": {},
                "weighted_pct": 0.0,
            }
            if not trading_calendar.is_trading_time():
                self._live_cache = payload
                self._live_fetched_at = now_ts
                return payload

            quotes = self._fetch_realtime_index_quotes(
                codes=list(self.primary_index_weights.keys()),
                today=today,
            )

            payload = {
                "available": bool(quotes),
                "mode": "live",
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "quotes": quotes,
                "weighted_pct": self._calc_weighted_index_move({"quotes": quotes}),
                "missing_codes": [code for code in self.primary_index_weights if code not in quotes],
            }
            self._live_cache = payload
            self._live_fetched_at = now_ts
            return payload

    def _get_close_indices(self, latest_trade_date: Optional[str]) -> dict[str, Any]:
        payload = {
            "available": False,
            "mode": "close",
            "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
            "quotes": {},
            "weighted_pct": 0.0,
            "missing_codes": list(self.primary_index_weights.keys()),
        }
        if not latest_trade_date:
            return payload

        placeholders = ",".join(["?"] * len(self.primary_index_weights))
        try:
            df = fetch_df(
                f"""
                SELECT ts_code, trade_date, close, pre_close, pct_chg
                FROM market_index
                WHERE trade_date = ?
                  AND ts_code IN ({placeholders})
                """,
                [latest_trade_date, *self.primary_index_weights.keys()],
            )
        except Exception as exc:
            logger.warning("获取收盘指数快照失败: %s", exc)
            return payload

        quotes: dict[str, Any] = {}
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                quote = self._extract_quote_snapshot(row)
                if not quote:
                    continue
                ts_code = quote.get("ts_code")
                if ts_code:
                    quote["trade_date"] = str(latest_trade_date)[:10]
                    quote["quote_time"] = str(latest_trade_date)[:10]
                    quotes[ts_code] = quote

        payload["available"] = bool(quotes)
        payload["quotes"] = quotes
        payload["weighted_pct"] = self._calc_weighted_index_move({"quotes": quotes})
        payload["missing_codes"] = [code for code in self.primary_index_weights if code not in quotes]
        return payload

    def _fetch_realtime_index_quotes(self, codes: list[str], today: str) -> dict[str, Any]:
        quotes: dict[str, Any] = {}
        remaining = [code for code in codes if code]
        if not remaining:
            return quotes

        try:
            from etl.sync import sync_engine
        except Exception as exc:
            logger.warning("加载行情同步引擎失败: %s", exc)
            return quotes

        for src in ("sina", "dc"):
            if not remaining:
                break
            try:
                quote_df = sync_engine.provider.realtime_quote(
                    ts_code=",".join(remaining),
                    src=src,
                )
            except Exception as exc:
                logger.warning("获取盘中指数快照失败 src=%s: %s", src, exc)
                continue

            if quote_df is None or quote_df.empty:
                continue

            for _, row in quote_df.iterrows():
                quote = self._extract_quote_snapshot(row)
                if not quote:
                    continue
                ts_code = quote.get("ts_code")
                if ts_code and (quote.get("trade_date") == today or not quote.get("trade_date")):
                    quote["trade_date"] = quote.get("trade_date") or today
                    quotes[ts_code] = quote
            remaining = [code for code in codes if code not in quotes]

        return quotes

    def _fetch_ten_year_signal(self) -> dict[str, Any]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        }
        tushare_quote = self._fetch_tushare_ten_year()
        if tushare_quote and tushare_quote.get("available"):
            return tushare_quote
        fred_quote = self._fetch_fred_ten_year_fallback(headers=headers)
        if fred_quote and fred_quote.get("available"):
            if tushare_quote and tushare_quote.get("error"):
                fred_quote["fallback_reason"] = tushare_quote.get("error")
            return fred_quote
        cnbc_quote = self._fetch_cnbc_ten_year(headers=headers)
        if tushare_quote and tushare_quote.get("error"):
            cnbc_quote["fallback_reason"] = tushare_quote.get("error")
        return cnbc_quote

    def _fetch_tushare_ten_year(self) -> dict[str, Any]:
        try:
            from etl.sync import sync_engine
        except Exception as exc:
            logger.warning("加载同步引擎失败，无法调用 Tushare us_tycr: %s", exc)
            return {
                "available": False,
                "source": "Tushare us_tycr",
                "yield": None,
                "change": None,
                "change_bp": None,
                "quote_time": None,
                "as_of": None,
                "stale_days": None,
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "confidence": "low",
                "error": "provider_unavailable",
            }

        end_date = arrow.now("Asia/Shanghai").shift(days=-1).format("YYYYMMDD")
        start_date = arrow.now("Asia/Shanghai").shift(days=-14).format("YYYYMMDD")
        try:
            df = sync_engine.provider.us_tycr(
                start_date=start_date,
                end_date=end_date,
                fields="date,y10",
            )
        except Exception as exc:
            logger.warning("Tushare us_tycr 抓取失败: %s", exc)
            return {
                "available": False,
                "source": "Tushare us_tycr",
                "yield": None,
                "change": None,
                "change_bp": None,
                "quote_time": None,
                "as_of": None,
                "stale_days": None,
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "confidence": "low",
                "error": str(exc) or "fetch_failed",
            }

        if df is None or df.empty:
            return {
                "available": False,
                "source": "Tushare us_tycr",
                "yield": None,
                "change": None,
                "change_bp": None,
                "quote_time": None,
                "as_of": None,
                "stale_days": None,
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "confidence": "low",
                "error": "empty_result",
            }

        work = df.copy()
        work.columns = [str(col).lower() for col in work.columns]
        if "y10" not in work.columns or "date" not in work.columns:
            return {
                "available": False,
                "source": "Tushare us_tycr",
                "yield": None,
                "change": None,
                "change_bp": None,
                "quote_time": None,
                "as_of": None,
                "stale_days": None,
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "confidence": "low",
                "error": "missing_fields",
            }

        work["y10"] = pd.to_numeric(work["y10"], errors="coerce")
        work = work.dropna(subset=["y10"]).copy()
        if work.empty:
            return {
                "available": False,
                "source": "Tushare us_tycr",
                "yield": None,
                "change": None,
                "change_bp": None,
                "quote_time": None,
                "as_of": None,
                "stale_days": None,
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "confidence": "low",
                "error": "no_valid_y10",
            }

        work["date"] = work["date"].map(self._normalize_macro_as_of)
        work = work.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
        latest = work.iloc[-1]
        previous_yield = float(work.iloc[-2]["y10"]) if len(work) >= 2 else None
        latest_yield = float(latest["y10"])
        as_of = str(latest["date"])
        return {
            "available": True,
            "source": "Tushare us_tycr",
            "symbol": "US10Y",
            "yield": latest_yield,
            "change": None,
            "change_bp": (
                round((latest_yield - previous_yield) * 100.0, 1)
                if previous_yield is not None
                else None
            ),
            "previous_yield": previous_yield,
            "quote_time": as_of,
            "as_of": as_of,
            "stale_days": self._calc_stale_days(as_of),
            "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
            "confidence": "high",
        }

    def _fetch_cnbc_ten_year(self, headers: Optional[dict[str, str]] = None) -> dict[str, Any]:
        params_list = [
            {
                "noform": "1",
                "partnerId": "2",
                "fund": "1",
                "exthrs": "1",
                "output": "json",
                "requestMethod": "extended",
                "symbols": "US10Y",
            },
            {
                "noform": "1",
                "partnerId": "2",
                "fund": "1",
                "exthrs": "1",
                "output": "json",
                "requestMethod": "extended",
                "symbols": "US.10",
            },
        ]

        headers = headers or {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        }
        error_message = ""

        with httpx.Client(
            timeout=12.0,
            follow_redirects=True,
            headers=headers,
            trust_env=False,
        ) as client:
            for params in params_list:
                try:
                    response = client.get(self.CNBC_QUOTE_URL, params=params)
                    response.raise_for_status()
                    payload = response.json()
                    quote = self._parse_cnbc_quote_payload(payload)
                    if quote:
                        yield_value = self._safe_float(quote.get("yield"), None)
                        return {
                            "available": yield_value is not None,
                            "source": "CNBC",
                            "symbol": str(quote.get("symbol") or "US10Y"),
                            "yield": yield_value,
                            "change": self._safe_float(quote.get("change"), None),
                            "change_bp": self._safe_float(quote.get("change"), None),
                            "quote_time": str(
                                quote.get("quote_time")
                                or quote.get("last_time")
                                or quote.get("formatted_last_time")
                                or ""
                            ).strip()
                            or None,
                            "as_of": self._normalize_macro_as_of(
                                quote.get("quote_time")
                                or quote.get("last_time")
                                or quote.get("formatted_last_time")
                            ),
                            "stale_days": self._calc_stale_days(
                                quote.get("quote_time")
                                or quote.get("last_time")
                                or quote.get("formatted_last_time")
                            ),
                            "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                            "source_url": self.CNBC_QUOTE_PAGE,
                            "confidence": "medium",
                        }
                except Exception as exc:
                    error_message = str(exc)
                    continue

        logger.warning("CNBC 十年期收益率抓取失败: %s", error_message or "unknown error")
        return {
            "available": False,
            "source": "CNBC",
            "yield": None,
            "change": None,
            "change_bp": None,
            "quote_time": None,
            "as_of": None,
            "stale_days": None,
            "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
            "source_url": self.CNBC_QUOTE_PAGE,
            "confidence": "low",
            "error": error_message or "fetch_failed",
        }

    def _fetch_fred_ten_year_fallback(self, headers: dict[str, str]) -> Optional[dict[str, Any]]:
        try:
            request = Request(self.FRED_TEN_YEAR_CSV_URL, headers=headers)
            opener = build_opener(ProxyHandler({}))
            with opener.open(request, timeout=15) as response:
                raw_text = response.read().decode("utf-8", errors="ignore")
            lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
            records: list[tuple[str, float]] = []
            for line in reversed(lines[1:]):
                try:
                    observation_date, value = line.split(",", 1)
                except ValueError:
                    continue
                yield_value = self._safe_float(value, None)
                if yield_value is None:
                    continue
                records.append((observation_date, yield_value))
                if len(records) >= 2:
                    break
            if records:
                latest_date, latest_value = records[0]
                previous_value = records[1][1] if len(records) > 1 else None
                return {
                    "available": True,
                    "source": "FRED",
                    "symbol": "DGS10",
                    "yield": latest_value,
                    "change": None,
                    "change_bp": (
                        round((latest_value - previous_value) * 100.0, 1)
                        if previous_value is not None
                        else None
                    ),
                    "previous_yield": previous_value,
                    "quote_time": latest_date,
                    "as_of": latest_date,
                    "stale_days": self._calc_stale_days(latest_date),
                    "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                    "source_url": self.FRED_TEN_YEAR_CSV_URL,
                    "fallback": True,
                    "confidence": "high",
                }
        except Exception as exc:
            logger.warning("FRED 十年期收益率回退抓取失败: %s", exc)
        return None

    def _fetch_pizza_index(self) -> dict[str, Any]:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
            )
        }
        error_message = ""
        try:
            with httpx.Client(
                timeout=12.0,
                follow_redirects=True,
                headers=headers,
                trust_env=False,
            ) as client:
                response = client.get(self.PIZZA_URL)
                response.raise_for_status()
            text = self._html_to_text(response.text)
            live_section = self._extract_pizza_live_section(text)
            doughcon_match = re.search(r"DOUGHCON\s*(\d+)", live_section, flags=re.IGNORECASE)
            locations_matches = re.findall(
                r"(\d+)\s+LOCATIONS\s+MONITORED",
                live_section,
                flags=re.IGNORECASE,
            )
            active_spikes = self._extract_pizza_spikes(live_section)
            spike_values = [
                float(item["spike_pct"])
                for item in active_spikes
                if self._safe_float(item.get("spike_pct"), None) is not None
            ]
            doughcon = int(doughcon_match.group(1)) if doughcon_match else None
            max_spike = max(spike_values) if spike_values else None
            return {
                "available": doughcon is not None or max_spike is not None,
                "source": "PizzINT",
                "doughcon": doughcon,
                "max_spike_pct": max_spike,
                "active_spikes": active_spikes,
                "locations_monitored": int(locations_matches[-1]) if locations_matches else None,
                "as_of": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "stale_days": 0,
                "confidence": "medium" if doughcon is not None else "low",
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "source_url": self.PIZZA_URL,
            }
        except Exception as exc:
            error_message = str(exc)

        logger.warning("Pizza 指数抓取失败: %s", error_message or "unknown error")
        return {
            "available": False,
            "source": "PizzINT",
            "doughcon": None,
            "max_spike_pct": None,
            "active_spikes": [],
            "locations_monitored": None,
            "as_of": None,
            "stale_days": None,
            "confidence": "low",
            "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
            "source_url": self.PIZZA_URL,
            "error": error_message or "fetch_failed",
        }

    def _parse_cnbc_quote_payload(self, payload: Any) -> Optional[dict[str, Any]]:
        if not isinstance(payload, dict):
            return None

        result = payload.get("ExtendedQuoteResult") or payload.get("QuickQuoteResult") or payload
        candidates = result.get("ExtendedQuote") or result.get("QuickQuote") or []
        if isinstance(candidates, dict):
            candidates = [candidates]

        for item in candidates:
            quote = item.get("QuickQuote") if isinstance(item, dict) else None
            quote = quote if isinstance(quote, dict) else item
            if not isinstance(quote, dict):
                continue
            last_value = self._safe_float(quote.get("last"), None)
            if last_value is None:
                last_value = self._safe_float(quote.get("last_trade"), None)
            if last_value is None:
                continue
            return {
                "symbol": quote.get("symbol") or quote.get("symbolName"),
                "yield": last_value,
                "change": quote.get("change"),
                "quote_time": (
                    quote.get("last_time")
                    or quote.get("formatted_last_time")
                    or quote.get("trade_time")
                ),
            }
        return None

    def _assess_ten_year_signal(
        self,
        ten_year_payload: dict[str, Any],
        weighted_pct: float,
    ) -> tuple[float, float, str]:
        yield_value = self._safe_float((ten_year_payload or {}).get("yield"), None)
        if yield_value is None:
            return 0.0, 0.0, ""

        as_of = ten_year_payload.get("as_of") or ten_year_payload.get("quote_time")
        as_of_suffix = f" ({as_of})" if as_of else ""

        if yield_value >= self.ten_year_risk_high:
            return -8.0, 22.0, f"10Y{as_of_suffix} {yield_value:.3f}% 处于高利率区，外部风险偏压制"
        if yield_value >= self.ten_year_warn_high:
            return -4.0, 12.0, f"10Y{as_of_suffix} {yield_value:.3f}% 靠近风险上沿，需防估值再压缩"

        if yield_value <= self.ten_year_support_low:
            if weighted_pct >= 0.2:
                return 3.0, -8.0, f"10Y{as_of_suffix} {yield_value:.3f}% 处于低位区，且A股同步偏强，情绪有修复空间"
            return -1.5, 6.0, f"10Y{as_of_suffix} {yield_value:.3f}% 处于低位区，但A股未跟随，偏避险交易"

        if yield_value <= self.ten_year_warn_low:
            if weighted_pct >= 0:
                return 1.5, -3.0, f"10Y{as_of_suffix} {yield_value:.3f}% 回到低位观察带，外围利率压力缓和"
            return -0.5, 2.0, f"10Y{as_of_suffix} {yield_value:.3f}% 回到低位观察带，但指数承接仍弱"

        return 0.0, 0.0, f"10Y{as_of_suffix} {yield_value:.3f}%"

    def _assess_pizza_signal(self, pizza_payload: dict[str, Any]) -> tuple[float, float, str]:
        if not pizza_payload or not pizza_payload.get("available"):
            return 0.0, 0.0, ""

        doughcon = self._safe_float(pizza_payload.get("doughcon"), None)
        max_spike = self._safe_float(pizza_payload.get("max_spike_pct"), None)
        as_of = pizza_payload.get("as_of")
        as_of_suffix = f" ({str(as_of)[5:16]})" if as_of else ""

        if (
            (doughcon is not None and doughcon <= 2)
            or (max_spike is not None and max_spike >= self.pizza_spike_risk)
        ):
            return -3.0, 8.0, f"Pizza{as_of_suffix} 异常放大，按低置信度地缘噪声做风险观察"

        if (
            (doughcon is not None and doughcon <= 3)
            or (max_spike is not None and max_spike >= self.pizza_spike_warn)
        ):
            return -1.0, 3.0, f"Pizza{as_of_suffix} 偏热，只做弱风险提示，不单独决定仓位"

        if doughcon is not None:
            return 0.0, 0.0, f"Pizza{as_of_suffix} DOUGHCON {int(doughcon)}"
        return 0.0, 0.0, ""

    def _build_risk_prediction(
        self,
        risk_score: float,
        weighted_pct: float,
        index_mode: str,
        ten_year_payload: dict[str, Any],
        pizza_payload: dict[str, Any],
    ) -> dict[str, Any]:
        level = "neutral"
        headline = (
            "外部风险中性，继续按盘中承接判断。"
            if index_mode == "live"
            else "外部风险中性，先按最新收盘情绪处理。"
        )
        if risk_score >= 70:
            level = "high"
            headline = "外部风险偏高，题材追价和高弹性仓位都应收敛。"
        elif risk_score >= 56:
            level = "elevated"
            headline = "外部扰动在升温，仓位可以做，但不宜放大进攻。"
        elif risk_score <= 28:
            level = "relief"
            headline = "外部压力暂缓，若上证继续稳住，情绪更偏修复。"

        reasons = []
        ten_year_value = self._safe_float((ten_year_payload or {}).get("yield"), None)
        ten_year_as_of = (ten_year_payload or {}).get("as_of")
        if ten_year_value is not None:
            if ten_year_value >= self.ten_year_risk_high:
                reasons.append(f"10Y {ten_year_value:.3f}% 已高于风险线{self._format_as_of_suffix(ten_year_as_of)}。")
            elif ten_year_value >= self.ten_year_warn_high:
                reasons.append(f"10Y {ten_year_value:.3f}% 正逼近风险线{self._format_as_of_suffix(ten_year_as_of)}。")
            elif ten_year_value <= self.ten_year_support_low:
                reasons.append(f"10Y {ten_year_value:.3f}% 已回到低位区{self._format_as_of_suffix(ten_year_as_of)}。")
            elif ten_year_value <= self.ten_year_warn_low:
                reasons.append(f"10Y {ten_year_value:.3f}% 靠近低位观察带{self._format_as_of_suffix(ten_year_as_of)}。")

        if weighted_pct >= 0.5:
            prefix = "A股盘中" if index_mode == "live" else "最近收盘"
            reasons.append(f"{prefix}上证/创业板均值 {weighted_pct:+.2f}%，承接偏强。")
        elif weighted_pct <= -0.5:
            prefix = "A股盘中" if index_mode == "live" else "最近收盘"
            reasons.append(f"{prefix}上证/创业板均值 {weighted_pct:+.2f}%，承接偏弱。")

        doughcon = self._safe_float((pizza_payload or {}).get("doughcon"), None)
        max_spike = self._safe_float((pizza_payload or {}).get("max_spike_pct"), None)
        pizza_as_of = (pizza_payload or {}).get("as_of")
        if doughcon is not None:
            reasons.append(
                f"Pizza 指数 DOUGHCON {int(doughcon)}{self._format_as_of_suffix(pizza_as_of)}，仅作低置信度旁证。"
            )
        elif max_spike is not None:
            reasons.append(
                f"Pizza 指数最高门店热度 {max_spike:.0f}%{self._format_as_of_suffix(pizza_as_of)}。"
            )

        return {
            "score": risk_score,
            "level": level,
            "headline": headline,
            "reasons": reasons[:4],
        }

    def _extract_quote_snapshot(self, raw_row: Any) -> Optional[dict[str, Any]]:
        if raw_row is None:
            return None

        if isinstance(raw_row, pd.Series):
            row_map = {str(k).lower(): raw_row[k] for k in raw_row.index}
        elif isinstance(raw_row, dict):
            row_map = {str(k).lower(): v for k, v in raw_row.items()}
        else:
            return None

        ts_code = self._normalize_ts_code(row_map.get("ts_code"))
        price = self._safe_float(
            row_map.get("price") or row_map.get("current") or row_map.get("close"),
            None,
        )
        if not ts_code or price is None:
            return None

        pre_close = self._safe_float(row_map.get("pre_close") or row_map.get("yclose"), None)
        pct = self._safe_float(
            row_map.get("pct_chg") or row_map.get("pct_change") or row_map.get("changepercent"),
            None,
        )
        if pct is None and pre_close not in (None, 0):
            pct = (price - pre_close) / pre_close * 100.0

        trade_date = self._normalize_trade_date(row_map.get("trade_date") or row_map.get("date"))
        return {
            "ts_code": ts_code,
            "name": str(row_map.get("name") or "").strip(),
            "trade_date": trade_date,
            "quote_time": str(
                row_map.get("time") or row_map.get("datetime") or row_map.get("update_time") or ""
            ).strip() or None,
            "close": price,
            "pre_close": pre_close,
            "pct": round(pct, 3) if pct is not None else None,
        }

    def _calc_weighted_index_move(self, indices_payload: dict[str, Any]) -> float:
        quotes = (indices_payload or {}).get("quotes") or {}
        weighted = 0.0
        total = 0.0
        for code, weight in self.primary_index_weights.items():
            pct = self._safe_float((quotes.get(code) or {}).get("pct"), None)
            if pct is None:
                continue
            weighted += pct * weight
            total += weight
        if total <= 0:
            return 0.0
        return round(weighted / total, 3)

    def _score_to_label(self, score: float) -> str:
        return score_to_label(score)

    def _normalize_ts_code(self, code: Any) -> str:
        raw = str(code or "").strip().upper()
        if not raw:
            return ""
        if "." in raw:
            return raw
        if raw.startswith("6"):
            return f"{raw}.SH"
        if raw.startswith("0") or raw.startswith("3"):
            return f"{raw}.SZ"
        return raw

    def _normalize_trade_date(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        digits = "".join(ch for ch in raw if ch.isdigit())
        for candidate in (raw, digits[:8] if len(digits) >= 8 else ""):
            if not candidate:
                continue
            for fmt in ("YYYY-MM-DD", "YYYYMMDD", "YYYY-MM-DD HH:mm:ss", "YYYYMMDD HH:mm:ss"):
                try:
                    return arrow.get(candidate, fmt).format("YYYY-MM-DD")
                except Exception:
                    continue
        try:
            return arrow.get(raw).format("YYYY-MM-DD")
        except Exception:
            return raw[:10] if len(raw) >= 10 else None

    def _html_to_text(self, raw_html: str) -> str:
        text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html, flags=re.IGNORECASE)
        text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "\n", text)
        text = html.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n+", "\n", text)
        return text

    def _extract_pizza_live_section(self, text: str) -> str:
        section = text
        for marker in ("### MARKET INTELLIGENCE", "## OSINT Feed", "## OSINT FEED"):
            if marker in section:
                section = section.split(marker, 1)[0]
        return section

    def _extract_pizza_spikes(self, live_section: str) -> list[dict[str, Any]]:
        spikes: list[dict[str, Any]] = []
        lines = [line.strip() for line in live_section.splitlines() if line.strip()]
        for idx, line in enumerate(lines):
            spike_match = re.search(r"(\d{2,4})%\s*SPIKE", line, flags=re.IGNORECASE)
            if not spike_match:
                continue
            spike_pct = self._safe_float(spike_match.group(1), None)
            if spike_pct is None:
                continue

            location = None
            for lookup in range(idx - 1, max(-1, idx - 4), -1):
                candidate = re.sub(r"^[#\-\s]+", "", lines[lookup]).strip()
                if "PIZZA" in candidate.upper():
                    location = candidate
                    break

            spikes.append(
                {
                    "location": location,
                    "spike_pct": round(spike_pct, 1),
                }
            )
        return spikes[:6]

    def _build_index_note(self, indices: dict[str, Any], weighted_pct: float) -> str:
        quotes = (indices or {}).get("quotes") or {}
        sse_pct = self._safe_float((quotes.get("000001.SH") or {}).get("pct"), None)
        cyb_pct = self._safe_float((quotes.get("399006.SZ") or {}).get("pct"), None)
        mode = str((indices or {}).get("mode") or "live")

        parts = []
        if sse_pct is not None:
            parts.append(f"上证 {sse_pct:+.2f}%")
        if cyb_pct is not None:
            parts.append(f"创业板 {cyb_pct:+.2f}%")
        if not parts:
            return ""

        prefix = "盘中" if mode == "live" else "收盘"
        if len(parts) == 2:
            return f"{prefix}双指数 {parts[0]} / {parts[1]}，均值 {weighted_pct:+.2f}%"
        return f"{prefix}{parts[0]}"

    def _normalize_macro_as_of(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        raw = str(value).strip()
        if not raw:
            return None
        for fmt in (
            "YYYY-MM-DD",
            "YYYY-MM-DD HH:mm:ss",
            "MMM D, YYYY",
            "MMM D YYYY",
            "YYYYMMDD",
        ):
            try:
                return arrow.get(raw, fmt).format("YYYY-MM-DD")
            except Exception:
                continue
        digits = "".join(ch for ch in raw if ch.isdigit())
        if len(digits) >= 8:
            try:
                return arrow.get(digits[:8], "YYYYMMDD").format("YYYY-MM-DD")
            except Exception:
                return digits[:8]
        return raw[:19]

    def _calc_stale_days(self, value: Any) -> Optional[int]:
        as_of = self._normalize_macro_as_of(value)
        if not as_of:
            return None
        try:
            return abs((arrow.now("Asia/Shanghai").date() - arrow.get(as_of).date()).days)
        except Exception:
            return None

    def _format_as_of_suffix(self, value: Any) -> str:
        as_of = self._normalize_macro_as_of(value)
        if not as_of:
            return ""
        return f" ({as_of})"

    def _safe_float(self, value: Any, default: Optional[float] = 0.0) -> Optional[float]:
        try:
            if value is None:
                return default
            if isinstance(value, str):
                value = value.replace("%", "").replace(",", "").strip()
            number = float(value)
            if math.isnan(number) or math.isinf(number):
                return default
            return number
        except Exception:
            return default

    def _clip(self, value: float, lower: float, upper: float) -> float:
        return max(lower, min(upper, value))


live_sentiment_monitor = LiveSentimentMonitor()

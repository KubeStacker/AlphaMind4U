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

from etl.calendar import trading_calendar
from strategy.sentiment.config import SENTIMENT_CONFIG

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

            ten_year = self._fetch_cnbc_ten_year()
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
    ) -> dict[str, Any]:
        now = arrow.now("Asia/Shanghai")
        market_status = "TRADING" if trading_calendar.is_trading_time() else "CLOSED"
        macro = self.refresh_macro_signals(force=force_macro_refresh)
        indices = self._get_live_indices(force=market_status == "TRADING")

        base_score = self._safe_float(
            (latest_sentiment or {}).get("value"),
            self._safe_float((latest_sentiment or {}).get("score"), 50.0),
        )
        base_label = str((latest_sentiment or {}).get("label") or self._score_to_label(base_score))

        weighted_pct = self._calc_weighted_index_move(indices)
        score_delta = self._clip(weighted_pct * 7.0, -12.0, 12.0)
        score_notes: list[str] = []
        if indices.get("available"):
            score_notes.append(f"A股盘中指数均值 {weighted_pct:+.2f}%")

        risk_score = 45.0 + self._clip(-weighted_pct * 10.0, -14.0, 14.0)
        ten_year_payload = macro.get("ten_year_yield") or {}
        pizza_payload = macro.get("pizza_index") or {}

        ten_year_adjustment, ten_year_risk, ten_year_note = self._apply_ten_year_signal(
            ten_year_payload,
            weighted_pct,
        )
        if ten_year_note:
            score_notes.append(ten_year_note)

        pizza_adjustment, pizza_risk, pizza_note = self._apply_pizza_signal(pizza_payload)
        if pizza_note:
            score_notes.append(pizza_note)

        score_delta += ten_year_adjustment + pizza_adjustment
        risk_score += ten_year_risk + pizza_risk

        live_score = round(self._clip(base_score + score_delta, 0.0, 100.0), 1)
        live_label = self._score_to_label(live_score)
        risk_score = round(self._clip(risk_score, 0.0, 100.0), 1)
        risk_prediction = self._build_risk_prediction(
            risk_score=risk_score,
            weighted_pct=weighted_pct,
            ten_year_payload=ten_year_payload,
            pizza_payload=pizza_payload,
        )

        sse_quote = (indices.get("quotes") or {}).get("000001.SH") or {}
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
            "external_signals": macro,
            "risk_prediction": risk_prediction,
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
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "quotes": {},
                "weighted_pct": 0.0,
            }
            if not trading_calendar.is_trading_time():
                self._live_cache = payload
                self._live_fetched_at = now_ts
                return payload

            codes = ["000001.SH", "000300.SH", "399001.SZ", "000688.SH"]
            try:
                from etl.sync import sync_engine

                quote_df = sync_engine.provider.realtime_quote(
                    ts_code=",".join(codes),
                    src="sina",
                )
            except Exception as exc:
                logger.warning("获取盘中指数快照失败: %s", exc)
                self._live_cache = payload
                self._live_fetched_at = now_ts
                return payload

            quotes: dict[str, Any] = {}
            if quote_df is not None and not quote_df.empty:
                for _, row in quote_df.iterrows():
                    quote = self._extract_quote_snapshot(row)
                    if not quote:
                        continue
                    ts_code = quote.get("ts_code")
                    if ts_code and quote.get("trade_date") == today:
                        quotes[ts_code] = quote

            payload = {
                "available": bool(quotes),
                "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                "quotes": quotes,
                "weighted_pct": self._calc_weighted_index_move({"quotes": quotes}),
            }
            self._live_cache = payload
            self._live_fetched_at = now_ts
            return payload

    def _fetch_cnbc_ten_year(self) -> dict[str, Any]:
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

        headers = {
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
                            "quote_time": str(
                                quote.get("quote_time")
                                or quote.get("last_time")
                                or quote.get("formatted_last_time")
                                or ""
                            ).strip()
                            or None,
                            "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                            "source_url": self.CNBC_QUOTE_PAGE,
                        }
                except Exception as exc:
                    error_message = str(exc)
                    continue

        fallback_quote = self._fetch_fred_ten_year_fallback(headers=headers)
        if fallback_quote:
            fallback_quote["error"] = error_message or "cnbc_blocked"
            return fallback_quote

        logger.warning("CNBC 十年期收益率抓取失败: %s", error_message or "unknown error")
        return {
            "available": False,
            "source": "CNBC",
            "yield": None,
            "change": None,
            "quote_time": None,
            "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
            "source_url": self.CNBC_QUOTE_PAGE,
            "error": error_message or "fetch_failed",
        }

    def _fetch_fred_ten_year_fallback(self, headers: dict[str, str]) -> Optional[dict[str, Any]]:
        try:
            request = Request(self.FRED_TEN_YEAR_CSV_URL, headers=headers)
            opener = build_opener(ProxyHandler({}))
            with opener.open(request, timeout=15) as response:
                raw_text = response.read().decode("utf-8", errors="ignore")
            lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
            for line in reversed(lines[1:]):
                try:
                    observation_date, value = line.split(",", 1)
                except ValueError:
                    continue
                yield_value = self._safe_float(value, None)
                if yield_value is None:
                    continue
                return {
                    "available": True,
                    "source": "FRED fallback",
                    "symbol": "DGS10",
                    "yield": yield_value,
                    "change": None,
                    "quote_time": observation_date,
                    "updated_at": arrow.now("Asia/Shanghai").format("YYYY-MM-DD HH:mm:ss"),
                    "source_url": self.FRED_TEN_YEAR_CSV_URL,
                    "fallback": True,
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
            doughcon_match = re.search(r"DOUGHCON\s*(\d+)", text, flags=re.IGNORECASE)
            locations_match = re.search(r"(\d+)\s+LOCATIONS\s+MONITORED", text, flags=re.IGNORECASE)
            spike_values = [
                float(v)
                for v in re.findall(r"(\d{2,4})%\s*SPIKE", text, flags=re.IGNORECASE)
                if self._safe_float(v, None) is not None
            ]
            doughcon = int(doughcon_match.group(1)) if doughcon_match else None
            max_spike = max(spike_values) if spike_values else None
            return {
                "available": doughcon is not None or max_spike is not None,
                "source": "PizzINT",
                "doughcon": doughcon,
                "max_spike_pct": max_spike,
                "locations_monitored": (
                    int(locations_match.group(1)) if locations_match else None
                ),
                "confidence": "low",
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
            "locations_monitored": None,
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

    def _apply_ten_year_signal(
        self,
        ten_year_payload: dict[str, Any],
        weighted_pct: float,
    ) -> tuple[float, float, str]:
        yield_value = self._safe_float((ten_year_payload or {}).get("yield"), None)
        if yield_value is None:
            return 0.0, 0.0, ""

        if yield_value >= self.ten_year_risk_high:
            return -8.0, 22.0, f"10Y {yield_value:.3f}% 已上穿 4.4%，外部风险偏压制"
        if yield_value >= self.ten_year_warn_high:
            return -4.0, 12.0, f"10Y {yield_value:.3f}% 接近 4.4%，需防估值再压缩"

        if yield_value <= self.ten_year_support_low:
            if weighted_pct >= 0.2:
                return 3.0, -8.0, f"10Y 回到 {yield_value:.3f}%，且A股同步偏强，情绪有修复空间"
            return -1.5, 6.0, f"10Y 回到 {yield_value:.3f}%，但A股未跟随，偏避险交易"

        if yield_value <= self.ten_year_warn_low:
            if weighted_pct >= 0:
                return 1.5, -3.0, f"10Y 回落到 4.3% 附近，风险偏好边际缓和"
            return -0.5, 2.0, f"10Y 回落到 4.3% 附近，但指数承接仍弱"

        return 0.0, 0.0, f"10Y 当前 {yield_value:.3f}%"

    def _apply_pizza_signal(
        self,
        pizza_payload: dict[str, Any],
    ) -> tuple[float, float, str]:
        if not pizza_payload or not pizza_payload.get("available"):
            return 0.0, 0.0, ""

        doughcon = self._safe_float(pizza_payload.get("doughcon"), None)
        max_spike = self._safe_float(pizza_payload.get("max_spike_pct"), None)

        if (
            (doughcon is not None and doughcon <= 2)
            or (max_spike is not None and max_spike >= self.pizza_spike_risk)
        ):
            return -3.0, 8.0, "Pizza 指数异常放大，按低置信度地缘噪声做风险观察"

        if (
            (doughcon is not None and doughcon <= 3)
            or (max_spike is not None and max_spike >= self.pizza_spike_warn)
        ):
            return -1.0, 3.0, "Pizza 指数偏热，只做弱风险提示，不单独决定仓位"

        return 0.0, 0.0, ""

    def _build_risk_prediction(
        self,
        risk_score: float,
        weighted_pct: float,
        ten_year_payload: dict[str, Any],
        pizza_payload: dict[str, Any],
    ) -> dict[str, Any]:
        level = "neutral"
        headline = "外部风险中性，继续按盘中承接判断。"
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
        if ten_year_value is not None:
            if ten_year_value >= self.ten_year_risk_high:
                reasons.append(f"CNBC 10Y {ten_year_value:.3f}% 已高于 4.4% 风险线。")
            elif ten_year_value >= self.ten_year_warn_high:
                reasons.append(f"CNBC 10Y {ten_year_value:.3f}% 正逼近 4.4% 风险线。")
            elif ten_year_value <= self.ten_year_support_low:
                reasons.append(f"CNBC 10Y {ten_year_value:.3f}% 已回到 4.3% 支撑带。")
            elif ten_year_value <= self.ten_year_warn_low:
                reasons.append(f"CNBC 10Y {ten_year_value:.3f}% 靠近 4.3% 观察带。")

        if weighted_pct >= 0.5:
            reasons.append(f"A股盘中指数均值 {weighted_pct:+.2f}%，承接偏强。")
        elif weighted_pct <= -0.5:
            reasons.append(f"A股盘中指数均值 {weighted_pct:+.2f}%，承接偏弱。")

        doughcon = self._safe_float((pizza_payload or {}).get("doughcon"), None)
        max_spike = self._safe_float((pizza_payload or {}).get("max_spike_pct"), None)
        if doughcon is not None:
            reasons.append(f"Pizza 指数 DOUGHCON {int(doughcon)}，仅作低置信度地缘旁证。")
        elif max_spike is not None:
            reasons.append(f"Pizza 指数最高门店热度 {max_spike:.0f}% SPIKE。")

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
            "quote_time": str(row_map.get("time") or "").strip() or None,
            "close": price,
            "pre_close": pre_close,
            "pct": round(pct, 3) if pct is not None else None,
        }

    def _calc_weighted_index_move(self, indices_payload: dict[str, Any]) -> float:
        quotes = (indices_payload or {}).get("quotes") or {}
        weights = {
            "000001.SH": 0.45,
            "000300.SH": 0.30,
            "399001.SZ": 0.15,
            "000688.SH": 0.10,
        }
        weighted = 0.0
        total = 0.0
        for code, weight in weights.items():
            pct = self._safe_float((quotes.get(code) or {}).get("pct"), None)
            if pct is None:
                continue
            weighted += pct * weight
            total += weight
        if total <= 0:
            return 0.0
        return round(weighted / total, 3)

    def _score_to_label(self, score: float) -> str:
        if score >= 85:
            return "沸腾"
        if score >= 70:
            return "高热"
        if score >= 55:
            return "修复"
        if score >= 42:
            return "拉锯"
        if score >= 25:
            return "低温"
        return "冰点"

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

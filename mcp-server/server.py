import logging
import os
from typing import Any

import httpx
import uvicorn
from mcp.server.fastmcp import FastMCP
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("jarvis-mcp")

JARVIS_API_BASE_URL = os.getenv("JARVIS_API_BASE_URL", "http://jarvis-backend:8000/admin").rstrip("/")
JARVIS_STATIC_AUTH_TOKEN = os.getenv("JARVIS_STATIC_AUTH_TOKEN", "").strip()
MCP_API_KEY = os.getenv("JARVIS_MCP_API_KEY", "").strip()
HOST = os.getenv("MCP_HOST", "0.0.0.0")
PORT = int(os.getenv("MCP_PORT", "8081"))
REQUEST_TIMEOUT = float(os.getenv("JARVIS_REQUEST_TIMEOUT", "20"))

if not MCP_API_KEY:
    raise RuntimeError("JARVIS_MCP_API_KEY is required for the public MCP server")


class ApiKeyMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, api_key: str):
        super().__init__(app)
        self.api_key = api_key

    async def dispatch(self, request: Request, call_next):
        if request.url.path in {"/health", "/"} or request.method == "OPTIONS":
            return await call_next(request)

        auth = request.headers.get("authorization", "")
        header_key = request.headers.get("x-mcp-api-key", "")
        bearer_key = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
        if self.api_key not in {bearer_key, header_key}:
            return JSONResponse({"error": "unauthorized"}, status_code=401)

        return await call_next(request)


def _headers() -> dict[str, str]:
    headers = {"Accept": "application/json"}
    if JARVIS_STATIC_AUTH_TOKEN:
        headers["Authorization"] = f"Bearer {JARVIS_STATIC_AUTH_TOKEN}"
    return headers


async def _get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    url = f"{JARVIS_API_BASE_URL}{path}"
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.get(url, params=params, headers=_headers())
    if response.status_code >= 400:
        detail = response.text[:500]
        raise RuntimeError(f"Jarvis API failed: {response.status_code} {detail}")
    return response.json()


def _clamp(value: int, min_value: int, max_value: int) -> int:
    return max(min_value, min(max_value, int(value)))


def _bounded_codes(codes: str | None, max_codes: int = 80) -> str | None:
    if not codes:
        return None
    parts = [part.strip() for part in codes.split(",") if part.strip()]
    if not parts:
        return None
    return ",".join(parts[:max_codes])


DAILY_PROBE_FIELD_DEFINITIONS: dict[str, dict[str, str]] = {
    "trade_date": {"meaning": "交易日", "unit": "YYYY-MM-DD"},
    "ts_code": {"meaning": "Tushare 标准股票代码", "unit": "无"},
    "name": {"meaning": "股票简称", "unit": "无"},
    "open": {"meaning": "开盘价", "unit": "元/股"},
    "high": {"meaning": "最高价", "unit": "元/股"},
    "low": {"meaning": "最低价", "unit": "元/股"},
    "close": {"meaning": "收盘价", "unit": "元/股"},
    "pre_close": {"meaning": "前收盘价", "unit": "元/股"},
    "change": {"meaning": "涨跌额，close - pre_close", "unit": "元/股"},
    "pct_chg": {"meaning": "涨跌幅", "unit": "%"},
    "volume": {"meaning": "成交量，对应 daily_price.vol", "unit": "手"},
    "total_money": {"meaning": "成交额，由 daily_price.amount * 1000 换算", "unit": "元"},
    "total_money_k": {"meaning": "成交额原始库字段 daily_price.amount", "unit": "千元"},
    "net_mf_vol": {"meaning": "主力净流入量", "unit": "手"},
    "net_inflow": {"meaning": "主力净流入金额，对应 stock_moneyflow.net_mf_amount", "unit": "万元"},
    "net_mf_ratio": {"meaning": "主力净流入占比", "unit": "%"},
    "rzye": {"meaning": "融资余额", "unit": "元"},
    "rzmre": {"meaning": "融资买入额", "unit": "元"},
    "rzche": {"meaning": "融资偿还额", "unit": "元"},
    "rqye": {"meaning": "融券余额", "unit": "元"},
    "rqmcl": {"meaning": "融券卖出量", "unit": "股"},
    "rzrqye": {"meaning": "融资融券余额", "unit": "元"},
    "rqyl": {"meaning": "融券余量", "unit": "股"},
    "ma5": {"meaning": "5 日收盘价均线", "unit": "元/股"},
    "ma10": {"meaning": "10 日收盘价均线", "unit": "元/股"},
    "ma20": {"meaning": "20 日收盘价均线", "unit": "元/股"},
    "ma60": {"meaning": "60 日收盘价均线", "unit": "元/股"},
    "ma120": {"meaning": "120 日收盘价均线", "unit": "元/股"},
    "high_250": {"meaning": "近 250 个交易日最高价", "unit": "元/股"},
    "vol_ma5": {"meaning": "5 日成交量均线", "unit": "手"},
    "avg_ret_60": {"meaning": "近 60 日平均日收益率", "unit": "%"},
    "vol_60": {"meaning": "近 60 日收益波动率", "unit": "%"},
    "rps_20": {"meaning": "20 日相对强度排名百分位", "unit": "0-100 分"},
    "rps_50": {"meaning": "50 日相对强度排名百分位", "unit": "0-100 分"},
    "rps_120": {"meaning": "120 日相对强度排名百分位", "unit": "0-100 分"},
    "rps_250": {"meaning": "250 日相对强度排名百分位", "unit": "0-100 分"},
    "turnover_rate": {"meaning": "换手率", "unit": "%"},
    "turnover_rate_f": {"meaning": "自由流通股换手率", "unit": "%"},
    "volume_ratio": {"meaning": "量比或相对成交量指标", "unit": "倍"},
    "pe": {"meaning": "市盈率 PE", "unit": "倍"},
    "pe_ttm": {"meaning": "滚动市盈率 PE TTM", "unit": "倍"},
    "pb": {"meaning": "市净率 PB", "unit": "倍"},
    "ps": {"meaning": "市销率 PS", "unit": "倍"},
    "ps_ttm": {"meaning": "滚动市销率 PS TTM", "unit": "倍"},
    "total_mv": {"meaning": "总市值", "unit": "万元"},
    "circ_mv": {"meaning": "流通市值", "unit": "万元"},
    "big_order_ratio": {"meaning": "大单资金强度/占比衍生指标", "unit": "比例"},
    "trend_score": {"meaning": "趋势因子评分", "unit": "0-100 分"},
    "liquidity_score": {"meaning": "流动性因子评分", "unit": "0-100 分"},
    "quality_score": {"meaning": "质量因子评分", "unit": "0-100 分"},
    "value_score": {"meaning": "估值因子评分", "unit": "0-100 分"},
    "flow_score": {"meaning": "资金流因子评分", "unit": "0-100 分"},
    "event_score": {"meaning": "事件/财报催化因子评分", "unit": "0-100 分"},
    "factor_score": {"meaning": "综合因子评分", "unit": "0-100 分"},
    "margin": {"meaning": "两融字段嵌套对象，字段同 rzye/rzmre/rzche/rqye/rqmcl/rzrqye/rqyl", "unit": "混合"},
    "factors": {"meaning": "因子字段嵌套对象，便于模型或工具一次读取 MA/RPS/估值/评分", "unit": "混合"},
}


mcp = FastMCP(
    "jarvis-quant",
    instructions=(
        "Jarvis Quant read-only MCP server. Tools expose bounded A-share market, "
        "stock, indicator, and strategy data from the Jarvis backend."
    ),
    host=HOST,
    port=PORT,
    streamable_http_path="/mcp",
    stateless_http=True,
)


@mcp.tool()
async def jarvis_search_stock(q: str = "", limit: int = 10) -> dict[str, Any]:
    """Search A-share stocks by code, Chinese name, pinyin, or abbreviation."""
    return await _get("/stock/search", {"q": q, "limit": _clamp(limit, 1, 20)})


@mcp.tool()
async def jarvis_get_stock_kline(ts_code: str, limit: int = 120) -> dict[str, Any]:
    """Get bounded daily K-line data for one stock."""
    return await _get(f"/stock/{ts_code}/kline", {"limit": _clamp(limit, 20, 300)})


@mcp.tool()
async def jarvis_get_stock_daily_probe(ts_code: str, days: int = 30) -> dict[str, Any]:
    """
    Get recent daily probe data for one A-share stock.

    The result contains the latest N trading days of OHLCV, amount, money flow,
    margin trading, and factor fields. Returned units are also included in
    `unit` and detailed field descriptions are included in `field_definitions`.
    `net_inflow` is in 10k CNY, `total_money` is CNY, `volume` is lots, and
    margin amount fields are CNY.
    """
    payload = await _get(f"/stock/{ts_code}/daily-probe", {"days": _clamp(days, 1, 260)})
    payload["field_definitions"] = DAILY_PROBE_FIELD_DEFINITIONS
    return payload


@mcp.tool()
async def jarvis_get_stock_indicators(ts_code: str, limit: int = 100) -> dict[str, Any]:
    """Get technical indicator summary and recent indicator history for one stock."""
    return await _get(f"/stock/{ts_code}/indicators", {"limit": _clamp(limit, 30, 180)})


@mcp.tool()
async def jarvis_get_market_suggestion(trade_date: str | None = None) -> dict[str, Any]:
    """Get Jarvis market environment suggestion for a trade date or latest date."""
    params = {}
    if trade_date:
        params["trade_date"] = trade_date
    return await _get("/market/suggestion", params)


@mcp.tool()
async def jarvis_get_mainline_leaders(trade_date: str | None = None, limit: int = 20) -> dict[str, Any]:
    """Get bounded mainline leader stocks and sector candidates."""
    params: dict[str, Any] = {"limit": _clamp(limit, 1, 30)}
    if trade_date:
        params["trade_date"] = trade_date
    return await _get("/mainline/leaders", params)


@mcp.tool()
async def jarvis_get_stock_mainline_analysis(ts_code: str) -> dict[str, Any]:
    """Get mainline/sector analysis for one stock."""
    return await _get(f"/stock/{ts_code}/mainline_analysis")


@mcp.tool()
async def jarvis_get_holdings() -> dict[str, Any]:
    """Get the current authenticated user's holdings and portfolio PnL summary."""
    return await _get("/users/me/holdings")


@mcp.tool()
async def jarvis_get_realtime_quotes(
    codes: str | None = None,
    src: str = "sina",
    include_analysis: bool = True,
    analysis_depth: str = "compact",
    sort_mode: str = "auto",
) -> dict[str, Any]:
    """Get user-scoped watchlist realtime quotes, optionally limited to comma-separated stock codes."""
    depth = str(analysis_depth or "compact").lower()
    if depth not in {"compact", "full"}:
        depth = "compact"

    sort = str(sort_mode or "auto").lower()
    if sort not in {"auto", "manual"}:
        sort = "auto"

    params: dict[str, Any] = {
        "src": src or "sina",
        "include_analysis": include_analysis,
        "analysis_depth": depth,
        "sort_mode": sort,
    }
    bounded_codes = _bounded_codes(codes, 80 if depth == "compact" else 50)
    if bounded_codes:
        params["codes"] = bounded_codes
    return await _get("/watchlist/realtime", params)


@mcp.tool()
async def jarvis_get_strategy_plaza_strategies() -> dict[str, Any]:
    """List available built-in Strategy Plaza strategies."""
    return await _get("/strategy-plaza/strategies")


@mcp.tool()
async def jarvis_get_strategy_plaza_summary(strategy_key: str, trade_date: str) -> dict[str, Any]:
    """Get daily summary for one Strategy Plaza strategy."""
    return await _get(
        "/strategy-plaza/summary",
        {"strategy_key": strategy_key, "trade_date": trade_date},
    )


@mcp.tool()
async def jarvis_get_strategy_plaza_observations(
    strategy_key: str,
    trade_date: str,
    limit: int = 50,
) -> dict[str, Any]:
    """Get bounded observation list for one Strategy Plaza strategy and date."""
    return await _get(
        "/strategy-plaza/observations",
        {"strategy_key": strategy_key, "trade_date": trade_date, "limit": _clamp(limit, 1, 80)},
    )


async def health(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "service": "jarvis-mcp",
            "jarvis_api_base_url": JARVIS_API_BASE_URL,
            "transport": "streamable-http",
            "mcp_path": "/mcp",
        }
    )


app = mcp.streamable_http_app()
app.add_route("/health", health, methods=["GET"])
app.add_middleware(ApiKeyMiddleware, api_key=MCP_API_KEY)


if __name__ == "__main__":
    logger.info("Starting Jarvis MCP server on %s:%s, backend=%s", HOST, PORT, JARVIS_API_BASE_URL)
    uvicorn.run(app, host=HOST, port=PORT, log_level=os.getenv("UVICORN_LOG_LEVEL", "info"))

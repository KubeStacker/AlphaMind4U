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

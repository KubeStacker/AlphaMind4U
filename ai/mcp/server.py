"""Jarvis MCP server: minimal, read-only access to collected DuckDB data."""

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

JARVIS_API_BASE_URL = os.getenv(
    "JARVIS_API_BASE_URL", "http://jarvis-backend:8000/admin"
).rstrip("/")
JARVIS_STATIC_AUTH_TOKEN = os.getenv("JARVIS_STATIC_AUTH_TOKEN", "").strip()
MCP_API_KEY = os.getenv("JARVIS_MCP_API_KEY", "").strip()
HOST = os.getenv("MCP_HOST", "0.0.0.0")
PORT = int(os.getenv("MCP_PORT", "8081"))
REQUEST_TIMEOUT = float(os.getenv("JARVIS_REQUEST_TIMEOUT", "20"))

if not MCP_API_KEY:
    raise RuntimeError("JARVIS_MCP_API_KEY is required for the public MCP server")
if not JARVIS_STATIC_AUTH_TOKEN:
    raise RuntimeError("JARVIS_STATIC_AUTH_TOKEN is required to access the backend")


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
    return {
        "Accept": "application/json",
        "Authorization": f"Bearer {JARVIS_STATIC_AUTH_TOKEN}",
    }


async def _request(method: str, path: str, **kwargs) -> dict[str, Any]:
    url = f"{JARVIS_API_BASE_URL}{path}"
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:
        response = await client.request(method, url, headers=_headers(), **kwargs)
    if response.status_code >= 400:
        raise RuntimeError(
            f"Jarvis data API failed: {response.status_code} {response.text[:500]}"
        )
    return response.json()


mcp = FastMCP(
    "jarvis-data",
    instructions=(
        "Read-only access to raw A-share datasets collected into Jarvis DuckDB. "
        "The tools do not calculate indicators, scores, recommendations, or forecasts."
    ),
    host=HOST,
    port=PORT,
    streamable_http_path="/mcp",
    stateless_http=True,
)


@mcp.tool()
async def jarvis_list_datasets() -> dict[str, Any]:
    """List the raw collected datasets that may be queried."""
    return await _request("GET", "/datasets")


@mcp.tool()
async def jarvis_describe_dataset(dataset: str) -> dict[str, Any]:
    """Describe columns and types for one allowed raw dataset."""
    return await _request("GET", f"/datasets/{dataset}/schema")


@mcp.tool()
async def jarvis_query_dataset(
    dataset: str,
    columns: list[str] | None = None,
    filters: dict[str, Any] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    order_by: str | None = None,
    descending: bool = False,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """Query rows from one allowed raw dataset without SQL, joins, or aggregation."""
    payload = {
        "dataset": dataset,
        "columns": columns,
        "filters": filters or {},
        "start_date": start_date,
        "end_date": end_date,
        "order_by": order_by,
        "descending": descending,
        "limit": max(1, min(1000, int(limit))),
        "offset": max(0, min(1_000_000, int(offset))),
    }
    return await _request("POST", "/datasets/query", json=payload)


async def health(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "service": "jarvis-data-mcp",
            "jarvis_api_base_url": JARVIS_API_BASE_URL,
            "transport": "streamable-http",
            "mcp_path": "/mcp",
        }
    )


app = mcp.streamable_http_app()
app.add_route("/health", health, methods=["GET"])
app.add_middleware(ApiKeyMiddleware, api_key=MCP_API_KEY)


if __name__ == "__main__":
    logger.info("Starting Jarvis data MCP server on %s:%s", HOST, PORT)
    uvicorn.run(app, host=HOST, port=PORT, log_level=os.getenv("UVICORN_LOG_LEVEL", "info"))

# AGENTS.md - Jarvis-Quant Development Guide

## Project Overview

Jarvis-Quant is an A-share quantitative trading decision system with FastAPI backend (Python 3.12) and Vue 3 frontend.

## Build & Development Commands

### Backend

```bash
# Install dependencies
cd backend
pip install -r requirements.txt

# Run development server (inside Docker or locally)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# For heavy ETL/recalculate tasks, prefer single process (avoid reload workers)
uvicorn main:app --host 0.0.0.0 --port 8000

# Run with Docker
docker-compose up backend
```

### Frontend

```bash
cd frontend
npm install
npm run dev      # Development server
npm run build    # Production build
npm run preview  # Preview production build
```

### Running with Docker Compose

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f backend
docker-compose logs -f frontend

# Restart a service
docker-compose restart backend
```

### Testing

Currently there are **no formal unit tests** in this project. To test functionality:

```bash
# Test API endpoints using curl (see below)
curl http://localhost:8000/

# Check system status
curl http://localhost:8000/system/status
```

## Database Access (Important)

**DuckDB uses an embedded single-process model in this project**. To reduce `Unique file handle conflict` and lock contention:

- Backend uses a **process-level shared DuckDB connection** (singleton) with serialized access.
- Do **not** open direct ad-hoc DuckDB sessions from other processes for routine reads.
- For diagnostics/querying during development, prefer **HTTP API + curl** (`/admin/db/query`) instead of direct DB exec.

### Query Database via API

```bash
# Execute read-only SQL query
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM stock_basic LIMIT 5"}'

# Get market sentiment
curl "http://localhost:8000/admin/market_sentiment?days=30"

# Get data integrity report
curl "http://localhost:8000/admin/integrity?start_date=2025-01-01&end_date=2025-12-31"

# Unified ETL sync entry (examples)
curl -X POST http://localhost:8000/admin/etl/sync \
  -H "Content-Type: application/json" \
  -d '{"task":"daily","years":1}'

# Unified sentiment entry
curl -X POST "http://localhost:8000/admin/etl/sentiment?days=365&sync_index=false"
```

### Available System Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check |
| `/system/status` | GET | Market status (trading/closed) |
| `/system/db_check` | GET | Analyze panic day pathology |
| `/system/trigger_daily_sync` | GET | Trigger daily data sync |
| `/system/backfill_history` | GET | Backfill historical data |
| `/admin/db/query` | POST | Execute SELECT queries |
| `/admin/etl/sync` | POST | Unified ETL sync trigger (`task`-driven) |
| `/admin/etl/sentiment` | POST | Unified sentiment trigger (`days`, `sync_index`) |
| `/admin/tasks/status` | GET | Task queue status |
| `/admin/market_sentiment` | GET | Market sentiment history |
| `/admin/integrity` | GET | Data integrity report |

## Code Style Guidelines

### Python (Backend)

- **Imports**: Group in order: stdlib → third-party → local
- **Formatting**: Use Black-compatible style (max line length 100)
- **Types**: Use Pydantic models for all API request/response schemas
- **Naming**:
  - `snake_case` for functions, variables, database columns
  - `PascalCase` for classes, Pydantic models
  - `UPPER_SNAKE_CASE` for constants
- **Error Handling**: Use try/except with logging, raise HTTPException for API errors
- **Database**: Use the shared connection helpers in `db/connection.py`; avoid opening new raw `duckdb.connect()` in business modules
- **Async**: Use `async/await` for FastAPI endpoints, wrap sync functions with `asyncio.to_thread()`

### Vue 3 (Frontend)

- **Style**: Composition API with `<script setup>` syntax
- **State**: Use Pinia for global state management
- **HTTP**: Use axios for API calls
- **Components**: Follow Vue 3 naming conventions (PascalCase)

### General

- **Logging**: Use Python's `logging` module, include timestamps (Shanghai timezone)
- **Configuration**: Use `.env` files, access via `os.getenv()` or pydantic-settings
- **Dates**: Use `arrow` library for date handling, store as DATE type in DuckDB

## Project Structure

```
/root/jarvis
├── backend/
│   ├── api/           # FastAPI routes (admin.py, auth.py)
│   ├── db/            # Database connection & schema
│   ├── etl/           # Data sync tasks & providers
│   │   ├── tasks/    # Individual data tasks
│   │   ├── providers/# Data source providers (Tushare)
│   │   └── utils/    # Utilities (backfill, factors)
│   ├── strategy/      # Quant strategies
│   │   ├── sentiment/# Market sentiment analysis
│   │   └── mainline/ # Mainstream sector analysis
│   └── main.py       # FastAPI app entry point
├── frontend/         # Vue 3 + Vite + TailwindCSS
├── docs/             # Documentation
└── data/             # DuckDB database file
```

## Key Technologies

- **Backend**: FastAPI, uvicorn, pandas, duckdb, pydantic, tushare
- **Frontend**: Vue 3, Vite, Pinia, Vue Router, ECharts, Axios, TailwindCSS
- **Database**: DuckDB (analytics-focused, not for high-concurrency writes)
- **Scheduling**: APScheduler for periodic tasks

## Common Development Tasks

### Add a new ETL task

1. Create task class in `backend/etl/tasks/`
2. Inherit from base task or follow existing patterns
3. Register in `sync.py` or expose via API endpoint

### Add a new API endpoint

1. Add route in `backend/api/admin.py` or create new router file
2. Use Pydantic models for request/response validation
3. Use `get_db_connection()` for database operations

### Modify database schema

1. Create tables via API: `POST /system/create_tables`
2. Or modify `db/schema.py` and restart backend

## Notes

- DuckDB has concurrency limitations - prefer API-based reads (`curl /admin/db/query`) and avoid out-of-process direct DB access
- Use the task queue (`/admin/tasks/status`) for long-running operations
- Tushare API has rate limits - implement retry logic with tenacity
- Market data updates happen after trading hours (16:00+ Shanghai time)

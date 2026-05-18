# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Spec-driven development

This project uses spec-driven development. 5 modules map to UI tabs:

| Module Spec | Tab |
|-------------|-----|
| `spec/modules/watchlist/spec.md` | Watchlist (Tab1) |
| `spec/modules/dashboard/spec.md` | Dashboard (Tab2) |
| `spec/modules/strategy/spec.md` | Strategy Plaza (Tab3) |
| `spec/modules/data-sync/spec.md` | Data Sync |
| `spec/modules/basic/spec.md` | Settings/Basic |

Each module spec covers both frontend and backend in one file.

**Change workflow**: use the `spec-writer` skill → clarify intent → draft spec → user confirms → implement. Cross-cutting constraints (DuckDB, Tushare, design rules) live in `spec/concepts/`.


## Commands

```bash
# Backend — code auto-syncs into Docker, uvicorn --reload handles the rest
docker-compose up backend

# Frontend — auto-syncs too, dev server on port 8080
docker-compose up frontend

# Full stack
docker-compose up -d

# Verify backend changes via curl (no need to restart containers)
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" -d '{"sql": "SELECT 1"}'
```

No formal test suite; validate features by curling the API. Frontend changes can be verified in a headless browser (login: `yuanpeng` / `1qaz2wsx`).

## Database access

DuckDB (embedded, single-process). **Never** open `jarvis.duckdb` directly from the host. All queries go through the HTTP API:

```bash
curl -X POST http://localhost:8000/admin/db/query \
  -H "Content-Type: application/json" -d '{"sql": "SELECT ..."}'
```

The backend uses a process-level shared DuckDB connection (singleton). If a Pandas DataFrame is registered on it (`con.register()`), it must be unregistered after use.

## Architecture

Jarvis-Quant is an A-share quantitative trading decision system. FastAPI + Vue 3, containerized with Docker Compose.

**Three-tier strategy engine:**
1. **Sentiment** (`backend/strategy/sentiment/`) — answers "should we trade today?" Market breadth, limit-up/down stats, adaptive sentiment scoring.
2. **Mainline** (`backend/strategy/mainline/`) — answers "which sectors?" Concept resonance clustering, sector rotation tracking, leaderboard with leader stocks.
3. **Strategy Plaza** (`backend/strategy/plaza/builtin/`) — answers "which specific setups?" Currently three local-indicator strategies: head7_dragon_return, single_yang_hold, golden_eye.

**Data pipeline:**
- ETL tasks in `backend/etl/tasks/` fetch from Tushare Pro (A-share data provider) and write to DuckDB.
- `backend/etl/sync.py` dispatches; `backend/etl/scheduler.py` handles cron-like scheduling via APScheduler.
- `backend/etl/providers/tushare_pro.py` wraps the Tushare API. Current token has 2000 points — verify interface availability before implementation.
- Concept sync uses staging tables (`__staging` suffix) with atomic publish to prevent readers from seeing partial data.

**API layer:**
- `backend/main.py` — app factory with lifespan. Registers sub-routers under `/admin` prefix.
- `backend/api/routes/` — domain-split route modules (users, stocks, market, etl, db, system, docs, strategy_plaza, ai).
- `backend/api/auth.py` — JWT-based auth, not mounted under `/admin`.

**Watchlist/levels engine:**
- Support/resistance levels are computed via multi-source rolling-window resonance (MAs, swing highs/lows, trendlines, gaps, volume profiles) with ATR/amplitude normalization and board-specific calibration (main board vs ChiNext vs STAR).
- Compact realtime endpoint (`/admin/watchlist/realtime?analysis_depth=compact`) for batch polling; deep analysis endpoint (`/admin/watchlist/{ts_code}/analysis`) for detail.
- K-line pattern engine in `backend/etl/utils/kline_patterns.py` — pattern recognition is supplementary to the main signal pipeline (trend/volume/capital flow/factor scores dominate).

**Frontend:**
- Vue 3 + Composition API (`<script setup>`), Pinia, Vue Router, ECharts, TailwindCSS.
- Design rules: restrained palette (low-saturation warm for sentiment, cool for mainline; **never** use purple/indigo gradients), information-dense cards, one fact one place, no decorative summaries.

## Key constraints

- **Container-first**: code lives in Docker. Edit on host, uvicorn `--reload` picks it up. Do not `npm run build` or `pip install` on the host.
- **Fix at source**: when asked to change UI text, prefer modifying the backend data generator (e.g. `get_professional_commentary_detailed`) over frontend string filtering.
- **Tushare rate limits**: use tenacity retry logic. High-point interfaces (like `stk_factor`) may be unavailable.
- **Daily data units**: `vol` = lots (手), `amount` = thousands of CNY (千元). Convert `amount / 1e5` for display in hundred-millions (亿).
- **Stock codes**: format is `ts_code` like `600519.SH`, `300750.SZ`. North Exchange (BJ) stocks are excluded from leader rankings.
- **Watchlist**: user-scoped via `user_id + ts_code` composite PK. Authentication required (`Bearer` token).

## Project structure (top-level)

```
backend/
  main.py                 — FastAPI app with lifespan
  api/routes/             — domain routers (stocks, market, etl, db, system, docs, plaza, ai, users)
  api/auth.py             — JWT auth (outside /admin prefix)
  db/connection.py        — shared DuckDB singleton
  db/schema.py            — table DDL
  etl/tasks/              — per-task ETL (stock_basic, daily, concepts, factors, financials, etc.)
  etl/providers/tushare_pro.py — Tushare API wrapper
  etl/utils/              — kline_patterns, factors, scoring, backfill, technical_indicators
  strategy/sentiment/     — market sentiment analysis
  strategy/mainline/      — sector mainline analysis
  strategy/plaza/builtin/ — strategy plaza built-in strategies
  strategy/watchlist/     — watchlist recommendation engine
  tests/                  — pytest tests
frontend/
  src/
    main.js               — Vue app entry
    router/index.js       — routes
    stores/auth.js        — Pinia auth store
    services/api.js       — axios API layer
    composables/          — useKlineChart, useStockSearch, watchlistHoldings, etc.
```

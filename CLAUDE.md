# CLAUDE.md

This file is the authoritative agent guidance for this repository.

## Project scope

Jarvis is intentionally limited to two capabilities:

1. Collect raw A-share data from Tushare into DuckDB.
2. Expose allowlisted, read-only database rows through a minimal MCP server.

Do not add frontend, watchlist, holdings, strategies, scoring, indicators,
recommendations, forecasts, AI analysis, or arbitrary SQL endpoints.

## Spec-driven changes

- Data collection: `spec/modules/data-sync/spec.md`
- MCP database access: `spec/modules/mcp/spec.md`
- Cross-cutting constraints: `spec/concepts/`

For feature changes, use `ai/skills/spec-writer`, update the relevant spec, obtain
confirmation, then implement.

## Runtime

```bash
docker compose up backend
docker compose -f docker-compose.mcp.yml up mcp-server
```

The backend exposes:

- `POST /admin/etl/sync` — enqueue a raw collection task.
- `GET /admin/tasks/status` — inspect collection jobs.
- `GET /admin/datasets` — list MCP-readable datasets.
- `GET /admin/datasets/{name}/schema` — describe a dataset.
- `POST /admin/datasets/query` — bounded, parameterized single-table reads.

All `/admin` endpoints require `Authorization: Bearer <JARVIS_STATIC_AUTH_TOKEN>`.

## DuckDB rules

- The backend owns one process-level shared DuckDB connection.
- Other processes, including MCP, must never open `jarvis.duckdb` directly.
- MCP calls the backend data API.
- Registered DataFrames must always be unregistered, including error paths.
- Do not DROP legacy tables or delete historical database files during cleanup.

## Tushare rules

- The configured token has about 2000 points; verify endpoint availability.
- Keep bounded retry/rate-limit behavior.
- Store provider fields with only schema, date, and unit normalization.
- Do not turn collected data into scores, signals, advice, or predictions.

## Structure

```text
backend/
  main.py
  api/routes/datasets.py   # allowlisted database reads
  api/routes/etl.py        # collection queue API
  core/                    # config, logging, security
  db/                      # shared DuckDB connection and raw schema
  etl/providers/           # Tushare adapter
  etl/tasks/               # raw collection tasks
  etl/sync.py              # collection orchestration
  etl/scheduler.py         # collection schedules
  etl/utils/               # safe write, backfill, quality
ai/
  mcp/
    server.py              # exactly three generic read-only data tools
  skills/                  # agent skills (spec-writer, etc.)
data/                      # persistent DuckDB data
scripts/                   # deploy, restart, init scripts
spec/                      # spec-driven documents
```

## Validation

- Prefer container validation; do not install dependencies on the host.
- Verify that only the intended routes and exactly three MCP tools are registered.
- Test allowlist rejection as well as successful bounded reads.

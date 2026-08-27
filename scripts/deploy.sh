#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_dir"

docker compose build backend
docker compose up -d --force-recreate backend
docker compose --env-file .env.mcp -f docker-compose.mcp.yml up -d --build mcp-server

echo "Jarvis data backend and MCP server are running."

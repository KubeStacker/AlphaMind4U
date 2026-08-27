#!/bin/sh
set -eu

if ! command -v docker >/dev/null 2>&1; then
    echo "Docker is required. Install Docker Engine with the Compose plugin first." >&2
    exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
    echo "The Docker Compose plugin is required." >&2
    exit 1
fi

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
mkdir -p "$project_dir/data"

echo "Host prerequisites are ready. Configure backend/.env and .env.mcp before deployment."

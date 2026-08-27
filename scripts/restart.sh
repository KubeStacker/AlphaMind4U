#!/bin/sh
set -eu

project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_dir"

echo "Rebuilding Jarvis backend..."
docker compose build backend

echo "Restarting Jarvis backend container..."
docker compose up -d --force-recreate backend

echo "Jarvis backend has been restarted with the latest code."

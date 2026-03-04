#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# MongoDB Document Seeder — Container Entrypoint
#
# This is a STANDALONE SEEDER ENGINE.
# Seeding is NOT triggered at startup — it is triggered on-demand by external
# regulation repos via:
#   • CLI:  python -m src.cli seed /path/to/their/seed.yaml
#   • HTTP: POST /api/seed/bundle  or  POST /api/seed/manifest
#
# This container only starts the FastAPI server that serves those endpoints.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

echo "=== MongoDB Document Seeder API Server Starting ==="
echo "[startup] MONGO_URI=${MONGO_URI:-mongodb://localhost:27017}"
echo "[startup] MONGO_DB_NAME=${MONGO_DB_NAME:-doc_management}"

exec "$@"

#!/bin/bash
set -e

echo "Starting deployment seeding process..."
# Automatically seed the production bundles
python -m src.cli seed seeds/seed.yaml || echo "Seeding completed or encountered non-fatal errors (e.g., manifest missing)."

echo "Starting Gunicorn API server..."
# Execute the original CMD
exec "$@"

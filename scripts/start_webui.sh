#!/usr/bin/env bash
set -eu

PROJECT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$PROJECT_DIR"
exec "$PROJECT_DIR/.venv/bin/python" -m streamlit run "$PROJECT_DIR/app.py" \
  --server.address 0.0.0.0 --server.port 8501 --server.headless true \
  --browser.gatherUsageStats false "$@"

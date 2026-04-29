#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON_BIN="${PYTHON_BIN:-python3}"
PORT="${PORT:-8876}"
VENV_DIR="${VENV_DIR:-$SCRIPT_DIR/.venv}"

if [ ! -d "$VENV_DIR" ]; then
  "$PYTHON_BIN" -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip >/dev/null
python -m pip install -r "$SCRIPT_DIR/requirements.txt"

if [ -f "$SCRIPT_DIR/.env" ]; then
  echo "Loaded API settings from $SCRIPT_DIR/.env"
else
  echo "No .env found. You can still paste your API key into the dashboard UI."
fi

echo "Starting Insider Form 4 dashboard on http://127.0.0.1:${PORT}"
python "$SCRIPT_DIR/dashboard_server.py" --port "$PORT"

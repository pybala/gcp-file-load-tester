#!/usr/bin/env bash
# =============================================================================
# start_api.sh — Start the Data Validator Flask API server
#
# Usage:
#   ./start_api.sh              # default port 8000, with BigQuery writes
#   ./start_api.sh --no-bq      # skip BigQuery writes (no GCP credentials needed)
#   ./start_api.sh --port 9000  # use a different port
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"
ENV_FILE="$SCRIPT_DIR/.env"

# ---------------------------------------------------------------------------
# 1. Check venv exists
# ---------------------------------------------------------------------------
if [ ! -f "$VENV_PYTHON" ]; then
  echo "✗  Virtual environment not found."
  echo "   Run: cd $SCRIPT_DIR && make setup"
  exit 1
fi

# ---------------------------------------------------------------------------
# 2. Load .env (export all variables into this shell session)
# ---------------------------------------------------------------------------
if [ -f "$ENV_FILE" ]; then
  echo "▶  Loading environment from $ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "⚠  No .env file found — BigQuery writes may fail without credentials."
  echo "   Copy .env.example → .env and fill in your values."
fi

# ---------------------------------------------------------------------------
# 3. Parse optional flags
# ---------------------------------------------------------------------------
PORT=${PORT:-8000}
HOST=${HOST:-0.0.0.0}
NO_BQ=""

for arg in "$@"; do
  case $arg in
    --no-bq)      NO_BQ="--no-metadata" ;;
    --port=*)     PORT="${arg#*=}" ;;
    --port)       shift; PORT="$1" ;;
    --host=*)     HOST="${arg#*=}" ;;
    --host)       shift; HOST="$1" ;;
  esac
done

# ---------------------------------------------------------------------------
# 4. Start the server
# ---------------------------------------------------------------------------
echo "▶  Starting Data Validator API server"
echo "   Python : $VENV_PYTHON"
echo "   URL    : http://$HOST:$PORT"
if [ -n "$NO_BQ" ]; then
  echo "   Mode   : local only (BigQuery writes disabled)"
else
  echo "   Mode   : full (BigQuery writes enabled)"
  echo "   Creds  : ${GOOGLE_APPLICATION_CREDENTIALS:-<not set — BQ writes will fail>}"
fi
echo ""

cd "$SCRIPT_DIR"
exec "$VENV_PYTHON" api_server.py --host "$HOST" --port "$PORT" $NO_BQ
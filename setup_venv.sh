#!/usr/bin/env bash
# =============================================================================
# setup_venv.sh
# -------------
# Creates a Python virtual environment, installs all dependencies, and
# optionally activates it in the current shell session.
#
# Usage:
#   bash setup_venv.sh            # create + install (do NOT activate)
#   source setup_venv.sh          # create + install + activate in current shell
#
# Requirements:
#   - Python 3.10+ installed and on $PATH
#   - pip available
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
VENV_DIR=".venv"
PYTHON_MIN_MAJOR=3
PYTHON_MIN_MINOR=13
PYTHON_PREFERRED="python3.13"
REQUIREMENTS_FILE="requirements.txt"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { echo "▶  $*"; }
ok()   { echo "✓  $*"; }
warn() { echo "⚠  $*" >&2; }
err()  { echo "✗  $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Detect Python interpreter
# ---------------------------------------------------------------------------
log "Detecting Python interpreter..."

# Prefer explicit python3.13, fall back to python3 / python
if command -v "${PYTHON_PREFERRED}" &>/dev/null; then
  PYTHON="${PYTHON_PREFERRED}"
elif command -v python3 &>/dev/null; then
  PYTHON="python3"
elif command -v python &>/dev/null; then
  PYTHON="python"
else
  err "Python not found. Please install Python ${PYTHON_MIN_MAJOR}.${PYTHON_MIN_MINOR}+."
fi

PYTHON_VERSION=$("$PYTHON" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MAJOR=$("$PYTHON" -c "import sys; print(sys.version_info.major)")
PYTHON_MINOR=$("$PYTHON" -c "import sys; print(sys.version_info.minor)")

if [ "$PYTHON_MAJOR" -lt "$PYTHON_MIN_MAJOR" ] || \
   ([ "$PYTHON_MAJOR" -eq "$PYTHON_MIN_MAJOR" ] && [ "$PYTHON_MINOR" -lt "$PYTHON_MIN_MINOR" ]); then
  err "Python ${PYTHON_MIN_MAJOR}.${PYTHON_MIN_MINOR}+ is required. Found: ${PYTHON_VERSION}"
fi

ok "Python ${PYTHON_VERSION} — $(command -v "$PYTHON")"

# ---------------------------------------------------------------------------
# 2. Create virtual environment
# ---------------------------------------------------------------------------
if [ -d "$VENV_DIR" ]; then
  warn "Virtual environment already exists at '${VENV_DIR}'. Skipping creation."
  warn "To recreate, run: rm -rf ${VENV_DIR} && bash setup_venv.sh"
else
  log "Creating virtual environment at '${VENV_DIR}'..."
  "$PYTHON" -m venv "$VENV_DIR"
  ok "Virtual environment created."
fi

# ---------------------------------------------------------------------------
# 3. Upgrade pip inside the venv
# ---------------------------------------------------------------------------
log "Upgrading pip..."
"${VENV_DIR}/bin/pip" install --quiet --upgrade pip
ok "pip upgraded."

# ---------------------------------------------------------------------------
# 4. Install project dependencies
# ---------------------------------------------------------------------------
if [ ! -f "$REQUIREMENTS_FILE" ]; then
  err "Requirements file not found: ${REQUIREMENTS_FILE}"
fi

log "Installing dependencies from ${REQUIREMENTS_FILE}..."
"${VENV_DIR}/bin/pip" install --quiet -r "$REQUIREMENTS_FILE"
ok "Dependencies installed."

# ---------------------------------------------------------------------------
# 5. Copy .env if not present
# ---------------------------------------------------------------------------
if [ -f ".env.example" ] && [ ! -f ".env" ]; then
  cp .env.example .env
  ok "Copied .env.example → .env (edit it with your project values)."
fi

# ---------------------------------------------------------------------------
# 6. Activation instructions (or auto-activate if sourced)
# ---------------------------------------------------------------------------
echo ""
echo "════════════════════════════════════════════════════"
echo "  Setup complete!"
echo "════════════════════════════════════════════════════"

# Detect if this script was sourced (activation possible) or executed
if [[ "${BASH_SOURCE[0]}" != "${0}" ]]; then
  # Script was sourced — activate the venv in the current shell
  # shellcheck disable=SC1091
  source "${VENV_DIR}/bin/activate"
  ok "Virtual environment activated: $(command -v python)"
else
  # Script was executed — print activation instructions
  echo ""
  echo "  To activate the virtual environment, run:"
  echo "    source ${VENV_DIR}/bin/activate"
  echo ""
  echo "  Then run the validator:"
  echo "    python main.py --config config/validation_config.yaml"
  echo ""
  echo "  Or use the Makefile:"
  echo "    make run"
  echo ""
fi
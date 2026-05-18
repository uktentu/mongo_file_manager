#!/usr/bin/env bash
# =============================================================================
#  run.sh — MongoDB Document Seeder  |  Startup orchestration script
#
#  What this script does (in order):
#    1. Checks prerequisites (Python, venv, .env)
#    2. Kills any existing process on port 3089
#    3. Purges all __pycache__ and .pyc files
#    4. Activates the virtual environment
#    5. Launches the app via nohup → output.log
#    6. Tails the log and confirms the server is up
#
#  Usage:
#    chmod +x run.sh
#    ./run.sh
# =============================================================================

set -euo pipefail

# ── Config ────────────────────────────────────────────────────────────────────
PORT=3089
APP_MODULE="src.api:app"
LOG_FILE="output.log"
VENV_DIR=".venv"
PID_FILE=".seeder.pid"
STARTUP_TIMEOUT=15        # seconds to wait for server to become ready

# ── Colours & print helpers ───────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

print_banner() {
    echo ""
    echo -e "${CYAN}${BOLD}"
    echo "  ███╗   ███╗ ██████╗ ███╗   ██╗ ██████╗  ██████╗      ██████╗  ██████╗ "
    echo "  ████╗ ████║██╔═══██╗████╗  ██║██╔════╝ ██╔═══██╗     ██╔══██╗██╔════╝ "
    echo "  ██╔████╔██║██║   ██║██╔██╗ ██║██║  ███╗██║   ██║     ██║  ██║██████╗  "
    echo "  ██║╚██╔╝██║██║   ██║██║╚██╗██║██║   ██║██║   ██║     ██║  ██║██╔══██╗ "
    echo "  ██║ ╚═╝ ██║╚██████╔╝██║ ╚████║╚██████╔╝╚██████╔╝     ██████╔╝╚██████╔╝"
    echo "  ╚═╝     ╚═╝ ╚═════╝ ╚═╝  ╚═══╝ ╚═════╝  ╚═════╝      ╚═════╝  ╚═════╝ "
    echo -e "${RESET}"
    echo -e "${BOLD}       MongoDB Document Seeder  —  Regulatory Bundle Versioning Engine${RESET}"
    echo ""
}

print_step() {
    local step="$1"
    local msg="$2"
    echo -e "${BLUE}${BOLD}[STEP $step]${RESET} ${BOLD}$msg${RESET}"
}

print_ok() {
    echo -e "  ${GREEN}✔${RESET}  $1"
}

print_warn() {
    echo -e "  ${YELLOW}⚠${RESET}  $1"
}

print_err() {
    echo -e "  ${RED}✖${RESET}  $1"
}

print_info() {
    echo -e "  ${CYAN}→${RESET}  $1"
}

print_divider() {
    echo -e "${BLUE}──────────────────────────────────────────────────────────────────${RESET}"
}

# ── Step 0: Banner & preflight ────────────────────────────────────────────────
print_banner
print_divider
echo -e "  ${BOLD}Port       :${RESET} $PORT"
echo -e "  ${BOLD}App module :${RESET} $APP_MODULE"
echo -e "  ${BOLD}Log file   :${RESET} $LOG_FILE"
echo -e "  ${BOLD}Started at :${RESET} $(date '+%Y-%m-%d %H:%M:%S')"
print_divider
echo ""

# ── Step 1: Prerequisites ─────────────────────────────────────────────────────
print_step 1 "Checking prerequisites"

if ! command -v python3 &>/dev/null; then
    print_err "python3 not found — install Python 3.9+ and retry"
    exit 1
fi
PYTHON_VER=$(python3 --version 2>&1)
print_ok "Python found: $PYTHON_VER"

if [ ! -d "$VENV_DIR" ]; then
    print_warn "Virtual environment '$VENV_DIR' not found — creating it now"
    python3 -m venv "$VENV_DIR"
    print_ok "Created virtual environment at $VENV_DIR"
fi
print_ok "Virtual environment present: $VENV_DIR"

if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        print_warn ".env not found — copying .env.example → .env (review before production use)"
        cp .env.example .env
        print_ok "Created .env from .env.example"
    else
        print_err ".env file not found and no .env.example to copy from. Cannot start."
        exit 1
    fi
fi
print_ok ".env configuration file present"
echo ""

# ── Step 2: Kill existing process on port ─────────────────────────────────────
print_step 2 "Freeing port $PORT"

PIDS_ON_PORT=$(lsof -ti tcp:"$PORT" 2>/dev/null || true)
if [ -n "$PIDS_ON_PORT" ]; then
    print_info "Found process(es) on port $PORT: PID(s) $PIDS_ON_PORT"
    echo "$PIDS_ON_PORT" | xargs kill -9 2>/dev/null || true
    sleep 0.5
    STILL_UP=$(lsof -ti tcp:"$PORT" 2>/dev/null || true)
    if [ -n "$STILL_UP" ]; then
        print_err "Could not kill all processes on port $PORT (PID: $STILL_UP) — may need sudo"
        exit 1
    fi
    print_ok "Killed existing process(es) on port $PORT"
else
    print_ok "Port $PORT is free — no existing process to kill"
fi

# Remove stale PID file if any
if [ -f "$PID_FILE" ]; then
    rm -f "$PID_FILE"
    print_info "Removed stale PID file $PID_FILE"
fi
echo ""

# ── Step 3: Clean __pycache__ and .pyc files ──────────────────────────────────
print_step 3 "Purging __pycache__ and .pyc files"

CACHE_COUNT=$(find . -type d -name "__pycache__" -not -path "./.venv/*" | wc -l | tr -d ' ')
PYC_COUNT=$(find . -name "*.pyc" -not -path "./.venv/*" | wc -l | tr -d ' ')

if [ "$CACHE_COUNT" -gt 0 ] || [ "$PYC_COUNT" -gt 0 ]; then
    find . -type d -name "__pycache__" -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
    find . -name "*.pyc" -not -path "./.venv/*" -delete 2>/dev/null || true
    print_ok "Removed $CACHE_COUNT __pycache__ dir(s) and $PYC_COUNT .pyc file(s)"
else
    print_ok "No __pycache__ or .pyc files found — workspace is clean"
fi
echo ""

# ── Step 4: Activate venv & install / verify deps ─────────────────────────────
print_step 4 "Activating virtual environment and verifying dependencies"

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"
print_ok "Virtual environment activated"

if [ -f "requirements.txt" ]; then
    print_info "Installing / verifying dependencies from requirements.txt …"
    pip install -q -r requirements.txt
    print_ok "Dependencies ready"
else
    print_warn "requirements.txt not found — skipping pip install"
fi
echo ""

# ── Step 5: Rotate / initialise log file ──────────────────────────────────────
print_step 5 "Preparing log file"

if [ -f "$LOG_FILE" ]; then
    ARCHIVE="output_$(date '+%Y%m%d_%H%M%S').log"
    mv "$LOG_FILE" "$ARCHIVE"
    print_ok "Previous log archived → $ARCHIVE"
fi

# Write a header into the fresh log
{
    echo "========================================================"
    echo "  MongoDB Document Seeder — output.log"
    echo "  Started : $(date '+%Y-%m-%d %H:%M:%S %Z')"
    echo "  Port    : $PORT"
    echo "  Module  : $APP_MODULE"
    echo "========================================================"
    echo ""
} > "$LOG_FILE"
print_ok "Log file initialised: $LOG_FILE"
echo ""

# ── Step 6: Launch with nohup ─────────────────────────────────────────────────
print_step 6 "Launching application with nohup"

nohup uvicorn "$APP_MODULE" \
    --host 0.0.0.0 \
    --port "$PORT" \
    --workers 1 \
    --log-level info \
    >> "$LOG_FILE" 2>&1 &

APP_PID=$!
echo "$APP_PID" > "$PID_FILE"
print_ok "Process started — PID: $APP_PID"
print_info "All output is being written to: $LOG_FILE"
echo ""

# ── Step 7: Wait for server to become ready ───────────────────────────────────
print_step 7 "Waiting for server to come up on port $PORT …"

elapsed=0
while [ $elapsed -lt $STARTUP_TIMEOUT ]; do
    if curl -sf "http://127.0.0.1:${PORT}/api/health" >/dev/null 2>&1; then
        break
    fi
    # Check the process is still alive
    if ! kill -0 "$APP_PID" 2>/dev/null; then
        echo ""
        print_err "Server process died unexpectedly (PID $APP_PID). Last 20 lines of log:"
        echo ""
        tail -20 "$LOG_FILE"
        exit 1
    fi
    sleep 1
    elapsed=$((elapsed + 1))
    printf "  ${CYAN}→${RESET}  Waiting … %ds\r" "$elapsed"
done
echo ""

if ! curl -sf "http://127.0.0.1:${PORT}/api/health" >/dev/null 2>&1; then
    print_err "Server did not respond within ${STARTUP_TIMEOUT}s. Last 20 lines of log:"
    echo ""
    tail -20 "$LOG_FILE"
    exit 1
fi

# ── Step 8: Success summary ───────────────────────────────────────────────────
echo ""
print_divider
echo -e "${GREEN}${BOLD}  ✔  Server is UP and healthy!${RESET}"
print_divider
echo ""
echo -e "  ${BOLD}PID            :${RESET} $APP_PID  (saved to $PID_FILE)"
echo -e "  ${BOLD}Health check   :${RESET} http://127.0.0.1:${PORT}/api/health"
echo -e "  ${BOLD}API docs       :${RESET} http://127.0.0.1:${PORT}/api/docs"
echo -e "  ${BOLD}Architecture   :${RESET} http://127.0.0.1:${PORT}/api/details"
echo -e "  ${BOLD}Records        :${RESET} http://127.0.0.1:${PORT}/api/records"
echo -e "  ${BOLD}Live log       :${RESET} tail -f $LOG_FILE"
echo ""
echo -e "  ${YELLOW}To stop the server:${RESET}  kill \$(cat $PID_FILE)  ${YELLOW}or${RESET}  kill $APP_PID"
echo ""
print_divider
echo ""

# ── Step 9: Tail the log ──────────────────────────────────────────────────────
echo -e "${BOLD}  Live log output (Ctrl-C to detach — server keeps running):${RESET}"
echo ""
tail -f "$LOG_FILE"

#!/usr/bin/env bash
# Production launcher: native HyperData API + external screener.
# Legacy fork mode: ./start.sh --legacy [old crypto-scalper flags]
set -euo pipefail

cd "$(dirname "$0")"

step() { printf '\033[36m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[32mOK  %s\033[0m\n' "$*"; }
warn() { printf '\033[33m!!  %s\033[0m\n' "$*"; }

usage() {
    cat <<'EOF'
Usage: ./start.sh [options]

Starts the native HyperData API (8420) and the external screener (8421).

  --demo              synthetic data; disables AI and Telegram
  --api-port N        native API port (default 8420)
  --port N            screener web port (default 8421)
  --tui               also show the Rich Market Overview
  --notify            enable Telegram delivery
  --journal PATH      screener SQLite journal
  --record PATH       append every scan to JSONL
  --legacy            run the old crypto-scalper fork instead
  --help              this help

Examples:
  ./start.sh
  ./start.sh --demo
  ./start.sh --tui --notify
  ./start.sh --legacy --port 8420
EOF
}

for arg in "$@"; do
    case "$arg" in
        --help|-h) usage; exit 0 ;;
    esac
done

find_python() {
    for candidate in python3.13 python3.12 python3 python; do
        if command -v "$candidate" >/dev/null 2>&1 &&
           "$candidate" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 12) else 1)' 2>/dev/null; then
            echo "$candidate"
            return 0
        fi
    done
    return 1
}

legacy=0
for arg in "$@"; do
    [ "$arg" = "--legacy" ] && legacy=1
done

if command -v uv >/dev/null 2>&1; then
    ok "Using $(uv --version)"
    RUNNER=(uv run python)
else
    PYTHON="$(find_python || true)"
    if [ -z "$PYTHON" ]; then
        warn "Python 3.12+ not found and uv is not installed."
        echo "    Install uv (https://docs.astral.sh/uv/) or Python 3.12+ and run this script again."
        exit 1
    fi
    ok "Using $("$PYTHON" --version)"
    if [ ! -x .venv/bin/python ]; then
        step "Creating the virtual environment in .venv"
        "$PYTHON" -m venv .venv
    fi
    if ! .venv/bin/python -c 'import hyperdata_terminal, extensions.scalper, apps.scalper_web, apps.screener_web' 2>/dev/null; then
        step "Installing dependencies (the first run can take a minute)"
        .venv/bin/python -m pip install --upgrade pip
        .venv/bin/python -m pip install -e .
    fi
    RUNNER=(.venv/bin/python)
fi

if [ ! -f .env ]; then
    cp .env.example .env
    warn "Created .env from .env.example."
    echo "    Add GEMINI_API_KEY (or configure OpenRouter) to enable AI analysis."
fi

if [ "$legacy" = 1 ]; then
    forward=()
    for arg in "$@"; do
        [ "$arg" = "--legacy" ] || forward+=("$arg")
    done
    step "Starting legacy crypto-scalper"
    if command -v uv >/dev/null 2>&1; then
        exec uv run crypto-scalper "${forward[@]}"
    else
        exec .venv/bin/python -m apps.scalper_web "${forward[@]}"
    fi
fi

serve_args=()
while [ $# -gt 0 ]; do
    case "$1" in
        --port)
            serve_args+=(--web-port "${2:-8421}")
            shift 2 || shift
            ;;
        *)
            serve_args+=("$1")
            shift
            ;;
    esac
done

exec "${RUNNER[@]}" scripts/serve.py "${serve_args[@]}"

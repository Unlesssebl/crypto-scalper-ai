#!/usr/bin/env bash
# Launcher: native HyperData API + external screener.
set -euo pipefail

cd "$(dirname "$0")"

step() { printf '\033[36m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[32mOK  %s\033[0m\n' "$*"; }
warn() { printf '\033[33m!!  %s\033[0m\n' "$*"; }

usage() {
    cat <<'EOF'
Usage: ./start.sh [options]

Starts the complete project: native API, screener, Market Pulse and PostgreSQL.
  --stop              stop all project services, retaining data
  --restart           restart all project services
  --status            show combined service status

  --demo              synthetic data; disables AI, Telegram and Market Pulse
  --api-port N        native API port (default 8420)
  --port N            screener web port (default 8421)
  --tui               also show the Rich Market Overview
  --tui-page NAME     auto|all|market|risk|vault (default: auto)
  --tui-rotate N      seconds between rotated pages (default: 0 = keys only)
  --notify            enable Telegram delivery
  --journal PATH      screener SQLite journal
  --record PATH       append every scan to JSONL
  --help              this help

Examples:
  ./start.sh
  ./start.sh --demo
  ./start.sh --tui --notify
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
    if ! .venv/bin/python -c 'import hyperdata_terminal, extensions.scalper, apps.screener_web' 2>/dev/null; then
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

exec "${RUNNER[@]}" scripts/project.py "${serve_args[@]}"

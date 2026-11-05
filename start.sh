#!/usr/bin/env bash
# One-command launcher for the Crypto Scalper fork.
# Usage: ./start.sh [--demo] [--tui] [--port 8420] [--notify] [--journal PATH]
set -euo pipefail

cd "$(dirname "$0")"

step() { printf '\033[36m==> %s\033[0m\n' "$*"; }
ok()   { printf '\033[32mOK  %s\033[0m\n' "$*"; }
warn() { printf '\033[33m!!  %s\033[0m\n' "$*"; }

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

    if ! .venv/bin/python -c 'import hyperdata_terminal, extensions.scalper, apps.scalper_web' 2>/dev/null; then
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
    echo "    Without a key the dashboard still runs; signals stay HYPERDATA_ONLY."
fi

PORT=8420
expect_port=0
help_requested=0
for arg in "$@"; do
    if [ "$expect_port" = 1 ]; then
        PORT="$arg"
        expect_port=0
        continue
    fi
    case "$arg" in
        --port) expect_port=1 ;;
        --port=*) PORT="${arg#--port=}" ;;
        --help|-h) help_requested=1 ;;
    esac
done

if [ "$help_requested" = 0 ]; then
    step "Starting Crypto Scalper"
    echo "    Dashboard: http://127.0.0.1:$PORT/scalper"
    echo "    Stop with Ctrl+C"
    echo "    Other modes: ./start.sh --demo | --tui | --port 8500 | --notify"
fi

exec "${RUNNER[@]}" -m apps.scalper_web "$@"

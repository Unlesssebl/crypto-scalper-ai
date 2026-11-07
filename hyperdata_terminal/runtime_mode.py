"""Unified runtime contract shared by the native API and the screener.

There is one supported runtime: the native hub owns exchange sockets and the
screener owns the attention plan. ``runtime_mode`` is kept in the attention
report so a stale or downgraded native API fails explicitly.
"""

from __future__ import annotations

import os

UNIFIED = "unified"
# Set by the unified launcher (scripts/serve.py). Standalone HyperData commands
# keep their own exchange collectors and never depend on Market Pulse.
UNIFIED_ENV = "HYPERDATA_UNIFIED_RUNTIME"


def unified_enabled() -> bool:
    return os.getenv(UNIFIED_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


class RuntimeModeError(RuntimeError):
    """Native attention is not compatible with the unified screener."""


def compatibility_error(attention: dict | None) -> str | None:
    """Return an explicit diagnostic when native attention is unsafe."""
    attention = attention if isinstance(attention, dict) else {}
    actual = attention.get("runtime_mode")
    if actual != UNIFIED:
        return f"native attention is not unified (native mode: {actual or 'missing'})"
    if not attention.get("enabled"):
        return "native attention is disabled"
    return None


def require_compatible(attention: dict | None) -> None:
    error = compatibility_error(attention)
    if error:
        raise RuntimeModeError(error)

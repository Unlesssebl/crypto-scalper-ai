"""Explicit process mode shared by the native API and the screener.

``PULSE_UNIFIED`` is the single switch: the launcher sets it for both child
processes, so each side can decide its own behaviour without depending on the
temporary state of the other process's API.
"""

from __future__ import annotations

import os

UNIFIED = "unified"
LEGACY = "legacy"


class RuntimeModeError(RuntimeError):
    """Native and screener disagree about the process mode."""


def runtime_mode() -> str:
    return UNIFIED if os.getenv("PULSE_UNIFIED", "0").strip() == "1" else LEGACY


def compatibility_error(mode: str, attention: dict | None) -> str | None:
    """Return an explicit diagnostic when the native attention state is unsafe."""
    attention = attention if isinstance(attention, dict) else {}
    actual = attention.get("runtime_mode")
    if mode == UNIFIED:
        if actual != UNIFIED:
            return (
                "unified screener requires unified native attention "
                f"(native mode: {actual or 'missing'})"
            )
        if not attention.get("enabled"):
            return "unified screener requires native attention, but it is disabled"
    elif actual == UNIFIED:
        return f"legacy screener cannot use unified native attention (native mode: {actual})"
    return None


def require_compatible(mode: str, attention: dict | None) -> None:
    error = compatibility_error(mode, attention)
    if error:
        raise RuntimeModeError(error)

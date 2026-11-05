"""Runtime helpers shared by the screener entry points.

These used to live in the legacy single-process app (`apps/scalper_web`).
They are independent of the hub implementation, so both the external screener
and any future entry point can reuse them.
"""
from __future__ import annotations

import os
import time

from .analysis import Gemini, OpenRouter
from .market import WINDOWS

SIGNAL_LABELS = {
    "CONSENSUS_LONG": "Long", "CONSENSUS_SHORT": "Short", "DIVERGENCE": "Diverge",
    "HYPERDATA_ONLY": "HD only", "AI_ONLY": "AI only", "NEUTRAL": "Neutral",
    "INSUFFICIENT_DATA": "No signal",
}


def analyst(session, demo):
    """Build the configured AI client (Gemini or OpenRouter)."""
    provider = os.getenv("AI_PROVIDER", "gemini").strip().lower()
    if provider == "openrouter":
        model = os.getenv("OPENROUTER_MODEL", "").strip()
        key = "" if demo or not model else os.getenv("OPENROUTER_API_KEY", "").strip()
        return OpenRouter(session, model or "openrouter/auto", key,
                          proxy=os.getenv("OPENROUTER_PROXY", "").strip() or None)
    if provider == "gemini":
        return Gemini(session, os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite").strip(),
                      "" if demo else os.getenv("GEMINI_API_KEY", "").strip())
    raise ValueError("AI_PROVIDER must be gemini or openrouter")


def terminal_signals(journal, symbol):
    """One TUI cell per window: accepted consensus and its age in minutes."""
    cells = []
    for window in WINDOWS:
        rows = journal.latest(symbol, window, accepted=True)
        if not rows:
            cells.append("—")
            continue
        event = rows[0]
        age = max(0, int((time.time() - event["captured_at"]) / 60))
        cells.append(f'{SIGNAL_LABELS[event["consensus"]["category"]]} {age}m')
    return cells

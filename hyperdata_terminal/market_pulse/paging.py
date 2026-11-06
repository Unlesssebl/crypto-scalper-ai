"""Window pagination that never splits a minute across pages.

``list_windows`` orders by ``window_start_ms`` and applies a hard ``LIMIT``.
Several channels can share the same minute, so advancing by the last row's time
would silently drop the rest of that minute.  A page therefore always includes
the complete boundary minute and returns a cursor at the next minute.
"""
from __future__ import annotations

from .contracts import WINDOW_MS, WindowAggregate
from .storage import MarketPulseStore

KEY = tuple[int, str, str, str, str]


def window_key(window: WindowAggregate) -> KEY:
    return (
        window.window_start_ms,
        window.venue,
        window.native_market_id,
        window.channel,
        window.aggregation_kind,
    )


async def page_windows(
    store: MarketPulseStore,
    *,
    venue: str | None = None,
    native_market_id: str | None = None,
    from_ms: int | None = None,
    to_ms: int | None = None,
    limit: int = 500,
) -> tuple[list[WindowAggregate], int | None]:
    """Return a page plus the next ``from_ms`` cursor, or ``None`` at the end."""
    rows = await store.list_windows(
        venue=venue,
        native_market_id=native_market_id,
        from_ms=from_ms,
        to_ms=to_ms,
        limit=limit,
    )
    if len(rows) < limit or not rows:
        return rows, None
    boundary = rows[-1].window_start_ms
    extra_to = boundary + WINDOW_MS if to_ms is None else min(to_ms, boundary + WINDOW_MS)
    extra = await store.list_windows(
        venue=venue,
        native_market_id=native_market_id,
        from_ms=boundary,
        to_ms=extra_to,
        limit=10_000,
    )
    merged: dict[KEY, WindowAggregate] = {window_key(window): window for window in rows}
    for window in extra:
        merged.setdefault(window_key(window), window)
    ordered = sorted(
        merged.values(),
        key=lambda item: (
            item.window_start_ms,
            item.venue,
            item.native_market_id,
            item.channel,
            item.aggregation_kind,
        ),
    )
    return ordered, boundary + WINDOW_MS

"""Single-writer ingestion and gap-control service.

An HTTP request is acknowledged only after the normalized event, any conflict
record and the recalculation request are committed.  Window calculation starts
after that commit; if it fails, the affected window remains in the durable
recalculation queue instead of silently disappearing.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any, Iterable, Mapping, Sequence

from .contracts import (
    SCHEMA_VERSION,
    ExecutionEvent,
    InstrumentSpec,
    QualityState,
    known_channel,
)
from .gaps import GapTracker
from .storage import AppendResult, MarketPulseStore
from .windows import WindowService

LOG = logging.getLogger(__name__)
MAX_BATCH_EVENTS = 1_000


class BatchError(ValueError):
    pass


def decode_batch(payload: Mapping[str, Any]) -> list[ExecutionEvent]:
    if not isinstance(payload, Mapping):
        raise BatchError("batch must be a JSON object")
    if payload.get("schema_version", SCHEMA_VERSION) != SCHEMA_VERSION:
        raise BatchError(f"unsupported schema_version {payload.get('schema_version')!r}")
    rows = payload.get("events")
    if not isinstance(rows, list) or not rows:
        raise BatchError("events must be a non-empty list")
    if len(rows) > MAX_BATCH_EVENTS:
        raise BatchError(f"batch exceeds {MAX_BATCH_EVENTS} events")
    try:
        return [ExecutionEvent.from_payload(row) for row in rows]
    except (KeyError, TypeError, ValueError) as exc:
        raise BatchError(f"invalid event: {exc}") from exc


class IngestionService:
    def __init__(
        self,
        store: MarketPulseStore,
        windows: WindowService,
        gaps: GapTracker,
    ) -> None:
        self.store = store
        self.windows = windows
        self.gaps = gaps
        self._lock = asyncio.Lock()

    async def register_instruments(self, specs: Iterable[InstrumentSpec]) -> None:
        await self.store.register_instruments(specs)

    async def register_raw_segments(self, segments) -> None:
        for segment in segments:
            await self.store.record_raw_segment(segment)

    async def append_batch(
        self,
        events: Sequence[ExecutionEvent],
        *,
        now_ms: int,
        touch_coverage: bool = True,
    ) -> dict[str, Any]:
        if not events:
            raise BatchError("empty batch")
        if len(events) > MAX_BATCH_EVENTS:
            raise BatchError(f"batch exceeds {MAX_BATCH_EVENTS} events")
        async with self._lock:
            result = await self.store.append_events(events, now_ms=now_ms)
            if touch_coverage and result.accepted:
                await self._touch_coverage(result.accepted, now_ms=now_ms)
            rebuilt = await self._rebuild(result.affected, now_ms=now_ms)
            await self.store.complete_recalc(
                key
                for key in result.affected
                if any(window.aggregate_key == key for window in rebuilt)
            )
            return {
                "status": _status(result),
                "accepted": len(result.accepted),
                "duplicates": result.duplicates,
                "conflicts": result.conflicts,
                "cursor": result.cursor,
                "event_sequences": list(result.accepted_event_sequences),
                "windows": [window.to_payload() for window in rebuilt],
            }

    async def drain_recalc(self, *, now_ms: int, limit: int = 1_000) -> list[dict[str, Any]]:
        async with self._lock:
            affected = await self.store.pending_recalc(limit=limit)
            if not affected:
                return []
            rebuilt = await self.windows.rebuild_affected(affected, now_ms=now_ms)
            done = {
                window.aggregate_key
                for window in rebuilt
            }
            await self.store.complete_recalc(key for key in affected if key in done)
            return [window.to_payload() for window in rebuilt]

    async def close_due(self, *, now_ms: int) -> list[dict[str, Any]]:
        async with self._lock:
            windows = await self.windows.close_due(now_ms=now_ms)
            return [window.to_payload() for window in windows]

    async def rebuild_range(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        start_ms: int,
        end_ms: int,
        *,
        now_ms: int,
    ) -> list[dict[str, Any]]:
        """Rebuild every minute touched by a gap after its state changed."""
        from .contracts import WINDOW_MS

        first = start_ms // WINDOW_MS * WINDOW_MS
        last = max(first, (max(start_ms + 1, end_ms) - 1) // WINDOW_MS * WINDOW_MS)
        rebuilt = []
        for window_start in range(first, last + WINDOW_MS, WINDOW_MS):
            rebuilt.append(
                await self.windows.rebuild(
                    venue,
                    native_market_id,
                    channel,
                    aggregation_kind,
                    window_start,
                    now_ms=now_ms,
                )
            )
        return [window.to_payload() for window in rebuilt]

    async def _touch_coverage(self, events: Iterable[ExecutionEvent], *, now_ms: int) -> None:
        latest: dict[tuple[str, str, str], ExecutionEvent] = {}
        for event in events:
            key = (event.venue, event.native_market_id, event.channel)
            current = latest.get(key)
            if current is None or event.receive_time_ms >= current.receive_time_ms:
                latest[key] = event
        for event in latest.values():
            await self.gaps.touch_events(
                event.venue,
                event.native_market_id,
                event.channel,
                event.source_epoch,
                now_ms=now_ms,
                event_time_ms=event.event_time_ms,
                receive_time_ms=event.receive_time_ms,
            )

    async def _rebuild(self, affected: Iterable[tuple[str, str, str, str, int]], *, now_ms: int):
        try:
            return await self.windows.rebuild_affected(affected, now_ms=now_ms)
        except Exception:
            # The event is already durable.  Leave the recalculation marker in
            # place so a retry or restart repairs the aggregate.
            LOG.exception("Window recalculation failed; request remains queued")
            return []


class GapControlService:
    """Translate collector lifecycle messages into durable gap records."""

    ACTIONS = {"connected", "disconnected", "heartbeat", "unavailable", "reconciled", "released"}

    def __init__(self, store: MarketPulseStore, gaps: GapTracker) -> None:
        self.store = store
        self.gaps = gaps

    async def release_outside_attention(self, venue, symbols, now_ms):
        """Reconcile durable channels forgotten by a restarted collector, preserving gaps."""
        from .collectors.specs import market_for_venue

        wanted = {market_for_venue(venue, symbol) for symbol in symbols}
        channel = "trades" if venue == "hyperliquid" else "trades-all"
        for row in await self.store.list_coverage():
            if (row.venue == venue and row.channel == channel and row.native_market_id not in wanted
                    and row.state != QualityState.OUTSIDE_ATTENTION.value):
                await self.gaps.on_released(venue, row.native_market_id, channel, row.source_epoch, now_ms)

    async def apply(self, payload: Mapping[str, Any], *, now_ms: int) -> dict[str, Any]:
        action = payload.get("action")
        if action not in self.ACTIONS:
            raise BatchError(f"unknown gap action {action!r}")
        required = ("venue", "native_market_id", "channel", "source_epoch")
        for field in required:
            if not isinstance(payload.get(field), str) or not payload[field]:
                raise BatchError(f"{field} is required")
        venue = payload["venue"]
        market = payload["native_market_id"]
        channel = payload["channel"]
        source_epoch = payload["source_epoch"]
        if action == "released":
            coverage = await self.gaps.on_released(venue, market, channel, source_epoch, now_ms)
            return {"status": "ok", "coverage": coverage.to_payload()}
        if action == "connected":
            coverage = await self.gaps.on_connected(venue, market, channel, source_epoch, now_ms)
            return {"status": "ok", "coverage": coverage.to_payload()}
        if action == "heartbeat":
            coverage = await self.gaps.on_heartbeat(venue, market, channel, now_ms)
            return {"status": "ok", "coverage": None if coverage is None else coverage.to_payload()}
        if action == "disconnected":
            gap = await self.gaps.on_disconnected(
                venue,
                market,
                channel,
                source_epoch,
                now_ms,
                reason=str(payload.get("reason") or "websocket disconnected"),
                last_receive_ms=_optional_int(payload.get("last_receive_ms")),
            )
            return {"status": "ok", "gap": gap.to_payload()}
        if action == "unavailable":
            coverage = await self.gaps.mark_unavailable(
                venue,
                market,
                channel,
                source_epoch,
                now_ms,
                reason=str(payload.get("reason") or "collector unavailable"),
            )
            return {"status": "ok", "coverage": coverage.to_payload()}
        gap_id = _optional_int(payload.get("gap_id"))
        end_ms = _optional_int(payload.get("end_ms"))
        if gap_id is None or end_ms is None:
            raise BatchError("reconciled requires gap_id and end_ms")
        gap = await self.gaps.mark_reconciled(
            gap_id,
            end_ms=end_ms,
            method=str(payload.get("method") or "manual"),
            now_ms=now_ms,
        )
        if gap is None:
            raise BatchError(f"gap {gap_id} not found")
        # Reconciled quality must reach the windows even when the interval had
        # no recovered trades to trigger a rebuild.
        from .contracts import WINDOW_MS
        from .windows import WindowService

        windows = WindowService(self.store)
        kind = known_channel(gap.venue, gap.channel).value
        first = gap.start_ms // WINDOW_MS * WINDOW_MS
        last = max(first, (max(gap.start_ms + 1, end_ms) - 1) // WINDOW_MS * WINDOW_MS)
        for window_start in range(first, last + WINDOW_MS, WINDOW_MS):
            await windows.rebuild(
                gap.venue,
                gap.native_market_id,
                gap.channel,
                kind,
                window_start,
                now_ms=now_ms,
            )
        return {"status": "ok", "gap": gap.to_payload()}


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise BatchError("expected integer, got bool")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise BatchError("expected integer") from exc


def _status(result: AppendResult) -> str:
    if result.conflicts and not result.accepted:
        return "conflict"
    if result.conflicts:
        return "partial"
    if result.accepted:
        return "ok"
    return "duplicate"


def quality_counts(windows: Iterable[Any]) -> dict[str, int]:
    counts: dict[str, int] = {state.value: 0 for state in QualityState}
    for window in windows:
        quality = window["quality"] if isinstance(window, dict) else window.quality
        counts[quality] = counts.get(quality, 0) + 1
    return counts

"""Storage contract and an in-memory reference implementation.

The PostgreSQL implementation lives in :mod:`hyperdata_terminal.market_pulse.postgres`.
Both stores implement the same atomic append semantics:

* an unseen deduplication key becomes an event and a recalculation request;
* the same key with the same content fingerprint is an idempotent duplicate;
* the same key with different price, size, side or event time is a conflict,
  never a silent ``INSERT OR IGNORE``.  The window touched by the conflict is
  excluded from normal baselines until it is reviewed.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any, Iterable, Protocol, Sequence, runtime_checkable

from .contracts import (
    WINDOW_MS,
    ExecutionEvent,
    Gap,
    InstrumentSpec,
    QualityState,
    WindowAggregate,
    affected_window,
)


@dataclass(frozen=True, slots=True)
class AppendResult:
    accepted: tuple[ExecutionEvent, ...]
    accepted_event_sequences: tuple[int, ...]
    duplicates: int
    conflicts: int
    affected: tuple[tuple[str, str, str, str, int], ...]
    cursor: int


@dataclass(frozen=True, slots=True)
class StoredEvent:
    event_seq: int
    event: ExecutionEvent

    def to_payload(self) -> dict[str, Any]:
        return {"event_seq": self.event_seq, **self.event.to_payload()}


@dataclass(frozen=True, slots=True)
class ConflictRecord:
    venue: str
    native_market_id: str
    channel: str
    aggregation_kind: str
    trade_id: str
    existing_fingerprint: str
    incoming_fingerprint: str
    incoming_payload: dict[str, Any]
    detected_at_ms: int
    reason: str
    resolved: bool = False


@dataclass(frozen=True, slots=True)
class ChannelCoverage:
    venue: str
    native_market_id: str
    channel: str
    source_epoch: str
    state: str
    updated_at_ms: int
    last_event_ms: int | None = None
    last_receive_ms: int | None = None
    heartbeat_ms: int | None = None
    connected_since_ms: int | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        QualityState(self.state)

    def to_payload(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "native_market_id": self.native_market_id,
            "channel": self.channel,
            "source_epoch": self.source_epoch,
            "state": self.state,
            "updated_at_ms": self.updated_at_ms,
            "last_event_ms": self.last_event_ms,
            "last_receive_ms": self.last_receive_ms,
            "heartbeat_ms": self.heartbeat_ms,
            "connected_since_ms": self.connected_since_ms,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class RawSegmentRecord:
    venue: str
    path: str
    first_receive_ms: int
    last_receive_ms: int
    bytes: int
    sha256: str | None
    sealed: bool
    created_at_ms: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "venue": self.venue,
            "path": self.path,
            "first_receive_ms": self.first_receive_ms,
            "last_receive_ms": self.last_receive_ms,
            "bytes": self.bytes,
            "sha256": self.sha256,
            "sealed": self.sealed,
            "created_at_ms": self.created_at_ms,
        }


@runtime_checkable
class MarketPulseStore(Protocol):
    async def register_instruments(self, specs: Iterable[InstrumentSpec]) -> None: ...

    async def get_instrument_spec(
        self, venue: str, native_market_id: str, at_ms: int
    ) -> InstrumentSpec | None: ...

    async def append_events(self, events: Sequence[ExecutionEvent], *, now_ms: int) -> AppendResult: ...

    async def list_events_for_window(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
    ) -> list[ExecutionEvent]: ...

    async def list_events_after(
        self,
        cursor: int,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 500,
    ) -> list[StoredEvent]: ...

    async def upsert_window(self, window: WindowAggregate) -> None: ...

    async def get_window(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
    ) -> WindowAggregate | None: ...

    async def list_windows(
        self,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 500,
    ) -> list[WindowAggregate]: ...

    async def mark_window_quality(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
        quality: str,
        reason: str,
        *,
        now_ms: int,
    ) -> None: ...

    async def record_conflict(self, conflict: ConflictRecord) -> None: ...

    async def list_conflicts(
        self, *, venue: str | None = None, native_market_id: str | None = None, limit: int = 200
    ) -> list[ConflictRecord]: ...

    async def add_gap(self, gap: Gap) -> Gap: ...

    async def update_gap(self, gap_id: int, **changes: Any) -> Gap | None: ...

    async def list_gaps(
        self,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        channel: str | None = None,
        state: str | None = None,
        limit: int = 500,
    ) -> list[Gap]: ...

    async def gaps_overlapping(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        start_ms: int,
        end_ms: int,
    ) -> list[Gap]: ...

    async def set_coverage(self, coverage: ChannelCoverage) -> None: ...

    async def get_coverage(self, venue: str, native_market_id: str, channel: str) -> ChannelCoverage | None: ...

    async def list_coverage(self) -> list[ChannelCoverage]: ...

    async def record_raw_segment(self, segment: RawSegmentRecord) -> None: ...

    async def list_raw_segments(self, *, venue: str | None = None, limit: int = 200) -> list[RawSegmentRecord]: ...

    async def pending_recalc(self, limit: int = 1_000) -> list[tuple[str, str, str, str, int]]: ...

    async def complete_recalc(self, affected: Iterable[tuple[str, str, str, str, int]]) -> None: ...

    async def health(self) -> dict[str, Any]: ...

    async def close(self) -> None: ...


class InMemoryMarketPulseStore:
    """Reference store used by unit tests and offline contract checks.

    It is intentionally not a fallback for production: losing the process loses
    the journal, which would violate the durable-cursor requirement.
    """

    def __init__(self, *, now_ms: int | None = None) -> None:
        self._now_ms = now_ms or 0
        self._next_seq = 1
        self._next_gap_id = 1
        self._events: list[StoredEvent] = []
        self._by_key: dict[tuple[str, str, str, str, str], StoredEvent] = {}
        self._instruments: dict[tuple[str, str, str], InstrumentSpec] = {}
        self._windows: dict[tuple[str, str, str, str, int], WindowAggregate] = {}
        self._conflicts: list[ConflictRecord] = []
        self._gaps: list[Gap] = []
        self._coverage: dict[tuple[str, str, str], ChannelCoverage] = {}
        self._raw_segments: list[RawSegmentRecord] = []
        self._conflict_windows: set[tuple[str, str, str, str, int]] = set()
        self._manual_quality: dict[tuple[str, str, str, str, int], tuple[str, str]] = {}
        self._recalc: set[tuple[str, str, str, str, int]] = set()

    async def register_instruments(self, specs: Iterable[InstrumentSpec]) -> None:
        # First registration wins for a given spec version.  Re-registering the
        # same version must not move ``valid_from_ms`` forward, or historical
        # recovery would lose the spec for older events.  A changed contract
        # value must arrive under a new ``spec_version``.
        for spec in specs:
            key = (spec.venue, spec.native_market_id, spec.spec_version)
            self._instruments.setdefault(key, spec)

    async def get_instrument_spec(
        self, venue: str, native_market_id: str, at_ms: int
    ) -> InstrumentSpec | None:
        candidates = [
            spec
            for (spec_venue, spec_market, _version), spec in self._instruments.items()
            if spec_venue == venue
            and spec_market == native_market_id
            and spec.valid_from_ms <= at_ms
            and (spec.valid_to_ms is None or at_ms < spec.valid_to_ms)
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda spec: (spec.valid_from_ms, spec.spec_version))

    async def append_events(self, events: Sequence[ExecutionEvent], *, now_ms: int) -> AppendResult:
        self._now_ms = now_ms
        accepted: list[ExecutionEvent] = []
        sequences: list[int] = []
        duplicates = 0
        conflicts = 0
        affected: set[tuple[str, str, str, str, int]] = set()

        # Validate before mutating: a bad item must not leave a half-batch.
        fingerprints = [event.content_fingerprint() for event in events]
        for event, fingerprint in zip(events, fingerprints, strict=True):
            existing = self._by_key.get(event.dedup_key)
            if existing is None:
                sequence = self._next_seq
                self._next_seq += 1
                stored = StoredEvent(sequence, event)
                self._events.append(stored)
                self._by_key[event.dedup_key] = stored
                accepted.append(event)
                sequences.append(sequence)
                affected.add(affected_window(event))
                self._recalc.add(affected_window(event))
                continue
            if existing.event.content_fingerprint() == fingerprint:
                duplicates += 1
                continue
            conflicts += 1
            # A conflicting trade invalidates both minutes: we no longer know
            # which version of the execution is correct.
            keys = {affected_window(existing.event), affected_window(event)}
            for key in keys:
                affected.add(key)
                self._conflict_windows.add(key)
                self._recalc.add(key)
                await self.add_gap(
                    Gap(
                        gap_id=0,
                        venue=event.venue,
                        native_market_id=event.native_market_id,
                        channel=event.channel,
                        source_epoch=event.source_epoch,
                        start_ms=key[4],
                        end_ms=key[4] + WINDOW_MS,
                        detected_at_ms=now_ms,
                        state=QualityState.GAP_DETECTED.value,
                        reason=f"trade id conflict: {event.trade_id}",
                    )
                )
            self._conflicts.append(
                ConflictRecord(
                    venue=event.venue,
                    native_market_id=event.native_market_id,
                    channel=event.channel,
                    aggregation_kind=event.aggregation_kind,
                    trade_id=event.trade_id,
                    existing_fingerprint=existing.event.content_fingerprint(),
                    incoming_fingerprint=fingerprint,
                    incoming_payload=event.to_payload(),
                    detected_at_ms=now_ms,
                    reason="same dedup key with different content",
                )
            )

        cursor = self._next_seq - 1
        return AppendResult(
            accepted=tuple(accepted),
            accepted_event_sequences=tuple(sequences),
            duplicates=duplicates,
            conflicts=conflicts,
            affected=tuple(sorted(affected)),
            cursor=cursor,
        )

    async def list_events_for_window(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
    ) -> list[ExecutionEvent]:
        end = window_start_ms + WINDOW_MS
        return [
            item.event
            for item in self._events
            if item.event.venue == venue
            and item.event.native_market_id == native_market_id
            and item.event.channel == channel
            and item.event.aggregation_kind == aggregation_kind
            and window_start_ms <= item.event.event_time_ms < end
        ]

    async def list_events_after(
        self,
        cursor: int,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 500,
    ) -> list[StoredEvent]:
        result = []
        for item in self._events:
            if item.event_seq <= cursor:
                continue
            event = item.event
            if venue is not None and event.venue != venue:
                continue
            if native_market_id is not None and event.native_market_id != native_market_id:
                continue
            if from_ms is not None and event.event_time_ms < from_ms:
                continue
            if to_ms is not None and event.event_time_ms >= to_ms:
                continue
            result.append(item)
            if len(result) >= limit:
                break
        return result

    async def upsert_window(self, window: WindowAggregate) -> None:
        self._windows[window.aggregate_key] = window

    async def get_window(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
    ) -> WindowAggregate | None:
        return self._windows.get((venue, native_market_id, channel, aggregation_kind, window_start_ms))

    async def list_windows(
        self,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        from_ms: int | None = None,
        to_ms: int | None = None,
        limit: int = 500,
    ) -> list[WindowAggregate]:
        rows = [
            window
            for window in self._windows.values()
            if (venue is None or window.venue == venue)
            and (native_market_id is None or window.native_market_id == native_market_id)
            and (from_ms is None or window.window_start_ms >= from_ms)
            and (to_ms is None or window.window_start_ms < to_ms)
        ]
        rows.sort(key=lambda window: (window.window_start_ms, window.venue, window.native_market_id))
        return rows[:limit]

    async def mark_window_quality(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
        quality: str,
        reason: str,
        *,
        now_ms: int,
    ) -> None:
        key = (venue, native_market_id, channel, aggregation_kind, window_start_ms)
        self._manual_quality[key] = (quality, reason)
        current = self._windows.get(key)
        if current is not None and current.quality != quality:
            self._windows[key] = replace(
                current,
                quality=quality,
                version=current.version + 1,
                revised_at_ms=now_ms,
            )

    async def record_conflict(self, conflict: ConflictRecord) -> None:
        self._conflicts.append(conflict)

    async def list_conflicts(
        self, *, venue: str | None = None, native_market_id: str | None = None, limit: int = 200
    ) -> list[ConflictRecord]:
        rows = [
            item
            for item in self._conflicts
            if (venue is None or item.venue == venue)
            and (native_market_id is None or item.native_market_id == native_market_id)
        ]
        return rows[:limit]

    async def add_gap(self, gap: Gap) -> Gap:
        stored = Gap(
            gap_id=self._next_gap_id,
            venue=gap.venue,
            native_market_id=gap.native_market_id,
            channel=gap.channel,
            source_epoch=gap.source_epoch,
            start_ms=gap.start_ms,
            end_ms=gap.end_ms,
            detected_at_ms=gap.detected_at_ms,
            state=gap.state,
            method=gap.method,
            reason=gap.reason,
        )
        self._next_gap_id += 1
        self._gaps.append(stored)
        return stored

    async def update_gap(self, gap_id: int, **changes: Any) -> Gap | None:
        for index, gap in enumerate(self._gaps):
            if gap.gap_id != gap_id:
                continue
            allowed = {"end_ms", "state", "method", "reason"}
            unknown = set(changes) - allowed
            if unknown:
                raise ValueError(f"unsupported gap fields: {sorted(unknown)}")
            payload = gap.to_payload()
            payload.update(changes)
            updated = Gap(**payload)
            self._gaps[index] = updated
            return updated
        return None

    async def list_gaps(
        self,
        *,
        venue: str | None = None,
        native_market_id: str | None = None,
        channel: str | None = None,
        state: str | None = None,
        limit: int = 500,
    ) -> list[Gap]:
        rows = [
            gap
            for gap in self._gaps
            if (venue is None or gap.venue == venue)
            and (native_market_id is None or gap.native_market_id == native_market_id)
            and (channel is None or gap.channel == channel)
            and (state is None or gap.state == state)
        ]
        rows.sort(key=lambda gap: (gap.start_ms, gap.gap_id))
        return rows[:limit]

    async def gaps_overlapping(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        start_ms: int,
        end_ms: int,
    ) -> list[Gap]:
        return [
            gap
            for gap in self._gaps
            if gap.venue == venue
            and gap.native_market_id == native_market_id
            and gap.channel == channel
            and gap.overlaps(start_ms, end_ms)
        ]

    async def set_coverage(self, coverage: ChannelCoverage) -> None:
        self._coverage[(coverage.venue, coverage.native_market_id, coverage.channel)] = coverage

    async def get_coverage(self, venue: str, native_market_id: str, channel: str) -> ChannelCoverage | None:
        return self._coverage.get((venue, native_market_id, channel))

    async def list_coverage(self) -> list[ChannelCoverage]:
        return sorted(
            self._coverage.values(),
            key=lambda item: (item.venue, item.native_market_id, item.channel),
        )

    async def record_raw_segment(self, segment: RawSegmentRecord) -> None:
        self._raw_segments = [item for item in self._raw_segments if item.path != segment.path]
        self._raw_segments.append(segment)

    async def list_raw_segments(self, *, venue: str | None = None, limit: int = 200) -> list[RawSegmentRecord]:
        rows = [item for item in self._raw_segments if venue is None or item.venue == venue]
        return rows[:limit]

    async def pending_recalc(self, limit: int = 1_000) -> list[tuple[str, str, str, str, int]]:
        return sorted(self._recalc)[:limit]

    async def complete_recalc(self, affected: Iterable[tuple[str, str, str, str, int]]) -> None:
        for key in affected:
            self._recalc.discard(key)

    async def health(self) -> dict[str, Any]:
        open_gaps = sum(
            1 for gap in self._gaps if gap.state == QualityState.GAP_DETECTED.value
        )
        declared = sum(
            1 for gap in self._gaps if gap.state == QualityState.DECLARED_OUTAGE.value
        )
        coverage = await self.list_coverage()
        unavailable = [item for item in coverage if item.state == QualityState.UNAVAILABLE.value]
        return {
            "status": "degraded" if open_gaps or unavailable or self._conflicts else "ok",
            "events": len(self._events),
            "windows": len(self._windows),
            "cursor": self._next_seq - 1,
            "conflicts": len(self._conflicts),
            "open_gaps": open_gaps,
            "declared_outages": declared,
            "unavailable_channels": len(unavailable),
            "pending_recalc": len(self._recalc),
        }

    async def close(self) -> None:
        return None

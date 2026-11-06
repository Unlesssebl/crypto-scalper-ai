"""UTC-minute aggregation with explicit late-data and quality semantics.

V1 compares calendar minutes ``[t, t+60s)``.  A minute is provisional until
``end + grace`` (five seconds by default), then it is closed.  A valid late
execution is never discarded: the aggregate version is incremented and the
window is marked as revised.  Missing data is represented as an ``unavailable``
quality state, never as a real zero-volume minute.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Iterable, Sequence

from .contracts import (
    DEFAULT_LATE_GRACE_MS,
    WINDOW_MS,
    DeliveryKind,
    ExecutionEvent,
    QualityState,
    WindowAggregate,
)
from .storage import MarketPulseStore


@dataclass(frozen=True, slots=True)
class AggregatedNumbers:
    event_count: int
    execution_count: int
    buy_base: Decimal | None
    sell_base: Decimal | None
    buy_quote: Decimal | None
    sell_quote: Decimal | None
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    first_event_ms: int | None
    last_event_ms: int | None


def aggregate_events(events: Sequence[ExecutionEvent]) -> AggregatedNumbers:
    ordered = sorted(events, key=lambda event: (event.event_time_ms, event.trade_id))
    buys = [event for event in ordered if event.side == "buy"]
    sells = [event for event in ordered if event.side == "sell"]

    def side_volume(rows: Iterable[ExecutionEvent], attribute: str) -> Decimal | None:
        known = [getattr(event, attribute) for event in rows if getattr(event, attribute) is not None]
        if not known:
            return None
        return sum(known, Decimal(0))

    prices = [event.price for event in ordered]
    return AggregatedNumbers(
        event_count=len(ordered),
        execution_count=sum(event.execution_count for event in ordered),
        buy_base=side_volume(buys, "base_amount"),
        sell_base=side_volume(sells, "base_amount"),
        buy_quote=side_volume(buys, "quote_amount"),
        sell_quote=side_volume(sells, "quote_amount"),
        open=prices[0] if prices else None,
        high=max(prices) if prices else None,
        low=min(prices) if prices else None,
        close=prices[-1] if prices else None,
        first_event_ms=ordered[0].event_time_ms if ordered else None,
        last_event_ms=ordered[-1].event_time_ms if ordered else None,
    )


def _signature(window: WindowAggregate) -> tuple[object, ...]:
    return (
        window.quality,
        window.event_count,
        window.execution_count,
        window.buy_base,
        window.sell_base,
        window.buy_quote,
        window.sell_quote,
        window.open,
        window.high,
        window.low,
        window.close,
        window.first_event_ms,
        window.last_event_ms,
        window.provisional,
        window.revised_at_ms is not None,
        window.coverage,
    )


class WindowService:
    def __init__(
        self,
        store: MarketPulseStore,
        *,
        grace_ms: int = DEFAULT_LATE_GRACE_MS,
        baseline_version: str = "unassigned",
    ) -> None:
        self.store = store
        self.grace_ms = grace_ms
        self.baseline_version = baseline_version

    async def rebuild_affected(
        self,
        affected: Iterable[tuple[str, str, str, str, int]],
        *,
        now_ms: int,
    ) -> list[WindowAggregate]:
        result = []
        for venue, market, channel, aggregation_kind, window_start_ms in sorted(set(affected)):
            result.append(
                await self.rebuild(
                    venue,
                    market,
                    channel,
                    aggregation_kind,
                    window_start_ms,
                    now_ms=now_ms,
                )
            )
        return result

    async def rebuild(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        aggregation_kind: str,
        window_start_ms: int,
        *,
        now_ms: int,
    ) -> WindowAggregate:
        window_start_ms = window_start_ms // WINDOW_MS * WINDOW_MS
        window_end_ms = window_start_ms + WINDOW_MS
        events = await self.store.list_events_for_window(
            venue,
            native_market_id,
            channel,
            aggregation_kind,
            window_start_ms,
        )
        numbers = aggregate_events(events)
        quality, coverage = await self._quality_for(
            venue,
            native_market_id,
            channel,
            events,
            window_start_ms,
            window_end_ms,
            now_ms,
        )
        provisional = now_ms < window_end_ms + self.grace_ms
        existing = await self.store.get_window(
            venue,
            native_market_id,
            channel,
            aggregation_kind,
            window_start_ms,
        )
        revised_at_ms = existing.revised_at_ms if existing else None
        if existing is not None and not existing.provisional and not provisional:
            # A late event (or a quality change) after close is a correction.
            if _signature(replace(existing, revised_at_ms=None)) != _signature(
                replace(
                    existing,
                    quality=quality,
                    event_count=numbers.event_count,
                    execution_count=numbers.execution_count,
                    buy_base=_zero_if_covered(numbers.buy_base, numbers.event_count, quality),
                    sell_base=_zero_if_covered(numbers.sell_base, numbers.event_count, quality),
                    buy_quote=_zero_if_covered(numbers.buy_quote, numbers.event_count, quality),
                    sell_quote=_zero_if_covered(numbers.sell_quote, numbers.event_count, quality),
                    open=numbers.open,
                    high=numbers.high,
                    low=numbers.low,
                    close=numbers.close,
                    first_event_ms=numbers.first_event_ms,
                    last_event_ms=numbers.last_event_ms,
                    provisional=provisional,
                    coverage=coverage,
                    revised_at_ms=None,
                )
            ):
                revised_at_ms = now_ms

        window = WindowAggregate(
            venue=venue,
            native_market_id=native_market_id,
            channel=channel,
            aggregation_kind=aggregation_kind,
            window_start_ms=window_start_ms,
            window_end_ms=window_end_ms,
            version=1 if existing is None else existing.version + 1,
            quality=quality,
            event_count=numbers.event_count,
            execution_count=numbers.execution_count,
            buy_base=_zero_if_covered(numbers.buy_base, numbers.event_count, quality),
            sell_base=_zero_if_covered(numbers.sell_base, numbers.event_count, quality),
            buy_quote=_zero_if_covered(numbers.buy_quote, numbers.event_count, quality),
            sell_quote=_zero_if_covered(numbers.sell_quote, numbers.event_count, quality),
            open=numbers.open,
            high=numbers.high,
            low=numbers.low,
            close=numbers.close,
            first_event_ms=numbers.first_event_ms,
            last_event_ms=numbers.last_event_ms,
            provisional=provisional,
            revised_at_ms=revised_at_ms,
            baseline_version=self.baseline_version,
            coverage=coverage,
        )
        if existing is not None and _signature(existing) == _signature(window):
            return existing
        await self.store.upsert_window(window)
        return window

    async def close_due(self, *, now_ms: int) -> list[WindowAggregate]:
        rows = await self.store.list_windows(limit=100_000)
        due = [
            window
            for window in rows
            if window.provisional and now_ms >= window.window_end_ms + self.grace_ms
        ]
        result = []
        for window in due:
            result.append(
                await self.rebuild(
                    window.venue,
                    window.native_market_id,
                    window.channel,
                    window.aggregation_kind,
                    window.window_start_ms,
                    now_ms=now_ms,
                )
            )
        return result

    async def _quality_for(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        events: Sequence[ExecutionEvent],
        window_start_ms: int,
        window_end_ms: int,
        now_ms: int,
    ) -> tuple[str, dict[str, object]]:
        coverage = await self.store.get_coverage(venue, native_market_id, channel)
        gaps = await self.store.gaps_overlapping(
            venue,
            native_market_id,
            channel,
            window_start_ms,
            window_end_ms,
        )
        conflict_marked = False
        for gap in gaps:
            if gap.state == QualityState.GAP_DETECTED.value:
                conflict_marked = True
                break
        recovery_events = sum(
            1 for event in events if event.delivery_kind == DeliveryKind.RECOVERY.value
        )
        notional_blocked = sum(1 for event in events if event.base_amount is None)
        coverage_payload: dict[str, object] = {
            "coverage_state": None if coverage is None else coverage.state,
            "source_epoch": None if coverage is None else coverage.source_epoch,
            "gap_ids": [gap.gap_id for gap in gaps],
            "recovered_events": recovery_events,
            "live_events": len(events) - recovery_events,
            "notional_blocked_events": notional_blocked,
            "window_start_ms": window_start_ms,
            "window_end_ms": window_end_ms,
        }

        if coverage is not None:
            coverage_payload["last_event_ms"] = coverage.last_event_ms
            coverage_payload["last_receive_ms"] = coverage.last_receive_ms
            coverage_payload["heartbeat_ms"] = coverage.heartbeat_ms
        if coverage is not None and coverage.state == QualityState.UNAVAILABLE.value:
            return QualityState.UNAVAILABLE.value, coverage_payload
        if coverage is not None and coverage.state == QualityState.WARMING_UP.value:
            return QualityState.WARMING_UP.value, coverage_payload
        if coverage is not None and coverage.state == QualityState.RECOVERING.value:
            return QualityState.RECOVERING.value, coverage_payload
        if any(gap.state in {QualityState.GAP_DETECTED.value, QualityState.RECOVERING.value} for gap in gaps):
            return QualityState.GAP_DETECTED.value, coverage_payload
        if coverage is not None and coverage.state == QualityState.RECONCILED.value:
            return QualityState.RECONCILED.value, coverage_payload
        if gaps and all(gap.state == QualityState.RECONCILED.value for gap in gaps):
            return QualityState.RECONCILED.value, coverage_payload
        if now_ms < window_end_ms + self.grace_ms:
            return QualityState.WARMING_UP.value, coverage_payload
        if coverage is None and not events:
            return QualityState.UNAVAILABLE.value, coverage_payload
        if conflict_marked:
            return QualityState.GAP_DETECTED.value, coverage_payload
        return QualityState.OBSERVED_NO_DETECTED_GAP.value, coverage_payload


def _zero_if_covered(value: Decimal | None, event_count: int, quality: str) -> Decimal | None:
    """Return a real zero only for a covered window with no unknown-unit events."""
    if value is not None:
        return value
    if event_count == 0 and quality not in {
        QualityState.UNAVAILABLE.value,
        QualityState.GAP_DETECTED.value,
        QualityState.RECOVERING.value,
    }:
        return Decimal(0)
    return None

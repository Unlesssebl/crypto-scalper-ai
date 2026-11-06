"""Gap lifecycle and channel coverage.

A websocket reconnect does not prove that no trade was lost.  The collector
reports disconnects and reconnects; the gap stays open until raw/history
recovery reconciles it or an operator explicitly marks it unrecoverable.  A
fresh order book or heartbeat is tracked separately from the trades channel.
"""
from __future__ import annotations

from typing import Any

from .contracts import Gap, QualityState
from .storage import ChannelCoverage, MarketPulseStore


class GapTracker:
    def __init__(self, store: MarketPulseStore) -> None:
        self.store = store

    async def _coverage(self, venue: str, market: str, channel: str) -> ChannelCoverage | None:
        return await self.store.get_coverage(venue, market, channel)

    async def on_connected(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        source_epoch: str,
        now_ms: int,
    ) -> ChannelCoverage:
        current = await self._coverage(venue, native_market_id, channel)
        open_gaps = await self.store.list_gaps(
            venue=venue,
            native_market_id=native_market_id,
            channel=channel,
            state=QualityState.GAP_DETECTED.value,
        )
        reason: str | None = None
        if open_gaps:
            state = QualityState.RECOVERING.value
            reason = "subscription acknowledged; interval is not reconciled"
        elif (
            current is not None
            and current.last_receive_ms is not None
            and current.source_epoch != source_epoch
        ):
            # A new epoch means the collector restarted.  A clean stop does not
            # raise a disconnect, but the downtime is still missing data, so the
            # interval from the last received trade is recorded explicitly.
            await self.store.add_gap(
                Gap(
                    gap_id=0,
                    venue=venue,
                    native_market_id=native_market_id,
                    channel=channel,
                    source_epoch=source_epoch,
                    start_ms=current.last_receive_ms,
                    detected_at_ms=now_ms,
                    state=QualityState.GAP_DETECTED.value,
                    reason="source epoch changed: collector restarted",
                )
            )
            state = QualityState.RECOVERING.value
            reason = "source epoch changed; interval is not reconciled"
        else:
            state = QualityState.OBSERVED_NO_DETECTED_GAP.value
        coverage = ChannelCoverage(
            venue=venue,
            native_market_id=native_market_id,
            channel=channel,
            source_epoch=source_epoch,
            state=state,
            updated_at_ms=now_ms,
            last_event_ms=None if current is None else current.last_event_ms,
            last_receive_ms=None if current is None else current.last_receive_ms,
            heartbeat_ms=now_ms,
            connected_since_ms=now_ms,
            reason=reason,
        )
        await self.store.set_coverage(coverage)
        return coverage

    async def on_heartbeat(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        now_ms: int,
    ) -> ChannelCoverage | None:
        current = await self._coverage(venue, native_market_id, channel)
        if current is None:
            return None
        coverage = ChannelCoverage(
            **{
                **current.to_payload(),
                "heartbeat_ms": now_ms,
                "updated_at_ms": now_ms,
            }
        )
        await self.store.set_coverage(coverage)
        return coverage

    async def on_disconnected(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        source_epoch: str,
        now_ms: int,
        *,
        reason: str,
        last_receive_ms: int | None = None,
    ) -> Gap:
        existing = await self.store.list_gaps(
            venue=venue,
            native_market_id=native_market_id,
            channel=channel,
            state=QualityState.GAP_DETECTED.value,
        )
        if existing:
            gap = existing[-1]
        else:
            gap = await self.store.add_gap(
                Gap(
                    gap_id=0,
                    venue=venue,
                    native_market_id=native_market_id,
                    channel=channel,
                    source_epoch=source_epoch,
                    start_ms=last_receive_ms if last_receive_ms is not None else now_ms,
                    detected_at_ms=now_ms,
                    state=QualityState.GAP_DETECTED.value,
                    reason=reason,
                )
            )
        current = await self._coverage(venue, native_market_id, channel)
        coverage = ChannelCoverage(
            venue=venue,
            native_market_id=native_market_id,
            channel=channel,
            source_epoch=source_epoch,
            state=QualityState.GAP_DETECTED.value,
            updated_at_ms=now_ms,
            last_event_ms=None if current is None else current.last_event_ms,
            last_receive_ms=None if current is None else current.last_receive_ms,
            heartbeat_ms=None,
            connected_since_ms=None if current is None else current.connected_since_ms,
            reason=reason,
        )
        await self.store.set_coverage(coverage)
        return gap

    async def touch_events(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        source_epoch: str,
        *,
        now_ms: int,
        event_time_ms: int,
        receive_time_ms: int,
    ) -> ChannelCoverage:
        current = await self._coverage(venue, native_market_id, channel)
        state = QualityState.OBSERVED_NO_DETECTED_GAP.value
        reason = None
        if current is not None:
            if current.state in {QualityState.GAP_DETECTED.value, QualityState.RECOVERING.value}:
                state = current.state
                reason = current.reason
            elif current.source_epoch != source_epoch:
                state = QualityState.WARMING_UP.value
                reason = "source epoch changed"
        coverage = ChannelCoverage(
            venue=venue,
            native_market_id=native_market_id,
            channel=channel,
            source_epoch=source_epoch,
            state=state,
            updated_at_ms=now_ms,
            last_event_ms=event_time_ms,
            last_receive_ms=receive_time_ms,
            heartbeat_ms=now_ms,
            connected_since_ms=None if current is None else current.connected_since_ms,
            reason=reason,
        )
        await self.store.set_coverage(coverage)
        return coverage

    async def mark_reconciled(
        self,
        gap_id: int,
        *,
        end_ms: int,
        method: str,
        now_ms: int,
        venue: str | None = None,
        native_market_id: str | None = None,
        channel: str | None = None,
    ) -> Gap | None:
        gap = await self.store.update_gap(
            gap_id,
            end_ms=end_ms,
            state=QualityState.RECONCILED.value,
            method=method,
        )
        if gap is None:
            return None
        venue = venue or gap.venue
        native_market_id = native_market_id or gap.native_market_id
        channel = channel or gap.channel
        current = await self._coverage(venue, native_market_id, channel)
        if current is not None:
            await self.store.set_coverage(
                ChannelCoverage(
                    venue=venue,
                    native_market_id=native_market_id,
                    channel=channel,
                    source_epoch=current.source_epoch,
                    state=QualityState.RECONCILED.value,
                    updated_at_ms=now_ms,
                    last_event_ms=current.last_event_ms,
                    last_receive_ms=current.last_receive_ms,
                    heartbeat_ms=current.heartbeat_ms,
                    connected_since_ms=current.connected_since_ms,
                    reason=f"reconciled via {method}",
                )
            )
        return gap

    async def mark_unavailable(
        self,
        venue: str,
        native_market_id: str,
        channel: str,
        source_epoch: str,
        now_ms: int,
        *,
        reason: str,
    ) -> ChannelCoverage:
        coverage = ChannelCoverage(
            venue=venue,
            native_market_id=native_market_id,
            channel=channel,
            source_epoch=source_epoch,
            state=QualityState.UNAVAILABLE.value,
            updated_at_ms=now_ms,
            heartbeat_ms=None,
            reason=reason,
        )
        await self.store.set_coverage(coverage)
        return coverage

    async def summary(self) -> dict[str, Any]:
        coverage = await self.store.list_coverage()
        gaps = await self.store.list_gaps(limit=10_000)
        counts: dict[str, int] = {}
        for gap in gaps:
            counts[gap.state] = counts.get(gap.state, 0) + 1
        return {
            "channels": [item.to_payload() for item in coverage],
            "gap_states": counts,
            "channels_total": len(coverage),
            "gaps_total": len(gaps),
        }

"""Bounded recovery from raw archives and optional historical sources.

Recovery never assumes that trade IDs are contiguous.  It considers a gap
reconciled only when the archive actually covers both gap boundaries; a partial
or truncated archive leaves the gap open and the affected windows excluded from
baselines.
"""
from __future__ import annotations

from dataclasses import replace
from typing import Any, Callable, Iterable, Mapping, Sequence

from .contracts import DeliveryKind, ExecutionEvent, QualityState, known_channel
from .gaps import GapTracker
from .ingest import IngestionService
from .raw_archive import RawArchive
from .storage import MarketPulseStore

Normalizer = Callable[..., Sequence[ExecutionEvent]]


class RawRecoveryService:
    def __init__(
        self,
        store: MarketPulseStore,
        ingest: IngestionService,
        gaps: GapTracker,
    ) -> None:
        self.store = store
        self.ingest = ingest
        self.gaps = gaps

    async def recover_from_raw(
        self,
        archive: RawArchive,
        *,
        venue: str,
        market: str,
        channel: str,
        start_ms: int,
        end_ms: int,
        normalize: Normalizer,
        batch_size: int = 500,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        now_ms = now_ms or _now_ms()
        batch: list[ExecutionEvent] = []
        scanned = 0
        accepted = 0
        duplicates = 0
        conflicts = 0
        min_receive: int | None = None
        max_receive: int | None = None
        for envelope, reference in archive.iter_records(start_ms=start_ms, end_ms=end_ms):
            scanned += 1
            receive_time_ms = int(envelope.get("receive_time_ms", 0))
            source_epoch = str(envelope.get("source_epoch") or "recovery")
            payload = envelope.get("payload")
            if not isinstance(payload, Mapping):
                continue
            events = normalize(
                payload,
                receive_time_ms=receive_time_ms,
                source_epoch=source_epoch,
                raw_ref=str(reference),
            )
            for event in events:
                if event.venue != venue or event.native_market_id != market or event.channel != channel:
                    continue
                batch.append(replace(event, delivery_kind=DeliveryKind.RECOVERY.value))
                min_receive = receive_time_ms if min_receive is None else min(min_receive, receive_time_ms)
                max_receive = receive_time_ms if max_receive is None else max(max_receive, receive_time_ms)
            if len(batch) >= batch_size:
                result = await self.ingest.append_batch(
                    batch, now_ms=now_ms, touch_coverage=False
                )
                accepted += result["accepted"]
                duplicates += result["duplicates"]
                conflicts += result["conflicts"]
                batch.clear()

        if batch:
            result = await self.ingest.append_batch(batch, now_ms=now_ms, touch_coverage=False)
            accepted += result["accepted"]
            duplicates += result["duplicates"]
            conflicts += result["conflicts"]

        reconciled = 0
        remaining = 0
        if archive.last_error is None and min_receive is not None and max_receive is not None:
            gaps = await self.store.gaps_overlapping(venue, market, channel, start_ms, end_ms)
            for gap in gaps:
                if gap.state != QualityState.GAP_DETECTED.value:
                    continue
                if gap.reason and gap.reason.startswith("trade id conflict"):
                    # A content conflict is not missing data; raw replay must
                    # not silently turn it into a reconciled window.
                    remaining += 1
                    continue
                # A closed gap needs records at both boundaries.  An open gap
                # is proven by a boundary frame at its start plus data that
                # continued after it: a socket outage leaves only the boundary
                # frame, while an ingest outage leaves records throughout.
                if gap.end_ms is None:
                    covered = min_receive <= gap.start_ms and max_receive > gap.start_ms
                    reconcile_end = max_receive
                else:
                    covered = min_receive <= gap.start_ms and max_receive >= gap.end_ms
                    reconcile_end = gap.end_ms
                if covered:
                    await self.gaps.mark_reconciled(
                        gap.gap_id,
                        end_ms=reconcile_end,
                        method="raw",
                        now_ms=now_ms,
                        venue=venue,
                        native_market_id=market,
                        channel=channel,
                    )
                    await self.ingest.rebuild_range(
                        venue,
                        market,
                        channel,
                        known_channel(venue, channel).value,
                        gap.start_ms,
                        reconcile_end,
                        now_ms=now_ms,
                    )
                    reconciled += 1
                else:
                    remaining += 1
        return {
            "status": "partial" if archive.last_error or remaining else "ok",
            "venue": venue,
            "market": market,
            "channel": channel,
            "scanned": scanned,
            "accepted": accepted,
            "duplicates": duplicates,
            "conflicts": conflicts,
            "reconciled_gaps": reconciled,
            "remaining_gaps": remaining,
            "raw_error": archive.last_error,
            "window_start_ms": min_receive,
            "window_end_ms": max_receive,
        }

    async def recover_from_source(
        self,
        fetch: Callable[[str, str, int, int], Any],
        *,
        venue: str,
        market: str,
        channel: str,
        start_ms: int,
        end_ms: int,
        normalize: Normalizer,
        now_ms: int | None = None,
    ) -> dict[str, Any]:
        """Recover from a compatible history source with explicit boundaries.

        ``fetch`` returns source messages; deduplication is the normal store
        path, so overlap with live data is harmless.  The source is not assumed
        to be complete: the caller must supply already-verified boundaries.
        """
        now_ms = now_ms or _now_ms()
        rows = await fetch(venue, market, start_ms, end_ms)
        events: list[ExecutionEvent] = []
        for index, row in enumerate(rows):
            raw_ref = f"history:{venue}:{market}:{start_ms}:{end_ms}:{index}"
            for event in normalize(
                row,
                receive_time_ms=now_ms,
                source_epoch=f"history-{start_ms}",
                raw_ref=raw_ref,
            ):
                if event.venue == venue and event.native_market_id == market and event.channel == channel:
                    events.append(replace(event, delivery_kind=DeliveryKind.RECOVERY.value))
        result = await self.ingest.append_batch(events, now_ms=now_ms, touch_coverage=False)
        return {
            "status": "ok",
            "venue": venue,
            "market": market,
            "channel": channel,
            "accepted": result["accepted"],
            "duplicates": result["duplicates"],
            "conflicts": result["conflicts"],
            "scanned": len(events),
        }


class AutoRawRecovery:
    """Automatically reconcile a recovering gap from the local raw archive.

    This is best-effort and intentionally conservative: it runs only after the
    collector has reconnected (coverage state ``recovering`` with a known
    ``connected_since_ms``), and a gap is reconciled only when raw actually
    covers both boundaries.  A collector that was down leaves the gap open.
    """

    def __init__(
        self,
        store: MarketPulseStore,
        ingest: IngestionService,
        gaps: GapTracker,
        *,
        raw_root: str,
        min_attempt_interval_ms: int = 60_000,
    ) -> None:
        self.store = store
        self.ingest = ingest
        self.gaps = gaps
        self.raw_root = raw_root
        self.min_attempt_interval_ms = min_attempt_interval_ms
        self._attempted: dict[int, int] = {}

    async def run_once(self, *, now_ms: int) -> list[dict[str, Any]]:
        from .collectors.hyperliquid import normalize_hyperliquid_trades
        from .collectors.okx import normalize_okx_trades
        from .collectors.specs import default_instruments, spec_index

        stored_gaps = await self.store.list_gaps(state=QualityState.GAP_DETECTED.value, limit=10_000)
        specs = spec_index(default_instruments())
        results: list[dict[str, Any]] = []
        for gap in stored_gaps:
            if gap.reason and gap.reason.startswith("trade id conflict"):
                continue
            last_attempt = self._attempted.get(gap.gap_id, 0)
            if now_ms - last_attempt < self.min_attempt_interval_ms:
                continue
            coverage = await self.store.get_coverage(
                gap.venue, gap.native_market_id, gap.channel
            )
            if (
                coverage is None
                or coverage.state != QualityState.RECOVERING.value
                or coverage.connected_since_ms is None
            ):
                continue
            end_ms = min(coverage.connected_since_ms, now_ms)
            if end_ms <= gap.start_ms:
                continue
            self._attempted[gap.gap_id] = now_ms
            spec = await self.store.get_instrument_spec(
                gap.venue, gap.native_market_id, gap.start_ms
            )
            if spec is None:
                spec = specs.get((gap.venue, gap.native_market_id))
            if spec is None:
                continue
            if gap.venue == "okx":
                normalizer = lambda payload, **kwargs: normalize_okx_trades(payload, spec, **kwargs)  # noqa: E731
            else:
                normalizer = lambda payload, **kwargs: normalize_hyperliquid_trades(payload, spec, **kwargs)  # noqa: E731
            service = RawRecoveryService(self.store, self.ingest, self.gaps)
            result = await service.recover_from_raw(
                RawArchive(self.raw_root, gap.venue, read_only=True),
                venue=gap.venue,
                market=gap.native_market_id,
                channel=gap.channel,
                start_ms=gap.start_ms,
                end_ms=end_ms,
                normalize=normalizer,
                now_ms=now_ms,
            )
            results.append(result)
        return results


def _now_ms() -> int:
    import time

    return int(time.time() * 1_000)


def recovery_specs(venue: str, markets: Iterable[str]):
    from .collectors.specs import default_instruments, spec_index

    return spec_index([spec for spec in default_instruments(tuple(markets)) if spec.venue == venue])

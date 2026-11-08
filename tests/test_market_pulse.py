"""P0-P2 Market Pulse contract, ingestion, window, recovery and API tests."""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace
from decimal import Decimal

import pytest
from aiohttp.test_utils import TestClient, TestServer

from hyperdata_terminal.market_pulse.api import create_app
from hyperdata_terminal.market_pulse.collectors.hyperliquid import (
    normalize_hyperliquid_trades,
)
from hyperdata_terminal.market_pulse.collectors.okx import normalize_okx_trades
from hyperdata_terminal.market_pulse.collectors.specs import default_instruments, spec_index
from hyperdata_terminal.market_pulse.contracts import (
    WINDOW_MS,
    ContractError,
    DeliveryKind,
    ExecutionEvent,
    Gap,
    InstrumentSpec,
    QualityState,
    WindowAggregate,
)
from hyperdata_terminal.market_pulse.gaps import GapTracker
from hyperdata_terminal.market_pulse.ingest import IngestionService
from hyperdata_terminal.market_pulse.raw_archive import RawArchive
from hyperdata_terminal.market_pulse.recovery import RawRecoveryService
from hyperdata_terminal.market_pulse.storage import ChannelCoverage, InMemoryMarketPulseStore
from hyperdata_terminal.market_pulse.windows import WindowService

MINUTE = 1_700_000_040_000 // 60_000 * 60_000


def spec_for(venue: str, market: str) -> InstrumentSpec:
    specs = spec_index(default_instruments())
    return specs[(venue, market)]


def event(
    *,
    venue: str = "hyperliquid",
    market: str = "BTC",
    trade_id: str = "t1",
    event_time_ms: int = MINUTE + 1_000,
    receive_time_ms: int | None = None,
    price: str = "100",
    amount: str = "2",
    side: str = "buy",
) -> ExecutionEvent:
    spec = spec_for(venue, market)
    base = spec.base_quantity(amount)
    return ExecutionEvent(
        venue=venue,
        native_market_id=market,
        channel="trades" if venue == "hyperliquid" else "trades-all",
        aggregation_kind="execution",
        trade_id=trade_id,
        event_time_ms=event_time_ms,
        receive_time_ms=event_time_ms if receive_time_ms is None else receive_time_ms,
        source_epoch="epoch-1",
        side=side,
        price=Decimal(price),
        native_amount=Decimal(amount),
        base_amount=base,
        quote_amount=base * Decimal(price) if base is not None else None,
        instrument_spec_version=spec.spec_version,
    )


def test_contract_rejects_floats_and_unknown_notional_unit():
    with pytest.raises(ContractError):
        event(price="100").__class__(
            **{**event().to_payload(), "price": 100.0, "schema_version": "market-pulse.event.v1"}
        )

    unknown = InstrumentSpec(
        venue="okx",
        native_market_id="XRP-USDT-SWAP",
        base_asset="XRP",
        market_type="perp",
        quote_currency="USDT",
        settlement_currency="USDT",
        contract_type="linear",
        native_amount_unit="unknown",
        contract_size=None,
        contract_multiplier=None,
        spec_version="unknown-v1",
        valid_from_ms=0,
        source="test",
    )
    assert unknown.base_quantity("10") is None
    assert unknown.is_v1_eligible is False


def test_duplicate_fingerprint_ignores_envelope_but_detects_content_change():
    first = event()
    same = replace(first, receive_time_ms=first.receive_time_ms + 5_000, raw_ref="other:1")
    changed = replace(first, price=Decimal("101"))
    assert first.content_fingerprint() == same.content_fingerprint()
    assert first.content_fingerprint() != changed.content_fingerprint()


def test_channel_aggregation_kind_is_not_mixed():
    with pytest.raises(ContractError):
        replace(event(venue="okx", market="BTC-USDT-SWAP"), aggregation_kind="aggregated")


def test_collector_normalizers_convert_contracts_to_base_quantity():
    hyperliquid = normalize_hyperliquid_trades(
        {
            "channel": "trades",
            "data": [
                {"coin": "BTC", "side": "B", "px": "42000.5", "sz": "0.25", "time": 1_700_000_000_123, "tid": 9}
            ],
        },
        spec_for("hyperliquid", "BTC"),
        receive_time_ms=1_700_000_000_200,
        source_epoch="e1",
        raw_ref="seg:1",
    )
    assert hyperliquid[0].base_amount == Decimal("0.25")
    assert hyperliquid[0].quote_amount == Decimal("10500.125")

    okx = normalize_okx_trades(
        {
            "arg": {"channel": "trades-all", "instId": "BTC-USDT-SWAP"},
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "tradeId": "55",
                    "px": "42000",
                    "sz": "3",
                    "side": "sell",
                    "ts": "1700000000123",
                    "source": "0",
                }
            ],
        },
        spec_for("okx", "BTC-USDT-SWAP"),
        receive_time_ms=1_700_000_000_200,
        source_epoch="e1",
        raw_ref="seg:2",
    )
    # Live `trades-all` rows carry no aggregation count: one row, one execution.
    assert okx[0].execution_count == 1
    assert okx[0].aggregation_kind == "execution"
    assert okx[0].base_amount == Decimal("0.03")
    assert okx[0].quote_amount == Decimal("1260.00")


async def test_okx_aggregated_channel_is_never_mixed_with_execution():
    store, _gaps, _windows, ingest = await make_services()
    spec = spec_for("okx", "BTC-USDT-SWAP")
    event_time = MINUTE + 1_000

    def payload(channel: str) -> dict:
        row = {
            "instId": "BTC-USDT-SWAP",
            "tradeId": "77",
            "px": "42000",
            "sz": "3",
            "side": "sell",
            "ts": str(event_time),
            "source": "0",
        }
        if channel == "trades":
            # Only the aggregated public channel carries a count.
            row["count"] = "3"
            row["seqId"] = 123
        return {"arg": {"channel": channel, "instId": "BTC-USDT-SWAP"}, "data": [row]}

    execution = normalize_okx_trades(
        payload("trades-all"), spec, receive_time_ms=event_time, source_epoch="e1", raw_ref="a:1"
    )[0]
    aggregated = normalize_okx_trades(
        payload("trades"), spec, receive_time_ms=event_time, source_epoch="e1", raw_ref="a:2"
    )[0]
    assert execution.aggregation_kind == "execution"
    assert aggregated.aggregation_kind == "aggregated"
    assert execution.execution_count == 1
    assert aggregated.execution_count == 3
    assert execution.dedup_key != aggregated.dedup_key
    assert execution.content_fingerprint() != aggregated.content_fingerprint()

    result = await ingest.append_batch([execution, aggregated], now_ms=event_time)
    assert result["accepted"] == 2

    execution_rows = await store.list_events_for_window(
        "okx", "BTC-USDT-SWAP", "trades-all", "execution", MINUTE
    )
    aggregated_rows = await store.list_events_for_window(
        "okx", "BTC-USDT-SWAP", "trades", "aggregated", MINUTE
    )
    assert [row.trade_id for row in execution_rows] == ["77"]
    assert [row.trade_id for row in aggregated_rows] == ["77"]
    assert execution_rows[0].base_amount == Decimal("0.03")
    assert aggregated_rows[0].base_amount == Decimal("0.03")


async def make_services():
    store = InMemoryMarketPulseStore()
    await store.register_instruments(default_instruments())
    gaps = GapTracker(store)
    await gaps.on_connected("hyperliquid", "BTC", "trades", "epoch-1", MINUTE)
    windows = WindowService(store, grace_ms=5_000)
    ingest = IngestionService(store, windows, gaps)
    return store, gaps, windows, ingest


async def test_ingest_is_idempotent_and_conflicts_are_explicit():
    store, _gaps, _windows, ingest = await make_services()
    first = await ingest.append_batch([event()], now_ms=MINUTE + 1_000)
    assert first["accepted"] == 1
    cursor = first["cursor"]

    duplicate = await ingest.append_batch([event(receive_time_ms=MINUTE + 2_000)], now_ms=MINUTE + 2_000)
    assert duplicate["status"] == "duplicate"
    assert duplicate["duplicates"] == 1
    assert duplicate["cursor"] == cursor

    conflict = await ingest.append_batch([event(price="101")], now_ms=MINUTE + 3_000)
    assert conflict["status"] == "conflict"
    assert conflict["conflicts"] == 1
    assert conflict["cursor"] == cursor
    assert await store.list_conflicts() != []
    gaps = await store.gaps_overlapping("hyperliquid", "BTC", "trades", MINUTE, MINUTE + 60_000)
    assert gaps and gaps[0].state == QualityState.GAP_DETECTED.value


async def test_window_closes_after_grace_and_late_trade_revises_version():
    store, gaps, windows, ingest = await make_services()
    result = await ingest.append_batch([event()], now_ms=MINUTE + 1_000)
    window = result["windows"][0]
    assert window["provisional"] is True
    assert window["quality"] == QualityState.WARMING_UP.value

    # Liveness must be proven through the end of the minute; a sent heartbeat
    # is not enough, so the collector reports a received frame here.
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 65_000)
    closed = await ingest.close_due(now_ms=MINUTE + 60_000 + 5_001)
    assert closed[0]["provisional"] is False
    assert closed[0]["quality"] == QualityState.OBSERVED_NO_DETECTED_GAP.value
    first_version = closed[0]["version"]

    late = await ingest.append_batch(
        [event(trade_id="t2", event_time_ms=MINUTE + 2_000, amount="1", side="sell")],
        now_ms=MINUTE + 60_000 + 10_000,
    )
    revised = late["windows"][0]
    assert revised["event_count"] == 2
    assert revised["version"] == first_version + 1
    assert revised["revised_at_ms"] == MINUTE + 60_000 + 10_000
    assert Decimal(revised["sell_base"]) == Decimal("1")
    assert await store.pending_recalc() == []


async def test_unavailable_channel_does_not_become_zero_activity():
    store, _gaps, windows, _ingest = await make_services()
    await store.set_coverage(
        ChannelCoverage(
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            state=QualityState.UNAVAILABLE.value,
            updated_at_ms=MINUTE,
            reason="test",
        )
    )
    window = await windows.rebuild(
        "hyperliquid",
        "BTC",
        "trades",
        "execution",
        MINUTE,
        now_ms=MINUTE + 60_000 + 10_000,
    )
    assert window.quality == QualityState.UNAVAILABLE.value
    assert window.buy_base is None
    assert window.event_count == 0


async def test_gap_lifecycle_requires_explicit_reconciliation():
    store, gaps, _windows, _ingest = await make_services()
    await gaps.on_connected("hyperliquid", "BTC", "trades", "e1", MINUTE)
    gap = await gaps.on_disconnected(
        "hyperliquid", "BTC", "trades", "e1", MINUTE + 1_000, reason="tcp reset", last_receive_ms=MINUTE + 500
    )
    assert gap.start_ms == MINUTE + 500
    await gaps.on_connected("hyperliquid", "BTC", "trades", "e2", MINUTE + 2_000)
    coverage = await store.get_coverage("hyperliquid", "BTC", "trades")
    assert coverage.state == QualityState.RECOVERING.value
    reconciled = await gaps.mark_reconciled(
        gap.gap_id, end_ms=MINUTE + 1_500, method="raw", now_ms=MINUTE + 3_000
    )
    assert reconciled.state == QualityState.RECONCILED.value
    assert (await store.get_coverage("hyperliquid", "BTC", "trades")).state == QualityState.RECONCILED.value


def test_raw_archive_keeps_readable_prefix_and_rotates(tmp_path):
    archive = RawArchive(tmp_path, "hyperliquid", max_bytes=300)
    for index in range(4):
        archive.append(
            {"channel": "trades", "data": [{"tid": index, "x": "y" * 40}]},
            receive_time_ms=MINUTE + index,
            source_epoch="e1",
        )
    archive.close()
    segments = archive.segments()
    assert len(segments) >= 2
    assert all(record.sealed and record.sha256 for record in segments)
    reader = RawArchive(tmp_path, "hyperliquid", read_only=True)
    assert reader.prune(now_ms=MINUTE + 10**12) == []
    records = list(reader.iter_records())
    assert [record[0]["payload"]["data"][0]["tid"] for record in records] == [0, 1, 2, 3]
    assert reader.last_error is None

    path = next((tmp_path / "hyperliquid").glob("*.jsonl"))
    path.with_suffix(path.suffix + ".sha256").unlink()
    data = path.read_bytes()
    path.write_bytes(data[:-10])
    broken = RawArchive(tmp_path, "hyperliquid", read_only=True)
    list(broken.iter_records())
    assert broken.last_error is not None


async def test_declared_outage_is_audited_and_not_zero_filled():
    store, gaps, windows, _ingest = await make_services()
    gap = await store.add_gap(
        Gap(
            gap_id=0,
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            start_ms=MINUTE,
            end_ms=None,
            detected_at_ms=MINUTE,
            state=QualityState.GAP_DETECTED.value,
            reason="source epoch changed: collector restarted",
        )
    )
    await store.set_coverage(
        ChannelCoverage(
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e2",
            state=QualityState.RECOVERING.value,
            updated_at_ms=MINUTE,
            connected_since_ms=MINUTE + 5_000,
        )
    )
    declared = await gaps.declare_outage(
        gap.gap_id, end_ms=MINUTE + 5_000, reason="planned redeploy", now_ms=MINUTE + 6_000
    )
    assert declared is not None
    assert declared.state == QualityState.DECLARED_OUTAGE.value
    assert declared.end_ms == MINUTE + 5_000
    assert declared.reason == "declared outage: planned redeploy"
    coverage = await store.get_coverage("hyperliquid", "BTC", "trades")
    assert coverage.state == QualityState.OBSERVED_NO_DETECTED_GAP.value
    assert "declared outage" in (coverage.reason or "")

    window = await windows.rebuild(
        "hyperliquid",
        "BTC",
        "trades",
        "execution",
        MINUTE,
        now_ms=MINUTE + 60_000 + 10_000,
    )
    assert window.quality == QualityState.DECLARED_OUTAGE.value
    # A declared outage is missing data, never a real zero.
    assert window.buy_base is None
    assert window.sell_base is None


async def test_raw_recovery_marks_only_covered_gap_reconciled(tmp_path):
    archive = RawArchive(tmp_path, "hyperliquid")
    event_time = MINUTE + 1_000
    archive.append(
        {
            "channel": "trades",
            "data": [
                {"coin": "BTC", "side": "B", "px": "100", "sz": "2", "time": event_time, "tid": 7}
            ],
        },
        receive_time_ms=event_time,
        source_epoch="e1",
    )
    archive.close()

    store, gaps, _windows, ingest = await make_services()
    gap = await store.add_gap(
        Gap(
            gap_id=0,
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            start_ms=event_time,
            end_ms=event_time,
            detected_at_ms=event_time,
            state=QualityState.GAP_DETECTED.value,
            reason="test",
        )
    )
    assert gap.gap_id > 0
    recovery = RawRecoveryService(store, ingest, gaps)
    spec = spec_for("hyperliquid", "BTC")
    result = await recovery.recover_from_raw(
        RawArchive(tmp_path, "hyperliquid", read_only=True),
        venue="hyperliquid",
        market="BTC",
        channel="trades",
        start_ms=event_time - 1,
        end_ms=event_time + 1,
        normalize=lambda payload, **kwargs: normalize_hyperliquid_trades(payload, spec, **kwargs),
        now_ms=event_time + 2,
    )
    assert result["accepted"] == 1
    assert result["reconciled_gaps"] == 1
    events = await store.list_events_for_window("hyperliquid", "BTC", "trades", "execution", MINUTE)
    assert events[0].delivery_kind == DeliveryKind.RECOVERY.value
    assert (await store.list_gaps())[0].state == QualityState.RECONCILED.value


async def test_raw_recovery_leaves_uncovered_gap_open(tmp_path):
    archive = RawArchive(tmp_path, "hyperliquid")
    event_time = MINUTE + 5_000
    archive.append(
        {
            "channel": "trades",
            "data": [
                {"coin": "BTC", "side": "B", "px": "100", "sz": "2", "time": event_time, "tid": 8}
            ],
        },
        receive_time_ms=event_time,
        source_epoch="e1",
    )
    archive.close()

    store, gaps, _windows, ingest = await make_services()
    await store.add_gap(
        Gap(
            gap_id=0,
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            start_ms=MINUTE,
            end_ms=MINUTE + 10_000,
            detected_at_ms=MINUTE,
            state=QualityState.GAP_DETECTED.value,
            reason="network",
        )
    )
    recovery = RawRecoveryService(store, ingest, gaps)
    spec = spec_for("hyperliquid", "BTC")
    result = await recovery.recover_from_raw(
        RawArchive(tmp_path, "hyperliquid", read_only=True),
        venue="hyperliquid",
        market="BTC",
        channel="trades",
        start_ms=MINUTE,
        end_ms=MINUTE + 10_000,
        normalize=lambda payload, **kwargs: normalize_hyperliquid_trades(payload, spec, **kwargs),
        now_ms=MINUTE + 10_001,
    )
    assert result["status"] == "partial"
    assert result["reconciled_gaps"] == 0
    assert result["remaining_gaps"] == 1
    assert (await store.list_gaps())[0].state == QualityState.GAP_DETECTED.value


async def test_reconnect_records_gap_and_raw_cannot_fake_coverage(tmp_path):
    archive = RawArchive(tmp_path, "hyperliquid")
    boundary_time = MINUTE + 1_000
    archive.append(
        {
            "channel": "trades",
            "data": [
                {"coin": "BTC", "side": "B", "px": "100", "sz": "2", "time": boundary_time, "tid": 12}
            ],
        },
        receive_time_ms=boundary_time,
        source_epoch="e1",
    )
    archive.close()

    store, gaps, _windows, ingest = await make_services()
    await store.set_coverage(
        ChannelCoverage(
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            state=QualityState.OBSERVED_NO_DETECTED_GAP.value,
            updated_at_ms=boundary_time,
            last_event_ms=boundary_time,
            last_receive_ms=boundary_time,
        )
    )
    # A clean restart has no disconnect callback, but the hole is still real.
    coverage = await gaps.on_connected(
        "hyperliquid", "BTC", "trades", "e2", MINUTE + 60_000
    )
    assert coverage.state == QualityState.RECOVERING.value
    stored = await store.list_gaps()
    assert len(stored) == 1
    assert stored[0].start_ms == boundary_time
    assert stored[0].state == QualityState.GAP_DETECTED.value

    # Only the pre-restart boundary frame exists, so replay must not reconcile.
    recovery = RawRecoveryService(store, ingest, gaps)
    spec = spec_for("hyperliquid", "BTC")
    result = await recovery.recover_from_raw(
        RawArchive(tmp_path, "hyperliquid", read_only=True),
        venue="hyperliquid",
        market="BTC",
        channel="trades",
        start_ms=boundary_time,
        end_ms=MINUTE + 60_000,
        normalize=lambda payload, **kwargs: normalize_hyperliquid_trades(payload, spec, **kwargs),
        now_ms=MINUTE + 60_001,
    )
    assert result["reconciled_gaps"] == 0
    assert result["remaining_gaps"] == 1
    assert (await store.list_gaps())[0].state == QualityState.GAP_DETECTED.value

    # A later reconnect while the first gap is still open must also be recorded.
    await store.set_coverage(
        ChannelCoverage(
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e2",
            state=QualityState.RECOVERING.value,
            updated_at_ms=MINUTE + 90_000,
            last_event_ms=MINUTE + 89_000,
            last_receive_ms=MINUTE + 89_500,
        )
    )
    await gaps.on_connected("hyperliquid", "BTC", "trades", "e3", MINUTE + 120_000)
    assert len(await store.list_gaps()) == 2


async def test_auto_recovery_only_after_reconnect_and_raw_coverage(tmp_path):
    archive = RawArchive(tmp_path, "hyperliquid")
    event_time = MINUTE + 1_000
    archive.append(
        {
            "channel": "trades",
            "data": [
                {"coin": "BTC", "side": "B", "px": "100", "sz": "2", "time": event_time, "tid": 11}
            ],
        },
        receive_time_ms=event_time,
        source_epoch="e1",
    )
    archive.close()

    store, gaps, _windows, ingest = await make_services()
    await store.add_gap(
        Gap(
            gap_id=0,
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            start_ms=event_time,
            end_ms=event_time,
            detected_at_ms=event_time,
            state=QualityState.GAP_DETECTED.value,
            reason="network",
        )
    )
    await store.set_coverage(
        ChannelCoverage(
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e2",
            state=QualityState.RECOVERING.value,
            updated_at_ms=event_time,
            connected_since_ms=event_time + 1,
        )
    )
    from hyperdata_terminal.market_pulse.recovery import AutoRawRecovery

    auto = AutoRawRecovery(store, ingest, gaps, raw_root=str(tmp_path), min_attempt_interval_ms=0)
    results = await auto.run_once(now_ms=event_time + 10)
    assert results and results[0]["reconciled_gaps"] == 1
    assert (await store.list_gaps())[0].state == QualityState.RECONCILED.value


async def test_acceptance_report_counts_only_closed_usable_minutes():
    from hyperdata_terminal.market_pulse.acceptance import (
        build_acceptance_report,
        render_markdown,
    )

    store, gaps, windows, ingest = await make_services()
    # Observed minutes 0 and 2; minute 1 is quiet inside the observed span;
    # minutes 3 and 4 are outside the observed span.
    await ingest.append_batch(
        [event(trade_id="t0", event_time_ms=MINUTE + 1_000)], now_ms=MINUTE + 2_000
    )
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 65_000)
    await ingest.close_due(now_ms=MINUTE + 66_000)
    await ingest.append_batch(
        [event(trade_id="t2", event_time_ms=MINUTE + 2 * 60_000 + 1_000)],
        now_ms=MINUTE + 2 * 60_000 + 2_000,
    )
    await gaps.on_heartbeat(
        "hyperliquid", "BTC", "trades", MINUTE + 3 * 60_000 + 1_000
    )

    # Minute 2 is lost and declared.
    gap = await store.add_gap(
        Gap(
            gap_id=0,
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            start_ms=MINUTE + 2 * 60_000,
            end_ms=None,
            detected_at_ms=MINUTE + 2 * 60_000,
            state=QualityState.GAP_DETECTED.value,
            reason="test",
        )
    )
    await gaps.declare_outage(
        gap.gap_id,
        end_ms=MINUTE + 3 * 60_000,
        reason="planned",
        now_ms=MINUTE + 10 * 60_000,
    )
    await windows.rebuild(
        "hyperliquid",
        "BTC",
        "trades",
        "execution",
        MINUTE + 2 * 60_000,
        now_ms=MINUTE + 10 * 60_000,
    )

    # A 5-minute request observed for only 3 minutes cannot pass, even though
    # every observed minute is usable.
    wide = await build_acceptance_report(
        store,
        start_ms=MINUTE,
        end_ms=MINUTE + 5 * 60_000,
        required_pct=99.0,
    )
    wide_row = wide["channels"][0]
    assert wide_row["effective_minutes"] == 3
    assert wide_row["outside_coverage_minutes"] == 2
    assert wide_row["coverage_span_pct"] == 60.0
    assert wide_row["usable_pct"] == 100.0
    assert wide_row["passed"] is False

    report = await build_acceptance_report(
        store,
        start_ms=MINUTE,
        end_ms=MINUTE + 3 * 60_000,
        required_pct=99.0,
    )
    row = report["channels"][0]
    assert row["effective_minutes"] == 3
    assert row["outside_coverage_minutes"] == 0
    assert row["closed_windows"] == 2
    assert row["declared_outage_windows"] == 1
    assert row["usable_windows"] == 2  # 1 observed + 1 quiet minute
    assert row["quiet_minutes"] == 1
    assert row["missing_windows"] == 0
    # Declared minutes are excluded from the denominator.
    assert row["usable_pct"] == 100.0
    assert row["coverage_span_pct"] == 100.0
    assert row["gap_states"] == {"declared_outage": 1}
    assert row["passed"] is True

    # If the channel cannot vouch for the quiet minute, it is a real loss.
    await store.set_coverage(
        ChannelCoverage(
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            state=QualityState.UNAVAILABLE.value,
            updated_at_ms=MINUTE + 9 * 60_000,
            reason="collector down",
        )
    )
    degraded = await build_acceptance_report(
        store,
        start_ms=MINUTE,
        end_ms=MINUTE + 3 * 60_000,
        required_pct=99.0,
    )
    row = degraded["channels"][0]
    assert row["missing_windows"] == 1
    assert row["quiet_minutes"] == 0
    assert row["usable_pct"] < 99.0
    assert row["passed"] is False
    assert degraded["passed"] is False

    rendered = render_markdown(degraded)
    assert "Market Pulse P3 acceptance" in rendered
    assert "hyperliquid" in rendered


async def test_read_only_api_cursor_and_versions():
    store, gaps, _windows, ingest = await make_services()
    await ingest.append_batch([event(), event(trade_id="t2", event_time_ms=MINUTE + 2_000)], now_ms=MINUTE + 2_000)
    # Fixed test timestamps are far in the past; refresh liveness so the
    # staleness guard does not (correctly) report the channel as dead.
    await gaps.on_heartbeat(
        "hyperliquid", "BTC", "trades", int(time.time() * 1_000)
    )
    app = create_app(store)
    async with TestClient(TestServer(app)) as client:
        health = await client.get("/health")
        assert health.status == 200
        coverage = await client.get("/coverage")
        assert coverage.status == 200
        windows = await client.get("/windows", params={"market": "BTC"})
        body = await windows.json()
        assert body["schema_version"] == "market-pulse.window.v1"
        assert body["windows"][0]["event_count"] == 2
        events = await client.get("/events", params={"cursor": 0, "limit": 10})
        payload = await events.json()
        assert [row["trade_id"] for row in payload["events"]] == ["t1", "t2"]
        assert payload["next_cursor"] == 2
        bad = await client.get("/events", params={"cursor": "nope"})
        assert bad.status == 400


class _NullSink:
    async def append_batch(self, events):
        return {"accepted": len(events)}

    async def report(self, payload):
        return {"status": "ok"}

    async def close(self):
        return None


def test_collector_wiring_matches_live_exchange_protocols(tmp_path):
    from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
    from hyperdata_terminal.market_pulse.collectors.hyperliquid import HyperliquidCollector
    from hyperdata_terminal.market_pulse.collectors.okx import OkxCollector

    config = CollectorConfig(
        venue="okx",
        markets=("BTC",),
        raw_dir=str(tmp_path),
        ingest_url="http://127.0.0.1:8430",
    )
    archive = RawArchive(tmp_path, "okx")
    try:
        okx = OkxCollector(config, archive, _NullSink())
        # `trades-all` is a business-channel stream; the public endpoint rejects it.
        assert "v5/business" in okx.url
        assert "8443" not in okx.url
        assert okx.is_ignorable({"event": "notice", "code": "64010"})
        assert okx.is_ignorable({"event": "subscribe", "arg": {"channel": "trades-all"}})
        assert okx.is_heartbeat_response("pong") is True
        assert okx.is_heartbeat_response({"channel": "trades"}) is False
        # Gap/coverage keys must match the native ids used by the events.
        assert okx.native_market_id("BTC") == "BTC-USDT-SWAP"
        assert okx.native_market_id("XRP") == "XRP-USDT-SWAP"

        hyperliquid = HyperliquidCollector(
            replace(config, venue="hyperliquid"), archive, _NullSink()
        )
        # A dict payload must never reach an unhashable set-membership test.
        assert hyperliquid.is_heartbeat_response({"channel": "pong"}) is True
        assert hyperliquid.is_heartbeat_response({"channel": "trades", "data": []}) is False
        assert hyperliquid.native_market_id("BTC") == "BTC"

        # A control frame after data must not be archived or normalized.
        import asyncio as _asyncio
        import json as _json

        _asyncio.run(
            okx._handle_payload(
                _json.dumps(
                    {
                        "event": "subscribe",
                        "arg": {"channel": "trades-all", "instId": "BTC-USDT-SWAP"},
                    }
                ),
                "epoch-1",
            )
        )
        assert okx._pending == []
    finally:
        archive.close()


class _FakeMessage:
    def __init__(self, type_, data):
        self.type = type_
        self.data = data


class _FakeWS:
    def __init__(self, messages, stop):
        self.messages = list(messages)
        self.stop = stop
        self.sent = []

    async def send_json(self, payload):
        self.sent.append(payload)

    async def send_str(self, payload):
        self.sent.append(payload)

    async def receive(self, timeout=None):
        import asyncio

        if self.messages:
            return self.messages.pop(0)
        self.stop.set()
        raise asyncio.TimeoutError

    async def close(self):
        return None


class _FakeSession:
    def __init__(self, ws):
        self.ws = ws

    async def ws_connect(self, *args, **kwargs):
        return self.ws


class _OrderGaps:
    def __init__(self, order):
        self.order = order

    async def connected(self, *args, **kwargs):
        self.order.append("connected")

    async def disconnected(self, *args, **kwargs):
        return None

    async def heartbeat(self, *args, **kwargs):
        self.order.append("heartbeat")

    async def unavailable(self, *args, **kwargs):
        return None


class _OrderSink:
    def __init__(self, order):
        self.order = order

    async def append_batch(self, events):
        self.order.append("append_batch")
        return {"accepted": len(events)}

    async def report(self, payload):
        return {"status": "ok"}

    async def close(self):
        return None


def test_connection_is_reported_before_early_data_is_ingested(tmp_path):
    import asyncio
    import json

    import aiohttp

    from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
    from hyperdata_terminal.market_pulse.collectors.okx import OkxCollector

    data_frame = {
        "arg": {"channel": "trades-all", "instId": "BTC-USDT-SWAP"},
        "data": [
            {
                "instId": "BTC-USDT-SWAP",
                "tradeId": "1",
                "px": "100",
                "sz": "1",
                "side": "buy",
                "ts": str(MINUTE + 1_000),
                "source": "0",
            }
        ],
    }
    ack_frame = {
        "event": "subscribe",
        "arg": {"channel": "trades-all", "instId": "BTC-USDT-SWAP"},
    }
    stop = asyncio.Event()
    ws = _FakeWS(
        [
            _FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps(data_frame)),
            _FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps(ack_frame)),
        ],
        stop,
    )
    order: list[str] = []
    config = CollectorConfig(
        venue="okx",
        markets=("BTC",),
        raw_dir=str(tmp_path),
        ingest_url="http://127.0.0.1:8430",
    )
    archive = RawArchive(tmp_path, "okx")
    try:
        collector = OkxCollector(config, archive, _OrderSink(order), _OrderGaps(order))
        asyncio.run(collector._run_connection(_FakeSession(ws), "epoch-1", stop))
        # The epoch must be observed before early data can overwrite it.
        assert order.index("connected") < order.index("append_batch")
    finally:
        archive.close()


def test_cli_entrypoint_and_service_import(monkeypatch):
    from datetime import datetime, timezone

    from hyperdata_terminal.market_pulse import __main__ as cli
    from hyperdata_terminal.market_pulse import service

    monkeypatch.setenv("PULSE_MARKETS", " btc, xrp ")
    assert service.markets() == ("BTC", "XRP")
    assert cli.build_parser().parse_args(["init-db"]).command == "init-db"
    outage = cli.build_parser().parse_args(
        ["declare-outage", "--gap-id", "7", "--reason", "planned"]
    )
    assert outage.command == "declare-outage"
    assert outage.gap_id == 7
    expected = int(datetime(2026, 11, 6, 10, tzinfo=timezone.utc).timestamp() * 1_000)
    assert service.parse_boundary("2026-11-06T10:00:00Z", default_ms=0) == expected
    assert service.parse_boundary("1793959200", default_ms=0) == expected
    assert (
        service.safe_database_url("postgresql://user:s3cret@db:5432/marketpulse")
        == "postgresql://user:***@db:5432/marketpulse"
    )
    assert service.safe_database_url("postgresql://db/marketpulse") == "postgresql://db/marketpulse"


def test_sent_ping_is_not_recorded_as_liveness(tmp_path):
    import aiohttp

    from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
    from hyperdata_terminal.market_pulse.collectors.okx import OkxCollector

    ack = {
        "event": "subscribe",
        "arg": {"channel": "trades-all", "instId": "BTC-USDT-SWAP"},
    }

    def run(messages):
        stop = asyncio.Event()
        ws = _FakeWS(messages, stop)
        order: list[str] = []
        config = CollectorConfig(
            venue="okx",
            markets=("BTC",),
            raw_dir=str(tmp_path),
            ingest_url="http://127.0.0.1:8430",
        )
        archive = RawArchive(tmp_path, "okx")
        try:
            collector = OkxCollector(config, archive, _OrderSink(order), _OrderGaps(order))
            asyncio.run(collector._run_connection(_FakeSession(ws), "epoch-1", stop))
            return order, ws.sent
        finally:
            archive.close()

    # Only the ack arrives: the collector sends a ping but must not claim that
    # the channel is alive.
    order, sent = run([_FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps(ack))])
    assert order == ["connected"]
    assert "ping" in sent

    # A real pong is evidence and is recorded.
    order, _sent = run(
        [
            _FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps(ack)),
            _FakeMessage(aiohttp.WSMsgType.TEXT, "pong"),
        ]
    )
    assert order == ["connected", "heartbeat"]


async def test_window_touching_connection_is_partial():
    store, gaps, windows, _ingest = await make_services()
    connect = MINUTE + 30_000
    await gaps.on_connected("hyperliquid", "BTC", "trades", "epoch-1", connect)
    await _ingest.append_batch(
        [event(trade_id="p1", event_time_ms=connect + 1_000, receive_time_ms=connect + 1_000)],
        now_ms=connect + 1_000,
    )
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 65_000)
    window = await windows.rebuild(
        "hyperliquid",
        "BTC",
        "trades",
        "execution",
        MINUTE,
        now_ms=MINUTE + 65_000,
    )
    assert window.quality == QualityState.PARTIAL.value

    # The next minute was observed from its start and is a full minute.
    await _ingest.append_batch(
        [
            event(
                trade_id="p2",
                event_time_ms=MINUTE + 61_000,
                receive_time_ms=MINUTE + 61_000,
            )
        ],
        now_ms=MINUTE + 61_000,
    )
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 125_000)
    full = await windows.rebuild(
        "hyperliquid",
        "BTC",
        "trades",
        "execution",
        MINUTE + WINDOW_MS,
        now_ms=MINUTE + 125_000,
    )
    assert full.quality == QualityState.OBSERVED_NO_DETECTED_GAP.value


async def test_auto_recovery_closes_covered_open_gap(tmp_path):
    archive = RawArchive(tmp_path, "hyperliquid")
    first = MINUTE + 1_000
    last = MINUTE + 181_000
    for tid, stamp in ((21, first), (22, last), (23, last + 1_000)):
        archive.append(
            {
                "channel": "trades",
                "data": [
                    {"coin": "BTC", "side": "B", "px": "100", "sz": "1", "time": stamp, "tid": tid}
                ],
            },
            receive_time_ms=stamp,
            source_epoch="e1",
        )
    archive.close()

    store, gaps, _windows, ingest = await make_services()
    await store.add_gap(
        Gap(
            gap_id=0,
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            start_ms=first,
            end_ms=None,
            detected_at_ms=first,
            state=QualityState.GAP_DETECTED.value,
            reason="network",
        )
    )
    await store.set_coverage(
        ChannelCoverage(
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e2",
            state=QualityState.RECOVERING.value,
            updated_at_ms=first,
            last_receive_ms=last,
            connected_since_ms=last + 1_000,
        )
    )
    from hyperdata_terminal.market_pulse.recovery import AutoRawRecovery

    auto = AutoRawRecovery(store, ingest, gaps, raw_root=str(tmp_path), min_attempt_interval_ms=0)
    results = await auto.run_once(now_ms=last + 2_000)
    assert results and results[0]["reconciled_gaps"] == 1
    assert (await store.list_gaps())[0].state == QualityState.RECONCILED.value
    # Minutes fully inside the reconciled gap have no trades, but they are
    # rebuilt as reconciled instead of staying ``recovering`` forever.
    middle = await store.get_window(
        "hyperliquid", "BTC", "trades", "execution", MINUTE + WINDOW_MS
    )
    assert middle is not None
    assert middle.quality == QualityState.RECONCILED.value


async def test_acceptance_known_gap_is_never_called_quiet():
    from hyperdata_terminal.market_pulse.acceptance import build_acceptance_report

    store, gaps, windows, ingest = await make_services()
    await ingest.append_batch(
        [event(trade_id="g0", event_time_ms=MINUTE + 1_000)], now_ms=MINUTE + 1_000
    )
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 65_000)
    await ingest.close_due(now_ms=MINUTE + 66_000)
    # A known gap with no window row must still poison the minute.
    await store.add_gap(
        Gap(
            gap_id=0,
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            start_ms=MINUTE + WINDOW_MS,
            end_ms=MINUTE + 2 * WINDOW_MS,
            detected_at_ms=MINUTE + WINDOW_MS,
            state=QualityState.GAP_DETECTED.value,
            reason="network",
        )
    )
    report = await build_acceptance_report(
        store,
        start_ms=MINUTE,
        end_ms=MINUTE + 2 * WINDOW_MS,
        required_pct=99.0,
    )
    row = report["channels"][0]
    assert row["quiet_minutes"] == 0
    assert row["unusable_windows"] == 1
    assert row["usable_pct"] == 50.0
    assert row["passed"] is False


async def test_conflict_marks_both_minutes_unusable():
    store, _gaps, _windows, ingest = await make_services()
    await ingest.append_batch(
        [event(trade_id="c1", event_time_ms=MINUTE + 1_000)], now_ms=MINUTE + 2_000
    )
    result = await ingest.append_batch(
        [event(trade_id="c1", event_time_ms=MINUTE + 61_000, price="101")],
        now_ms=MINUTE + 62_000,
    )
    assert result["conflicts"] == 1
    for start in (MINUTE, MINUTE + WINDOW_MS):
        window = await store.get_window("hyperliquid", "BTC", "trades", "execution", start)
        assert window is not None
        assert window.quality == QualityState.GAP_DETECTED.value
    conflicts = [
        gap for gap in await store.list_gaps() if "conflict" in (gap.reason or "")
    ]
    assert {gap.start_ms for gap in conflicts} == {MINUTE, MINUTE + WINDOW_MS}


async def test_spec_reregistration_keeps_historical_availability():
    store, _gaps, _windows, _ingest = await make_services()
    base = spec_for("okx", "BTC-USDT-SWAP")
    early = replace(base, spec_version="same-v1", valid_from_ms=MINUTE)
    later = replace(base, spec_version="same-v1", valid_from_ms=MINUTE + 100_000)
    await store.register_instruments([early])
    await store.register_instruments([later])
    found = await store.get_instrument_spec("okx", "BTC-USDT-SWAP", MINUTE + 500)
    assert found is not None
    assert found.valid_from_ms == MINUTE


async def test_windows_api_pagination_does_not_split_a_minute():
    store, _gaps, _windows, _ingest = await make_services()
    channels = [
        ("hyperliquid", "BTC", "trades", "execution"),
        ("hyperliquid", "XRP", "trades", "execution"),
        ("okx", "BTC-USDT-SWAP", "trades-all", "execution"),
        ("okx", "XRP-USDT-SWAP", "trades-all", "execution"),
    ]
    expected = set()
    for index in range(8):
        start = MINUTE + index * WINDOW_MS
        for venue, market, channel, kind in channels:
            await store.upsert_window(
                WindowAggregate(
                    venue=venue,
                    native_market_id=market,
                    channel=channel,
                    aggregation_kind=kind,
                    window_start_ms=start,
                    window_end_ms=start + WINDOW_MS,
                    version=1,
                    quality=QualityState.OBSERVED_NO_DETECTED_GAP.value,
                    event_count=1,
                    execution_count=1,
                    buy_base=Decimal("1"),
                    sell_base=None,
                    buy_quote=Decimal("1"),
                    sell_quote=None,
                    open=Decimal("100"),
                    high=Decimal("100"),
                    low=Decimal("100"),
                    close=Decimal("100"),
                    first_event_ms=start + 1_000,
                    last_event_ms=start + 1_000,
                    provisional=False,
                )
            )
            expected.add((start, venue, market, channel))

    app = create_app(store)
    seen = set()
    cursor = MINUTE
    async with TestClient(TestServer(app)) as client:
        for _ in range(20):
            response = await client.get(
                "/windows", params={"from_ms": cursor, "limit": 10}
            )
            body = await response.json()
            for row in body["windows"]:
                seen.add(
                    (
                        row["window_start_ms"],
                        row["venue"],
                        row["native_market_id"],
                        row["channel"],
                    )
                )
            if body["next_from_ms"] is None:
                break
            cursor = body["next_from_ms"]
    assert len(expected) == 32
    assert seen == expected


async def test_stale_channel_degrades_health_and_is_flagged():
    store, gaps, _windows, ingest = await make_services()
    now = int(time.time() * 1_000)
    await ingest.append_batch(
        [event(trade_id="s1", event_time_ms=now - 1_000)], now_ms=now - 1_000
    )
    await store.set_coverage(
        ChannelCoverage(
            venue="hyperliquid",
            native_market_id="BTC",
            channel="trades",
            source_epoch="e1",
            state=QualityState.OBSERVED_NO_DETECTED_GAP.value,
            updated_at_ms=now - 300_000,
            last_event_ms=now - 300_000,
            last_receive_ms=now - 300_000,
            heartbeat_ms=now - 300_000,
        )
    )
    app = create_app(store)
    async with TestClient(TestServer(app)) as client:
        health = await client.get("/health")
        assert health.status == 503
        body = await health.json()
        assert body["status"] == "degraded"
        assert body["stale_channels"]
        coverage = await client.get("/coverage")
        payload = await coverage.json()
        assert payload["channels"][0]["stale"] is True
        assert payload["channels"][0]["silence_ms"] > 120_000


async def test_acceptance_rejects_one_minute_for_a_24h_window():
    from hyperdata_terminal.market_pulse.acceptance import build_acceptance_report

    store, gaps, _windows, ingest = await make_services()
    await ingest.append_batch(
        [event(trade_id="one", event_time_ms=MINUTE + 1_000)], now_ms=MINUTE + 1_000
    )
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 65_000)
    await ingest.close_due(now_ms=MINUTE + 66_000)
    report = await build_acceptance_report(
        store,
        start_ms=MINUTE,
        end_ms=MINUTE + 24 * 60 * 60 * 1_000,
        required_pct=99.0,
    )
    row = report["channels"][0]
    assert row["effective_minutes"] == 1
    assert row["expected_minutes"] == 1_440
    assert row["usable_pct"] == 100.0
    assert row["coverage_span_pct"] < 1.0
    assert row["passed"] is False
    assert report["passed"] is False


@pytest.mark.parametrize("connect_offset", [30_000, 60_000, 120_000])
async def test_snapshot_before_observation_is_never_a_complete_minute(connect_offset):
    store, gaps, windows, ingest = await make_services()
    connected = MINUTE + connect_offset
    await gaps.on_connected("hyperliquid", "BTC", "trades", "epoch-1", connected)
    await ingest.append_batch(
        [event(event_time_ms=MINUTE + 1_000, receive_time_ms=connected + 1_000)],
        now_ms=connected + 1_000,
    )
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 180_000)
    window = await windows.rebuild(
        "hyperliquid", "BTC", "trades", "execution", MINUTE, now_ms=MINUTE + 185_000,
    )
    assert window.quality == "partial"


@pytest.mark.parametrize(
    "frames, should_reconcile",
    [([], False), ([(1_000, "e1")], False),
     ([(1_000, "e1"), (2_000, "e1")], False),
     ([(1_000, "e1"), (60_000, "e2")], False),
     ([(1_000, "e1"), (60_000, "e1")], True),
     ([(1_000, "e1"), (61_000, "e1")], True)],
)
async def test_raw_recovery_requires_target_boundary_and_one_epoch(tmp_path, frames, should_reconcile):
    from hyperdata_terminal.market_pulse.recovery import AutoRawRecovery

    archive = RawArchive(tmp_path, "hyperliquid")
    for tid, (offset, epoch) in enumerate(frames):
        stamp = MINUTE + offset
        archive.append(
            {"channel": "trades", "data": [
                {"coin": "BTC", "side": "B", "px": "100", "sz": "1", "time": stamp, "tid": tid}
            ]}, receive_time_ms=stamp, source_epoch=epoch,
        )
    archive.close()
    store, gaps, _windows, ingest = await make_services()
    await store.add_gap(Gap(
        gap_id=0, venue="hyperliquid", native_market_id="BTC", channel="trades", source_epoch="e1",
        start_ms=MINUTE + 1_000, end_ms=None, detected_at_ms=MINUTE + 2_000,
        state="gap_detected", reason="ingest interrupted",
    ))
    await store.set_coverage(ChannelCoverage(
        venue="hyperliquid", native_market_id="BTC", channel="trades", source_epoch="e2",
        state="recovering", updated_at_ms=MINUTE + 60_000, connected_since_ms=MINUTE + 60_000,
    ))
    result = (await AutoRawRecovery(
        store, ingest, gaps, raw_root=str(tmp_path), min_attempt_interval_ms=0,
    ).run_once(now_ms=MINUTE + 65_000))[0]
    gap = (await store.list_gaps())[0]
    assert result["reconciled_gaps"] == int(should_reconcile)
    assert result["remaining_gaps"] == int(not should_reconcile)
    assert result["status"] == ("ok" if should_reconcile else "partial")
    assert gap.end_ms == (MINUTE + 60_000 if should_reconcile else None)
    assert gap.state == ("reconciled" if should_reconcile else "gap_detected")
    # The boundary frame proves coverage but belongs outside the replay range.
    stored = await store.list_events_after(0, limit=100)
    assert all(row.event.receive_time_ms < MINUTE + 60_000 for row in stored)


async def test_reconciled_channel_does_not_bless_unobserved_future_window():
    store, _gaps, windows, _ingest = await make_services()
    await store.set_coverage(ChannelCoverage(
        venue="hyperliquid", native_market_id="BTC", channel="trades", source_epoch="e1",
        state="reconciled", updated_at_ms=MINUTE, connected_since_ms=MINUTE, heartbeat_ms=MINUTE,
    ))
    window = await windows.rebuild(
        "hyperliquid", "BTC", "trades", "execution", MINUTE + 3_600_000, now_ms=MINUTE + 3_665_000,
    )
    assert window.quality == "partial"
    assert window.buy_base is None


@pytest.mark.parametrize("covered_end, expected", [(30_000, "partial"), (60_000, "reconciled")])
async def test_reconciled_interval_must_cover_whole_minute(covered_end, expected):
    store, _gaps, windows, _ingest = await make_services()
    await store.set_coverage(ChannelCoverage(
        venue="hyperliquid", native_market_id="BTC", channel="trades", source_epoch="e1",
        state="reconciled", updated_at_ms=MINUTE + 120_000, connected_since_ms=MINUTE + 120_000,
        heartbeat_ms=MINUTE + 120_000,
    ))
    await store.add_gap(Gap(
        gap_id=0, venue="hyperliquid", native_market_id="BTC", channel="trades", source_epoch="e1",
        start_ms=MINUTE, end_ms=MINUTE + covered_end, detected_at_ms=MINUTE, state="reconciled",
    ))
    window = await windows.rebuild(
        "hyperliquid", "BTC", "trades", "execution", MINUTE, now_ms=MINUTE + 125_000,
    )
    assert window.quality == expected
    if expected == "reconciled":
        provisional = await windows.rebuild(
            "hyperliquid", "BTC", "trades", "execution", MINUTE, now_ms=MINUTE + 61_000,
        )
        assert provisional.provisional
        assert provisional.quality == "warming_up"


async def test_reconciling_one_gap_preserves_other_open_gap():
    store, gaps, windows, _ingest = await make_services()
    first, _second = [await store.add_gap(Gap(
        gap_id=0, venue="hyperliquid", native_market_id="BTC", channel="trades", source_epoch="e1",
        start_ms=MINUTE + offset, end_ms=None, detected_at_ms=MINUTE + offset, state="gap_detected",
    )) for offset in (0, 120_000)]
    await gaps.mark_reconciled(first.gap_id, end_ms=MINUTE + 60_000, method="raw", now_ms=MINUTE + 180_000)
    coverage = await store.get_coverage("hyperliquid", "BTC", "trades")
    assert coverage.state == "recovering"
    recovered = await windows.rebuild(
        "hyperliquid", "BTC", "trades", "execution", MINUTE, now_ms=MINUTE + 185_000,
    )
    assert recovered.quality == "reconciled"
    lost = await windows.rebuild(
        "hyperliquid", "BTC", "trades", "execution", MINUTE + 120_000, now_ms=MINUTE + 185_000,
    )
    assert lost.quality == "gap_detected"


async def test_delayed_heartbeat_revisits_partial_window():
    store, gaps, _windows, ingest = await make_services()
    await ingest.append_batch([event()], now_ms=MINUTE + 1_000)
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 59_000)
    await ingest.close_due(now_ms=MINUTE + 65_001)
    before = await store.get_window("hyperliquid", "BTC", "trades", "execution", MINUTE)
    assert before.quality == "partial" and not before.provisional
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 75_000)
    await ingest.close_due(now_ms=MINUTE + 80_000)
    after = await store.get_window("hyperliquid", "BTC", "trades", "execution", MINUTE)
    assert after.quality == "observed_no_detected_gap"
    assert after.event_count == before.event_count
    assert after.revised_at_ms == MINUTE + 80_000


async def test_heartbeat_deadline_enters_disconnect_path(tmp_path, monkeypatch):
    import aiohttp

    from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
    from hyperdata_terminal.market_pulse.collectors.okx import OkxCollector

    stop = asyncio.Event()
    store, tracker, _windows, _ingest = await make_services()

    class Reporter:
        async def connected(self, *args):
            await tracker.on_connected(*args)

        async def disconnected(self, *args, **kwargs):
            await tracker.on_disconnected(*args, **kwargs)
            stop.set()

    class SilentWS(_FakeWS):
        async def receive(self, timeout=None):
            if self.messages:
                return self.messages.pop(0)
            raise asyncio.TimeoutError

    class Session(_FakeSession):
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    ws = SilentWS([_FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps({
        "event": "subscribe", "arg": {"channel": "trades-all", "instId": "BTC-USDT-SWAP"},
    }))], stop)
    monkeypatch.setattr(aiohttp, "ClientSession", lambda **kwargs: Session(ws))
    archive = RawArchive(tmp_path, "okx")
    try:
        collector = OkxCollector(CollectorConfig(
            venue="okx", markets=("BTC",), raw_dir=str(tmp_path), ingest_url="http://unused",
            base_backoff_ms=0,
        ), archive, _NullSink(), Reporter())
        await asyncio.wait_for(collector.run(stop), timeout=2)
    finally:
        archive.close()
    gaps = await store.list_gaps(venue="okx")
    assert len(gaps) == 1
    assert "heartbeat response deadline" in gaps[0].reason
    assert gaps[0].state == "gap_detected"
    assert ws.sent.count("ping") == 1


async def test_pending_minutes_cannot_pass_acceptance():
    from hyperdata_terminal.market_pulse.acceptance import build_acceptance_report

    store, gaps, _windows, ingest = await make_services()
    await ingest.append_batch([event()], now_ms=MINUTE + 1_000)
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 65_000)
    await ingest.close_due(now_ms=MINUTE + 66_000)
    for minute in (1, 2, 3):
        await ingest.append_batch([event(
            trade_id=f"later-{minute}", event_time_ms=MINUTE + minute * WINDOW_MS + 1_000,
        )], now_ms=MINUTE + minute * WINDOW_MS + 1_000)
    report = await build_acceptance_report(
        store, start_ms=MINUTE, end_ms=MINUTE + 4 * WINDOW_MS,
        expected_channels=(("hyperliquid", "BTC", "trades"),),
    )
    row = report["channels"][0]
    assert row["closed_windows"] == 1
    assert row["provisional_windows"] == 3
    assert row["usable_pct"] == 25
    assert not row["passed"] and not report["passed"]


async def test_acceptance_requires_channels_or_an_explicit_subset():
    from hyperdata_terminal.market_pulse.acceptance import build_acceptance_report

    store, gaps, _windows, ingest = await make_services()
    await ingest.append_batch([event()], now_ms=MINUTE + 1_000)
    await gaps.on_heartbeat("hyperliquid", "BTC", "trades", MINUTE + 65_000)
    await ingest.close_due(now_ms=MINUTE + 66_000)
    report = await build_acceptance_report(store, start_ms=MINUTE, end_ms=MINUTE + WINDOW_MS)
    assert len(report["channels"]) == 4
    assert len(report["missing_channels"]) == 3
    assert not report["passed"]
    subset = await build_acceptance_report(
        store, start_ms=MINUTE, end_ms=MINUTE + WINDOW_MS,
        expected_channels=(("hyperliquid", "BTC", "trades"),),
    )
    assert subset["passed"]
    assert subset["expected_channels"] == [["hyperliquid", "BTC", "trades"]]


async def test_acceptance_passes_all_four_observed_channels():
    from hyperdata_terminal.market_pulse.acceptance import build_acceptance_report, required_channels

    store, gaps, _windows, ingest = await make_services()
    for venue, market, channel in required_channels():
        await gaps.on_connected(venue, market, channel, "epoch-1", MINUTE)
        await ingest.append_batch([event(venue=venue, market=market)], now_ms=MINUTE + 1_000)
        await gaps.on_heartbeat(venue, market, channel, MINUTE + 65_000)
    await ingest.close_due(now_ms=MINUTE + 66_000)
    report = await build_acceptance_report(store, start_ms=MINUTE, end_ms=MINUTE + WINDOW_MS)
    assert report["passed"]
    assert len(report["channels"]) == 4
    assert report["missing_channels"] == []


async def test_acceptance_uses_configured_markets(monkeypatch):
    from hyperdata_terminal.market_pulse import service

    store, _gaps, _windows, _ingest = await make_services()

    async def connect(*args, **kwargs):
        return store

    monkeypatch.setenv("PULSE_MARKETS", "BTC")
    monkeypatch.setattr(service.PostgresStore, "connect", connect)
    result = json.loads(await service.acceptance(start_ms=MINUTE, end_ms=MINUTE + WINDOW_MS))
    assert result["expected_channels"] == [
        ["hyperliquid", "BTC", "trades"], ["okx", "BTC-USDT-SWAP", "trades-all"],
    ]
    assert not result["passed"]


async def test_no_coverage_does_not_create_zero_activity():
    store = InMemoryMarketPulseStore()
    window = await WindowService(store).rebuild(
        "hyperliquid", "BTC", "trades", "execution", MINUTE, now_ms=MINUTE + 65_001,
    )
    assert window.quality == "unavailable"
    assert window.buy_base is None


async def test_real_response_resets_heartbeat_deadline(tmp_path):
    import aiohttp

    from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
    from hyperdata_terminal.market_pulse.collectors.okx import OkxCollector

    stop = asyncio.Event()

    class WS(_FakeWS):
        async def receive(self, timeout=None):
            message = self.messages.pop(0)
            if message is None:
                if not self.messages:
                    stop.set()
                raise asyncio.TimeoutError
            return message

    ws = WS([
        _FakeMessage(aiohttp.WSMsgType.TEXT, json.dumps({"event": "subscribe"})),
        None, _FakeMessage(aiohttp.WSMsgType.TEXT, "pong"), None,
    ], stop)
    archive = RawArchive(tmp_path, "okx")
    try:
        collector = OkxCollector(CollectorConfig(
            venue="okx", markets=("BTC",), raw_dir=str(tmp_path), ingest_url="http://unused",
        ), archive, _NullSink())
        await collector._run_connection(_FakeSession(ws), "e1", stop)
    finally:
        archive.close()
    assert ws.sent.count("ping") == 2


@pytest.mark.parametrize("kwargs", [
    {"expected_channels": ()}, {"required_pct": 0}, {"required_pct": 101},
])
async def test_acceptance_rejects_bypass_configuration(kwargs):
    from hyperdata_terminal.market_pulse.acceptance import build_acceptance_report

    store, _gaps, _windows, _ingest = await make_services()
    with pytest.raises(ValueError):
        await build_acceptance_report(store, start_ms=MINUTE, end_ms=MINUTE + WINDOW_MS, **kwargs)

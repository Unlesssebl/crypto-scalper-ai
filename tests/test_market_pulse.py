"""P0-P2 Market Pulse contract, ingestion, window, recovery and API tests."""
from __future__ import annotations

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
    ContractError,
    DeliveryKind,
    ExecutionEvent,
    Gap,
    InstrumentSpec,
    QualityState,
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
                    "count": "3",
                }
            ],
        },
        spec_for("okx", "BTC-USDT-SWAP"),
        receive_time_ms=1_700_000_000_200,
        source_epoch="e1",
        raw_ref="seg:2",
    )
    assert okx[0].execution_count == 3
    assert okx[0].base_amount == Decimal("0.03")
    assert okx[0].quote_amount == Decimal("1260.00")


async def test_okx_aggregated_channel_is_never_mixed_with_execution():
    store, _gaps, _windows, ingest = await make_services()
    spec = spec_for("okx", "BTC-USDT-SWAP")
    event_time = MINUTE + 1_000

    def payload(channel: str) -> dict:
        return {
            "arg": {"channel": channel, "instId": "BTC-USDT-SWAP"},
            "data": [
                {
                    "instId": "BTC-USDT-SWAP",
                    "tradeId": "77",
                    "px": "42000",
                    "sz": "3",
                    "side": "sell",
                    "ts": str(event_time),
                    "count": "3",
                }
            ],
        }

    execution = normalize_okx_trades(
        payload("trades-all"), spec, receive_time_ms=event_time, source_epoch="e1", raw_ref="a:1"
    )[0]
    aggregated = normalize_okx_trades(
        payload("trades"), spec, receive_time_ms=event_time, source_epoch="e1", raw_ref="a:2"
    )[0]
    assert execution.aggregation_kind == "execution"
    assert aggregated.aggregation_kind == "aggregated"
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
    store, _gaps, windows, ingest = await make_services()
    result = await ingest.append_batch([event()], now_ms=MINUTE + 1_000)
    window = result["windows"][0]
    assert window["provisional"] is True
    assert window["quality"] == QualityState.WARMING_UP.value

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


async def test_read_only_api_cursor_and_versions():
    store, _gaps, _windows, ingest = await make_services()
    await ingest.append_batch([event(), event(trade_id="t2", event_time_ms=MINUTE + 2_000)], now_ms=MINUTE + 2_000)
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

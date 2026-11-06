"""Isolated PostgreSQL regression check; run with PULSE_TEST_DATABASE_URL set.

Requires asyncpg and aiohttp. Creates a unique schema and drops it in finally.
It never reads or changes an existing application's tables.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
import uuid
from dataclasses import replace
from decimal import Decimal

import asyncpg
from aiohttp.test_utils import TestClient, TestServer

from hyperdata_terminal.market_pulse.acceptance import build_acceptance_report
from hyperdata_terminal.market_pulse.api import create_app
from hyperdata_terminal.market_pulse.collectors.specs import default_instruments
from hyperdata_terminal.market_pulse.contracts import ExecutionEvent, Gap
from hyperdata_terminal.market_pulse.gaps import GapTracker
from hyperdata_terminal.market_pulse.ingest import IngestionService
from hyperdata_terminal.market_pulse.postgres import PostgresStore
from hyperdata_terminal.market_pulse.raw_archive import RawArchive
from hyperdata_terminal.market_pulse.recovery import AutoRawRecovery
from hyperdata_terminal.market_pulse.storage import ChannelCoverage
from hyperdata_terminal.market_pulse.windows import WindowService

MINUTE = 1_700_000_040_000


def execution(market: str, stamp: int, trade_id: str = "one") -> ExecutionEvent:
    spec = next(s for s in default_instruments() if s.venue == "hyperliquid" and s.native_market_id == market)
    return ExecutionEvent(
        venue="hyperliquid", native_market_id=market, channel="trades", aggregation_kind="execution",
        trade_id=trade_id, event_time_ms=stamp, receive_time_ms=stamp, source_epoch="e1",
        side="buy", price=Decimal("100"), native_amount=Decimal("2"), base_amount=Decimal("2"),
        quote_amount=Decimal("200"), instrument_spec_version=spec.spec_version,
    )


async def verify(store: PostgresStore) -> dict:
    await store.register_instruments(default_instruments())
    gaps = GapTracker(store)
    windows = WindowService(store)
    ingest = IngestionService(store, windows, gaps)
    results = {}
    from hyperdata_terminal.market_pulse.overview import VERSION
    summary = dict(schema_version=VERSION, symbol="NEW", start_ms=MINUTE,
                   received_at_ms=MINUTE+59_000, epoch_started_ms=MINUTE-60_000,
                   trade_count=7, base_volume="10", close="100", source_epoch="summary", raw_ref="raw:1")
    await store.put_overview([summary], MINUTE+60_000)
    await store.put_overview([{**summary, "received_at_ms": MINUTE+1_000, "trade_count": 1}], MINUTE+60_000)
    assert (await store.get_overview(MINUTE))[0]["trade_count"] == 7
    assert await store.get_overview(MINUTE+60_000) == []
    results["broad_summary_durable_monotonic"] = True

    await gaps.on_connected("hyperliquid", "BTC", "trades", "e1", MINUTE + 120_000)
    old = replace(execution("BTC", MINUTE + 1_000), receive_time_ms=MINUTE + 121_000)
    await ingest.append_batch([old], now_ms=MINUTE + 125_000)
    startup = await store.get_window("hyperliquid", "BTC", "trades", "execution", MINUTE)
    assert startup.quality == "partial"
    results["historical_startup"] = startup.quality
    await ingest.append_batch([replace(old, event_time_ms=MINUTE + 121_000)], now_ms=MINUTE + 185_000)
    qualities = [(await store.get_window("hyperliquid", "BTC", "trades", "execution", stamp)).quality
                 for stamp in (MINUTE, MINUTE + 120_000)]
    assert qualities == ["gap_detected", "gap_detected"]
    results["cross_minute_conflict"] = qualities
    spec = next(s for s in default_instruments() if s.venue == "okx")
    await store.register_instruments([replace(spec, valid_from_ms=MINUTE)])
    assert await store.get_instrument_spec(spec.venue, spec.native_market_id, MINUTE - 1) is not None
    results["historical_spec_preserved"] = True

    raw_gap = await store.add_gap(Gap(
        gap_id=0, venue="hyperliquid", native_market_id="XRP", channel="trades", source_epoch="e1",
        start_ms=MINUTE + 1_000, end_ms=None, detected_at_ms=MINUTE + 2_000,
        state="gap_detected", reason="ingest interrupted",
    ))
    await store.set_coverage(ChannelCoverage(
        venue="hyperliquid", native_market_id="XRP", channel="trades", source_epoch="e2",
        state="recovering", updated_at_ms=MINUTE + 60_000, connected_since_ms=MINUTE + 60_000,
    ))
    with tempfile.TemporaryDirectory() as root:
        archive = RawArchive(root, "hyperliquid")
        for offset in (1_000, 2_000):
            stamp = MINUTE + offset
            archive.append({"channel": "trades", "data": [
                {"coin": "XRP", "side": "B", "px": "100", "sz": "2", "time": stamp, "tid": offset}
            ]}, receive_time_ms=stamp, source_epoch="e1")
        archive.close()
        result = await AutoRawRecovery(store, ingest, gaps, raw_root=root, min_attempt_interval_ms=0).run_once(
            now_ms=MINUTE + 65_000,
        )
        assert result and all(item["reconciled_gaps"] == 0 for item in result)
    raw = next(g for g in await store.list_gaps() if g.gap_id == raw_gap.gap_id)
    assert raw.state == "gap_detected" and raw.end_ms is None
    results["truncated_raw_retains_tail"] = True
    await gaps.on_connected("hyperliquid", "XRP", "trades", "e3", MINUTE + 120_000)
    await gaps.on_heartbeat("hyperliquid", "XRP", "trades", MINUTE + 185_000)
    future = await windows.rebuild(
        "hyperliquid", "XRP", "trades", "execution", MINUTE + 120_000, now_ms=MINUTE + 185_001,
    )
    assert future.quality == "observed_no_detected_gap"
    bounded = next(g for g in await store.list_gaps() if g.gap_id == raw_gap.gap_id)
    assert bounded.state == "gap_detected" and bounded.end_ms == MINUTE + 120_000
    results["old_gap_preserved_future_minute_usable"] = True

    # A separate healthy market tests delayed right-boundary confirmation.
    market, channel = "BTC-USDT-SWAP", "trades-all"
    await gaps.on_connected("okx", market, channel, "e1", MINUTE)
    await gaps.on_heartbeat("okx", market, channel, MINUTE + 59_000)
    pending = await windows.rebuild("okx", market, channel, "execution", MINUTE, now_ms=MINUTE + 65_001)
    assert pending.quality == "partial" and not pending.provisional
    await gaps.on_heartbeat("okx", market, channel, MINUTE + 75_000)
    await ingest.close_due(now_ms=MINUTE + 80_000)
    ready = await store.get_window("okx", market, channel, "execution", MINUTE)
    assert ready.quality == "observed_no_detected_gap"
    results["late_heartbeat_revises_window"] = True
    report = await build_acceptance_report(store, start_ms=MINUTE, end_ms=MINUTE + 60_000)
    assert not report["passed"]
    assert ["okx", "XRP-USDT-SWAP", "trades-all"] in report["missing_channels"]
    results["missing_channel_blocks_acceptance"] = True
    async with TestClient(TestServer(create_app(store))) as client:
        health = await client.get("/health")
        assert health.status == 503
        full = (await (await client.get("/windows", params={"limit": 10_000})).json())["windows"]
        seen = []
        cursor = MINUTE
        for _ in range(100):
            response = await client.get("/windows", params={"from_ms": cursor, "limit": 1})
            assert response.status == 200
            page = await response.json()
            seen.extend(page["windows"])
            if page["next_from_ms"] is None:
                break
            cursor = page["next_from_ms"]
        else:
            raise AssertionError("pagination failed to terminate")
        def key(window):
            return tuple(window[name] for name in (
                "venue", "native_market_id", "channel", "aggregation_kind", "window_start_ms",
            ))
        assert len(seen) == len({key(window) for window in seen})
        assert {key(window) for window in seen} == {key(window) for window in full}
        results["http_api_pagination"] = {"full_rows": len(full), "paged_rows": len(seen), "missing": 0}
        results["http_health_rejects_degraded_data"] = True
    test_event = execution("BTC", MINUTE + 1_000, "quarantine-proof")
    await ingest.append_batch([test_event], now_ms=MINUTE + 200_000)
    async with store.pool.acquire() as connection:
        sequence = await connection.fetchval("SELECT event_seq FROM execution_events WHERE trade_id='quarantine-proof'")
    assert await store.quarantine_events([sequence], "raw identity audit failed", MINUTE + 201_000) == 1
    assert await store.quarantine_events([sequence], "raw identity audit failed", MINUTE + 202_000) == 0
    assert all(e.trade_id != 'quarantine-proof' for e in await store.list_events_for_window(
        "hyperliquid", "BTC", "trades", "execution", MINUTE))
    assert all(e.event_seq != sequence for e in await store.list_events_after(sequence - 1))
    window = await store.get_window("hyperliquid", "BTC", "trades", "execution", MINUTE)
    assert window.quality == 'gap_detected'
    async with store.pool.acquire() as connection:
        assert await connection.fetchval("SELECT COUNT(*) FROM execution_events WHERE event_seq=$1", sequence) == 1
    results['quarantine_preserves_evidence_and_blocks_reads_atomically'] = True
    return results


async def main() -> None:
    dsn = os.environ["PULSE_TEST_DATABASE_URL"]
    schema = "pulse_regression_" + uuid.uuid4().hex
    admin = await asyncpg.connect(dsn)
    pool = None
    try:
        await admin.execute("CREATE SCHEMA " + schema)
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, server_settings={"search_path": schema})
        store = PostgresStore(pool)
        await store.initialize()
        print(json.dumps({"isolated_schema": True, "checks": await verify(store)}, indent=2))
    finally:
        if pool is not None:
            await pool.close()
        await admin.execute("DROP SCHEMA " + schema + " CASCADE")
        await admin.close()


if __name__ == "__main__":
    asyncio.run(main())

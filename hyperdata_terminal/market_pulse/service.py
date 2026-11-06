"""Service entry points for Market Pulse (ingest, API, collectors, recovery)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .api import run_api
from .collectors.base import CollectorConfig, StreamCollector
from .collectors.hyperliquid import HyperliquidCollector, normalize_hyperliquid_trades
from .collectors.okx import OkxCollector, OkxInstrumentCatalog, normalize_okx_trades
from .collectors.specs import default_instruments, spec_index
from .gaps import GapTracker
from .ingest import GapControlService, IngestionService
from .ingest_http import run_ingest
from .paths import DATA_DIR
from .postgres import PostgresStore
from .raw_archive import RawArchive
from .recovery import AutoRawRecovery, RawRecoveryService
from .transport import HttpIngestClient
from .windows import WindowService

LOG = logging.getLogger(__name__)
DEFAULT_DATABASE_URL = "postgresql://marketpulse:marketpulse@127.0.0.1:5432/marketpulse"


def database_url() -> str:
    return os.getenv("PULSE_DATABASE_URL", DEFAULT_DATABASE_URL)


def raw_root() -> Path:
    return Path(os.getenv("PULSE_RAW_DIR", str(DATA_DIR / "market-pulse" / "raw")))


def markets() -> tuple[str, ...]:
    raw = os.getenv("PULSE_MARKETS", "BTC,XRP")
    values = tuple(item.strip().upper() for item in raw.split(",") if item.strip())
    return values or ("BTC", "XRP")


def ingest_url() -> str:
    return os.getenv("PULSE_INGEST_URL", "http://127.0.0.1:8430")


def ingest_token() -> str | None:
    return os.getenv("PULSE_INGEST_TOKEN") or None


def host() -> str:
    return os.getenv("PULSE_HOST", "127.0.0.1")


def ingest_port() -> int:
    return int(os.getenv("PULSE_INGEST_PORT", "8430"))


def api_port() -> int:
    return int(os.getenv("PULSE_API_PORT", "8431"))


async def init_db() -> dict[str, Any]:
    store = await PostgresStore.connect(database_url())
    try:
        await store.register_instruments(default_instruments(markets()))
        health = await store.health()
        return {"status": "ok", "database": database_url(), "storage": health}
    finally:
        await store.close()


async def run_ingest_service(stop: asyncio.Event | None = None) -> None:
    stop = stop or asyncio.Event()
    store = await PostgresStore.connect(database_url())
    try:
        await store.register_instruments(default_instruments(markets()))
        gaps = GapTracker(store)
        windows = WindowService(store)
        ingest = IngestionService(store, windows, gaps)
        gap_control = GapControlService(store, gaps)
        auto_recovery = AutoRawRecovery(
            store,
            ingest,
            gaps,
            raw_root=str(raw_root()),
        )
        runner = await run_ingest(
            store,
            ingest,
            gap_control,
            host=host(),
            port=ingest_port(),
            token=ingest_token(),
        )
        LOG.info("ingest listening on %s:%s", host(), ingest_port())
        housekeeping = asyncio.create_task(
            _housekeeping(ingest, auto_recovery, stop),
            name="pulse-housekeeping",
        )
        try:
            await stop.wait()
        finally:
            housekeeping.cancel()
            await asyncio.gather(housekeeping, return_exceptions=True)
            await runner.cleanup()
    finally:
        await store.close()


async def _housekeeping(
    ingest: IngestionService,
    auto_recovery: AutoRawRecovery,
    stop: asyncio.Event,
) -> None:
    last_recovery_ms = 0
    while not stop.is_set():
        try:
            now = _now_ms()
            await ingest.drain_recalc(now_ms=now)
            await ingest.close_due(now_ms=now)
            if now - last_recovery_ms >= 30_000:
                last_recovery_ms = now
                await auto_recovery.run_once(now_ms=now)
        except Exception:
            LOG.exception("Market Pulse housekeeping failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass


async def run_api_service(stop: asyncio.Event | None = None) -> None:
    stop = stop or asyncio.Event()
    store = await PostgresStore.connect(database_url())
    try:
        runner = await run_api(store, host=host(), port=api_port())
        LOG.info("read-only API listening on %s:%s", host(), api_port())
        try:
            await stop.wait()
        finally:
            await runner.cleanup()
    finally:
        await store.close()


async def run_collector(
    venue: str,
    *,
    markets_override: tuple[str, ...] | None = None,
    stop: asyncio.Event | None = None,
    refresh_okx: bool = True,
) -> None:
    stop = stop or asyncio.Event()
    selected = markets_override or markets()
    config = CollectorConfig(
        venue=venue,
        markets=selected,
        raw_dir=str(raw_root()),
        ingest_url=ingest_url(),
        token=ingest_token(),
    )
    archive = RawArchive(
        raw_root(),
        venue,
        max_bytes=int(os.getenv("PULSE_RAW_MAX_BYTES", str(5 * 1024**3))),
        max_age_ms=int(os.getenv("PULSE_RAW_MAX_AGE_MS", str(48 * 60 * 60 * 1_000))),
        fsync=os.getenv("PULSE_RAW_FSYNC", "0") == "1",
    )
    client = HttpIngestClient(ingest_url(), token=ingest_token())
    specs = [spec for spec in default_instruments(selected) if spec.venue == venue]
    if venue == "okx" and refresh_okx:
        try:
            specs = await OkxInstrumentCatalog().fetch_many(selected)
            LOG.info("refreshed OKX instrument specs: %s", [spec.spec_version for spec in specs])
        except Exception:
            LOG.exception("OKX instrument refresh failed; using checked defaults")
    try:
        await client.register_instruments(specs)
    except Exception:
        LOG.exception("Could not register instrument specs before collection")
    collector: StreamCollector
    if venue == "hyperliquid":
        collector = HyperliquidCollector(config, archive, client, client, specs=specs)
    elif venue == "okx":
        collector = OkxCollector(config, archive, client, client, specs=specs)
    else:
        raise ValueError(f"unsupported venue {venue!r}")
    try:
        await collector.run(stop)
    finally:
        archive.close()
        try:
            await client.register_raw_segments(archive.segments())
        except Exception:
            LOG.exception("Could not register raw segments")
        await client.close()


async def recover(
    venue: str,
    market: str,
    *,
    start_ms: int,
    end_ms: int,
) -> dict[str, Any]:
    store = await PostgresStore.connect(database_url())
    try:
        specs = spec_index([spec for spec in default_instruments((market.upper(),)) if spec.venue == venue])
        await store.register_instruments(specs.values())
        gaps = GapTracker(store)
        windows = WindowService(store)
        ingest = IngestionService(store, windows, gaps)
        service = RawRecoveryService(store, ingest, gaps)
        archive = RawArchive(raw_root(), venue, read_only=True)
        spec = await store.get_instrument_spec(venue, _market_id(venue, market.upper()), start_ms)
        if spec is None:
            spec = specs[(venue, _market_id(venue, market.upper()))]
        if venue == "okx":
            normalizer = lambda payload, **kwargs: normalize_okx_trades(payload, spec, **kwargs)  # noqa: E731
        else:
            normalizer = lambda payload, **kwargs: normalize_hyperliquid_trades(payload, spec, **kwargs)  # noqa: E731
        result = await service.recover_from_raw(
            archive,
            venue=venue,
            market=spec.native_market_id,
            channel="trades-all" if venue == "okx" else "trades",
            start_ms=start_ms,
            end_ms=end_ms,
            normalize=normalizer,
        )
        return result
    finally:
        await store.close()


def _market_id(venue: str, market: str) -> str:
    from .collectors.specs import market_for_venue

    return market_for_venue(venue, market)


async def status() -> dict[str, Any]:
    store = await PostgresStore.connect(database_url())
    try:
        health = await store.health()
        coverage = [item.to_payload() for item in await store.list_coverage()]
        gaps = [gap.to_payload() for gap in await store.list_gaps(state="gap_detected", limit=100)]
        return {"storage": health, "channels": coverage, "open_gaps": gaps}
    finally:
        await store.close()


def parse_boundary(value: str | None, *, default_ms: int) -> int:
    if value is None:
        return default_ms
    text = value.strip()
    if text.isdigit():
        number = int(text)
        return number * 1_000 if len(text) == 10 else number
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1_000)


def _now_ms() -> int:
    import time

    return int(time.time() * 1_000)


def dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)

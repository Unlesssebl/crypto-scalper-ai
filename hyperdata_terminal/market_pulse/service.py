"""Service entry points for Market Pulse (ingest, API, collectors, recovery)."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from ..paths import DATA_DIR
from .acceptance import build_acceptance_report, render_markdown, required_channels
from .api import run_api
from .collectors.attention import follow_attention
from .collectors.base import CollectorConfig, StreamCollector
from .collectors.binance import BinanceCollector
from .collectors.hyperliquid import HyperliquidCollector, normalize_hyperliquid_trades
from .collectors.okx import OkxCollector, OkxInstrumentCatalog, normalize_okx_trades
from .collectors.specs import default_instruments, spec_index
from .contracts import WINDOW_MS, QualityState, known_channel
from .gaps import GapTracker
from .ingest import GapControlService, IngestionService
from .ingest_http import run_ingest
from .maintenance import prune_raw
from .postgres import PostgresStore
from .raw_archive import RawArchive
from .recovery import AutoRawRecovery, RawRecoveryService
from .transport import HttpIngestClient
from .windows import WindowService

LOG = logging.getLogger(__name__)
DEFAULT_DATABASE_URL = "postgresql://marketpulse:marketpulse@127.0.0.1:5432/marketpulse"


def stop_event(provided=None):
    if provided is not None:
        return provided
    stop = asyncio.Event()
    try:
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, stop.set)
    except (NotImplementedError, RuntimeError):
        pass
    return stop


def database_url() -> str:
    return os.getenv("PULSE_DATABASE_URL", DEFAULT_DATABASE_URL)


def safe_database_url(value: str) -> str:
    """Redact the password so status output can be pasted into a ticket."""
    parts = urlsplit(value)
    if not parts.password:
        return value
    netloc = f"{parts.username or ''}:***@{parts.hostname or ''}"
    if parts.port:
        netloc += f":{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def raw_root() -> Path:
    return Path(os.getenv("PULSE_RAW_DIR", str(DATA_DIR / "market-pulse" / "raw")))


def markets() -> tuple[str, ...]:
    if os.getenv("PULSE_DYNAMIC_ATTENTION", "0") == "1":
        return ()
    raw = os.getenv("PULSE_MARKETS", "")
    values = tuple(item.strip().upper() for item in raw.split(",") if item.strip())
    return values


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
        return {"status": "ok", "database": safe_database_url(database_url()), "storage": health}
    finally:
        await store.close()


async def run_ingest_service(stop: asyncio.Event | None = None) -> None:
    stop = stop_event(stop)
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
        recovery_task = asyncio.create_task(_recover_periodically(auto_recovery, stop), name="pulse-recovery")
        try:
            await stop.wait()
        finally:
            housekeeping.cancel()
            recovery_task.cancel()
            await asyncio.gather(housekeeping, recovery_task, return_exceptions=True)
            await runner.cleanup()
    finally:
        await store.close()


async def _housekeeping(
    ingest: IngestionService,
    auto_recovery: AutoRawRecovery,
    stop: asyncio.Event,
) -> None:
    last_maintenance_ms = 0
    while not stop.is_set():
        try:
            now = _now_ms()
            await ingest.drain_recalc(now_ms=now)
            await ingest.close_due(now_ms=now)
            if now - last_maintenance_ms >= 60_000:
                last_maintenance_ms = now
                storage_gaps = await ingest.store.list_gaps(limit=100_000)
                maintenance = await asyncio.to_thread(prune_raw, raw_root(), storage_gaps, now,
                                                     max_bytes=int(os.getenv("PULSE_RAW_BUDGET_BYTES", 5 * 1024**3)))
                maintenance.update(await ingest.store.resources())
                maintenance["database_budget_bytes"] = int(os.getenv("PULSE_DB_BUDGET_BYTES", 10 * 1024**3))
                if maintenance["database_bytes"] >= maintenance["database_budget_bytes"]:
                    maintenance.update(status="blocked", reason="database_budget_exhausted")
                await ingest.store.set_control("maintenance", maintenance, now)
        except Exception:
            LOG.exception("Market Pulse housekeeping failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass


async def _recover_periodically(recovery, stop):
    while not stop.is_set():
        try:
            await recovery.run_once(now_ms=_now_ms())
        except Exception:
            LOG.exception("Background recovery failed; live housekeeping continues")
        try:
            await asyncio.wait_for(stop.wait(), timeout=30)
        except asyncio.TimeoutError:
            pass


async def run_api_service(stop: asyncio.Event | None = None) -> None:
    stop = stop_event(stop)
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
    stop = stop_event(stop)
    dynamic = os.getenv("PULSE_DYNAMIC_ATTENTION", "0") == "1"
    selected = () if dynamic else markets_override or markets()
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
    if venue == "okx" and refresh_okx and selected:
        try:
            specs = await OkxInstrumentCatalog().fetch_many(selected)
            LOG.info("refreshed OKX instrument specs: %s", [spec.spec_version for spec in specs])
        except Exception:
            LOG.exception("OKX instrument refresh failed; using checked defaults")
    try:
        if specs:
            await client.register_instruments(specs)
    except Exception:
        LOG.exception("Could not register instrument specs before collection")
    collector: StreamCollector
    if venue == "hyperliquid":
        collector = HyperliquidCollector(config, archive, client, client, specs=specs)
    elif venue == "okx":
        collector = OkxCollector(config, archive, client, client, specs=specs)
    elif venue == "binance":
        collector = BinanceCollector(config, archive, client, client)
    else:
        raise ValueError(f"unsupported venue {venue!r}")
    try:
        controller = None
        if dynamic:
            collector.dynamic = True
            controller = asyncio.create_task(follow_attention(collector, client, stop))
        worker = asyncio.create_task(collector.run(stop))

        async def stop_socket():
            await stop.wait()
            if collector._ws is not None:
                await collector._ws.close()

        stopper = asyncio.create_task(stop_socket())
        try:
            if controller:
                await asyncio.wait((worker, controller), return_when=asyncio.FIRST_COMPLETED)
                if not stop.is_set() and controller.done():
                    await controller
                    raise RuntimeError("attention controller stopped unexpectedly")
            await worker
        finally:
            worker.cancel()
            stopper.cancel()
            await asyncio.gather(worker, stopper, return_exceptions=True)
            if controller:
                controller.cancel()
                await asyncio.gather(controller, return_exceptions=True)
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
        declared = [
            gap.to_payload()
            for gap in await store.list_gaps(state=QualityState.DECLARED_OUTAGE.value, limit=100)
        ]
        return {
            "storage": health,
            "channels": coverage,
            "open_gaps": gaps,
            "declared_outages": declared,
        }
    finally:
        await store.close()


async def declare_outage(
    gap_id: int,
    *,
    end_ms: int,
    reason: str,
) -> dict[str, Any]:
    store = await PostgresStore.connect(database_url())
    try:
        gaps = GapTracker(store)
        now_ms = _now_ms()
        gap = await gaps.declare_outage(
            gap_id,
            end_ms=end_ms,
            reason=reason,
            now_ms=now_ms,
        )
        if gap is None:
            return {"status": "not_found", "gap_id": gap_id}
        # Recompute the affected minutes so they immediately show the declared
        # state instead of a stale ``recovering`` value.
        windows = WindowService(store)
        kind = known_channel(gap.venue, gap.channel).value
        first = gap.start_ms // WINDOW_MS * WINDOW_MS
        last = max(first, (min(end_ms, now_ms) - 1) // WINDOW_MS * WINDOW_MS)
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
    finally:
        await store.close()


async def acceptance(
    *,
    start_ms: int,
    end_ms: int,
    required_pct: float = 99.0,
    report_format: str = "json",
) -> str:
    store = await PostgresStore.connect(database_url())
    try:
        report = await build_acceptance_report(
            store,
            start_ms=start_ms,
            end_ms=end_ms,
            required_pct=required_pct,
            expected_channels=required_channels(markets()),
        )
        return render_markdown(report) if report_format == "markdown" else dumps(report)
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

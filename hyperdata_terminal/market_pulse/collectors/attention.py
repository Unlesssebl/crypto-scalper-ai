"""Reconcile attention leases over the existing exchange socket."""
from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from hyperdata_terminal.instruments import is_binance
from hyperdata_terminal.network import exchange_session

from ..control import desired_symbols
from ..transport import IngestError
from .okx import OkxInstrumentCatalog
from .specs import hyperliquid_spec

LOG = logging.getLogger(__name__)


async def follow_attention(collector, client, stop, *, interval=5):
    if collector.venue == "binance":
        return await follow_binance_attention(collector, client, stop, interval=interval)
    collector.dynamic = True
    cached_plan = None
    checks, metadata, retry_at = {}, {}, {}
    errors = {}
    catalog = OkxInstrumentCatalog()
    async with exchange_session() as session:
        while not stop.is_set():
            now = int(time.time() * 1000)
            try:
                cached_plan = await client.attention()
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
                LOG.warning("Attention control unavailable; cached lease expires normally")
            wanted = tuple(s for s in desired_symbols(cached_plan, now) if not is_binance(s))
            blocked = client.maintenance.get("status") == "blocked"
            if blocked:
                wanted = ()
            if collector.venue == "hyperliquid":
                try:
                    if not blocked and metadata.get("expires", 0) <= now:
                        async with session.post("https://api.hyperliquid.xyz/info", json={"type": "meta"},
                                                timeout=aiohttp.ClientTimeout(total=10)) as response:
                            response.raise_for_status()
                            body = await response.json()
                        names = {r["name"] for r in body["universe"] if not r.get("isDelisted")}
                        if len(names) > 500:
                            raise ValueError("overview subscription capacity exceeded")
                        metadata = {"expires": now + 600_000, "names": names}
                    await collector.set_overview_markets(() if blocked else metadata.get("names", ()))
                    await collector.flush_overview()
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, IngestError):
                    LOG.exception("Broad candle observation unavailable")
            enabled, statuses = [], {}
            for symbol in wanted:
                native = collector.native_market_id(symbol)
                if checks.get(symbol, 0) <= now and retry_at.get(symbol, 0) <= now:
                    try:
                        if collector.venue == "hyperliquid":
                            if metadata.get("expires", 0) <= now:
                                async with session.post("https://api.hyperliquid.xyz/info", json={"type": "meta"},
                                                        timeout=aiohttp.ClientTimeout(total=10)) as response:
                                    response.raise_for_status()
                                    body = await response.json()
                                metadata = {"expires": now + 600_000, "names": {
                                    r["name"] for r in body["universe"] if not r.get("isDelisted")}}
                            if symbol not in metadata["names"]:
                                raise ValueError("instrument_not_listed")
                            spec = hyperliquid_spec(symbol, valid_from_ms=now)
                        else:
                            spec = await catalog.fetch(session, symbol, verify=True)
                        await client.register_instruments([spec])
                        collector.specs[native] = spec
                        checks[symbol] = now + 600_000
                        retry_at.pop(symbol, None)
                        errors.pop(symbol, None)
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError, IngestError) as exc:
                        retry_at[symbol] = now + 60_000
                        # No invented defaults for dynamic instruments.
                        statuses[symbol] = {"status": "unavailable", "reason": str(exc)[:180]}
                        errors[symbol] = str(exc)[:180]
                if checks.get(symbol, 0) > now:
                    enabled.append(symbol)
                    statuses[symbol] = {"status": "observing" if native in collector.confirmed else "pending"}
                else:
                    statuses.setdefault(symbol, {"status": "unavailable", "reason": errors.get(
                        symbol, "specification_unverified")})
            try:
                await collector.change_markets(enabled, reason="storage_pressure" if blocked else "attention_released")
                await collector.expire_pending()
                await client.collector_status(collector.venue, statuses,
                                              desired=list(wanted), confirmed=sorted(collector.confirmed),
                                              pending_events=len(collector._pending),
                                              last_receive_ms=collector._last_receive_ms,
                                              socket_connected=collector._ws is not None,
                                              overview_subscriptions=len(getattr(
                                                  collector, "_overview_subscribed", ())),
                                              overview_pending=len(getattr(collector, "_overview_pending", ())),
                                              overview_rejected=getattr(collector, "overview_rejected", 0),
                                              overview_last_error=getattr(collector, "overview_last_error", None))
            except Exception:
                LOG.exception("Attention reconciliation failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


async def follow_binance_attention(collector, client, stop, *, interval=5):
    """The common plan is the only author of Binance detail subscriptions."""
    collector.dynamic = True
    cached_plan = None
    async with exchange_session() as session:
        while not stop.is_set():
            now = int(time.time() * 1000)
            try:
                cached_plan = await client.attention()
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
                LOG.warning("Binance attention unavailable; cached lease expires normally")
            blocked = client.maintenance.get("status") == "blocked"
            wanted = tuple(s for s in desired_symbols(cached_plan, now) if is_binance(s)) if not blocked else ()
            statuses, enabled = {}, []
            try:
                if not blocked:
                    await collector.refresh_catalog(session)
                    await collector.sync_overview()
                    await collector.flush_overview()
                for symbol in wanted:
                    native = collector.native_market_id(symbol)
                    if native in collector.specs:
                        enabled.append(symbol)
                        statuses[symbol] = dict(status="observing" if native in collector.confirmed else "pending")
                    else:
                        statuses[symbol] = dict(status="unavailable", reason="instrument_not_verified")
                await collector.change_markets(enabled, reason="storage_pressure" if blocked else "attention_released")
                await collector.expire_pending()
                await client.collector_status("binance", statuses, desired=list(wanted),
                                              confirmed=sorted(collector.confirmed),
                                              socket_connected=collector._ws is not None,
                                              last_receive_ms=collector._last_receive_ms,
                                              pending_events=len(collector._pending),
                                              overview_subscriptions=len(collector._overview_subscribed),
                                              overview_pending=len(collector._overview_pending),
                                              overview_rejected=collector.overview_rejected,
                                              overview_last_error=collector.overview_last_error)
            except Exception:
                # Expiry still releases old detail even if metadata/ingest fails.
                await collector.change_markets(set(collector.config.markets) & set(wanted))
                LOG.exception("Binance attention reconciliation failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass

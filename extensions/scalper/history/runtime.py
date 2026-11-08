"""Durable archive and facts shared by the native attention observation."""

import asyncio
import os
import time
from dataclasses import asdict
from pathlib import Path

from extensions.scalper.relative_attention import AttentionPolicy

from .archive import Archive
from .facts import Facts


def history_dir():
    configured = os.getenv("HYPERDATA_HISTORY_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    base = Path(os.getenv("LOCALAPPDATA", str(Path.home() / ".local" / "share")))
    return base / "hyperdata-screener" / "market-history"


class Observation:
    """Archive/facts base. The concrete native owner is ``PulseObservation``."""

    def __init__(self, hub, root=None):
        self.hub = hub
        self.facts = Facts(version=2)
        total = int(os.getenv("HYPERDATA_HISTORY_MAX_BYTES", str(5 * 1024**3)))
        reserve = min(256 * 1024**2, total // 20)
        self.archive = Archive(root or history_dir(), quota_bytes=total - reserve)
        self.last_scout = None
        self.subscription_error = None
        self.closed = False
        self._record("policy", asdict(AttentionPolicy(version=4)), time.time())
        self._record(
            "source",
            dict(
                venue="per_instrument",
                venues=["hyperliquid", "binance"],
                market="perpetual",
                book="venue-specific top-n snapshots",
                book_sources=dict(hyperliquid="l2Book", binance="depth20"),
                nSigFigs=5,
                scope="top-n",
                trade_sampling=False,
                fact_version=2,
            ),
            time.time(),
        )

    def _record(self, kind, payload, now, **kwargs):
        event = self.archive.record(kind, payload, now, **kwargs)
        if event is not None:
            self.facts.apply(event)
        return event

    def raw(self, source, text, epoch, now, monotonic_ns):
        if self.closed:
            return
        if text is None:
            symbols = source.split(":", 1)[1].split(",") if ":" in source else list(getattr(self, "applied", {}))
            if source in {"book", "binance_book"}:
                symbols = [s for s in symbols if s.startswith("BINANCE:") == (source == "binance_book")]
            self._record(
                "transport",
                dict(source=source, symbols=symbols, status="disconnected"),
                now,
                epoch=epoch,
                monotonic_ns=monotonic_ns,
            )
        else:
            self._record("raw", dict(source=source, text=text), now, epoch=epoch, monotonic_ns=monotonic_ns)

    async def close(self):
        if self.closed:
            return
        self.closed = True
        await asyncio.to_thread(self.archive.close)


def configure(hub):
    """Create the native attention owner only for the unified Crypto Scalper run.

    Standalone HyperData commands (``hyperdata cvd``, ``hyperdata api``, ...)
    must keep their own exchange collectors: Market Pulse may not exist there.
    ``scripts/serve.py`` marks its children with ``HYPERDATA_UNIFIED_RUNTIME``.
    """
    from hyperdata_terminal.runtime_mode import unified_enabled

    if hub.demo or not unified_enabled():
        return None
    from .pulse_runtime import PulseObservation

    observation = PulseObservation(hub)
    from hyperdata_terminal.data_layer.binance_book import BinanceOrderBookEngine

    hub.orderbook.binance = BinanceOrderBookEngine()
    hub.orderbook.symbols = []
    hub.orderbook.books.clear()
    hub.orderbook.snapshots.clear()
    hub.orderflow.selective = True
    hub.orderflow.pulse_external = True
    hub.orderflow.include_binance = False
    hub.orderflow.symbols = []
    hub.orderbook.raw_callback = observation.raw
    return observation

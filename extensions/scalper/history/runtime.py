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
                venue="hyperliquid",
                market="perpetual",
                book="l2Book snapshots",
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
    """Create the single native attention owner for a live hub."""
    if hub.demo:
        return None
    from .pulse_runtime import PulseObservation

    observation = PulseObservation(hub)
    hub.orderbook.symbols = []
    hub.orderbook.books.clear()
    hub.orderbook.snapshots.clear()
    hub.orderflow.selective = True
    hub.orderflow.pulse_external = True
    hub.orderflow.include_binance = False
    hub.orderflow.symbols = []
    hub.orderbook.raw_callback = observation.raw
    return observation

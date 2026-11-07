"""Attention attached to the process owning native HyperData sockets."""

import asyncio
import math
import os
import time
from dataclasses import asdict
from pathlib import Path

from extensions.scalper.relative_attention import Attention, AttentionPolicy

from .archive import Archive
from .facts import Facts


def history_dir():
    configured = os.getenv("HYPERDATA_HISTORY_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    base = Path(os.getenv("LOCALAPPDATA", str(Path.home() / ".local" / "share")))
    return base / "hyperdata-screener" / "market-history"


class Observation:
    def __init__(self, hub, root=None, policy=None):
        self.hub = hub
        self.attention = Attention(policy)
        self.facts = Facts(version=2)
        total = int(os.getenv("HYPERDATA_HISTORY_MAX_BYTES", str(5 * 1024**3)))
        reserve = min(256 * 1024**2, total // 20)
        self.archive = Archive(root or history_dir(), quota_bytes=total - reserve)
        self.last_scout = None
        self.subscription_error = None
        self.closed = False
        self._record("policy", asdict(self.attention.policy), time.time())
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
            symbols = source.split(":", 1)[1].split(",") if ":" in source else list(self.attention.active)
            self._record(
                "transport",
                dict(source=source, symbols=symbols, status="disconnected"),
                now,
                epoch=epoch,
                monotonic_ns=monotonic_ns,
            )
        else:
            self._record("raw", dict(source=source, text=text), now, epoch=epoch, monotonic_ns=monotonic_ns)

    async def tick(self, now):
        observations = {}
        for symbol, asset in self.hub.market.assets.items():
            if not 0 <= now - self.hub.market.observed_at <= 20:
                continue
            if not {"price", "volume_24h"} <= self.hub.market.observed_fields.get(symbol, set()):
                continue
            if (
                math.isfinite(asset.price)
                and math.isfinite(asset.volume_24h)
                and asset.price > 0
                and asset.volume_24h >= 0
            ):
                observations[symbol] = dict(price=asset.price, volume_24h=asset.volume_24h)
        if self.last_scout is not None and now <= self.last_scout:
            return
        self.last_scout = now
        self._record("scout", observations, now)
        for change in self.attention.update(observations, now):
            self._record("assignment", change, now)
        wanted = list(self.attention.active)
        try:
            await self.hub.orderbook.set_symbols(wanted)
            await self.hub.orderflow.set_attention_symbols(wanted)
            self.subscription_error = None
        except Exception as exc:
            self.subscription_error = f"{type(exc).__name__}: {exc}"
            self._record(
                "transport", dict(symbols=wanted, status="subscription_error", error=self.subscription_error), now
            )
        for symbol in wanted:
            self._record("facts", self.facts.card(symbol, now), now)

    def report(self, now=None):
        now = time.time() if now is None else now
        cards = []
        for symbol, assignment in self.attention.active.items():
            card = self.facts.card(symbol, now)
            cards.append(
                {
                    **assignment,
                    "facts": card,
                    "ready": card["status"] == "observing" and not self.archive.error and not self.subscription_error,
                }
            )
        return {
            **self.attention.report(),
            "active": cards,
            "archive": self.archive.report(),
            "subscription_error": self.subscription_error,
            "last_scout_at": self.last_scout,
            "context_only": True,
        }

    async def close(self):
        if self.closed:
            return
        self.closed = True
        await asyncio.to_thread(self.archive.close)


def configure(hub):
    """Default on for live native hub; never create live research history in demo."""
    if hub.demo:
        return None
    if os.getenv("PULSE_UNIFIED", "0") == "1":
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
    if os.getenv("HYPERDATA_ATTENTION_ENABLED", "1").lower() in {"0", "false", "no"}:
        return None
    slots = int(os.getenv("HYPERDATA_ATTENTION_SLOTS", "4"))
    observation = Observation(hub, policy=AttentionPolicy(slots=slots))
    hub.orderbook.symbols = []
    hub.orderbook.books.clear()
    hub.orderbook.snapshots.clear()
    hub.orderflow.selective = True
    hub.orderflow.include_binance = False
    hub.orderflow.symbols = []
    hub.orderbook.raw_callback = observation.raw
    hub.orderflow.raw_callback = observation.raw
    return observation

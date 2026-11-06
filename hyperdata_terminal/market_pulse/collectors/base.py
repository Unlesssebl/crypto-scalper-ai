"""Shared websocket collector loop.

Collectors own exchange sockets and raw archives only.  They never write the
normalized journal directly: batches go to the ingest service, which is the
single writer.  A failed batch leaves the events in a bounded local queue and
the connection is treated as interrupted until a retry succeeds.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from typing import Any, Protocol, Sequence

import aiohttp

from ..contracts import ExecutionEvent
from ..raw_archive import RawArchive

LOG = logging.getLogger(__name__)


class CollectorUnavailable(RuntimeError):
    pass


class EventSink(Protocol):
    async def append_batch(self, events: Sequence[ExecutionEvent]) -> dict[str, Any]: ...

    async def report(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    async def close(self) -> None: ...


class GapReporter(Protocol):
    async def connected(
        self, venue: str, market: str, channel: str, source_epoch: str, now_ms: int
    ) -> None: ...
    async def disconnected(
        self,
        venue: str,
        market: str,
        channel: str,
        source_epoch: str,
        now_ms: int,
        *,
        reason: str,
        last_receive_ms: int | None,
    ) -> None: ...
    async def heartbeat(
        self, venue: str, market: str, channel: str, now_ms: int
    ) -> None: ...
    async def unavailable(
        self, venue: str, market: str, channel: str, source_epoch: str, now_ms: int, *, reason: str
    ) -> None: ...


class NullGapReporter:
    async def released(self, *args, **kwargs):
        return None
    async def connected(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def disconnected(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def heartbeat(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def unavailable(self, *args: Any, **kwargs: Any) -> None:
        return None


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    venue: str
    markets: tuple[str, ...]
    raw_dir: str
    ingest_url: str
    token: str | None = None
    base_backoff_ms: int = 500
    max_backoff_ms: int = 30_000
    max_consecutive_failures: int = 10
    batch_size: int = 200
    flush_interval_ms: int = 1_000
    ack_timeout_ms: int = 10_000
    max_raw_bytes: int = 5 * 1024**3
    raw_max_age_ms: int = 48 * 60 * 60 * 1_000
    fsync_raw: bool = False


class StreamCollector(ABC):
    venue: str
    channel: str

    def __init__(
        self,
        config: CollectorConfig,
        archive: RawArchive,
        sink: EventSink,
        gaps: GapReporter | None = None,
    ) -> None:
        self.config = config
        self.archive = archive
        self.sink = sink
        self.gaps = gaps or NullGapReporter()
        self._pending: list[ExecutionEvent] = []
        self._last_flush_ms = 0
        self._last_receive_ms: int | None = None
        self._session: aiohttp.ClientSession | None = None
        self.dynamic = False
        self._ws = None
        self._epoch = None
        self.confirmed = set()
        self.pending_ack = {}
        self.rejected_until = {}
        self._flush_lock = asyncio.Lock()
        self._reported_heartbeat = {}

    async def auxiliary_connected(self, ws, epoch):
        pass

    async def auxiliary_payload(self, payload, epoch, received, raw_ref):
        pass

    def acknowledged_market(self, payload):
        return None

    def _messages_for(self, markets):
        previous = self.config
        try:
            self.config = replace(previous, markets=tuple(markets))
            return self.subscriptions()
        finally:
            self.config = previous

    async def change_markets(self, markets, *, reason="attention_released"):
        """Change subscriptions on one socket; keep retained markets and their epochs."""
        wanted = tuple(markets)
        removed = set(self.config.markets) - set(wanted)
        self.config = replace(self.config, markets=wanted)
        if self._ws is not None and not self._ws.closed:
            for market in removed:
                for message in self._messages_for((market,)):
                    message = {**message, ("method" if self.venue == "hyperliquid" else "op"): "unsubscribe"}
                    await self._ws.send_json(message)
                native = self.native_market_id(market)
                self.confirmed.discard(native)
                self.pending_ack.pop(native, None)
                if reason == "attention_released":
                    await self.gaps.released(self.venue, native, self.channel, self._epoch, now_ms())
                else:
                    await self.gaps.disconnected(self.venue, native, self.channel, self._epoch, now_ms(),
                                                 reason=reason, last_receive_ms=self._last_receive_ms)
                    await self.gaps.unavailable(self.venue, native, self.channel, self._epoch, now_ms(), reason=reason)
            for market in wanted:
                native = self.native_market_id(market)
                if (native not in self.confirmed and native not in self.pending_ack
                        and self.rejected_until.get(native, 0) <= now_ms()):
                    self.pending_ack[native] = now_ms() + self.config.ack_timeout_ms
                    for message in self._messages_for((market,)):
                        await self._ws.send_json(message)
        self.config = replace(self.config, markets=wanted)

    async def expire_pending(self):
        for native, deadline in list(self.pending_ack.items()):
            if now_ms() >= deadline:
                self.pending_ack.pop(native, None)
                self.rejected_until[native] = now_ms() + 60_000
                await self.gaps.unavailable(self.venue, native, self.channel, self._epoch, now_ms(),
                                            reason="subscription_ack_timeout")

    async def _confirm(self, native, epoch, received):
        if native in self.pending_ack:
            await self.gaps.connected(self.venue, native, self.channel, epoch, received)
            self.pending_ack.pop(native, None)
            self.confirmed.add(native)

    @property
    @abstractmethod
    def url(self) -> str: ...

    @abstractmethod
    def subscriptions(self) -> list[dict[str, Any]]:
        """Messages sent immediately after opening the socket."""

    @abstractmethod
    def is_subscription_ack(self, payload: Any) -> bool: ...

    @abstractmethod
    def is_error(self, payload: Any) -> bool: ...

    def is_heartbeat_response(self, payload: Any) -> bool:
        # Text pings arrive as bare strings; OKX/Hyperliquid also send
        # ``{"channel": "pong"}``.  Test the type first: a dict is unhashable and
        # a set-membership test would raise on every data message.
        if isinstance(payload, str):
            return payload in {"pong", "ping"}
        return isinstance(payload, dict) and payload.get("channel") in {"pong", "ping"}

    def is_ignorable(self, payload: Any) -> bool:
        """True for control frames that must not enter the raw archive.

        OKX sends data *before* the subscribe ack and may emit operational
        notices later, so any frame carrying an ``event`` is control traffic,
        not market data.  Real errors are rejected earlier by ``is_error``.
        """
        return isinstance(payload, dict) and payload.get("event") is not None

    def native_market_id(self, market: str) -> str:
        """Map a V1 label (``BTC``) to the venue's native instrument id."""
        return market

    @abstractmethod
    def normalize(
        self,
        payload: Any,
        *,
        receive_time_ms: int,
        source_epoch: str,
        raw_ref: str,
    ) -> list[ExecutionEvent]: ...

    def heartbeat_payload(self) -> Any:
        return None

    @property
    def heartbeat_interval_ms(self) -> int:
        return 25_000

    async def run(self, stop: asyncio.Event | None = None) -> None:
        stop = stop or asyncio.Event()
        failures = 0
        async with aiohttp.ClientSession() as session:
            self._session = session
            try:
                while not stop.is_set():
                    epoch = uuid.uuid4().hex
                    try:
                        await self._run_connection(session, epoch, stop)
                        failures = 0
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        failures += 1
                        now = now_ms()
                        LOG.warning("%s collector interrupted: %s", self.venue, exc)
                        await self._report_disconnect(epoch, now, str(exc))
                        if failures >= self.config.max_consecutive_failures:
                            for market in self.config.markets:
                                await self.gaps.unavailable(
                                    self.venue,
                                    self.native_market_id(market),
                                    self.channel,
                                    epoch,
                                    now,
                                    reason=f"{failures} consecutive failures: {exc}",
                                )
                            raise CollectorUnavailable(
                                f"{self.venue} collector failed {failures} times"
                            ) from exc
                        await asyncio.sleep(self._backoff_seconds(failures))
            finally:
                try:
                    await self.flush(force=True)
                except Exception:
                    LOG.exception("Could not flush pending %s events at stop", self.venue)
                self._session = None

    async def _run_connection(
        self,
        session: aiohttp.ClientSession,
        epoch: str,
        stop: asyncio.Event,
    ) -> None:
        ws = await session.ws_connect(self.url, heartbeat=None, max_msg_size=8 * 1024**2)
        self._ws, self._epoch = ws, epoch
        if self.dynamic:
            self.confirmed.clear()
            self.pending_ack = {self.native_market_id(m): now_ms() + self.config.ack_timeout_ms
                                for m in self.config.markets}
        try:
            for subscription in self.subscriptions():
                await ws.send_json(subscription)
            await self.auxiliary_connected(ws, epoch)
            early = await self._wait_for_subscription(ws)
            # Report the new epoch before ingesting anything: a data frame may
            # arrive before the subscription ack, and ingesting it first would
            # overwrite the previous epoch before the restart gap is recorded.
            await self._report_connected(epoch)
            for raw_text in early:
                await self._handle_payload(raw_text, epoch)
            awaiting_response = False
            while not stop.is_set():
                try:
                    message = await ws.receive(timeout=self.heartbeat_interval_ms / 1_000)
                except asyncio.TimeoutError:
                    # Sending a ping proves only that we tried.  Liveness is
                    # recorded when a frame actually arrives, so a dead socket
                    # cannot keep reporting itself healthy.
                    if awaiting_response:
                        raise TimeoutError(f"{self.venue} heartbeat response deadline exceeded")
                    await self._send_heartbeat(ws)
                    awaiting_response = True
                    continue
                if message.type == aiohttp.WSMsgType.TEXT:
                    awaiting_response = False
                    await self._handle_payload(message.data, epoch)
                elif message.type == aiohttp.WSMsgType.BINARY:
                    awaiting_response = False
                    await self._handle_payload(message.data.decode("utf-8"), epoch)
                elif message.type in {
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                }:
                    raise ConnectionError(f"{self.venue} websocket closed")
            return
        finally:
            self._ws = None
            await ws.close()

    async def _wait_for_subscription(self, ws: aiohttp.ClientWebSocketResponse) -> list[str]:
        """Return messages that arrived before an explicit acknowledgement.

        Some feeds (OKX) push data before the subscribe ack, and others never
        send an ack at all.  Returning the raw frames lets the caller report the
        connection first and only then normalize them.
        """
        deadline = time.monotonic() + self.config.ack_timeout_ms / 1_000
        early: list[str] = []
        while time.monotonic() < deadline:
            try:
                message = await ws.receive(timeout=max(0.1, deadline - time.monotonic()))
            except asyncio.TimeoutError as exc:
                raise TimeoutError(f"{self.venue} subscription was not acknowledged") from exc
            if message.type != aiohttp.WSMsgType.TEXT:
                continue
            payload = json.loads(message.data)
            if self.is_error(payload):
                raise ConnectionError(f"{self.venue} subscription rejected: {payload}")
            if self.is_subscription_ack(payload):
                return [message.data, *early] if self.dynamic else early
            early.append(message.data)
            return early
        raise TimeoutError(f"{self.venue} subscription was not acknowledged")

    async def _handle_payload(self, raw_text: str, epoch: str) -> None:
        received = now_ms()
        if raw_text.strip() in {"pong", "ping"}:
            await self._report_heartbeat(received)
            return
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ConnectionError(f"{self.venue} sent invalid JSON") from exc
        if self.is_error(payload):
            native = (payload.get("arg") or {}).get("instId") if isinstance(payload, dict) else None
            if self.dynamic and native in {self.native_market_id(m) for m in self.config.markets}:
                self.confirmed.discard(native)
                self.pending_ack.pop(native, None)
                self.rejected_until[native] = received + 60_000
                await self.gaps.unavailable(self.venue, native, self.channel, epoch, received,
                                            reason="subscription_rejected")
                return
            raise ConnectionError(f"{self.venue} error message: {payload}")
        if self.is_heartbeat_response(payload):
            await self._report_heartbeat(received)
            return
        if self.dynamic and self.is_subscription_ack(payload):
            await self._confirm(self.acknowledged_market(payload), epoch, received)
            return
        if self.is_ignorable(payload):
            # Subscription notices are operational, not market data.
            await self._report_heartbeat(received)
            return
        reference = self.archive.append(payload, receive_time_ms=received, source_epoch=epoch)
        self._last_receive_ms = received
        await self.auxiliary_payload(payload, epoch, received, str(reference))
        events = self.normalize(
            payload,
            receive_time_ms=received,
            source_epoch=epoch,
            raw_ref=str(reference),
        )
        if self.dynamic:
            allowed = {self.native_market_id(m) for m in self.config.markets}
            events = [event for event in events if event.native_market_id in allowed]
            for native in {event.native_market_id for event in events}:
                await self._confirm(native, epoch, received)
        if events:
            self._pending.extend(events)
        await self._report_heartbeat(received)
        if len(self._pending) >= self.config.batch_size:
            await self.flush(force=True)
        elif received - self._last_flush_ms >= self.config.flush_interval_ms:
            await self.flush()

    async def flush(self, *, force: bool = False) -> None:
        async with self._flush_lock:
            await self._flush(force=force)

    async def _flush(self, *, force: bool = False) -> None:
        if not self._pending:
            self._last_flush_ms = now_ms()
            return
        if not force and now_ms() - self._last_flush_ms < self.config.flush_interval_ms:
            return
        batch = self._pending[: self.config.batch_size]
        await self.sink.append_batch(batch)
        del self._pending[: len(batch)]
        self._last_flush_ms = now_ms()

    async def _send_heartbeat(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        payload = self.heartbeat_payload()
        if payload is None:
            return
        if isinstance(payload, str):
            await ws.send_str(payload)
        else:
            await ws.send_json(payload)

    async def _report_disconnect(self, epoch: str, now: int, reason: str) -> None:
        for market in self.config.markets:
            try:
                await self.gaps.disconnected(
                    self.venue,
                    self.native_market_id(market),
                    self.channel,
                    epoch,
                    now,
                    reason=reason,
                    last_receive_ms=self._last_receive_ms,
                )
            except Exception:
                LOG.exception("Could not record %s/%s disconnect", self.venue, market)
        if self.dynamic:
            self.confirmed.clear()
            self.pending_ack.clear()
        self._reported_heartbeat.clear()

    async def _report_connected(self, epoch: str) -> None:
        if self.dynamic:
            return
        for market in self.config.markets:
            await self.gaps.connected(
                self.venue, self.native_market_id(market), self.channel, epoch, now_ms()
            )

    async def _report_heartbeat(self, now: int) -> None:
        for market in self.config.markets:
            native = self.native_market_id(market)
            if self.dynamic and native not in self.confirmed:
                continue
            if now - self._reported_heartbeat.get(native, now - 1000) < 1000:
                continue
            await self.gaps.heartbeat(
                self.venue, native, self.channel, now
            )
            self._reported_heartbeat[native] = now

    def _backoff_seconds(self, failure: int) -> float:
        base = min(self.config.max_backoff_ms, self.config.base_backoff_ms * 2 ** (failure - 1))
        jitter = random.uniform(0, base * 0.25)
        return (base + jitter) / 1_000


def now_ms() -> int:
    return int(time.time() * 1_000)

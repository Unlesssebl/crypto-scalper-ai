"""HTTP transport between collectors and the single ingest writer."""
from __future__ import annotations

import asyncio
from typing import Any, Sequence

import aiohttp

from .contracts import SCHEMA_VERSION, ExecutionEvent


class IngestError(RuntimeError):
    pass


class HttpIngestClient:
    """Small client implementing both event and gap-report protocols."""

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        self._session: aiohttp.ClientSession | None = None
        self.maintenance = {}

    async def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        async with self._session.post(f"{self.base_url}{path}", json=payload, headers=headers) as response:
            body = await response.text()
            if response.status == 409:
                # A conflict is a valid terminal outcome, not a transport
                # failure.  The collector must not retry it forever.
                return {"status": "conflict", "detail": _json_or_text(body)}
            if response.status >= 400:
                raise IngestError(f"ingest returned {response.status}: {body[:500]}")
            return _json_or_text(body)

    async def append_batch(self, events: Sequence[ExecutionEvent]) -> dict[str, Any]:
        payload = {
            "schema_version": SCHEMA_VERSION,
            "events": [event.to_payload() for event in events],
        }
        return await self._post("/ingest/v1/batches", payload)

    async def attention(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=self.timeout)
        async with self._session.get(self.base_url + "/ingest/v1/attention") as response:
            response.raise_for_status()
            body = await response.json()
            self.maintenance = body.get("maintenance") or {}
            return body.get("plan")

    async def overview(self, candles):
        return await self._post("/ingest/v1/overview", {"candles": candles})

    async def collector_status(self, venue, markets, **details):
        return await self._post("/ingest/v1/collector-status/" + venue, {"markets": markets, **details})

    async def report(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._post("/ingest/v1/gaps", payload)

    async def register_instruments(self, specs) -> dict[str, Any]:
        return await self._post(
            "/ingest/v1/instruments",
            {"instruments": [spec.to_payload() for spec in specs]},
        )

    async def register_raw_segments(self, segments) -> dict[str, Any]:
        return await self._post(
            "/ingest/v1/raw-segments",
            {"segments": [segment.to_payload() for segment in segments]},
        )

    async def connected(self, venue, market, channel, source_epoch, now_ms) -> None:
        await self.report(
            {
                "action": "connected",
                "venue": venue,
                "native_market_id": market,
                "channel": channel,
                "source_epoch": source_epoch,
                "now_ms": now_ms,
            }
        )

    async def disconnected(
        self,
        venue,
        market,
        channel,
        source_epoch,
        now_ms,
        *,
        reason,
        last_receive_ms,
    ) -> None:
        await self.report(
            {
                "action": "disconnected",
                "venue": venue,
                "native_market_id": market,
                "channel": channel,
                "source_epoch": source_epoch,
                "now_ms": now_ms,
                "reason": reason,
                "last_receive_ms": last_receive_ms,
            }
        )

    async def heartbeat(self, venue, market, channel, now_ms) -> None:
        await self.report(
            {
                "action": "heartbeat",
                "venue": venue,
                "native_market_id": market,
                "channel": channel,
                "source_epoch": "heartbeat",
                "now_ms": now_ms,
            }
        )

    async def unavailable(self, venue, market, channel, source_epoch, now_ms, *, reason) -> None:
        await self.report(
            {
                "action": "unavailable",
                "venue": venue,
                "native_market_id": market,
                "channel": channel,
                "source_epoch": source_epoch,
                "now_ms": now_ms,
                "reason": reason,
            }
        )

    async def released(self, venue, market, channel, source_epoch, now_ms):
        return await self.report({"action": "released", "venue": venue, "native_market_id": market,
                                  "channel": channel, "source_epoch": source_epoch, "now_ms": now_ms})

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None


def _json_or_text(body: str) -> dict[str, Any]:
    import json

    if not body:
        return {}
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return {"body": body}
    return parsed if isinstance(parsed, dict) else {"body": parsed}


async def with_retry(call, *, attempts: int = 3, base_delay: float = 0.25):
    """Bounded retry helper for control messages, never for event ambiguity."""
    last_error = None
    for attempt in range(attempts):
        try:
            return await call()
        except (aiohttp.ClientError, asyncio.TimeoutError, IngestError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(base_delay * (2**attempt))
    raise IngestError(str(last_error))

"""Hyperliquid public trades feed."""
from __future__ import annotations

import asyncio
import hashlib
import json
from typing import Any

from ..contracts import (
    ExecutionEvent,
    InstrumentSpec,
    known_channel,
    normalize_epoch_ms,
)
from .base import CollectorConfig, StreamCollector, now_ms


def normalize_hyperliquid_trades(
    payload: Any,
    spec: InstrumentSpec,
    *,
    receive_time_ms: int,
    source_epoch: str,
    raw_ref: str,
) -> list[ExecutionEvent]:
    if not isinstance(payload, dict) or payload.get("channel") != "trades":
        return []
    rows = payload.get("data")
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        raise ValueError("Hyperliquid trades payload has no data list")
    aggregation = known_channel("hyperliquid", "trades")
    result: list[ExecutionEvent] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError("Hyperliquid trade is not an object")
        if not row.get("coin"):
            raise ValueError("Hyperliquid execution has no instrument identity")
        if row["coin"] != spec.native_market_id:
            continue
        side = _side(row.get("side"))
        event_time_ms = normalize_epoch_ms(row.get("time"), "ms")
        price = row.get("px")
        amount = row.get("sz")
        trade_id = row.get("tid")
        if trade_id is None:
            trade_id = row.get("hash")
        if trade_id is None:
            encoded = json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
            trade_id = f"sha256:{hashlib.sha256(encoded).hexdigest()[:24]}:{index}"
        base = spec.base_quantity(amount)
        quote = spec.quote_amount(price, amount) if base is not None else None
        result.append(
            ExecutionEvent(
                venue="hyperliquid",
                native_market_id=spec.native_market_id,
                channel="trades",
                aggregation_kind=aggregation.value,
                trade_id=str(trade_id),
                event_time_ms=event_time_ms,
                receive_time_ms=receive_time_ms,
                source_epoch=source_epoch,
                side=side,
                price=_decimal_text(price),
                native_amount=_decimal_text(amount),
                base_amount=base,
                quote_amount=quote,
                instrument_spec_version=spec.spec_version,
                raw_ref=raw_ref,
                execution_count=1,
            )
        )
    return result


def _side(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"b", "buy", "bid"}:
        return "buy"
    if text in {"a", "ask", "sell", "s"}:
        return "sell"
    raise ValueError(f"unknown Hyperliquid side {value!r}")


def _decimal_text(value: Any):
    from decimal import Decimal

    return Decimal(str(value))


class HyperliquidCollector(StreamCollector):
    venue = "hyperliquid"
    channel = "trades"

    def __init__(self, config: CollectorConfig, archive, sink, gaps=None, specs=None) -> None:
        super().__init__(config, archive, sink, gaps)
        self.overview_markets = set()
        self._overview_subscribed = set()
        self._overview_pending = {}
        self._overview_epoch_start = 0
        self.overview_rejected = 0
        self.overview_last_error = None
        self._overview_lock = asyncio.Lock()
        self.specs = {spec.native_market_id: spec for spec in (specs or [])}
        if not self.specs:
            from .specs import hyperliquid_spec

            self.specs = {market: hyperliquid_spec(market) for market in config.markets}

    async def auxiliary_connected(self, ws, epoch):
        self._overview_subscribed.clear()
        self._overview_epoch_start = now_ms()
        await self.set_overview_markets(self.overview_markets)

    async def set_overview_markets(self, markets):
        async with self._overview_lock:
            self.overview_markets = set(markets)
            if self._ws is None or self._ws.closed:
                return
            for method, symbols in (("unsubscribe", self._overview_subscribed - self.overview_markets),
                                    ("subscribe", self.overview_markets - self._overview_subscribed)):
                for symbol in sorted(symbols):
                    await self._ws.send_json({"method": method, "subscription": {
                        "type": "candle", "coin": symbol, "interval": "1m"}})
            self._overview_subscribed = set(self.overview_markets)

    async def auxiliary_payload(self, payload, epoch, received, raw_ref):
        if not isinstance(payload, dict) or payload.get("channel") != "candle":
            return
        try:
            self._accept_overview_candle(payload, epoch, received, raw_ref)
        except ValueError as exc:
            # Discovery summaries share a socket with executions. A malformed
            # summary must not tear down healthy trade subscriptions and create
            # gaps in the canonical tape. The raw frame is already archived.
            self.overview_rejected += 1
            self.overview_last_error = dict(reason=str(exc), received_at_ms=received, raw_ref=raw_ref)
            data = payload.get("data")
            if isinstance(data, dict) and isinstance(data.get("s"), str) and type(data.get("t")) is int:
                self._overview_pending.pop((data["s"], data["t"]), None)

    def _accept_overview_candle(self, payload, epoch, received, raw_ref):
        from ..overview import VERSION, validate_candle
        data = payload.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("s"), str):
            raise ValueError("invalid candle payload")
        if data.get("s") not in self.overview_markets or data.get("i") != "1m":
            return
        start = data.get("t")
        if type(start) is not int or data.get("T") != start + 59_999:
            raise ValueError("unexpected candle boundaries")
        row = validate_candle({"schema_version": VERSION, "symbol": data["s"],
            "start_ms": start, "received_at_ms": received,
            "epoch_started_ms": self._overview_epoch_start, "trade_count": data.get("n"),
            "base_volume": data.get("v"), "close": data.get("c"),
            "source_epoch": epoch, "raw_ref": raw_ref}, received)
        self._overview_pending[(data["s"], start)] = row
        # Missing transport history remains missing, never a zero observation.
        self._overview_pending = {k: v for k, v in self._overview_pending.items()
                                  if v["start_ms"] >= received // 60_000*60_000 - 120_000}

    async def flush_overview(self):
        rows = list(self._overview_pending.items())[:1000]
        if rows:
            await self.sink.overview([r for _, r in rows])
            for key, row in rows:
                if self._overview_pending.get(key) is row:
                    self._overview_pending.pop(key, None)

    @property
    def url(self) -> str:
        return "wss://api.hyperliquid.xyz/ws"

    def subscriptions(self) -> list[dict[str, Any]]:
        return [
            {"method": "subscribe", "subscription": {"type": "trades", "coin": market}}
            for market in self.config.markets
        ]

    def is_subscription_ack(self, payload: Any) -> bool:
        return isinstance(payload, dict) and payload.get("channel") == "subscriptionResponse"

    def acknowledged_market(self, payload):
        if self.is_subscription_ack(payload):
            data = payload.get("data") or {}
            if (data.get("method") == "subscribe"
                    and (data.get("subscription") or {}).get("type") == "trades"):
                return (data.get("subscription") or {}).get("coin")
        return None

    def is_error(self, payload: Any) -> bool:
        return isinstance(payload, dict) and (
            payload.get("channel") == "error" or "error" in payload
        )

    def heartbeat_payload(self) -> Any:
        return {"method": "ping"}

    @property
    def heartbeat_interval_ms(self) -> int:
        return 50_000

    def normalize(
        self,
        payload: Any,
        *,
        receive_time_ms: int,
        source_epoch: str,
        raw_ref: str,
    ) -> list[ExecutionEvent]:
        if not isinstance(payload, dict) or payload.get("channel") != "trades":
            return []
        rows = payload.get("data")
        if isinstance(rows, dict):
            rows = [rows]
        if not isinstance(rows, list):
            raise ValueError("Hyperliquid trades payload has no data list")
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("Hyperliquid trade is not an object")
            coin = str(row.get("coin") or "")
            if coin not in self.specs:
                # Uncovered markets are not part of V1; they are never reported
                # as an observed cross-venue comparison.
                continue
            grouped.setdefault(coin, []).append(row)
        events: list[ExecutionEvent] = []
        for coin, coin_rows in grouped.items():
            events.extend(
                normalize_hyperliquid_trades(
                    {"channel": "trades", "data": coin_rows},
                    self.specs[coin],
                    receive_time_ms=receive_time_ms,
                    source_epoch=source_epoch,
                    raw_ref=raw_ref,
                )
            )
        return events

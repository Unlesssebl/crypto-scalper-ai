"""Hyperliquid public trades feed."""
from __future__ import annotations

import hashlib
import json
from typing import Any

from ..contracts import (
    ExecutionEvent,
    InstrumentSpec,
    known_channel,
    normalize_epoch_ms,
)
from .base import CollectorConfig, StreamCollector


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
        self.specs = {spec.native_market_id: spec for spec in (specs or [])}
        if not self.specs:
            from .specs import hyperliquid_spec

            self.specs = {market: hyperliquid_spec(market) for market in config.markets}

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
            coin = str(row.get("coin") or "").upper()
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

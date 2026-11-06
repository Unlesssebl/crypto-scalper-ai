"""OKX public trades feed.

``trades-all`` is the V1 canonical channel.  The aggregated ``trades`` channel
is accepted only for explicit reconciliation and is stored under a different
``aggregation_kind`` so it can never be counted as the same executions.
"""
from __future__ import annotations

import hashlib
import json
import time
from decimal import Decimal
from typing import Any

import aiohttp

from ..contracts import (
    ExecutionEvent,
    InstrumentSpec,
    known_channel,
    normalize_epoch_ms,
)
from .base import CollectorConfig, StreamCollector
from .specs import OKX_DEFAULT_CONTRACT_SIZE, OKX_MARKET_IDS, market_for_venue, okx_spec


def normalize_okx_trades(
    payload: Any,
    spec: InstrumentSpec,
    *,
    receive_time_ms: int,
    source_epoch: str,
    raw_ref: str,
) -> list[ExecutionEvent]:
    if not isinstance(payload, dict):
        return []
    argument = payload.get("arg") or {}
    channel = argument.get("channel") if isinstance(argument, dict) else None
    if channel not in {"trades-all", "trades"}:
        return []
    rows = payload.get("data")
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        raise ValueError("OKX trades payload has no data list")
    aggregation = known_channel("okx", channel)
    result: list[ExecutionEvent] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError("OKX trade is not an object")
        inst_id = str(row.get("instId") or argument.get("instId") or "")
        if inst_id != spec.native_market_id:
            continue
        side = _side(row.get("side"))
        event_time_ms = normalize_epoch_ms(row.get("ts"), "ms")
        price = row.get("px")
        amount = row.get("sz")
        trade_id = row.get("tradeId")
        if trade_id is None:
            encoded = json.dumps(row, sort_keys=True, separators=(",", ":")).encode()
            trade_id = f"sha256:{hashlib.sha256(encoded).hexdigest()[:24]}:{index}"
        count = _execution_count(row.get("count"))
        base = spec.base_quantity(amount)
        quote = spec.quote_amount(price, amount) if base is not None else None
        result.append(
            ExecutionEvent(
                venue="okx",
                native_market_id=spec.native_market_id,
                channel=channel,
                aggregation_kind=aggregation.value,
                trade_id=str(trade_id),
                event_time_ms=event_time_ms,
                receive_time_ms=receive_time_ms,
                source_epoch=source_epoch,
                side=side,
                price=_decimal(price),
                native_amount=_decimal(amount),
                base_amount=base,
                quote_amount=quote,
                instrument_spec_version=spec.spec_version,
                raw_ref=raw_ref,
                execution_count=count,
            )
        )
    return result


def _side(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"buy", "b", "bid"}:
        return "buy"
    if text in {"sell", "s", "ask"}:
        return "sell"
    raise ValueError(f"unknown OKX side {value!r}")


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _execution_count(value: Any) -> int:
    if value is None:
        return 1
    count = int(value)
    return max(1, count)


class OkxInstrumentCatalog:
    """Fetch public contract values before the stream starts.

    A failed refresh is not fatal if the operator supplied a checked default;
    the fallback keeps its explicit ``source`` label so the provenance stays
    visible in the window and API.
    """

    def __init__(self, base_url: str = "https://www.okx.com") -> None:
        self.base_url = base_url.rstrip("/")

    async def fetch(self, session: aiohttp.ClientSession, market: str, *, verify=False) -> InstrumentSpec:
        inst_id = market_for_venue("okx", market)
        async with session.get(
            f"{self.base_url}/api/v5/public/instruments",
            params={"instType": "SWAP", "instId": inst_id},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            response.raise_for_status()
            body = await response.json()
        if body.get("code") != "0" or not body.get("data"):
            raise ValueError(f"OKX instrument catalog rejected {inst_id}: {body.get('msg')}")
        row = body["data"][0]
        if verify and (row.get("instId") != inst_id or row.get("ctType") != "linear"
                       or row.get("settleCcy") != "USDT" or row.get("ctValCcy") != market.upper()
                       or row.get("state") != "live"):
            raise ValueError("unsupported OKX contract specification: " + inst_id)
        contract_size = _decimal(row["ctVal"])
        multiplier = _decimal(row.get("ctMult") or "1")
        if contract_size <= 0 or multiplier <= 0:
            raise ValueError(f"OKX returned a non-positive contract value for {inst_id}")
        return okx_spec(
            market,
            contract_size,
            contract_multiplier=multiplier,
            valid_from_ms=int(time.time() * 1_000),
            source=f"okx public instruments {inst_id}",
        )

    async def fetch_many(self, markets: tuple[str, ...]) -> list[InstrumentSpec]:
        async with aiohttp.ClientSession() as session:
            return [await self.fetch(session, market) for market in markets]


class OkxCollector(StreamCollector):
    venue = "okx"
    channel = "trades-all"

    def __init__(self, config: CollectorConfig, archive, sink, gaps=None, specs=None) -> None:
        super().__init__(config, archive, sink, gaps)
        self.specs = {spec.native_market_id: spec for spec in (specs or [])}
        if not self.specs:
            self.specs = {
                OKX_MARKET_IDS[market]: okx_spec(market, OKX_DEFAULT_CONTRACT_SIZE[market])
                for market in config.markets
            }

    def native_market_id(self, market: str) -> str:
        # Coverage and gaps must be keyed like the events themselves, or the
        # store ends up with both "BTC" and "BTC-USDT-SWAP" channels.
        return market_for_venue("okx", market)

    @property
    def url(self) -> str:
        # ``trades-all`` is served by the business endpoint, not /public.
        # Verified live: /public rejects it with code 60018, while /business
        # streams execution rows without an aggregation ``count``.
        return "wss://ws.okx.com:443/ws/v5/business"

    def subscriptions(self) -> list[dict[str, Any]]:
        return [
            {
                "op": "subscribe",
                "args": [{"channel": "trades-all", "instId": market_for_venue("okx", market)}],
            }
            for market in self.config.markets
        ]

    def is_subscription_ack(self, payload: Any) -> bool:
        return isinstance(payload, dict) and payload.get("event") == "subscribe"

    def acknowledged_market(self, payload):
        return (payload.get("arg") or {}).get("instId") if self.is_subscription_ack(payload) else None

    def is_error(self, payload: Any) -> bool:
        return isinstance(payload, dict) and payload.get("event") == "error"

    def is_heartbeat_response(self, payload: Any) -> bool:
        return payload == "pong"

    def heartbeat_payload(self) -> Any:
        return "ping"

    @property
    def heartbeat_interval_ms(self) -> int:
        return 25_000

    def normalize(
        self,
        payload: Any,
        *,
        receive_time_ms: int,
        source_epoch: str,
        raw_ref: str,
    ) -> list[ExecutionEvent]:
        if not isinstance(payload, dict):
            return []
        argument = payload.get("arg") or {}
        inst_id = argument.get("instId") if isinstance(argument, dict) else None
        if inst_id not in self.specs:
            return []
        return normalize_okx_trades(
            payload,
            self.specs[inst_id],
            receive_time_ms=receive_time_ms,
            source_epoch=source_epoch,
            raw_ref=raw_ref,
        )

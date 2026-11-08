"""USD-M discovery candles and a durable, explicitly aggregated trade tape."""

from __future__ import annotations

import json
from decimal import Decimal

import aiohttp

from hyperdata_terminal.instruments import binance_key, identity

from ..contracts import ExecutionEvent, InstrumentSpec
from ..overview import VERSION, validate_candle
from .base import StreamCollector, now_ms

REST = "https://fapi.binance.com"
WS = "wss://fstream.binance.com/market/ws"


def binance_spec(row, *, valid_from_ms=0):
    if (row.get("status") != "TRADING" or row.get("contractType") != "PERPETUAL"
            or row.get("quoteAsset") != "USDT" or row.get("marginAsset") != "USDT"
            or row.get("symbol") != row.get("baseAsset", "") + "USDT"):
        raise ValueError("not a verified trading USDT-M perpetual")
    native = row["symbol"]
    binance_key(native)
    tick = next((f["tickSize"] for f in row.get("filters", []) if f.get("filterType") == "PRICE_FILTER"), None)
    return InstrumentSpec(venue="binance", native_market_id=native, base_asset=row["baseAsset"],
                          market_type="perp", quote_currency="USDT", settlement_currency="USDT",
                          contract_type="linear", native_amount_unit="base", contract_size=Decimal(1),
                          contract_multiplier=Decimal(1), spec_version=f"binance-{native}-base-v1",
                          valid_from_ms=valid_from_ms, source="Binance USD-M exchangeInfo",
                          tick_size=Decimal(tick) if tick is not None else None)


def normalize_binance_trades(payload, spec, *, receive_time_ms, source_epoch, raw_ref):
    data = payload.get("data", payload) if isinstance(payload, dict) else {}
    if data.get("e") != "aggTrade" or data.get("s") != spec.native_market_id:
        return []
    if type(data.get("m")) is not bool or type(data.get("a")) is not int or type(data.get("T")) is not int:
        raise ValueError("invalid Binance aggregate identity, time or aggressor")
    amount, price = Decimal(data["q"]), Decimal(data["p"])
    # f..l is an ID span, not an independently measured number of fills.
    # One record remains ONE aggregate, never a claimed individual execution.
    return [ExecutionEvent(venue="binance", native_market_id=spec.native_market_id,
                           channel="aggTrade", aggregation_kind="aggregated", trade_id=str(data["a"]),
                           event_time_ms=data["T"], receive_time_ms=receive_time_ms, source_epoch=source_epoch,
                           side="sell" if data["m"] else "buy", price=price, native_amount=amount,
                           base_amount=spec.base_quantity(amount), quote_amount=spec.quote_amount(price, amount),
                           instrument_spec_version=spec.spec_version, raw_ref=raw_ref, execution_count=1)]


class BinanceCollector(StreamCollector):
    send_interval_s = 0.25  # Binance futures allow 10 incoming messages per second per connection
    venue, channel = "binance", "aggTrade"

    def __init__(self, config, archive, sink, gaps=None, specs=()):
        super().__init__(config, archive, sink, gaps)
        self.specs = {s.native_market_id: s for s in specs}
        self.catalog_until = 0
        self._request_id = 0
        self._requests = {}
        self._overview_pending = {}
        self._overview_subscribed = set()
        self._overview_epoch_start = 0
        self.overview_rejected = 0
        self.overview_last_error = None

    @property
    def url(self):
        return WS

    def native_market_id(self, market):
        venue, native = identity(market)
        if venue != "binance":
            raise ValueError("Binance subscriptions require namespaced instruments")
        return native

    def request(self, method, streams, markets=()):
        self._request_id += 1
        self._requests[self._request_id] = tuple(markets) if method == "SUBSCRIBE" else ()
        return dict(method=method, params=streams, id=self._request_id)

    def subscriptions(self):
        # One frame for every market: the ack confirms them all, and a reconnect stays far under the
        # venue's 10 messages a second however many detail slots there are.
        markets = list(self.config.markets)
        if not markets:
            return []
        return [self.request("SUBSCRIBE", [self.native_market_id(s).lower() + "@aggTrade" for s in markets], markets)]

    def is_subscription_ack(self, payload):
        return isinstance(payload, dict) and "result" in payload and "id" in payload

    def is_error(self, payload):
        return isinstance(payload, dict) and "code" in payload

    async def refresh_catalog(self, session):
        if now_ms() < self.catalog_until:
            return
        async with session.get(REST + "/fapi/v1/exchangeInfo", timeout=aiohttp.ClientTimeout(total=15)) as response:
            response.raise_for_status()
            body = await response.json()
        specs = {}
        for row in body["symbols"]:
            try:
                spec = binance_spec(row)
            except ValueError:
                continue
            specs[spec.native_market_id] = spec
        if not specs:
            raise ValueError("empty verified Binance catalogue")
        await self.sink.register_instruments(list(specs.values()))
        self.specs, self.catalog_until = specs, now_ms() + 600_000
        await self.sync_overview()

    async def sync_overview(self):
        if self._ws is None or self._ws.closed:
            return
        wanted = set(self.specs)
        for method, names in (("UNSUBSCRIBE", self._overview_subscribed - wanted),
                              ("SUBSCRIBE", wanted - self._overview_subscribed)):
            ordered = sorted(names)
            for offset in range(0, len(ordered), 200):
                await self._send_json(self._ws, self.request(method, [n.lower() + "@kline_1m"
                                                               for n in ordered[offset:offset + 200]]))
        self._overview_subscribed = wanted

    async def auxiliary_connected(self, ws, epoch):
        self._requests = {k: v for k, v in self._requests.items() if v}
        self._overview_subscribed.clear()
        self._overview_epoch_start = now_ms()
        self._overview_pending.clear()
        await self.sync_overview()

    async def change_markets(self, markets, *, reason="attention_released"):
        from dataclasses import replace

        old, wanted = set(self.config.markets), set(markets)
        self.config = replace(self.config, markets=tuple(sorted(wanted)))
        removed = sorted(old - wanted)
        if removed and self._ws is not None and not self._ws.closed:
            await self._send_json(self._ws, self.request(
                "UNSUBSCRIBE", [self.native_market_id(s).lower() + "@aggTrade" for s in removed]))
        for symbol in removed:
            native = self.native_market_id(symbol)
            self.confirmed.discard(native)
            self.pending_ack.pop(native, None)
            await self.gaps.released(self.venue, native, self.channel, self._epoch or "pending", now_ms())
        if self._ws is not None and not self._ws.closed:
            added = []
            for symbol in sorted(wanted):
                native = self.native_market_id(symbol)
                if native not in self.confirmed and native not in self.pending_ack:
                    self.pending_ack[native] = now_ms() + self.config.ack_timeout_ms
                    added.append(symbol)
            if added:
                await self._send_json(self._ws, self.request(
                    "SUBSCRIBE", [self.native_market_id(s).lower() + "@aggTrade" for s in added], added))

    async def _handle_payload(self, raw_text, epoch):
        payload = json.loads(raw_text)
        received = now_ms()
        if self.is_subscription_ack(payload):
            for market in self._requests.pop(payload["id"], ()):
                if market in self.config.markets:
                    await self._confirm(self.native_market_id(market), epoch, received)
            return
        data = payload.get("data", payload)
        if data.get("e") == "kline" and not (data.get("k") or {}).get("x"):
            # Broad discovery retains CLOSED summaries only, not four copies
            # per second for hundreds of instruments. Executions are never sampled.
            self._last_receive_ms = received
            await self._report_heartbeat(received)
            return
        await super()._handle_payload(raw_text, epoch)

    async def auxiliary_payload(self, payload, epoch, received, raw_ref):
        data = payload.get("data", payload)
        if data.get("e") != "kline":
            return
        try:
            k = data["k"]
            if k.get("x") is not True or k.get("i") != "1m" or k.get("s") not in self.specs:
                return
            row = validate_candle(dict(schema_version=VERSION, symbol=binance_key(k["s"]),
                                       start_ms=k["t"], received_at_ms=received,
                                       epoch_started_ms=self._overview_epoch_start, trade_count=k["n"],
                                       base_volume=k["v"], close=k["c"], source_epoch=epoch, raw_ref=raw_ref), received)
            self._overview_pending[(k["s"], k["t"])] = row
        except (ValueError, TypeError, KeyError) as exc:
            self.overview_rejected += 1
            self.overview_last_error = dict(reason=str(exc), raw_ref=raw_ref, received_at_ms=received)

    async def flush_overview(self):
        rows = list(self._overview_pending.items())
        if rows:
            await self.sink.overview([r for _, r in rows])
            for key, row in rows:
                if self._overview_pending.get(key) is row:
                    self._overview_pending.pop(key)

    def normalize(self, payload, **kwargs):
        data = payload.get("data", payload)
        spec = self.specs.get(data.get("s"))
        return normalize_binance_trades(payload, spec, **kwargs) if spec else []

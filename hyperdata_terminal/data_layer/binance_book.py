"""Top-20 full depth snapshots; no diff-depth reconstruction or mixed venues."""

import asyncio
import time
import uuid

import aiohttp

from hyperdata_terminal.instruments import binance_key, identity
from hyperdata_terminal.network import exchange_session
from hyperdata_terminal.utils.helpers import exchange_time_fresh

from .orderbook import OrderBookEngine


class BinanceOrderBookEngine(OrderBookEngine):
    def __init__(self):
        super().__init__([], depth=20)
        self._request_id = 0
        self._last_update_ids = {}

    async def send(self, method, symbols):
        if symbols and self._ws is not None and not self._ws.closed:
            self._request_id += 1
            await self._ws.send_json(dict(method=method, id=self._request_id,
                                         params=[identity(s)[1].lower() + "@depth20@500ms" for s in sorted(symbols)]))

    async def set_symbols(self, symbols, *, reset=()):
        old, wanted = set(self.symbols), set(symbols)
        self.symbols = sorted(wanted)
        for symbol in old - wanted | set(reset):
            self.books.pop(symbol, None)
            self.snapshots.pop(symbol, None)
            self._last_update_ids.pop(symbol, None)
        for symbol in wanted:
            self.books.setdefault(symbol, dict(bids=[], asks=[], updated_at=0.0))
        await self.send("UNSUBSCRIBE", old - wanted)
        await self.send("SUBSCRIBE", wanted - old)

    async def _connect_and_listen(self):
        epoch = uuid.uuid4().hex
        async with exchange_session() as self._session:
            try:
                async with self._session.ws_connect("wss://fstream.binance.com/public/ws", heartbeat=20) as self._ws:
                    await self.send("SUBSCRIBE", self.symbols)
                    async for message in self._ws:
                        if message.type != aiohttp.WSMsgType.TEXT:
                            if message.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break
                            continue
                        now = time.time()
                        data = message.json()
                        if "code" in data:
                            raise ValueError("Binance depth subscription rejected: " + str(data))
                        if data.get("e") != "depthUpdate":
                            continue
                        symbol = binance_key(data["s"])
                        if symbol not in self.symbols:
                            continue
                        if self.raw_callback:
                            self.raw_callback("binance_book", message.data, epoch, now, time.monotonic_ns())
                        stamp, update = data.get("T", data.get("E")), data.get("u")
                        if (type(stamp) is not int or not exchange_time_fresh(now, stamp / 1000, 15)
                                or type(update) is not int or update <= self._last_update_ids.get(symbol, -1)):
                            continue
                        self._last_update_ids[symbol] = update
                        self._update_book(symbol, dict(levels=[
                            [dict(px=p, sz=q) for p, q in data["b"]],
                            [dict(px=p, sz=q) for p, q in data["a"]]], time=data.get("T", data.get("E"))))
            finally:
                if self.raw_callback:
                    self.raw_callback("binance_book", None, epoch, time.time(), time.monotonic_ns())
                self._ws = None
                # A reconnect starts a fresh observation of all visible zones.
                for symbol in self.symbols:
                    self.books[symbol] = dict(bids=[], asks=[], updated_at=0.0)
                self.snapshots.clear()
                self._last_update_ids.clear()
        await asyncio.sleep(.1)

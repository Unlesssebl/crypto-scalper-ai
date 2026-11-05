"""Hub-shaped adapter for running the screener against a native API.

The screener was written against `HyperDataHub`. This adapter exposes the same
small surface (`get_all_assets`, `market`, `orderflow`, `get_orderbook`,
`alerts`) over `/v1/market`, `/v1/snapshot` and `/v1/candles`, so the AI,
journal, phase, profile and delivery code can be reused unchanged.
"""
from __future__ import annotations

import logging
import math
import os
import time
from types import SimpleNamespace

import aiohttp

from hyperdata_terminal.data_layer.alerts import AlertManager
from hyperdata_terminal.data_layer.market_data import AssetInfo
from hyperdata_terminal.data_layer.orderbook import OrderBookLevel, OrderBookSnapshot
from hyperdata_terminal.data_layer.orderflow_engine import CVDSnapshot, Trade

LOG = logging.getLogger(__name__)


class RemoteBucket:
    """One pre-computed venue window from `/v1/snapshot`."""

    def __init__(self, snapshot: CVDSnapshot, trades: list[Trade]) -> None:
        self.snapshot = snapshot
        self.trades = trades

    def get_snapshot(self, now: float | None = None) -> CVDSnapshot:
        return self.snapshot

    def window_trades(self, now: float | None = None) -> tuple[Trade, ...]:
        return tuple(self.trades)


class RemoteOrderFlow:
    """The subset of `OrderFlowEngine` the screener reads."""

    def __init__(self) -> None:
        self.buckets: dict[str, dict[str, RemoteBucket]] = {}
        self.recent_trades: dict[str, list[Trade]] = {}
        self._connected_at: dict[str, float] = {}

    def hyperliquid_connected_at(self, symbol: str) -> float:
        return self._connected_at.get(symbol, 0.0)

    def get_snapshot(self, symbol: str, timeframe: str) -> CVDSnapshot:
        return self.buckets[symbol][timeframe].get_snapshot()

    def venue_window_trades(self, symbol: str, timeframe: str) -> tuple[Trade, ...]:
        bucket = self.buckets.get(symbol, {}).get(timeframe)
        return tuple(bucket.trades) if bucket else ()


class RemoteMarket:
    """`MarketData`-shaped cache fed by the native API."""

    def __init__(self, hub: "RemoteHub") -> None:
        self._hub = hub
        self.assets: dict[str, AssetInfo] = {}
        self.observed_fields: dict[str, set[str]] = {}
        self.observed_at: float = 0.0
        self.last_update: float = 0.0

    async def get_candles(self, symbol: str, timeframe: str = "15m", bars: int = 100) -> list[dict]:
        return await self._hub.get_candles(symbol, timeframe, bars)


class RemoteHub:
    """A `HyperDataHub`-shaped facade over the native REST API."""

    def __init__(self, api_url: str, *, timeout: float = 10.0) -> None:
        self.api_url = api_url.rstrip("/")
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.market = RemoteMarket(self)
        self.orderflow = RemoteOrderFlow()
        self.alerts = AlertManager()
        self.status = SimpleNamespace(mode="unknown", market_data="starting")
        self.demo = False
        self.ready = False
        self.last_error: str | None = None
        self._session: aiohttp.ClientSession | None = None
        self._books: dict[str, OrderBookSnapshot] = {}
        self._as_of = 0.0
        self._warnings: list[str] = []
        key = os.getenv("HYPERDATA_API_KEY", "").strip()
        self._headers = {"X-API-Key": key} if key else {}

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(timeout=self.timeout, headers=self._headers)
        await self.alerts.start()
        try:
            await self.refresh_snapshot()
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.status.market_data = "error"
            LOG.warning("Initial snapshot failed: %s", self.last_error)

    async def stop(self) -> None:
        await self.alerts.stop()
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    # -- native API ----------------------------------------------------------

    async def _get(self, path: str, **params) -> dict:
        if self._session is None:
            raise RuntimeError("RemoteHub.start() has not been called")
        async with self._session.get(f"{self.api_url}{path}", params=params) as response:
            response.raise_for_status()
            return await response.json()

    async def refresh_snapshot(self) -> None:
        try:
            market = await self._get("/v1/market", limit=20)
            assets = market.get("assets", [])[:20]
            symbols = [asset["symbol"] for asset in assets]
            if not symbols:
                raise RuntimeError("native API returned no market assets")
            payload = await self._get(
                "/v1/snapshot",
                symbols=",".join(symbols),
                windows="1m,5m",
                include_candles="0",
            )
        except Exception as exc:
            self.last_error = f"{type(exc).__name__}: {exc}"
            self.status.market_data = "error"
            raise
        self._apply(payload, assets)
        self.ready = True
        self.last_error = None

    async def get_candles(self, symbol: str, timeframe: str = "15m", bars: int = 100) -> list[dict]:
        payload = await self._get(
            "/v1/candles", symbols=symbol, interval=timeframe, bars=bars,
        )
        series = payload.get("series") or []
        return series[0].get("candles", []) if series else []

    def _resolve_symbol(self, symbol: str) -> str:
        if symbol in self.market.assets:
            return symbol
        upper = symbol.upper()
        for known in self.market.assets:
            if known.upper() == upper:
                return known
        return symbol

    async def fetch_orderbook(self, symbol: str) -> OrderBookSnapshot | None:
        """Full book for the detail endpoint; snapshots only carry top-of-book."""
        resolved = self._resolve_symbol(symbol)
        try:
            payload = await self._get(f"/v1/orderbook/{resolved}")
        except Exception as exc:
            LOG.debug("Orderbook detail failed for %s: %s", resolved, type(exc).__name__)
            return None
        bids = [OrderBookLevel(price=float(level["price"]), size=float(level["size"]))
                for level in payload.get("bids", [])]
        asks = [OrderBookLevel(price=float(level["price"]), size=float(level["size"]))
                for level in payload.get("asks", [])]
        if not bids or not asks:
            return None
        best_bid, best_ask = bids[0].price, asks[0].price
        return OrderBookSnapshot(
            timestamp=float(payload.get("timestamp") or self._as_of or time.time()),
            symbol=payload.get("symbol", resolved), bids=bids, asks=asks,
            imbalance=float(payload.get("imbalance") or 0.0),
            best_bid=best_bid, best_ask=best_ask,
            spread=float(payload.get("spread") or (best_ask - best_bid)),
        )

    def _apply(self, payload: dict, assets: list[dict]) -> None:
        now = float(payload.get("as_of") or time.time())
        self._as_of = now
        self.demo = payload.get("mode") == "demo"
        self.status.mode = payload.get("mode", "live")
        market_quality = (payload.get("market") or {}).get("quality", "unavailable")
        self.status.market_data = (
            "connected" if market_quality in ("ok", "synthetic")
            else "stale" if market_quality == "stale" else "error"
        )
        self._warnings = list(payload.get("warnings") or [])
        self.market.observed_at = float((payload.get("market") or {}).get("observed_at") or now)
        self.market.last_update = time.monotonic()
        self.market.assets = {
            item["symbol"]: AssetInfo(
                symbol=item["symbol"], price=item.get("price") or 0.0,
                funding_rate=item.get("funding_rate") or 0.0,
                open_interest=item.get("open_interest") or 0.0,
                volume_24h=item.get("volume_24h") or 0.0,
                price_change_24h_pct=item.get("price_change_24h_pct") or 0.0,
                mark_price=item.get("mark_price") or 0.0,
                index_price=item.get("index_price") or 0.0,
                premium_pct=item.get("premium_pct") or 0.0,
            )
            for item in assets
        }
        self.market.observed_fields = {}
        # The screener holds `RemoteHub.orderflow` for the process lifetime and
        # reads it on every scan. Rebuild the buckets in place: replacing the
        # whole `RemoteOrderFlow` here would orphan references (and churn the
        # GC) every 5s poll with no benefit.
        orderflow = self.orderflow
        orderflow.buckets.clear()
        orderflow.recent_trades.clear()
        orderflow._connected_at.clear()
        books: dict[str, OrderBookSnapshot] = {}
        for row in payload.get("rows", []):
            symbol, window = row["symbol"], row["window"]
            market = row.get("market") or {}
            self.market.observed_fields.setdefault(symbol, set(market.get("observed_fields") or []))
            flow = row.get("flow") or {}
            volume = float(flow.get("volume") or 0.0)
            ofi = float(flow.get("ofi") or 0.0)
            buy = max(0.0, volume * (1.0 + ofi) / 2.0)
            snapshot = CVDSnapshot(
                timestamp=now, symbol=symbol, timeframe=window,
                cvd=float(flow.get("cvd") or 0.0),
                buy_volume=buy, sell_volume=max(0.0, volume - buy),
                trade_count=int(flow.get("trade_count") or 0),
                ofi=ofi, trades_per_sec=float(flow.get("trades_per_second") or 0.0),
                signal=flow.get("signal") or "NEUTRAL",
                coverage=float(flow.get("coverage") or 0.0),
            )
            trades: list[Trade] = []
            last_at = float(flow.get("last_trade_at") or now)
            for price in (flow.get("first_trade_price"), flow.get("last_trade_price")):
                if price is not None:
                    trades.append(Trade(
                        timestamp=last_at, symbol=symbol, side="buy",
                        price=float(price), size=0.0, size_usd=0.0,
                    ))
            orderflow.buckets.setdefault(symbol, {})[window] = RemoteBucket(snapshot, trades)
            orderflow.recent_trades[symbol] = trades
            orderflow._connected_at[symbol] = float(flow.get("connected_at") or 0.0)
            book = row.get("book") or {}
            if (book.get("quality") not in (None, "unavailable")
                    and book.get("best_bid") and book.get("best_ask")):
                best_bid, best_ask = float(book["best_bid"]), float(book["best_ask"])
                books[symbol] = OrderBookSnapshot(
                    timestamp=float(book.get("timestamp") or now), symbol=symbol,
                    bids=[OrderBookLevel(price=best_bid, size=1.0)],
                    asks=[OrderBookLevel(price=best_ask, size=1.0)],
                    imbalance=float(book.get("imbalance") or 0.0),
                    best_bid=best_bid, best_ask=best_ask,
                    spread=float(book.get("spread") or (best_ask - best_bid)),
                )
        self._books = books

    # -- HyperDataHub-shaped accessors --------------------------------------

    def get_all_assets(self) -> list[AssetInfo]:
        return sorted(self.market.assets.values(), key=lambda asset: asset.volume_24h, reverse=True)

    def get_orderbook(self, symbol: str) -> OrderBookSnapshot | None:
        return self._books.get(symbol)

    def market_timestamp(self) -> float:
        return self._as_of

    def market_age(self) -> float:
        return time.time() - self._as_of if self._as_of else math.inf

    @property
    def warnings(self) -> list[str]:
        return list(self._warnings)

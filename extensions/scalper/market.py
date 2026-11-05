"""Venue-scoped snapshots from the native HyperData hub."""
import asyncio
import logging
import math
import time
from dataclasses import asdict

from hyperdata_terminal.data_layer.hub import HyperDataHub
from hyperdata_terminal.data_layer.orderbook import OrderBookEngine
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine

from .activity import CANDLE_SOURCE, tape_baseline

WINDOWS = {"1m": 60, "5m": 300}
DIRECTIONS = {
    "STRONG_BULL": "BULLISH", "BULLISH": "BULLISH", "NEUTRAL": "NEUTRAL",
    "BEARISH": "BEARISH", "STRONG_BEAR": "BEARISH",
}
MARKET_FIELDS = {
    "price": "USD mark", "volume_24h": "USD", "open_interest": "USD",
    "funding_rate": "fraction/hour",
}
LOG = logging.getLogger(__name__)


class ScalperHub(HyperDataHub):
    """Run the Hyperliquid components needed by Market Overview and the screener."""

    # Parity runs need a stable subscription set: the native hub cannot rotate
    # its orderflow shard plan, so a fork rebuild would reset every window and
    # make the comparison meaningless. Opt-in via --fixed-universe.
    fixed_universe = False

    async def start(self):
        if self._running:
            return
        self._running = True
        self.status.started_at = time.time()
        if self.demo:
            self.status.mode = "demo"
            await self._start_demo()
            self._tasks.append(asyncio.create_task(self._market_refresh_loop(), name="demo-market"))
            return
        self.status.mode = "live"
        self.status.market_data = "starting"
        self.store.attach(self)
        await self.alerts.start()
        self._tasks.append(asyncio.create_task(self.refresh_market(), name="market-refresh"))
        self._tasks.append(asyncio.create_task(self.prune_market_history(), name="market-prune"))

    async def refresh_market(self):
        last_rotation = float("-inf")
        while self._running:
            try:
                await self.market.refresh()
                assets = self.get_all_assets()
                self.status.market_data = "connected" if self.market_age() <= 20 else "stale"
                self.status.last_market_refresh = self.market.observed_at
                self.status.tracked_assets = len(assets)
                self.status.uptime_seconds = time.time() - self.status.started_at
                self.status.orderflow_engine = "connected" if any(
                    self.orderflow.hyperliquid_connected_at(asset.symbol) for asset in assets[:20]
                ) else "connecting"
                self.status.orderbook_feed = "connected" if not self.orderbook.is_stale() else "connecting"
                top = {asset.symbol for asset in assets[:20]}
                now = time.monotonic()
                needs_symbols = not top <= set(self.orderflow.symbols)
                needs_rotation = needs_symbols and now - last_rotation >= 60
                if top and self.status.market_data == "connected" and (
                    not self.orderflow.running or (needs_rotation and not self.fixed_universe)
                ):
                    await self.subscribe([asset.symbol for asset in assets[:30]])
                    last_rotation = now
            except Exception:
                self.status.market_data = "error"
                LOG.exception("Hyperliquid market update failed")
            await asyncio.sleep(self.market_refresh_interval)

    async def prune_market_history(self):
        while self._running:
            await asyncio.sleep(3600)
            try:
                await asyncio.to_thread(self.store.prune)
            except Exception:
                LOG.exception("HyperData market history pruning failed")

    async def subscribe(self, symbols):
        await self.orderflow.stop()
        await self.orderbook.stop()
        self.symbols = symbols
        self.orderflow = OrderFlowEngine(symbols, include_binance=False)
        self.orderflow.on_trade(self._handle_trade)
        self.orderbook = OrderBookEngine(symbols)
        await self.orderflow.start()
        await self.orderbook.start()

    def market_age(self):
        if self.demo:
            return time.time() - self.status.last_market_refresh
        return time.monotonic() - self.market.last_update if self.market.last_update else math.inf

    def market_timestamp(self):
        return (self.status.last_market_refresh if self.demo else self.market.observed_at) or None


def metric(value, source, updated_at, unit, quality="ok"):
    if isinstance(value, float) and not math.isfinite(value):
        value, quality = None, "unavailable"
    return {"value": value, "source": source, "updated_at": updated_at,
            "unit": unit, "quality": quality}


def capture(hub, asset, window, now, candles=None):
    market_at = hub.market_timestamp()
    market_quality = "synthetic" if hub.demo else ("ok" if hub.market_age() <= 20 else "stale")
    market_source = "HyperData demo" if hub.demo else "Hyperliquid metaAndAssetCtxs"
    observed = hub.market.observed_fields.get(asset.symbol, set())
    metrics = {}
    for name, unit in MARKET_FIELDS.items():
        value = getattr(asset, "mark_price" if name == "price" else name) if hub.demo or name in observed else None
        if name == "price" and value is not None and value <= 0:
            value = None
        metrics[name] = metric(value, market_source, market_at, unit,
                               market_quality if value is not None else "unavailable")

    engine = hub.orderflow
    connected_at = engine.hyperliquid_connected_at(asset.symbol)
    bucket = engine.buckets.get(asset.symbol, {}).get(window)
    recent = engine.recent_trades.get(asset.symbol)
    last_trade = max((trade.timestamp for trade in recent), default=None) if recent else None
    flow = bucket.get_snapshot(now) if bucket else None
    if hub.demo:
        flow_quality = "synthetic"
    elif not connected_at or not last_trade or not flow:
        flow_quality = "unavailable"
    elif last_trade > now + 5:
        flow_quality = "clock_skew"
    elif now - last_trade > min(WINDOWS[window] / 2, 60):
        flow_quality = "stale"
    elif flow.warming_up or now - connected_at < WINDOWS[window]:
        flow_quality = "warming_up"
    else:
        flow_quality = "ok"

    flow_source = "HyperData demo" if hub.demo else "Hyperliquid trades / HyperData Order Flow"
    for name, value, unit in (
        ("cvd", flow.cvd if flow else None, "USD"),
        ("ofi", flow.ofi if flow else None, "ratio"),
        ("volume", flow.buy_volume + flow.sell_volume if flow else None, "USD"),
        ("trade_count", flow.trade_count if flow else None, "trades"),
        ("trade_rate", flow.trades_per_sec if flow else None, "trades/s"),
    ):
        metrics[name] = metric(value if flow_quality != "unavailable" else None, flow_source,
                               last_trade, unit, flow_quality)
    seconds = WINDOWS[window]
    closed_at = candles["closed_at"] if candles else None
    candle_quality = "unavailable" if not candles else "ok" if now - closed_at <= seconds + 90 else "stale"
    for name, unit in (("natr", "%"), ("range_ratio", "×")):
        value = candles[name] if candles else None
        metrics[name] = metric(value, CANDLE_SOURCE, closed_at, unit,
                               candle_quality if value is not None else "unavailable")
    baseline = tape_baseline(candles, now, seconds) if candle_quality == "ok" else None
    relative = flow.trade_count / baseline if flow_quality == "ok" and baseline and flow else None
    metrics["activity_ratio"] = metric(
        relative, f"{flow_source} / {CANDLE_SOURCE} median", last_trade, "×",
        "ok" if relative is not None else "unavailable",
    )
    # These are observed trades from the same native window as OFI and CVD.
    # Price levels provide context for the independent analysis without a new indicator.
    if bucket and bucket.trades:
        first_trade, closing_trade = bucket.trades[0], bucket.trades[-1]
    else:
        first_trade = closing_trade = None
    for name, trade in (("first_trade_price", first_trade), ("last_trade_price", closing_trade)):
        metrics[name] = metric(trade.price if trade and flow_quality != "unavailable" else None,
                               flow_source, trade.timestamp if trade else None, "USD", flow_quality)
    book = hub.get_orderbook(asset.symbol)
    book_source = "HyperData demo" if hub.demo else "Hyperliquid l2Book / HyperData"
    if not book or not book.bids or not book.asks or not 0 < book.best_bid <= book.best_ask:
        book_quality = "unavailable"
    elif hub.demo:
        book_quality = "synthetic"
    else:
        book_quality = "ok" if now - book.timestamp <= 15 else "stale"
    for name, unit in (("spread", "USD"), ("imbalance", "ratio"),
                       ("best_bid", "USD"), ("best_ask", "USD")):
        metrics[name] = metric(getattr(book, name) if book_quality != "unavailable" else None,
                               book_source, book.timestamp if book else None, unit, book_quality)
    metrics["liquidations"] = metric(None, "Hyperliquid: no confirmed public liquidation feed",
                                      None, "events", "unavailable")
    direction = DIRECTIONS[flow.signal] if flow_quality == "ok" else "NOT_AVAILABLE"
    snapshot = {
        "symbol": asset.symbol, "exchange": "hyperliquid", "contract": f"{asset.symbol}-PERP",
        "window": window, "captured_at": now, "metrics": metrics,
    }
    vote = {
        "direction": direction, "native_signal": flow.signal if flow_quality == "ok" else None,
        "source": flow_source, "evidence": ["ofi"] if direction != "NOT_AVAILABLE" else [],
        "quality": flow_quality,
    }
    return snapshot, vote


def orderbook_details(hub, symbol):
    book = hub.get_orderbook(symbol)
    return asdict(book) if book else None

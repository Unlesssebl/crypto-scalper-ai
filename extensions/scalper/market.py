"""Hub-agnostic snapshot helpers used by the screener."""
import math
from dataclasses import asdict

from hyperdata_terminal.instruments import identity, is_binance

from .activity import BASELINE_BARS, CANDLE_SOURCE, tape_baseline

WINDOWS = {"1m": 60, "5m": 300}
DIRECTIONS = {
    "STRONG_BULL": "BULLISH", "BULLISH": "BULLISH", "NEUTRAL": "NEUTRAL",
    "BEARISH": "BEARISH", "STRONG_BEAR": "BEARISH",
}
MARKET_FIELDS = {
    "price": "USD mark", "volume_24h": "USD", "open_interest": "USD",
    "funding_rate": "fraction/hour",
}


def metric(value, source, updated_at, unit, quality="ok"):
    if isinstance(value, float) and not math.isfinite(value):
        value, quality = None, "unavailable"
    return {"value": value, "source": source, "updated_at": updated_at,
            "unit": unit, "quality": quality}


def capture(hub, asset, window, now, candles=None):
    venue, contract = identity(asset.symbol)
    binance = is_binance(asset.symbol)
    candle_source = "Binance USD-M klines" if binance else CANDLE_SOURCE
    market_at = getattr(asset, "observed_at", 0) or hub.market_timestamp()
    market_quality = "synthetic" if hub.demo else ("ok" if -5 <= now - market_at <= 20 else "stale")
    market_source = ("HyperData demo" if hub.demo else "Binance USD-M ticker/premiumIndex"
                     if binance else "Hyperliquid metaAndAssetCtxs")
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

    flow_source = ("HyperData demo" if hub.demo else "Binance aggTrade / HyperData Order Flow"
                   if binance else "Hyperliquid trades / HyperData Order Flow")
    for name, value, unit in (
        ("cvd", flow.cvd if flow else None, "USD"),
        ("ofi", flow.ofi if flow else None, "ratio"),
        ("volume", flow.buy_volume + flow.sell_volume if flow else None, "USD"),
        ("trade_count", flow.trade_count if flow else None, "aggregates" if binance else "trades"),
        ("trade_rate", flow.trades_per_sec if flow else None, "aggregates/s" if binance else "trades/s"),
    ):
        metrics[name] = metric(value if flow_quality != "unavailable" else None, flow_source,
                               last_trade, unit, flow_quality)
    seconds = WINDOWS[window]
    closed_at = candles["closed_at"] if candles else None
    candle_quality = "unavailable" if not candles else "ok" if now - closed_at <= seconds + 90 else "stale"
    for name, unit in (("natr", "%"), ("range_ratio", "×")):
        value = candles[name] if candles else None
        metrics[name] = metric(value, candle_source, closed_at, unit,
                               candle_quality if value is not None else "unavailable")
    baseline = tape_baseline(candles, now, seconds) if candle_quality == "ok" else None
    # Aggregate records cannot be divided by a kline's individual trade count.
    relative = flow.trade_count / baseline if not binance and flow_quality == "ok" and baseline and flow else None
    metrics["activity_ratio"] = metric(
        relative, f"{flow_source} / {candle_source} median", last_trade, "×",
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
    book_source = ("HyperData demo" if hub.demo else "Binance depth20 / HyperData"
                   if binance else "Hyperliquid l2Book / HyperData")
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
        "symbol": asset.symbol, "exchange": venue, "contract": contract if binance else f"{asset.symbol}-PERP",
        "window": window, "captured_at": now, "metrics": metrics,
    }
    if not binance and baseline is not None and baseline > 0:
        snapshot["activity_baseline"] = {
            "kind": "median_trade_count", "samples": BASELINE_BARS,
            "median_count": baseline, "window_seconds": seconds, "source": candle_source,
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

"""Batch snapshot used by external screeners.

`/v1/snapshot` is deliberately one request: a screener that polls 20 symbols
every 5 seconds would otherwise need 40+ requests per scan and hit the API
rate limit. The payload carries the same quality labels the terminal uses, so
a consumer can tell "no data" from "not loaded yet" without re-deriving
freshness from timestamps.
"""
from __future__ import annotations

import math
import time

from hyperdata_terminal.indicators import candle_indicators, tape_baseline

SNAPSHOT_WINDOWS = {"1m": 60, "5m": 300, "15m": 900}
CANDLE_INTERVAL_SECONDS = {"1m": 60, "5m": 300, "15m": 900}
MARKET_FIELDS = {"price", "volume_24h", "open_interest", "funding_rate"}
CANDLE_CACHE_TTL = 30.0
_MAX_SYMBOLS = 40
_CANDLE_CACHE: dict[tuple[str, str], tuple[float, dict | None]] = {}


def _finite(value):
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _market_age(hub, now: float) -> float:
    observed_at = getattr(hub.market, "observed_at", 0.0) or 0.0
    if observed_at:
        return max(0.0, now - observed_at)
    last_update = getattr(hub.market, "last_update", 0.0) or 0.0
    return float("inf") if not last_update else max(0.0, now - last_update)


def _market_quality(hub, now: float, age: float) -> str:
    if getattr(hub, "demo", False):
        return "synthetic"
    if not getattr(hub.market, "assets", None):
        return "unavailable"
    return "ok" if age <= 20 else "stale"


def _flow_quality(hub, flow, last_trade_at: float, connected_at: float,
                  now: float, window_seconds: int) -> str:
    if getattr(hub, "demo", False):
        return "synthetic"
    if flow is None or not connected_at or not last_trade_at:
        return "unavailable"
    if last_trade_at > now + 5:
        return "clock_skew"
    if now - last_trade_at > min(window_seconds / 2, 60):
        return "stale"
    if flow.warming_up or now - connected_at < window_seconds:
        return "warming_up"
    return "ok"


def _book_quality(hub, book, now: float) -> str:
    if not book or not book.bids or not book.asks or not 0 < book.best_bid <= book.best_ask:
        return "unavailable"
    if getattr(hub, "demo", False):
        return "synthetic"
    return "ok" if now - book.timestamp <= 15 else "stale"


def _candle_quality(hub, indicators, closed_at, now: float, seconds: int) -> str:
    if getattr(hub, "demo", False):
        return "unavailable"
    if not indicators or closed_at is None:
        return "unavailable"
    return "ok" if now - closed_at <= seconds + 90 else "stale"


def _market_row(asset, observed_fields: set[str], age: float, quality: str) -> dict:
    fields = {
        "price": asset.mark_price,
        "volume_24h": asset.volume_24h,
        "open_interest": asset.open_interest,
        "funding_rate": asset.funding_rate,
    }
    return {
        "price": _finite(fields["price"]) if "price" in observed_fields else None,
        "volume_24h": _finite(fields["volume_24h"]) if "volume_24h" in observed_fields else None,
        "open_interest": _finite(fields["open_interest"]) if "open_interest" in observed_fields else None,
        "funding_rate": _finite(fields["funding_rate"]) if "funding_rate" in observed_fields else None,
        "observed_fields": sorted(observed_fields),
        "quality": quality if observed_fields else "unavailable",
        "source": "hyperliquid_meta_and_asset_ctxs",
    }


def _flow_row(hub, symbol: str, window: str, now: float) -> dict:
    engine = hub.orderflow
    demo = getattr(hub, "demo", False)
    bucket = engine.buckets.get(symbol, {}).get(window)
    if demo:
        flow = bucket.get_snapshot(now) if bucket else None
        trades = bucket.window_trades(now) if bucket else ()
        connected_at = 0.0
        venue = "combined"
    else:
        flow = engine.get_venue_snapshot(symbol, window, "hyperliquid", now)
        trades = engine.venue_window_trades(symbol, window, "hyperliquid", now)
        connected_at = engine.hyperliquid_connected_at(symbol)
        venue = "hyperliquid"
        if flow is None:
            flow = bucket.get_snapshot(now) if bucket else None
            trades = bucket.window_trades(now) if bucket else ()
            venue = "combined"
    last_trade_at = trades[-1].timestamp if trades else 0.0
    quality = _flow_quality(hub, flow, last_trade_at, connected_at, now, SNAPSHOT_WINDOWS[window])
    return {
        "cvd": _finite(flow.cvd) if flow else None,
        "ofi": _finite(flow.ofi) if flow else None,
        "volume": _finite(flow.buy_volume + flow.sell_volume) if flow else None,
        "trade_count": flow.trade_count if flow else None,
        "trades_per_second": _finite(flow.trades_per_sec) if flow else None,
        "coverage": round(flow.coverage, 4) if flow else None,
        "warming_up": bool(flow.warming_up) if flow else None,
        "signal": flow.signal if flow else None,
        "first_trade_price": _finite(trades[0].price) if trades else None,
        "last_trade_price": _finite(trades[-1].price) if trades else None,
        "connected_at": connected_at or None,
        "last_trade_at": last_trade_at or None,
        "quality": quality,
        "venue": venue,
        "source": "hyperliquid_trades" if venue == "hyperliquid" else "hyperdata_demo",
    }


def _book_row(hub, symbol: str, now: float) -> dict:
    book = hub.get_orderbook(symbol)
    quality = _book_quality(hub, book, now)
    return {
        "spread": _finite(book.spread) if book and quality != "unavailable" else None,
        "imbalance": _finite(book.imbalance) if book and quality != "unavailable" else None,
        "best_bid": _finite(book.best_bid) if book and quality != "unavailable" else None,
        "best_ask": _finite(book.best_ask) if book and quality != "unavailable" else None,
        "timestamp": book.timestamp if book and quality != "unavailable" else None,
        "quality": quality,
        "source": "hyperliquid_l2book",
    }


def _candle_row(hub, symbol: str, window: str, now: float, indicators, cached: bool) -> dict:
    seconds = SNAPSHOT_WINDOWS[window]
    closed_at = indicators["closed_at"] if indicators else None
    quality = _candle_quality(hub, indicators, closed_at, now, seconds)
    baseline = tape_baseline(indicators, now, seconds) if indicators else None
    return {
        "natr": _finite(indicators["natr"]) if indicators else None,
        "range_ratio": _finite(indicators["range_ratio"]) if indicators else None,
        "tape_baseline": baseline,
        "closed_at": closed_at,
        "quality": quality,
        "source": "hyperliquid_candles",
        "cached": cached,
    }


async def _indicators(hub, symbol: str, window: str, now: float) -> tuple[dict | None, bool]:
    key = (symbol, window)
    cached = _CANDLE_CACHE.get(key)
    if cached and now - cached[0] < CANDLE_CACHE_TTL:
        return cached[1], True
    seconds = SNAPSHOT_WINDOWS[window]
    try:
        candles = await hub.market.get_candles(symbol, window, bars=50)
        indicators = candle_indicators(candles, seconds, now)
    except Exception:
        indicators = None
    _CANDLE_CACHE[key] = (now, indicators)
    if len(_CANDLE_CACHE) > 400:
        oldest = min(_CANDLE_CACHE, key=lambda item: _CANDLE_CACHE[item][0])
        _CANDLE_CACHE.pop(oldest, None)
    return indicators, False


async def build_snapshot(hub, symbols: list[str] | None = None,
                         windows: list[str] | None = None,
                         now: float | None = None,
                         include_candles: bool = True) -> dict:
    """Build the `/v1/snapshot` payload from a running hub."""
    now = now if now is not None else time.time()
    windows = list(dict.fromkeys(windows or ["1m", "5m"]))
    invalid = [window for window in windows if window not in SNAPSHOT_WINDOWS]
    if invalid:
        raise ValueError(f"unsupported windows: {', '.join(invalid)}")

    assets = hub.get_all_assets()
    by_symbol = {asset.symbol: asset for asset in assets}
    if symbols is None:
        symbols = [asset.symbol for asset in assets[:_MAX_SYMBOLS]]
    else:
        symbols = [symbol.strip().upper() for symbol in symbols if symbol.strip()]
        if not symbols:
            raise ValueError("symbols must not be empty")
        if len(symbols) > _MAX_SYMBOLS:
            raise ValueError(f"at most {_MAX_SYMBOLS} symbols per request")

    age = _market_age(hub, now)
    market_quality = _market_quality(hub, now, age)
    warnings: list[str] = []
    if not assets:
        warnings.append("market data not loaded yet (first ~5s)")

    rows = []
    for symbol in symbols:
        asset = by_symbol.get(symbol)
        if asset is None:
            warnings.append(f"{symbol}: not listed on Hyperliquid")
            continue
        observed = (MARKET_FIELDS if getattr(hub, "demo", False)
                    else getattr(hub.market, "observed_fields", {}).get(symbol, set()))
        for window in windows:
            indicators = None
            cached = False
            if include_candles and not getattr(hub, "demo", False):
                indicators, cached = await _indicators(hub, symbol, window, now)
            rows.append({
                "symbol": symbol,
                "window": window,
                "market": _market_row(asset, observed, age, market_quality),
                "flow": _flow_row(hub, symbol, window, now),
                "book": _book_row(hub, symbol, now),
                "candles": _candle_row(hub, symbol, window, now, indicators, cached),
            })

    return {
        "as_of": now,
        "mode": "demo" if getattr(hub, "demo", False) else "live",
        "market": {
            "age_seconds": None if not math.isfinite(age) else round(age, 3),
            "observed_at": getattr(hub.market, "observed_at", 0.0) or None,
            "quality": market_quality,
        },
        "warnings": warnings,
        "rows": rows,
    }


async def build_candles(hub, symbols: list[str], interval: str = "5m",
                        bars: int = 36) -> dict:
    """Raw closed candles for correlation / debugging."""
    if interval not in CANDLE_INTERVAL_SECONDS:
        raise ValueError(f"unsupported interval: {interval}")
    if not 2 <= bars <= 500:
        raise ValueError("bars must be between 2 and 500")
    if not symbols:
        raise ValueError("symbols must not be empty")
    result = []
    for symbol in symbols:
        candles = await hub.market.get_candles(symbol, interval, bars=bars)
        result.append({"symbol": symbol, "interval": interval, "candles": candles})
    return {"as_of": time.time(), "series": result}

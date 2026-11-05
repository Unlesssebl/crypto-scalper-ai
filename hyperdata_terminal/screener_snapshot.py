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
    # Demo data has no wall-clock observed_at; `last_update` is monotonic and
    # must never be mixed with `time.time()`.
    if getattr(hub, "demo", False):
        return 0.0
    observed_at = getattr(hub.market, "observed_at", 0.0) or 0.0
    return max(0.0, now - observed_at) if observed_at else float("inf")


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


def _context_block(hub, now: float) -> dict:
    """Market-wide context the screener surfaces alongside per-symbol rows.

    These come from hub sub-engines that only cover the majors or are global
    (L/S ratio, basis, IV, liquidations, whales, funding), so they belong in a
    single top-level block rather than being repeated on every symbol row.
    Missing sub-engines (older hub / demo) degrade to empty dicts, never raise.
    """
    context: dict[str, dict] = {}

    # L/S account ratio (BTC/ETH/SOL; venue crowd via ``source``).
    lsr = getattr(hub, "lsr", None)
    if lsr is not None:
        block = {}
        for symbol in ("BTC", "ETH", "SOL"):
            snap = lsr.get_latest(symbol)
            if snap:
                block[symbol] = {
                    "long_short_ratio": snap.long_short_ratio,
                    "long_ratio": snap.long_ratio, "short_ratio": snap.short_ratio,
                    "timestamp": snap.timestamp, "source": snap.source,
                }
        if block:
            context["lsr"] = block

    # Spot-perp basis (BTC/ETH/SOL).
    spot = getattr(hub, "spot", None)
    if spot is not None:
        block = {}
        for symbol in ("BTC", "ETH", "SOL"):
            snap = spot.get_latest(symbol)
            if snap:
                block[symbol] = {
                    "spot_price": snap.spot_price, "perp_price": snap.perp_price,
                    "basis_pct": snap.basis_pct, "timestamp": snap.timestamp,
                    "source": snap.source,
                }
        if block:
            context["basis"] = block

    # Deribit implied volatility (BTC/ETH).
    deribit = getattr(hub, "deribit", None)
    if deribit is not None:
        block = {}
        for underlying in ("BTC", "ETH"):
            snap = deribit.get_latest(underlying)
            if snap:
                block[underlying] = {
                    "mark_iv": snap.mark_iv, "index_price": snap.index_price,
                    "oi_usd": snap.oi_usd, "timestamp": snap.timestamp,
                }
        if block:
            context["iv"] = block

    # Confirmed liquidation census over the last hour (not a complete census;
    # ``coverage`` reports which exchange feeds are contributing).
    liquidations = getattr(hub, "liquidations", None)
    stats = None
    if liquidations is not None:
        try:
            stats = liquidations.get_stats(window_minutes=60, include_estimated=False)
        except Exception:
            stats = None
    if stats:
        context["liquidations"] = {
            "window_minutes": stats["window_minutes"],
            "confirmed_count": stats["confirmed_count"],
            "confirmed_volume_usd": stats["confirmed_volume_usd"],
            "long_count": stats["long_count"], "short_count": stats["short_count"],
            "by_symbol": dict(list(stats["by_symbol"].items())[:10]),
            "coverage": stats["coverage"],
        }

    # Whale book (largest tracked positions).
    whales = []
    if hasattr(hub, "get_whale_positions"):
        try:
            whales = hub.get_whale_positions(min_size_usd=250_000)[:20]
        except Exception:
            whales = []
    if whales:
        context["whales"] = [
            {"symbol": whale.symbol, "size_usd": whale.size_usd,
             "leverage": getattr(whale, "leverage", None),
             "distance_pct": whale.distance_pct}
            for whale in whales
        ]

    # Top extreme funding across venues (annualised %).
    extreme = []
    if hasattr(hub, "get_extreme_funding"):
        try:
            extreme = hub.get_extreme_funding(threshold_annualized=0.10)[:15]
        except Exception:
            extreme = []
    if extreme:
        context["extreme_funding"] = [
            {"symbol": asset.symbol,
             "annualized_pct": asset.funding_rate * 8760 * 100}
            for asset in extreme
        ]

    if context:
        context["as_of"] = now
    return context


def build_context(hub, now: float | None = None) -> dict:
    """Public wrapper for the market-context block (dashboard/TUI consumers)."""
    return _context_block(hub, now if now is not None else time.time())


def build_extra(hub, symbols: int = 6, heatmap_symbol: str = "BTC") -> dict:
    """Native panels payload for the in-process TUI (danger zone, whales...).

    Shapes match the REST endpoints so the same Rich builders serve both the
    in-process hub and `RemoteHub`.
    """
    from hyperdata_terminal.native_payloads import (
        heatmap_payload,
        hlp_payload,
        smart_money_payload,
    )

    flow = []
    for asset in hub.get_all_assets()[:symbols]:
        snapshot = hub.orderflow.get_snapshot(asset.symbol, "1m")
        if snapshot is None:
            continue
        flow.append({
            "symbol": asset.symbol, "window": "1m", "venue": "combined",
            "signal": snapshot.signal, "cvd": snapshot.cvd, "ofi": snapshot.ofi,
            "trades": snapshot.trade_count,
            "quality": "warming_up" if snapshot.warming_up else "ok",
        })
    try:
        liquidations = hub.liquidations.get_stats(window_minutes=60, include_estimated=False)
    except Exception:
        liquidations = None
    try:
        danger = {"positions": list(hub.positions.get_danger_zone(threshold_pct=5.0))}
    except Exception:
        danger = None
    try:
        whales = {"positions": list(hub.get_whale_positions(min_size_usd=250_000)[:10])}
    except Exception:
        whales = None
    try:
        hlp = hlp_payload(hub)
    except Exception:
        hlp = None
    try:
        heatmap = heatmap_payload(hub, heatmap_symbol, buckets=20, range_pct=10.0)
    except Exception:
        heatmap = None
    try:
        smart = smart_money_payload(hub)
    except Exception:
        smart = None
    return {
        "orderflow": flow, "liquidations": liquidations,
        "danger_zone": danger, "whales": whales,
        "hlp": hlp, "heatmap": heatmap, "smart_money": smart,
    }


def build_health(hub) -> dict:
    """In-process equivalent of the ``/v1/health`` subset the TUI renders.

    Keeps the same shape as ``RemoteHub.health_summary`` so both dashboard
    entry points can use one renderer.
    """
    monitor = getattr(hub, "health", None)
    result = monitor.latest() if monitor is not None else None
    overall = (result or {}).get("overall")
    status = getattr(hub, "status", None)
    feeds = {
        "liquidation_feed": getattr(status, "liquidation_feed", None),
        "orderflow_engine": getattr(status, "orderflow_engine", None),
        "orderbook_feed": getattr(status, "orderbook_feed", None),
        "market_data": getattr(status, "market_data", None),
        "hlp": getattr(status, "hlp_status", None),
    }
    venues = {}
    orderflow = getattr(hub, "orderflow", None)
    if orderflow is not None and hasattr(orderflow, "venue_freshness"):
        try:
            venues = orderflow.venue_freshness()
        except Exception:
            venues = {}
    return {
        "status": overall or getattr(status, "mode", None),
        "overall": overall,
        "feeds": {key: value for key, value in feeds.items() if value is not None},
        "orderflow_venues": venues,
        "failed_components": list(getattr(status, "failed_components", []) or []),
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
    # Hyperliquid has mixed-case tickers (kPEPE, kSHIB, ...). Match requests
    # case-insensitively but always use the exchange's own spelling downstream.
    by_symbol = {asset.symbol.upper(): asset for asset in assets}
    if symbols is None:
        requested = [asset.symbol for asset in assets[:_MAX_SYMBOLS]]
    else:
        requested = [symbol.strip() for symbol in symbols if symbol.strip()]
        if not requested:
            raise ValueError("symbols must not be empty")
        if len(requested) > _MAX_SYMBOLS:
            raise ValueError(f"at most {_MAX_SYMBOLS} symbols per request")

    age = _market_age(hub, now)
    market_quality = _market_quality(hub, now, age)
    warnings: list[str] = []
    if not assets:
        warnings.append("market data not loaded yet (first ~5s)")

    # Universe drift: the orderflow shard plan is fixed at hub start(), so a
    # newly listed (or volume-rotating) asset can enter the top of the book
    # without a Hyperliquid trades subscription. Surface that explicitly:
    # it needs a restart, not a hot fix.
    if not getattr(hub, "demo", False):
        subscribed = set(getattr(hub.orderflow, "symbols", ()))
        top_n = min(len(assets), 60)
        extra = {asset.symbol for asset in assets[:top_n]} - subscribed
        if extra:
            warnings.append(
                "universe drifted: top-volume symbols without an orderflow subscription — "
                + ", ".join(sorted(extra)) + " (restart to pick them up)"
            )

    rows = []
    for requested_symbol in requested:
        asset = by_symbol.get(requested_symbol.upper())
        if asset is None:
            warnings.append(f"{requested_symbol}: not listed on Hyperliquid")
            continue
        symbol = asset.symbol
        observed = (MARKET_FIELDS if getattr(hub, "demo", False)
                    else getattr(hub.market, "observed_fields", {}).get(symbol, set()))
        for window in windows:
            indicators = None
            cached = False
            if include_candles and not getattr(hub, "demo", False):
                indicators, cached = await _indicators(hub, symbol, window, now)
            flow_row = _flow_row(hub, symbol, window, now)
            if flow_row["quality"] == "unavailable" and not getattr(hub, "demo", False):
                warnings.append(f"{symbol}: no Hyperliquid orderflow subscription")
            rows.append({
                "symbol": symbol,
                "window": window,
                "market": _market_row(asset, observed, age, market_quality),
                "flow": flow_row,
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
        "context": _context_block(hub, now),
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
    by_symbol = {asset.symbol.upper(): asset.symbol for asset in hub.get_all_assets()}
    result = []
    for symbol in symbols:
        resolved = by_symbol.get(symbol.upper(), symbol)
        candles = await hub.market.get_candles(resolved, interval, bars=bars)
        result.append({"symbol": resolved, "interval": interval, "candles": candles})
    return {"as_of": time.time(), "series": result}

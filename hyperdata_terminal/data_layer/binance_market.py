"""Verified USDT-M universe and public snapshots, independent of HL listings."""

import asyncio
import math
import time

from hyperdata_terminal.instruments import binance_key, identity
from hyperdata_terminal.market_pulse.collectors.binance import REST, binance_spec


async def get_json(session, path, **params):
    import aiohttp

    async with session.get(REST + path, params=params, timeout=aiohttp.ClientTimeout(total=12)) as response:
        response.raise_for_status()
        return await response.json()


async def refresh(owner):
    from .market_data import AssetInfo

    async with owner._managed_session() as session:
        now = time.time()
        if now >= getattr(owner, "_binance_catalog_until", 0):
            body = await get_json(session, "/fapi/v1/exchangeInfo")
            catalogue = {}
            for row in body["symbols"]:
                try:
                    spec = binance_spec(row)
                except ValueError:
                    continue
                catalogue[spec.native_market_id] = spec
            if not catalogue:
                raise ValueError("no verified Binance USDT-M perpetuals")
            owner._binance_catalog, owner._binance_catalog_until = catalogue, now + 600
        tickers, premiums = await asyncio.gather(get_json(session, "/fapi/v1/ticker/24hr"),
                                                get_json(session, "/fapi/v1/premiumIndex"))
    marks = {p["symbol"]: p for p in premiums}
    assets = {}
    for row in tickers:
        native = row.get("symbol")
        if native not in owner._binance_catalog or native not in marks:
            continue
        p = marks[native]
        try:
            price, mark, index, volume, change, observed = (float(v) for v in
                (row["lastPrice"], p["markPrice"], p["indexPrice"], row["quoteVolume"],
                 row["priceChangePercent"], p["time"] / 1000))
            if not all(math.isfinite(v) for v in (price, mark, index, volume, change, observed)):
                continue
            if min(price, mark, index) <= 0 or volume < 0 or not -5 <= now - observed <= 20:
                continue
        except (ValueError, TypeError, KeyError):
            continue
        key = binance_key(native)
        assets[key] = AssetInfo(symbol=key, price=price, mark_price=mark, index_price=index, volume_24h=volume,
                                price_change_24h_pct=change / 100, funding_rate=0, open_interest=0,
                                premium_pct=(mark / index - 1) * 100, observed_at=observed)
    if not assets:
        raise ValueError("no fresh Binance market snapshots")
    owner.assets = {s: a for s, a in owner.assets.items() if not s.startswith("BINANCE:")} | assets
    for s in list(owner.observed_fields):
        if s.startswith("BINANCE:"):
            owner.observed_fields.pop(s)
    owner.observed_fields.update({s: {"price", "volume_24h"} for s in assets})
    owner.observed_at = max(owner.observed_at, max(a.observed_at for a in assets.values()))


async def candles(owner, symbol, timeframe, bars):
    async with owner._managed_session() as session:
        rows = await get_json(session, "/fapi/v1/klines", symbol=identity(symbol)[1], interval=timeframe,
                              limit=min(1000, max(1, bars)))
    return [{"timestamp": r[0], "open": float(r[1]), "high": float(r[2]), "low": float(r[3]),
             "close": float(r[4]), "volume": float(r[5]), "trade_count": int(r[8])}
            for r in rows]

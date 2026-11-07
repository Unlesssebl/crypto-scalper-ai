"""Closed-candle volatility and relative tape activity."""
import asyncio
import logging
import math
import time

from hyperdata_terminal.indicators import (
    BASELINE_BARS,
    BTC_BAR_SECONDS,
    BTC_RETURN_SAMPLES,
    CANDLE_SOURCE,
    btc_context,
    candle_indicators,
    tape_baseline,
)

from .profile import fresh

LOG = logging.getLogger(__name__)
BURST_RATIO = 2.0
MIN_BURST_TRADES = 10
WIDE_SPREAD_ATR_FRACTION = 0.25
MIN_STRONG_MOVE_PCT = 0.10
MIN_LARGE_MOVE_PCT = 0.20
SPREAD_MULTIPLE = 4
FINDING_LEVELS = (
    ("⚪", "Нет свежих данных"),
    ("🔵", "Фоновая"),
    ("🟡", "Заметная"),
    ("🟠", "Выраженное движение"),
    ("🔴", "Сильное движение"),
)

__all__ = [
    "BASELINE_BARS", "BTC_BAR_SECONDS", "BTC_RETURN_SAMPLES", "BURST_RATIO",
    "CANDLE_SOURCE", "FINDING_LEVELS", "MIN_BURST_TRADES", "MIN_LARGE_MOVE_PCT",
    "MIN_STRONG_MOVE_PCT", "SPREAD_MULTIPLE", "WIDE_SPREAD_ATR_FRACTION",
    "active_anomalies", "btc_context", "candle_indicators", "finding_strength",
    "market_risk_context", "refresh_candles", "tape_baseline", "wide_spread",
]


def market_risk_context(context):
    """Human-readable risk-appetite notes from the snapshot's market context.

    `context` is the ``/v1/snapshot`` ``context`` block (L/S, basis, IV,
    extreme funding). Returns a list of short strings for the dashboard and
    notification facts; empty when the block is absent. Pure and side-effect
    free so the fork (which passes None) degrades gracefully.
    """
    if not context:
        return []
    notes = []
    if lsr := context.get("lsr", {}).get("BTC"):
        ratio = lsr.get("long_short_ratio")
        if ratio is not None:
            if ratio >= 1.5:
                notes.append(f"L/S BTC {ratio:.2f} — перекос в лонг ({lsr.get('source', '')})")
            elif ratio <= 0.7:
                notes.append(f"L/S BTC {ratio:.2f} — перекос в шорт ({lsr.get('source', '')})")
    if iv := context.get("iv", {}).get("BTC"):
        mark = iv.get("mark_iv")
        if mark is not None and mark >= 60:
            notes.append(f"IV BTC {mark:.0f}% — повышенная подразумеваемая волатильность")
    if basis := context.get("basis", {}).get("BTC"):
        bp = basis.get("basis_pct")
        if bp is not None and abs(bp) >= 0.5:
            notes.append(f"Базис BTC {bp:+.2f}% ({basis.get('source', '')})")
    if liquidations := context.get("liquidations"):
        count = liquidations.get("confirmed_count")
        if count and count >= 5:
            notes.append(f"Ликвидации за час: {count} ({int(liquidations.get('confirmed_volume_usd', 0)):,} USD)")
    return notes


def active_anomalies(snapshot, *, tape_ratio=BURST_RATIO):
    metrics = snapshot["metrics"]
    ratio = metrics.get("activity_ratio", {})
    trades = metrics.get("trade_count", {})
    volatility = metrics.get("range_ratio", {})
    active = set()
    if ratio.get("quality") == "ok" and ratio["value"] >= tape_ratio and trades["value"] >= MIN_BURST_TRADES:
        active.add("tape_burst")
    if volatility.get("quality") == "ok" and volatility["value"] >= BURST_RATIO:
        active.add("range_burst")
    return frozenset(active)


def wide_spread(snapshot):
    metrics = snapshot["metrics"]
    spread, mark, natr = (fresh(metrics, key) for key in ("spread", "price", "natr"))
    return (spread is not None and mark is not None and mark > 0 and natr is not None and natr > 0
            and 100 * spread / mark >= WIDE_SPREAD_ATR_FRACTION * natr)


def finding_strength(snapshot, native, *, tape_ratio=BURST_RATIO, price_floor=MIN_STRONG_MOVE_PCT,
                     price_natr_multiple=1.0):
    metrics = snapshot["metrics"]
    anomalies = active_anomalies(snapshot, tape_ratio=tape_ratio)
    reasons = []
    if "tape_burst" in anomalies:
        reasons.append("Ускорение ленты")
    if "range_burst" in anomalies:
        reasons.append("Всплеск диапазона")

    if fresh(metrics, "price") is None or fresh(metrics, "ofi") is None:
        return {"rank": 0, "icon": FINDING_LEVELS[0][0], "label": FINDING_LEVELS[0][1],
                "reasons": ["Нужны свежая цена и лента", *reasons]}

    start, end, natr = (fresh(metrics, key) for key in ("first_trade_price", "last_trade_price", "natr"))
    direction = native["direction"]
    change = 100 * (end / start - 1) if start and end is not None else 0
    spread, mark = (fresh(metrics, key) for key in ("spread", "price"))
    spread_pct = 100 * spread / mark if spread is not None and mark and mark > 0 else None
    aligned = direction in {"BULLISH", "BEARISH"} and change != 0 and (change > 0) == (direction == "BULLISH")
    confirms = (aligned and natr is not None and natr > 0 and spread_pct is not None
                and abs(change) >= max(price_floor, price_natr_multiple * natr, SPREAD_MULTIPLE * spread_pct))
    if confirms:
        reasons.append(f"Цена уже прошла {abs(change):.2f}% в сторону потока")
    strong_flow = native["native_signal"] in {"STRONG_BULL", "STRONG_BEAR"}
    rank = 2 if anomalies or (strong_flow and confirms) else 1
    if confirms and (anomalies or strong_flow):
        rank = 3
        if len(anomalies) == 2 and abs(change) >= max(MIN_LARGE_MOVE_PCT, 1.5 * natr):
            rank = 4
    elif anomalies:
        reasons.append("Всплеск без подтверждённого движения цены")

    if fresh(metrics, "spread") is None:
        rank = min(rank, 2)
        reasons.append("Нет свежего стакана")
    elif wide_spread(snapshot):
        rank = min(rank, 2)
        reasons.append("Широкий спред ограничивает приоритет")
    icon, label = FINDING_LEVELS[rank]
    return {"rank": rank, "icon": icon, "label": label, "reasons": reasons}


def observation_assets(hub):
    """Apply the existing adaptive plan; never allocate a second universe."""
    assets = hub.get_all_assets()
    selector = getattr(hub, "attention_selector", None)
    plan = getattr(selector, "plan", {})
    if plan.get("schema_version") == "attention.v1" and plan.get("generated_at_ms"):
        selected = set(plan.get("selected", []))
        return [a for a in assets if a.symbol in selected]
    if getattr(hub, "attention", {}).get("context_only"):
        return []
    active = {r["symbol"] for r in getattr(hub, "attention", {}).get("active", [])}
    return [a for i, a in enumerate(assets) if i < 20 or a.symbol in active]


async def refresh_candles(hub, windows, cache):
    due = {}
    while True:
        assets = observation_assets(hub)
        active_symbols = {r["symbol"] for r in getattr(hub, "attention", {}).get("active", [])}
        assets.sort(key=lambda a: a.symbol not in active_symbols)
        keys = [(asset.symbol, window) for asset in assets for window in windows]
        active = set(keys)
        for key in due.keys() - active:
            del due[key]
            cache.pop(key, None)
        for key in sorted(keys, key=lambda key: due.get(key, 0)):
            now = time.time()
            if due.get(key, 0) > now:
                continue
            symbol, window = key
            seconds = windows[window]
            try:
                candles = await hub.market.get_candles(symbol, window, bars=50)
                indicators = candle_indicators(candles, seconds, time.time())
                if indicators:
                    cache[key] = indicators
            except Exception as exc:
                LOG.debug("Candle refresh failed for %s/%s: %s", symbol, window, type(exc).__name__)
            due[key] = max(now + 30, (math.floor(now / seconds) + 1) * seconds + 2)
            # Keep candle requests below the venue's shared REST weight limit.
            await asyncio.sleep(2)
        await asyncio.sleep(2)

"""Screener signals for a trading terminal's chart (the TigerTrade indicator pilot).

The indicator asks with the chart's own instrument (Tiger `Symbol.Code` + `Exchange`, e.g. BTCUSDT on
BINANCE-FUT) and gets back the cards this screener sent for that asset, plus the live reading if the
asset is in attention right now. Text is the card's own text without HTML, so the chart and Telegram
say the same thing. Read-only: nothing here changes screener state.
"""

import html
import json
import re
import time

from .context_auto import summarize
from .fact_packet import packet_ready
from .signal_outcomes import SignalOutcomes
from .telegram_ux import number, operational, usd

EPISODE_FOLD_SECONDS = 600  # cards of one episode within 10 minutes share one chart marker

TAG = re.compile(r"<[^>]+>")
QUOTES = ("USDT", "USDC", "BUSD", "USD")


def plain(line):
    return html.unescape(TAG.sub("", line)).strip()


def screener_symbols(code, exchange=""):
    """BTCUSDT / BTCUSDT.P / TigerTrade's BTCUSDT_FUT_BINANCE-FUT → BINANCE:BTC first, then the Hyperliquid BTC."""
    base = (code or "").upper().split(":")[-1].split("_")[0].split(".")[0]
    for quote in QUOTES:
        if base.endswith(quote) and len(base) > len(quote):
            base = base[: -len(quote)]
            break
    if not re.fullmatch(r"[A-Z0-9]{1,20}", base):
        return []
    binance, hyperliquid = f"BINANCE:{base}", base
    return [hyperliquid, binance] if exchange.upper().startswith("HYPER") else [binance, hyperliquid]


def card_lines(text):
    return [plain(line) for line in text.split("\n") if plain(line)]


def tape_of(state):
    """Numbers behind the card's effort-vs-result lines, for the chart to draw instead of read."""
    reading = state.get("tape")
    if not reading:
        return None
    up = reading["side"] > 0
    recent, prior = reading["recent"], reading["prior"]
    buys, sells = (recent["aggressor_usd"], recent["opposite_usd"]) if up else (
        recent["opposite_usd"], recent["aggressor_usd"])
    return dict(
        kind=reading["kind"],
        impulse_side=reading["side"],
        buys_usd=buys,
        sells_usd=sells,
        recent_seconds=recent["seconds"],
        recent_move_bps=recent["move_bps"],
        prior_seconds=prior["seconds"],
        prior_move_bps=prior["move_bps"],
    )


def zone_of(at, zone):
    if not zone:
        return None
    age, eat = zone.get("observed_age_seconds"), zone.get("size_to_observed_turnover")
    return dict(
        side=zone["side"],
        price=zone["price"],
        notional_usd=zone.get("notional_usd"),
        first_seen=int(at * 1000 - age * 1000) if isinstance(age, (int, float)) else None,
        eat_seconds=eat * 60 if isinstance(eat, (int, float)) else None,
    )


def eat_words(ratio):
    """Zone notional / last-60s turnover, said as time of trading at the current pace plus what that means."""
    seconds = ratio * 60
    took = ("<1с" if seconds < 1 else f"~{seconds:.0f}с" if seconds < 120
            else f"~{seconds / 60:.0f} мин" if seconds < 600 else "10+ мин")
    size = "тонкая" if seconds < 30 else "заметная" if seconds < 120 else "крупная"
    return f"Чтобы её съесть, нужно {took} сделок в нынешнем темпе — {size}"


def zone_lines(state):
    """The card's terse 🧱 line, spelled out for a hover tooltip where there is room to say what the band is."""
    zone, mid = state.get("zone"), state.get("midpoint")
    if not zone:
        return []
    bid = zone["side"] == "bid"
    wall = (state.get("tape") or {}).get("wall")
    head = f"🧱 {'Зелёная' if bid else 'Красная'} полоса — заявки на {'покупку' if bid else 'продажу'}"
    if wall and wall["price"] == zone["price"]:
        head += " на пути цены"
    head += f": {number(zone['price'], 8)}"
    if mid:
        head += f" ({(zone['price'] / mid - 1) * 100:+.2f}% от цены)"
    if zone.get("notional_usd"):
        head += f" на {usd(zone['notional_usd'])}"
    lines = [head]
    if zone.get("size_to_observed_turnover") is not None:
        lines.append(eat_words(zone["size_to_observed_turnover"]))
    age = zone.get("observed_age_seconds")
    if age is not None and age < 10:
        lines.append("Появилась меньше 10с назад — может исчезнуть")
    return lines


def mark_of(at, symbol, reason, state, text):
    lines = card_lines(text)
    explained = zone_lines(state)
    if explained:
        at_zone = next((i for i, line in enumerate(lines) if line.startswith("🧱")), None)
        if at_zone is not None:
            lines[at_zone:at_zone + 1] = explained
    header = lines[0] if lines else ""
    return dict(
        at=int(at * 1000),
        symbol=symbol,
        venue="Binance" if symbol.startswith("BINANCE:") else "Hyperliquid",
        reason=reason,
        price=state.get("midpoint"),
        side=1 if "▲" in header else -1 if "▼" in header else 0,
        strength=state.get("strength"),
        tier=state.get("tier"),
        route=state.get("route"),
        lines=lines[:-1] if len(lines) > 1 else lines,  # the muted source/time footer is the chart's own axis
        zone=zone_of(at, state.get("zone")),
        tape=tape_of(state),
    )


def chart_marks(service, code, exchange, minutes, now):
    symbols = screener_symbols(code, exchange)
    out = dict(code=code, exchange=exchange, symbols=symbols, now=int(now * 1000), marks=[], live=None,
               summary=None)
    if not symbols:
        return out
    out["summary"] = SignalOutcomes(service.journal.db).summary(symbols, now)
    minutes = max(1, min(int(minutes), 24 * 60))
    marks = []
    rows = service.journal.db.execute(
        "SELECT e.at,e.reason,e.state,e.packet,p.symbol,e.channel,e.episode FROM telegram_ux_events e "
        "JOIN fact_packets p ON p.id=e.packet WHERE e.status='queued' AND p.symbol IN (?,?) AND e.at>=? "
        "ORDER BY e.at",
        (symbols[0], symbols[-1], now - minutes * 60),
    ).fetchall()
    primary = {}  # episode -> its first signal mark on the chart
    for at, reason, state_json, packet_id, symbol, channel, episode in rows:
        packet = service.consistency.packet(packet_id)
        if packet is None:
            continue
        state = json.loads(state_json)
        if channel == "levels":
            # A level is a band on the chart, not a signal: no marker, caption or outcome of its own.
            level = state.get("level") or {}
            zone = level.get("zone") or state.get("zone")
            if zone:
                marks.append(dict(at=int(at * 1000), symbol=symbol, kind="level", price=state.get("midpoint"),
                                  side=0, lines=[], zone=zone_of(at, zone), reason=reason))
            continue
        kind = "strengthening" if reason in {"strengthened", "new_level"} else "situation"
        text, _ = operational(packet, state, reason, kind, state.get("ratio"), state.get("pulse"))
        mark = mark_of(at, symbol, reason, state, text)
        first = primary.get(episode)
        if first is not None and at * 1000 - first["at"] <= EPISODE_FOLD_SECONDS * 1000:
            # Later cards of one episode fold into its first marker: the hover lists them, the chart shows one.
            verdict = mark["lines"][1] if len(mark["lines"]) > 1 else mark["lines"][0] if mark["lines"] else ""
            stamp = time.strftime("%H:%M:%S", time.localtime(at))
            first["lines"].append(f"↻ {stamp} · {verdict}")
            if mark.get("zone"):
                first["zone"] = mark["zone"]
            continue
        mark["kind"] = "signal"
        primary[episode] = mark
        marks.append(mark)
    out["marks"] = marks
    for symbol in symbols:
        packet = (getattr(service, "context_packets", None) or {}).get(symbol)
        if packet and packet_ready(packet, now):
            state = summarize(packet)
            text, _ = operational(packet, state, "live", "situation")
            out["live"] = mark_of(packet["captured_at"], symbol, "live", state, text)
            break
    return out

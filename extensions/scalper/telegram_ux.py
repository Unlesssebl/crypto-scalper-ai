"""Telegram product layer: deterministic events, quiet watchlist and separate analysis."""

import html
import json
import logging
import math
import os
import secrets
import time
from datetime import datetime, timedelta, timezone

from .analytics_context import admission, fact_value
from .bots.triggers import level_break, level_hold, tape_reading
from .context_auto import decide, summarize
from .desktop import DesktopNotifier
from .fact_packet import checksum, packet_ready
from .levels import LevelWatcher, zones_of
from .liquidation_context import liquidation_line
from .microstructure import NOTABLE
from .microstructure import label as microstructure_label
from .notification_quality import NotificationQuality
from .phase import PHASES
from .signal_outcomes import SignalOutcomes
from .tiger import TigerBridge

LOG = logging.getLogger(__name__)
MSK = timezone(timedelta(hours=3))
TITLES = {
    "interesting_instrument": "Кандидат для наблюдения",
    "active_instrument": "Инструмент активен",
    "interesting_level": "Необычная зона в стакане",
    "formation_confirmed": "Движение подтвердилось",
    "density_context": "Удерживается значимая зона",
    "flow_price_conflict": "Поток расходится с реакцией цены",
    "early_context": "Ускорение подтверждается движением цены",
    "mover_selected": "Ликвидное движение",
}


def number(value, digits=2):
    if digits == 8:
        return f"{value:.10g}"
    return format(value, f".{digits}f").rstrip("0").rstrip(".") if digits else f"{value:g}"


def value(packet, key):
    fact = next((f for f in packet["facts"] if f["id"] == key), {})
    v = fact.get("value")
    return v if fact.get("quality") == "ok" and type(v) in (int, float) and math.isfinite(v) else None


def scale(packet, state):
    """A five-minute flow is context only when the observed endpoints span most of it."""
    facts = {f["id"]: f for f in packet["facts"]}
    if packet.get("context_version"):
        return fact_value(packet, "formation.scale") or "не определён"
    for window, minimum in (("5m", 240), ("1m", 45)):
        first = facts.get(f"frame.{window}.first_trade_price", {})
        last = facts.get(f"frame.{window}.last_trade_price", {})
        a, b = first.get("source_time"), last.get("source_time")
        flow = value(packet, f"frame.{window}.ofi")
        if (
            first.get("quality") == last.get("quality") == "ok"
            and type(a) in (int, float)
            and type(b) in (int, float)
            and b - a >= minimum
            and flow is not None
            and abs(flow) >= 0.6
            and state.get("flow") is not None
            and flow * state["flow"] > 0
        ):
            return window
    return "не определён"


def activity(service, symbol, now):
    """Trade-count ratio of the last closed minute (or the one before, while the newest is settling).

    Prints, not base volume: one large print in a dead market multiplies volume, not activity.
    """
    candle = getattr(service.attention, "activity", {}).get(symbol, {})
    target = int(now * 1000) // 60000 * 60000 - 60000
    if candle.get("status") != "ready" or candle.get("start_ms") not in (target, target - 60000):
        return None
    ratio = candle.get("ratios", {}).get("trade_count")
    return ratio if type(ratio) in (int, float) and math.isfinite(ratio) and ratio >= 0 else None


def scenarios(state):
    plan = state.get("scenario") or {}
    if plan.get("kind") in {"observed_zone", "observed_impulse"}:
        anchor = number(plan["anchor"], 8)
        main = "выше" if plan["main_relation"] == "above" else "ниже"
        cancel = "ниже" if plan["cancel_relation"] == "below" else "выше"
        source = "наблюдаемой зоны" if plan["kind"] == "observed_zone" else "наблюдаемого начала импульса"
        return [
            f"Основной: цена остаётся {main} {anchor} — цены {source}. "
            "Подтверждение: два последовательных пригодных наблюдения при сохранении потока/видимой зоны.",
            f"Альтернативный: два наблюдения {cancel} {anchor} ослабляют основной сценарий. "
            "Исчезновение зоны из видимости или разрыв данных прекращают её подтверждение, "
            "но не доказывают исполнение или отмену заявок.",
        ]
    zone = state.get("zone")
    if zone:
        side = "спроса" if zone["side"] == "bid" else "предложения"
        return [
            f"Основной: зона {side} остаётся видимой; повторные исполнения сопровождаются удержанием цены.",
            "Альтернативный: зона уменьшается или исчезает, цена проходит её. "
            "Это ослабляет сценарий удержания; исчезновение само по себе не доказывает исполнение.",
        ]
    if state.get("relation") in {"stalled", "opposed"}:
        return [
            "Основной: поток сохраняется, но цена не следует за ним — наблюдаем устойчивость расхождения.",
            "Альтернативный: цена начинает двигаться по потоку либо поток меняет сторону — "
            "прежнее расхождение теряет основание.",
        ]
    if state.get("flow") is None or state.get("count") is None:
        return [
            "Основной и альтернативный сценарии не определены: пригодного потока пока недостаточно.",
            "Следующее наблюдение: дождаться прогретого потока и сопоставить его с реакцией цены.",
        ]
    return [
        "Основной: активность сохраняется и цена продолжает реагировать в сторону потока.",
        "Альтернативный: активность затухает или реакция цены расходится с потоком — сценарий ускорения ослабевает.",
    ]


# Below this many prints a buy/sell share is noise, not a side of the flow.
MIN_FLOW_TRADES = 10
# A share this far from 50% is shown as a side (▲/▼); closer is "balanced".
SIDE_SHARE = 0.2
DISCOVERY_TITLES = {
    "potential": "аномалия активности",
    "active": "активный поток сделок",
    "level": "зона в стакане",
}
WATCH_REASONS = {
    "interesting_instrument": "аномалия активности",
    "active_instrument": "активный поток",
    "interesting_level": "зона в стакане",
    "formation_confirmed": "движение подтвердилось",
    "flow_price_conflict": "поток против цены",
    "early_context": "ранний импульс",
    "density_context": "значимая зона",
}
# Phases worth a line on the card; "unknown"/"not observed" are omitted, not printed.
CARD_PHASES = {"early", "confirmed", "extended", "weakening"}


def small(x, digits=1):
    """Keeps tiny non-zero values visible: 0.04 б.п. must not print as 0."""
    return number(x, digits) if x == 0 or abs(x) >= 10 ** -digits else f"{x:.1g}"


def usd(amount):
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "k")):
        if amount >= div:
            scaled = amount / div
            return "$" + (number(scaled, 1) if scaled < 100 else f"{scaled:.0f}") + suffix
    return f"${amount:.0f}"


def flow_side(state):
    """Returns (arrow, line) for the tape, or (None, line|None) when the sample is too thin."""
    flow, count = state.get("flow"), state.get("count")
    if flow is None or count is None or abs(flow) > 1:
        return None, None
    window = "1м" if state.get("window") == "1m" else "≤60с"
    if count < MIN_FLOW_TRADES:
        return None, f"Сделок мало ({int(count)} за {window}) · сторона не определена"
    buys = (flow + 1) * 50
    if flow >= SIDE_SHARE:
        arrow, side = "▲", f"покупки {buys:.0f}%"
    elif flow <= -SIDE_SHARE:
        arrow, side = "▼", f"продажи {100 - buys:.0f}%"
    else:
        arrow, side = None, f"баланс · покупки {buys:.0f}%"
    return arrow, f"{arrow + ' ' if arrow else ''}{side} · {int(count)} сделок за {window}"


# Discovery push gate: initial engineering thresholds, not proven trading edges.
PUSH_MIN_TRADES = 20
PUSH_MIN_ACTIVITY = 3
# The zone criterion is relative to the instrument (no dollar floor): large against this book's usual
# level, near the price for this instrument's volatility, eaten in seconds-to-minutes at its own pace.
PUSH_ZONE_MIN_RELATIVE = 6
PUSH_ZONE_NEAR_NATR = 2.0
PUSH_ZONE_NEAR_FALLBACK_BPS = 50
PUSH_ZONE_EAT_SECONDS = (10, 300)


def discovery_gate(state):
    """None when a discovery candidate deserves its own push; otherwise why it stays in the watch list.

    A push needs two of three: a sided tape, activity well above the norm, a tradable zone
    (large for this book, near the price for this volatility, eaten in seconds-to-minutes at this pace).
    """
    if state.get("wide_spread"):
        return "discovery_wide_spread"
    flow, count = state.get("flow"), state.get("count") or 0
    tape = flow is not None and abs(flow) >= SIDE_SHARE and count >= PUSH_MIN_TRADES
    active = any(
        type(state.get(k)) in (int, float) and state[k] >= PUSH_MIN_ACTIVITY for k in ("ratio", "rolling_activity")
    )
    zone = state.get("zone") or {}
    eat = zone.get("size_to_observed_turnover")
    natr = state.get("natr_1m")
    near = PUSH_ZONE_NEAR_NATR * natr * 100 if natr else PUSH_ZONE_NEAR_FALLBACK_BPS
    tradable_zone = bool(
        (zone.get("relative_size") or 0) >= PUSH_ZONE_MIN_RELATIVE
        and zone.get("distance_bps") is not None
        and abs(zone["distance_bps"]) <= near
        and eat is not None
        and PUSH_ZONE_EAT_SECONDS[0] <= eat * 60 <= PUSH_ZONE_EAT_SECONDS[1]
    )
    return None if tape + active + tradable_zone >= 2 else "discovery_quiet"


# Liquidity tiers by 24h quote volume; the 60s turnover is the fallback when 24h is unknown.
TIERS_24H = ((1, 200e6), (2, 20e6), (3, 2e6))
TIERS_60S = ((1, 140e3), (2, 14e3), (3, 1.4e3))  # the same floors spread evenly over a day


def liquidity_tier(volume_24h, turnover_60s):
    for source, amount, floors in (("24h", volume_24h, TIERS_24H), ("60s", turnover_60s, TIERS_60S)):
        if type(amount) in (int, float) and math.isfinite(amount) and amount > 0:
            return next((tier for tier, floor in floors if amount >= floor), 4), source
    return 4, "unknown"  # unknown liquidity never interrupts


# A tape reading is "strong" only when the impulse it reads is at least this share of a usual 1m range:
# a +0.11% push and a -0.07% pullback inside a 0.3% minute are noise however clean the 15s split looks.
STRONG_IMPULSE_NATR = 0.5
STRONG_IMPULSE_FALLBACK_BPS = 40  # when the 1m volatility is unknown: an impulse of at least 0.4%


def signal_strength(state, discovery):
    """strong / notable / background — one scale for every kind of event."""
    reading = state.get("tape")
    natr = state.get("natr_1m")
    if reading and reading["kind"] in NOTABLE:
        floor = STRONG_IMPULSE_NATR * natr * 100 if natr else STRONG_IMPULSE_FALLBACK_BPS
        if abs(reading["prior"]["move_bps"]) >= floor:
            return "strong"
        return "notable"  # a real reading, but too small to interrupt with sound
    rank = state.get("formation_strength") or state.get("rank") or 0
    if state.get("phase") == "confirmed" and rank >= 3:
        return "strong"
    if discovery:
        return "notable" if discovery_gate(state) is None else "background"
    return "notable"  # core signals here already passed admission and the activity check


def route(tier, strength):
    """alert: Telegram with sound + Windows toast; silent: Telegram without sound; watch: quiet list."""
    if tier >= 4 or strength == "background":
        return "watch"
    if strength == "strong":
        return "alert" if tier <= 2 else "silent"
    return "silent" if tier <= 2 else "watch"


TIGER_BUTTON_SECONDS = 1800  # the TigerTrade button works for 30 minutes after the card
STRENGTH_MARKS = {"strong": "🔥", "notable": "🔸"}


def bps(x):
    if abs(x) < 0.05:
        return "0 б.п."
    return f"{x:+.1f} б.п." if abs(x) < 10 else f"{x:+.0f} б.п."


def pct(basis_points):
    """Signed percent from basis points: +1.76%, -0.48%, 0%."""
    value = basis_points / 100
    if abs(value) < 0.005:
        return "0%"
    return f"{value:+.2f}%" if abs(value) < 10 else f"{value:+.1f}%"


def fold(x):
    """'в 2.6 раза', 'в 12 раз' — a multiple said in words, not as ×N."""
    shown = f"{x:.0f}" if x >= 10 else number(x, 1)
    if "." in shown:
        return f"в {shown} раза"  # fractions always take "раза"
    n = int(shown)
    word = "раза" if n % 10 in (2, 3, 4) and n % 100 not in (12, 13, 14) else "раз"
    return f"в {shown} {word}"


def pace_text(ratio, noun):
    if ratio >= 1.2:
        return f"{noun} усилились {fold(ratio)}"
    if ratio <= 0.83:
        return f"{noun} ослабли {fold(1 / ratio)}" if ratio > 0 else f"{noun} прекратились"
    return f"{noun} в прежнем темпе"


def norm_text(ratio):
    if ratio >= 1000:
        return "сделок в 1000+ раз больше обычного"
    return f"сделок {fold(ratio)} больше обычного"


def efficiency_text(ratio):
    """How much price a unit of aggressor volume buys now, compared with the impulse."""
    if ratio is None:
        return None
    if ratio <= 0.05:
        return "объём не двигает цену"
    if ratio < 0.3:
        return "объём почти не двигает цену"
    if ratio < 0.75:
        return "объём двигает цену слабее"
    if ratio > 1.5:
        return "цена идёт легче, чем в импульсе"
    return None


def share_bar(buy_share):
    """Five squares: green for the buy share, red for the sell share."""
    green = max(0, min(5, round(buy_share * 5)))
    return "🟩" * green + "🟥" * (5 - green)


def eat_text(ratio):
    """Zone notional / last-60s turnover → how long the current pace takes to trade through it."""
    seconds = ratio * 60
    if seconds < 1:
        return "съедят за <1с"
    if seconds < 120:
        return f"съедят за ~{seconds:.0f}с"
    return f"съедят за ~{seconds / 60:.0f} мин" if seconds < 600 else "съедят за 10+ мин"


def tape_lines(reading, mid=None):
    """Both sides of the last 15s, then what the impulse before it looked like — in plain words."""
    if not reading:
        return []
    up = reading["side"] > 0
    prior, recent = reading["prior"], reading["recent"]
    buys, sells = recent["aggressor_usd"], recent["opposite_usd"]
    if not up:
        buys, sells = sells, buys
    lines = [f"🟩 Покупки {usd(buys)} · 🟥 Продажи {usd(sells)} · за 15с"]
    context = [f"↩️ До этого {pct(prior['move_bps'])} за 30с"]
    if reading.get("effort_ratio") is not None:
        context.append(pace_text(reading["effort_ratio"], "покупки" if up else "продажи"))
    # With aggression faded or reversed, price per unit of volume says nothing.
    if reading["kind"] not in {"counter_aggression", "exhaustion"}:
        effect = efficiency_text(reading.get("efficiency_ratio"))
        if effect:
            context.append(effect)
    lines.append(" · ".join(context))
    return lines


PHASE_VERDICTS = {
    "early": "🚀 Ранний импульс",
    "confirmed": "✅ Движение подтвердилось",
    "extended": "⚠️ Движение уже далеко ушло",
    "weakening": "🪫 Импульс слабеет",
}


def verdict(state, arrow):
    """One phrase a scalper can act on: the tape reading first, then phase, then who is pushing."""
    reading = state.get("tape")
    if reading and reading["kind"] in NOTABLE:
        return "⚡ " + microstructure_label(reading)
    if state.get("phase") in PHASE_VERDICTS:
        return PHASE_VERDICTS[state["phase"]]
    if reading:
        return microstructure_label(reading)
    if arrow:
        return "🟩 Покупатели давят" if arrow == "▲" else "🟥 Продавцы давят"
    return None


def cost_line(state, natr):
    """Price (tap to copy) and the cost of entry; the light compares the spread with the usual 1m range."""
    parts = []
    if state.get("midpoint") is not None:
        parts.append(f"💲 <code>{number(state['midpoint'], 8)}</code>")
    spread = state.get("spread")
    if spread is not None:
        text = f"спред {small(spread / 100, 2)}%"
        if natr:
            share = spread / (natr * 100)
            text = ("🟢 " if share < 0.1 else "🟡 " if share < 0.25 else "🔴 ") + text
        elif state.get("wide_spread"):
            text = "🔴 " + text
        parts.append(text)
    return " · ".join(parts)


def zone_line(state):
    zone, mid = state.get("zone"), state.get("midpoint")
    if not zone:
        return None
    wall = (state.get("tape") or {}).get("wall")
    on_path = bool(wall and wall["price"] == zone["price"])
    side = "Спрос" if zone["side"] == "bid" else "Предложение"
    head = ("🧱 На пути: " if on_path else "🧱 ") + f"{side} <code>{number(zone['price'], 8)}</code>"
    if mid:
        head += f" ({(zone['price'] / mid - 1) * 100:+.2f}%)"
    parts = [head]
    if zone.get("notional_usd"):
        parts.append(usd(zone["notional_usd"]))
    if zone.get("size_to_observed_turnover") is not None:
        parts.append(eat_text(zone["size_to_observed_turnover"]))
    age = zone.get("observed_age_seconds")
    if age is not None and age < 10:
        parts.append("новая")  # a level seen for seconds is not yet a level to lean on
    return " · ".join(parts)


def card_arrow(state):
    """The side a card names now: ▲, ▼ or None. The same rule draws the card and scores its outcome."""
    arrow, _ = flow_side(state)
    reading = state.get("tape")
    if not reading:
        return arrow
    # The arrow describes what is happening now, not the impulse that preceded it.
    moved = reading["recent"]["move_bps"]
    if reading["kind"] == "counter_aggression":
        return "▼" if reading["side"] > 0 else "▲"
    if reading["kind"] in {"exhaustion", "slowdown"} or (moved is not None and moved * reading["side"] < 0):
        return None  # stalled or going back: the impulse direction no longer describes it
    return "▲" if reading["side"] > 0 else "▼"


def operational(packet, state, reason, kind, ratio=None, pulse=None):
    """Who/what/how much first, then why, then the level, then the cost of entry; source last and muted."""
    raw = packet["symbol"]
    binance = raw.startswith("BINANCE:")
    symbol = html.escape(raw.removeprefix("BINANCE:"))
    if state.get("mover"):
        title = "ликвидное движение"
    elif state.get("discovery_kind"):
        title = DISCOVERY_TITLES.get(state["discovery_kind"], "активность")
    else:
        title = TITLES.get(reason, "изменение ситуации").lower()
    if kind == "strengthening":
        title = "усиление: " + ("новый значимый уровень" if reason == "new_level" else "ситуация усилилась")
    arrow, tape = flow_side(state)
    arrow = card_arrow(state)
    reading_state = state.get("tape")
    change = state.get("change")
    window = "1м"
    if change is None and state.get("tape_change") is not None:
        change, window = state["tape_change"], "60с"
    if reading_state and reading_state["recent"]["move_bps"] is not None:
        change, window = reading_state["recent"]["move_bps"], "15с"
    header = symbol + (f" {arrow}" if arrow else "")
    if change is not None:
        header += f" {pct(change)} за {window}"
    if state.get("tier") is not None:
        header = f"{STRENGTH_MARKS.get(state.get('strength'), '')} T{state['tier']} · {header}".lstrip()
    parts = [f"<b>{header}</b>"]

    phrase = verdict(state, arrow)
    mover = state.get("mover")
    spike = phrase is None and not tape and ratio is not None and ratio >= 1
    if spike:
        # Nothing but a burst of prints: the burst itself is the headline.
        phrase = "📊 " + norm_text(ratio).capitalize()
    liquidations = state.get("liquidations")
    if phrase and liquidations and liquidations.get("cascade"):
        phrase += " · 💥 на каскаде"
    if phrase:
        parts.append(phrase)
    if mover:
        moves = []
        if mover.get("change_24h") is not None:
            moves.append(f"{pct(mover['change_24h'] * 10000)} за 24ч")
        if mover.get("change_1h") is not None:
            moves.append(f"{pct(mover['change_1h'] * 10000)} за 1ч")
        if mover.get("volume_24h"):
            moves.append(f"оборот {usd(mover['volume_24h'])} за 24ч")
        if moves:
            parts.append("🚀 " + " · ".join(moves))

    reading = tape_lines(reading_state)
    if reading:
        parts.extend(reading)
    elif tape:
        flow, count = state.get("flow"), state.get("count")
        if tape.startswith("Сделок мало"):
            parts.append(f"▫️ Сделок мало ({int(count)}) — сторона не ясна")
        else:
            buys = (flow + 1) * 50  # name the side that outweighs, never "покупки 0%"
            side = f"Покупки {buys:.0f}%" if flow >= 0 else f"Продажи {100 - buys:.0f}%"
            line = f"{share_bar((flow + 1) / 2)} {side}"
            turnover = state.get("turnover_60s")
            line += f" · {usd(turnover)}/мин" if turnover else f" · {int(count)} сделок/мин"
            parts.append(line)
            # Activity below the norm contradicts any "active" title; only an excess is informative.
            if ratio is not None and ratio >= 1.5:
                parts.append("📊 " + norm_text(ratio).capitalize())
    if state.get("context_version") or (pulse and pulse.get("status") == "ready"):
        flow = state.get("cross_venue") if state.get("context_version") else pulse.get("flow")
        if flow in {"aligned_buy", "aligned_sell"}:
            parts.append("🔗 HL и OKX: " + ("покупки" if flow == "aligned_buy" else "продажи") + " на обеих")
        elif flow == "opposed":
            parts.append("🔗 HL и OKX: стороны расходятся")

    forced = liquidation_line(liquidations, usd)
    if forced:
        parts.append(forced)

    level = zone_line(state)
    if level:
        parts.append(level)
    cost = cost_line(state, value(packet, "context.1m.natr"))
    if cost:
        parts.append(cost)
    clock = datetime.fromtimestamp(packet["captured_at"], MSK).strftime("%H:%M:%S")
    parts.append(f"<i>{'Binance' if binance else 'Hyperliquid'} · {title} · {clock}</i>")
    return "\n".join(parts), "<p>" + "<br>".join(parts) + "</p>"


def level_card(packet, state):
    """A density card: where the level is, what it means for this instrument, who trades at it now."""
    level, mid = state["level"], state.get("midpoint")
    zone = level["zone"]
    raw = packet["symbol"]
    symbol = html.escape(raw.removeprefix("BINANCE:"))
    side = "Спрос" if zone["side"] == "bid" else "Предложение"
    where = f"🧱 {side} <code>{number(zone['price'], 8)}</code>"
    if mid:
        where += f" ({(zone['price'] / mid - 1) * 100:+.2f}%)"
    header = f"{STRENGTH_MARKS.get(state.get('strength'), '')} T{state['tier']} · {symbol} · {where}".lstrip()
    parts = [f"<b>{header}</b>"]
    if level["kind"] == "appeared":
        parts.append(f"Появилась крупная заявка и стоит {zone['observed_age_seconds']:.0f}с")
    elif (level.get("eaten_share") or 0) >= 0.25:
        parts.append(f"⚡ Заявку съедают: −{level['eaten_share'] * 100:.0f}% размера за {level.get('since') or 0:.0f}с")
    else:
        parts.append("⚡ Цена подошла к заявке")
    size = []
    if zone.get("notional_usd"):
        size.append(usd(zone["notional_usd"]))
    size.append(f"{zone['relative_size']:.0f}× обычного уровня стакана")
    if zone.get("size_to_observed_turnover") is not None:
        size.append(eat_text(zone["size_to_observed_turnover"]))
    parts.append(" · ".join(size))
    reading = tape_lines(state.get("tape"))
    if reading:
        parts.extend(reading)
    else:
        arrow, tape = flow_side(state)
        if tape and not tape.startswith("Сделок мало"):
            flow = state["flow"]
            buys = (flow + 1) * 50
            side_text = f"Покупки {buys:.0f}%" if flow >= 0 else f"Продажи {100 - buys:.0f}%"
            line = f"{share_bar((flow + 1) / 2)} {side_text}"
            if state.get("turnover_60s"):
                line += f" · {usd(state['turnover_60s'])}/мин"
            parts.append(line)
    forced = liquidation_line(state.get("liquidations"), usd)
    if forced:
        parts.append(forced)
    cost = cost_line(state, value(packet, "context.1m.natr"))
    if cost:
        parts.append(cost)
    clock = datetime.fromtimestamp(packet["captured_at"], MSK).strftime("%H:%M:%S")
    venue = "Binance" if raw.startswith("BINANCE:") else "Hyperliquid"
    parts.append(f"<i>{venue} · плотность · {clock}</i>")
    return "\n".join(parts), "<p>" + "<br>".join(parts) + "</p>"


def analytical_details(packet, state):
    if not state.get("context_version"):
        return []
    lines = [
        "<b>Контекст и своевременность</b>",
        "Фаза 1м: " + PHASES.get(state.get("phase"), "не определена"),
        "Фаза 5м: " + PHASES.get(state.get("phase5"), "не определена"),
    ]
    level = state.get("formation_strength")
    if level is not None:
        lines.append(
            "Сила контекста: "
            + {0: "не определена", 1: "фоновая", 2: "заметная", 3: "сильная", 4: "очень сильная"}.get(
                level, "не определена"
            )
        )
    for key, label, unit in (
        ("context.phase.1m.distance_natr", "Пройдено от наблюдаемого начала", " ATR"),
        ("context.phase.1m.retracement_natr", "Откат от наблюдаемого максимума", " ATR"),
        ("context.1m.natr", "Обычная волатильность 1м", "%"),
        ("context.1m.range_ratio", "Расширение диапазона", "× ATR"),
        ("context.1m.activity_ratio", "Темп сделок в rolling-окне", "× базы"),
        ("btc.relative_last_return_pp", "Отличие от BTC за закрытые 5м", " п.п."),
        ("btc.correlation", "Корреляция с BTC на 36 окнах 5м", ""),
        ("senior.15m.return_4", "Изменение за 4 закрытые свечи 15м", "%"),
        ("context.1m.funding_rate", "Funding", " доля/час"),
        ("context.1m.open_interest", "Открытый интерес", " USD"),
    ):
        v = value(packet, key)
        if v is not None:
            lines.append(label + ": " + number(v) + unit)
    if state.get("wide_spread") is not None:
        lines.append(
            "Спред: " + ("высок относительно волатильности" if state["wide_spread"] else "ниже порога 0,25 NATR")
        )
    for days in (7, 30):
        prefix = f"profile.1m.{days}d.trades"
        ratio, samples = value(packet, prefix + ".current_ratio"), value(packet, prefix + ".samples")
        if ratio is not None and samples is not None:
            lines.append(f"Выборка {days}д: текущие сделки {number(ratio)}× медианы, {int(samples)} наблюдений")
    zone = state.get("zone") or {}
    if zone.get("distance_bps") is not None:
        lines.append("Зона: расстояние " + number(zone["distance_bps"] / 100) + "% от середины спреда")
    if zone.get("size_change") is not None:
        lines.append("Изменение размера зоны: " + number(zone["size_change"], 8) + " базовых единиц")
    if zone.get("size_to_observed_turnover") is not None:
        lines.append("Размер зоны / наблюдаемый оборот ≤60с: " + number(zone["size_to_observed_turnover"]) + "×")
    relation = state.get("cross_venue")
    if relation in {"aligned_buy", "aligned_sell", "opposed", "neutral_or_mixed"}:
        lines.append(
            "HL/OKX, одна закрытая минута: "
            + {
                "aligned_buy": "покупки на обеих",
                "aligned_sell": "продажи на обеих",
                "opposed": "стороны расходятся",
                "neutral_or_mixed": "единой стороны нет",
            }[relation]
        )
    for venue in ("hyperliquid", "okx"):
        count, ofi = value(packet, f"venue.{venue}.execution_count"), value(packet, f"venue.{venue}.ofi")
        if count is not None and ofi is not None:
            lines.append(f"{venue.upper()}: {int(count)} исполнений, покупки {(ofi + 1) * 50:.0f}% объёма")
    missing = sum(f["quality"] != "ok" for f in packet["facts"] if f.get("calculation_version") == "scalper-context-v2")
    if missing:
        lines.append("Часть контекста недоступна или устарела; отсутствующие значения не заменены нулями.")
    lines.append("Профили описывают наблюдавшуюся выборку, не доказанную сезонную редкость.")
    return lines


class TelegramUX:
    def __init__(self, service):
        self.service = service
        self.db = service.journal.db
        self.tiger = TigerBridge()
        self.desktop = DesktopNotifier()
        self.outcomes = SignalOutcomes(self.db)
        self.levels = LevelWatcher()
        self.watch_at = 0
        self.quality = NotificationQuality(self.db)
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS telegram_episodes(
                episode TEXT PRIMARY KEY,symbol TEXT,baseline TEXT,generation INTEGER DEFAULT 0,last_at REAL);
            CREATE TABLE IF NOT EXISTS telegram_actions(
                token TEXT PRIMARY KEY,card TEXT,action TEXT,symbol TEXT,episode TEXT,expires_at REAL);
            CREATE TABLE IF NOT EXISTS telegram_clicks(
                query_id TEXT PRIMARY KEY,at REAL,status TEXT);
            CREATE TABLE IF NOT EXISTS telegram_ux_events(
                id INTEGER PRIMARY KEY,episode TEXT,at REAL,reason TEXT,status TEXT,packet TEXT,state TEXT);
        """)
        columns = {r[1] for r in self.db.execute("PRAGMA table_info(telegram_ux_events)")}
        if "channel" not in columns:
            # Which bot sent the card; rows before multi-bot delivery belong to the anomalies bot.
            self.db.execute("ALTER TABLE telegram_ux_events ADD COLUMN channel TEXT DEFAULT 'anomalies'")
        # Pending old-format cards must not leak after switching the interface.
        with self.db:
            self.db.execute(
                "UPDATE episode_cards SET status='expired' WHERE status='pending' "
                "AND destination IS NULL AND episode_id NOT LIKE 'ux:%'"
            )

    def analytics_chat(self):
        chat = os.getenv("SCALPER_ANALYTICS_CHAT_ID", "").strip()
        return chat if chat and chat != str(self.service.hub.alerts.telegram_chat_id) else ""

    def button(self, card, action, packet, expires):
        old = self.db.execute(
            "SELECT token FROM telegram_actions WHERE card=? AND action=? AND expires_at>=?",
            (card, action, expires - 1),
        ).fetchone()
        if old:
            return "ux:" + old[0]
        token = secrets.token_hex(12)
        self.db.execute(
            "INSERT INTO telegram_actions VALUES(?,?,?,?,?,?)",
            (token, card, action, packet["symbol"], packet["episode_id"], expires),
        )
        return "ux:" + token

    def markup(self, card, packet, expires, *, expanded=False):
        buttons = [
            dict(
                text="Свернуть" if expanded else "Разбор",
                callback_data=self.button(card, "compact" if expanded else "details", packet, expires),
            )
        ]
        try:
            self.tiger.command(packet["symbol"])
            buttons.insert(0, dict(text="TigerTrade · ПК", callback_data=self.button(card, "tiger", packet, expires)))
        except (ValueError, TypeError):
            pass
        ticker = packet["symbol"].removeprefix("BINANCE:")
        return dict(inline_keyboard=[buttons, [dict(text="Копировать тикер", copy_text=dict(text=ticker))]])

    def audit(self, packet, reason, status, state, channel="anomalies"):
        self.db.execute(
            "INSERT INTO telegram_ux_events(episode,at,reason,status,packet,state,channel) VALUES(?,?,?,?,?,?,?)",
            (packet["episode_id"], packet["captured_at"], reason, status, packet["packet_id"], json.dumps(state),
             channel),
        )

    def observe(self, now):
        service = self.service
        self.quality.sample(service.context_packets, now)
        for packet in service.context_packets.values():
            if not packet_ready(packet, now):
                continue
            try:
                self.observe_levels(packet, now)
            except Exception:  # a level card must never stop the anomaly/mover pipeline
                LOG.exception("Level event failed for %s", packet["symbol"])
            memory = service.context_memory.get(packet["episode_id"])
            priority, reason, state = decide(packet, memory["samples"])
            bots = getattr(service, "bots", None)
            if bots is not None:
                # Measure every tape reading, sent or not: the observer, not the push filter, decides
                # whether readings carry information.
                bots.record("tape.reading", packet["symbol"],
                            tape_reading(state.get("tape"), state.get("midpoint"),
                                         value(packet, "context.1m.natr")), now)
            state["scale"] = scale(packet, state)
            state["ratio"] = activity(service, packet["symbol"], now)
            candle = getattr(service.attention, "activity", {}).get(packet["symbol"], {})
            state["activity_source"] = {
                k: candle.get(k) for k in ("source", "start_ms", "reference_policy", "reference_minutes", "ratios")
            }
            senior = getattr(service, "candles", {}).get((packet["symbol"], "15m"), {})
            closed = senior.get("closed_at")
            state["senior"] = (
                {k: senior.get(k) for k in ("closed_at", "return_4", "range_4")}
                if type(closed) in (int, float) and 0 <= now - closed <= 990
                else {}
            )
            previous = self.db.execute(
                "SELECT * FROM telegram_episodes WHERE episode=?", (packet["episode_id"],)
            ).fetchone()
            movers = getattr(getattr(service, "attention", None), "movers", None)
            mover = movers.picked.get(packet["symbol"]) if movers else None
            if mover:
                state["mover"] = {k: mover.get(k) for k in ("change_1h", "change_24h", "volume_24h", "score")}
                if priority is None and previous is None:
                    # Being picked is the news for the movers bot, once the fresh tape can say who is
                    # pushing (or 90s have passed): a first snapshot right after subscribing reads "1 trade".
                    warm = (state.get("count") or 0) >= MIN_FLOW_TRADES or now - mover["assigned_at"] >= 90
                    if not warm:
                        continue
                    priority, reason = 1, "mover_selected"
            if priority is None:
                continue
            discovery = bool(state.get("discovery_kind")) and not mover
            # Discovery still finds instruments for the quiet watch list; only a tradable one interrupts.
            if mover:
                blocked = "mover_wide_spread" if state.get("wide_spread") else None
            elif discovery:
                blocked = "discovery_wide_spread" if state.get("wide_spread") else None
            else:
                blocked = admission(state, reason)
            if blocked:
                self.quality.record(packet, state, "blocked", blocked)
                continue
            # A quiet large level alone is not a reason to interrupt the trader.
            active = (
                (state["ratio"] is not None and state["ratio"] >= 2)
                or (state.get("rolling_activity") is not None and state["rolling_activity"] >= 2)
                or (state.get("flow") is not None and abs(state["flow"]) >= 0.6 and (state.get("count") or 0) >= 20)
            )
            if not active and not discovery and not mover:
                self.quality.record(packet, state, "blocked", "activity_unconfirmed")
                continue
            if previous is None:
                # Losing and reacquiring attention does not make the same situation new.
                previous = self.db.execute(
                    "SELECT * FROM telegram_episodes WHERE symbol=? AND last_at>? ORDER BY last_at DESC LIMIT 1",
                    (packet["symbol"], now - 900),
                ).fetchone()
            baseline = json.loads(previous["baseline"]) if previous else None
            generation = previous["generation"] if previous else 0
            kind = "situation"
            if baseline:
                zone, old = state.get("zone"), baseline.get("zone")
                new_zone = bool(
                    zone
                    and (not old or zone["id"] != old["id"])
                    and zone["relative_size"] >= 6
                    and zone["observed_age_seconds"] >= 30
                )
                stronger = (
                    state.get("ratio") is not None
                    and baseline.get("ratio") is not None
                    and state["ratio"] >= max(3, baseline["ratio"] * 1.75)
                )
                expanded = (
                    state["scale"] == "5m"
                    and baseline.get("scale") != "5m"
                    and (not state.get("context_version") or state.get("phase5") == "confirmed")
                )
                confirmed = reason == "formation_confirmed" and baseline.get("phase") != "confirmed"
                stronger_zone = bool(
                    zone and old and zone["id"] == old["id"] and zone["relative_size"] >= old["relative_size"] * 1.75
                )
                discovery_upgrade = (state.get("discovery_kind") == "active"
                                     and baseline.get("discovery_kind") != "active")
                discovery_stronger = bool(
                    discovery and state.get("discovery_score", 0) >=
                    max(6, (baseline.get("discovery_score") or 0) * 1.75)
                    and state.get("discovery_score", 0) > (baseline.get("discovery_score") or 0))
                if (now - previous["last_at"] < 120 and not discovery_upgrade) or not (
                    new_zone or stronger or expanded or stronger_zone or confirmed
                    or discovery_upgrade or discovery_stronger
                ):
                    continue
                kind = "situation" if discovery_upgrade else "strengthening"
                reason = "active_instrument" if discovery_upgrade else "new_level" if new_zone else "strengthened"
                # Candidate → active within the cooldown updates the same message instead of a second push.
                if not (discovery_upgrade and now - previous["last_at"] < 120):
                    generation += 1
            assets = getattr(getattr(service.hub, "market", None), "assets", None) or {}
            asset = assets.get(packet["symbol"])
            state["turnover_60s"] = value(packet, "tape.observed_turnover_60s")
            state["natr_1m"] = value(packet, "context.1m.natr")
            context = getattr(service, "liquidations", None)
            if context is not None:
                tape = state.get("tape") or {}
                moved = (tape.get("recent") or {}).get("move_bps")
                if moved is None:
                    moved = state.get("change") if state.get("change") is not None else state.get("tape_change")
                state["liquidations"] = context.recent(packet["symbol"], now, state["turnover_60s"], moved)
            state["tier"], state["tier_source"] = liquidity_tier(
                getattr(asset, "volume_24h", None), state["turnover_60s"]
            )
            state["strength"] = signal_strength(state, discovery)
            state["route"] = route(state["tier"], state["strength"])
            if state["route"] == "watch":
                # Found, but not worth an interruption at this liquidity/strength: quiet watch list only.
                self.quality.record(packet, state, "blocked", f"watch_t{state['tier']}_{state['strength']}")
                continue
            # Existing delivered episodes establish a baseline; no retrospective push.
            existing = self.db.execute(
                "SELECT message_id FROM episode_cards WHERE episode_id=?", (packet["episode_id"],)
            ).fetchone()
            migrated = not previous and existing and existing["message_id"]
            channels = getattr(service, "channels", None) or {}
            channel = channels.get("movers" if mover else "anomalies")
            card = f"ux:{'mov' if mover else 'ops'}:{packet['episode_id']}:{generation}"
            with self.db:
                if not migrated:
                    pulse = service.pulse.facts(packet["symbol"], now) if service.pulse else None
                    state["pulse"] = (
                        {k: pulse.get(k) for k in ("status", "flow", "window_start_ms", "window_end_ms")}
                        if pulse
                        else None
                    )
                    text, rich = operational(packet, state, reason, kind, state["ratio"], pulse)
                    service.consistency.save_packet(packet)
                    service.outbox.enqueue(
                        card,
                        text,
                        rich,
                        now + 120,
                        chat_id=channel.chat_id if channel else str(service.hub.alerts.telegram_chat_id),
                        destination=channel.spec.destination if channel else "operations",
                        reply_markup=self.markup(card, packet, now + 120),
                        silent=state["route"] == "silent",
                    )
                    if state["route"] == "alert" and (channel is None or channel.spec.desktop):
                        # Strong and liquid only: the trading PC; a click opens it in TigerTrade.
                        self.desktop.notify(packet["symbol"], text, now + 120)
                    self.audit(packet, reason, "queued", state, channel.name if channel else "anomalies")
                    self.quality.record(packet, state, "queued", reason, card=card)
                self.db.execute(
                    "INSERT OR REPLACE INTO telegram_episodes VALUES(?,?,?,?,?)",
                    (packet["episode_id"], packet["symbol"], json.dumps(state), generation, now),
                )
        if now - self.watch_at >= 60:
            self.watch(now)

    def observe_levels(self, packet, now):
        """Densities for the levels bot: relative to the instrument, one card per level and event."""
        service = self.service
        events = self.levels.process(packet["symbol"], zones_of(packet), value(packet, "context.1m.natr"), now)
        if not events:
            return
        assets = getattr(getattr(service.hub, "market", None), "assets", None) or {}
        turnover = value(packet, "tape.observed_turnover_60s")
        tier, tier_source = liquidity_tier(getattr(assets.get(packet["symbol"]), "volume_24h", None), turnover)
        channels = getattr(service, "channels", None) or {}
        channel = channels.get("levels")
        bots = getattr(service, "bots", None)
        natr = value(packet, "context.1m.natr")
        price = value(packet, "book.midpoint")
        for event in events:
            if bots is not None and event["kind"] == "approach":
                # The same approach is measured both ways: did the level hold, or did price go through?
                bots.record("formation.hold", packet["symbol"], level_hold(event["zone"], price, natr), now)
                bots.record("formation.break", packet["symbol"], level_break(event["zone"], price, natr), now)
            # Coming to (or eating) an announced level is the urgent one; a new level is notable.
            strength = "strong" if event["kind"] == "approach" else "notable"
            state = dict(summarize(packet), tier=tier, tier_source=tier_source, strength=strength,
                         route=route(tier, strength), turnover_60s=turnover,
                         level=dict(kind=event["kind"], zone=event["zone"], eaten_share=event.get("eaten_share"),
                                    since=event.get("since")))
            reason = "level_" + event["kind"]
            if state["route"] == "watch":
                self.quality.record(packet, state, "blocked", f"{reason}_t{tier}")
                continue
            context = getattr(service, "liquidations", None)
            if context is not None:
                state["liquidations"] = context.recent(packet["symbol"], now, turnover, state.get("tape_change"))
            text, rich = level_card(packet, state)
            zone_key = checksum(dict(zone=event["zone"]["id"]))[:12]
            card = f"ux:lvl:{packet['episode_id']}:{zone_key}:{event['kind']}"
            with self.db:
                service.consistency.save_packet(packet)
                service.outbox.enqueue(
                    card, text, rich, now + 120,
                    chat_id=channel.chat_id if channel else str(service.hub.alerts.telegram_chat_id),
                    destination=channel.spec.destination if channel else "operations",
                    reply_markup=self.markup(card, packet, now + 120),
                    silent=state["route"] == "silent",
                )
                self.audit(packet, reason, "queued", state, channel.name if channel else "anomalies")
                self.quality.record(packet, state, "queued", reason, card=card)

    def watch(self, now):
        self.watch_at = now
        service = self.service
        attention = getattr(service.hub, "attention", {})
        plan = service.attention.plan or {}
        active = attention.get("active", [])
        lines = ["<b>Наблюдение · ранние кандидаты</b>"]
        if plan.get("expires_at_ms", 0) < now * 1000 or attention.get("subscription_error"):
            lines.append("План или данные устарели; текущие кандидаты не подтверждены.")
        else:
            dynamic = [r for r in active if r.get("state") != "control"][:6]
            for row in dynamic:
                lines.append(self.watch_line(row, now))
            if not dynamic:
                lines.append("Содержательных кандидатов пока нет.")
        lines.append("Тихий список: отдельный push только при потоке, активности или торгуемой зоне.")
        lines.append(datetime.fromtimestamp(now, MSK).strftime("Обновлено %H:%M МСК"))
        text = "\n".join(lines)
        service.outbox.enqueue(
            "ux:watch",
            text,
            "<p>" + "<br>".join(lines) + "</p>",
            now + 180,
            destination="operations",
            chat_id=str(service.hub.alerts.telegram_chat_id),
            silent=True,
        )

    def watch_line(self, row, now):
        service = self.service
        symbol = row["symbol"]
        packet = service.context_packets.get(symbol)
        name = html.escape(symbol.removeprefix("BINANCE:"))
        if not (packet and packet_ready(packet, now)):
            return f"{name} · прогрев, свежих данных нет"
        reason = (service.context_memory.latest(packet["episode_id"]) or {}).get("reason")
        why = WATCH_REASONS.get(reason) or {"focus": "в фокусе", "probe": "проверяем", "cooling": "затухает"}.get(
            row.get("state"), "наблюдение"
        )
        state = summarize(packet)
        arrow, tape = flow_side(state)
        parts = [f"<b>{arrow + ' ' if arrow else ''}{name}</b> · {why}"]
        if tape and not tape.startswith("Сделок мало"):
            parts.append(tape.removeprefix(arrow + " ") if arrow else tape)
        zone, mid = state.get("zone"), state.get("midpoint")
        if zone and mid:
            side = "спрос" if zone["side"] == "bid" else "предл."
            parts.append(
                f"{side} {(zone['price'] / mid - 1) * 100:+.2f}%"
                + (" " + usd(zone["notional_usd"]) if zone.get("notional_usd") else "")
            )
        phase = state.get("phase")
        if phase in CARD_PHASES:
            parts.append(PHASES[phase].lower())
        if state.get("wide_spread"):
            parts.append("⚠ спред")
        return " · ".join(parts)

    def publish_analysis(self, response):
        packet = response["packet"]
        review = response.get("review") or {}
        result = review.get("result") or {}
        if response.get("status") != "ok" or result.get("abstention") or result.get("expired_at_completion", True):
            return False
        meta = packet["symbol"] == "MARKET"
        hypotheses = [
            c for c in result.get("claims", []) if c["section"] == "interpretation" and c["status"] == "hypothesis"
        ]
        if meta and not hypotheses:
            return False
        card = "ux:analysis:" + packet["episode_id"]
        if meta:
            card += ":" + packet["packet_id"][:16]
        current = summarize(packet) if not meta else None
        heading = "Связи эпизодов · обзор рынка" if meta else html.escape(packet["symbol"]) + " · Разбор ситуации"
        lines = ["<b>" + heading + "</b>"]
        if meta:
            symbols = sorted({f["id"].rsplit(":", 1)[0] for f in packet["facts"] if ":" in f["id"]})
            lines.append("Наблюдаемые инструменты: " + html.escape(", ".join(symbols)))
            for symbol in symbols[:8]:
                fact = next(
                    (f for f in packet["facts"] if f["id"] == symbol + ":book.midpoint" and f["quality"] == "ok"), None
                )
                if fact:
                    parts = ["цена " + number(fact["value"], 8)]
                    for key, label, unit in (
                        ("frame.1m.observed_price_change_bps", "Δ 1м", "%"),
                        ("frame.1m.ofi", "покупки 1м", "%"),
                    ):
                        v = value(packet, symbol + ":" + key)
                        if v is not None:
                            transformed = v / 100 if key.endswith("bps") else (v + 1) * 50
                            parts.append(label + " " + number(transformed) + unit)
                    lines.append(html.escape(symbol) + " · " + " · ".join(parts))
        else:
            lines.append(TITLES.get(review.get("trigger_reason"), "Изменение наблюдаемого контекста"))
            for key, label in (("book.midpoint", "Цена"), ("book.spread_bps", "Спред, б.п.")):
                measurement = value(packet, key)
                if measurement is not None:
                    lines.append(label + ": " + number(measurement, 8 if key == "book.midpoint" else 2))
            lines.append("Масштаб: " + scale(packet, current))
            lines.extend(analytical_details(packet, current))
            lines.extend(["", "<b>Сценарии наблюдения · гипотезы</b>", *[html.escape(s) for s in scenarios(current)]])
        hypotheses = [
            c for c in result.get("claims", []) if c["section"] == "interpretation" and c["status"] == "hypothesis"
        ]
        if hypotheses:
            lines.extend(["", "<b>Интерпретация AI · гипотеза</b>"])
            lines.extend(html.escape(" ".join(c["assertion"]["text"].split())[:280]) for c in hypotheses[:2])
        elif meta:
            lines.append("Проверяемая связь эпизодов не сформулирована.")
        lines.append(
            "Снимок "
            + datetime.fromtimestamp(packet["captured_at"], MSK).strftime("%H:%M:%S МСК")
            + (" · данные наблюдаемых площадок." if meta else " · данные Binance USDT-M."
               if packet["symbol"].startswith("BINANCE:") else " · данные Hyperliquid.")
        )
        # Rich detail is optional depth, not a longer first screen in operations.
        text = "\n".join(lines)
        rich = (
            "<h3>"
            + heading
            + "</h3><details><summary>Объяснение и сценарии</summary><p>"
            + "<br>".join(lines[1:])
            + "</p></details>"
        )
        self.service.outbox.enqueue(
            card,
            text,
            rich,
            packet["captured_at"] + 300,
            chat_id=self.analytics_chat(),
            destination="analytics",
            silent=True,
        )
        return True

    def status(self):
        return dict(
            enabled=True,
            analytics_configured=bool(self.analytics_chat()),
            tiger_group=self.tiger.group or None,
            tiger_exchange=self.tiger.exchange,
            tiger_config_error=self.tiger.config_error,
            desktop=self.desktop.status(),
            callback_errors=self.db.execute("SELECT COUNT(*) FROM telegram_clicks WHERE status='error'").fetchone()[0],
        )

    def report(self, now):
        events = self.db.execute(
            "SELECT COUNT(*),COUNT(DISTINCT episode),SUM(reason IN ('strengthened','new_level')) "
            "FROM telegram_ux_events WHERE at BETWEEN ? AND ?",
            (now - 86400, now),
        ).fetchone()
        delivered = self.db.execute(
            "SELECT COUNT(*) FROM episode_cards WHERE destination='operations' AND episode_id<>'ux:watch' "
            "AND status='sent' AND expires_at BETWEEN ? AND ?",
            (now - 86400, now + 120),
        ).fetchone()[0]
        observations = self.quality.report(now)["outcomes"][0]
        followup = (
            (f"\nПосле отбора, 1м: наблюдалось {observations['observed']}, нет данных {observations['missing']}.")
            if observations["observed"] or observations["missing"]
            else ""
        )
        return (
            "<b>Скринер · последние 24 часа</b>\n"
            f"Отобрано событий: {events[0]} · эпизодов: {events[1]}\n"
            f"Существенных усилений: {events[2] or 0} · доставлено карточек: {delivered}\n"
            + (
                "Аналитика: отдельный канал подключён."
                if self.analytics_chat()
                else "Аналитика: канал пока не подключён; разборы сохраняются в проекте."
            )
            + followup
            + "\nЭто статистика наблюдения и доставки, не торговых результатов."
        )

    def operator_allowed(self, user):
        allowed = {x.strip() for x in os.getenv("SCALPER_TELEGRAM_OPERATOR_IDS", "").split(",") if x.strip()}
        default_chat = str(self.service.hub.alerts.telegram_chat_id)
        if default_chat.isdecimal():
            allowed.add(default_chat)
        return str(user) in allowed

    async def callback(self, query, session):
        now = time.time()
        token = str(query.get("data", "")).removeprefix("ux:")
        action = self.db.execute("SELECT * FROM telegram_actions WHERE token=?", (token,)).fetchone()
        message = query.get("message", {})
        user = str(query.get("from", {}).get("id", ""))
        chat = str(message.get("chat", {}).get("id", ""))
        if not action or not self.operator_allowed(user):
            return "Действие недоступно."
        card = self.db.execute("SELECT * FROM episode_cards WHERE episode_id=?", (action["card"],)).fetchone()
        if not card or chat != str(card["chat_id"]) or message.get("message_id") != card["message_id"]:
            return "Действие недоступно."
        # Opening the chart stays useful long after the snapshot; other actions follow the card's lifetime.
        grace = TIGER_BUTTON_SECONDS if action["action"] == "tiger" else 0
        if action["expires_at"] + grace < now:
            return "Карточка устарела. Используй свежую ситуацию."
        if self.db.execute("SELECT 1 FROM telegram_clicks WHERE query_id=?", (query["id"],)).fetchone():
            return "Этот запрос уже обработан."
        with self.db:
            self.db.execute("INSERT INTO telegram_clicks VALUES(?,?,'processing')", (query["id"], now))
        try:
            if action["action"] == "tiger":
                reply = await self.tiger.open(session, action["symbol"])
            else:
                analysis = self.db.execute(
                    "SELECT * FROM episode_cards WHERE episode_id=?", ("ux:analysis:" + action["episode"],)
                ).fetchone()
                if not self.analytics_chat():
                    reply = "Показаны сценарии наблюдения. Аналитический канал ещё не настроен."
                elif analysis and analysis["message_id"] and analysis["status"] == "sent":
                    reply = "Разбор опубликован в аналитическом канале."
                else:
                    reply = "Показаны сценарии наблюдения. /analyze " + action["symbol"] + " запросит AI-разбор."
                event = self.db.execute(
                    "SELECT state,packet,reason FROM telegram_ux_events WHERE episode=? AND at<=? "
                    "ORDER BY at DESC LIMIT 1",
                    (action["episode"], action["expires_at"] - 119),
                ).fetchone()
                if event:
                    packet = self.service.consistency.packet(event["packet"])
                    state = json.loads(event["state"])
                    kind = "strengthening" if event["reason"] in {"strengthened", "new_level"} else "situation"
                    base, base_rich = operational(
                        packet, state, event["reason"], kind, state.get("ratio"), state.get("pulse")
                    )
                    lines = [
                        base,
                        "",
                        "<b>Сценарии наблюдения · гипотезы</b>",
                        *[html.escape(s) for s in scenarios(state)],
                        "",
                        "<b>История наблюдений</b>",
                    ]
                    details = analytical_details(packet, state)
                    if details:
                        lines[2:2] = [*details, ""]
                    if state.get("ratio") is not None:
                        lines.insert(
                            2, "Норма сделок: медиана 14 предыдущих полных минут; статистическая редкость не оценена."
                        )
                    senior = state.get("senior") or {}
                    if type(senior.get("return_4")) in (int, float):
                        lines.extend(
                            [
                                "",
                                "<b>Контекст 15м · закрытые свечи</b>",
                                f"Изменение за 4 свечи: {senior['return_4']:+.2f}%",
                            ]
                        )
                    samples = self.service.context_memory.get(action["episode"])["samples"]
                    for sample in samples[-3:]:
                        if sample.get("at") and sample.get("midpoint") is not None:
                            clock = datetime.fromtimestamp(sample["at"], MSK).strftime("%H:%M:%S")
                            lines.append(f"{clock} · цена {number(sample['midpoint'], 8)}")
                    if analysis and analysis["message_id"] and analysis["status"] == "sent":
                        channel = str(analysis["chat_id"])
                        if channel.startswith("-100"):
                            url = f"https://t.me/c/{channel[4:]}/{analysis['message_id']}"
                            lines.append(f'<a href="{url}">Подробный разбор в аналитическом канале</a>')
                    text = "\n".join(lines)
                    rich = (
                        "<p>" + base.replace("\n", "<br>") + "</p><details>"
                        "<summary>Сценарии и история</summary><p>" + "<br>".join(lines[2:]) + "</p></details>"
                    )
                    self.service.outbox.enqueue(
                        action["card"],
                        base if action["action"] == "compact" else text,
                        base_rich if action["action"] == "compact" else rich,
                        action["expires_at"],
                        chat_id=card["chat_id"],
                        destination=card["destination"],
                        reply_markup=self.markup(
                            action["card"], packet, action["expires_at"], expanded=action["action"] != "compact"
                        ),
                    )
            with self.db:
                self.db.execute("UPDATE telegram_clicks SET status='done' WHERE query_id=?", (query["id"],))
            return reply
        except Exception:
            with self.db:
                self.db.execute("UPDATE telegram_clicks SET status='error' WHERE query_id=?", (query["id"],))
            return "Действие не завершено. Автоматического повтора нет."

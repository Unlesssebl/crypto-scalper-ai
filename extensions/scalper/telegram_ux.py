"""Telegram product layer: deterministic events, quiet watchlist and separate analysis."""

import html
import json
import math
import os
import secrets
import time
from datetime import datetime, timedelta, timezone

from .context_auto import decide, summarize
from .fact_packet import packet_ready
from .tiger import TigerBridge

MSK = timezone(timedelta(hours=3))
TITLES = {
    "density_context": "Удерживается значимая зона",
    "flow_price_conflict": "Поток расходится с реакцией цены",
    "early_context": "Ускорение подтверждается движением цены",
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
    return "ранняя"


def activity(service, symbol, now):
    candle = getattr(service.attention, "activity", {}).get(symbol, {})
    target = int(now * 1000) // 60000 * 60000 - 60000
    if candle.get("status") != "ready" or candle.get("start_ms") != target:
        return None
    ratio = candle.get("ratios", {}).get("trade_count")
    return ratio if type(ratio) in (int, float) and math.isfinite(ratio) and ratio >= 0 else None


def scenarios(state):
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
    return [
        "Основной: активность сохраняется и цена продолжает реагировать в сторону потока.",
        "Альтернативный: активность затухает или реакция цены расходится с потоком — сценарий ускорения ослабевает.",
    ]


def operational(packet, state, reason, kind, ratio=None, pulse=None):
    symbol = html.escape(packet["symbol"])
    frame = scale(packet, state)
    title = TITLES.get(reason, "Изменение наблюдаемой ситуации")
    if kind == "strengthening":
        title = "Появился новый значимый уровень" if reason == "new_level" else "Ситуация заметно усилилась"
    parts = [f"<b>{symbol} · {title}</b>", f"{'Усиление' if kind == 'strengthening' else 'Ситуация'} · масштаб {frame}"]
    metrics = []
    if state.get("midpoint") is not None:
        metrics.append("Цена " + number(state["midpoint"], 8))
    if state.get("spread") is not None:
        metrics.append("спред " + number(state["spread"]) + " б.п.")
    if state.get("change") is not None:
        metrics.append(f"Δ {state['change'] / 100:+.2f}% за наблюдаемую 1м")
    if metrics:
        parts.append(" · ".join(metrics))
    if ratio is not None:
        parts.append(f"Сделок {'≥' if ratio >= 1000 else ''}{number(ratio)}× нормы · закрытая 1м")
    if state.get("flow") is not None and abs(state["flow"]) <= 1 and state.get("count") is not None:
        label = "1м" if state["window"] == "1m" else "наблюдаемые ≤60с"
        parts.append(f"Покупки {(state['flow'] + 1) * 50:.0f}% объёма · {int(state['count'])} сделок · {label}")
    zone = state.get("zone")
    if zone:
        side = "Спрос" if zone["side"] == "bid" else "Предложение"
        parts.append(
            f"{side} {number(zone['price'], 8)} · {number(zone['relative_size'])}× "
            f"медианы уровней · видна {zone['observed_age_seconds']:.0f}с"
        )
    if pulse and pulse.get("status") == "ready":
        flow = pulse.get("flow")
        if flow in {"aligned_buy", "aligned_sell"}:
            parts.append("HL и OKX: совпадает сторона потока · закрытая 1м")
        elif flow == "opposed":
            parts.append("HL и OKX: стороны потока расходятся · закрытая 1м")
    clock = datetime.fromtimestamp(packet["captured_at"], MSK).strftime("%H:%M:%S")
    parts.append(f"Данные HL · график Binance · {clock} МСК")
    return "\n".join(parts), "<p>" + "<br>".join(parts) + "</p>"


class TelegramUX:
    def __init__(self, service):
        self.service = service
        self.db = service.journal.db
        self.tiger = TigerBridge()
        self.watch_at = 0
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
        return dict(inline_keyboard=[buttons, [dict(text="Копировать тикер", copy_text=dict(text=packet["symbol"]))]])

    def audit(self, packet, reason, status, state):
        self.db.execute(
            "INSERT INTO telegram_ux_events(episode,at,reason,status,packet,state) VALUES(?,?,?,?,?,?)",
            (packet["episode_id"], packet["captured_at"], reason, status, packet["packet_id"], json.dumps(state)),
        )

    def observe(self, now):
        service = self.service
        for packet in service.context_packets.values():
            if not packet_ready(packet, now):
                continue
            memory = service.context_memory.get(packet["episode_id"])
            priority, reason, state = decide(packet, memory["samples"])
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
            if priority is None:
                continue
            # A quiet large level alone is not a reason to interrupt the trader.
            active = (state["ratio"] is not None and state["ratio"] >= 2) or (
                state.get("flow") is not None and abs(state["flow"]) >= 0.6 and (state.get("count") or 0) >= 20
            )
            if not active:
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
                expanded = state["scale"] == "5m" and baseline.get("scale") != "5m"
                stronger_zone = bool(
                    zone and old and zone["id"] == old["id"] and zone["relative_size"] >= old["relative_size"] * 1.75
                )
                if now - previous["last_at"] < 120 or not (new_zone or stronger or expanded or stronger_zone):
                    continue
                kind, reason = "strengthening", "new_level" if new_zone else "strengthened"
                generation += 1
            # Existing delivered legacy episodes establish a baseline; no retrospective push.
            legacy = self.db.execute(
                "SELECT message_id FROM episode_cards WHERE episode_id=?", (packet["episode_id"],)
            ).fetchone()
            migrated = not previous and legacy and legacy["message_id"]
            card = f"ux:ops:{packet['episode_id']}:{generation}"
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
                        chat_id=str(service.hub.alerts.telegram_chat_id),
                        destination="operations",
                        reply_markup=self.markup(card, packet, now + 120),
                    )
                    self.audit(packet, reason, "queued", state)
                self.db.execute(
                    "INSERT OR REPLACE INTO telegram_episodes VALUES(?,?,?,?,?)",
                    (packet["episode_id"], packet["symbol"], json.dumps(state), generation, now),
                )
        if now - self.watch_at >= 60:
            self.watch(now)

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
                symbol = row["symbol"]
                packet = service.context_packets.get(symbol)
                ready = bool(packet and packet_ready(packet, now))
                why = (
                    TITLES.get(
                        (service.context_memory.latest(packet["episode_id"]) or {}).get("reason"),
                        "относительная активность",
                    )
                    if packet
                    else "относительная активность"
                )
                stage = {"focus": "в фокусе", "probe": "проверяем", "cooling": "затухает"}.get(
                    row["state"], "наблюдение"
                )
                lines.append(
                    f"<b>{html.escape(symbol)}</b> · {stage if ready else 'прогрев/нет свежих данных'} · {why}"
                )
            if not dynamic:
                lines.append("Содержательных кандидатов пока нет.")
        lines.append("Без отдельного оповещения до появления ситуации.")
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

    def publish_analysis(self, response):
        packet = response["packet"]
        review = response.get("review") or {}
        result = review.get("result") or {}
        if response.get("status") != "ok" or result.get("abstention") or result.get("expired_at_completion", True):
            return False
        meta = packet["symbol"] == "MARKET"
        card = "ux:analysis:" + packet["episode_id"]
        if meta:
            card += ":" + packet["packet_id"][:16]
        current = summarize(packet) if not meta else None
        heading = "Связи эпизодов · обзор рынка" if meta else html.escape(packet["symbol"]) + " · Разбор ситуации"
        lines = ["<b>" + heading + "</b>"]
        if meta:
            symbols = sorted({f["id"].split(":")[0] for f in packet["facts"] if ":" in f["id"]})
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
            + " · данные Hyperliquid."
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
            callback_errors=self.db.execute("SELECT COUNT(*) FROM telegram_clicks WHERE status='error'").fetchone()[0],
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
        if action["expires_at"] < now:
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

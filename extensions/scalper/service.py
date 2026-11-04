"""Schedule significant native signals, independent analysis and notifications."""
import asyncio
import hashlib
import html
import logging
import math
import time
from datetime import datetime, timedelta, timezone

from .activity import active_anomalies, refresh_candles
from .analysis import DIRECTIONAL, compare, snapshot_key
from .journal import dump
from .market import WINDOWS, capture
from .quality import calculate_quality

LOG = logging.getLogger(__name__)
AI_INTERVAL = 12
EVENT_COOLDOWN = 60
ALERT_COOLDOWN = 300
HYPOTHESIS_MAX_AGE = 900
QUEUE_LIMIT = 20
ALERT_CATEGORIES = {"CONSENSUS_LONG", "CONSENSUS_SHORT", "DIVERGENCE"}


ALERT_TITLES = {
    "CONSENSUS_LONG": "Обе оценки указывают вверх",
    "CONSENSUS_SHORT": "Обе оценки указывают вниз",
    "DIVERGENCE": "Оценки расходятся",
    "HYPERDATA_ONLY": "Направление видит только HyperData",
    "AI_ONLY": "Направление видит только AI",
    "NEUTRAL": "Обе оценки нейтральны",
    "INSUFFICIENT_DATA": "Направление не подтверждено",
}
ALERT_DIRECTIONS = {
    "BULLISH": "вверх",
    "BEARISH": "вниз",
    "NEUTRAL": "нейтрально",
    "NOT_AVAILABLE": "оценка недоступна",
    "INSUFFICIENT_DATA": "недостаточно данных",
}
MOSCOW = timezone(timedelta(hours=3), "МСК")


def alert_text(snapshot, native, ai, consensus):
    stamp = datetime.fromtimestamp(snapshot["captured_at"], MOSCOW).strftime("%d.%m.%Y в %H:%M:%S %Z")
    title = ALERT_TITLES[consensus["category"]]
    native_direction = ALERT_DIRECTIONS[native["direction"]]
    ai_direction = ALERT_DIRECTIONS.get(ai.get("direction"), "оценка недоступна")
    ofi = snapshot["metrics"]["ofi"]["value"]
    flow = ""
    if "ofi" in native["evidence"] and ofi:
        flow = " (в потоке преобладают покупки)" if ofi > 0 else " (в потоке преобладают продажи)"
    lines = [
        f'<b>{html.escape(snapshot["contract"])} · {title}</b>',
        f'Hyperliquid · окно {html.escape(snapshot["window"])}',
        "",
        f"<b>HyperData</b> — {native_direction}{flow}",
        f"<b>AI</b> — {ai_direction}",
    ]
    arguments = ai.get("arguments") or []
    explanation = ai.get("summary") or (arguments[0]["reason"] if arguments else None)
    if explanation:
        lines.extend(("", f"<b>Почему</b> — {html.escape(explanation)}"))
    contradictions = ai.get("contradictions") or []
    if contradictions:
        lines.append(f'<b>Что против</b> — {html.escape(contradictions[0]["reason"])}')
    if ai.get("invalidation"):
        lines.append(f'<b>Когда пересмотреть</b> — {html.escape(ai["invalidation"])}')
    lines.extend(("", f"<i>Снимок рынка: {stamp}</i>"))
    return "\n".join(lines)


class Screener:
    def __init__(self, hub, journal, gemini, notify):
        self.hub, self.journal, self.gemini, self.notify = hub, journal, gemini, notify
        self.rows = {}
        self.quality_report = None
        self.candles = {}
        self.states = {}
        self.pending = set()
        self.cooldowns = {}
        self.sequence = 0
        self.jobs = asyncio.PriorityQueue(maxsize=QUEUE_LIMIT)
        self.notifications = asyncio.Queue(maxsize=QUEUE_LIMIT)

    def scan(self, now):
        assets = self.hub.get_all_assets()
        rows = {}
        for asset in assets[:20]:
            for window in WINDOWS:
                key = (asset.symbol, window)
                snapshot, native = capture(self.hub, asset, window, now, self.candles.get(key))
                native["snapshot"] = snapshot_key(snapshot)
                rows[key] = {"snapshot": snapshot, "hyperdata": native}
                if not self.hub.demo:
                    self.detect(key, snapshot, native, now)
        self.rows = rows
        self.states = {key: value for key, value in self.states.items() if key in rows}
        self.cooldowns = {key: until for key, until in self.cooldowns.items() if until > now}

        prices = {}
        market_at = self.hub.market_timestamp()
        if not self.hub.demo and market_at is not None and self.hub.market_age() <= 20:
            prices = {asset.symbol: (asset.mark_price, market_at) for asset in assets
                      if "price" in self.hub.market.observed_fields.get(asset.symbol, set())
                      and math.isfinite(asset.mark_price) and asset.mark_price > 0}
        self.journal.observe(now, prices)

    def detect(self, key, snapshot, native, now):
        direction = native["direction"]
        anomalies = active_anomalies(snapshot)
        previous, was_strong, previous_anomalies = self.states.get(key, (None, False, frozenset()))
        new_anomalies = anomalies - previous_anomalies
        strong = native["native_signal"] in {"STRONG_BULL", "STRONG_BEAR"}
        if direction == "NOT_AVAILABLE":
            return
        trigger = ((direction in DIRECTIONAL and direction != previous)
                   or (previous in DIRECTIONAL and direction == "NEUTRAL")
                   or (strong and not was_strong) or bool(new_anomalies))
        if not trigger:
            self.states[key] = direction, strong, anomalies
            return
        if key in self.pending or self.jobs.full() or self.cooldowns.get(key, 0) > now:
            return

        event = {
            "type": "market_activity" if new_anomalies else "orderflow_transition",
            "anomalies": sorted(new_anomalies), "previous_direction": previous,
            "direction": direction, "native_signal": native["native_signal"],
            "source": native["source"],
        }
        self.states[key] = direction, strong, anomalies
        self.pending.add(key)
        self.sequence += 1
        # New candidates in one scan have the same age. Let strong events
        # lead that batch, while older candidates retain priority over new ones.
        priority = 0 if new_anomalies else 1 if strong else 2 if direction in DIRECTIONAL else 3
        self.jobs.put_nowait((now, priority, self.sequence, key, event))

    async def run(self):
        while True:
            try:
                self.scan(time.time())
            except Exception:
                LOG.exception("Screener scan failed")
            await asyncio.sleep(5)

    async def refresh_activity(self):
        await refresh_candles(self.hub, WINDOWS, self.candles)

    async def refresh_quality(self, path):
        while True:
            try:
                self.quality_report = await asyncio.to_thread(calculate_quality, path, time.time(), WINDOWS)
            except Exception:
                LOG.exception("AI quality report refresh failed")
            await asyncio.sleep(60)

    async def analyze(self):
        next_request = 0
        while True:
            if self.gemini.key:
                await asyncio.sleep(max(0, next_request - time.monotonic()))
            detected_at, _, _, candidate, event = await self.jobs.get()
            event_id = None
            try:
                now = time.time()
                row = self.rows.get(candidate)
                if not row:
                    continue
                snapshot, native = row["snapshot"], row["hyperdata"]
                if (now - snapshot["captured_at"] > 10
                        or snapshot["metrics"]["price"]["quality"] != "ok"
                        or native["direction"] == "NOT_AVAILABLE"):
                    self.states.pop(candidate, None)
                    continue
                if event["anomalies"] and not active_anomalies(snapshot).intersection(event["anomalies"]):
                    self.states.pop(candidate, None)
                    continue
                if native["direction"] != event["direction"]:
                    # The original transition is obsolete. Let the next scan
                    # decide whether the new direction merits a fresh event.
                    continue
                event = {**event, "detected_at": detected_at, "analyzed_signal": native["native_signal"]}
                event_id = hashlib.sha256(dump(snapshot_key(snapshot)).encode()).hexdigest()
                if not self.journal.record(event_id, event, snapshot, native):
                    continue
                self.cooldowns[candidate] = now + EVENT_COOLDOWN
                next_request = time.monotonic() + AI_INTERVAL
                ai = await self.gemini.analyze(snapshot)
                verdict = compare(snapshot, native, ai)
                self.journal.finish(event_id, ai, verdict, time.time())
                self.schedule_alert(event_id, snapshot, native, ai, verdict)
            except Exception:
                LOG.exception("Event analysis failed")
                if event_id is not None:
                    self.journal.delivery(event_id, "error")
            finally:
                self.pending.discard(candidate)
                self.jobs.task_done()

    def schedule_alert(self, key, snapshot, native, ai, verdict):
        symbol, window = snapshot["symbol"], snapshot["window"]
        previous = self.journal.latest(symbol, window, before=snapshot["captured_at"], completed=True)
        changed = False
        if (ai["status"] == "ok" and previous
                and snapshot["captured_at"] - previous[0]["captured_at"] <= HYPOTHESIS_MAX_AGE):
            last = previous[0]
            prior_ai = last["ai"].get("direction")
            direction = ai.get("direction")
            native_changed = (last["hyperdata"]["direction"] in DIRECTIONAL
                              and last["hyperdata"]["direction"] != native["direction"])
            model_changed = prior_ai in DIRECTIONAL and direction is not None and prior_ai != direction and direction != "INSUFFICIENT_DATA"
            changed = native_changed or model_changed
        state = "not_applicable"
        if verdict["category"] in ALERT_CATEGORIES or changed:
            if not self.notify:
                state = "disabled"
            elif self.notifications.full():
                state = "queue_full"
            else:
                self.notifications.put_nowait((key, snapshot, native, ai, verdict))
                return
        self.journal.delivery(key, state)

    async def deliver(self):
        while True:
            key, snapshot, native, ai, verdict = await self.notifications.get()
            try:
                now = time.time()
                last = self.journal.last_delivery(snapshot["symbol"], snapshot["window"])
                if last and now - last < ALERT_COOLDOWN:
                    self.journal.delivery(key, "cooldown")
                    continue
                if now - snapshot["captured_at"] > 120:
                    self.journal.delivery(key, "expired")
                    continue
                self.journal.delivery(key, "sending", now)
                sent = await self.hub.alerts.send_telegram(alert_text(snapshot, native, ai, verdict))
                self.journal.delivery(key, "sent" if sent else "failed_or_unconfirmed",
                                      time.time() if sent else now)
            except Exception:
                LOG.exception("Notification processing failed")
            finally:
                self.notifications.task_done()

"""Schedule significant native signals, independent analysis and notifications."""
import asyncio
import hashlib
import html
import logging
import math
import time
from datetime import datetime, timedelta, timezone

from .activity import CANDLE_SOURCE, active_anomalies, refresh_candles
from .analysis import DIRECTIONAL, compare, snapshot_key
from .journal import dump
from .market import WINDOWS, capture, metric
from .quality import calculate_quality

LOG = logging.getLogger(__name__)
AI_INTERVAL = 12
EVENT_COOLDOWN = 60
ALERT_COOLDOWN = 300
HYPOTHESIS_MAX_AGE = 900
QUEUE_LIMIT = 20
WIDE_SPREAD_ATR_FRACTION = 0.25
PRIORITY_STEP_SECONDS = 15
CONTEXT_WINDOW_SECONDS = 900
CONTEXT_TRIAL_INTERVAL = 10
ALERT_CATEGORIES = {"CONSENSUS_LONG", "CONSENSUS_SHORT", "DIVERGENCE"}


ALERT_TITLES = {
    "CONSENSUS_LONG": "🟢 Совпадение · вверх",
    "CONSENSUS_SHORT": "🔴 Совпадение · вниз",
    "DIVERGENCE": "🟠 Оценки расходятся",
    "HYPERDATA_ONLY": "🔵 Направление видит только HyperData",
    "AI_ONLY": "🔵 Направление видит только AI",
    "NEUTRAL": "⚪ Обе оценки нейтральны",
    "INSUFFICIENT_DATA": "⚪ Направление не подтверждено",
}
ALERT_DIRECTIONS = {
    "BULLISH": "🟢 вверх",
    "BEARISH": "🔴 вниз",
    "NEUTRAL": "⚪ нейтрально",
    "NOT_AVAILABLE": "⚪ оценка недоступна",
    "INSUFFICIENT_DATA": "⚪ недостаточно данных",
}
MOSCOW = timezone(timedelta(hours=3), "МСК")


def alert_text(snapshot, native, ai, consensus):
    stamp = datetime.fromtimestamp(snapshot["captured_at"], MOSCOW).strftime("%d.%m.%Y в %H:%M:%S %Z")
    title = ALERT_TITLES[consensus["category"]]
    native_direction = ALERT_DIRECTIONS[native["direction"]]
    ai_direction = ALERT_DIRECTIONS.get(ai.get("direction"), ALERT_DIRECTIONS["NOT_AVAILABLE"])
    ofi = snapshot["metrics"]["ofi"]["value"]
    flow = ""
    if "ofi" in native["evidence"] and ofi:
        flow = " (в потоке преобладают покупки)" if ofi > 0 else " (в потоке преобладают продажи)"
    lines = [
        f"<b>{title}</b>",
        f'<b>{html.escape(snapshot["contract"])}</b> · {html.escape(snapshot["window"])} · Hyperliquid',
        "",
        f"<b>HyperData</b> — {native_direction}{flow}",
        f"<b>AI</b> — {ai_direction}",
    ]
    arguments = ai.get("arguments") or []
    explanation = ai.get("summary") or (arguments[0]["reason"] if arguments else None)
    contradictions = ai.get("contradictions") or []
    details = (
        ("💬", "Почему", explanation),
        ("⚠️", "Что против", contradictions[0]["reason"] if contradictions else None),
        ("🔄", "Когда пересмотреть", ai.get("invalidation")),
    )
    for icon, label, detail in details:
        if detail:
            lines.extend(("", f"{icon} <b>{label}</b>", html.escape(detail)))
    lines.extend(("", f"🕒 <i>Снимок рынка · {stamp}</i>"))
    return "\n".join(lines)


def minute_gate(snapshot):
    if snapshot["window"] != "1m":
        return "ready"
    metrics = snapshot["metrics"]
    if metrics["ofi"]["quality"] != "ok":
        return "tape_unavailable"
    if metrics["spread"]["quality"] != "ok":
        return "book_unavailable"
    spread, mark, natr = (metrics[name] for name in ("spread", "price", "natr"))
    if (mark["quality"] == natr["quality"] == "ok" and mark["value"] > 0 and natr["value"] > 0
            and 100 * spread["value"] / mark["value"] >= WIDE_SPREAD_ATR_FRACTION * natr["value"]):
        return "wide_spread"
    return "ready"


def five_minute_relation(native, context):
    if not context:
        return "unavailable"
    other = context["hyperdata"]["direction"]
    if other == "NOT_AVAILABLE":
        return "unavailable"
    direction = native["direction"]
    if direction in DIRECTIONAL and other in DIRECTIONAL:
        return "aligned" if direction == other else "opposed"
    return "neutral"


def with_context(snapshot, context):
    if not context:
        return snapshot
    other = context["snapshot"]
    window = other["window"]
    if (window not in WINDOWS or window == snapshot["window"]
            or other["symbol"] != snapshot["symbol"]
            or other["captured_at"] != snapshot["captured_at"]):
        return snapshot
    keys = ("ofi", "trade_count", "activity_ratio", "natr", "range_ratio",
            "first_trade_price", "last_trade_price", "spread", "imbalance")
    metrics = {name: other["metrics"][name] for name in keys
               if other["metrics"][name]["quality"] == "ok"}
    if not metrics:
        return snapshot
    return {**snapshot, f"context_{window}": {"window": window, "captured_at": other["captured_at"],
                                            "metrics": metrics}}


def with_senior_context(snapshot, candles):
    if not candles:
        return snapshot
    closed_at = candles["closed_at"]
    if not 0 <= snapshot["captured_at"] - closed_at <= CONTEXT_WINDOW_SECONDS + 90:
        return snapshot
    units = {"return_4": "%", "range_4": "%", "range_position_4": "ratio", "natr": "%"}
    metrics = {name: metric(candles[name], CANDLE_SOURCE, closed_at, unit)
               for name, unit in units.items() if candles[name] is not None}
    return {**snapshot, "context_15m": {"window": "15m", "captured_at": closed_at, "metrics": metrics}}


class Screener:
    def __init__(self, hub, journal, gemini, notify):
        self.hub, self.journal, self.gemini, self.notify = hub, journal, gemini, notify
        self.rows = {}
        self.quality_report = None
        self.candles = {}
        self.context_events = 0
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
        for key, row in rows.items():
            snapshot, native = row["snapshot"], row["hyperdata"]
            context = rows.get((key[0], "5m")) if key[1] == "1m" else None
            row["analysis_gate"] = minute_gate(snapshot)
            row["context_5m"] = five_minute_relation(native, context)
            if not self.hub.demo:
                self.detect(key, snapshot, native, now, row["analysis_gate"], row["context_5m"])
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

    def detect(self, key, snapshot, native, now, gate, relation):
        direction = native["direction"]
        anomalies = active_anomalies(snapshot)
        previous, was_strong, previous_anomalies = self.states.get(key, (None, False, frozenset()))
        new_anomalies = anomalies - previous_anomalies
        strong = native["native_signal"] in {"STRONG_BULL", "STRONG_BEAR"}
        if direction == "NOT_AVAILABLE" or gate in {"tape_unavailable", "book_unavailable"}:
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
            "source": native["source"], "detected_at": now,
        }
        self.states[key] = direction, strong, anomalies
        self.pending.add(key)
        self.sequence += 1
        # New candidates in one scan have the same age. Let strong events
        # lead that batch, while older candidates retain priority over new ones.
        priority = 0 if new_anomalies else 1 if strong else 2 if direction in DIRECTIONAL else 3
        # Bounded adjustments let agreement and book quality matter without starving old events.
        order_at = now + PRIORITY_STEP_SECONDS * ((gate == "wide_spread") - (relation == "aligned"))
        self.jobs.put_nowait((order_at, priority, self.sequence, key, event))

    async def run(self):
        while True:
            try:
                self.scan(time.time())
            except Exception:
                LOG.exception("Screener scan failed")
            await asyncio.sleep(5)

    async def refresh_activity(self):
        await refresh_candles(self.hub, {**WINDOWS, "15m": CONTEXT_WINDOW_SECONDS}, self.candles)

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
            _, _, _, candidate, event = await self.jobs.get()
            event_id = None
            try:
                now = time.time()
                row = self.rows.get(candidate)
                if not row:
                    continue
                snapshot, native = row["snapshot"], row["hyperdata"]
                if (now - snapshot["captured_at"] > 10
                        or snapshot["metrics"]["price"]["quality"] != "ok"
                        or native["direction"] == "NOT_AVAILABLE"
                        or row["analysis_gate"] in {"tape_unavailable", "book_unavailable"}):
                    self.states.pop(candidate, None)
                    continue
                if event["anomalies"] and not active_anomalies(snapshot).intersection(event["anomalies"]):
                    self.states.pop(candidate, None)
                    continue
                if native["direction"] != event["direction"]:
                    # The original transition is obsolete. Let the next scan
                    # decide whether the new direction merits a fresh event.
                    continue
                other_window = "5m" if candidate[1] == "1m" else "1m"
                baseline = with_context(snapshot, self.rows.get((candidate[0], other_window)))
                snapshot = with_senior_context(baseline, self.candles.get((candidate[0], "15m")))
                event = {**event, "analyzed_signal": native["native_signal"],
                         "analysis_gate": row["analysis_gate"], "context_5m": row["context_5m"]}
                event_id = hashlib.sha256(dump(snapshot_key(snapshot)).encode()).hexdigest()
                if not self.journal.record(event_id, event, snapshot, native):
                    continue
                self.cooldowns[candidate] = now + EVENT_COOLDOWN
                next_request = time.monotonic() + AI_INTERVAL
                ai = await self.gemini.analyze(snapshot)
                verdict = compare(snapshot, native, ai)
                self.journal.finish(event_id, ai, verdict, time.time())
                self.schedule_alert(event_id, snapshot, native, ai, verdict)
                if self.gemini.key and "context_15m" in snapshot and f"context_{other_window}" in baseline:
                    sample = self.context_events % CONTEXT_TRIAL_INTERVAL == 0
                    self.context_events += 1
                    if sample:
                        await asyncio.sleep(max(0, next_request - time.monotonic()))
                        next_request = time.monotonic() + AI_INTERVAL
                        try:
                            comparison = await self.gemini.analyze(baseline)
                            if comparison.get("snapshot") != snapshot_key(snapshot):
                                raise ValueError("Mismatched comparison snapshot")
                            self.journal.record_context_trial(event_id, comparison, time.time())
                        except Exception:
                            LOG.exception("Context comparison failed")
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

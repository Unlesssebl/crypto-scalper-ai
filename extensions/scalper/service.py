"""Schedule significant native signals, independent analysis and notifications."""
import asyncio
import hashlib
import html
import logging
import math
import time
from datetime import datetime, timedelta, timezone

from .activity import CANDLE_SOURCE, active_anomalies, finding_strength, refresh_candles, wide_spread
from .analysis import DIRECTIONAL, compare, snapshot_key
from .journal import dump
from .market import WINDOWS, capture, metric
from .quality import calculate_quality
from .profile import DAY, calculate_profiles, fresh, profile_context, sample_window, situation_facts

LOG = logging.getLogger(__name__)
AI_INTERVAL = 12
EVENT_COOLDOWN = 60
ALERT_COOLDOWN = 300
QUEUE_LIMIT = 20
PRIORITY_STEP_SECONDS = 15
STRENGTH_STEP_SECONDS = 10
CONTEXT_WINDOW_SECONDS = 900
CONTEXT_TRIAL_INTERVAL = 10
ALERT_DIRECTIONS = {
    "BULLISH": "🟢 вверх",
    "BEARISH": "🔴 вниз",
    "NEUTRAL": "⚪ нейтрально",
    "NOT_AVAILABLE": "⚪ оценка недоступна",
    "INSUFFICIENT_DATA": "⚪ недостаточно данных",
}
MOSCOW = timezone(timedelta(hours=3), "МСК")


def alert_text(snapshot, ai, event):
    stamp = datetime.fromtimestamp(snapshot["captured_at"], MOSCOW).strftime("%d.%m.%Y в %H:%M:%S %Z")
    labels = {"tape_burst": "Всплеск ленты", "range_burst": "Всплеск диапазона"}
    reason = " + ".join(labels[name] for name in event.get("anomalies", ()) if name in labels)
    lines = [
        f'<b>🟡 {html.escape(snapshot["contract"])} · Стоит посмотреть</b>',
        f'Hyperliquid · {html.escape(snapshot["window"])} · {reason or "Смена направления потока"}',
        "",
        "<b>📊 Что происходит</b>",
        *(html.escape(fact) for fact in (event.get("facts") or situation_facts(snapshot))),
    ]
    strength = event.get("strength")
    if strength:
        lines.insert(2, f'{strength["icon"]} <b>Сила находки: {html.escape(strength["label"])}</b>')
    if ai["status"] == "ok":
        direction = ALERT_DIRECTIONS.get(ai.get("direction"), ALERT_DIRECTIONS["NOT_AVAILABLE"])
        hypothesis = [f"<b>🧠 Гипотеза AI · {direction}</b>"]
        if ai.get("scenario"):
            hypothesis.append(html.escape(ai["scenario"]))
        if ai.get("watch"):
            hypothesis.append(f'<b>Что наблюдать:</b> {html.escape(ai["watch"])}')
        if ai.get("invalidation"):
            hypothesis.append(f'<b>Когда пересмотреть:</b> {html.escape(ai["invalidation"])}')
        lines.extend(("", "<blockquote>" + "\n".join(hypothesis) + "</blockquote>"))
    else:
        lines.extend(("", "<i>Гипотеза AI недоступна для этого снимка.</i>"))
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
    return "wide_spread" if wide_spread(snapshot) else "ready"


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
        self.profiles = {}
        self.sample_slots = {}
        self.last_profile_prune = 0
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
        samples = []
        for key, row in rows.items():
            snapshot, native = row["snapshot"], row["hyperdata"]
            context = rows.get((key[0], "5m")) if key[1] == "1m" else None
            profile = self.profiles.get(key)
            row["facts"] = situation_facts(snapshot, profile)
            row["strength"] = finding_strength(snapshot, native)
            row["profile"] = profile
            row["analysis_gate"] = minute_gate(snapshot)
            row["context_5m"] = five_minute_relation(native, context)
            if not self.hub.demo:
                self.detect(key, snapshot, native, now, row["analysis_gate"], row["context_5m"])
                sample = sample_window(snapshot, WINDOWS[key[1]])
                if sample and self.sample_slots.get(key) != sample[2]:
                    samples.append(sample)
        if samples:
            self.journal.record_instrument_samples(samples)
            self.sample_slots.update({(sample[0], sample[1]): sample[2] for sample in samples})
        self.sample_slots = {key: slot for key, slot in self.sample_slots.items() if key in rows}
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
        strength = finding_strength(snapshot, native)["rank"]
        order_at = (now + PRIORITY_STEP_SECONDS * ((gate == "wide_spread") - (relation == "aligned"))
                    - STRENGTH_STEP_SECONDS * max(0, strength - 1))
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

    async def refresh_profiles(self, path):
        while True:
            try:
                now = time.time()
                symbols = sorted({symbol for symbol, _ in self.rows})
                if symbols:
                    self.profiles = await asyncio.to_thread(calculate_profiles, path, now, symbols)
                    if now - self.last_profile_prune > DAY:
                        self.journal.prune_instrument_samples(now - 30 * DAY)
                        self.last_profile_prune = now
            except Exception:
                LOG.exception("Instrument profile refresh failed")
            await asyncio.sleep(300 if self.profiles else 30)

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
                profile = profile_context(self.profiles.get(candidate))
                if profile:
                    baseline = {**baseline, "instrument_profile": profile}
                snapshot = with_senior_context(baseline, self.candles.get((candidate[0], "15m")))
                event = {**event, "analyzed_signal": native["native_signal"],
                         "analysis_gate": row["analysis_gate"], "context_5m": row["context_5m"],
                         "facts": row["facts"], "strength": row["strength"]}
                event_id = hashlib.sha256(dump(snapshot_key(snapshot)).encode()).hexdigest()
                if not self.journal.record(event_id, event, snapshot, native):
                    continue
                self.cooldowns[candidate] = now + EVENT_COOLDOWN
                next_request = time.monotonic() + AI_INTERVAL
                ai = await self.gemini.analyze(snapshot)
                verdict = compare(snapshot, native, ai)
                self.journal.finish(event_id, ai, verdict, time.time())
                self.schedule_alert(event_id, snapshot, native, ai, event)
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

    def schedule_alert(self, key, snapshot, native, ai, event):
        metrics = snapshot.get("metrics", {})
        start, end = (fresh(metrics, name) for name in ("first_trade_price", "last_trade_price"))
        direction = native["direction"]
        reversal = (event.get("previous_direction") in DIRECTIONAL
                    and direction in DIRECTIONAL and event["previous_direction"] != direction)
        price_agrees = (start is not None and end is not None and start != end
                        and (end > start) == (direction == "BULLISH"))
        state = "not_applicable"
        if event.get("anomalies") or (reversal and price_agrees):
            if not self.notify:
                state = "disabled"
            elif self.notifications.full():
                state = "queue_full"
            else:
                self.notifications.put_nowait((key, snapshot, ai, event))
                return
        self.journal.delivery(key, state)

    async def deliver(self):
        while True:
            key, snapshot, ai, event = await self.notifications.get()
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
                sent = await self.hub.alerts.send_telegram(alert_text(snapshot, ai, event))
                self.journal.delivery(key, "sent" if sent else "failed_or_unconfirmed",
                                      time.time() if sent else now)
            except Exception:
                LOG.exception("Notification processing failed")
            finally:
                self.notifications.task_done()

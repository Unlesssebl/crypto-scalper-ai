"""Schedule significant native signals, independent analysis and notifications."""
import asyncio
import hashlib
import html
import logging
import math
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from hyperdata_terminal.utils.helpers import format_price, format_usd

from .audit import CONTROL_INTERVAL, STRONG_RANK
from .activity import CANDLE_SOURCE, active_anomalies, btc_context, finding_strength, refresh_candles, wide_spread
from .analysis import DIRECTIONAL, compare, snapshot_key
from .journal import dump
from .market import WINDOWS, capture, metric
from .shadow import ShadowDetector
from .quality import calculate_quality
from .phase import PhaseTracker
from .profile import DAY, calculate_profiles, fresh, profile_context, sample_window, situation_facts

LOG = logging.getLogger(__name__)
AI_INTERVAL = 12
EVENT_COOLDOWN = 60
ALERT_COOLDOWN = 300
MAX_EVENT_AGE = {"1m": 45, "5m": 120}
QUEUE_LIMIT = 20
PRIORITY_STEP_SECONDS = 15
STRENGTH_STEP_SECONDS = 10
CONTEXT_WINDOW_SECONDS = 900
CONTEXT_TRIAL_INTERVAL = 10
ALERT_DIRECTIONS = {
    "BULLISH": "🟢 вверх",
    "BEARISH": "🔴 вниз",
    "NEUTRAL": "⚪ нейтрально",
    "NOT_AVAILABLE": "оценка недоступна",
    "INSUFFICIENT_DATA": "недостаточно данных",
}
MOSCOW = timezone(timedelta(hours=3), "МСК")


def futures_ticker(snapshot):
    return f'HYPERLIQUID:{snapshot["symbol"].upper()}USDC.P'


def alert_market_facts(snapshot):
    metrics = snapshot["metrics"]
    facts = []
    start, end, pace, rate = (fresh(metrics, key) for key in (
        "first_trade_price", "last_trade_price", "activity_ratio", "trade_rate",
    ))
    impulse = []
    if start and end is not None:
        change = f"{100 * (end / start - 1):+.2f}%"
        marker = {"-": "🔻 ", "+": "🟢 "}[change[0]] if change[1:] != "0.00%" else ""
        impulse.append(f"{marker}<b>{change}</b> за окно")
    if pace is not None:
        impulse.append(f"<b>{pace - 1:+.0%}</b> к обычному темпу")
    elif rate is not None:
        impulse.append(f"{rate:.1f} сделок/с")
    if impulse:
        facts.append(("Импульс", " · ".join(impulse)))

    volume, cvd, ofi = (fresh(metrics, key) for key in ("volume", "cvd", "ofi"))
    flow = []
    if ofi is not None:
        flow.append(f"перевес сделок <b>{ofi:+.0%}</b>")
    if cvd is not None:
        flow.append(f"дельта объёма {'+' if cvd > 0 else ''}{format_usd(cvd)}")
    if volume is not None:
        flow.append(f"объём {format_usd(volume)}")
    if flow:
        facts.append(("Поток", " · ".join(flow)))

    spread, price, imbalance = (fresh(metrics, key) for key in ("spread", "price", "imbalance"))
    book = []
    if imbalance is not None:
        book.append(f"дисбаланс <b>{imbalance:+.0%}</b>")
    if price and spread is not None:
        book.append(f"спред {100 * spread / price:.3f}%")
    if book:
        facts.append(("Стакан", " · ".join(book)))
    return facts


def alert_text(snapshot, native, ai, event, *, rich=False, updates=()):
    stamp = datetime.fromtimestamp(snapshot["captured_at"], MOSCOW).strftime("%H:%M:%S %Z")
    labels = {"tape_burst": "Всплеск ленты", "range_burst": "Всплеск диапазона"}
    reason = " + ".join(labels[name] for name in event.get("anomalies", ()) if name in labels)
    ticker = futures_ticker(snapshot)
    market_url = f'https://app.hyperliquid.xyz/trade/{quote(snapshot["symbol"], safe="")}'
    headline = f'<b><a href="{market_url}">{html.escape(ticker)}</a></b> · {html.escape(snapshot["window"])}'
    price = fresh(snapshot["metrics"], "price")
    if price is not None:
        headline += f" · Mark {format_price(price)}"
    strength = event.get("strength")
    trigger = f'<b>Рынок</b> · {reason or "Смена направления потока"}'
    if strength:
        trigger += f' · {html.escape(strength["icon"])} <b>Сила: {html.escape(strength["label"])}</b>'
    phase = event.get("movement_phase")
    if phase and phase["phase"] not in {"unavailable", "untracked"}:
        trigger += f'<br><b>Фаза:</b> {html.escape(phase["label"])}' if rich else f'\n<b>Фаза:</b> {html.escape(phase["label"])}'
        if phase["distance_natr"] is not None:
            trigger += f' · {phase["distance_natr"]:+.1f} NATR от начала'
        trigger += f' · {phase["age_seconds"]} с'
    characterization = hypothesis = None
    ai_direction = ALERT_DIRECTIONS["NOT_AVAILABLE"]
    if ai["status"] == "ok":
        ai_direction = ALERT_DIRECTIONS.get(ai.get("direction"), ai_direction)
        characterization = ai.get("summary")
        hypothesis = ai.get("scenario")
    if ai["status"] == "pending":
        ai_direction = "характеристика пока не готова"
    native_direction = ALERT_DIRECTIONS.get(native["direction"], ALERT_DIRECTIONS["NOT_AVAILABLE"])
    header = [headline, f"<i>Бессрочный фьючерс · {stamp}</i>"]
    facts = alert_market_facts(snapshot)
    if btc := snapshot.get("btc_context"):
        facts.append(("Фон BTC", f'BTC {btc["btc_last_return_pct"]:+.2f}% · актив {btc["asset_last_return_pct"]:+.2f}%'
                                 f' за 5m · корреляция {btc["correlation"]:+.2f} за 3 ч'))
    order_flow = f"<b>HyperData</b> {native_direction} · <b>AI</b> {ai_direction}"
    updates_text = []
    for update in updates:
        window = html.escape(update["window"])
        direction = ALERT_DIRECTIONS[update["hyperdata"]["direction"]]
        strength = html.escape(update["event"]["strength"]["label"])
        captured = datetime.fromtimestamp(update["captured_at"], MOSCOW).strftime("%H:%M:%S %Z")
        updates_text.append(f"<b>Обновление {window}</b> · {direction} · {strength} · {captured}")
    if rich:
        lines = [f"<p>{'<br>'.join(header)}</p>", "<hr/>", f"<p>{trigger}</p>"]
        if facts:
            table = "".join(f"<tr><th>{label}</th><td>{value}</td></tr>" for label, value in facts)
            lines.append(f"<table striped compact>{table}</table>")
        else:
            lines.append("<p>Свежие метрики недоступны.</p>")
        lines.append(f"<p>{order_flow}</p>")
        lines.extend(f"<p>{text}</p>" for text in updates_text)
        if characterization:
            lines.append(f"<p><b>🧠 Характеристика AI</b><br>{html.escape(characterization)}</p>")
        if hypothesis:
            lines.append(f"<blockquote><b>Гипотеза AI</b><br>{html.escape(hypothesis)}</blockquote>")
        lines.append(
            f'<tg-button-row align="left"><tg-button type="copy_text" text="{html.escape(ticker)}">'
            'Копировать фьючерс</tg-button></tg-button-row>'
        )
        return "\n".join(lines)
    market = [trigger, *(f"<b>{label}</b> · {value}" for label, value in facts)]
    if not facts:
        market.append("Свежие метрики недоступны.")
    message = "\n\n".join(("\n".join(header), "\n".join(market), order_flow, *updates_text))
    if characterization:
        message += f"\n\n<b>🧠 Характеристика AI</b>\n{html.escape(characterization)}"
    if hypothesis:
        message += f"\n\n<blockquote><b>Гипотеза AI</b>\n{html.escape(hypothesis)}</blockquote>"
    return message


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
    window = "5m" if snapshot["window"] == "1m" else "1m"
    context_key = f"context_{window}"
    if context_key in snapshot:
        return snapshot
    other = context["snapshot"] if context else None
    if (not other or other["window"] != window or other["symbol"] != snapshot["symbol"]
            or other["captured_at"] != snapshot["captured_at"]):
        if window == "1m":
            return snapshot
        return {**snapshot, context_key: {
            "window": window, "captured_at": None, "metrics": {},
            "flow_quality": {"ofi": "unavailable", "cvd": "unavailable"},
        }}
    keys = ("ofi", "cvd", "trade_count", "activity_ratio", "natr", "range_ratio",
            "first_trade_price", "last_trade_price", "spread", "imbalance")
    metrics = {name: other["metrics"][name] for name in keys
               if fresh(other["metrics"], name) is not None}
    return {**snapshot, context_key: {
        "window": window, "captured_at": other["captured_at"], "metrics": metrics,
        "flow_quality": {name: other["metrics"][name]["quality"] for name in ("ofi", "cvd")},
    }}


def with_movement_context(snapshot, phase):
    if not phase:
        return snapshot
    if "started_at" not in phase:
        return {**snapshot, "movement_context": {"status": phase["phase"]}}
    fields = ("age_seconds", "distance_natr", "retracement_natr", "natr_at_start")
    return {**snapshot, "movement_context": {
        "status": "observed", **{field: phase.get(field) for field in fields},
    }}


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
    def __init__(self, hub, journal, gemini, notify, recorder=None):
        self.hub, self.journal, self.gemini, self.notify = hub, journal, gemini, notify
        self.recorder = recorder
        self.rows = {}
        self.quality_report = None
        self.profiles = {}
        self.phases = PhaseTracker()
        self.shadow = ShadowDetector()
        self.sample_slots = {}
        self.control_slots = {}
        self.last_profile_prune = 0
        self.candles = {}
        self.context_events = 0
        self.states = {}
        self.pending = set()
        self.candidate_seen = {}
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
        samples, controls, shadow_findings = [], [], []
        control_slot = int(now // CONTROL_INTERVAL) * CONTROL_INTERVAL
        for key, row in rows.items():
            snapshot, native = row["snapshot"], row["hyperdata"]
            context = rows.get((key[0], "5m")) if key[1] == "1m" else None
            profile = self.profiles.get(key)
            row["facts"] = situation_facts(snapshot, profile)
            row["strength"] = finding_strength(snapshot, native)
            row["movement_phase"] = self.phases.observe(key, snapshot, native, WINDOWS[key[1]])
            row["btc_context"] = btc_context(self.candles.get((key[0], "5m")),
                                             self.candles.get(("BTC", "5m")), now)
            row["profile"] = profile
            row["analysis_gate"] = minute_gate(snapshot)
            row["context_5m"] = five_minute_relation(native, context)
            if not self.hub.demo:
                shadow_findings.extend(self.shadow.evaluate(key, snapshot, native))
                other = rows.get((key[0], "5m" if key[1] == "1m" else "1m"))
                self.detect(key, snapshot, native, now, row["analysis_gate"], row["context_5m"],
                            {**row, "other": other})
                natr = fresh(snapshot["metrics"], "natr")
                if (now - control_slot <= 10 and self.control_slots.get(key) != control_slot
                        and row["strength"]["rank"] == 1 and key not in self.pending
                        and row["analysis_gate"] == "ready" and fresh(snapshot["metrics"], "spread") is not None
                        and not wide_spread(snapshot) and natr is not None and natr > 0):
                    controls.append((*key, control_slot, now, natr))
                sample = sample_window(snapshot, WINDOWS[key[1]])
                if sample and self.sample_slots.get(key) != sample[2]:
                    samples.append(sample)
        if shadow_findings:
            self.journal.record_shadow_findings(shadow_findings)
        self.shadow.keep(rows)
        if samples:
            self.journal.record_instrument_samples(samples)
            self.sample_slots.update({(sample[0], sample[1]): sample[2] for sample in samples})
        if controls:
            self.journal.record_quiet_moves(controls)
            self.control_slots.update({(sample[0], sample[1]): sample[2] for sample in controls})
        self.sample_slots = {key: slot for key, slot in self.sample_slots.items() if key in rows}
        self.control_slots = {key: slot for key, slot in self.control_slots.items() if key in rows}
        self.phases.keep(rows)
        self.rows = rows
        if self.recorder is not None:
            self.recorder.record(now, rows)
        self.states = {key: value for key, value in self.states.items() if key in rows}
        self.candidate_seen = {key: value for key, value in self.candidate_seen.items() if key in rows}
        self.cooldowns = {key: until for key, until in self.cooldowns.items() if until > now}

        prices = {}
        market_at = self.hub.market_timestamp()
        if not self.hub.demo and market_at is not None and self.hub.market_age() <= 20:
            prices = {asset.symbol: (asset.mark_price, market_at) for asset in assets
                      if "price" in self.hub.market.observed_fields.get(asset.symbol, set())
                      and math.isfinite(asset.mark_price) and asset.mark_price > 0}
        self.journal.observe(now, prices)

    def detect(self, key, snapshot, native, now, gate, relation, row=None):
        direction = native["direction"]
        anomalies = active_anomalies(snapshot)
        previous, was_strong, previous_anomalies = self.states.get(key, (None, False, frozenset()))
        new_anomalies = anomalies - previous_anomalies
        strong = native["native_signal"] in {"STRONG_BULL", "STRONG_BEAR"}
        trigger = ((direction in DIRECTIONAL and direction != previous)
                   or (previous in DIRECTIONAL and direction == "NEUTRAL")
                   or (strong and not was_strong) or bool(new_anomalies))
        if not trigger:
            self.candidate_seen.pop(key, None)
            if direction != "NOT_AVAILABLE" and gate not in {"tape_unavailable", "book_unavailable"}:
                self.states[key] = direction, strong, anomalies
            return

        decision = "queued"
        if gate in {"tape_unavailable", "book_unavailable"} or direction == "NOT_AVAILABLE":
            decision = gate if gate != "ready" else "tape_unavailable"
        elif key in self.pending:
            decision = "already_pending"
        elif self.jobs.full():
            decision = "queue_full"
        elif self.cooldowns.get(key, 0) > now:
            decision = "cooldown"

        event = {
            "type": "market_activity" if new_anomalies else "orderflow_transition",
            "anomalies": sorted(new_anomalies), "previous_direction": previous,
            "direction": direction, "native_signal": native["native_signal"],
            "source": native["source"], "detected_at": now,
            "analysis_gate": gate, "context_5m": relation, "strength": finding_strength(snapshot, native),
        }
        signature = (direction, strong, frozenset(new_anomalies))
        previous_candidate = self.candidate_seen.get(key)
        if previous_candidate and previous_candidate[0] == signature:
            candidate_id, detected_at, old_decision = previous_candidate[1:]
            event["detected_at"] = detected_at
            if old_decision != decision and self.journal is not None:
                self.journal.candidate_decision(candidate_id, decision, now)
        else:
            candidate_id = hashlib.sha256(dump(snapshot_key(snapshot)).encode()).hexdigest()
            detected_at = now
            if self.journal is not None:
                self.journal.record_candidate(candidate_id, snapshot, event, decision, now)
        self.candidate_seen[key] = (signature, candidate_id, detected_at, decision)
        event["candidate_id"] = candidate_id
        if self.notify and self.journal is not None and row and gate not in {"book_unavailable", "tape_unavailable"}:
            self.enqueue_facts(candidate_id, snapshot, native, event, row, now)
        if decision != "queued":
            return
        self.states[key] = direction, strong, anomalies
        self.pending.add(key)
        self.sequence += 1
        # New candidates in one scan have the same age. Let strong events
        # lead that batch, while older candidates retain priority over new ones.
        priority = 0 if new_anomalies else 1 if strong else 2 if direction in DIRECTIONAL else 3
        # Bounded adjustments let agreement and book quality matter without starving old events.
        strength = event["strength"]["rank"]
        order_at = (now + PRIORITY_STEP_SECONDS * ((gate == "wide_spread") - (relation == "aligned"))
                    - STRENGTH_STEP_SECONDS * max(0, strength - 1))
        self.jobs.put_nowait((order_at, priority, self.sequence, key, event))

    async def run(self):
        while True:
            try:
                refresh = getattr(self.hub, "refresh_snapshot", None)
                if refresh is not None:
                    await refresh()
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
            candidate_id = event["candidate_id"]
            try:
                now = time.time()
                row = self.rows.get(candidate)
                if not row:
                    self.journal.candidate_decision(candidate_id, "left_universe", now)
                    continue
                if now - event["detected_at"] > MAX_EVENT_AGE[candidate[1]]:
                    self.journal.candidate_decision(candidate_id, "event_expired", now)
                    continue
                snapshot, native = row["snapshot"], row["hyperdata"]
                if (now - snapshot["captured_at"] > 10
                        or snapshot["metrics"]["price"]["quality"] != "ok"
                        or native["direction"] == "NOT_AVAILABLE"
                        or row["analysis_gate"] in {"tape_unavailable", "book_unavailable"}):
                    self.journal.candidate_decision(candidate_id, "stale_at_dispatch", now)
                    self.states.pop(candidate, None)
                    continue
                if event["anomalies"] and not active_anomalies(snapshot).intersection(event["anomalies"]):
                    self.journal.candidate_decision(candidate_id, "anomaly_ended", now)
                    self.states.pop(candidate, None)
                    continue
                if native["direction"] != event["direction"]:
                    # The original transition is obsolete. Let the next scan
                    # decide whether the new direction merits a fresh event.
                    self.journal.candidate_decision(candidate_id, "direction_changed", now)
                    continue
                fact = self.journal.fact_message(candidate_id)
                if fact:
                    snapshot, native = fact["snapshot"], fact["hyperdata"]
                else:
                    snapshot = with_movement_context(snapshot, row["movement_phase"])
                other_window = "5m" if candidate[1] == "1m" else "1m"
                baseline = with_context(snapshot, self.rows.get((candidate[0], other_window)))
                profile = profile_context(self.profiles.get(candidate))
                if profile and profile["updated_at"] <= snapshot["captured_at"]:
                    baseline = {**baseline, "instrument_profile": profile}
                if (row["strength"]["rank"] >= STRONG_RANK and row["btc_context"]
                        and row["btc_context"]["updated_at"] <= snapshot["captured_at"]):
                    baseline = {**baseline, "btc_context": row["btc_context"]}
                snapshot = with_senior_context(baseline, self.candles.get((candidate[0], "15m")))
                event = {**event, "analyzed_signal": native["native_signal"],
                         "analysis_gate": row["analysis_gate"], "context_5m": row["context_5m"],
                         "facts": row["facts"], "strength": row["strength"], "movement_phase": row["movement_phase"]}
                event_id = candidate_id if fact else hashlib.sha256(dump(snapshot_key(snapshot)).encode()).hexdigest()
                if fact:
                    self.journal.save_fact_analysis_input(event_id, snapshot)
                elif not self.journal.record(event_id, event, snapshot, native):
                    self.journal.candidate_decision(candidate_id, "duplicate_snapshot", now)
                    continue
                self.journal.candidate_decision(candidate_id, "analyzing", now, event_id)
                self.cooldowns[candidate] = now + EVENT_COOLDOWN
                next_request = time.monotonic() + AI_INTERVAL
                ai = await self.gemini.analyze(snapshot)
                verdict = compare(snapshot, native, ai)
                self.journal.finish(event_id, ai, verdict, time.time())
                if fact:
                    self.queue_hypothesis(event_id)
                else:
                    self.journal.delivery(event_id, "not_applicable")
                if (self.gemini.key and "context_15m" in snapshot
                        and baseline.get(f"context_{other_window}", {}).get("metrics")):
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
                if event_id is not None and not self.journal.fact_message(event_id):
                    self.journal.delivery(event_id, "error")
                else:
                    self.journal.candidate_decision(candidate_id, "error", time.time())
            finally:
                self.pending.discard(candidate)
                self.jobs.task_done()

    @staticmethod
    def alert_reason(snapshot, native, event):
        metrics = snapshot.get("metrics", {})
        start, end = (fresh(metrics, name) for name in ("first_trade_price", "last_trade_price"))
        direction = native["direction"]
        reversal = (event.get("previous_direction") in DIRECTIONAL
                    and direction in DIRECTIONAL and event["previous_direction"] != direction)
        price_agrees = (start is not None and end is not None and start != end
                        and (end > start) == (direction == "BULLISH"))
        return bool(event.get("anomalies")) or (reversal and price_agrees)

    def enqueue_facts(self, key, snapshot, native, event, row, now):
        if (event["strength"]["rank"] < STRONG_RANK or self.notifications.full()
                or now - event["detected_at"] > MAX_EVENT_AGE[snapshot["window"]]
                or not self.alert_reason(snapshot, native, event)):
            return
        event = {**event, "facts": row["facts"], "movement_phase": row["movement_phase"]}
        snapshot = with_context(snapshot, row["other"])
        snapshot = with_movement_context(snapshot, row["movement_phase"])
        if row["btc_context"]:
            snapshot = {**snapshot, "btc_context": row["btc_context"]}
        if self.journal.register_fact_message(key, snapshot, event, native):
            self.notifications.put_nowait(("facts", key))

    def queue_hypothesis(self, key):
        fact = self.journal.fact_message(key)
        if (fact and fact["message_id"] is not None and fact["ai"]
                and fact["ai"]["status"] == "ok" and fact["edit_status"] is None
                and not self.notifications.full()):
            self.journal.fact_message_edit(key, "queued")
            self.notifications.put_nowait(("edit", key))

    async def edit_alert(self, row, updates, *, include_ai):
        snapshot, native, event = row["snapshot"], row["hyperdata"], row["event"]
        ai = row["ai"] if include_ai else {"status": "pending"}
        return await self.hub.alerts.edit_telegram(
            row["message_id"], alert_text(snapshot, native, ai, event, updates=updates),
            rich_html=alert_text(snapshot, native, ai, event, rich=True, updates=updates) if row["rich"] else None,
            copy_text=futures_ticker(snapshot),
        )

    async def deliver(self):
        while True:
            action, key = await self.notifications.get()
            try:
                row = self.journal.fact_message(key)
                if not row:
                    continue
                snapshot, native, event = row["snapshot"], row["hyperdata"], row["event"]
                ticker = futures_ticker(snapshot)
                if action == "edit":
                    edited = await self.edit_alert(row, self.journal.fact_updates(key).values(), include_ai=True)
                    self.journal.fact_message_edit(key, "edited" if edited else "failed")
                    continue

                now = time.time()
                if now - event["detected_at"] > MAX_EVENT_AGE[snapshot["window"]]:
                    self.journal.delivery(key, "expired")
                    continue
                parent = self.journal.recent_fact_message(snapshot["symbol"], now - ALERT_COOLDOWN)
                if parent:
                    updates = self.journal.fact_updates(parent["id"])
                    previous = updates.get(row["window"])
                    if previous is None and parent["window"] == row["window"]:
                        previous = parent
                    if previous and event["strength"]["rank"] <= previous["event"]["strength"]["rank"]:
                        self.journal.delivery(key, "cooldown")
                        continue
                    updates[row["window"]] = row
                    if await self.edit_alert(parent, updates.values(), include_ai=parent["edit_status"] == "edited"):
                        self.journal.merge_fact_message(parent["id"], key, row["window"], time.time())
                    else:
                        self.journal.delivery(key, "merge_failed")
                    continue
                last = self.journal.last_delivery(snapshot["symbol"], snapshot["window"])
                if last and now - last < ALERT_COOLDOWN:
                    self.journal.delivery(key, "cooldown")
                    continue
                self.journal.delivery(key, "sending", now)
                pending = {"status": "pending"}
                result = await self.hub.alerts.send_telegram(
                    alert_text(snapshot, native, pending, event),
                    rich_html=alert_text(snapshot, native, pending, event, rich=True),
                    copy_text=ticker, return_message=True,
                )
                self.journal.delivery(key, "sent" if result else "failed_or_unconfirmed",
                                      time.time() if result else now)
                if result:
                    self.journal.fact_message_sent(key, *result)
                    self.queue_hypothesis(key)
            except Exception:
                LOG.exception("Notification processing failed")
            finally:
                self.notifications.task_done()

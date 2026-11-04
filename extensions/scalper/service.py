"""Schedule significant native signals, independent analysis and notifications."""
import asyncio
import hashlib
import html
import logging
import math
import time
from datetime import datetime, timezone

from .analysis import DIRECTIONAL, compare, snapshot_key
from .journal import dump
from .market import WINDOWS, capture

LOG = logging.getLogger(__name__)
AI_INTERVAL = 12
EVENT_COOLDOWN = 60
ALERT_COOLDOWN = 300
QUEUE_LIMIT = 20
ALERT_CATEGORIES = {"CONSENSUS_LONG", "CONSENSUS_SHORT", "DIVERGENCE"}


def alert_text(snapshot, native, ai, consensus):
    stamp = datetime.fromtimestamp(snapshot["captured_at"], timezone.utc).isoformat(timespec="seconds")
    ofi = snapshot["metrics"]["ofi"]["value"]
    native_reason = f" (OFI {ofi:+.3f})" if native["evidence"] else ""
    reasons = [arg["reason"] for arg in ai.get("arguments", [])[:2]]
    return html.escape("\n".join([
        f'{snapshot["contract"]} · Hyperliquid · {snapshot["window"]}',
        f'HyperData: {native["direction"]}{native_reason}',
        f'AI ({ai.get("provider", "—")}): {ai.get("direction", ai["status"])}',
        f'{consensus["category"]} · {consensus["overlap"]}',
        *reasons, *(["Контраргумент: " + ai["contradictions"][0]["reason"]]
                    if ai.get("contradictions") else []), stamp,
    ]))


class Screener:
    def __init__(self, hub, journal, gemini, notify):
        self.hub, self.journal, self.gemini, self.notify = hub, journal, gemini, notify
        self.rows = {}
        self.states = {}
        self.cooldowns = {}
        self.sequence = 0
        self.jobs = asyncio.PriorityQueue(maxsize=QUEUE_LIMIT)
        self.notifications = asyncio.Queue(maxsize=QUEUE_LIMIT)

    def scan(self, now):
        assets = self.hub.get_all_assets()
        rows = {}
        for asset in assets[:20]:
            for window in WINDOWS:
                snapshot, native = capture(self.hub, asset, window, now)
                native["snapshot"] = snapshot_key(snapshot)
                key = (asset.symbol, window)
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
        previous, was_strong = self.states.get(key, (None, False))
        strong = native["native_signal"] in {"STRONG_BULL", "STRONG_BEAR"}
        if direction == "NOT_AVAILABLE":
            return
        trigger = ((direction in DIRECTIONAL and direction != previous)
                   or (previous in DIRECTIONAL and direction == "NEUTRAL")
                   or (strong and not was_strong))
        if not trigger:
            self.states[key] = direction, strong
            return
        if self.jobs.full() or self.cooldowns.get(key, 0) > now:
            return

        event = {
            "type": "orderflow_transition", "previous_direction": previous,
            "direction": direction, "native_signal": native["native_signal"],
            "source": native["source"],
        }
        event_id = hashlib.sha256(dump(snapshot_key(snapshot)).encode()).hexdigest()
        self.states[key] = direction, strong
        self.cooldowns[key] = now + EVENT_COOLDOWN
        if self.journal.record(event_id, event, snapshot, native):
            self.sequence += 1
            priority = 0 if strong else 1 if direction in DIRECTIONAL else 2
            self.jobs.put_nowait((priority, self.sequence, event_id, snapshot, native))

    async def run(self):
        while True:
            try:
                self.scan(time.time())
            except Exception:
                LOG.exception("Screener scan failed")
            await asyncio.sleep(5)

    async def analyze(self):
        next_request = 0
        while True:
            if self.gemini.key:
                await asyncio.sleep(max(0, next_request - time.monotonic()))
            _, _, key, snapshot, native = await self.jobs.get()
            try:
                if time.time() - snapshot["captured_at"] > 55:
                    ai = {"status": "expired", "snapshot": snapshot_key(snapshot)}
                elif snapshot["metrics"]["price"]["quality"] != "ok":
                    ai = {"status": "insufficient_market_data", "snapshot": snapshot_key(snapshot)}
                else:
                    next_request = time.monotonic() + AI_INTERVAL
                    ai = await self.gemini.analyze(snapshot)
                verdict = compare(snapshot, native, ai)
                now = time.time()
                self.journal.finish(key, ai, verdict, now)
                self.schedule_alert(key, snapshot, native, ai, verdict)
            except Exception:
                LOG.exception("Event analysis failed")
                self.journal.delivery(key, "error")
            finally:
                self.jobs.task_done()

    def schedule_alert(self, key, snapshot, native, ai, verdict):
        symbol, window = snapshot["symbol"], snapshot["window"]
        previous = self.journal.latest(symbol, window, before=snapshot["captured_at"], completed=True)
        changed = False
        if previous:
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

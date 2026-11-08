"""Unified screener: attention plan, native signals and context reviews."""

import asyncio
import logging
import math
import os
import time
from collections import OrderedDict

from hyperdata_terminal.runtime_mode import UNIFIED, RuntimeModeError, compatibility_error

from .activity import btc_context, finding_strength, market_risk_context, refresh_candles, wide_spread
from .analysis import snapshot_key
from .audit import CONTROL_INTERVAL
from .bots import BotObservatory
from .consistency_store import ConsistencyStore
from .context_auto import REASONS as CONTEXT_REASONS
from .context_auto import ContextMemory, decide, enrich
from .fact_packet import build_packet, packet_ready
from .fact_review import FactReviewer, validate_review
from .market import WINDOWS, capture
from .opportunity import OpportunityPolicy
from .phase import PhaseTracker
from .profile import DAY, calculate_profiles, fresh, sample_window, situation_facts
from .shadow import ShadowDetector
from .unified_attention import UnifiedAttention as Attention

LOG = logging.getLogger(__name__)
CONTEXT_WINDOW_SECONDS = 900





def minute_gate(snapshot):
    if snapshot["window"] != "1m":
        return "ready"
    metrics = snapshot["metrics"]
    if metrics["ofi"]["quality"] != "ok":
        return "tape_unavailable"
    if metrics["spread"]["quality"] != "ok":
        return "book_unavailable"
    return "wide_spread" if wide_spread(snapshot) else "ready"












class Screener:
    def __init__(self, hub, journal, gemini, notify, recorder=None, pulse=None):
        self.hub, self.journal, self.gemini, self.notify = hub, journal, gemini, notify
        self.recorder = recorder
        self.pulse = pulse
        self.runtime_mode = UNIFIED
        self.attention = Attention(
            journal if hasattr(journal, "db") else None,
            slots=int(os.getenv("SCALPER_ATTENTION_SLOTS", "20")),
            detail_slots=int(os.getenv("PULSE_ATTENTION_DETAIL_SLOTS", "6")),
            hold_seconds=int(os.getenv("PULSE_ATTENTION_HOLD_SECONDS", "600")),
        )
        if hub is not None:
            hub.attention_selector = self.attention
        saved_policy = journal.load_policy() if journal is not None and hasattr(journal, "load_policy") else None
        self.policy = OpportunityPolicy(**saved_policy) if saved_policy else OpportunityPolicy.from_env()
        self.rows = {}
        self.profiles = {}
        self.phases = PhaseTracker()
        self.shadow = ShadowDetector()
        self.sample_slots = {}
        self.control_slots = {}
        self.last_profile_prune = 0
        self.candles = {}
        self.market_context: list[str] = []
        self.consistency = ConsistencyStore(journal.db) if journal is not None and hasattr(journal, "db") else None
        self.bots = (
            BotObservatory(journal.db)
            if journal is not None
            and hasattr(journal, "db")
            and os.getenv("SCALPER_BOTS", "1").lower() in {"1", "true", "yes"}
            else None
        )
        self.context_packets = {}
        self.context_cache = OrderedDict()
        self.context_lock = asyncio.Lock()
        self.context_memory = ContextMemory(journal.db) if self.consistency else None
        self.context_auto_enabled = os.getenv("SCALPER_CONTEXT_AUTO", "true").lower() == "true"
        self.context_auto_error = None
        self.outbox = None
        self.ai_routes = None
        self.telegram_ux = None
        if self.consistency and getattr(gemini, "session", None) is not None:
            from dataclasses import replace

            from .ai_routes import AIRoutes

            self.ai_routes = AIRoutes(journal, gemini)
            self.policy = replace(self.policy, hourly_limit=min(40, self.policy.hourly_limit), experiments=False)
            if getattr(hub, "alerts", None) is not None:
                from .delivery import CardOutbox

                self.outbox = CardOutbox(journal, hub.alerts)
                if os.getenv("SCALPER_TELEGRAM_UX", "0") == "1":
                    from .telegram_ux import TelegramUX

                    self.telegram_ux = TelegramUX(self)

    def runtime_diagnostic(self):
        # An unreachable native API is a transient availability problem, not a
        # confirmed incompatible mode. Do not label it as a runtime error.
        if not getattr(self.hub, "attention_known", True):
            return None
        return compatibility_error(getattr(self.hub, "attention", {}))

    def validate_runtime(self) -> bool:
        """Raise only for a confirmed incompatibility; wait on transient outages."""
        error = getattr(self.hub, "runtime_error", None)
        if error:
            raise RuntimeModeError(error)
        if not getattr(self.hub, "attention_known", True):
            return False
        compat = compatibility_error(getattr(self.hub, "attention", {}))
        if compat:
            raise RuntimeModeError(compat)
        return True

    def recover_pending(self):
        """Close persisted interrupted work without re-queueing it."""
        if self.journal is None:
            return []
        interrupted = self.journal.recover()
        self.journal.close_pending_edits()
        return interrupted

    def scan(self, now):
        if self.pulse is not None and hasattr(self.pulse, "desired_plan"):
            self.pulse.desired_plan = self.attention.plan
            self.attention.activity = {r["symbol"]: r for r in getattr(self.pulse, "overview", {}).get("items", [])}
        assets = self.hub.get_all_assets()
        self.market_context = market_risk_context(getattr(self.hub, "context", None))
        rows = {}
        from .activity import observation_assets

        for asset in observation_assets(self.hub):
            for window in WINDOWS:
                key = (asset.symbol, window)
                snapshot, native = capture(self.hub, asset, window, now, self.candles.get(key))
                if self.pulse is not None:
                    snapshot["market_pulse"] = self.pulse.facts(asset.symbol, now)
                native["snapshot"] = snapshot_key(snapshot)
                rows[key] = {"snapshot": snapshot, "hyperdata": native}
        samples, controls, shadow_findings = [], [], []
        control_slot = int(now // CONTROL_INTERVAL) * CONTROL_INTERVAL
        for key, row in rows.items():
            snapshot, native = row["snapshot"], row["hyperdata"]
            profile = self.profiles.get(key)
            row["facts"] = situation_facts(snapshot, profile)
            if self.market_context:
                row["facts"] = [*row["facts"], *self.market_context]
            row["strength"] = finding_strength(snapshot, native)
            row["movement_phase"] = self.phases.observe(key, snapshot, native, WINDOWS[key[1]])
            row["btc_context"] = btc_context(self.candles.get((key[0], "5m")), self.candles.get(("BTC", "5m")), now)
            row["profile"] = profile
            row["analysis_gate"] = minute_gate(snapshot)
            if not self.hub.demo:
                shadow_findings.extend(self.shadow.evaluate(key, snapshot, native))
                natr = fresh(snapshot["metrics"], "natr")
                if (
                    now - control_slot <= 10
                    and self.control_slots.get(key) != control_slot
                    and row["strength"]["rank"] == 1
                    and row["analysis_gate"] == "ready"
                    and fresh(snapshot["metrics"], "spread") is not None
                    and not wide_spread(snapshot)
                    and natr is not None
                    and natr > 0
                ):
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
            self.recorder.record(now, rows, attention=self.attention.plan)

        prices = {}
        market_at = self.hub.market_timestamp()
        if not self.hub.demo and market_at is not None and self.hub.market_age() <= 20:
            prices = {
                asset.symbol: (asset.mark_price, market_at)
                for asset in assets
                if "price" in self.hub.market.observed_fields.get(asset.symbol, set())
                and math.isfinite(asset.mark_price)
                and asset.mark_price > 0
            }
        self.journal.observe(now, prices)
        bot_prices = dict(prices)
        if getattr(self.hub, "attention", {}).get("enabled"):
            scout_prices = {
                s: (p, at)
                for s, (p, at) in getattr(self.hub, "scout_prices", {}).items()
                if isinstance(p, (float, int))
                and math.isfinite(p)
                and p > 0
                and isinstance(at, (float, int))
                and 0 <= now - at <= 20
            }
            self.journal.observe(now, scout_prices)
            bot_prices.update(scout_prices)
        if self.bots is not None and not self.hub.demo:
            try:
                self.bots.tick(now, self.rows, bot_prices, self.attention.plan)
            except Exception:
                LOG.exception("Bot observation tick failed")
        self.refresh_context_packets(now)
        if self.context_memory:
            for packet in self.context_packets.values():
                self.context_memory.observe(packet)
        if self.telegram_ux and self.notify:
            self.telegram_ux.observe(now)

    def refresh_context_packets(self, now):
        attention = getattr(self.hub, "attention", {})
        if not attention.get("context_only"):
            self.context_packets = {}
            return
        packets = {}
        for assignment in attention.get("active", []):
            packet = self.build_context_packet(attention, assignment, now)
            packets[packet["symbol"]] = packet
            self.context_cache[packet["packet_id"]] = packet
            if self.consistency:
                self.consistency.observe_episode(packet)
        self.context_packets = packets
        while len(self.context_cache) > 64:
            self.context_cache.popitem(last=False)

    def build_context_packet(self, attention, assignment, now):
        from .analytics_context import extend_packet

        packet = build_packet(attention, assignment, now, frames=[r["snapshot"] for r in self.rows.values()])
        return extend_packet(packet, self)

    def context_state(self, now):
        if not getattr(self.hub, "attention", {}).get("context_only"):
            return {"enabled": False}
        self.refresh_context_packets(now)
        meta = self.consistency.latest_episode("market.meta.v1") if self.consistency else None
        return dict(
            meta=dict(review=meta, packet=self.consistency.packet(meta["packet_id"])) if meta else None,
            enabled=True,
            configured=bool(self.gemini.key),
            automatic=dict(
                enabled=self.context_auto_enabled,
                last_error=self.context_auto_error,
                reason_labels=CONTEXT_REASONS,
                **(self.context_memory.report(now) if self.context_memory else {}),
            ),
            routes=self.ai_routes.summary(now, limit=self.policy.hourly_limit,
                                          cooldown_seconds=self.policy.cooldown_seconds) if self.ai_routes else None,
            telegram_ux=self.telegram_ux.status() if self.telegram_ux else dict(enabled=False),
            notification_quality=self.telegram_ux.quality.report(now) if self.telegram_ux else None,
            delivery=self.outbox.summary() if self.outbox else None,
            packets=[
                dict(
                    packet=p,
                    selection=self.context_memory.latest(p["episode_id"]) if self.context_memory else None,
                    latest=self.consistency.latest_episode(p["episode_id"]) if self.consistency else None,
                )
                for p in self.context_packets.values()
            ],
            report=self.consistency.report(now) if self.consistency else None,
        )

    def bot_report(self, bot_id=None):
        """Statistics for one observer or the compact list."""
        if self.bots is None:
            text = "Наблюдатели недоступны: журнал не открыт."
            return {"status": "disabled", "text": text, "html": text}
        try:
            return self.bots.report(bot_id)
        except Exception:
            LOG.exception("Bot report failed")
            text = "Не удалось построить отчёт наблюдателей."
            return {"status": "error", "text": text, "html": text}

    async def review_context(
        self, symbol, packet_id=None, *, origin="manual", trigger_reason=None, auto_state=None, route="strong"
    ):
        """Explicit manual request; factual review shares the global request budget."""
        if route == "free" and not self.ai_routes:
            return dict(status="unavailable", reason="free_route_disabled")
        async with self.context_lock:
            now = time.time()
            if not getattr(self.hub, "attention", {}).get("context_only") or not self.consistency:
                return {"status": "unavailable", "reason": "context_mode_required"}
            if getattr(self.hub, "last_error", None):
                # The connection may have dropped after the last snapshot was
                # captured. A packet inside its freshness window is not proof
                # that the native source is still alive.
                return {"status": "skipped", "reason": "native_unavailable"}
            if packet_id is None:
                self.refresh_context_packets(now)
                packet = next((p for s, p in self.context_packets.items() if s.casefold() == symbol.casefold()), None)
            else:
                packet = self.context_cache.get(packet_id) or self.consistency.packet(packet_id)
            active = {r["symbol"]: r for r in self.hub.attention.get("active", [])}
            if (
                packet is None
                or packet["symbol"].casefold() != symbol.casefold()
                or packet["symbol"] not in active
                or not packet_ready(packet, now)
            ):
                return {"status": "skipped", "reason": "stale_or_unknown_packet"}
            current = self.build_context_packet(self.hub.attention, active[packet["symbol"]], now)
            if current["episode_id"] != packet["episode_id"] or not packet_ready(current, now):
                return {"status": "skipped", "reason": "episode_changed_or_unavailable"}
            prior = self.consistency.latest(packet["packet_id"])
            if prior and prior["status"] == "ok" and (not self.ai_routes or prior.get("route") == route):
                return dict(status="ok", cached=True, packet=packet, review=prior)
            client = self.ai_routes.free if self.ai_routes and route == "free" else self.gemini
            if self.ai_routes and route == "free" and not await self.ai_routes.check_free(now):
                return dict(status="skipped", reason=self.ai_routes.catalog_error)
            key = self.consistency.begin(
                packet,
                now,
                client.provider,
                getattr(client, "model", None),
                origin=origin,
                trigger_reason=trigger_reason,
                route=route,
            )
            memory = self.context_memory.observe(packet)
            if auto_state is None:
                _, _, auto_state = decide(packet, memory["samples"])
            auto_state = {**auto_state, "_reason": trigger_reason}
            route_job = None
            if not client.key or self.hub.demo:
                status = "unconfigured" if not client.key else "skipped"
                review = self.consistency.finish(key, status, now, reason="no_api_key" if not client.key else "demo")
                return dict(status=status, packet=packet, review=review)
            if self.ai_routes:
                route_job, reason = self.ai_routes.reserve(packet, route, auto_state, now, self.policy.hourly_limit,
                                                          cooldown_seconds=self.policy.cooldown_seconds)
            else:
                reason = self.journal.reserve_request(key, packet["symbol"], now, self.policy)
            if reason:
                review = self.consistency.finish(key, "skipped", now, reason=reason)
                return dict(status="skipped", packet=packet, review=review)
            if not self.ai_routes:
                with self.journal.db:
                    self.journal.db.execute(
                        "UPDATE ai_requests SET purpose=? WHERE candidate_id=?",
                        ("context_auto" if origin == "automatic" else "context", key),
                    )
            self.context_memory.sent(packet, auto_state, now)
            try:
                response = await FactReviewer(client).review(packet)
                status, checked, reason = response["status"], None, response.get("reason")
                if self.ai_routes and route == "free" and (response.get("usage", {}).get("cost") or 0) > 0:
                    self.ai_routes.free_available = False
                    self.ai_routes.catalog_until = time.time() + 3600
                    self.ai_routes.catalog_error = "unexpected_nonzero_cost"
                    response["retry_after"] = 3600
                    status, reason = "billing_mismatch", "free_provider_reported_cost"

                if status == "ok":
                    try:
                        checked = validate_review(response["raw"], packet, time.time())
                    except (ValueError, TypeError, KeyError) as exc:
                        status, reason = "invalid_response", str(exc)
                review = self.consistency.finish(
                    key,
                    status,
                    time.time(),
                    result=checked,
                    raw=response.get("raw"),
                    reason=reason,
                    model=response.get("model"),
                    usage=response.get("usage"),
                )
                if self.ai_routes:
                    self.ai_routes.finish(route_job, status, key, response.get("retry_after"))
                self.context_memory.completed(packet, auto_state, key, status == "ok")
                return dict(status=status, packet=packet, review=review)
            except asyncio.CancelledError:
                self.consistency.finish(key, "interrupted", time.time(), reason="cancelled")
                raise
            except Exception as exc:
                LOG.exception("Context review failed")
                review = self.consistency.finish(key, "error", time.time(), reason=type(exc).__name__)
                return dict(status="error", packet=packet, review=review)

    async def auto_context_once(self):
        if not self.context_memory or self.hub.demo or not getattr(self.hub, "attention", {}).get("context_only"):
            return []
        if getattr(self.hub, "last_error", None):
            # Do not spend budget while the native source is unreachable.
            return []
        now = time.time()
        self.refresh_context_packets(now)
        candidates, outcomes = [], []
        for packet in self.context_packets.values():
            memory = self.context_memory.observe(packet)
            rank = max(
                (r["strength"]["rank"] for (symbol, _), r in self.rows.items() if symbol == packet["symbol"]), default=0
            )
            route = "strong" if rank >= 3 or not self.ai_routes else "free"
            baseline = self.ai_routes.last_sent(packet["episode_id"], route) if self.ai_routes else memory["last_sent"]
            priority, reason, state = decide(
                packet,
                memory["samples"],
                baseline,
                economical=False if self.ai_routes else self.policy.mode == "economical" or not self.policy.early,
            )
            state["_route"] = route
            if priority is None:
                self.context_memory.record(packet, reason, "rejected", state)
                continue
            block = (
                "disabled"
                if not self.context_auto_enabled
                else "no_api_key"
                if not (self.ai_routes.free.key if self.ai_routes and route == "free" else self.gemini.key)
                else None
                if self.ai_routes
                else self.journal.request_block(packet["symbol"], now, self.policy)
            )
            if block:
                self.context_memory.record(packet, block, "skipped", state)
                continue
            candidates.append((-priority, packet["symbol"], packet, memory, reason, state))
        # One fresh dispatch per pass. Other candidates are re-evaluated from live context next pass.
        if self.ai_routes and not any(c[-1]["_route"] == "strong" for c in candidates):
            meta = await self.meta_context_once()
            if meta:
                return [meta]
        if candidates:
            _, symbol, packet, memory, reason, state = min(
                candidates, key=lambda c: (c[-1].get("_route") != "strong" if self.ai_routes else False, *c[:2])
            )
            if not packet_ready(packet, time.time()):
                self.context_memory.record(packet, "stale_or_unknown_packet", "skipped", state)
                return []
            selected = enrich(packet, memory["samples"], reason)
            self.context_cache[selected["packet_id"]] = selected
            self.context_memory.record(selected, reason, "queued", state)
            if self.outbox and self.notify and not self.telegram_ux:
                self.queue_context_card(dict(status="pending", packet=selected))
            result = await self.review_context(
                symbol,
                selected["packet_id"],
                origin="automatic",
                trigger_reason=reason,
                auto_state=state,
                route=state.get("_route", "strong"),
            )
            self.context_memory.record(
                selected, reason, result["status"], state, (result.get("review") or {}).get("id")
            )
            outcomes.append(result)
            if self.outbox and self.notify and result.get("packet"):
                self.queue_context_card(result)
            # Telegram is a separate decision: only a checked, current, non-abstaining contradiction/change.
            checked = (result.get("review") or {}).get("result") or {}
            if (
                not self.outbox
                and self.notify
                and result["status"] == "ok"
                and reason in {"flow_price_conflict", "density_changed"}
                and not checked.get("abstention")
                and not checked.get("expired_at_completion", True)
                and any(
                    c["section"] == "interpretation" and c["status"] == "hypothesis" for c in checked.get("claims", [])
                )
            ):
                from .telegram import context_review_text

                try:
                    await self.hub.alerts.send_telegram(
                        context_review_text(result), rich_html=context_review_text(result, rich=True)
                    )
                except Exception:
                    LOG.exception("Context review Telegram delivery failed")
        return outcomes

    def queue_context_card(self, result):
        if self.telegram_ux:
            return self.telegram_ux.publish_analysis(result)
        from .telegram import context_review_text

        packet = result["packet"]
        self.outbox.enqueue(
            packet["episode_id"],
            context_review_text(result),
            context_review_text(result, rich=True),
            packet["captured_at"] + 120,
        )

    async def meta_context_once(self):
        from .meta_review import run_meta

        async with self.context_lock:
            result = await run_meta(self)
            if result and self.notify and result.get("packet"):
                self.queue_context_card(result)
            return result

    async def auto_context(self):
        while True:
            try:
                await self.auto_context_once()
                self.context_auto_error = None
            except Exception as exc:
                self.context_auto_error = type(exc).__name__
                LOG.exception("Automatic context review failed")
            await asyncio.sleep(10)


    async def run(self):
        while True:
            now = time.time()
            try:
                refresh = getattr(self.hub, "refresh_snapshot", None)
                if refresh is not None:
                    await refresh()
                self.scan(now)
            except Exception:
                LOG.exception("Screener scan failed")
            if self.bots is not None and not self.hub.demo:
                try:
                    await self.bots.settle(now, self.hub)
                except Exception:
                    LOG.exception("Bot outcome settlement failed")
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

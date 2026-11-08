"""Native L2 owner and durable-tape consumer. Attention is authored by screener."""

import asyncio
import json
import os
import time

import aiohttp

from hyperdata_terminal.market_pulse.control import desired_symbols, validate_plan
from hyperdata_terminal.runtime_mode import UNIFIED

from .runtime import Observation, history_dir


class PulseObservation(Observation):
    def __init__(self, hub, root=None):
        super().__init__(hub, root)
        self.plan = None
        self.applied = {}
        self.cursor_path = (root or history_dir()) / "pulse-consumer.json"
        self.cursor = None
        if self.cursor_path.exists():
            self.cursor = json.loads(self.cursor_path.read_text(encoding="utf-8"))["cursor"]
        self.session = None
        self.last_control = 0
        self.plan_error = None
        self.plan_status = "waiting"
        self.last_plan_at = None
        self._record("source", dict(fact_version=3, assignment_mode="external_plan.v1"), time.time())
        self.task = asyncio.create_task(self.run(), name="pulse-native-consumer")

    async def get(self, path, **params):
        async with self.session.get(
            os.getenv("PULSE_API_URL", "http://127.0.0.1:8431") + path,
            params=params,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def tick(self, now):
        # The independent loop keeps tape delivery responsive while market REST waits.
        return

    def assignments(self, now):
        symbols = desired_symbols(self.plan, int(now * 1000))
        detail = {r["symbol"]: r for r in (self.plan or {}).get("active", [])}
        return {s: detail[s] for s in symbols if s in detail}

    def lease_active(self, now):
        try:
            return (self.plan or {}).get("expires_at_ms", 0) > int(now * 1000)
        except (TypeError, ValueError):
            return False

    async def _refresh_plan(self, now):
        """Refresh the lease, but never apply a plan past its expiry."""
        if now - self.last_control < 2:
            return True
        self.last_control = now
        try:
            body = await self.get("/attention")
            plan = body.get("plan") if isinstance(body, dict) else None
            if plan is not None and not isinstance(plan, dict):
                raise ValueError("attention plan is not an object")
            if plan is not None:
                # The screener owns plan authoring but still validates the
                # published contract before applying it to native sockets.
                validate_plan(plan, int(now * 1000))
        except Exception as exc:
            message = f"{type(exc).__name__}: {str(exc)[:120]}"
            if self.plan_error is None and self.lease_active(now):
                # Keep the valid subscriptions but mark the tape gap explicitly.
                self._invalidate_flow(sorted(getattr(self, "applied", {})), now, "attention_unavailable", message)
            self.plan_error = message
            self.plan_status = ("cached_valid" if self.lease_active(now)
                                else "expired" if self.plan else "unavailable")
            return False
        if self.plan_error is not None:
            self._record("transport", dict(status="attention_recovered", symbols=[], at=now), now)
        self.plan, self.plan_error, self.last_plan_at = plan, None, now
        self.plan_status = "valid" if self.lease_active(now) else "expired" if plan else "missing"
        return True

    def _invalidate_flow(self, symbols, now, status, error=None):
        """Old rolling windows must not look like continuous observation."""
        symbols = sorted({s for s in symbols if s})
        if not symbols:
            return
        payload = dict(symbols=symbols, status=status, at=now)
        if error is not None:
            payload["error"] = error
        self._record("transport", payload, now)
        reset = getattr(self.hub.orderflow, "reset_external", None)
        if reset is not None:
            for symbol in symbols:
                reset(symbol)

    async def _release_all(self, now, reason):
        applied = dict(getattr(self, "applied", {}))
        for symbol, row in applied.items():
            self._record(
                "assignment",
                dict(symbol=symbol, previous=row.get("state", "scout"), state="scout", reason=reason, at=now),
                now,
            )
        self.applied = {}
        self._invalidate_flow(applied, now, reason)
        await self.hub.orderbook.set_symbols([])
        await self.hub.orderflow.set_attention_symbols([])

    async def _reconcile(self, now):
        applied = getattr(self, "applied", {})
        wanted = self.assignments(now)
        released = [s for s in applied if s not in wanted]
        reassigned = [
            s for s in wanted
            if s in applied and applied[s].get("assigned_at") != wanted[s].get("assigned_at")
        ]
        for symbol in released:
            row = applied[symbol]
            self._record(
                "assignment",
                dict(symbol=symbol, previous=row.get("state", "scout"), state="scout", reason="plan_released", at=now),
                now,
            )
        for symbol in reassigned:
            row = applied[symbol]
            self._record(
                "assignment",
                dict(symbol=symbol, previous=row.get("state", "scout"), state="scout",
                     reason="assignment_changed", at=now),
                now,
            )
            new = wanted[symbol]
            self._record(
                "assignment",
                dict(symbol=symbol, previous="scout", state=new.get("state", "scout"),
                     reason=new.get("reason", "reassigned"), at=now),
                now,
            )
        for symbol, row in wanted.items():
            if symbol not in applied:
                self._record(
                    "assignment",
                    dict(symbol=symbol, previous="scout", state=row.get("state", "scout"),
                         reason=row.get("reason", "assigned"), at=now),
                    now,
                )
        self._invalidate_flow(released, now, "plan_released")
        self._invalidate_flow(reassigned, now, "assignment_changed")
        if wanted != applied:
            self._record("attention_plan", dict(plan=self.plan, applied=wanted), now)
            await self.hub.orderbook.set_symbols(list(wanted), reset=reassigned)
            await self.hub.orderflow.set_attention_symbols(list(wanted))
            self.applied = wanted

    async def cycle(self):
        now = time.time()
        if self.archive.error or ((self.plan or {}).get("decision_archive") or {}).get("error"):
            # A broken archive releases detail feeds even when Pulse is unreachable.
            await self._release_all(now, "history_unavailable")
            raise RuntimeError("history_unavailable_collection_paused")
        refreshed = await self._refresh_plan(now)
        # Reconcile after every attempt: an expired lease releases L2 even during an API outage.
        await self._reconcile(now)
        if self.plan_status in ("valid", "cached_valid") and not self.lease_active(now):
            self.plan_status = "expired" if self.plan else "missing"
        if not refreshed or self.plan_status not in ("valid", "cached_valid"):
            return
        wanted = self.assignments(now)
        body = await self.get("/events", cursor=self.cursor or 0, limit=5000)
        head = body["head_cursor"]
        if self.cursor is not None and head < self.cursor:
            self._record("transport", dict(symbols=list(wanted), status="cursor_regressed"), now)
            for symbol in wanted:
                self.hub.orderflow.reset_external(symbol)
            self.cursor = head
        if self.cursor is None:
            self.cursor = head  # Never replay an old live trigger on first attachment.
        else:
            rows = body["events"]
            for row in rows:
                if row["event_seq"] <= self.cursor:
                    continue
                from hyperdata_terminal.instruments import observation_key

                symbol = observation_key(row.get("venue", "hyperliquid"), row["native_market_id"])
                if (symbol in wanted and row.get("venue", "hyperliquid") in {"hyperliquid", "binance"}
                        and row.get("delivery_kind") == "live"
                        and row["event_time_ms"] >= wanted[symbol]["assigned_at"] * 1000):
                    event = self._record("execution", row, now, epoch=row["source_epoch"])
                    if event is not None:
                        self.hub.orderflow.accept_execution(row)
            self.cursor = max(self.cursor, body["next_cursor"] or head)
        self.cursor_path.parent.mkdir(parents=True, exist_ok=True)
        temp = self.cursor_path.with_suffix(".tmp")
        temp.write_text(json.dumps(dict(cursor=self.cursor)), encoding="utf-8")
        temp.replace(self.cursor_path)
        for symbol in wanted:
            self._record("facts", self.facts.card(symbol, now), now)
        self.last_scout = now
        self.subscription_error = None

    async def run(self):
        async with aiohttp.ClientSession() as self.session:
            while not self.closed:
                try:
                    await self.cycle()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self.subscription_error is None:
                        names = list(self.applied)
                        self._record("transport", dict(symbols=names, status="pulse_unavailable"), time.time())
                        for s in names:
                            self.hub.orderflow.reset_external(s)
                    self.subscription_error = f"{type(exc).__name__}: {str(exc)[:120]}"
                await asyncio.sleep(0.25)

    def report(self, now=None):
        now = time.time() if now is None else now
        if self.task.done() and not self.closed:
            self.subscription_error = "pulse_consumer_stopped"
        source_ready = (
            not self.plan_error
            and not self.subscription_error
            and self.plan_status in ("valid", "cached_valid")
        )
        active = []
        for s, row in self.applied.items():
            card = self.facts.card(s, now)
            # ready must use the same source-health criteria as degraded: a
            # fresh L2 cannot certify an instrument while /attention is down.
            active.append(dict(row, facts=card, ready=source_ready and card["status"] == "observing"))
        return dict(
            enabled=True,
            context_only=True,
            runtime_mode=UNIFIED,
            policy_version=(self.plan or {}).get("policy_version", 4),
            plan=self.plan,
            plan_status=self.plan_status,
            plan_error=self.plan_error,
            plan_expires_at_ms=(self.plan or {}).get("expires_at_ms"),
            last_plan_at=self.last_plan_at,
            degraded=(
                bool(self.plan_error)
                or self.plan_status not in ("valid", "cached_valid")
                or bool(self.subscription_error)
            ),
            active=active,
            archive=self.archive.report(),
            subscription_error=self.subscription_error,
            last_scout_at=self.last_scout,
            cursor=self.cursor,
            scout_count=(self.plan or {}).get("universe_size", 0),
            warming=(self.plan or {}).get("warming", 0),
            policy=dict(slots=8),
            trade_owner="market_pulse",
            book_owner="native_hub",
        )

    async def close(self):
        if self.closed:
            return
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await super().close()

"""Native L2 owner and durable-tape consumer. Attention is authored by screener."""

import asyncio
import json
import os
import time

import aiohttp

from hyperdata_terminal.market_pulse.control import desired_symbols

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
        self._record("source", dict(fact_version=2, assignment_mode="external_plan.v1"), time.time())
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
        since = (self.plan or {}).get("controls_since", now)
        return {
            s: detail.get(
                s, dict(symbol=s, state="control", assigned_at=since, since=since, score=0, reason="diagnostic_control")
            )
            for s in symbols
        }

    async def cycle(self):
        now = time.time()
        if now - self.last_control >= 2:
            self.plan = (await self.get("/attention")).get("plan")
            self.last_control = now
        wanted = self.assignments(now)
        for s, row in list(self.applied.items()):
            if s not in wanted or wanted[s]["assigned_at"] != row["assigned_at"]:
                self._record(
                    "assignment",
                    dict(symbol=s, previous=row["state"], state="scout", reason="plan_released", at=now),
                    now,
                )
        for s, row in wanted.items():
            if s not in self.applied or self.applied[s]["assigned_at"] != row["assigned_at"]:
                self._record(
                    "assignment",
                    dict(symbol=s, previous="scout", state=row["state"], reason=row["reason"], at=now),
                    now,
                )
        if wanted != self.applied:
            self._record("attention_plan", dict(plan=self.plan, applied=wanted), now)
            await self.hub.orderbook.set_symbols(list(wanted))
            await self.hub.orderflow.set_attention_symbols(list(wanted))
            self.applied = wanted
        body = await self.get("/events", cursor=self.cursor or 0, venue="hyperliquid", limit=5000)
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
                if row["native_market_id"] in wanted and row.get("delivery_kind") == "live":
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
        active = [
            dict(
                row,
                facts=self.facts.card(s, now),
                ready=not self.subscription_error and self.facts.card(s, now)["status"] == "observing",
            )
            for s, row in self.applied.items()
        ]
        return dict(
            enabled=True,
            context_only=True,
            policy_version=4,
            plan=self.plan,
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

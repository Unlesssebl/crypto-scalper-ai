"""New and increased whale positions on Hyperliquid, for the whale.follow observer (stage 4).

The native position scanner re-reads tracked addresses round-robin, so a position is seen minutes after
it changed (scanned_at says when); the observer measures from detection, which is what a trader could act
on. A position counts as
- "opened": a (address, coin, side) absent when the same address was last read, with an entry price
  within 1% of the price. Both are needed: after a restart the scanner refills its cache address by
  address, and a newly discovered address shows positions held for long; an address is a baseline the
  first time it is seen;
- "added": the same key with ≥ 50% more coins than when last seen (price moves do not change coins).
The first poll is a baseline: everything already open is known, nothing fires.
"""

import asyncio
import time

POLL = 30
MIN_USD = 250_000  # the API returns the largest positions first; 500 rows cover this floor
LIMIT = 500
FRESH_ENTRY = 0.01
GROWTH = 0.5
FORGET_SECONDS = 3600


class WhaleWatcher:
    def __init__(self):
        self.known = {}  # (address, symbol, side) -> dict(coins, scanned_at, seen_at)
        self.addresses = {}  # address -> dict(scanned_at, seen_at) of its latest read
        self.baseline = False
        self.pending = []
        self.error = None

    def ingest(self, positions, now):
        """Diff one /v1/whales payload against the last one; returns and queues the new events."""
        events = []
        rescanned = {}
        current = set()
        for p in positions:
            price, usd, scanned = p.get("current_price"), p.get("size_usd"), p.get("scanned_at") or 0.0
            if not price or price <= 0 or not usd or usd <= 0:
                continue
            key = (p["address"], p["symbol"], p["side"])
            current.add(key)
            rescanned[p["address"]] = max(rescanned.get(p["address"], 0.0), scanned)
            coins = usd / price
            prev = self.known.get(key)
            if self.baseline and scanned > (prev["scanned_at"] if prev else 0.0):
                event = None
                read_before = self.addresses.get(p["address"], {}).get("scanned_at", scanned) < scanned
                if (prev is None and read_before and p.get("entry_price")
                        and abs(p["entry_price"] / price - 1) <= FRESH_ENTRY):
                    event = dict(kind="opened", added_usd=usd)
                elif prev is not None and coins >= prev["coins"] * (1 + GROWTH):
                    event = dict(kind="added", added_usd=(coins - prev["coins"]) * price)
                if event:
                    events.append(dict(event, symbol=p["symbol"], side=p["side"], address=p["address"],
                                       size_usd=usd, price=price, entry_price=p.get("entry_price"),
                                       leverage=p.get("leverage"), lag_s=max(0.0, now - scanned)))
            if prev is None or scanned >= prev["scanned_at"]:
                self.known[key] = dict(coins=coins, scanned_at=scanned, seen_at=now)
        for address, scanned in rescanned.items():
            self.addresses[address] = dict(scanned_at=scanned, seen_at=now)
        for address in [a for a, v in self.addresses.items() if now - v["seen_at"] > FORGET_SECONDS]:
            del self.addresses[address]
        for key, v in list(self.known.items()):
            # Closed: its address was re-read after we last saw it and it is gone. Or simply long unseen.
            gone = key not in current and rescanned.get(key[0], 0.0) > v["scanned_at"]
            if gone or now - v["seen_at"] > FORGET_SECONDS:
                del self.known[key]
        self.baseline = True
        self.pending.extend(events)
        return events

    def drain(self):
        events, self.pending = self.pending, []
        return events

    async def run(self, hub):
        while True:
            try:
                payload = await hub.fetch_whales(min_size=MIN_USD, limit=LIMIT)
                self.ingest(payload.get("positions") or [], time.time())
                self.error = None
            except Exception as exc:  # the native API may be briefly unavailable
                self.error = type(exc).__name__
            await asyncio.sleep(POLL)

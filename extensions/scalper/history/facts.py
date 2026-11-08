"""Facts about observed price levels. Aggregate L2 is never order identity."""
import json
import math
from collections import OrderedDict, defaultdict, deque
from decimal import Decimal, InvalidOperation
from statistics import median

from hyperdata_terminal.utils.helpers import exchange_time_fresh


def number(value):
    try:
        n = float(value)
    except (TypeError, ValueError):
        return None
    return n if math.isfinite(n) else None


class Facts:
    def __init__(self, max_gap=15, version=1):
        self.version = version
        self.max_gap = max_gap
        self.zones = {}
        self.trades = defaultdict(lambda: deque(maxlen=20000))
        self.trade_truncated_at = {}
        self.seen = OrderedDict()
        self.books = {}
        self.epochs = {}
        self.errors = 0
        self.managed = False
        self.selected = set()

    def reset(self, symbol):
        self.zones.pop(symbol, None)
        self.books.pop(symbol, None)
        self.trades.pop(symbol, None)
        self.trade_truncated_at.pop(symbol, None)

    def apply(self, envelope):
        if envelope["kind"] == "assignment":
            p = envelope["payload"]
            self.managed = True
            if p.get("state") == "scout" or p.get("previous") == "scout":
                self.reset(p["symbol"])
            if p.get("state") == "scout":
                self.selected.discard(p["symbol"])
            else:
                self.selected.add(p["symbol"])
            return
        if envelope["kind"] == "transport":
            for symbol in envelope["payload"].get("symbols", []):
                self.reset(symbol)
            return
        if envelope["kind"] == "execution":
            p = envelope["payload"]
            from hyperdata_terminal.instruments import observation_key

            self._trade(dict(coin=observation_key(p["venue"], p["native_market_id"]),
                             px=p["price"], sz=p["base_amount"],
                             time=p["event_time_ms"], tid=p["trade_id"],
                             side="B" if p["side"] == "buy" else "A"), envelope["received_at"],
                        envelope["epoch"], p.get("raw_ref") or str(p["event_seq"]))
            return
        if envelope["kind"] != "raw":
            return
        raw = envelope["payload"].get("message")
        if raw is None:
            try:
                raw = json.loads(envelope["payload"].get("text", ""))
            except (ValueError, TypeError):
                raw = None
        if not isinstance(raw, dict):
            self.errors += 1
            return
        now, epoch = envelope["received_at"], envelope["epoch"]
        ref = f'{envelope["run"]}:{envelope["sequence"]}'
        if raw.get("e") == "depthUpdate" and envelope["payload"].get("source") == "binance_book":
            from hyperdata_terminal.instruments import binance_key

            try:
                self._book(dict(coin=binance_key(raw["s"]), time=raw.get("T", raw.get("E")), levels=[
                    [dict(px=p, sz=q) for p, q in raw["b"]], [dict(px=p, sz=q) for p, q in raw["a"]]]),
                    now, epoch, ref)
            except (ValueError, TypeError, KeyError):
                self.errors += 1
            return
        if raw.get("channel") == "trades" and isinstance(raw.get("data"), list):
            for t in raw["data"]:
                self._trade(t, now, epoch, ref)
        elif raw.get("channel") == "l2Book" and isinstance(raw.get("data"), dict):
            self._book(raw["data"], now, epoch, ref)

    def _trade(self, t, now, epoch, ref):
        if not isinstance(t, dict):
            self.errors += 1
            return
        symbol = t.get("coin")
        if not isinstance(symbol, str) or self.managed and symbol not in self.selected:
            return
        px, sz, stamp = (number(t.get(key)) for key in ("px", "sz", "time"))
        if not symbol or px is None or sz is None or px <= 0 or sz <= 0 or stamp is None:
            self.errors += 1
            return
        if not exchange_time_fresh(now, stamp / 1000, self.max_gap):
            return  # Reconnect batches are preserved raw but are not fresh observation.
        key = (symbol, t.get("time"), t.get("tid"))
        if t.get("tid") is None or key in self.seen:
            return
        self.seen[key] = None
        while len(self.seen) > 100000:
            self.seen.popitem(last=False)
        rows = self.trades[symbol]
        while rows and now - rows[0][0] > 60:
            rows.popleft()
        if len(rows) == rows.maxlen:
            self.trade_truncated_at[symbol] = now
        rows.append((now, px * sz, ref, t.get("side")))

    def _book(self, book, now, epoch, ref):
        symbol, levels = book.get("coin"), book.get("levels")
        if not isinstance(symbol, str) or self.managed and symbol not in self.selected:
            return
        stamp = number(book.get("time"))
        if not symbol or stamp is None or not exchange_time_fresh(now, stamp / 1000, self.max_gap):
            if symbol:
                self.reset(symbol)
            return
        if not isinstance(levels, list) or len(levels) != 2:
            self.errors += 1
            self.reset(symbol)
            return
        parsed = []
        for side, rows in zip(("bid", "ask"), levels):
            if not isinstance(rows, list):
                self.reset(symbol)
                self.errors += 1
                return
            part = []
            for row in rows:
                try:
                    px, sz = Decimal(row["px"]), Decimal(row["sz"])
                    if not px.is_finite() or not sz.is_finite() or px <= 0 or sz <= 0:
                        raise ValueError("invalid level")
                    if not math.isfinite(float(px * sz)):
                        raise ValueError("level exceeds numeric range")
                    part.append((side, str(px.normalize()), float(px), float(sz)))
                except (KeyError, ValueError, TypeError, InvalidOperation):
                    self.errors += 1
                    self.reset(symbol)
                    return
            parsed.append(part)
        if not all(parsed):
            self.reset(symbol)
            return
        bid, ask = max(x[2] for x in parsed[0]), min(x[2] for x in parsed[1])
        if bid >= ask:
            self.reset(symbol)
            self.errors += 1
            return
        old = self.books.get(symbol)
        if old and (now - old["received_at"] > self.max_gap or old["epoch"] != epoch):
            self.reset(symbol)
            old = None
        if old and stamp < old["exchange_time"]:
            return
        if old and stamp == old["exchange_time"]:
            return
        existing = self.zones.get(symbol, {})
        zones = {}
        for side, key, price, size in parsed[0] + parsed[1]:
            prior = existing.get((side, key))
            zones[side, key] = dict(side=side, price=price, size=size, notional_usd=price * size,
                                   first_seen=prior["first_seen"] if prior else now,
                                   samples=prior["samples"] + 1 if prior else 1,
                                   size_change=size - prior["size"] if prior else None,
                                   evidence=ref)
        self.zones[symbol] = zones
        self.books[symbol] = dict(symbol=symbol, received_at=now, exchange_time=stamp, epoch=epoch,
                                  midpoint=(bid + ask) / 2, spread_bps=(ask - bid) / ((bid + ask) / 2) * 10000,
                                  observed_levels=[len(p) for p in parsed], evidence=ref)

    def card(self, symbol, now):
        book = self.books.get(symbol)
        if not book or not 0 <= now - book["received_at"] <= self.max_gap:
            return dict(symbol=symbol, status="waiting_for_fresh_book", densities=[])
        zones = list(self.zones[symbol].values())
        typical = median(z["notional_usd"] for z in zones)
        trades = [t for t in self.trades[symbol] if 0 <= now - t[0] <= 60]
        turnover = sum(t[1] for t in trades)
        truncated = now - self.trade_truncated_at.get(symbol, -1e30) <= 60
        densities = []
        for z in sorted(zones, key=lambda z: -z["notional_usd"]):
            if z["notional_usd"] < typical * 3:
                continue
            densities.append({**z, "observed_age_seconds": book["received_at"] - z["first_seen"],
                              "distance_bps": abs(z["price"] / book["midpoint"] - 1) * 10000,
                              "relative_size": z["notional_usd"] / typical,
                              "size_to_observed_turnover": z["notional_usd"] / turnover
                              if turnover and not truncated else None})
        early = {}
        if self.version >= 2:
            known = all(t[3] in {"B", "A"} for t in trades)
            buy = sum(t[1] for t in trades if t[3] == "B")
            sell = sum(t[1] for t in trades if t[3] == "A")
            early = dict(observed_trade_count_60s=len(trades), last_trade_received_at=trades[-1][0] if trades else None,
                         observed_trade_imbalance=(buy - sell) / turnover
                         if known and turnover and not truncated else None)
        return {**book, **early, "status": "observing", "densities": densities[:8],
                "observed_turnover_60s_usd": turnover, "trade_window_truncated": truncated,
                "trade_evidence": [t[2] for t in trades[-3:]],
                "limitation": "Age since first continuous observation, not order age. Top-N absence is out of view."}

"""Point-in-time signal replay against recorded executable bid/ask quotes.

No exchange orders, LLM calls, live database writes, reconstructed OFI from
candles, stop/target optimization, or future observations in signal selection.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
import sqlite3
import statistics
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from functools import partial
from pathlib import Path
from zoneinfo import ZoneInfo

from extensions.scalper.activity import finding_strength
from extensions.scalper.opportunity import OpportunityPolicy
from extensions.scalper.profile import fresh

DIRECTIONS = {"BULLISH": 1, "BEARISH": -1}
HORIZONS = (1, 5, 15)
RULES = ("native", "rank3", "balanced", "price_book", "no_conflicts", "context_aligned")


@dataclass(frozen=True)
class Quote:
    at: float
    book_at: float
    bid: float
    ask: float


@dataclass
class Signal:
    id: str
    symbol: str
    window: str
    at: float
    available_at: float
    direction: int
    rank: int
    kind: str | None
    price_aligned: bool
    book_side: float | None
    context: str
    source: str = "algorithm"
    ai_direction: int = 0
    natr: float | None = None
    spread_bps: float | None = None


def positive(v):
    return isinstance(v, (float, int)) and math.isfinite(v) and v > 0


def quote_from(snapshot):
    m, at = snapshot["metrics"], snapshot["captured_at"]
    bid, ask = fresh(m, "best_bid"), fresh(m, "best_ask")
    stamp = m.get("best_bid", {}).get("updated_at")
    other_stamp = m.get("best_ask", {}).get("updated_at")
    if (not positive(bid) or not positive(ask) or bid > ask or not isinstance(stamp, (int, float))
            or stamp != other_stamp or not 0 <= at - stamp <= 15):
        return None
    return Quote(at, stamp, bid, ask)


def quote_after(quotes, times, target, tolerance=10):
    """A later capture of an old book is not a new executable quote."""
    index = bisect_left(times, target)
    while index < len(quotes) and quotes[index].at <= target + tolerance:
        q = quotes[index]
        if q.book_at >= target:
            return q
        index += 1
    return None


def economic_return(entry, exit_quote, direction, fee_bps=4.5, slippage_bps=1):
    slip = slippage_bps / 10000
    enter = entry.ask * (1 + slip) if direction == 1 else entry.bid * (1 - slip)
    leave = exit_quote.bid * (1 - slip) if direction == 1 else exit_quote.ask * (1 + slip)
    gross = direction * (leave / enter - 1) * 10000
    fees = fee_bps * (1 + leave / enter)
    mid_enter, mid_exit = (entry.ask + entry.bid) / 2, (exit_quote.ask + exit_quote.bid) / 2
    return dict(net_bps=gross - fees, execution_gross_bps=gross, fee_bps=fees,
                midpoint_bps=direction * (mid_exit / mid_enter - 1) * 10000,
                entry_price=enter, exit_price=leave)


def trade_outcome(signal, quotes, times, horizon, end, fee=4.5, slippage=1, latency=1, tolerance=10):
    target = signal.available_at + latency
    base = dict(id=signal.id, symbol=signal.symbol, window=signal.window, signal_at=signal.at,
                available_at=signal.available_at, direction=signal.direction, rank=signal.rank,
                kind=signal.kind, context=signal.context)
    if target + horizon * 60 + tolerance > end:
        return {**base, "status": "right_censored"}
    entry = quote_after(quotes, times, target, tolerance)
    if entry is None:
        return {**base, "status": "missing_entry_quote"}
    due = entry.at + horizon * 60
    if due + tolerance > end:
        return {**base, "status": "right_censored"}
    exit_quote = quote_after(quotes, times, due, tolerance)
    base.update(entry_at=entry.at, exit_due=due)
    if exit_quote is None:
        return {**base, "status": "missing_exit_quote"}
    return {**base, "status": "closed", "exit_at": exit_quote.at,
            **economic_return(entry, exit_quote, signal.direction, fee, slippage)}


def eligible(s, rule):
    if rule == "native":
        return True
    if rule == "rank3":
        return s.rank >= 3
    if rule == "balanced":
        return s.kind is not None
    if rule == "price_book":
        return bool(s.kind and s.price_aligned and s.book_side is not None and s.book_side >= 0.25)
    if rule == "no_conflicts":
        return bool(s.kind and s.price_aligned and s.book_side is not None
                    and s.book_side > -0.25 and s.context != "opposed")
    if rule == "context_aligned":
        return bool(eligible(s, "no_conflicts") and s.context == "aligned")
    if rule in {"ai_native_ready", "ai_native_fast"}:
        return True
    if rule == "ai_direction":
        return s.ai_direction != 0
    if rule == "ai_agreement":
        return s.ai_direction == s.direction
    raise ValueError(rule)


def bar_outcome(signal, quotes, times, horizon, end, fee=4.5, slippage=1, latency=1, tolerance=10,
                *, bars, funding, entry_quotes=False):
    """Next-minute-open proxy; historical bars price fills but never select signals."""
    observed_entry = quote_after(quotes, times, signal.available_at + latency, tolerance) if entry_quotes else None
    at = observed_entry.at if observed_entry else math.ceil((signal.available_at + latency) / 60) * 60
    finish = math.ceil((at + 60 * horizon) / 60) * 60
    base = dict(id=signal.id, symbol=signal.symbol, window=signal.window, signal_at=signal.at,
                available_at=signal.available_at, direction=signal.direction, rank=signal.rank,
                kind=signal.kind, context=signal.context,
                execution="quote_entry_bar_exit" if entry_quotes else "minute_open_proxy")
    if entry_quotes and observed_entry is None:
        return {**base, "status": "missing_entry_quote"}
    if finish > end:
        return {**base, "status": "right_censored"}
    if at - signal.at > {"1m": 45, "5m": 120}[signal.window]:
        return {**base, "status": "expired_before_minute_fill"}
    if signal.spread_bps is None or signal.spread_bps < 0:
        return {**base, "status": "missing_signal_spread"}
    series = bars.get(signal.symbol, {})
    start_bar, end_bar = series.get(at), series.get(finish)
    if not entry_quotes and (start_bar is None or not positive(float(start_bar["o"])) or not start_bar["n"]):
        return {**base, "status": "missing_entry_quote"}
    base.update(entry_at=at, exit_due=finish)
    if end_bar is None or not positive(float(end_bar["o"])) or not end_bar["n"]:
        return {**base, "status": "missing_exit_quote"}
    leave = float(end_bar["o"])
    half = ((observed_entry.ask - observed_entry.bid) / (observed_entry.ask + observed_entry.bid)
            if observed_entry else signal.spread_bps / 20000)
    if observed_entry:
        entry = observed_entry
    else:
        enter = float(start_bar["o"])
        entry = Quote(at, at, enter * (1 - half), enter * (1 + half))
    exit_quote = Quote(finish, finish, leave * (1 - half), leave * (1 + half))
    result = economic_return(entry, exit_quote, signal.direction, fee, slippage)
    funding_bps = 0
    unknown = signal.symbol not in funding
    for item in funding.get(signal.symbol, []):
        settled = item["time"] / 1000
        if at < settled <= finish:
            mark_proxy = series.get(int(settled))
            if mark_proxy is None:
                unknown = True
            else:
                funding_bps += (signal.direction * float(item["fundingRate"])
                                * float(mark_proxy["o"]) / result["entry_price"] * 10000)
    result["net_bps"] -= funding_bps
    return {**base, **result, "funding_bps": funding_bps, "funding_unknown": unknown,
            "entry_delay_s": at - signal.available_at, "exit_at": finish, "status": "closed"}


def context_at(histories, times, symbol, window, at, direction):
    key = (symbol, "5m" if window == "1m" else "1m")
    rows = histories.get(key, [])
    index = bisect_right(times.get(key, []), at) - 1
    if index < 0:
        return "unavailable"
    other = rows[index]
    if at - other["captured_at"] > 10:
        return "unavailable"
    item = other["metrics"].get("ofi", {})
    stamp = item.get("updated_at")
    ofi = fresh(other["metrics"], "ofi")
    if ofi is None or not isinstance(stamp, (float, int)) or not 0 <= at - stamp <= 60:
        return "unavailable"
    other_side = 1 if ofi > 0.15 else -1 if ofi < -0.15 else 0
    return "neutral" if not other_side else "aligned" if other_side == direction else "opposed"


def signal_from(key, snapshot, native, histories, history_times, available_at=None):
    at, m = snapshot["captured_at"], snapshot["metrics"]
    direction = DIRECTIONS.get(native.get("direction"), 0)
    ofi, price = fresh(m, "ofi"), fresh(m, "price")
    stamp = m.get("ofi", {}).get("updated_at")
    price_stamp = m.get("price", {}).get("updated_at")
    if (not direction or ofi is None or not positive(price) or not isinstance(stamp, (float, int))
            or not isinstance(price_stamp, (float, int)) or not 0 <= at - stamp <= 60
            or not 0 <= at - price_stamp <= 20):
        return None
    rank = finding_strength(snapshot, native)["rank"]
    kind, _ = OpportunityPolicy().classify(snapshot, native, at)
    start, finish, spread, natr = (fresh(m, name) for name in
                                   ("first_trade_price", "last_trade_price", "spread", "natr"))
    aligned = False
    if positive(start) and positive(finish) and spread is not None and positive(natr):
        aligned = direction * (finish - start) > max(2 * spread, start * natr / 100 * 0.1)
    imbalance = fresh(m, "imbalance")
    book_stamp = m.get("imbalance", {}).get("updated_at")
    if not isinstance(book_stamp, (float, int)) or not 0 <= at - book_stamp <= 15:
        imbalance = None
    return Signal(key, snapshot["symbol"], snapshot["window"], at, available_at or at, direction, rank, kind,
                  aligned, direction * imbalance if imbalance is not None else None,
                  context_at(histories, history_times, snapshot["symbol"], snapshot["window"], at, direction),
                  natr=natr, spread_bps=spread / price * 10000 if spread is not None else None)


def load_data(db):
    candidates = db.execute("SELECT * FROM finding_candidates ORDER BY detected_at,id").fetchall()
    analyses = db.execute("SELECT * FROM analyses ORDER BY captured_at,id").fetchall()
    histories, books = defaultdict(dict), defaultdict(dict)
    decoded = {}
    for table, rows in (("candidate", candidates), ("analysis", analyses)):
        for row in rows:
            snap = json.loads(row["snapshot"])
            decoded[table, row["id"]] = snap
            key = snap["symbol"], snap["window"]
            histories[key].setdefault(snap["captured_at"], snap)
            quote = quote_from(snap)
            if quote:
                books[key[0]].setdefault(quote.at, quote)
    ordered = {key: [values[t] for t in sorted(values)] for key, values in histories.items()}
    history_times = {key: [s["captured_at"] for s in values] for key, values in ordered.items()}
    quotes = {symbol: [values[t] for t in sorted(values)] for symbol, values in books.items()}
    quote_times = {symbol: [q.at for q in values] for symbol, values in quotes.items()}
    signals = []
    for row in candidates:
        native = json.loads(row["event"])
        s = signal_from(row["id"], decoded["candidate", row["id"]], native, ordered, history_times)
        if s:
            signals.append(s)
    ai_signals, ai_exclusions = [], Counter()
    for row in analyses:
        ai = json.loads(row["ai"]) if row["ai"] else {}
        if not ai or ai.get("status") != "ok":
            continue
        snap, native = decoded["analysis", row["id"]], json.loads(row["hyperdata"])
        ready = row["analyzed_at"]
        if ready is None or ready < snap["captured_at"]:
            ai_exclusions["invalid_ready_time"] += 1
            continue
        if ready - snap["captured_at"] > {"1m": 45, "5m": 120}[snap["window"]]:
            ai_exclusions["stale_ai"] += 1
            continue
        # Require the actual model answer and native vote to reference this exact input.
        identity = {k: snap[k] for k in ("symbol", "exchange", "contract", "window", "captured_at")}
        if ai.get("snapshot") != identity or native.get("snapshot") != identity:
            ai_exclusions["identity_mismatch"] += 1
            continue
        s = signal_from(row["id"], snap, native, ordered, history_times, ready)
        if not s:
            ai_exclusions["native_unavailable_or_neutral"] += 1
            continue
        s.source, s.ai_direction = "recorded_ai", DIRECTIONS.get(ai.get("direction"), 0)
        ai_signals.append(s)
    return signals, ai_signals, quotes, quote_times, dict(ai_exclusions)


def confidence_interval(trades, field="net_bps", block_seconds=900, repeats=1000):
    blocks = defaultdict(list)
    for t in trades:
        blocks[int(t["available_at"] // block_seconds)].append(t[field])
    groups = list(blocks.values())
    if len(groups) < 3:
        return None
    rng = random.Random(20261105)
    means = []
    for _ in range(repeats):
        sampled = [v for group in rng.choices(groups, k=len(groups)) for v in group]
        means.append(statistics.mean(sampled))
    means.sort()
    return [means[int(0.025 * repeats)], means[int(0.975 * repeats)]]


def metrics(trades, notional=100):
    if not trades:
        return dict(n=0, mean_net_bps=None, net_win_rate=None, profit_factor=None,
                    net_usd=0, mean_midpoint_bps=None, ci95_bps=None, blocks=0)
    net = [t["net_bps"] for t in trades]
    wins, losses = sum(v for v in net if v > 0), -sum(v for v in net if v < 0)
    return dict(n=len(trades), mean_net_bps=statistics.mean(net), median_net_bps=statistics.median(net),
                net_win_rate=sum(v > 0 for v in net) / len(net), profit_factor=wins / losses if losses else None,
                net_usd=notional * sum(net) / 10000,
                mean_midpoint_bps=statistics.mean(t["midpoint_bps"] for t in trades),
                gross_direction_hit=sum(t["midpoint_bps"] > 0 for t in trades) / len(trades),
                ci95_bps=confidence_interval(trades),
                blocks=len({int(t["available_at"] // 900) for t in trades}))


def replay(signals, quotes, times, rule, horizon, start, stop, data_end, fee=4.5, slippage=1,
           latency=1, tolerance=10, portfolio=True, pricing=None):
    trades, decisions, busy, counts = [], [], {}, Counter()
    for s in sorted(signals, key=lambda x: (x.available_at, -x.rank, x.window != "1m", x.id)):
        if not start <= s.available_at < stop:
            continue
        counts["opportunities"] += 1
        if not eligible(s, rule):
            counts["rejected"] += 1
            continue
        counts["selected"] += 1
        if portfolio and busy.get(s.symbol, 0) > s.available_at:
            counts["position_open"] += 1
            continue
        candidate = Signal(**asdict(s))
        if rule == "ai_direction":
            candidate.direction = candidate.ai_direction
        t = (pricing or trade_outcome)(candidate, quotes.get(s.symbol, []), times.get(s.symbol, []), horizon,
                                       data_end, fee, slippage, latency, tolerance)
        counts[t["status"]] += 1
        decisions.append(t)
        if t["status"] == "closed":
            trades.append(t)
            busy[s.symbol] = t["exit_at"]
        elif t["status"] == "missing_exit_quote":
            # Do not erase an entered losing/unpriced position using future coverage.
            busy[s.symbol] = data_end + 1
    return dict(rule=rule, horizon=horizon, portfolio=portfolio, counts=dict(counts),
                metrics=metrics(trades), trades=trades, decisions=decisions)


def paired_ai(signals, quotes, times, horizon, start, stop, end, pricing=None):
    outcomes, omissions = [], Counter()
    for s in signals:
        if not start <= s.available_at < stop:
            continue
        evaluate = pricing or trade_outcome
        base = evaluate(s, quotes.get(s.symbol, []), times.get(s.symbol, []), horizon, end)
        if base["status"] != "closed":
            omissions[base["status"]] += 1
            continue
        independent = 0
        if s.ai_direction:
            alt = Signal(**asdict(s))
            alt.direction = s.ai_direction
            independent = evaluate(alt, quotes[s.symbol], times[s.symbol], horizon, end)["net_bps"]
        agreement = base["net_bps"] if s.ai_direction == s.direction else 0
        outcomes.append({**base, "native_net_bps": base["net_bps"], "ai_net_bps": independent,
                         "agreement_net_bps": agreement, "ai_direction": s.ai_direction,
                         "independent_delta_bps": independent - base["net_bps"],
                         "agreement_delta_bps": agreement - base["net_bps"]})
    return dict(n=len(outcomes), omissions=dict(omissions),
                mean_independent_delta_bps=statistics.mean(t["independent_delta_bps"] for t in outcomes)
                if outcomes else None,
                mean_agreement_delta_bps=statistics.mean(t["agreement_delta_bps"] for t in outcomes)
                if outcomes else None,
                independent_ci95=confidence_interval(outcomes, "independent_delta_bps"),
                agreement_ci95=confidence_interval(outcomes, "agreement_delta_bps"),
                random_thinning=random_thinning(outcomes), outcomes=outcomes)


def random_thinning(outcomes, repeats=2000):
    """Diagnostic control for the apparent benefit of abstaining on a losing base strategy."""
    selected = [t for t in outcomes if t["ai_direction"]]
    if not selected:
        return None
    pools, counts = defaultdict(list), Counter()
    for t in outcomes:
        pools[int(t["available_at"] // 900), t["window"]].append(t)
    for t in selected:
        counts[int(t["available_at"] // 900), t["window"]] += 1
    rng = random.Random(20261105)
    sampled_means = []
    for _ in range(repeats):
        sample = [t for key, n in counts.items() for t in rng.sample(pools[key], n)]
        sampled_means.append(statistics.mean(t["native_net_bps"] for t in sample))
    sampled_means.sort()
    actual = statistics.mean(t["ai_net_bps"] for t in selected)
    return dict(n_selected=len(selected), ai_selected_mean_bps=actual,
                native_same_selected_mean_bps=statistics.mean(t["native_net_bps"] for t in selected),
                native_all_mean_bps=statistics.mean(t["native_net_bps"] for t in outcomes),
                random_mean_bps=statistics.mean(sampled_means),
                random_range95=[sampled_means[int(0.025 * repeats)], sampled_means[int(0.975 * repeats)]],
                random_fraction_at_least_ai=sum(v >= actual for v in sampled_means) / repeats)


def write_csv(path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({k for row in rows for k in row})
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def save_json(path, data):
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def run(journal, output, historical=None, hybrid=False):
    db = sqlite3.connect(journal.resolve().as_uri() + "?mode=ro", uri=True)
    db.row_factory = sqlite3.Row
    begin, end, raw_candidates = db.execute(
        "SELECT MIN(detected_at),MAX(detected_at),COUNT(*) FROM finding_candidates",
    ).fetchone()
    split, test_start = begin + 0.6 * (end - begin), begin + 0.6 * (end - begin) + 900
    protocol = dict(primary_horizon_minutes=5, diagnostic_horizons=[1, 15], fee_per_side_bps=4.5,
                    slippage_per_side_bps=1, latency_seconds=1, max_quote_wait_seconds=10,
                    notional_per_trade_usd=100, starting_equity_usd=10000,
                    one_position_per_symbol=True, compare_all_rules=list(RULES),
                    rule_thresholds="Existing balanced policy; price threshold=max(2*spread,0.1*NATR); "
                                    "book support>=0.25; opposite book<-0.25; context<=10s old.",
                    split=split, embargo_seconds=900, test_start=test_start, start=begin, end=end,
                    source_candidates=raw_candidates, thresholds_tuned=False,
                    funding="Not included: actual settlement history unavailable",
                    execution="Marketable bid/ask replay, not proof of liquidity or a live fill")
    pricing = None
    if historical:
        data = json.loads(historical.read_text(encoding="utf-8"))
        bars = {symbol: {int(c["t"] / 1000): c for c in rows} for symbol, rows in data["candles"].items()}
        pricing = partial(bar_outcome, bars=bars, funding=data["funding"], entry_quotes=hybrid)
        protocol.update(execution="Next minute open after actual signal/AI readiness; hypothetical fill",
                        spread="Signal-time half spread on each side; exit spread is not observed",
                        max_input_age_at_fill={"1m": 45, "5m": 120},
                        funding="Historical published rates; notional proxy uses settlement-minute trade open",
                        price_source_sha256=hashlib.sha256(historical.read_bytes()).hexdigest())
        if hybrid:
            protocol.update(execution="Observed later bid/ask entry; next minute open after scheduled exit",
                            spread="Observed entry spread; same relative spread assumed at exit")
    save_json(output / "protocol.json", protocol)  # Written before computing any returns.
    signals, ai_signals, quotes, times, ai_exclusions = load_data(db)
    db.close()
    segments = {"all": (begin, end), "development": (begin, split), "test": (test_start, end)}
    results, pairs = [], {}
    for segment, (lo, hi) in segments.items():
        # Segment boundary is also a liquidation boundary: no holdout price leaks into development.
        for horizon in HORIZONS:
            for rule in RULES:
                r = replay(signals, quotes, times, rule, horizon, lo, hi, hi, pricing=pricing)
                r["segment"] = segment
                results.append(r)
            for rule in ("ai_native_ready", "ai_native_fast", "ai_direction", "ai_agreement"):
                cohort = ai_signals
                if rule == "ai_native_fast":
                    cohort = [Signal(**{**asdict(s), "available_at": s.at}) for s in ai_signals]
                r = replay(cohort, quotes, times, rule, horizon, lo, hi, hi, pricing=pricing)
                r["segment"] = segment
                results.append(r)
            pairs[f"{segment}/{horizon}"] = paired_ai(ai_signals, quotes, times, horizon, lo, hi, hi, pricing=pricing)
    # Signal-level outcomes include overlap and are diagnostic only; portfolio rows above do not.
    diagnostics = {}
    for segment, (lo, hi) in segments.items():
        r = replay(signals, quotes, times, "native", 5, lo, hi, hi, portfolio=False, pricing=pricing)
        signal_map = {s.id: s for s in signals}
        groups = {}
        for rule in RULES[1:]:
            accepted = [t for t in r["trades"] if eligible(signal_map[t["id"]], rule)]
            rejected = [t for t in r["trades"] if not eligible(signal_map[t["id"]], rule)]
            groups[rule] = dict(accepted=metrics(accepted), rejected=metrics(rejected),
                                rejected_net_winners=sum(t["net_bps"] > 0 for t in rejected),
                                retained_net_winners=sum(t["net_bps"] > 0 for t in accepted))
        diagnostics[segment] = groups
        write_csv(output / f"signal_outcomes_{segment}.csv", r["trades"])
    sensitivity = []
    for rule in RULES:
        for fee, slip in ((0, 0), (4.5, 0), (4.5, 1), (4.5, 3)):
            r = replay(signals, quotes, times, rule, 5, test_start, end, end, fee=fee, slippage=slip,
                       pricing=pricing)
            sensitivity.append(dict(rule=rule, fee=fee, slip=slip, metrics=r["metrics"], counts=r["counts"]))
    coverage = dict(raw_candidates=raw_candidates, valid_directional_signals=len(signals),
                    recorded_ai_eligible=len(ai_signals), ai_directional=sum(bool(s.ai_direction) for s in ai_signals),
                    ai_exclusions=ai_exclusions, symbols=sorted(quotes),
                    executable_quotes=sum(len(v) for v in quotes.values()),
                    quote_gaps={k: dict(n=len(v), median_s=statistics.median([b.at - a.at for a, b in zip(v, v[1:])]),
                                       max_s=max(b.at - a.at for a, b in zip(v, v[1:])))
                                for k, v in quotes.items() if len(v) > 1})
    document = dict(protocol=protocol, coverage=coverage,
                    results=[{k: v for k, v in r.items() if k not in {"trades", "decisions"}} for r in results],
                    paired_ai={k: {field: value for field, value in v.items() if field != "outcomes"}
                               for k, v in pairs.items()}, diagnostics=diagnostics, sensitivity=sensitivity)
    save_json(output / "results.json", document)
    write_csv(output / "signals.csv", [asdict(s) for s in signals])
    write_csv(output / "recorded_ai_signals.csv", [asdict(s) for s in ai_signals])
    for r in results:
        write_csv(output / f"trades_{r['segment']}_{r['rule']}_{r['horizon']}m.csv", r["decisions"])
    for key, p in pairs.items():
        write_csv(output / ("paired_ai_" + key.replace("/", "_") + ".csv"), p["outcomes"])
    save_json(output / "manifest.json", {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                        for p in output.iterdir() if p.suffix in {".json", ".csv"}})
    return document


def stamp(value):
    return datetime.fromtimestamp(value, ZoneInfo("Europe/Moscow")).strftime("%d.%m.%Y %H:%M:%S МСК")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True, help="Frozen journal copy, opened read-only")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--historical", type=Path, help="Independent historical 1m candles and funding JSON")
    parser.add_argument("--hybrid", action="store_true",
                        help="Use observed bid/ask for entries and bar opens for exits")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    if args.hybrid and not args.historical:
        parser.error("--hybrid requires --historical")
    results = run(args.journal, args.output, args.historical, args.hybrid)
    print(json.dumps(results["coverage"], ensure_ascii=False))
    print(stamp(results["protocol"]["start"]), "—", stamp(results["protocol"]["end"]))
    for r in results["results"]:
        if r["segment"] in {"all", "test"} and r["horizon"] == 5:
            print(r["segment"], r["rule"], r["counts"], r["metrics"])


if __name__ == "__main__":
    main()

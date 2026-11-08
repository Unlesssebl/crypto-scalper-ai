"""Descriptive forward outcomes of attention episodes, including rejected triggers.

Price excursions are observations, never trade profits or AI forecast accuracy.
The background sample is unadjusted; its comparison is not a causal estimate.
"""
import argparse
import csv
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path

from .archive import read_archive
from .replay import replay


def evaluate(path, output, *, control_count=5):
    if type(control_count) is not int or not 1 <= control_count <= 20:
        raise ValueError("control_count must be 1..20")
    verification = replay(path, retain_cards=False)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    pending = defaultdict(list)
    latest = {}
    background_slot = {}
    active = set()
    paired = {}
    # Exclude every selected symbol from the same decision batch, including
    # assignments appearing later in the archive's event order.
    selected_at = defaultdict(set)
    for assignment in verification["assignments"]:
        if assignment["state"] != "scout":
            selected_at[assignment["at"]].add(assignment["symbol"])
    summaries = defaultdict(lambda: dict(total=0, observed=0, incomplete=0, censored=0,
                                         sum_abs_end_bps=0, sum_max_abs_bps=0))
    writer = None
    count = 0
    first = last = None
    fields = ["symbol", "group", "start", "horizon", "status", "end_bps", "max_up_bps",
              "max_down_bps", "trigger_evidence", "pair_id", "observed_until", "elapsed_seconds"]
    with (output / "outcomes.csv").open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()

        def finish(item, status, end_bps=None):
            nonlocal count
            row = {k: item[k] for k in ("symbol", "group", "start", "horizon", "trigger_evidence")}
            row.update(status=status, end_bps=end_bps, max_up_bps=item["up"], max_down_bps=item["down"],
                       pair_id=item.get("pair_id", ""), observed_until=item["last"],
                       elapsed_seconds=item["last"] - item["start"])
            writer.writerow(row)
            if item.get("pair_id"):
                pair = paired[item["pair_id"], item["horizon"]]
                if item["group"] == "selected":
                    pair["selected"] = row
                else:
                    pair["controls"][item["symbol"]] = row
            count += 1
            summary = summaries[item["group"], item["horizon"]]
            summary["total"] += 1
            summary[status] += 1
            if status == "observed":
                summary["sum_abs_end_bps"] += abs(end_bps)
                summary["sum_max_abs_bps"] += max(item["up"], -item["down"])

        def begin(symbol, group, at, evidence, pair_id=""):
            previous = latest.get(symbol)
            if previous is None or not 0 <= at - previous[0] <= 20:
                return False
            for horizon in (60, 300, 900):
                pending[symbol].append(dict(symbol=symbol, group=group, start=at, horizon=horizon,
                                            price=previous[1], last=at, up=0, down=0, gap=False,
                                            trigger_evidence=evidence, pair_id=pair_id))
            return True

        for event in read_archive(path):
            now, kind = event["received_at"], event["kind"]
            first = now if first is None else min(first, now)
            last = now if last is None else max(last, now)
            evidence = f'{event["run"]}:{event["sequence"]}'
            if kind == "attention_state":
                active = set(event["payload"].get("active", {}))
            elif kind == "scout":
                for symbol, observation in event["payload"].items():
                    price = observation["price"]
                    if type(price) not in (int, float) or not math.isfinite(price) or price <= 0:
                        latest.pop(symbol, None)
                        for item in pending[symbol]:
                            item["gap"] = True
                        continue
                    latest[symbol] = (now, price)
                    keep = []
                    for item in pending[symbol]:
                        gap = now - item["last"]
                        item["gap"] |= gap > 30
                        item["last"] = now
                        change = (price / item["price"] - 1) * 10000
                        due = item["start"] + item["horizon"]
                        # The endpoint quote is part of the observed excursion,
                        # even when it arrives a few seconds after the horizon.
                        item["up"] = max(item["up"], change)
                        item["down"] = min(item["down"], change)
                        if now >= due:
                            status = "incomplete" if item["gap"] or now - due > 20 else "observed"
                            finish(item, status, change)
                        else:
                            keep.append(item)
                    pending[symbol] = keep
                    slot = int(now // 900)
                    if background_slot.get(symbol) != slot:
                        background_slot[symbol] = slot
                        begin(symbol, "background_unmatched", now, evidence)
                # Disappeared symbols cannot leave unlimited pending outcomes.
                for symbol in list(pending):
                    keep = []
                    for item in pending[symbol]:
                        if now > item["start"] + item["horizon"] + 20:
                            finish(item, "incomplete")
                        else:
                            keep.append(item)
                    if keep:
                        pending[symbol] = keep
                    else:
                        pending.pop(symbol, None)
            elif kind == "assignment":
                item = event["payload"]
                if item["state"] == "scout":
                    active.discard(item["symbol"])
                else:
                    active.add(item["symbol"])
                if item["previous"] == "scout":
                    group = "rejected_capacity" if item["reason"] == "capacity" else "selected"
                    # Collapse repeated capacity denials to one episode per 15m.
                    key = (item["symbol"], group)
                    slot = int(now // 900)
                    if group == "selected" or background_slot.get(key) != slot:
                        if group == "selected":
                            # A reproducible random baseline uses only prices
                            # available in this exact scout batch. No future
                            # outcome, dollar volume or alphabetical priority.
                            eligible = [s for s, (at, _) in latest.items()
                                        if at == now and s not in active and s not in selected_at[now]]
                            controls = sorted(eligible, key=lambda s: hashlib.sha256(
                                f"{evidence}:{s}".encode()).digest())[:control_count]
                            for horizon in (60, 300, 900):
                                paired[evidence, horizon] = dict(symbol=item["symbol"], start=now,
                                    selected=None, controls={}, control_symbols=controls)
                            begin(item["symbol"], group, now, evidence, evidence)
                            for symbol in controls:
                                begin(symbol, "time_matched_control", now, evidence, evidence)
                        else:
                            begin(item["symbol"], group, now, evidence)
                        background_slot[key] = slot
        for items in pending.values():
            for item in items:
                finish(item, "censored")
    comparisons = _compare_pairs(paired, output)
    report = dict(schema_version=2, start=first, end=last, rows=count, replay_checksum=verification["checksum"],
                  input_manifest_sha256=hashlib.sha256((Path(path) / "manifest.json").read_bytes()).hexdigest(),
                  method="Observed scout-price excursions; no orders, fees, slippage or direction forecast.",
                  limitation="Controls match decision time only, not volatility or liquidity. Episodes overlap; "
                             "differences are descriptive, not causal or trading returns. "
                             "Historical L2 of unselected coins is unavailable.",
                  control_count=control_count, time_matched=comparisons,
                  groups={f"{group}/{horizon}s": {**s,
                          "mean_abs_end_bps": s["sum_abs_end_bps"] / s["observed"] if s["observed"] else None,
                          "mean_max_abs_bps": s["sum_max_abs_bps"] / s["observed"] if s["observed"] else None}
                          for (group, horizon), s in summaries.items()})
    (output / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    return report


def _compare_pairs(paired, output):
    """Complete pairs only; report missing controls and censored outcomes too."""
    summaries = defaultdict(lambda: dict(total=0, observed=0, unavailable=0,
                                         sum_abs_end_lift_bps=0, sum_max_abs_lift_bps=0))
    fields = ["pair_id", "symbol", "start", "horizon", "status", "control_symbols", "controls_observed",
              "selected_status", "abs_end_lift_bps", "max_abs_lift_bps"]
    with (output / "pairs.csv").open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for (identity, horizon), pair in paired.items():
            chosen, controls = pair["selected"], pair["controls"]
            observed = [r for r in controls.values() if r["status"] == "observed"]
            complete = (chosen is not None and chosen["status"] == "observed"
                        and len(observed) == len(pair["control_symbols"]) and len(observed) > 0)
            row = dict(pair_id=identity, symbol=pair["symbol"], start=pair["start"], horizon=horizon,
                       status="observed" if complete else "unavailable",
                       control_symbols=json.dumps(pair["control_symbols"]), controls_observed=len(observed),
                       selected_status=chosen["status"] if chosen else "missing_start",
                       abs_end_lift_bps=None, max_abs_lift_bps=None)
            summary = summaries[f"{horizon}s"]
            summary["total"] += 1
            summary[row["status"]] += 1
            if complete:
                end_lift = abs(chosen["end_bps"]) - sum(abs(r["end_bps"]) for r in observed) / len(observed)
                max_lift = max(chosen["max_up_bps"], -chosen["max_down_bps"]) - sum(
                    max(r["max_up_bps"], -r["max_down_bps"]) for r in observed) / len(observed)
                row.update(abs_end_lift_bps=end_lift, max_abs_lift_bps=max_lift)
                summary["sum_abs_end_lift_bps"] += end_lift
                summary["sum_max_abs_lift_bps"] += max_lift
            writer.writerow(row)
    return {h: {**s, "coverage": s["observed"] / s["total"],
                "mean_abs_end_lift_bps": s["sum_abs_end_lift_bps"] / s["observed"] if s["observed"] else None,
                "mean_max_abs_lift_bps": s["sum_max_abs_lift_bps"] / s["observed"] if s["observed"] else None}
            for h, s in summaries.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--controls", type=int, default=5, help="same-time random controls per selection (1..20)")
    args = parser.parse_args()
    result = evaluate(args.archive, args.output, control_count=args.controls)
    print(json.dumps(dict(rows=result["rows"], groups=result["groups"], time_matched=result["time_matched"])))


if __name__ == "__main__":
    main()

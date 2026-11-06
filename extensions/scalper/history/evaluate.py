"""Descriptive forward outcomes of attention episodes, including rejected triggers.

Price excursions are observations, never trade profits or AI forecast accuracy.
The background sample is unadjusted; its comparison is not a causal estimate.
"""
import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path

from .archive import read_archive
from .replay import replay


def evaluate(path, output):
    verification = replay(path, retain_cards=False)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    pending = defaultdict(list)
    latest = {}
    background_slot = {}
    summaries = defaultdict(lambda: dict(total=0, observed=0, incomplete=0, censored=0,
                                         sum_abs_end_bps=0, sum_max_abs_bps=0))
    writer = None
    count = 0
    first = last = None
    fields = ["symbol", "group", "start", "horizon", "status", "end_bps", "max_up_bps",
              "max_down_bps", "trigger_evidence"]
    with (output / "outcomes.csv").open("x", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()

        def finish(item, status, end_bps=None):
            nonlocal count
            row = {k: item[k] for k in ("symbol", "group", "start", "horizon", "trigger_evidence")}
            row.update(status=status, end_bps=end_bps, max_up_bps=item["up"], max_down_bps=item["down"])
            writer.writerow(row)
            count += 1
            summary = summaries[item["group"], item["horizon"]]
            summary["total"] += 1
            summary[status] += 1
            if status == "observed":
                summary["sum_abs_end_bps"] += abs(end_bps)
                summary["sum_max_abs_bps"] += max(item["up"], -item["down"])

        def begin(symbol, group, at, evidence):
            previous = latest.get(symbol)
            if previous is None or at - previous[0] > 20:
                return
            for horizon in (60, 300, 900):
                pending[symbol].append(dict(symbol=symbol, group=group, start=at, horizon=horizon,
                                            price=previous[1], last=at, up=0, down=0, gap=False,
                                            trigger_evidence=evidence))

        for event in read_archive(path):
            now, kind = event["received_at"], event["kind"]
            first = now if first is None else min(first, now)
            last = now if last is None else max(last, now)
            evidence = f'{event["run"]}:{event["sequence"]}'
            if kind == "scout":
                for symbol, observation in event["payload"].items():
                    price = observation["price"]
                    latest[symbol] = (now, price)
                    keep = []
                    for item in pending[symbol]:
                        gap = now - item["last"]
                        item["gap"] |= gap > 30
                        item["last"] = now
                        change = (price / item["price"] - 1) * 10000
                        due = item["start"] + item["horizon"]
                        if now <= due:
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
                if item["previous"] == "scout":
                    group = "rejected_capacity" if item["reason"] == "capacity" else "selected"
                    # Collapse repeated capacity denials to one episode per 15m.
                    key = (item["symbol"], group)
                    slot = int(now // 900)
                    if group == "selected" or background_slot.get(key) != slot:
                        begin(item["symbol"], group, now, evidence)
                        background_slot[key] = slot
        for items in pending.values():
            for item in items:
                finish(item, "censored")
    report = dict(start=first, end=last, rows=count, replay_checksum=verification["checksum"],
                  method="Observed scout-price excursions; no orders, fees, slippage or direction forecast.",
                  limitation="Background is unmatched; historical L2 of unselected coins is unavailable.",
                  groups={f"{group}/{horizon}s": {**s,
                          "mean_abs_end_bps": s["sum_abs_end_bps"] / s["observed"] if s["observed"] else None,
                          "mean_max_abs_bps": s["sum_max_abs_bps"] / s["observed"] if s["observed"] else None}
                          for (group, horizon), s in summaries.items()})
    (output / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = evaluate(args.archive, args.output)
    print(json.dumps(dict(rows=result["rows"], groups=result["groups"])))


if __name__ == "__main__":
    main()

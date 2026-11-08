"""Offline replay of bot observations from a screener JSONL recording.

This is deliberately not a trading backtest.  It re-runs the same declared
triggers over rows that the live screener recorded with ``--record``, writes
observations to a separate database with ``source='replay'``, and settles
outcomes from the recorded mark prices.  No exchange, AI, Telegram or live
journal is touched.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
import time
from pathlib import Path

from .contracts import active_specs, spec_by_id
from .observer import BotObservatory
from .store import ensure_schema

LIMITATIONS = [
    "Input must be a screener --record JSONL with full rows (snapshot/hyperdata/strength).",
    "Attention archive alone has no OFI/native snapshots and cannot replay flow.",
    "Strength and native direction come from the recording; a changed trigger "
    "needs a new recording or a versioned recompute.",
    "Outcomes use recorded mark prices only; no candle fallback, fees, funding or slippage.",
    "If the recording has no attention plan, controls may include active-plan symbols.",
]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _rows(record: dict) -> dict:
    rows = record.get("rows") or []
    if isinstance(rows, dict):
        rows = rows.values()
    result = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        symbol, window = row.get("symbol"), row.get("window")
        if not isinstance(symbol, str) or not isinstance(window, str):
            continue
        if not isinstance(row.get("snapshot"), dict) or not isinstance(row.get("hyperdata"), dict):
            continue
        if not isinstance(row.get("strength"), dict):
            continue
        result[(symbol, window)] = {**row, "analysis_gate": row.get("analysis_gate", "ready")}
    return result


def _prices(rows: dict) -> dict:
    prices = {}
    for (symbol, _window), row in rows.items():
        snapshot = row.get("snapshot") or {}
        item = (snapshot.get("metrics") or {}).get("price") or {}
        value, stamp = item.get("value"), item.get("updated_at")
        stamp = stamp if isinstance(stamp, (int, float)) else snapshot.get("captured_at")
        if (
            item.get("quality") == "ok"
            and isinstance(value, (int, float))
            and math.isfinite(value)
            and value > 0
            and isinstance(stamp, (int, float))
            and math.isfinite(stamp)
        ):
            prices[symbol] = (float(value), float(stamp))
    return prices


def _records(path: Path):
    previous = None
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except ValueError as exc:
                raise ValueError(f"line {line_number}: invalid JSON") from exc
            if not isinstance(record, dict):
                raise ValueError(f"line {line_number}: expected an object")
            at = record.get("as_of")
            if not isinstance(at, (int, float)) or not math.isfinite(at):
                raise ValueError(f"line {line_number}: missing numeric as_of")
            at = float(at)
            if previous is not None and at < previous:
                raise ValueError(f"line {line_number}: as_of moved backwards")
            previous = at
            attention = record.get("attention")
            yield at, record, attention if isinstance(attention, dict) else {}


def _write_observations(db: sqlite3.Connection, path: Path, bot_id: str | None) -> int:
    condition = " AND o.bot_id=?" if bot_id else ""
    params = (bot_id,) if bot_id else ()
    cursor = db.execute(
        "SELECT o.source,o.bot_id,o.version,o.symbol,o.window,o.strength,o.direction,"
        "o.decided_at,o.entry_at,o.entry_price,o.entry_source,"
        "x.horizon_s,x.exit_at,x.exit_price,x.exit_source,x.move_bps,x.mae_bps,x.mfe_bps,"
        "x.status AS outcome_status,x.unscored_reason,"
        "(SELECT AVG(p.move_bps) FROM bot_control_points p "
        " WHERE p.control_set_id=o.control_set_id AND p.horizon_s=x.horizon_s "
        " AND p.status='scored') AS control_mean_bps "
        "FROM bot_observations o LEFT JOIN bot_outcomes x ON x.observation_id=o.id "
        "WHERE o.source='replay'" + condition +
        " ORDER BY o.decided_at,o.bot_id,x.horizon_s",
        params,
    )
    count = 0
    with path.open("x", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([column[0] for column in cursor.description])
        for row in cursor:
            writer.writerow(row)
            count += 1
    return count


def replay_recording(recording: Path | str, output: Path | str, *, bot_id: str | None = None) -> dict:
    """Replay one recording and write a self-contained output directory."""
    recording = Path(recording)
    output = Path(output)
    if not recording.is_file():
        raise ValueError(
            "Input must be a screener --record JSONL file. "
            "An attention archive directory does not contain the full rows needed by flow."
        )
    if bot_id is not None and spec_by_id(bot_id) is None:
        raise ValueError(f"Unknown bot: {bot_id}")
    output.mkdir(parents=True, exist_ok=False)
    specs = [spec_by_id(bot_id)] if bot_id else list(active_specs())

    db = sqlite3.connect(output / "bots-replay.sqlite")
    db.row_factory = sqlite3.Row
    ensure_schema(db)
    observatory = BotObservatory(db, specs, now=time.time(), source="replay")

    records = 0
    scans = 0
    skipped = 0
    first = last = None
    for at, record, attention in _records(recording):
        records += 1
        rows = _rows(record)
        if not rows:
            skipped += 1
            continue
        observatory.tick(at, rows, _prices(rows), attention)
        observatory.settle_mark_only(at)
        first = at if first is None else first
        last = at
        scans += 1

    if last is not None:
        # Right-censored observations stay unscored instead of borrowing a
        # future price or pretending the recording continued forever.
        observatory.settle_mark_only(last + 3600.0)

    report = observatory.report(bot_id) if bot_id else observatory.report()
    bots = [report["bot"]] if bot_id else report["bots"]
    protocol = {
        "schema_version": 1,
        "source": "replay",
        "input": str(recording),
        "input_sha256": _sha256(recording),
        "records": records,
        "scans": scans,
        "skipped_records": skipped,
        "start": first,
        "end": last,
        "settlement": "recorded mark prices only; no candle fallback, fees, funding or slippage",
        "bots": [
            {
                "id": spec.id,
                "version": spec.version,
                "contract_hash": spec.contract_hash,
                "mode": spec.mode,
                "primary_horizon_s": spec.primary_horizon_s,
                "window": spec.window,
            }
            for spec in specs
        ],
        "limitations": LIMITATIONS,
    }
    (output / "protocol.json").write_text(
        json.dumps(protocol, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    )
    summary = {
        "schema_version": 1,
        "source": "replay",
        "input": str(recording),
        "input_sha256": protocol["input_sha256"],
        "records": records,
        "scans": scans,
        "skipped_records": skipped,
        "start": first,
        "end": last,
        "bots": bots,
        "limitations": LIMITATIONS,
    }
    (output / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    )
    _write_observations(db, output / "observations.csv", bot_id)
    db.close()
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("recording", type=Path, help="screener --record JSONL file")
    parser.add_argument("--output", type=Path, required=True, help="new output directory")
    parser.add_argument("--bot", default=None, help="limit to one bot id (default: all declared bots)")
    args = parser.parse_args()
    summary = replay_recording(args.recording, args.output, bot_id=args.bot)
    print(
        json.dumps(
            {
                "records": summary["records"],
                "scans": summary["scans"],
                "bots": [
                    {
                        "id": bot["id"],
                        "observations": bot["observations"],
                        "scored": bot["scored"],
                        "status": bot["status"],
                    }
                    for bot in summary["bots"]
                ],
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()

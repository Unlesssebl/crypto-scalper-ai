"""Replay acceptance: full-row recordings, separate source, no future prices."""

from __future__ import annotations

import json
import sqlite3

import pytest

from extensions.scalper.bots.replay import replay_recording


def row(symbol, direction="NEUTRAL", rank=1, price=10.0, natr=1.0, window="5m", gate="ready"):
    return {
        "symbol": symbol,
        "window": window,
        "snapshot": {
            "symbol": symbol,
            "window": window,
            "captured_at": 1_000.0,
            "metrics": {
                "price": {"value": price, "quality": "ok", "updated_at": 1_000.0},
                "ofi": {"value": 0.7 if direction != "NEUTRAL" else 0.0, "quality": "ok", "updated_at": 1_000.0},
                "natr": {"value": natr, "quality": "ok", "updated_at": 1_000.0},
                "spread": {"value": 0.01, "quality": "ok", "updated_at": 1_000.0},
            },
        },
        "hyperdata": {
            "direction": direction,
            "native_signal": "STRONG_BULL" if direction == "BULLISH" else "STRONG_BEAR"
            if direction == "BEARISH"
            else "NEUTRAL",
            "quality": "ok",
        },
        "strength": {"rank": rank, "reasons": ["test"]},
        "analysis_gate": gate,
    }


def record(at, btc_price, direction="BULLISH", rank=3):
    rows = [
        row("BTC", direction, rank, btc_price),
        row("ETH", "NEUTRAL", 1, 10.0, 1.1),  # active-plan symbol: must not become a control
        row("SOL", "NEUTRAL", 1, 10.0, 1.1),
        row("XRP", "NEUTRAL", 1, 10.0, 1.2),
        row("DOGE", "NEUTRAL", 1, 10.0, 0.9),
        row("ADA", "NEUTRAL", 1, 10.0, 1.05),
        row("AVAX", "NEUTRAL", 1, 10.0, 1.15),
    ]
    for item in rows:
        item["snapshot"]["captured_at"] = at
        for metric in item["snapshot"]["metrics"].values():
            metric["updated_at"] = at
    return {"as_of": at, "attention": {"selected": ["ETH"]}, "rows": rows}


def write_recording(path, records):
    with path.open("w", encoding="utf-8") as handle:
        for item in records:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")


def test_replay_writes_separate_source_and_expected_outcomes(tmp_path):
    recording = tmp_path / "scan.jsonl"
    write_recording(
        recording,
        [
            record(1_000.0, 100.0),
            record(1_002.0, 100.0),
            record(1_062.0, 101.0),
            record(1_302.0, 102.0),
            record(1_902.0, 99.0),
        ],
    )
    output = tmp_path / "out"
    summary = replay_recording(recording, output)

    assert summary["source"] == "replay"
    assert summary["scans"] == 5
    assert (output / "protocol.json").exists()
    assert (output / "summary.json").exists()
    assert (output / "observations.csv").exists()

    db = sqlite3.connect(output / "bots-replay.sqlite")
    db.row_factory = sqlite3.Row
    observation = db.execute("SELECT * FROM bot_observations").fetchone()
    assert observation["source"] == "replay"
    assert observation["bot_id"] == "flow.continuation"
    outcomes = db.execute("SELECT * FROM bot_outcomes ORDER BY horizon_s").fetchall()
    assert [item["status"] for item in outcomes] == ["scored", "scored", "scored"]
    assert round(outcomes[1]["move_bps"], 1) == 200.0

    controls = db.execute("SELECT symbols FROM bot_control_sets").fetchone()[0]
    assert "ETH" not in controls
    assert "BTC" not in controls
    db.close()

    flow = next(bot for bot in summary["bots"] if bot["id"] == "flow.continuation")
    assert flow["observations"] == 1
    assert flow["scored"] == 1
    assert flow["status"] == "мало данных"


def test_replay_can_limit_to_one_bot_and_is_deterministic(tmp_path):
    recording = tmp_path / "scan.jsonl"
    write_recording(recording, [record(1_000.0, 100.0), record(1_002.0, 100.0)])
    first = replay_recording(recording, tmp_path / "one", bot_id="flow.continuation")
    second = replay_recording(recording, tmp_path / "two", bot_id="flow.continuation")
    assert [bot["id"] for bot in first["bots"]] == ["flow.continuation"]
    assert first["bots"] == second["bots"]
    assert first["input_sha256"] == second["input_sha256"]


def test_replay_rejects_attention_archive_directory(tmp_path):
    archive = tmp_path / "archive"
    archive.mkdir()
    (archive / "manifest.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError):
        replay_recording(archive, tmp_path / "out")


def test_replay_marks_unfinished_outcomes_unscored_without_future_prices(tmp_path):
    recording = tmp_path / "scan.jsonl"
    write_recording(recording, [record(1_000.0, 100.0), record(1_002.0, 100.0)])
    summary = replay_recording(recording, tmp_path / "out")

    conn = sqlite3.connect(tmp_path / "out" / "bots-replay.sqlite")
    rows = conn.execute("SELECT status FROM bot_outcomes ORDER BY horizon_s").fetchall()
    assert [row[0] for row in rows] == ["unscored", "unscored", "unscored"]
    conn.close()

    flow = next(bot for bot in summary["bots"] if bot["id"] == "flow.continuation")
    assert flow["observations"] == 1
    assert flow["scored"] == 0
    assert flow["coverage"] == 0.0
    assert flow["status"] == "ожидает исходов"


def test_replay_rejects_backwards_time_and_unknown_bot(tmp_path):
    recording = tmp_path / "scan.jsonl"
    write_recording(recording, [record(2_000.0, 100.0), record(1_000.0, 100.0)])
    with pytest.raises(ValueError):
        replay_recording(recording, tmp_path / "out")
    with pytest.raises(ValueError):
        replay_recording(recording, tmp_path / "out2", bot_id="no.such.bot")

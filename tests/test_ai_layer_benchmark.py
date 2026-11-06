"""Benchmark integrity: no future context, honest controls and field-level grading."""
import json
import sqlite3
from copy import deepcopy

from scripts.benchmark_ai_layers import (
    LOCAL_SCHEMA,
    history_for,
    meta_reference,
    packets,
    reference,
    score_local,
    synthetic_cases,
    validate_structure,
)


def test_other_window_warmup_never_invalidates_current_flow():
    case = next(c for c in synthetic_cases() if c["id"] == "control_other_window_warming")
    ref = reference(case["packet"])
    assert ref["flow_price"] == "aligned"
    assert ref["cross_window"] == "unavailable"
    assert "weak_data" not in ref["risk_flags"]
    case = next(c for c in synthetic_cases() if c["id"] == "control_primary_unavailable")
    assert reference(case["packet"])["flow_price"] == "insufficient"


def test_paired_input_differs_only_in_history():
    case = synthetic_cases()[0]
    assert case["packet"]["current"]["window"] == "1m"
    assert case["packet"]["context"]["window"] == "5m"
    raw, episode = packets(case, "snapshot"), packets(case, "episode")
    assert raw["current"] == episode["current"] and raw["context"] == episode["context"]
    assert raw["history"] == [] and len(episode["history"]) == 2
    assert reference(raw)["history"] == "unknown"
    assert reference(episode)["history"] == "same_side"


def test_history_query_excludes_future_and_old_observations():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE finding_candidates(symbol TEXT,window TEXT,detected_at REAL,snapshot TEXT)")
    snap = synthetic_cases()[0]["packet"]["current"]
    for at in (100, 890, 990, 999, 1000, 1001):
        db.execute("INSERT INTO finding_candidates VALUES(?,?,?,?)",
                   ("CONTROL", "1m", at, json.dumps({**snap, "captured_at": at})))
    assert [s["captured_at"] for s in history_for(db, "CONTROL", 1000)] == [890, 990, 999]
    db.close()


def test_scoring_rejects_bad_shape_and_unavailable_evidence():
    p = synthetic_cases()[0]["packet"]
    assert not validate_structure({}, LOCAL_SCHEMA)
    reply = {**reference(p), "summary": "Поток согласован с ценой.", "invalidation": "Изменение реакции цены.",
             "evidence": ["current.metrics.ofi", "current.metrics.last_trade_price"]}
    assert score_local(reply, p)["grounded"]
    p["current"]["metrics"]["ofi"]["quality"] = "warming_up"
    assert not score_local(reply, p)["grounded"]


def test_meta_must_reject_corrupted_weak_review():
    cases = {c["id"]: c for c in synthetic_cases()[:1]}
    cid = next(iter(cases))
    bundle = dict(case_ids=[cid], coverage=0.05)
    gold = reference(cases[cid]["packet"])
    valid = dict(id="valid", case_id=cid, assessment=gold)
    corrupted = deepcopy(valid)
    corrupted["id"] = "corrupted"
    corrupted["assessment"]["flow_book"] = "opposes"
    result = meta_reference(bundle, cases, [valid, corrupted])
    assert result["unreliable_review_ids"] == ["corrupted"]
    assert result["scope"] == "partial_market"

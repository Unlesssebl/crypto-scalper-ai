"""No future leakage, quality gates, holdout accounting and persistent corrections."""
from __future__ import annotations

import json
import sqlite3
from dataclasses import replace

import pytest

from extensions.scalper.pulse_shadow import ShadowJournal
from hyperdata_terminal.market_pulse.models import (
    MINUTE,
    Parameters,
    eligible,
    fit,
    latest_rows,
    research,
    score,
)

P = Parameters(lookback_minutes=30, min_samples=20, min_tail=2, quantile=.9, min_coverage=.9,
               local_minutes=3)


def row(i, count=None, **changes):
    count = i % 9 + 1 if count is None else count
    return {"venue": "hyperliquid", "native_market_id": "BTC", "channel": "trades",
            "aggregation_kind": "execution", "window_start_ms": i * MINUTE,
            "window_end_ms": (i + 1) * MINUTE, "quality": "observed_no_detected_gap",
            "provisional": False, "version": 1, "event_count": count, "execution_count": count,
            "buy_quote": str(count * 2), "sell_quote": "0",
            "coverage": {"instrument_spec_versions": ["s1"]}, **changes}


def test_target_and_future_cannot_change_baseline_or_decision():
    history = [row(i) for i in range(30)]
    target = row(30, 100)
    baseline = fit(history, target, params=P)
    assert baseline["status"] == "exploratory"
    assert baseline == fit([*history, target, row(31, 999999)], target, params=P)
    assert baseline["to_ms"] == target["window_start_ms"]
    assert score(baseline, target, history)["candidates"]["quantile"]


@pytest.mark.parametrize("change", [
    {"quality": "gap_detected"}, {"provisional": True}, {"coverage": {}},
    {"coverage": {"instrument_spec_versions": ["s1", "s2"]}},
    {"coverage": {"instrument_spec_versions": ["s1"], "notional_blocked_events": 1}},
    {"execution_count": -1}, {"execution_count": 1.5}, {"buy_quote": "NaN"},
    {"buy_quote": "-1"}, {"window_end_ms": 1},
])
def test_invalid_windows_never_enter_baseline(change):
    broken = row(0, **change)
    metric = "quote_volume" if "buy_quote" in change else "execution_count"
    assert not eligible(broken, metric)


def test_unknown_missing_spec_and_changed_contract_do_not_mix():
    history = [row(i) for i in range(30)]
    assert fit(history, row(30, coverage={}), params=P)["status"] == "unknown_specification"
    target = row(30, coverage={"instrument_spec_versions": ["s2"]})
    assert fit(history, target, params=P)["samples"] == 0


def test_real_zero_kept_gap_not_imputed_and_provisional_not_kept():
    history = [row(i, 0) for i in range(30)]
    history[4] = row(4, quality="gap_detected")
    history[5] = row(5, provisional=True)
    profile = fit(history, row(30), params=P)
    assert profile["samples"] == 28 and profile["median"] == 0
    result = score(profile, row(30), history)
    assert result["local_status"] == "zero_median" and result["local_ratio"] is None
    assert result["mad_status"] == "zero_mad" and result["mad_score"] is None
    json.dumps(result, allow_nan=False)


def test_requested_horizon_and_tail_both_gate_rarity():
    assert fit([row(i) for i in range(25)], row(30), params=P)["status"] == "insufficient_coverage"
    tail = replace(P, quantile=.99)
    profile = fit([row(i) for i in range(30)], row(30), params=tail)
    assert profile["status"] == "insufficient_tail"
    assert not score(profile, row(30, 10000))["candidates"]["quantile"]


def test_equal_revision_conflict_rejected_and_later_revision_selected():
    with pytest.raises(ValueError, match="conflicting"):
        latest_rows([row(1), row(1, 200)])
    assert latest_rows([row(1), row(1, 200, version=2)])[0]["execution_count"] == 200


def test_frozen_holdout_no_future_threshold_selection_and_candidate_suppression():
    rows = [row(i) for i in range(100)]
    report = research(rows, 0, 100 * MINUTE, P)
    changed = research([*rows[:70], *[row(i, 9999) for i in range(70, 100)]], 0, 100 * MINUTE, P)
    assert report["split_ms"] == 70 * MINUTE and not report["live_approved"]
    assert report["reports"][0]["profile"] == changed["reports"][0]["profile"]
    counts = changed["reports"][0]["methods"]["quantile"]
    assert counts["candidates"] == 30 and counts["episodes"] == 6 and counts["suppressed"] == 24


def test_missing_test_minutes_reduce_coverage_and_break_local_and_cusum_state():
    rows = [row(i) for i in range(100) if i != 90]
    report = research(rows, 0, 100 * MINUTE, P)
    item = report["reports"][0]
    assert item["holdout_usable_minutes"] == 29 and item["holdout_expected_minutes"] == 30
    after = next(s for s in item["scores"] if s["at_ms"] == 91 * MINUTE)
    assert after["local_status"] == "insufficient_contiguous_minutes"
    profile = fit(rows[:30], row(30), params=P)
    assert score(profile, row(30, 1), cusum=999, contiguous=False)["cusum"] < 1


def test_shadow_restart_idempotency_correction_and_reproducible_inputs(tmp_path):
    path = tmp_path / "shadow.sqlite"
    rows = [row(i) for i in range(40)]
    with sqlite3.connect(path) as db:
        journal = ShadowJournal(db)
        original = journal.evaluate(rows, row(40), 1)
        assert not original["live_approved"]
    with sqlite3.connect(path) as db:
        journal = ShadowJournal(db)
        assert journal.evaluate(rows, row(40), 2) == original
        assert db.execute("SELECT COUNT(*) FROM pulse_evaluations").fetchone()[0] == 2
        journal.reconcile([*rows, row(40, 999, version=2)], 3)
        assert db.execute("SELECT COUNT(*) FROM pulse_evaluations").fetchone()[0] == 4
        payload = json.loads(db.execute("SELECT payload FROM pulse_evaluations ORDER BY rowid DESC").fetchone()[0])
        assert payload["correction_only"] and payload["correction_of"]
        profile = json.loads(db.execute("SELECT payload FROM pulse_profiles WHERE id=?",
                                       (payload["baseline_version"],)).fetchone()[0])
        replay = score(profile, payload["target"], payload["local_inputs"])
        replay["candidates"].pop("cusum")
        replay.pop("cusum")
        assert replay == payload["assessment"]
        journal.reconcile([*rows, row(40, 999, version=2)], 4)
        assert db.execute("SELECT COUNT(*) FROM pulse_evaluations").fetchone()[0] == 4


def test_late_revision_links_existing_card_and_survives_restart(tmp_path):
    from extensions.scalper.journal import Journal
    from extensions.scalper.service import alert_market_facts

    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    shadow = ShadowJournal(journal.db)
    history = [row(i) for i in range(40)]
    assessment = shadow.evaluate(history, row(40), 1)
    snapshot = {"symbol": "BTC", "window": "1m", "captured_at": 1,
                "metrics": {}, "market_pulse": {"status": "ready", "venues": [
                    {"venue": "hyperliquid", "quality": "observed_no_detected_gap", "shadow": assessment,
                     "execution_count": 5, "quote_volume": "10", "currency": "USDC", "ofi": 1.0}]}}
    assert journal.register_fact_message("card", snapshot, {"detected_at": 1}, {})
    journal.fact_message_sent("card", 123, False)
    shadow.reconcile([*history, row(40, 999, version=2)], 3)
    assert journal.pending_pulse_edits() == ["card"]
    saved = journal.fact_message("card")
    assert saved["message_id"] == 123
    assert len(saved["snapshot"]["market_pulse_corrections"]) == 2
    facts = str(alert_market_facts(saved["snapshot"]))
    assert "Исходные межбиржевые данные" in facts and "999 исполнений" in facts
    journal.pulse_edit_status("card", "queued")
    journal.close()
    journal = Journal(path)
    journal.recover()
    assert journal.pending_pulse_edits() == ["card"]
    journal.pulse_edit_status("card", "failed")
    assert journal.pending_pulse_edits() == []  # Failed edit remains visible, no uncontrolled retry loop.
    journal.close()


async def test_adapter_records_all_covered_markets_without_screener_slots(tmp_path):
    import aiohttp
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    from extensions.scalper.journal import Journal
    from extensions.scalper.pulse import PulseContext

    target = row(40)
    rows = [row(i) for i in range(41)]
    now = (target["window_end_ms"] + 5000) / 1000

    async def windows(request):
        return web.json_response({"schema_version": "market-pulse.window.v1", "windows": rows,
                                  "next_from_ms": None})

    async def coverage(request):
        return web.json_response({"schema_version": "market-pulse.window.v1", "channels": [{
            "venue": "hyperliquid", "native_market_id": "BTC", "channel": "trades", "stale": False}]})

    app = web.Application()
    app.router.add_get("/windows", windows)
    app.router.add_get("/coverage", coverage)
    journal = Journal(tmp_path / "journal.sqlite")
    async with TestServer(app) as server, aiohttp.ClientSession() as session:
        pulse = PulseContext(str(server.make_url("")), journal=journal)
        await pulse.refresh(session, now)
        facts = pulse.facts("BTC", now)
        assert facts["venues"][0]["shadow"]["metrics"]["execution_count"]["samples"] == 0
        assert facts["activity"] == "threshold_not_calibrated" and facts["status"] == "partial"
        assert journal.db.execute("SELECT COUNT(*) FROM pulse_evaluations").fetchone()[0] == 2
        rows[-1] = row(40, 500, version=2)
        await pulse.refresh(session, now + 1)
        assert pulse.facts("BTC", now + 1)["venues"][0]["shadow"]["metrics"]["execution_count"]["value"] == 500
        assert journal.db.execute("SELECT COUNT(*) FROM pulse_evaluations").fetchone()[0] == 4
    journal.close()


def test_completely_absent_streams_remain_in_research_denominator():
    report = research([], 0, 100 * MINUTE, P)
    assert len(report["reports"]) == 8
    assert all(r["holdout_coverage"] == 0 for r in report["reports"])


def test_late_baseline_revision_recomputes_dependent_assessment(tmp_path):
    rows = [row(i) for i in range(100)]
    with sqlite3.connect(tmp_path / "ledger.sqlite") as db:
        ledger = ShadowJournal(db)
        ledger.reconcile(rows, 6001)
        original = ledger.evaluate(rows, row(100), 6002)
        rows[5] = row(5, 1000, version=2)
        ledger.reconcile([*rows, row(100)], 6003)
        latest = ledger.evaluate(rows, row(100), 6004)
        assert original["metrics"]["execution_count"]["baseline_version"] != latest[
            "metrics"]["execution_count"]["baseline_version"]
        assert db.execute("SELECT COUNT(*) FROM pulse_evaluations").fetchone()[0] == 4


@pytest.mark.parametrize("success", [True, False])
async def test_correction_delivery_edits_existing_message_and_records_failure(success):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from extensions.scalper.service import Screener

    states = []
    saved = {"id": "existing", "snapshot": {"symbol": "BTC", "window": "1m"},
             "hyperdata": {}, "event": {}, "ai": None, "message_id": 123, "rich": False}
    journal = SimpleNamespace(fact_message=lambda key: saved, ai_reason=lambda key: "queued",
                              fact_updates=lambda key: {}, fact_message_edit=lambda *a: None,
                              pulse_edit_status=lambda *a: states.append(a))
    service = Screener(None, journal, None, True)
    service.edit_alert = AsyncMock(return_value=success)
    service.notifications.put_nowait(("pulse_edit", "existing"))
    task = asyncio.create_task(service.deliver())
    try:
        await asyncio.wait_for(service.notifications.join(), 2)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
    service.edit_alert.assert_awaited_once()
    assert states == [("existing", "edited" if success else "failed")]


def test_hourly_reference_reused_across_minutes_and_restart(tmp_path):
    history = [row(i) for i in range(91)]
    with sqlite3.connect(tmp_path / 'hourly.sqlite') as db:
        ledger = ShadowJournal(db)
        ledger.reconcile(history, 5500)
        first = ledger.evaluate(history, row(90), 5500)
        history.append(row(91))
        ledger.reconcile(history, 5501)
        second = ledger.evaluate(history, row(91), 5501)
        assert first['metrics']['execution_count']['baseline_version'] == second[
            'metrics']['execution_count']['baseline_version']
        assert db.execute('SELECT COUNT(*) FROM pulse_profiles').fetchone()[0] == 2
        ledger = ShadowJournal(db)
        third = ledger.evaluate(history, row(92), 5502)
        assert third['metrics']['execution_count']['baseline_version'] == first[
            'metrics']['execution_count']['baseline_version']
        assert db.execute('SELECT COUNT(*) FROM pulse_profiles').fetchone()[0] == 2

from pathlib import Path
from types import SimpleNamespace

from extensions.scalper.audit import RULESET
from extensions.scalper.journal import Journal
from extensions.scalper.quality import calculate_quality
from extensions.scalper.service import Screener
from extensions.scalper.shadow import ShadowDetector


def snapshot(at, natr=1.0):
    return {
        "symbol": "BTC", "exchange": "hyperliquid", "contract": "BTC-PERP", "window": "1m",
        "captured_at": at,
        "metrics": {
            "price": {"quality": "ok", "value": 100},
            "ofi": {"quality": "ok", "value": 0.5},
            "natr": {"quality": "ok", "value": natr},
        },
    }


def test_blocked_candidate_is_logged_once_then_updates_when_queued(tmp_path: Path):
    journal = Journal(tmp_path / "audit.sqlite")
    try:
        service = Screener(SimpleNamespace(), journal, None, False)
        key = ("BTC", "1m")
        native = {"direction": "BULLISH", "native_signal": "STRONG_BULL", "source": "test"}
        service.detect(key, snapshot(100), native, 100, "book_unavailable", "neutral")
        service.detect(key, snapshot(105), native, 105, "book_unavailable", "neutral")
        service.detect(key, snapshot(110), native, 110, "ready", "neutral")
        rows = journal.db.execute("SELECT detected_at, decision FROM finding_candidates").fetchall()
        assert [tuple(row) for row in rows] == [(100, "queued")]
        transitions = journal.db.execute("SELECT at, decision FROM finding_decisions ORDER BY at")
        assert [tuple(row) for row in transitions] == [(100, "book_unavailable"), (110, "queued")]
        assert service.jobs.get_nowait()[4]["detected_at"] == 100
    finally:
        journal.close()


def test_rank_outcome_uses_peak_after_delivery_and_separates_missing_coverage(tmp_path: Path):
    path = tmp_path / "audit.sqlite"
    journal = Journal(path)
    try:
        for name, delivered, rank in (("hit", 110, 4), ("quiet", 510, 4), ("gap", 910, 3)):
            snap = snapshot(delivered - 10)
            event = {"strength": {"rank": rank}}
            journal.record_candidate(name, snap, event, "queued", delivered - 12)
            journal.record(name, event, snap, {})
            journal.candidate_decision(name, "analyzing", delivered - 3, name)
            journal.delivery(name, "sent", delivered)
            journal.observe(delivered, {"BTC": (120, delivered - 1)})
            for offset in range(5, 300, 10):
                if name == "gap" and 60 <= offset <= 105:
                    continue
                price = 102 if name == "hit" and offset == 65 else 100
                journal.observe(delivered + offset, {"BTC": (price, delivered + offset)})
            journal.observe(delivered + 331, {"BTC": (150, delivered + 305)})
        report = calculate_quality(path, 1250, ("1m", "5m"))["finding_audit"]["1m"]
        assert report["decisions"] == {"delivered": 3}
        ranks = report["versions"][RULESET]
        assert ranks[4] == {"hit": 1, "quiet": 1, "median_delay_seconds": 12}
        assert ranks[3] == {"missing": 1, "median_delay_seconds": 12}
    finally:
        journal.close()


def test_completed_signals_match_quiet_controls_once_by_instrument_window_and_natr(tmp_path):
    path = tmp_path / "audit.sqlite"
    journal = Journal(path)
    try:
        for key, sent_at in (("first", 2000), ("second", 2400)):
            snap = snapshot(sent_at - 10)
            event = {"strength": {"rank": 4}}
            journal.record_candidate(key, snap, event, "queued", sent_at - 60)
            journal.record(key, event, snap, {})
            journal.candidate_decision(key, "analyzing", sent_at - 3, key)
            journal.delivery(key, "sent", sent_at)
        journal.db.execute("UPDATE delivered_moves SET status='hit'")
        journal.record_quiet_moves([
            ("BTC", "1m", 1000, 1000, 1.5), ("BTC", "1m", 1400, 1400, 5),
            ("BTC", "5m", 1000, 1000, 1), ("SOL", "1m", 1000, 1000, 1),
        ])
        journal.db.execute("UPDATE quiet_moves SET status='quiet'")
        journal.db.commit()
        audit = calculate_quality(path, 4000, ("1m", "5m"))["finding_audit"]["1m"]
        assert audit["control"] == {"pairs": 1, "signal_hits": 1, "control_hits": 0}
        assert audit["delay_buckets"][">45 с"] == {"hit": 2}
    finally:
        journal.close()

def test_weak_anomaly_does_not_generate_telegram_alert():
    service = Screener(None, None, None, True)
    service.enqueue_facts("weak", snapshot(100), {"direction": "BULLISH"},
                          {"anomalies": ["range_burst"], "strength": {"rank": 2}, "detected_at": 100},
                          {}, 100)
    assert service.notifications.empty()


def test_shadow_variants_use_same_snapshot_and_do_not_repeat_an_active_finding(tmp_path):
    snap = snapshot(100, natr=0.1)
    for key, value in {"spread": 0.001, "activity_ratio": 1.7, "trade_count": 20,
                       "range_ratio": 1, "first_trade_price": 100, "last_trade_price": 100.12}.items():
        snap["metrics"][key] = {"quality": "ok", "value": value}
    native = {"direction": "BULLISH", "native_signal": "BULLISH"}
    tracker = ShadowDetector()
    key = ("BTC", "1m")
    rows = tracker.evaluate(key, snap, native)
    assert [row[0] for row in rows] == ["Лента от 1,5×"]
    assert tracker.evaluate(key, {**snap, "captured_at": 105}, native) == []
    path = tmp_path / "shadow.sqlite"
    journal = Journal(path)
    try:
        journal.record_shadow_findings(rows)
        for at in range(105, 400, 10):
            journal.observe(at, {"BTC": (100 if at == 105 else 101, at)})
        journal.observe(431, {})
        report = calculate_quality(path, 431, ("1m", "5m"))
        assert report["shadow"]["1m"]["Лента от 1,5×"] == {"hit": 1}
        assert report["finding_audit"]["1m"]["candidates"] == 0
    finally:
        journal.close()


def test_fact_enqueue_does_not_require_an_ai_slot(tmp_path, monkeypatch):
    path = tmp_path / "audit.sqlite"
    journal = Journal(path)
    try:
        service = Screener(SimpleNamespace(), journal, None, True)
        for i in range(service.jobs.maxsize):
            service.jobs.put_nowait((i,))
        monkeypatch.setattr("extensions.scalper.service.active_anomalies", lambda snapshot: {"tape_burst"})
        monkeypatch.setattr("extensions.scalper.service.finding_strength", lambda *args: {"rank": 3})
        snap = snapshot(100)
        native = {"direction": "BULLISH", "native_signal": "STRONG_BULL", "source": "test"}
        row = {"facts": ["наблюдение"], "movement_phase": None, "other": None, "btc_context": None}
        service.detect(("BTC", "1m"), snap, native, 100, "ready", "neutral", row)
        action, key = service.notifications.get_nowait()
        assert action == "facts" and journal.fact_message(key)["ai"] is None
        assert journal.db.execute("SELECT decision FROM finding_candidates").fetchone()[0] == "queue_full"
    finally:
        journal.close()

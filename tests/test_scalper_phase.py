import pytest

from extensions.scalper.phase import PhaseTracker


def snapshot(at, price, rate=10, tape=1):
    return {"captured_at": at, "metrics": {
        name: {"value": value, "quality": "ok", "updated_at": at}
        for name, value in {"price": price, "ofi": 0.5, "trade_rate": rate,
                            "natr": 1.0, "activity_ratio": tape, "trade_count": 20}.items()
    }}


def test_impulse_phase_requires_an_observed_start_and_tracks_extension_and_fade():
    tracker = PhaseTracker()
    key = ("BTC", "1m")
    bull = {"direction": "BULLISH"}
    assert tracker.observe(key, snapshot(0, 100), bull, 60)["phase"] == "untracked"
    early = tracker.observe(key, snapshot(5, 100, tape=2.5), bull, 60)
    assert early["phase"] == "early" and early["started_at"] == 5
    for at in (10, 15, 20):
        phase = tracker.observe(key, snapshot(at, 100.6, tape=2.5), bull, 60)
    assert phase["phase"] == "confirmed"
    extended = tracker.observe(key, snapshot(25, 102.2), bull, 60)
    assert extended["phase"] == "extended"
    assert extended["distance_natr"] == pytest.approx(2.2)
    for at in (30, 35, 40):
        phase = tracker.observe(key, snapshot(at, 101.4, rate=4), bull, 60)
    assert phase["phase"] == "weakening"
    assert phase["retracement_natr"] == pytest.approx(0.8)
    reverse = tracker.observe(key, snapshot(45, 101.2), {"direction": "BEARISH"}, 60)
    assert reverse["phase"] == "early" and reverse["start_price"] == 101.2
    assert tracker.observe(key, snapshot(65, 100.8), {"direction": "BEARISH"}, 60)["phase"] == "untracked"


def test_repeated_mark_observation_does_not_confirm_the_impulse():
    tracker = PhaseTracker()
    key = ("BTC", "1m")
    bull = {"direction": "BULLISH"}
    tracker.observe(key, snapshot(0, 100), bull, 60)
    tracker.observe(key, snapshot(5, 100, tape=2.5), bull, 60)
    for at in (10, 15, 20):
        row = snapshot(at, 100.7)
        row["metrics"]["price"]["updated_at"] = 10
        assert tracker.observe(key, row, bull, 60)["phase"] == "early"

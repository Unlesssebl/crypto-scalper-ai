"""Causal context, phase-aware admission and descriptive quality without future leakage."""

import json
from copy import deepcopy
from types import SimpleNamespace

import pytest

from extensions.scalper.analytics_context import (
    VERSION,
    admission,
    compact_model_packet,
    extend_packet,
    fact_value,
    scenario_plan,
)
from extensions.scalper.context_auto import summarize
from extensions.scalper.fact_packet import build_packet, checksum, verify_packet
from extensions.scalper.journal import Journal
from extensions.scalper.notification_quality import NotificationQuality
from extensions.scalper.profile import calculate_profiles
from extensions.scalper.telegram_ux import analytical_details, operational, scenarios


def setup(now=1000200, phase="confirmed", wide=False):
    def metric(value, at=None, quality="ok"):
        return dict(value=value, updated_at=now if at is None else at, quality=quality, source="test-native")

    rows = {}
    for window, seconds in (("1m", 60), ("5m", 300)):
        metrics = {
            k: metric(v)
            for k, v in dict(
                price=102,
                ofi=0.8,
                cvd=1000,
                trade_count=100,
                spread=1 if wide else 0.05,
                natr=1,
                activity_ratio=5,
                volume=2000,
                trade_rate=2,
                imbalance=0.5,
                funding_rate=0.00001,
                open_interest=1e7,
            ).items()
        }
        metrics["first_trade_price"] = metric(100, now - seconds + 1)
        metrics["last_trade_price"] = metric(102)
        metrics["range_ratio"] = metric(1.8)
        rows["BTC", window] = dict(
            snapshot=dict(symbol="BTC", window=window, captured_at=now, metrics=metrics),
            movement_phase=dict(
                phase=phase,
                started_at=now - 300,
                start_price=100,
                natr_at_start=1,
                distance_natr=2,
                retracement_natr=0.1,
                age_seconds=300,
            ),
            strength=dict(rank=3),
            profile=dict(
                updated_at=now - 600,
                calculated_at=now - 5,
                baseline_policy="strictly_prior_rolling_intervals.v2",
                periods={d: dict(metrics=dict(trades=dict(samples=50, median=20, p90=60))) for d in (7, 30)},
            ),
            btc_context=dict(
                updated_at=now - 60,
                samples=36,
                correlation=0.8,
                relative_last_return_pp=0.4,
                btc_last_return_pct=0.1,
                asset_last_return_pct=0.5,
            ),
        )
    card = dict(
        status="observing",
        received_at=now,
        midpoint=102,
        spread_bps=5,
        evidence="book:1",
        observed_turnover_60s_usd=10000,
        densities=[
            dict(
                side="bid",
                price=101.9,
                size=100,
                relative_size=8,
                observed_age_seconds=40,
                size_change=-5,
                distance_bps=10,
                size_to_observed_turnover=1,
            )
        ],
    )
    assignment = dict(symbol="BTC", state="focus", assigned_at=now - 500, score=5, facts=card)
    base = build_packet(
        dict(policy_version=4, archive=dict(error=None)), assignment, now, frames=[r["snapshot"] for r in rows.values()]
    )
    end = ((int(now * 1000) - 5000) // 60000) * 60000
    pulse = dict(
        status="ready",
        window_start_ms=end - 60000,
        window_end_ms=end,
        evidence_id="pulse:v1",
        flow="aligned_buy",
        venues=[
            dict(
                venue=v,
                quality="observed_no_detected_gap",
                ofi=0.5,
                execution_count=100,
                quote_volume="2000",
                version=1,
            )
            for v in ("hyperliquid", "okx")
        ],
    )
    service = SimpleNamespace(
        rows=rows,
        pulse=SimpleNamespace(facts=lambda *_: pulse),
        attention=SimpleNamespace(
            activity={
                "BTC": dict(status="ready", start_ms=now * 1000 - 60000, ratios=dict(trade_count=3, base_volume=2))
            }
        ),
        candles={("BTC", "15m"): dict(closed_at=now - 300, return_4=2, range_4=3, range_position_4=0.8, natr=1)},
        hub=SimpleNamespace(context={"lsr": {"BTC": dict(long_short_ratio=1.4, timestamp=now, source="venue")}}),
    )
    return extend_packet(base, service), service, base, pulse


def test_full_context_is_immutable_and_preserves_sources():
    p, service, base, _ = setup()
    assert p["context_version"] == VERSION and verify_packet(p)
    assert "context_version" not in base
    assert fact_value(p, "context.phase.1m.phase") == "confirmed"
    assert fact_value(p, "profile.1m.7d.trades.current_ratio") == 5
    assert fact_value(p, "btc.relative_last_return_pp") == 0.4
    assert fact_value(p, "venue.relation") == "aligned_buy"
    assert fact_value(p, "context.1m.open_interest") == 1e7
    assert fact_value(p, "market.confirmed_liquidations_1h") is None
    frozen = deepcopy(p)
    service.rows["BTC", "1m"]["movement_phase"]["phase"] = "extended"
    assert p == frozen
    refs = next(f for f in p["facts"] if f["id"] == "venue.okx.ofi")
    assert refs["evidence"] and refs["window"] == "closed_utc_1m"


@pytest.mark.parametrize("corrupt", ["future", "stale", "overlap", "few_samples"])
def test_bad_profile_is_never_a_valid_comparison(corrupt):
    p, service, base, _ = setup()
    profile = service.rows["BTC", "1m"]["profile"]
    if corrupt == "future":
        profile["calculated_at"] = base["captured_at"] + 1
    elif corrupt == "stale":
        profile["calculated_at"] = base["captured_at"] - 601
    elif corrupt == "overlap":
        profile["updated_at"] = base["captured_at"] - 10
    else:
        profile["periods"][7]["metrics"]["trades"]["samples"] = 19
    result = extend_packet(base, service)
    assert fact_value(result, "profile.1m.7d.trades.current_ratio") is None


def test_cross_venue_requires_same_closed_minute_and_known_normalization():
    _, service, base, pulse = setup()
    pulse["window_end_ms"] += 60000
    assert fact_value(extend_packet(base, service), "venue.relation") is None
    pulse["window_end_ms"] -= 60000
    pulse["venues"][1]["quality"] = "normalization_unavailable"
    assert fact_value(extend_packet(base, service), "venue.okx.quote_volume") is None
    assert fact_value(extend_packet(base, service), "venue.relation") is None


def test_future_volatility_cannot_clear_the_spread_gate():
    _, service, base, _ = setup(wide=True)
    service.rows["BTC", "1m"]["snapshot"]["metrics"]["natr"]["updated_at"] += 1
    result = extend_packet(base, service)
    assert fact_value(result, "context.1m.natr") is None
    assert fact_value(result, "context.1m.wide_spread") is None


def test_scale_requires_observed_span_and_phase():
    _, service, base, _ = setup()
    assert fact_value(extend_packet(base, service), "formation.scale") == "5m"
    for fact in base["facts"]:
        if fact["id"].endswith("first_trade_price"):
            fact["source_time"] = base["captured_at"] - 10
    base["packet_id"] = checksum({k: v for k, v in base.items() if k != "packet_id"})
    assert fact_value(extend_packet(base, service), "formation.scale") is None


def test_rehash_cannot_turn_invented_anchor_into_observed_fact():
    p, _, _, _ = setup()
    p["scenarios"]["anchor"] += 10
    p["packet_id"] = checksum({k: v for k, v in p.items() if k != "packet_id"})
    with pytest.raises(ValueError, match="anchor differs"):
        verify_packet(p)


def test_late_phase_and_spread_gate_do_not_depend_on_ai():
    p, _, _, _ = setup(phase="extended")
    state = summarize(p)
    assert admission(state, "early_context") == "late_impulse"
    assert admission(state, "density_context") is None  # A new level may still matter, with a late-phase label.
    text, _ = operational(p, state, "density_context", "situation")
    assert "далеко ушло" in text and "Ранний импульс" not in text
    wide, _, _, _ = setup(wide=True)
    assert admission(summarize(wide), "density_context") == "wide_spread"


def test_real_levels_and_invalidation_conditions_are_not_invented():
    p, _, _, _ = setup()
    plan = scenario_plan(p)
    assert plan["anchor"] == 101.9 and plan["anchor_ref"].startswith("density.")
    text = "\n".join(scenarios(summarize(p)))
    assert "101.9" in text and "два наблюдения ниже" in text
    assert all(ref in {f["id"] for f in p["facts"]} for ref in plan["references"])
    details = "\n".join(analytical_details(p, summarize(p)))
    assert "Выборка 7д" in details and "Корреляция" in details and "Изменение размера зоны" in details
    assert "не доказанную сезонную редкость" in details


def test_compact_model_view_references_same_frozen_packet():
    p, _, _, _ = setup()
    view = compact_model_packet(p)
    assert view["packet_id"] == p["packet_id"]
    assert {f["id"] for f in view["facts"]} == {f["id"] for f in p["facts"]}
    assert len(json.dumps(view)) < len(json.dumps(p))
    assert verify_packet(p)


def test_profile_builder_excludes_the_current_and_overlapping_samples(tmp_path):
    path = tmp_path / "journal.sqlite"
    journal = Journal(path)
    try:
        now = 1000200
        for i in range(30):
            journal.record_instrument_samples([("BTC", "1m", now - 120 - i * 60, 20, 1, 2, 0.1, 0.5)])
        journal.record_instrument_samples(
            [("BTC", "1m", now, 1e9, 1, 2, 0.1, 0.5), ("BTC", "1m", now - 30, 1e9, 1, 2, 0.1, 0.5)]
        )
        profile = calculate_profiles(path, now, ["BTC"])["BTC", "1m"]
        assert profile["periods"][7]["metrics"]["trades"]["median"] == 20
        assert profile["updated_at"] <= now - 60
        assert profile["baseline_policy"].endswith(".v2")
    finally:
        journal.close()


def test_future_prices_only_enter_post_selection_observations(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    try:
        quality = NotificationQuality(journal.db)
        p, _, _, _ = setup()
        first = deepcopy(p)
        quality.record(p, summarize(p), "queued", "formation_confirmed", card="one")
        assert quality.report(p["captured_at"])["outcomes"][0]["observed"] == 0
        for offset in range(10, 71, 10):
            later = deepcopy(p)
            later["captured_at"] += offset
            for f in later["facts"]:
                if f["id"] == "book.midpoint":
                    f["observed_at"] += offset
                    f["value"] = 103
            later["packet_id"] = checksum({k: v for k, v in later.items() if k != "packet_id"})
            quality.sample({"BTC": later}, later["captured_at"])
        assert p == first
        observations = list(journal.db.execute("SELECT * FROM notification_observations WHERE horizon=60"))
        assert len(observations) == 1 and observations[0]["status"] == "observed"
        assert observations[0]["return_pct"] == pytest.approx((103 / 102 - 1) * 100)
        quality.sample({}, p["captured_at"] + 1000)
        rows = list(journal.db.execute("SELECT * FROM notification_observations WHERE horizon=300"))
        assert rows[0]["status"] == "outside_attention" and rows[0]["return_pct"] is None
    finally:
        journal.close()


def test_gap_withholds_extrema_even_when_endpoint_is_known(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    try:
        quality = NotificationQuality(journal.db)
        p, _, _, _ = setup()
        quality.record(p, summarize(p), "queued", "formation_confirmed")
        later = deepcopy(p)
        later["captured_at"] += 60
        for fact in later["facts"]:
            if fact["id"] == "book.midpoint":
                fact["observed_at"] += 60
        later["packet_id"] = checksum({k: v for k, v in later.items() if k != "packet_id"})
        quality.sample({"BTC": later}, later["captured_at"])
        observed = journal.db.execute("SELECT * FROM notification_observations WHERE horizon=60").fetchone()
        assert observed["status"] == "observed" and not observed["continuous"]
        assert observed["low_pct"] is None and observed["high_pct"] is None
    finally:
        journal.close()

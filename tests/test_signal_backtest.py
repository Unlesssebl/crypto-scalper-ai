"""Execution, causality, abstentions and missing-price accounting in signal replay."""
import pytest

from scripts.backtest_signals import (
    Quote,
    Signal,
    bar_outcome,
    context_at,
    economic_return,
    eligible,
    paired_ai,
    quote_after,
    random_thinning,
    replay,
    trade_outcome,
)


def signal(key="one", at=0, ready=0):
    return Signal(key, "BTC", "1m", at, ready, 1, 3, "confirmed", True, 0.5, "aligned")


def test_flat_market_loses_spread_fees_and_slippage_for_both_directions():
    q = Quote(2, 2, 99.99, 100.01)
    for direction in (1, -1):
        result = economic_return(q, q, direction)
        assert result["midpoint_bps"] == 0
        assert result["net_bps"] < -12.9


def test_later_capture_of_old_book_is_not_an_execution_quote():
    rows = [Quote(2, 0, 100, 101), Quote(5, 5, 100, 101)]
    assert quote_after(rows, [2, 5], 1) == rows[1]
    assert quote_after(rows, [2, 5], 10) is None


def test_ai_enters_after_actual_ready_time_and_holds_from_fill():
    rows = [Quote(2, 2, 100, 100.01), Quote(22, 22, 102, 102.01), Quote(82, 82, 103, 103.01)]
    s = signal(ready=20)
    result = trade_outcome(s, rows, [q.at for q in rows], 1, 1000)
    assert result["entry_at"] == 22 and result["exit_at"] == 82
    assert result["entry_price"] > 102


def test_no_overlapping_positions_across_1m_and_5m():
    a, b = signal(), signal("two", at=3, ready=3)
    b.window = "5m"
    rows = [Quote(2, 2, 100, 100.01), Quote(5, 5, 100, 100.01), Quote(62, 62, 101, 101.01)]
    r = replay([a, b], {"BTC": rows}, {"BTC": [q.at for q in rows]}, "native", 1, 0, 1000, 1000)
    assert r["metrics"]["n"] == 1
    assert r["counts"]["position_open"] == 1


def test_missing_exit_keeps_position_open_instead_of_selecting_future_winners():
    a, b = signal(), signal("two", at=120, ready=120)
    rows = [Quote(2, 2, 100, 100.01), Quote(122, 122, 101, 101.01), Quote(182, 182, 102, 102.01)]
    r = replay([a, b], {"BTC": rows}, {"BTC": [q.at for q in rows]}, "native", 1, 0, 1000, 1000)
    assert r["counts"]["missing_exit_quote"] == 1
    assert r["counts"]["position_open"] == 1
    assert r["metrics"]["n"] == 0


def test_context_uses_only_fresh_previous_snapshot():
    def snap(at, ofi):
        return {"captured_at": at, "metrics": {"ofi": {"quality": "ok", "value": ofi, "updated_at": at}}}
    key = ("BTC", "5m")
    rows = {key: [snap(10, 0.8), snap(21, -0.8)]}
    times = {key: [10, 21]}
    assert context_at(rows, times, "BTC", "1m", 20, 1) == "aligned"
    assert context_at(rows, times, "BTC", "1m", 9, 1) == "unavailable"
    assert context_at(rows, times, "BTC", "1m", 40, 1) == "unavailable"


def test_ai_abstention_is_zero_exposure_not_a_correct_forecast():
    s = signal()
    s.ai_direction = 0
    rows = [Quote(2, 2, 100, 100.01), Quote(62, 62, 101, 101.01)]
    r = paired_ai([s], {"BTC": rows}, {"BTC": [2, 62]}, 1, 0, 1000, 1000)
    assert r["n"] == 1
    assert r["outcomes"][0]["ai_net_bps"] == 0
    assert r["mean_independent_delta_bps"] < 0


def test_context_filter_rejects_unknown_but_nonconflict_filter_keeps_it():
    s = signal()
    s.context = "unavailable"
    assert eligible(s, "no_conflicts")
    assert not eligible(s, "context_aligned")


def test_bar_proxy_uses_next_open_and_ignores_future_high_low_close():
    s = signal(at=20, ready=25)
    s.spread_bps = 2
    bars = {"BTC": {60: {"o": "100", "n": 10, "h": 999, "l": 1, "c": 999},
                    120: {"o": "101", "n": 10, "h": 999, "l": 1, "c": 999}}}
    a = bar_outcome(s, [], [], 1, 1000, bars=bars, funding={"BTC": []})
    assert a["entry_at"] == 60 and a["exit_at"] == 120
    bars["BTC"][60].update(h=100000, l=0.001, c=0.01)
    b = bar_outcome(s, [], [], 1, 1000, bars=bars, funding={"BTC": []})
    assert a["net_bps"] == b["net_bps"]


def test_bar_entry_cannot_resurrect_an_expired_minute_signal():
    s = signal(at=0, ready=20)
    s.spread_bps = 2
    r = bar_outcome(s, [], [], 1, 1000, bars={}, funding={})
    assert r["status"] == "expired_before_minute_fill"


def test_bar_proxy_accounts_for_published_funding_at_exit():
    s = signal(at=20, ready=25)
    s.spread_bps = 2
    bars = {"BTC": {60: {"o": "100", "n": 10}, 120: {"o": "101", "n": 10}}}
    a = bar_outcome(s, [], [], 1, 1000, bars=bars, funding={"BTC": []})
    b = bar_outcome(s, [], [], 1, 1000, bars=bars,
                    funding={"BTC": [{"time": 120000, "fundingRate": "0.001"}]})
    assert b["funding_bps"] > 10
    assert a["net_bps"] - b["net_bps"] == pytest.approx(b["funding_bps"])


def test_hybrid_requires_observed_entry_and_rounds_exit_to_next_minute():
    s = signal(at=20, ready=25)
    s.spread_bps = 2
    bars = {"BTC": {120: {"o": "101", "n": 10}}}
    missing = bar_outcome(s, [], [], 1, 1000, bars=bars, funding={}, entry_quotes=True)
    assert missing["status"] == "missing_entry_quote"
    q = Quote(28, 28, 99.99, 100.01)
    result = bar_outcome(s, [q], [28], 1, 1000, bars=bars, funding={"BTC": []}, entry_quotes=True)
    assert result["entry_at"] == 28 and result["exit_at"] == 120
    assert result["entry_price"] == pytest.approx(100.01 * 1.0001)


def test_random_control_preserves_selection_count_by_time_block_and_window():
    rows = [dict(available_at=0, window="1m", ai_direction=1, ai_net_bps=-10, native_net_bps=-10),
            dict(available_at=1, window="1m", ai_direction=0, ai_net_bps=0, native_net_bps=-10),
            dict(available_at=0, window="5m", ai_direction=0, ai_net_bps=0, native_net_bps=999),
            dict(available_at=900, window="1m", ai_direction=0, ai_net_bps=0, native_net_bps=999)]
    result = random_thinning(rows, repeats=100)
    assert result["n_selected"] == 1
    assert result["random_range95"] == [-10, -10]
    assert result["random_fraction_at_least_ai"] == 1

"""Exchange time running ahead of a slightly slow local clock is still live data.

A PC one second behind NTP sees every fresh frame with a negative age. Treating
that as stale dropped every L2 book, so no attention probe was ever confirmed,
symbols rotated every ~90s and no 5m window ever reached quality "ok".
"""
import json
import time

import pytest

from extensions.scalper.history.facts import Facts
from hyperdata_terminal.data_layer.orderflow_engine import OrderFlowEngine
from hyperdata_terminal.utils.helpers import CLOCK_SKEW_TOLERANCE_SECONDS, exchange_time_fresh


def raw(message, at, sequence=1, epoch="book1"):
    return dict(kind="raw", received_at=at, sequence=sequence, run="test", epoch=epoch,
                payload={"text": json.dumps(message)})


def book(stamp_seconds):
    return {"channel": "l2Book", "data": {"coin": "SMALL", "time": int(stamp_seconds * 1000), "levels": [
        [{"px": "0.99", "sz": "100"}, {"px": "0.98", "sz": "1"}],
        [{"px": "1.01", "sz": "1"}, {"px": "1.02", "sz": "1"}]]}}


def test_exchange_time_fresh_bounds():
    assert exchange_time_fresh(100, 100, 15)
    assert exchange_time_fresh(100, 101.1, 15)  # local clock 1.1s behind
    assert exchange_time_fresh(100, 100 + CLOCK_SKEW_TOLERANCE_SECONDS, 15)
    assert not exchange_time_fresh(100, 100 + CLOCK_SKEW_TOLERANCE_SECONDS + 0.1, 15)
    assert exchange_time_fresh(100, 85, 15)
    assert not exchange_time_fresh(100, 84.9, 15)


@pytest.mark.parametrize("ahead", [0.2, 0.9, 1.1])
def test_facts_accept_book_slightly_ahead_of_local_clock(ahead):
    facts = Facts(version=2)
    facts.apply(raw(book(100 + ahead), 100))
    assert facts.card("SMALL", 100)["status"] == "observing"


def test_facts_reject_book_far_in_the_future():
    facts = Facts(version=2)
    facts.apply(raw(book(100 + CLOCK_SKEW_TOLERANCE_SECONDS + 1), 100))
    assert facts.card("SMALL", 100)["status"] == "waiting_for_fresh_book"


def test_facts_accept_trade_slightly_ahead_of_local_clock():
    facts = Facts(version=2)
    facts.apply(raw(book(100), 100))
    trade = {"coin": "SMALL", "time": 100_900, "px": "1", "sz": "2", "tid": 1, "side": "B"}
    facts.apply(raw({"channel": "trades", "data": [trade]}, 100, 2, "trades1"))
    assert facts.card("SMALL", 100)["observed_turnover_60s_usd"] == 2


@pytest.mark.asyncio
async def test_external_tape_accepts_trade_slightly_ahead_of_local_clock():
    engine = OrderFlowEngine([], include_binance=False)
    engine.selective, engine.pulse_external = True, True
    await engine.start()
    try:
        await engine.set_attention_symbols(["SMALL"])
        row = dict(venue="hyperliquid", native_market_id="SMALL", delivery_kind="live", side="buy",
                   trade_id="1", price="2", base_amount="3", quote_amount="6",
                   event_time_ms=int((time.time() + 1.1) * 1000), source_epoch="one")
        assert engine.accept_execution(row)
        far = dict(row, trade_id="2",
                   event_time_ms=int((time.time() + CLOCK_SKEW_TOLERANCE_SECONDS + 2) * 1000))
        assert not engine.accept_execution(far)
    finally:
        await engine.stop()

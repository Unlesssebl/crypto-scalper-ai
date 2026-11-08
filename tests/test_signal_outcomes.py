"""Five-minute outcome of each sent card and the per-asset summary."""

import time

import pytest

from extensions.scalper.chart_marks import chart_marks
from extensions.scalper.signal_outcomes import HORIZON, SETTLE, score
from tests.test_telegram_product import observe
from tests.test_telegram_product import service as service

AT = 1_000_020.0  # 20s into the bar that opened at 1_000_000


def bars(*rows, start=1_000_000):
    """rows: (high, low, close) per minute starting with the signal's own bar."""
    return [dict(timestamp=start + i * 60, open=c, high=h, low=lo, close=c) for i, (h, lo, c) in enumerate(rows)]


def test_score_skips_the_signal_bar_and_reads_the_bar_holding_plus_five_minutes():
    candles = bars((200, 1, 100), (101, 99.5, 100.5), (102, 100, 101), (101.5, 100.2, 101), (101, 100, 100.8),
                   (101.2, 100.1, 101.0), (110, 50, 105))
    result = score(AT, 100, 1, candles)
    # +5m = 1_000_320 → the bar opened at 1_000_300 (index 5) closes the window; the signal bar's 200/1 is ignored.
    assert result["close"] == 101.0 and result["high"] == 102 and result["low"] == 99.5
    assert result["move_pct"] == pytest.approx(1.0)
    assert result["favour_pct"] == pytest.approx(2.0) and result["against_pct"] == pytest.approx(0.5)
    sell = score(AT, 100, -1, candles)
    assert sell["favour_pct"] == pytest.approx(0.5) and sell["against_pct"] == pytest.approx(2.0)
    assert score(AT, 100, 0, candles)["favour_pct"] is None  # no side, no "right"/"wrong"


def test_score_needs_the_closing_bar():
    assert score(AT, 100, 1, bars((101, 99, 100), (101, 99, 100))) is None
    assert score(AT, 0, 1, bars(*[(101, 99, 100)] * 7)) is None


class Hub:
    def __init__(self, candles):
        self.candles, self.calls = candles, []

    async def get_candles(self, symbol, timeframe, bars):
        self.calls.append((symbol, timeframe))
        return self.candles


@pytest.mark.asyncio
async def test_sent_card_is_scored_once_and_summarised_for_the_chart(service):
    now = time.time()
    observe(service, now)  # BTC (Hyperliquid) ▲ card at price 10
    sent_at = service.journal.db.execute("SELECT at FROM telegram_ux_events WHERE status='queued'").fetchone()[0]
    start = int(sent_at // 60 * 60)
    candles = bars((10.5, 9.5, 10), (10.3, 9.9, 10.2), (10.4, 10.0, 10.3), (10.4, 10.1, 10.2), (10.3, 10.0, 10.1),
                   (10.2, 10.0, 10.1), (10.2, 10.0, 10.1), start=start)
    outcomes = service.ux.outcomes
    later = sent_at + HORIZON + SETTLE + 1
    hub = Hub(candles)
    assert await outcomes.once(hub, later) == 1
    assert await outcomes.once(hub, later) == 0  # never twice
    assert hub.calls == [("BTC", "1m")]
    row = service.journal.db.execute("SELECT side, status, move_pct FROM signal_outcomes").fetchone()
    assert row["side"] == 1 and row["status"] == "measured" and row["move_pct"] == pytest.approx(1.0)
    summary = chart_marks(service, "BTCUSDT", "BINANCE-FUT", 60, later)["summary"]
    assert summary["sent"] == 1 and summary["sided"] == 1 and summary["right"] == 1 and summary["pending"] == 0
    assert summary["favour_pct"] == pytest.approx(4.0) and summary["against_pct"] == pytest.approx(1.0)


@pytest.mark.asyncio
async def test_unavailable_candles_are_retried_not_recorded(service):
    now = time.time()
    observe(service, now)

    class Down:
        async def get_candles(self, *args, **kwargs):
            raise OSError("native API down")

    assert await service.ux.outcomes.once(Down(), now + HORIZON + SETTLE + 5) == 0
    assert service.ux.outcomes.error == "OSError"
    assert service.journal.db.execute("SELECT COUNT(*) FROM signal_outcomes").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_a_refused_instrument_does_not_block_the_queue(service):
    now = time.time()
    observe(service, now)

    class Refused(Exception):
        status = 400

    class Hub:
        async def get_candles(self, *args, **kwargs):
            raise Refused("bars must be between 2 and 500")

    assert await service.ux.outcomes.once(Hub(), now + HORIZON + SETTLE + 5) == 1
    assert service.journal.db.execute("SELECT status FROM signal_outcomes").fetchone()[0] == "missing"

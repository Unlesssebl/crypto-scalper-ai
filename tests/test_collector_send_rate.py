"""Outgoing frames on a Binance socket stay under the venue's 10 messages a second."""
import asyncio
import time

import pytest

from hyperdata_terminal.market_pulse.collectors.base import StreamCollector
from hyperdata_terminal.market_pulse.collectors.binance import BinanceCollector


class FakeSocket:
    def __init__(self):
        self.sent = []

    async def send_json(self, message):
        self.sent.append((time.monotonic(), message))


@pytest.mark.asyncio
async def test_binance_frames_are_spaced_even_from_concurrent_senders():
    collector = BinanceCollector.__new__(BinanceCollector)  # spacing needs no config or sockets
    collector.send_interval_s = 0.1  # Windows timers tick in ~15ms: keep the check far above that
    ws = FakeSocket()
    await asyncio.gather(*(collector._send_json(ws, {"id": i}) for i in range(6)))
    stamps = [t for t, _ in ws.sent]
    assert len(stamps) == 6
    assert all(b - a >= 0.08 for a, b in zip(stamps, stamps[1:]))
    assert BinanceCollector.send_interval_s == 0.25 and StreamCollector.send_interval_s == 0.0


def test_binance_subscribes_every_detail_market_in_one_frame():
    from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig

    markets = tuple(f"BINANCE:C{i}USDT" for i in range(20))
    collector = BinanceCollector.__new__(BinanceCollector)
    collector.config = CollectorConfig("binance", markets, "unused", "http://unused")
    collector._request_id, collector._requests = 0, {}
    frames = collector.subscriptions()
    assert len(frames) == 1 and len(frames[0]["params"]) == 20
    assert collector._requests[frames[0]["id"]] == markets  # one ack confirms all twenty


def test_plan_contract_allows_twenty_books_and_thirty_overview_symbols():
    import time as clock

    from hyperdata_terminal.market_pulse.control import validate_plan

    now = int(clock.time() * 1000)
    selected = [f"S{i}" for i in range(30)]

    def plan(detail):
        return dict(schema_version="attention.v1", selected=selected, detail=detail, controls=[],
                    generated_at_ms=now, expires_at_ms=now + 60_000)

    validate_plan(plan(selected[:20]), now)
    with pytest.raises(ValueError, match="detail"):
        validate_plan(plan(selected[:21]), now)

"""Attention discovery, subscription lifecycle, leases and retention acceptance."""
from __future__ import annotations

import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from aiohttp.test_utils import TestClient, TestServer

from extensions.scalper.attention import Attention
from extensions.scalper.journal import Journal
from hyperdata_terminal.market_pulse.api import create_app
from hyperdata_terminal.market_pulse.collectors.base import CollectorConfig
from hyperdata_terminal.market_pulse.collectors.hyperliquid import HyperliquidCollector
from hyperdata_terminal.market_pulse.collectors.specs import default_instruments, hyperliquid_spec, market_for_venue
from hyperdata_terminal.market_pulse.control import desired_symbols, validate_plan
from hyperdata_terminal.market_pulse.gaps import GapTracker
from hyperdata_terminal.market_pulse.ingest import GapControlService, IngestionService
from hyperdata_terminal.market_pulse.ingest_http import create_ingest_app
from hyperdata_terminal.market_pulse.maintenance import prune_raw
from hyperdata_terminal.market_pulse.raw_archive import RawArchive
from hyperdata_terminal.market_pulse.storage import InMemoryMarketPulseStore
from hyperdata_terminal.market_pulse.windows import WindowService


def asset(symbol, price=100, volume=100_000):
    return {"symbol": symbol, "mark_price": price, "volume_24h": volume}


def test_discovery_outside_top20_preempts_after_minimum_residence(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    attention = Attention(journal, slots=2, detail_slots=1)
    universe = [asset("BTC", volume=10**10), asset("ETH", volume=10**9), asset("NEW", volume=10**5)]
    assert "NEW" not in attention.choose(universe, 1000)
    attention.choose([*universe[:2], asset("NEW", 120)], 1031)
    assert "NEW" not in attention.selected  # Minimum residency prevents instant flapping.
    attention.choose([*universe[:2], asset("NEW", 130)], 1061)
    assert "NEW" in attention.selected and "NEW" in attention.plan["detail"]
    assert attention.plan["universe_size"] == 3
    assert journal.db.execute("SELECT COUNT(*) FROM market_overview_samples WHERE symbol='NEW'").fetchone()[0] >= 1
    journal.close()


def test_attention_restart_retains_slot_and_malformed_market_is_excluded(tmp_path):
    journal = Journal(tmp_path / "journal.sqlite")
    attention = Attention(journal, slots=2, detail_slots=1)
    attention.choose([asset("ETH"), asset("SOL"), asset("BAD", float('nan'))], 1000)
    saved = attention.plan["detail"]
    journal.close()
    journal = Journal(tmp_path / "journal.sqlite")
    attention = Attention(journal, slots=2, detail_slots=1)
    attention.choose([asset("SOL"), asset("ETH")], 1010)
    assert attention.plan["detail"] == saved and "BAD" not in attention.selected
    journal.close()


def plan(now):
    return {"schema_version": "attention.v1", "generated_at_ms": now, "expires_at_ms": now + 300_000,
            "selected": ["ETH", "SOL"], "detail": ["ETH"], "controls": ["BTC", "XRP"]}


@pytest.mark.parametrize("change", [{"detail": ["OTHER"]}, {"detail": ["ETH", "ETH"]},
                                   {"expires_at_ms": 0}, {"generated_at_ms": 10**16},
                                   {"selected": ["../bad"]}, {"schema_version": "unknown"}])
def test_invalid_attention_plan_is_rejected(change):
    with pytest.raises(ValueError):
        validate_plan({**plan(1000), **change}, 1000)


def test_expired_attention_releases_dynamic_but_keeps_controls():
    assert desired_symbols(plan(1000), 2000) == ("BTC", "XRP", "ETH")
    assert desired_symbols(plan(1000), 400_000) == ("BTC", "XRP")
    assert market_for_venue("okx", "ETH") == "ETH-USDT-SWAP"
    assert hyperliquid_spec("kPEPE").native_market_id == "kPEPE"


async def test_control_auth_persistence_monotonicity_and_missing_coverage():
    store = InMemoryMarketPulseStore()
    gaps = GapTracker(store)
    ingest = IngestionService(store, WindowService(store), gaps)
    payload = plan(int(time.time() * 1000))
    async with TestClient(TestServer(create_ingest_app(store, ingest, GapControlService(store, gaps),
                                                      token="secret"))) as client:
        assert (await client.post('/ingest/v1/attention', json=payload)).status == 401
        assert (await client.post('/ingest/v1/attention', json=payload,
                                  headers={"Authorization": "Bearer secret"})).status == 200
    older = {**payload, "detail": ["SOL"], "generated_at_ms": payload["generated_at_ms"] - 1}
    assert not await store.set_control("attention", older, older["generated_at_ms"])
    async with TestClient(TestServer(create_app(store))) as client:
        assert (await (await client.get('/attention')).json())["plan"]["detail"] == ["ETH"]
        health = await client.get('/health')
        assert health.status == 503 and len((await health.json())["missing_channels"]) == 6


async def test_subscriptions_change_in_place_and_new_channel_waits_for_own_ack(tmp_path):
    archive = RawArchive(tmp_path, "hyperliquid")
    gaps = SimpleNamespace(connected=AsyncMock(), released=AsyncMock(), heartbeat=AsyncMock(), unavailable=AsyncMock())
    collector = HyperliquidCollector(CollectorConfig("hyperliquid", ("BTC", "XRP"), str(tmp_path), ""),
                                     archive, SimpleNamespace(append_batch=AsyncMock()), gaps)
    collector.dynamic = True
    collector._epoch = "same-socket"
    ws = SimpleNamespace(closed=False, send_json=AsyncMock())
    collector._ws = ws
    collector.confirmed = {"BTC", "XRP"}
    collector.specs["ETH"] = hyperliquid_spec("ETH")
    await collector.change_markets(("BTC", "ETH"))
    assert collector.confirmed == {"BTC"} and "ETH" in collector.pending_ack
    assert [c.args[0]['method'] for c in ws.send_json.await_args_list] == ['unsubscribe', 'subscribe']
    gaps.connected.assert_not_awaited()  # BTC retained without a new epoch or artificial gap.
    await collector._report_heartbeat(1000)
    assert gaps.heartbeat.await_args.args[1] == "BTC"
    ack = {"channel": "subscriptionResponse", "data": {"method": "subscribe",
            "subscription": {"type": "trades", "coin": "ETH"}}}
    await collector._handle_payload(json.dumps(ack), "same-socket")
    assert collector.confirmed == {"BTC", "ETH"}
    assert gaps.connected.await_args.args[:4] == ("hyperliquid", "ETH", "trades", "same-socket")
    archive.close()


async def test_unacknowledged_channel_cannot_be_kept_healthy_by_other_trades(tmp_path):
    archive = RawArchive(tmp_path, "hyperliquid")
    gaps = SimpleNamespace(heartbeat=AsyncMock(), unavailable=AsyncMock())
    collector = HyperliquidCollector(CollectorConfig("hyperliquid", ("BTC", "ETH"), str(tmp_path), ""),
                                     archive, SimpleNamespace(), gaps)
    collector.dynamic = True
    collector.confirmed = {"BTC"}
    collector.pending_ack = {"ETH": 1}
    await collector.expire_pending()
    assert not collector.pending_ack
    assert gaps.unavailable.await_args.args[1] == "ETH"
    await collector._report_heartbeat(1000)
    assert len(gaps.heartbeat.await_args_list) == 1
    archive.close()


async def test_released_interval_is_not_zero_or_failure_and_closes_on_reselection():
    store = InMemoryMarketPulseStore()
    tracker = GapTracker(store)
    await store.register_instruments(default_instruments())
    await tracker.on_connected("hyperliquid", "BTC", "trades", "e1", 0)
    await tracker.on_released("hyperliquid", "BTC", "trades", "e1", 60_000)
    await tracker.touch_events("hyperliquid", "BTC", "trades", "e1", now_ms=80_000,
                               event_time_ms=50_000, receive_time_ms=50_000)
    assert (await store.list_coverage())[0].state == "outside_attention"
    await tracker.on_connected("hyperliquid", "BTC", "trades", "e2", 180_000)
    gaps = await store.list_gaps()
    assert len(gaps) == 1 and gaps[0].state == "outside_attention" and gaps[0].end_ms == 180_000
    window = await WindowService(store).rebuild("hyperliquid", "BTC", "trades", "execution", 60_000,
                                                now_ms=200_000)
    assert window.quality == "outside_attention" and window.buy_quote is None


async def test_live_quiet_minute_is_materialized_with_real_zero():
    store = InMemoryMarketPulseStore()
    await store.register_instruments(default_instruments())
    tracker = GapTracker(store)
    await tracker.on_connected("hyperliquid", "BTC", "trades", "e1", 0)
    await tracker.on_heartbeat("hyperliquid", "BTC", "trades", 65_001)
    await WindowService(store).close_due(now_ms=65_001)
    window = await store.get_window("hyperliquid", "BTC", "trades", "execution", 0)
    assert window.quality == "observed_no_detected_gap" and window.execution_count == 0
    assert str(window.buy_quote) == '0'


def test_retention_keeps_open_recovery_and_audit_inputs_and_reports_pressure(tmp_path):
    archive = RawArchive(tmp_path, 'hyperliquid')
    archive.append({"test": 1}, receive_time_ms=1000, source_epoch="e")
    archive.close()
    path = next((tmp_path / 'hyperliquid').glob('*.jsonl'))
    path.with_suffix('.jsonl.pin').touch()
    result = prune_raw(tmp_path, [], 10**15, max_bytes=1, max_age_ms=1, min_free_bytes=0)
    assert result['status'] == 'blocked' and path.exists()
    path.with_suffix('.jsonl.pin').unlink()
    gap = SimpleNamespace(venue='hyperliquid', state='gap_detected', start_ms=0, end_ms=None)
    assert prune_raw(tmp_path, [gap], 10**15, max_bytes=1, max_age_ms=1, min_free_bytes=0)['status'] == 'blocked'
    result = prune_raw(tmp_path, [], 10**15, max_bytes=1, max_age_ms=1, min_free_bytes=0)
    assert result['status'] == 'ok' and not path.exists()


def test_shared_hl_raw_cannot_relabel_trade_and_integrity_audit_detects_legacy_damage(tmp_path):
    from dataclasses import replace

    from hyperdata_terminal.market_pulse.collectors.hyperliquid import normalize_hyperliquid_trades
    from hyperdata_terminal.market_pulse.integrity import audit_rows

    archive = RawArchive(tmp_path, 'hyperliquid')
    raw = {"channel": "trades", "data": [{"coin": "BTC", "side": "B", "px": "75000",
            "sz": "1", "time": 1000, "tid": 1}]}
    reference = archive.append(raw, receive_time_ms=1000, source_epoch='e')
    btc, xrp = hyperliquid_spec('BTC'), hyperliquid_spec('XRP')
    kwargs = {"receive_time_ms": 1000, "source_epoch": 'e', "raw_ref": str(reference)}
    valid = normalize_hyperliquid_trades(raw, btc, **kwargs)[0]
    assert normalize_hyperliquid_trades(raw, xrp, **kwargs) == []
    damaged = replace(valid, native_market_id='XRP', instrument_spec_version=xrp.spec_version)
    archive.close()
    audit = audit_rows([{"event_seq": 1, "payload": valid.to_payload()},
                       {"event_seq": 2, "payload": damaged.to_payload()}],
                      {(s.native_market_id, s.spec_version): s for s in (btc, xrp)}, tmp_path)
    assert audit['verified'] == 1 and audit['suspect_sequences'] == [2]


def test_raw_reference_stays_on_original_segment_after_time_rotation(tmp_path):
    archive = RawArchive(tmp_path, 'hyperliquid')
    received = archive._created_at_ms + 300_001
    reference = archive.append({"test": 42}, receive_time_ms=received, source_epoch='e')
    assert reference.segment != archive._path.name
    with (archive.root / reference.segment).open('rb') as handle:
        handle.seek(reference.offset)
        assert json.loads(handle.readline())['payload']['payload']['test'] == 42
    archive.close()


def test_restore_rejects_changed_backup_before_any_database_command(tmp_path, monkeypatch):
    from scripts import pulse_ops

    path = tmp_path / 'market-pulse-test.dump'
    path.write_bytes(b'changed')
    path.with_suffix('.json').write_text(json.dumps({'sha256': 'wrong'}))
    calls = []
    monkeypatch.setattr(pulse_ops, 'database', lambda *a, **k: calls.append(a))
    with pytest.raises(ValueError, match='checksum'):
        pulse_ops.verify(path)
    assert not calls


def test_failed_restore_only_drops_its_own_generated_database(tmp_path, monkeypatch):
    import hashlib
    import subprocess

    from scripts import pulse_ops

    path = tmp_path / 'market-pulse-test.dump'
    path.write_bytes(b'backup')
    path.with_suffix('.json').write_text(json.dumps({'sha256': hashlib.sha256(b'backup').hexdigest()}))
    calls = []

    def database(command, **kwargs):
        calls.append(command)
        if command[0] == 'pg_restore':
            raise subprocess.CalledProcessError(1, command)
        return b''

    monkeypatch.setattr(pulse_ops, 'database', database)
    with pytest.raises(subprocess.CalledProcessError):
        pulse_ops.verify(path)
    target = calls[0][-1]
    assert target.startswith('pulse_restore_') and calls[-1] == ['dropdb', '-U', 'marketpulse', target]
    assert 'marketpulse' != target


async def test_one_okx_subscription_rejection_preserves_other_confirmed_market(tmp_path):
    from hyperdata_terminal.market_pulse.collectors.okx import OkxCollector

    archive = RawArchive(tmp_path, 'okx')
    gaps = SimpleNamespace(unavailable=AsyncMock())
    collector = OkxCollector(CollectorConfig('okx', ('BTC', 'XRP'), str(tmp_path), ''),
                            archive, SimpleNamespace(), gaps)
    collector.dynamic = True
    collector.confirmed = {'BTC-USDT-SWAP'}
    collector.pending_ack = {'XRP-USDT-SWAP': 99999}
    await collector._handle_payload(json.dumps({'event': 'error', 'arg': {'instId': 'XRP-USDT-SWAP'}}), 'e')
    assert collector.confirmed == {'BTC-USDT-SWAP'} and not collector.pending_ack
    assert gaps.unavailable.await_args.args[1] == 'XRP-USDT-SWAP'
    archive.close()


@pytest.mark.parametrize('status,body,expected,state', [
    (200, {'ok': True}, True, 'edited'),
    (400, {'ok': False, 'description': 'Bad Request: message is not modified'}, True, 'unchanged'),
    (400, {'ok': False, 'description': 'message cannot be edited'}, False, 'failed'),
    (429, {'ok': False, 'parameters': {'retry_after': 30}}, False, 'failed'),
    (503, {'ok': False}, False, 'outcome_unknown'),
])
async def test_telegram_edit_distinguishes_noop_rejection_and_unknown(status, body, expected, state):
    from unittest.mock import MagicMock

    from hyperdata_terminal.data_layer.alerts import AlertManager

    manager = AlertManager()
    manager.telegram_token, manager.telegram_chat_id = 'secret', '42'
    manager._session = MagicMock()
    context = MagicMock()
    response = context.__aenter__.return_value
    response.status, response.json.return_value = status, body
    manager._session.post.return_value = context
    assert await manager.edit_telegram(123, 'facts') is expected
    assert manager.last_telegram_result['state'] == state
    assert manager._session.post.call_count == 1


@pytest.mark.parametrize('failure', ['timeout', 'missing_id'])
async def test_ambiguous_send_never_creates_second_message(failure):
    from unittest.mock import MagicMock

    from hyperdata_terminal.data_layer.alerts import AlertManager

    manager = AlertManager()
    manager.telegram_token, manager.telegram_chat_id = 'secret', '42'
    manager._session = MagicMock()
    if failure == 'timeout':
        manager._session.post.side_effect = TimeoutError()
    else:
        context = MagicMock()
        response = context.__aenter__.return_value
        response.status, response.json.return_value = 200, {'ok': True, 'result': {}}
        manager._session.post.return_value = context
    assert not await manager.send_telegram('facts', return_message=True)
    assert manager.last_telegram_result['state'] == 'outcome_unknown'
    assert manager._session.post.call_count == 1

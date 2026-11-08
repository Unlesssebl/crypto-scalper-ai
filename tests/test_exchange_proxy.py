"""The shared exchange proxy handles REST/WS without capturing local ingest."""

import json

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from hyperdata_terminal.market_pulse.transport import HttpIngestClient
from hyperdata_terminal.network import exchange_session


@pytest.mark.asyncio
async def test_http_proxy_carries_rest_and_websocket_with_separate_credentials(monkeypatch):
    seen = []

    async def proxy(request):
        seen.append((request.raw_path, request.headers.get("Proxy-Authorization")))
        if request.headers.get("Upgrade", "").lower() == "websocket":
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            await ws.send_json(dict(exchange="test", stream="live"))
            async for message in ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    await ws.send_str(message.data)
            return ws
        return web.json_response(dict(exchange="test", value=1))

    app = web.Application()
    app.router.add_route("*", "/{path:.*}", proxy)
    async with TestServer(app) as server:
        address = str(server.make_url("/")).replace("http://", "http://user:p%40ss@")
        monkeypatch.setenv("HYPERDATA_EXCHANGE_PROXY", address)
        async with exchange_session() as session:
            async with session.get("http://exchange.invalid/market") as response:
                assert (await response.json())["value"] == 1
            async with session.ws_connect("ws://exchange.invalid/stream") as ws:
                assert (await ws.receive_json())["stream"] == "live"
            assert session._default_proxy.user is None
            assert session._default_proxy_auth.password == "p@ss"
    assert len(seen) == 2
    assert all(path.startswith("http://exchange.invalid/") or path.startswith("ws://exchange.invalid/")
               for path, _ in seen)
    assert all(auth == aiohttp.BasicAuth("user", "p@ss").encode() for _, auth in seen)


@pytest.mark.asyncio
async def test_exchange_proxy_does_not_route_or_leak_into_local_ingest(monkeypatch):
    monkeypatch.setenv("HYPERDATA_EXCHANGE_PROXY", "http://user:password@127.0.0.1:1")
    received = []

    async def ingest(request):
        received.append(dict(request.headers))
        return web.json_response(dict(status="ok", received=await request.json()))

    app = web.Application()
    app.router.add_post("/ingest/v1/gaps", ingest)
    async with TestServer(app) as server:
        client = HttpIngestClient(str(server.make_url("/")), token="local-secret")
        try:
            assert (await client.report(dict(action="test")))["status"] == "ok"
        finally:
            await client.close()
    assert received[0]["Authorization"] == "Bearer local-secret"
    assert "Proxy-Authorization" not in received[0]


@pytest.mark.asyncio
async def test_empty_proxy_keeps_direct_exchange_connections(monkeypatch):
    monkeypatch.setenv("HYPERDATA_EXCHANGE_PROXY", "")
    async with exchange_session() as session:
        assert session._default_proxy is None
        assert not session.trust_env


@pytest.mark.parametrize("value", ["socks5://user:secret@localhost:1080", "http://user:secret@host/path",
                                  "http://user:secret@host?token=hidden", "host:8080"])
def test_invalid_proxy_configuration_never_echoes_credentials(monkeypatch, value):
    monkeypatch.setenv("HYPERDATA_EXCHANGE_PROXY", value)
    with pytest.raises(ValueError) as exc:
        exchange_session()
    assert "secret" not in str(exc.value) and "hidden" not in str(exc.value)


def test_collector_launcher_has_no_fixed_instruments_and_passes_proxy_privately(monkeypatch, tmp_path):
    from scripts import project

    monkeypatch.setattr(project, "RUNTIME", tmp_path)
    monkeypatch.setattr(project, "dotenv_values", lambda path: dict(PULSE_INGEST_TOKEN="local-secret"))
    monkeypatch.setenv("HYPERDATA_EXCHANGE_PROXY", "http://user:secret@proxy.invalid:8000")
    calls = []

    def start(command, **kwargs):
        calls.append((command, kwargs))
        return object()

    monkeypatch.setattr(project.subprocess, "Popen", start)
    project.start_binance()
    command, options = calls[0]
    assert "--markets" not in command
    assert options["env"]["PULSE_MARKETS"] == ""
    assert options["env"]["PULSE_DYNAMIC_ATTENTION"] == "1"
    assert options["env"]["HYPERDATA_EXCHANGE_PROXY"].startswith("http://user:secret@")
    assert "secret" not in json.dumps(command)

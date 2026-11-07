"""Explicit, local-only chart navigation; never sends trading commands."""

import asyncio
import json
import os
import re
import uuid
from urllib.parse import urlsplit

import aiohttp


class TigerBridge:
    def __init__(self):
        self.url = os.getenv("SCALPER_TIGER_WS_URL", "ws://127.0.0.1:7819/")
        self.group = os.getenv("SCALPER_TIGER_LINK_GROUP", "").upper()
        self.exchange = os.getenv("SCALPER_TIGER_EXCHANGE", "BINANCE-FUT")
        self.market = os.getenv("SCALPER_TIGER_MARKET", "FUTURES")
        self.config_error = None
        try:
            self.aliases = json.loads(os.getenv("SCALPER_TIGER_SYMBOLS", "{}"))
            if not isinstance(self.aliases, dict):
                raise ValueError("Expected a symbol map")
        except (ValueError, TypeError):
            self.aliases = {}
            self.config_error = "Invalid Tiger symbol map"

    def command(self, symbol):
        if self.config_error:
            raise ValueError(self.config_error)
        address = urlsplit(self.url)
        if (
            address.scheme != "ws"
            or address.hostname != "127.0.0.1"
            or address.username
            or address.password
            or address.query
            or address.fragment
        ):
            raise ValueError("Tiger API must use loopback")
        if not re.fullmatch(r"[A-Z]|[0-9]|1[0-8]", self.group):
            raise ValueError("Tiger link group is not configured")
        mapped = self.aliases.get(symbol)
        if mapped is None:
            # Only the ordinary Binance USDT-M name is inferred. Never guess 1000/k aliases.
            if (
                self.exchange != "BINANCE-FUT"
                or self.market != "FUTURES"
                or not re.fullmatch(r"[A-Z][A-Z0-9]{1,19}", symbol)
            ):
                raise ValueError("An explicit Tiger symbol mapping is required")
            mapped = symbol + "USDT"
        if not isinstance(mapped, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", mapped):
            raise ValueError("Invalid Tiger symbol mapping")
        return dict(
            type="SETLINKSYMBOL",
            requestId=uuid.uuid4().hex,
            e=self.exchange,
            m=self.market,
            symbol=mapped,
            linkGroup=self.group,
        )

    async def open(self, session, symbol):
        try:
            command = self.command(symbol)
        except (ValueError, TypeError):
            return "Для этого инструмента переход в TigerTrade не настроен."
        try:
            async with asyncio.timeout(5):
                timeout = aiohttp.ClientWSTimeout(ws_close=1) if hasattr(aiohttp, "ClientWSTimeout") else 3
                async with session.ws_connect(self.url, timeout=timeout) as socket:
                    await socket.send_json(command)
                    async for message in socket:
                        if message.type != aiohttp.WSMsgType.TEXT:
                            break
                        if message.data == "pong":
                            continue
                        body = json.loads(message.data)
                        if body.get("requestId") != command["requestId"]:
                            continue
                        response = body.get("response", {})
                        if response.get("type") == "successfully":
                            return "TigerTrade принял переключение графика на ПК."
                        return "TigerTrade отклонил переход: проверь контракт и группу линковки."
        except (aiohttp.ClientError, OSError, TimeoutError, ValueError):
            return "TigerTrade на ПК недоступен. Переход не отложен и не будет выполнен позже."
        return "TigerTrade не подтвердил переключение. Автоматического повтора нет."

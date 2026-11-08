"""Explicit proxy configuration for exchange-only sessions.

Local API/ingest, Telegram and model clients do not use this factory.
Credentials are split from the URL before it enters transport diagnostics.
"""

import os

import aiohttp
from yarl import URL


def exchange_session(**kwargs):
    value = os.getenv("HYPERDATA_EXCHANGE_PROXY", "").strip()
    if not value:
        return aiohttp.ClientSession(**kwargs)
    try:
        proxy = URL(value)
        valid = (proxy.scheme in {"http", "https"} and proxy.host and proxy.port
                 and proxy.path in {"", "/"} and not proxy.query_string and not proxy.fragment)
    except (ValueError, TypeError):
        valid = False
    if not valid:
        raise ValueError("HYPERDATA_EXCHANGE_PROXY must be an HTTP(S) proxy URL without path/query/fragment")
    auth = aiohttp.BasicAuth(proxy.user, proxy.password or "") if proxy.user is not None else None
    kwargs.update(proxy=proxy.with_user(None), proxy_auth=auth, trust_env=False)
    return aiohttp.ClientSession(**kwargs)

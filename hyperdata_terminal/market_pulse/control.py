"""Versioned attention plans; no symbol is silently assumed to be observed."""
from __future__ import annotations

import re

VERSION = "attention.v1"


def validate_plan(payload, now_ms):
    if not isinstance(payload, dict) or payload.get("schema_version") != VERSION:
        raise ValueError("unsupported attention schema")
    for field, maximum in (("selected", 20), ("detail", 8), ("controls", 2)):
        items = payload.get(field)
        if (not isinstance(items, list) or len(items) > maximum
                or any(not isinstance(s, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,64}", s) for s in items)
                or len(set(items)) != len(items)):
            raise ValueError("invalid attention " + field)
    if not set(payload["detail"]) <= set(payload["selected"]):
        raise ValueError("detail must belong to selected")
    start, end = payload.get("generated_at_ms"), payload.get("expires_at_ms")
    if (type(start) is not int or type(end) is not int or start > now_ms + 5000
            or not now_ms < end <= now_ms + 600_000 or end <= start):
        raise ValueError("invalid or expired attention lease")
    return payload


def desired_symbols(plan, now_ms, fallback=("BTC", "XRP")):
    if not plan:
        return tuple(fallback)
    controls = plan.get("controls", list(fallback))
    detail = plan.get("detail", []) if plan.get("expires_at_ms", 0) > now_ms else []
    return tuple(dict.fromkeys([*controls, *detail]))

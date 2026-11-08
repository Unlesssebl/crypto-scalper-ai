"""Versioned attention plans; no symbol is silently assumed to be observed."""

from __future__ import annotations

import math
import re

VERSION = "attention.v1"
MAX_SELECTED = 30
MAX_DETAIL = 20


def validate_plan(payload, now_ms):
    if not isinstance(payload, dict) or payload.get("schema_version") != VERSION:
        raise ValueError("unsupported attention schema")
    # 20 order books + 10 overview-only symbols; native books and Pulse tapes follow `detail`.
    for field, maximum in (("selected", MAX_SELECTED), ("detail", MAX_DETAIL), ("controls", 2)):
        items = payload.get(field)
        if (
            not isinstance(items, list)
            or len(items) > maximum
            or any(not isinstance(s, str) or not re.fullmatch(r"[\w.:-]{1,64}", s) for s in items)
            or len(set(items)) != len(items)
        ):
            raise ValueError("invalid attention " + field)
    if not set(payload["detail"]) <= set(payload["selected"]):
        raise ValueError("detail must belong to selected")
    if payload["controls"]:
        raise ValueError("fixed control subscriptions are disabled; use adaptive detail assignments")
    start, end = payload.get("generated_at_ms"), payload.get("expires_at_ms")
    if (
        type(start) is not int
        or type(end) is not int
        or start > now_ms + 5000
        or not now_ms < end <= now_ms + 600_000
        or end <= start
    ):
        raise ValueError("invalid or expired attention lease")
    if payload.get("policy_version") in (4, 5, 6):
        active = payload.get("active")
        if (
            not isinstance(active, list)
            or any(not isinstance(r, dict) for r in active)
            or {r.get("symbol") for r in active} != set(payload["detail"])
            or len(active) != len(payload["detail"])
        ):
            raise ValueError("active assignments must match detail")
        for row in active:
            stamp = row.get("assigned_at")
            if (
                type(stamp) not in (int, float)
                or not math.isfinite(stamp)
                or stamp < 0
                or stamp > now_ms / 1000 + 5
                or row.get("state") not in {"probe", "focus", "cooling"}
            ):
                raise ValueError("invalid active assignment")
    return payload


def desired_symbols(plan, now_ms):
    # Ignore retired controls even in a saved older plan. No lease means no detail feeds.
    if not plan or plan.get("expires_at_ms", 0) <= now_ms:
        return ()
    return tuple(dict.fromkeys(plan.get("detail", [])))

"""Immutable, content-addressed facts; model conclusions never become measurements."""

import hashlib
import json
import math

PACKET_VERSION = 1
RULESET = "context-facts-v1"


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def checksum(value):
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def verify_packet(packet):
    if not isinstance(packet, dict) or packet.get("version") != PACKET_VERSION or packet.get("ruleset") != RULESET:
        raise ValueError("Unsupported fact packet")
    if packet.get("packet_id") != checksum({k: v for k, v in packet.items() if k != "packet_id"}):
        raise ValueError("Fact packet checksum mismatch")
    if len({f["id"] for f in packet["facts"]}) != len(packet["facts"]):
        raise ValueError("Duplicate fact identity")
    if (
        not isinstance(packet["captured_at"], (int, float))
        or not math.isfinite(packet["captured_at"])
        or not isinstance(packet["symbol"], str)
        or not packet["symbol"]
    ):
        raise ValueError("Invalid packet identity/time")
    for fact in packet["facts"]:
        if (
            not isinstance(fact["id"], str)
            or not fact["id"]
            or len(fact["id"]) > 256
            or fact["quality"] not in {"ok", "stale", "unknown"}
            or type(fact["value"]) not in (int, float, str, bool, type(None))
        ):
            raise ValueError("Invalid fact")
        stamp = fact["observed_at"]
        if stamp is not None and (type(stamp) not in (int, float) or not math.isfinite(stamp)):
            raise ValueError("Invalid fact timestamp")
    if packet.get("context_version"):
        if packet["context_version"] != "scalper-context-v2":
            raise ValueError("Unsupported analytical context")
        plan = packet.get("scenarios") or {}
        by_id = {f["id"]: f for f in packet["facts"]}
        if any(ref not in by_id for ref in plan.get("references", [])):
            raise ValueError("Scenario references an absent fact")
        if plan.get("kind") in {"observed_zone", "observed_impulse"}:
            anchor = by_id.get(plan.get("anchor_ref"), {})
            if anchor.get("quality") != "ok" or plan.get("anchor") != anchor.get("value"):
                raise ValueError("Scenario anchor differs from its fact")
    return packet


def build_packet(attention, assignment, now, frames=()):
    """Freeze the available card, explicitly preserving unknown order/tape history."""
    card = assignment["facts"]
    symbol = assignment["symbol"]
    run = (
        "attention-plan.v4"
        if attention.get("policy_version") == 4
        else attention.get("archive", {}).get("run", "unknown")
    )
    episode = checksum(dict(run=run, symbol=symbol, assigned_at=assignment["assigned_at"]))
    facts = []
    healthy = not attention.get("archive", {}).get("error") and not attention.get("subscription_error")
    book_at = card.get("received_at")

    def add(
        key,
        value,
        unit,
        *,
        source="hyperliquid_l2_snapshot",
        at=book_at,
        evidence=(),
        role="book",
        window=None,
        source_time=None,
    ):
        valid = value is not None and (not isinstance(value, (int, float)) or math.isfinite(value))
        quality = (
            "unknown"
            if not valid or not healthy or at is None or at < assignment["assigned_at"]
            else "ok"
            if 0 <= now - at <= (30 if role == "activity" else 15)
            else "stale"
        )
        facts.append(
            dict(
                id=key,
                value=value if valid else None,
                unit=unit,
                quality=quality,
                source=source,
                observed_at=at,
                evidence=list(evidence),
                role=role,
                window=window,
                source_time=at if source_time is None else source_time,
                calculation_version=RULESET,
            )
        )

    book_ref = [card["evidence"]] if card.get("evidence") else []
    add("book.midpoint", card.get("midpoint"), "price", evidence=book_ref)
    add("book.spread_bps", card.get("spread_bps"), "bps", evidence=book_ref)
    add("book.visible_bid_levels", (card.get("observed_levels") or [None, None])[0], "levels", evidence=book_ref)
    add("book.visible_ask_levels", (card.get("observed_levels") or [None, None])[1], "levels", evidence=book_ref)
    add(
        "activity.relative_score",
        assignment.get("score"),
        "ratio",
        source="relative_scout_algorithm",
        at=attention.get("last_scout_at"),
        role="activity",
    )
    add(
        "tape.observed_turnover_60s",
        None if card.get("trade_window_truncated") else card.get("observed_turnover_60s_usd"),
        "usd",
        source="hyperliquid_observed_trades",
        evidence=card.get("trade_evidence", []),
        role="tape",
    )
    # Trade-window completeness cannot be inferred from just the sum of observed prints.
    add("tape.full_window_coverage", None, "boolean", source="coverage", role="coverage")
    if "observed_trade_imbalance" in card:
        for field, unit in (("observed_trade_imbalance", "ratio"), ("observed_trade_count_60s", "trades")):
            add(
                "tape." + field,
                None if card.get("trade_window_truncated") else card.get(field),
                unit,
                source="hyperliquid_observed_trades",
                role="flow",
                at=card.get("last_trade_received_at"),
                evidence=card.get("trade_evidence", []),
                window="observed_60s",
            )
    add("history.before_attention", None, "boolean", source="coverage", role="coverage")
    for zone in card.get("densities", []):
        key = f"density.{zone['side']}.{format(zone['price'], '.17g')}"
        ref = [zone["evidence"]] if zone.get("evidence") else book_ref
        for field, unit in (
            ("price", "price"),
            ("size", "base_units"),
            ("notional_usd", "usd"),
            ("observed_age_seconds", "seconds"),
            ("distance_bps", "bps"),
            ("relative_size", "ratio"),
            ("size_change", "base_units"),
            ("size_to_observed_turnover", "ratio"),
        ):
            value = zone.get(field)
            if (
                field == "observed_age_seconds"
                and value is not None
                and book_at is not None
                and not 0 <= value <= book_at - assignment["assigned_at"]
            ):
                value = None
            add(f"{key}.{field}", value, unit, evidence=ref)
        add(f"{key}.actual_order_age", None, "seconds", source="coverage", role="coverage")
    # Native measurements are included, never the native direction/conclusion.
    for frame in frames:
        if (
            frame["symbol"] != symbol
            or frame["window"] not in {"1m", "5m"}
            or not assignment["assigned_at"] <= frame["captured_at"] <= now
        ):
            continue
        window, metrics = frame["window"], frame["metrics"]
        prefix = f"frame.{window}."
        valid_fields = {}
        for name, unit, role in (
            ("ofi", "ratio", "flow"),
            ("cvd", "usd", "flow"),
            ("trade_count", "trades", "flow"),
            ("first_trade_price", "price", "price_action"),
            ("last_trade_price", "price", "price_action"),
        ):
            item = metrics.get(name, {})
            stamp = item.get("updated_at")
            max_age = (60 if window == "1m" else 300) + 5 if name == "first_trade_price" else 60
            temporal = (
                type(stamp) in (int, float) and math.isfinite(stamp) and 0 <= frame["captured_at"] - stamp <= max_age
            )
            # A window endpoint may be older than the packet; its actual source time is retained.
            value = item.get("value") if item.get("quality") == "ok" and temporal else None
            valid_fields[name] = value is not None
            add(
                prefix + name,
                value,
                unit,
                source="hyperliquid_native_window",
                at=frame["captured_at"],
                role=role,
                window=window,
                source_time=item.get("updated_at"),
                evidence=[f"native:{symbol}:{window}:{frame['captured_at']}:{name}"],
            )
        first, last, flow = (metrics.get(n, {}) for n in ("first_trade_price", "last_trade_price", "ofi"))
        valid = all(valid_fields[n] for n in ("first_trade_price", "last_trade_price", "ofi"))
        valid = valid and all(
            m.get("quality") == "ok" and type(m.get("value")) in (int, float) and math.isfinite(m["value"])
            for m in (first, last, flow)
        )
        valid = valid and first["value"] > 0 and last["value"] > 0
        change = (last["value"] / first["value"] - 1) * 10000 if valid else None
        if change is not None and not math.isfinite(change):
            change = None
            valid = False
        conflict = flow["value"] * change <= 0 if valid and abs(flow["value"]) > 0.15 else None
        add(
            prefix + "observed_price_change_bps",
            change,
            "bps",
            source="native_window_calculation",
            at=frame["captured_at"],
            role="price_action",
            window=window,
            evidence=[prefix + "first_trade_price", prefix + "last_trade_price"],
        )
        add(
            prefix + "flow_price_conflict",
            conflict,
            "boolean",
            source="native_window_calculation",
            at=frame["captured_at"],
            role="price_action",
            window=window,
            evidence=[prefix + "ofi", prefix + "observed_price_change_bps"],
        )
    packet = dict(
        version=PACKET_VERSION,
        ruleset=RULESET,
        episode_id=episode,
        symbol=symbol,
        captured_at=now,
        facts=sorted(facts, key=lambda f: f["id"]),
        coverage=dict(
            book_status=card["status"],
            attention_state=assignment["state"],
            observation_healthy=healthy,
            trade_truncated=bool(card.get("trade_window_truncated")),
        ),
        limitations=[
            "Top-N L2 snapshots are not individual orders.",
            "Observed age is not the age of an order.",
            "Rolling daily volume changes are only an activity proxy.",
            "Before-attention history and full tape coverage are unknown.",
        ],
    )
    packet["packet_id"] = checksum(packet)
    return verify_packet(packet)


def packet_ready(packet, now):
    verify_packet(packet)
    return (
        0 <= now - packet["captured_at"] <= 15
        and packet["coverage"]["observation_healthy"]
        and packet["coverage"]["book_status"] == "observing"
        and any(f["id"] == "book.midpoint" and f["quality"] == "ok" for f in packet["facts"])
    )

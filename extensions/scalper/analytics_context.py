"""Versioned analytical context shared by selection, AI and Telegram; no model-derived facts."""

import math
from copy import deepcopy

from .fact_packet import checksum, verify_packet
from .phase import PHASES

VERSION = "scalper-context-v2"


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


def fact_value(packet, key):
    f = next((f for f in packet["facts"] if f["id"] == key), {})
    return f.get("value") if f.get("quality") == "ok" else None


def extend_packet(packet, service):
    result = deepcopy(packet)
    symbol, now = packet["symbol"], packet["captured_at"]
    rows = {window: row for (s, window), row in service.rows.items() if s == symbol}
    facts = result["facts"]
    healthy = packet["coverage"]["observation_healthy"]

    def add(
        key, value, unit, source, at, *, age=15, role="context", window=None, refs=(), source_time=None, available=True
    ):
        valid = (isinstance(value, (str, bool)) or finite(value)) and available
        time_valid = finite(at) and 0 <= now - at <= age
        quality = "ok" if valid and time_valid and healthy else "unknown" if not valid or not healthy else "stale"
        facts.append(
            dict(
                id=key,
                value=value if valid else None,
                quality=quality,
                unit=unit,
                source=source,
                observed_at=at if finite(at) else None,
                source_time=source_time if finite(source_time) else at if finite(at) else None,
                window=window,
                role=role,
                evidence=list(refs),
                calculation_version=VERSION,
                max_age_seconds=age,
            )
        )

    for window, seconds in (("1m", 60), ("5m", 300)):
        row = rows.get(window, {})
        snapshot = row.get("snapshot", {})
        metrics = snapshot.get("metrics", {})
        phase = row.get("movement_phase", {})
        stamp = metrics.get("price", {}).get("updated_at")
        source = f"native:{symbol}:{window}:{snapshot.get('captured_at')}"
        same_time = finite(snapshot.get("captured_at")) and 0 <= now - snapshot["captured_at"] <= 15
        for name, unit in (
            ("phase", "classification"),
            ("start_price", "price"),
            ("distance_natr", "atr_multiple"),
            ("retracement_natr", "atr_multiple"),
            ("age_seconds", "seconds"),
            ("started_at", "unix_seconds"),
            ("natr_at_start", "percent"),
        ):
            add(
                f"context.phase.{window}.{name}",
                phase.get(name),
                unit,
                "phase-tracker.v1",
                stamp,
                available=same_time and phase.get("phase") in PHASES,
                role="phase",
                window=window,
                refs=[source],
            )
        strength = row.get("strength", {})
        add(
            f"context.strength.{window}",
            strength.get("rank"),
            "rank",
            "finding-strength.v1",
            stamp,
            role="classification",
            window=window,
            refs=[source],
            available=same_time,
        )
        for field, unit in (
            ("natr", "percent"),
            ("range_ratio", "ratio"),
            ("activity_ratio", "ratio"),
            ("volume", "usd"),
            ("trade_rate", "trades_per_second"),
            ("imbalance", "ratio"),
            ("funding_rate", "fraction_per_hour"),
            ("open_interest", "usd"),
        ):
            m = metrics.get(field, {})
            age = seconds + 90 if field in {"natr", "range_ratio"} else 60
            add(
                f"context.{window}.{field}",
                m.get("value"),
                unit,
                m.get("source", "native_window"),
                m.get("updated_at"),
                age=age,
                window=window,
                refs=[source + ":" + field],
                available=same_time and m.get("quality") == "ok",
            )
        spread, price, natr = (metrics.get(k, {}) for k in ("spread", "price", "natr"))
        usable = all(m.get("quality") == "ok" and finite(m.get("value")) for m in (spread, price, natr))
        usable = usable and price["value"] > 0 and natr["value"] > 0 if usable else False
        usable = (
            usable
            and fact_value(result, f"context.{window}.natr") is not None
            and all(finite(m.get("updated_at")) and 0 <= now - m["updated_at"] <= 15 for m in (price, spread))
        )
        costly = 100 * spread["value"] / price["value"] >= 0.25 * natr["value"] if usable else None
        add(
            f"context.{window}.wide_spread",
            costly,
            "boolean",
            "spread-vs-natr.v1",
            stamp,
            available=same_time and usable,
            role="cost",
            window=window,
            refs=[source],
        )
        profile = row.get("profile") or {}
        sample_at = profile.get("updated_at")
        causal = (
            finite(sample_at)
            and sample_at <= now - seconds
            and profile.get("baseline_policy") == "strictly_prior_rolling_intervals.v2"
        )
        for days in (7, 30):
            period = profile.get("periods", {}).get(days, {})
            for field, native in (("trades", "trade_count"), ("spread_bps", None), ("natr", "natr")):
                reference = period.get("metrics", {}).get(field, {})
                prefix = f"profile.{window}.{days}d.{field}"
                stamp_profile = profile.get("calculated_at")
                for name, unit in (
                    ("median", "trades" if field == "trades" else "bps" if field == "spread_bps" else "percent"),
                    ("p90", "trades" if field == "trades" else "bps" if field == "spread_bps" else "percent"),
                    ("samples", "observations"),
                ):
                    add(
                        prefix + "." + name,
                        reference.get(name),
                        unit,
                        "instrument-observed-profile.v2",
                        stamp_profile,
                        source_time=sample_at,
                        age=600,
                        role="historical_baseline",
                        window=window,
                        available=causal and (reference.get("samples") or 0) >= 20,
                        refs=[source + f":profile:{days}"],
                    )
                current = metrics.get(native, {}) if native else {}
                median = reference.get("median")
                ratio = (
                    current["value"] / median
                    if finite(median) and median > 0 and current.get("quality") == "ok" and finite(current.get("value"))
                    else None
                )
                add(
                    prefix + ".current_ratio",
                    ratio,
                    "ratio",
                    "profile-comparison.v2",
                    current.get("updated_at"),
                    available=causal
                    and (reference.get("samples") or 0) >= 20
                    and finite(stamp_profile)
                    and 0 <= now - stamp_profile <= 600,
                    role="comparison",
                    age=seconds + 90 if native == "natr" else 60,
                    window=window,
                    refs=[prefix + ".median", source],
                )

    btc = rows.get("1m", {}).get("btc_context") or rows.get("5m", {}).get("btc_context") or {}
    for name, unit in (
        ("correlation", "ratio"),
        ("btc_last_return_pct", "percent"),
        ("asset_last_return_pct", "percent"),
        ("relative_last_return_pp", "percentage_points"),
        ("samples", "observations"),
    ):
        add(
            "btc." + name,
            btc.get(name),
            unit,
            "matched-closed-5m-returns.v1",
            btc.get("updated_at"),
            age=390,
            window="5m",
            refs=[f"btc:{symbol}:{btc.get('updated_at')}"],
            available=btc.get("samples") == 36,
        )

    senior = getattr(service, "candles", {}).get((symbol, "15m"), {})
    for field, unit in (
        ("return_4", "percent"),
        ("range_4", "percent"),
        ("range_position_4", "ratio"),
        ("natr", "percent"),
    ):
        add(
            "senior.15m." + field,
            senior.get(field),
            unit,
            "closed-candle-indicators.v1",
            senior.get("closed_at"),
            age=990,
            role="senior_context",
            window="4x15m" if field != "natr" else "15m",
            refs=[f"candles:{symbol}:15m:{senior.get('closed_at')}"],
        )

    candle = getattr(service.attention, "activity", {}).get(symbol, {})
    start = candle.get("start_ms")
    end = (start + 60000) / 1000 if finite(start) else None
    target = int(now * 1000) // 60000 * 60000 - 60000
    for name in ("trade_count", "base_volume"):
        ratio = candle.get("ratios", {}).get(name)
        add(
            "activity.closed_1m." + name + "_ratio",
            ratio,
            "ratio",
            "previous_14_contiguous.v1",
            end,
            age=75,
            available=candle.get("status") == "ready" and candle.get("start_ms") == target,
            role="activity",
            window="closed_utc_1m",
            refs=[f"overview:{symbol}:{candle.get('start_ms')}"],
        )

    pulse = service.pulse.facts(symbol, now) if service.pulse else {}
    closed = pulse.get("window_end_ms")
    expected_end = ((int(now * 1000) - 5000) // 60000) * 60000
    valid_window = closed == expected_end and pulse.get("window_start_ms") == expected_end - 60000
    closed = closed / 1000 if finite(closed) else None
    refs = [pulse["evidence_id"]] if pulse.get("evidence_id") else []
    for venue in ("hyperliquid", "okx"):
        observed = next((v for v in pulse.get("venues", []) if v["venue"] == venue), {})
        usable = valid_window and observed.get("quality") in {"observed_no_detected_gap", "reconciled"}
        for name, unit in (
            ("ofi", "ratio"),
            ("execution_count", "executions"),
            ("quote_volume", "USDC" if venue == "hyperliquid" else "USDT"),
        ):
            raw = observed.get(name)
            try:
                raw = float(raw) if raw is not None else None
            except (ValueError, TypeError):
                raw = None
            if finite(raw) and (abs(raw) > 1 if name == "ofi" else raw < 0):
                raw = None
            add(
                f"venue.{venue}." + name,
                raw,
                unit,
                "pulse-closed-window.v1",
                closed,
                age=75,
                available=usable,
                role="cross_venue",
                window="closed_utc_1m",
                refs=[*refs, f"{venue}:{symbol}:{pulse.get('window_start_ms')}:v{observed.get('version')}"],
            )
    add(
        "venue.relation",
        pulse.get("flow"),
        "classification",
        "same-utc-minute-comparison.v1",
        closed,
        age=75,
        available=pulse.get("status") == "ready"
        and valid_window
        and all(fact_value(result, f"venue.{v}.ofi") is not None for v in ("hyperliquid", "okx")),
        role="cross_venue",
        window="closed_utc_1m",
        refs=refs,
    )

    market = getattr(service.hub, "context", {}) or {}
    for section, field, unit, age in (
        ("lsr", "long_short_ratio", "ratio", 300),
        ("basis", "basis_pct", "percent", 120),
        ("iv", "mark_iv", "percent", 300),
    ):
        for instrument in dict.fromkeys((symbol, "BTC")):
            observed = market.get(section, {}).get(instrument, {})
            add(
                f"market.{instrument}.{section}",
                observed.get(field),
                unit,
                observed.get("source", section),
                observed.get("timestamp"),
                age=age,
                role="market_context",
                refs=[f"market:{instrument}:{section}:{observed.get('timestamp')}"],
            )
    # The aggregate currently has no source timestamp. It must not be promoted using fetch time.
    add(
        "market.confirmed_liquidations_1h",
        None,
        "events",
        "source_timestamp_unavailable",
        None,
        role="coverage",
        window="1h",
    )
    phase1, phase5 = (fact_value(result, f"context.phase.{w}.phase") for w in ("1m", "5m"))
    scale = None
    for window, phase, span in (("5m", phase5, 240), ("1m", phase1, 45)):
        endpoints = [
            next((f for f in facts if f["id"] == f"frame.{window}.{k}_trade_price"), {}) for k in ("first", "last")
        ]
        first, last = (f.get("source_time") for f in endpoints)
        if (
            phase in {"early", "confirmed", "extended", "weakening"}
            and all(f.get("quality") == "ok" for f in endpoints)
            and finite(first)
            and finite(last)
            and last - first >= span
        ):
            scale = window
            break
    ranks = [fact_value(result, f"context.strength.{w}") for w in ("1m", "5m")]
    ranks = [v for v in ranks if finite(v)]
    rank = max(ranks) if ranks else None
    activity5 = fact_value(result, "context.5m.activity_ratio")
    if rank is not None and scale == "5m" and phase1 == phase5 == "confirmed" and finite(activity5) and activity5 >= 2:
        rank = min(4, rank + 1)
    add(
        "formation.scale",
        scale,
        "classification",
        "multi-scale-context.v1",
        now,
        role="classification",
        refs=["context.phase.1m.phase", "context.phase.5m.phase"],
    )
    add(
        "formation.strength",
        rank,
        "rank",
        "multi-scale-context.v1",
        now,
        role="classification",
        refs=["context.strength.1m", "context.strength.5m", "context.5m.activity_ratio"],
    )
    result["context_version"] = VERSION
    result["facts"] = sorted(facts, key=lambda f: f["id"])
    result["scenarios"] = scenario_plan(result)
    result["packet_id"] = checksum({k: v for k, v in result.items() if k != "packet_id"})
    return verify_packet(result)


def analytical_state(packet):
    if packet.get("context_version") != VERSION:
        return {}
    return dict(
        context_version=VERSION,
        phase=fact_value(packet, "context.phase.1m.phase"),
        phase5=fact_value(packet, "context.phase.5m.phase"),
        rank=fact_value(packet, "context.strength.1m"),
        natr=fact_value(packet, "context.1m.natr"),
        wide_spread=fact_value(packet, "context.1m.wide_spread"),
        btc_relative=fact_value(packet, "btc.relative_last_return_pp"),
        cross_venue=fact_value(packet, "venue.relation"),
        rolling_activity=fact_value(packet, "context.1m.activity_ratio"),
        closed_activity=fact_value(packet, "activity.closed_1m.trade_count_ratio"),
        scenario=packet.get("scenarios", {}),
        formation_scale=fact_value(packet, "formation.scale"),
        formation_strength=fact_value(packet, "formation.strength"),
    )


def admission(state, reason):
    if state.get("wide_spread") is True:
        return "wide_spread"
    if state.get("phase") in {"extended", "weakening"} and reason in {"early_context", "formation_confirmed"}:
        return "late_impulse"
    return None


def scenario_plan(packet):
    zones = []
    for fact in packet["facts"]:
        if fact["id"].startswith("density.") and fact["id"].endswith(".relative_size") and fact["quality"] == "ok":
            prefix = fact["id"].removesuffix(".relative_size")
            price = fact_value(packet, prefix + ".price")
            if finite(price):
                zones.append((fact["value"], prefix, price))
    if zones:
        _, prefix, price = max(zones)
        side = prefix.split(".")[1]
        return dict(
            kind="observed_zone",
            anchor=price,
            anchor_ref=prefix + ".price",
            side=side,
            main_relation="above" if side == "bid" else "below",
            cancel_relation="below" if side == "bid" else "above",
            confirmation_observations=2,
            references=[prefix + ".price", prefix + ".relative_size", "book.midpoint"],
            qualification="Only a visible L2 zone; absence is not proof of cancellation or execution.",
        )
    anchor = fact_value(packet, "context.phase.1m.start_price")
    anchor_ref = "context.phase.1m.start_price"
    if not finite(anchor):
        anchor = fact_value(packet, "frame.1m.first_trade_price")
        anchor_ref = "frame.1m.first_trade_price"
    flow = fact_value(packet, "frame.1m.ofi")
    if finite(anchor) and finite(flow) and abs(flow) >= 0.6:
        return dict(
            kind="observed_impulse",
            anchor=anchor,
            anchor_ref=anchor_ref,
            main_relation="above" if flow > 0 else "below",
            cancel_relation="below" if flow > 0 else "above",
            confirmation_observations=2,
            references=[anchor_ref, "frame.1m.ofi", "book.midpoint"],
            qualification="Observed start price, not a forecast target or recommended entry.",
        )
    return dict(kind="insufficient_data", references=[])


def compact_model_packet(packet):
    if packet.get("context_version") != VERSION and packet.get("scope") != "market_meta":
        return packet
    view = {k: v for k, v in packet.items() if k not in {"facts", "limitations"}}
    fields = ("id", "value", "unit", "quality", "observed_at", "source_time", "window", "role", "calculation_version")
    view["facts"] = [{k: f.get(k) for k in fields} for f in packet["facts"]]
    view["transport_note"] = "Compact view of the immutable packet; complete source evidence is retained by packet_id."
    return view

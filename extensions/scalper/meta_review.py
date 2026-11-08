"""Selective cross-episode review of directly computed facts, not model votes."""

import json
import time

from .context_auto import decide, enrich, fingerprint
from .fact_packet import PACKET_VERSION, RULESET, checksum, packet_ready
from .fact_review import FactReviewer, validate_review


async def run_meta(service):
    now = time.time()
    routes = service.ai_routes
    if not routes or not service.gemini.key or not service.context_auto_enabled:
        return None
    last = routes.db.execute("SELECT MAX(at) FROM ai_route_requests WHERE route='meta'").fetchone()[0]
    if last is not None and now - last < 300:
        return None
    changed, packets = {}, []
    for packet in service.context_packets.values():
        coverage = packet.get("coverage", {})
        if coverage.get("attention_policy_version") == 5 and (
            coverage.get("attention_state") != "focus" or not coverage.get("live_quality_confirmed")
        ):
            continue
        if not packet_ready(packet, now):
            continue
        memory = service.context_memory.observe(packet)
        prior = routes.db.execute(
            "SELECT signature FROM ai_meta_seen WHERE episode_id=?", (packet["episode_id"],)
        ).fetchone()
        priority, reason, state = decide(packet, memory["samples"], json.loads(prior[0]) if prior else None)
        if priority is not None:
            state["_reason"] = reason
            changed[packet["episode_id"]] = state
        history = memory["samples"][-2:]
        if prior:
            prior_state = json.loads(prior[0])
            if prior_state.get("at") is not None and 0 < now - prior_state["at"] <= 900:
                history = [prior_state, *history]
        packets.append(enrich(packet, history, reason))
    if len(changed) < 2:
        return None
    facts = []
    for packet in sorted(packets, key=lambda p: p["symbol"]):
        priority_ids = (
            "formation.scale",
            "formation.strength",
            "context.phase.1m.phase",
            "context.phase.5m.phase",
            "context.strength.1m",
            "context.1m.natr",
            "context.1m.wide_spread",
            "context.1m.activity_ratio",
            "activity.closed_1m.trade_count_ratio",
            "btc.relative_last_return_pp",
            "btc.correlation",
            "venue.relation",
            "profile.1m.7d.trades.current_ratio",
            "profile.1m.30d.trades.current_ratio",
            "frame.1m.ofi",
            "frame.1m.trade_count",
            "frame.1m.observed_price_change_bps",
            "frame.1m.flow_price_conflict",
            "frame.5m.ofi",
            "frame.5m.trade_count",
            "tape.observed_trade_imbalance",
            "tape.observed_trade_count_60s",
            "activity.relative_score",
            "book.midpoint",
            "book.spread_bps",
        )
        eligible = sorted(
            (f for f in packet["facts"] if f["quality"] == "ok" and f["role"] != "history"),
            key=lambda f: priority_ids.index(f["id"]) if f["id"] in priority_ids else 100,
        )[:26]
        eligible += [f for f in packet["facts"] if f["role"] == "history"][-6:]
        for fact in eligible:
            item = dict(fact, id=packet["symbol"] + ":" + fact["id"])
            if item["unit"] in ("price", "base_units"):
                item["unit"] += ":" + packet["symbol"]
            facts.append(item)
    packet = dict(
        version=PACKET_VERSION,
        ruleset=RULESET,
        episode_id="market.meta.v1",
        symbol="MARKET",
        scope="market_meta",
        captured_at=now,
        facts=facts,
        coverage=dict(observation_healthy=True, book_status="observing"),
        reason="material_changes_in_multiple_episodes",
        input_packets=[p["packet_id"] for p in packets],
    )
    packet["packet_id"] = checksum(packet)
    state = dict(
        _meta={
            k: dict(
                signature=fingerprint(v),
                zone_size=(v.get("zone") or {}).get("size") if v["_reason"] == "density_changed" else None,
            )
            for k, v in changed.items()
        }
    )
    job, reason = routes.reserve(packet, "meta", state, now, service.policy.hourly_limit,
                                cooldown_seconds=service.policy.cooldown_seconds)
    if reason:
        return None
    for source_packet in packets:
        service.consistency.save_packet(source_packet)
    key = service.consistency.begin(
        packet,
        now,
        service.gemini.provider,
        service.gemini.model,
        origin="automatic",
        trigger_reason="multiple_episodes_changed",
        route="meta",
    )
    with routes.db:
        routes.db.executemany(
            "INSERT OR REPLACE INTO ai_meta_seen VALUES(?,?)", [(k, json.dumps(v)) for k, v in changed.items()]
        )
    response = await FactReviewer(service.gemini).review(packet)
    status, result, reason = response["status"], None, response.get("reason")
    if status == "ok":
        try:
            result = validate_review(response["raw"], packet, time.time())
        except (ValueError, KeyError, TypeError) as exc:
            status, reason = "invalid_response", str(exc)
    review = service.consistency.finish(
        key,
        status,
        time.time(),
        result=result,
        raw=response.get("raw"),
        reason=reason,
        model=response.get("model"),
        usage=response.get("usage"),
    )
    routes.finish(job, status, key, response.get("retry_after"))
    return dict(status=status, packet=packet, review=review)

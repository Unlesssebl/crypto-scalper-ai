"""Selective cross-episode review of directly computed facts, not model votes."""

import json
import time

from .context_auto import decide, fingerprint
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
        packets.append(packet)
    if len(changed) < 2:
        return None
    facts = []
    for packet in sorted(packets, key=lambda p: p["symbol"]):
        eligible = [f for f in packet["facts"] if f["quality"] == "ok" and f["role"] != "history"][:24]
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
    job, reason = routes.reserve(packet, "meta", state, now, service.policy.hourly_limit)
    if reason:
        return None
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

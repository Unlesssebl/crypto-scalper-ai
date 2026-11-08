"""Episode memory and deterministic reasons to request interpretation, never an entry signal."""

import json
import math
from copy import deepcopy

from .analytics_context import analytical_state
from .fact_packet import canonical, checksum, packet_ready

REASONS = {
    "interesting_instrument": "инструмент выделяется относительно собственной обычной активности",
    "active_instrument": "наблюдается устойчивый свежий поток сделок",
    "interesting_level": "устойчивая необычная зона в наблюдаемом стакане",
    "discovery_unconfirmed": "пока нет устойчивого повода выделить инструмент",
    "formation_confirmed": "движение подтвердилось последовательными наблюдениями с учётом волатильности",
    "early_context": "сильный поток подтверждён реакцией цены и контекстом",
    "flow_price_conflict": "устойчивое расхождение потока и реакции цены",
    "density_context": "устойчивая значимая зона в наблюдаемом стакане",
    "density_changed": "существенно изменился размер наблюдаемой зоны",
    "flow_reversed": "изменился подтверждённый поток эпизода",
    "cross_window_changed": "изменилось соотношение потоков 1m и 5m",
    "insufficient_context": "недостаточно пригодного контекста",
    "live_quality_unconfirmed": "реальный поток и реакция цены ещё не подтверждены",
    "ordinary_context": "нет значимого повода для интерпретации",
    "no_material_change": "нет содержательного изменения после отправленного разбора",
    "unstable_context": "изменение ещё не подтверждено последовательными наблюдениями",
    "no_api_key": "модель не настроена",
    "disabled": "автоматический разбор отключён",
    "stale_or_unknown_packet": "нет свежего пригодного пакета",
}


def summarize(packet):
    facts = {f["id"]: f for f in packet["facts"]}

    def value(key):
        f = facts.get(key, {})
        return f.get("value") if f.get("quality") == "ok" else None

    zones = []
    for key in facts:
        if not key.endswith(".relative_size") or not key.startswith("density."):
            continue
        prefix = key.removesuffix(".relative_size")
        zone = {n: value(prefix + "." + n) for n in ("price", "size", "relative_size", "observed_age_seconds")}
        if all(type(v) in (int, float) and math.isfinite(v) for v in zone.values()):
            zone.update(
                {
                    n: value(prefix + "." + n)
                    for n in ("distance_bps", "size_change", "size_to_observed_turnover", "notional_usd")
                }
            )
            zones.append(dict(zone, id=prefix, side=prefix.split(".")[1]))
    zones.sort(key=lambda z: (-z["relative_size"], z["id"]))
    flow = value("frame.1m.ofi")
    count = value("frame.1m.trade_count")
    window = "1m"
    if flow is None or count is None:
        flow, count = value("tape.observed_trade_imbalance"), value("tape.observed_trade_count_60s")
        window = "observed_60s"
    midpoint = facts.get("book.midpoint", {})
    flow_id = "frame.1m.ofi" if window == "1m" else "tape.observed_trade_imbalance"
    source_ids = dict(midpoint="book.midpoint", flow=flow_id, activity="activity.relative_score")
    sources = {
        name: {k: facts.get(key, {}).get(k) for k in ("observed_at", "source_time", "evidence")}
        for name, key in source_ids.items()
    }
    return dict(
        at=midpoint.get("observed_at"),
        midpoint=value("book.midpoint"),
        sources=sources,
        evidence=midpoint.get("evidence", []),
        flow=flow,
        count=count,
        window=window,
        flow5=value("frame.5m.ofi"),
        change=value("frame.1m.observed_price_change_bps"),
        activity=value("activity.relative_score"),
        spread=value("book.spread_bps"),
        zone=zones[0] if zones else None,
        **analytical_state(packet),
    )


def sign(value):
    return 0 if value is None or abs(value) < 0.6 else 1 if value > 0 else -1


def relation(state, history):
    if state.get("at") is None:
        return "unknown"
    change = state.get("change")
    if change is None:
        # Early context uses observed price history; never a fabricated full 1m window.
        usable = [s for s in history if s.get("midpoint") and 15 <= state["at"] - s["at"] <= 60]
        if not usable or not state.get("midpoint"):
            return "unknown"
        change = (state["midpoint"] / usable[0]["midpoint"] - 1) * 10000
    if state.get("spread") is None or not sign(state.get("flow")):
        return "unknown"
    threshold = max(2 * state["spread"], 0.5)
    return "stalled" if abs(change) <= threshold else "aligned" if change * state["flow"] > 0 else "opposed"


def fingerprint(state):
    zone = state.get("zone")
    context = (
        dict(
            context_version=state["context_version"],
            phase=state.get("phase"),
            phase5=state.get("phase5"),
            cross_venue=state.get("cross_venue"),
        )
        if state.get("context_version")
        else {}
    )
    return checksum(
        dict(
            flow=sign(state.get("flow")),
            relation=state.get("relation"),
            cross=state.get("cross"),
            **({"discovery_kind": state["discovery_kind"]} if "discovery_kind" in state else {}),
            **context,
            zone=zone["id"] if zone and zone["observed_age_seconds"] >= 30 else None,
        )
    )


def decide(packet, history, baseline=None, *, economical=False):
    state = summarize(packet)
    state["relation"] = relation(state, history)
    state["cross"] = (
        ("aligned" if sign(state["flow"]) == sign(state["flow5"]) else "opposed")
        if sign(state["flow"]) and sign(state["flow5"])
        else "unknown"
    )
    coverage = packet.get("coverage", {})
    if coverage.get("attention_policy_version") == 5 and (
        coverage.get("attention_state") != "focus" or not coverage.get("live_quality_confirmed")
    ):
        return None, "live_quality_unconfirmed", state
    if not packet_ready(packet, packet["captured_at"]) or state["at"] is None:
        return None, "insufficient_context", state
    history = [s for s in history if s.get("at") is not None and 10 <= state["at"] - s["at"] <= 30]
    strong = bool(sign(state["flow"]) and state.get("count") is not None and state["count"] >= 20)
    stable_flow = (
        state["relation"] != "unknown"
        and any(sign(s.get("flow")) == sign(state["flow"]) and s.get("relation") == state["relation"] for s in history)
        and strong
    )
    zone = state["zone"]
    stable_zone = bool(
        zone
        and zone["relative_size"] >= 6
        and zone["observed_age_seconds"] >= 30
        and any(s.get("zone", {}).get("id") == zone["id"] for s in history if s.get("zone"))
    )
    if coverage.get("attention_policy_version") == 6:
        discovery = coverage.get("discovery", {})
        live = coverage.get("live_quality_confirmed") is True
        potential = discovery.get("potential") is True
        kind = "active" if live else "potential" if potential else "level" if stable_zone else "ordinary"
        state["discovery_kind"] = kind
        state["discovery_score"] = discovery.get("score", 0)
        state["price_reaction_confirmed"] = coverage.get("price_reaction_confirmed", False)
        state["discovery_reason"] = discovery.get("reason")
        if kind == "ordinary":
            return None, "discovery_unconfirmed", state
        priority, reason = {"active": (60, "active_instrument"), "potential": (40, "interesting_instrument"),
                            "level": (35, "interesting_level")}[kind]
        if baseline:
            upgraded = kind == "active" and baseline.get("discovery_kind") != "active"
            stronger = (state["discovery_score"] >= max(6, (baseline.get("discovery_score") or 0) * 1.75)
                        and state["discovery_score"] > (baseline.get("discovery_score") or 0))
            new_zone = bool(stable_zone and (not baseline.get("zone") or zone["id"] != baseline["zone"]["id"]))
            if not (upgraded or stronger or new_zone):
                return None, "no_material_change", state
        return priority, reason, state
    phase = state.get("phase")
    stable_phase = stable_flow and phase == "confirmed" and any(s.get("phase") == phase for s in history)
    if stable_phase and (not baseline or baseline.get("phase") != phase):
        return 85, "formation_confirmed", state
    if baseline:
        old_zone = baseline.get("zone")
        if (
            stable_zone
            and old_zone
            and zone["id"] == old_zone["id"]
            and old_zone["size"] > 0
            and abs(zone["size"] / old_zone["size"] - 1) >= 0.5
        ):
            return 80, "density_changed", state
        if stable_flow and sign(baseline.get("flow")) and sign(baseline["flow"]) != sign(state["flow"]):
            return 90, "flow_reversed", state
        if (
            stable_flow
            and state["cross"] != "unknown"
            and (baseline.get("cross", "unknown") != "unknown" or state["cross"] == "opposed")
            and state["cross"] != baseline["cross"]
            and any(s.get("cross") == state["cross"] for s in history)
        ):
            return 80, "cross_window_changed", state
        if (
            stable_flow
            and state["relation"] in {"opposed", "stalled"}
            and baseline.get("relation") not in {"opposed", "stalled"}
        ):
            return 100, "flow_price_conflict", state
        return None, "no_material_change", state
    if stable_flow and state["relation"] in {"opposed", "stalled"}:
        return 100, "flow_price_conflict", state
    if stable_zone:
        return 50, "density_context", state
    book_support = bool(
        zone
        and zone["relative_size"] >= 4
        and zone["observed_age_seconds"] >= 15
        and (zone["side"] == "bid") == (sign(state["flow"]) > 0)
    )
    if (
        not economical
        and stable_flow
        and state["relation"] == "aligned"
        and (state.get("activity") or 0) >= 3
        and (state["window"] == "1m" or book_support)
    ):
        return 70, "early_context", state
    return None, "unstable_context" if strong or zone else "ordinary_context", state


def enrich(packet, samples, reason):
    result = deepcopy(packet)
    for sample in samples[-6:]:
        for field, unit in (("midpoint", "price"), ("flow", "ratio"), ("activity", "ratio")):
            source = sample.get("sources", {}).get(field, {})
            at = source.get("observed_at") or sample.get("at")
            if sample.get(field) is None or at is None or not 0 < packet["captured_at"] - at <= 900:
                continue
            result["facts"].append(
                dict(
                    id=f"history.{int(sample['at'] * 1000)}.{field}",
                    value=sample[field],
                    unit=unit,
                    quality="ok",
                    observed_at=at,
                    source_time=source.get("source_time") or at,
                    source="observed_episode_sample",
                    evidence=source.get("evidence") or sample.get("evidence", []),
                    role="history",
                    window="episode_sample",
                    calculation_version="context-memory-v1",
                    measurement=field + (":" + sample["window"] if field == "flow" else ""),
                )
            )
    result["selection"] = dict(reason=reason, ruleset="context-filter-v1", scope="same_episode")
    result["facts"].sort(key=lambda f: f["id"])
    result.pop("packet_id")
    result["packet_id"] = checksum(result)
    return result


class ContextMemory:
    def __init__(self, db):
        self.db = db
        db.executescript("""CREATE TABLE IF NOT EXISTS context_memory (
            episode_id TEXT PRIMARY KEY,symbol TEXT NOT NULL,samples TEXT NOT NULL,
            last_sent TEXT,last_ok TEXT,last_attempt REAL,last_review TEXT);
            CREATE TABLE IF NOT EXISTS context_decisions (
            id INTEGER PRIMARY KEY,episode_id TEXT,symbol TEXT,at REAL,last_at REAL,
            signature TEXT,reason TEXT,status TEXT,checks INTEGER DEFAULT 1,review_id TEXT,state TEXT,
            UNIQUE(episode_id,signature,reason,status));""")

    def get(self, episode):
        row = self.db.execute("SELECT * FROM context_memory WHERE episode_id=?", (episode,)).fetchone()
        return (
            {**dict(row), **{k: json.loads(row[k]) if row[k] else None for k in ("samples", "last_sent", "last_ok")}}
            if row
            else dict(samples=[], last_sent=None, last_ok=None, last_attempt=None)
        )

    def observe(self, packet):
        memory = self.get(packet["episode_id"])
        if not packet_ready(packet, packet["captured_at"]):
            return memory
        _, _, state = decide(packet, memory["samples"])
        samples = memory["samples"]
        if state["at"] is None or samples and state["at"] - samples[-1]["at"] < 10:
            return memory
        samples = [*samples, state][-64:]
        with self.db:
            self.db.execute(
                "INSERT INTO context_memory(episode_id,symbol,samples) VALUES(?,?,?) "
                "ON CONFLICT(episode_id) DO UPDATE SET samples=excluded.samples",
                (packet["episode_id"], packet["symbol"], canonical(samples)),
            )
        return {**memory, "samples": samples}

    def record(self, packet, reason, status, state, review_id=None):
        with self.db:
            self.db.execute(
                """INSERT INTO context_decisions
                (episode_id,symbol,at,last_at,signature,reason,status,review_id,state) VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(episode_id,signature,reason,status) DO UPDATE SET
                last_at=excluded.last_at,checks=checks+1,review_id=COALESCE(excluded.review_id,review_id)""",
                (
                    packet["episode_id"],
                    packet["symbol"],
                    packet["captured_at"],
                    packet["captured_at"],
                    fingerprint(state),
                    reason,
                    status,
                    review_id,
                    canonical(state),
                ),
            )

    def sent(self, packet, state, now):
        self.observe(packet)
        with self.db:
            self.db.execute(
                "UPDATE context_memory SET last_sent=?,last_attempt=? WHERE episode_id=?",
                (canonical(state), now, packet["episode_id"]),
            )

    def completed(self, packet, state, review_id, ok):
        with self.db:
            self.db.execute(
                "UPDATE context_memory SET last_review=?,last_ok=CASE WHEN ? THEN ? ELSE last_ok END "
                "WHERE episode_id=?",
                (review_id, ok, canonical(state), packet["episode_id"]),
            )

    def latest(self, episode):
        row = self.db.execute(
            "SELECT reason,status,last_at,review_id FROM context_decisions WHERE episode_id=? "
            "ORDER BY last_at DESC,id DESC LIMIT 1",
            (episode,),
        ).fetchone()
        return dict(row) if row else None

    def report(self, now):
        rows = list(
            self.db.execute(
                "SELECT reason,status,COUNT(*) AS episodes,SUM(checks) AS checks "
                "FROM context_decisions WHERE last_at BETWEEN ? AND ? GROUP BY reason,status",
                (now - 86400, now),
            )
        )
        return dict(ruleset="context-filter-v1", decisions=[dict(r) for r in rows])

"""Replay interpretation admission on recorded observations; never calls a model or Telegram."""
import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

from extensions.scalper.context_auto import decide, enrich, fingerprint
from extensions.scalper.fact_packet import build_packet
from extensions.scalper.history.archive import read_archive
from extensions.scalper.history.facts import Facts
from extensions.scalper.history.replay import replay


def evaluate(source, output, *, hourly_limit=12, cooldown=300):
    if hourly_limit < 0 or cooldown < 1:
        raise ValueError("Invalid shared budget")
    verification = replay(source, retain_cards=False)
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    facts = Facts(version=2)
    active, histories, baselines = {}, defaultdict(list), {}
    attempts, last_symbol, seen = [], {}, set()
    counts = Counter()
    observations = admitted = unique_candidates = 0
    last_scout = None
    with (output / "decisions.ndjson").open("x", encoding="utf-8") as decisions, (
            output / "selected-packets.ndjson").open("x", encoding="utf-8") as packets:
        for event in read_archive(source):
            now, kind, payload = event["received_at"], event["kind"], event["payload"]
            if kind == "scout":
                last_scout = now
            elif kind == "assignment":
                if payload["state"] == "scout":
                    active.pop(payload["symbol"], None)
                else:
                    old = active.get(payload["symbol"], {})
                    active[payload["symbol"]] = dict(symbol=payload["symbol"], state=payload["state"],
                        assigned_at=old.get("assigned_at", now), score=payload["score"])
            facts.apply(event)
            if kind != "facts" or payload["symbol"] not in active:
                continue
            assignment = {**active[payload["symbol"]], "facts": facts.card(payload["symbol"], now)}
            packet = build_packet(dict(archive=dict(run=event["run"]), last_scout_at=last_scout), assignment, now)
            episode = packet["episode_id"]
            samples = histories[episode]
            priority, reason, state = decide(packet, samples, baselines.get(episode))
            observations += 1
            if state["at"] is not None and (not samples or state["at"] - samples[-1]["at"] >= 10):
                samples.append(state)
                histories[episode] = samples = samples[-64:]
            status = "rejected"
            if priority is not None:
                signature = (episode, reason, fingerprint(state))
                unique_candidates += signature not in seen
                seen.add(signature)
                attempts = [at for at in attempts if at > now - 3600]
                if len(attempts) >= hourly_limit:
                    reason, status = "hourly_limit", "skipped"
                elif now - last_symbol.get(packet["symbol"], -1e30) < cooldown:
                    reason, status = "cooldown", "skipped"
                else:
                    attempts.append(now)
                    last_symbol[packet["symbol"]] = now
                    baselines[episode] = state
                    status = "would_dispatch"
                    admitted += 1
                    packets.write(json.dumps(enrich(packet, samples, reason), ensure_ascii=False) + "\n")
            counts[reason + "/" + status] += 1
            decisions.write(json.dumps(dict(at=now, episode_id=episode, symbol=packet["symbol"],
                                            reason=reason, status=status), ensure_ascii=False) + "\n")
    report = dict(replay_checksum=verification["checksum"], model_calls=0, observations=observations,
                  candidate_contexts=unique_candidates, would_dispatch=admitted, reasons=dict(counts),
                  hourly_limit=hourly_limit, cooldown_seconds=cooldown,
                  limitation="Zero-latency admission simulation on selected archived instruments; early trade facts "
                  "are re-derived from raw frames. Unselected L2, model responses and usefulness are unobserved.")
    (output / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hourly-limit", type=int, default=12)
    parser.add_argument("--cooldown", type=int, default=300)
    args = parser.parse_args()
    print(json.dumps(evaluate(args.archive, args.output, hourly_limit=args.hourly_limit, cooldown=args.cooldown)))


if __name__ == "__main__":
    main()

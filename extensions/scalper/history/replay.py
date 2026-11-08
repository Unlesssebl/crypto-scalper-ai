"""Offline attention/fact replay. No external effects or wall-clock decisions."""
import argparse
import hashlib
import json
from pathlib import Path

from extensions.scalper.relative_attention import Attention, AttentionPolicy

from .archive import read_archive
from .facts import Facts


def replay(path, *, retain_cards=True, allow_incomplete=False):
    facts = Facts()
    attention = None
    outputs = []
    digest = hashlib.sha256()
    expected = []
    assignments = []
    external = False
    first = None
    last = None
    manifest = json.loads((Path(path) / "manifest.json").read_text(encoding="utf-8"))
    for event in read_archive(path, allow_incomplete=allow_incomplete):
        now, kind = event["received_at"], event["kind"]
        first = now if first is None else first
        last = now
        if kind == "policy":
            attention = Attention(AttentionPolicy(**{"version": 1, **event["payload"]}))
        elif kind == "attention_state":
            from collections import deque
            for name in ("previous", "scouts", "active"):
                setattr(attention, name, event["payload"].get(name, {}))
            for name in ("baselines", "movement_baselines"):
                setattr(attention, name, {s: deque(v, maxlen=attention.policy.baseline_size)
                                          for s, v in event["payload"].get(name, {}).items()})
        elif kind == "source":
            external = event["payload"].get("assignment_mode") == "external_plan.v1"
            facts.version = event["payload"].get("fact_version", 1)
            if facts.version not in (1, 2, 3):
                raise ValueError("Unsupported recorded fact version")
        elif kind == "scout":
            if attention is None:
                raise ValueError("Missing recorded attention policy")
            expected.extend(attention.update(event["payload"], now))
        elif kind == "assignment":
            if not external and (not expected or event["payload"] != expected.pop(0)):
                raise ValueError("Recorded assignments differ from deterministic policy replay")
            assignments.append(event["payload"])
            digest.update(json.dumps(event["payload"], sort_keys=True, allow_nan=False).encode())
        elif kind == "facts":
            actual = facts.card(event["payload"]["symbol"], now)
            if actual != event["payload"]:
                raise ValueError("Recorded facts differ from replay")
            digest.update(json.dumps(actual, sort_keys=True, allow_nan=False).encode())
            if retain_cards:
                outputs.append(actual)
        facts.apply(event)
    if expected and manifest.get("complete"):
        raise ValueError("Missing attention assignments")
    return dict(start=first, end=last, assignments=assignments, cards=outputs,
                checksum=digest.hexdigest(), verified=True, complete=bool(manifest.get("complete")),
                pending_assignments=len(expected), limitation=manifest.get("limitation"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-incomplete", action="store_true",
                        help="explicitly inspect a recovered prefix; never marks it complete")
    args = parser.parse_args()
    result = replay(args.archive, allow_incomplete=args.allow_incomplete)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2, allow_nan=False)
    print(json.dumps(dict(verified=True, complete=result["complete"], checksum=result["checksum"],
                          cards=len(result["cards"]))))


if __name__ == "__main__":
    main()

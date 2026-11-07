"""Recheck saved factual reviews at their recorded validation times, offline and read-only."""

import argparse
import json
import math
import sqlite3
from contextlib import closing
from pathlib import Path

from extensions.scalper.fact_packet import verify_packet
from extensions.scalper.fact_review import META_VALIDATOR_VERSION, VALIDATOR_VERSION, validate_review


def replay(journal):
    checked = 0
    with closing(sqlite3.connect(Path(journal).resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        for row in db.execute("""SELECT r.*,p.payload FROM fact_reviews r JOIN fact_packets p ON p.id=r.packet_id
                WHERE r.status='ok' ORDER BY r.at,r.id"""):
            packet = verify_packet(json.loads(row["payload"]))
            expected = json.loads(row["result"])
            if expected["validator_version"] not in {VALIDATOR_VERSION, META_VALIDATOR_VERSION}:
                raise ValueError("Replay requires the original validator version")
            validated_at = expected["validated_at"]
            if (
                type(validated_at) not in (int, float)
                or not math.isfinite(validated_at)
                or row["completed_at"] is None
                or not row["at"] <= validated_at <= row["completed_at"]
            ):
                raise ValueError("Invalid recorded validation time")
            actual = validate_review(row["raw"], packet, validated_at)
            for claim in expected["claims"]:
                claim.pop("novelty", None)  # Cross-review history, not model assertion truth.
            if actual != expected:
                raise ValueError(f"Saved review differs from deterministic recheck: {row['id']}")
            checked += 1
    return dict(verified=True, reviews=checked, validator_version=VALIDATOR_VERSION, model_calls=0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = replay(args.journal)
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
    print(json.dumps(result))


if __name__ == "__main__":
    main()

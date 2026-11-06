"""Export blind template-versus-review trials without model calls or future prices."""
import argparse
import csv
import json
import random
import sqlite3
from contextlib import closing
from pathlib import Path

from extensions.scalper.fact_packet import verify_packet


def export(journal, output, limit=200, seed=20261105):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    rng = random.Random(seed)
    cases, mapping = [], []
    # A read transaction freezes both packet and review contents; never open Journal/recover.
    with closing(sqlite3.connect(Path(journal).resolve().as_uri() + "?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        rows = db.execute("""SELECT r.*,p.payload,p.episode_id FROM fact_reviews r
              JOIN fact_packets p ON p.id=r.packet_id ORDER BY r.at,r.id""").fetchall()
        rng.shuffle(rows)
        seen = set()
        for row in rows:
            if row["episode_id"] in seen:
                continue
            seen.add(row["episode_id"])
            packet = verify_packet(json.loads(row["payload"]))
            checked = json.loads(row["result"]) if row["result"] else None
            variants = [dict(method="template", content=dict(facts=packet["facts"], limitations=packet["limitations"])),
                        dict(method="model", content=dict(status=row["status"], result=checked))]
            rng.shuffle(variants)
            case_id = f"case-{len(cases) + 1:04d}"
            cases.append(dict(id=case_id, symbol=packet["symbol"], captured_at=packet["captured_at"],
                              variants={label: v["content"] for label, v in zip(("A", "B"), variants)}))
            mapping.append(dict(id=case_id, packet_id=packet["packet_id"], review_id=row["id"],
                                methods={label: v["method"] for label, v in zip(("A", "B"), variants)},
                                provider=row["provider"], model=row["model"]))
            if len(cases) >= limit:
                break
    (output / "blind-cases.json").write_text(json.dumps(cases, ensure_ascii=False, indent=2), encoding="utf-8")
    (output / "unblind-mapping.json").write_text(json.dumps(mapping, ensure_ascii=False, indent=2), encoding="utf-8")
    with (output / "human-ratings.csv").open("x", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["case", "variant", "seconds_to_understand", "missed_important_facts",
                         "unsupported_assertions", "usefulness_1_to_5", "notes"])
        for case in cases:
            for variant in ("A", "B"):
                writer.writerow([case["id"], variant, "", "", "", "", ""])
    protocol = dict(seed=seed, cases=len(cases), selection="Random episode sample, including failed/abstained reviews",
                    limitations=["Layout can reveal the method; variant labels hide model names only.",
                                 "Human usefulness is not inferred from matching numeric assertions.",
                                 "No automatic trading-profit or statistical-power claim."], model_calls=0)
    (output / "protocol.json").write_text(json.dumps(protocol, indent=2), encoding="utf-8")
    return protocol


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()
    if not 1 <= args.limit <= 1000:
        parser.error("limit must be 1..1000")
    print(json.dumps(export(args.journal, args.output, args.limit)))


if __name__ == "__main__":
    main()

"""Preserve a contiguous recorded prefix without claiming a complete archive."""
import argparse
import gzip
import hashlib
import json
import zlib
from pathlib import Path


def recover(source, destination):
    source, destination = Path(source).resolve(), Path(destination).resolve()
    if not source.is_dir() or destination == source or source in destination.parents:
        raise ValueError("Recovery requires a source directory and a separate new destination")
    if (source / "manifest.json").exists():
        raise ValueError("A finalized manifest already exists; use normal verification")
    files = sorted(p for p in source.iterdir() if p.name.endswith((".ndjson.gz", ".partial")))
    records, sequence, limitation = [], 0, "Unclean shutdown; completeness and original segment integrity unknown"
    stopped = False
    for path in files:
        try:
            content = path.read_bytes()
            if path.name.endswith(".gz"):
                content = gzip.decompress(content)
        except (OSError, EOFError, zlib.error):
            limitation += "; unreadable segment"
            break
        for line in content.splitlines(keepends=True):
            try:
                if not line.endswith(b"\n"):
                    raise ValueError("truncated record")
                record = json.loads(line)
                if (record.get("schema_version") != 1 or record.get("sequence") != sequence + 1
                        or record.get("run") != source.name
                        or not {"kind", "payload", "received_at", "run", "epoch"} <= record.keys()):
                    raise ValueError("invalid sequence or envelope")
            except (ValueError, TypeError, AttributeError):
                limitation += "; stopped at first damaged or missing record"
                stopped = True
                break
            sequence += 1
            records.append(line)
        if stopped:
            break
    if not records:
        raise ValueError("No contiguous records to recover")
    destination.mkdir(parents=True, exist_ok=False)
    packed = gzip.compress(b"".join(records), mtime=0)
    name = "000000.ndjson.gz"
    (destination / name).write_bytes(packed)
    manifest = dict(schema_version=1, complete=False, recovered=True, source=str(source),
                    run=source.name, accepted=sequence, written=sequence, dropped=None, closed=True,
                    limitation=limitation, segments=[dict(file=name, records=sequence,
                    sha256=hashlib.sha256(packed).hexdigest())])
    (destination / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    args = parser.parse_args()
    result = recover(args.source, args.destination)
    print(json.dumps(result))


if __name__ == "__main__":
    main()

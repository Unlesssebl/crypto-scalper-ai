"""Bounded raw retention. Unsealed, audit-pinned and recovery inputs are protected."""
from __future__ import annotations

import shutil
from pathlib import Path

from .raw_archive import RawArchive


def prune_raw(root, gaps, now_ms, *, max_bytes=5 * 1024**3, max_age_ms=48 * 3600_000,
              min_free_bytes=512 * 1024**2):
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    segments = [r for venue in ("hyperliquid", "okx")
                for r in RawArchive(root, venue, read_only=True).segments()]
    total = sum(r.bytes for r in segments)
    protected, removed = [], []
    for record in sorted(segments, key=lambda r: r.last_receive_ms):
        path = Path(record.path).resolve()
        if root not in path.parents or path.parent.name != record.venue:
            raise ValueError("raw path escaped configured root")
        pinned = (not record.sealed or path.with_suffix(path.suffix + ".pin").exists()
                  or any(g.venue == record.venue and g.state in {"gap_detected", "recovering"}
                         and g.start_ms - 60_000 <= record.last_receive_ms
                         and (g.end_ms is None or g.end_ms + 60_000 >= record.first_receive_ms) for g in gaps))
        if pinned:
            protected.append(path.name)
            continue
        if now_ms - record.last_receive_ms >= max_age_ms or total > max_bytes:
            path.unlink()
            path.with_suffix(path.suffix + ".sha256").unlink(missing_ok=True)
            total -= record.bytes
            removed.append(path.name)
    free = shutil.disk_usage(root).free
    blocked = total >= max_bytes or free < min_free_bytes
    return {"status": "blocked" if blocked else "ok", "bytes": total, "budget_bytes": max_bytes,
            "free_bytes": free, "protected_segments": len(protected), "removed_segments": removed,
            "reason": "protected_raw_budget_or_disk_reserve" if blocked else None}

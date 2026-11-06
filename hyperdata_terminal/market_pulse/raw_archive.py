"""Append-only raw archive with rotation, checksums and readable-prefix reads.

Raw messages are written before normalization.  Records are one JSON object per
line; the current segment may end mid-line after a hard crash, in which case the
reader keeps the valid prefix and reports the truncation instead of discarding
the whole segment.  A sealed segment gets a ``.sha256`` sidecar.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
import uuid
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping

from .storage import RawSegmentRecord

FORMAT = "market-pulse.raw.v1"


@dataclass(frozen=True, slots=True)
class RawReference:
    segment: str
    offset: int

    def __str__(self) -> str:
        return f"{self.segment}:{self.offset}"


class RawArchive:
    def __init__(
        self,
        root: str | os.PathLike[str],
        venue: str,
        *,
        max_bytes: int = 5 * 1024**3,
        max_age_ms: int = 48 * 60 * 60 * 1_000,
        fsync: bool = False,
        read_only: bool = False,
    ) -> None:
        self.root = Path(root) / venue
        self.venue = venue
        self.max_bytes = max_bytes
        self.max_age_ms = max_age_ms
        self.fsync = fsync
        self.read_only = read_only
        self.root.mkdir(parents=True, exist_ok=True)
        self._handle = None
        self._path: Path | None = None
        self._created_at_ms = 0
        self._size = 0
        self._counter = 0
        self._first_receive_ms: int | None = None
        self._last_receive_ms: int | None = None
        self.last_error: str | None = None
        self._space_checked_at = 0.0
        self._space_ok = True
        if not read_only:
            self._open_new(now_ms=_now_ms())

    def _open_new(self, *, now_ms: int) -> None:
        self.seal()
        now_ms = max(now_ms, self._created_at_ms + 1)
        stamp = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        self._counter += 1
        name = f"{self.venue}-{stamp}-{self._counter:04d}-{uuid.uuid4().hex[:8]}.jsonl"
        self._path = self.root / name
        self._handle = self._path.open("xb")
        self._created_at_ms = now_ms
        self._first_receive_ms = None
        self._last_receive_ms = None
        header = json.dumps(
            {"format": FORMAT, "venue": self.venue, "created_at_ms": now_ms},
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        self._handle.write(header + b"\n")
        self._handle.flush()
        self._size = len(header) + 1

    def append(self, payload: Mapping[str, Any], *, receive_time_ms: int, source_epoch: str) -> RawReference:
        if self.read_only or self._handle is None:
            raise RuntimeError("raw archive is read-only or closed")
        if time.monotonic() - self._space_checked_at >= 5:
            self._space_checked_at = time.monotonic()
            venue_budget = int(os.getenv("PULSE_RAW_VENUE_BUDGET_BYTES", str(5 * 1024**3 // 2)))
            reserve = int(os.getenv("PULSE_RAW_MIN_FREE_BYTES", str(512 * 1024**2)))
            self._space_ok = (shutil.disk_usage(self.root).free >= reserve
                             and sum(p.stat().st_size for p in self.root.glob("*.jsonl")) < venue_budget)
        if not self._space_ok:
            raise OSError("raw storage budget exhausted; collection interrupted explicitly")
        envelope = {
            "receive_time_ms": receive_time_ms,
            "source_epoch": source_epoch,
            "payload": payload,
        }
        encoded = json.dumps(envelope, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode()
        record = {
            "offset": self._size,
            "crc32": f"{zlib.crc32(encoded) & 0xFFFFFFFF:08x}",
            "payload": envelope,
        }
        line = json.dumps(record, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode() + b"\n"
        if self._size > 0 and self._size + len(line) > self.max_bytes:
            self._open_new(now_ms=receive_time_ms)
        assert self._handle is not None and self._path is not None
        offset = self._size
        self._handle.write(line)
        self._handle.flush()
        if self.fsync:
            os.fsync(self._handle.fileno())
        self._size += len(line)
        self._last_receive_ms = receive_time_ms
        if self._first_receive_ms is None:
            self._first_receive_ms = receive_time_ms
        reference = RawReference(self._path.name, offset)
        if receive_time_ms - self._created_at_ms >= min(self.max_age_ms, 300_000):
            self._open_new(now_ms=receive_time_ms)
        return reference

    def seal(self) -> RawSegmentRecord | None:
        if self._handle is None or self._path is None:
            return None
        self._handle.flush()
        if self.fsync:
            os.fsync(self._handle.fileno())
        self._handle.close()
        record = _segment_record(
            self.venue,
            self._path,
            first_receive_ms=self._first_receive_ms or self._created_at_ms,
            last_receive_ms=self._last_receive_ms or self._created_at_ms,
            created_at_ms=self._created_at_ms,
        )
        _write_checksum(self._path)
        self._path = None
        self._handle = None
        return record

    def close(self) -> None:
        self.seal()

    def segments(self) -> list[RawSegmentRecord]:
        rows = self._file_segments()
        if self._path is not None:
            current = _segment_record(
                self.venue,
                self._path,
                first_receive_ms=self._first_receive_ms or self._created_at_ms,
                last_receive_ms=self._last_receive_ms or self._created_at_ms,
                created_at_ms=self._created_at_ms,
            )
            rows = [item for item in rows if item.path != current.path] + [current]
        return rows

    def _file_segments(self) -> list[RawSegmentRecord]:
        rows: list[RawSegmentRecord] = []
        for path in sorted(self.root.glob("*.jsonl")):
            try:
                stat = path.stat()
            except OSError:
                continue
            created_at_ms = int(stat.st_mtime * 1_000)
            try:
                with path.open("rb") as handle:
                    header = json.loads(handle.readline())
                created_at_ms = int(header.get("created_at_ms", created_at_ms))
            except (OSError, json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError, AttributeError):
                pass
            sidecar = path.with_suffix(path.suffix + ".sha256")
            sha256 = None
            if sidecar.exists():
                try:
                    sha256 = sidecar.read_text(encoding="ascii").strip().split()[0] or None
                except (OSError, UnicodeDecodeError, IndexError):
                    sha256 = None
            rows.append(
                RawSegmentRecord(
                    venue=self.venue,
                    path=str(path),
                    first_receive_ms=created_at_ms,
                    last_receive_ms=int(stat.st_mtime * 1_000),
                    bytes=stat.st_size,
                    sha256=sha256,
                    sealed=sidecar.exists(),
                    created_at_ms=created_at_ms,
                )
            )
        return rows

    def iter_records(
        self,
        *,
        start_ms: int | None = None,
        end_ms: int | None = None,
        segment: str | None = None,
    ) -> Iterator[tuple[Mapping[str, Any], RawReference]]:
        self.last_error = None
        paths = sorted(self.root.glob("*.jsonl"))
        if segment is not None:
            paths = [path for path in paths if path.name == segment]
        for path in paths:
            checksum = path.with_suffix(path.suffix + ".sha256")
            if checksum.exists():
                expected = checksum.read_text(encoding="ascii").strip().split()[0]
                actual = _file_sha256(path)
                if expected and actual != expected:
                    self.last_error = f"checksum mismatch for {path.name}"
                    continue
            yield from self._iter_path(path, start_ms=start_ms, end_ms=end_ms)
            if self.last_error:
                return

    def _iter_path(
        self,
        path: Path,
        *,
        start_ms: int | None,
        end_ms: int | None,
    ) -> Iterator[tuple[Mapping[str, Any], RawReference]]:
        with path.open("rb") as handle:
            header = handle.readline()
            try:
                parsed_header = json.loads(header)
            except (json.JSONDecodeError, UnicodeDecodeError):
                self.last_error = f"invalid header in {path.name}"
                return
            if parsed_header.get("format") != FORMAT:
                self.last_error = f"unsupported raw format in {path.name}"
                return
            while True:
                offset = handle.tell()
                line = handle.readline()
                if not line:
                    break
                if not line.endswith(b"\n"):
                    self.last_error = f"truncated record at {path.name}:{offset}"
                    return
                try:
                    record = json.loads(line)
                    envelope = record["payload"]
                    encoded = json.dumps(
                        envelope,
                        separators=(",", ":"),
                        sort_keys=True,
                        ensure_ascii=False,
                    ).encode()
                    actual = f"{zlib.crc32(encoded) & 0xFFFFFFFF:08x}"
                    if actual != record["crc32"]:
                        self.last_error = f"checksum mismatch at {path.name}:{offset}"
                        return
                except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError):
                    self.last_error = f"invalid record at {path.name}:{offset}"
                    return
                receive_time_ms = int(envelope.get("receive_time_ms", 0))
                if start_ms is not None and receive_time_ms < start_ms:
                    continue
                if end_ms is not None and receive_time_ms >= end_ms:
                    continue
                yield envelope, RawReference(path.name, offset)

    def prune(self, *, now_ms: int, pinned: set[str] | None = None) -> list[str]:
        if self.read_only:
            return []
        pinned = pinned or set()
        removed = []
        for record in self.segments():
            path = Path(record.path)
            if path.parent != self.root or record.sealed is False:
                continue
            if path.name in pinned:
                continue
            if now_ms - record.last_receive_ms < self.max_age_ms:
                continue
            path.unlink(missing_ok=True)
            path.with_suffix(path.suffix + ".sha256").unlink(missing_ok=True)
            removed.append(str(path))
        return removed


def _segment_record(
    venue: str,
    path: Path,
    *,
    first_receive_ms: int,
    last_receive_ms: int,
    created_at_ms: int,
) -> RawSegmentRecord:
    return RawSegmentRecord(
        venue=venue,
        path=str(path),
        first_receive_ms=first_receive_ms,
        last_receive_ms=last_receive_ms,
        bytes=path.stat().st_size if path.exists() else 0,
        sha256=None,
        sealed=False,
        created_at_ms=created_at_ms,
    )


def _write_checksum(path: Path) -> None:
    sidecar = path.with_suffix(path.suffix + ".sha256")
    sidecar.write_text(f"{_file_sha256(path)}  {path.name}\n", encoding="ascii")


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

"""Per-task stats store for trace_matrix.

Two modes, selected at construction:

* ``persistent=False`` (default): pure in-memory dict. Survives BLE reconnects
  inside a single bot process; lost on Ctrl-C / restart. No disk I/O.
* ``persistent=True``: also writes one JSON file per task under
  ``./logs/<task>.stats.json``, atomically (write-to-temp + rename), after
  every cycle summary. Auto-invalidated when the task's config fingerprint
  (``my_repeater`` + sorted ``others``) changes — old file is treated as
  stale and counters restart from zero.

The bot's CLI exposes this as ``-p/--persistence``; ``-r/--reset`` only
makes sense alongside ``--persistence`` (there'd be nothing on disk to wipe
otherwise).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

logger = logging.getLogger(__name__)

# Bump if file format changes incompatibly.
_FORMAT_VERSION = 1


@dataclass
class TaskStats:
    """Per-OTHER cumulative counters + per-task cycle counter. Mirrors what
    TraceMatrixTask._Stats holds in memory, plus the cycle index.
    """
    cycle: int = 0
    per_other: dict[str, dict] = field(default_factory=dict)

    def to_json(self, task_name: str, fingerprint: str) -> dict:
        return {
            "version": _FORMAT_VERSION,
            "task": task_name,
            "config_fingerprint": fingerprint,
            "cycle": self.cycle,
            "stats": self.per_other,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }


class StatsStore:
    """In-memory cache backed by a directory of per-task JSON files.

    Lifecycle: created once by the supervisor. Each (re)build of a task asks
    for ``load(task_name, fingerprint)`` — gets back a TaskStats (possibly
    empty if the file is missing or stale). Task mutates the returned object
    directly; every ``save(task_name)`` writes it to disk atomically.
    """

    def __init__(self, persistent: bool = False, base_dir: str | Path = "./logs") -> None:
        self._persistent = persistent
        self._base = Path(base_dir)
        if self._persistent:
            self._base.mkdir(parents=True, exist_ok=True)
        # task_name → TaskStats (latest in-memory copy).
        self._cache: dict[str, TaskStats] = {}
        # task_name → fingerprint (so save() knows what to write).
        self._fingerprints: dict[str, str] = {}

    @property
    def persistent(self) -> bool:
        return self._persistent

    def _file(self, task_name: str) -> Path:
        # Sanitize task name for filesystem: replace os.sep just in case.
        safe = task_name.replace(os.sep, "_").replace("/", "_")
        return self._base / f"{safe}.stats.json"

    def load(self, task_name: str, fingerprint: str) -> TaskStats:
        """Return cached TaskStats; fall back to disk (persistent mode); fall back to empty.

        If the on-disk file's fingerprint differs from ``fingerprint`` (config
        changed), the file is ignored and a fresh empty TaskStats is returned.
        The new fingerprint is recorded for subsequent saves.
        """
        self._fingerprints[task_name] = fingerprint
        if task_name in self._cache:
            return self._cache[task_name]

        if not self._persistent:
            # In-memory mode: nothing on disk to consult, start fresh.
            stats = TaskStats()
            self._cache[task_name] = stats
            return stats

        path = self._file(task_name)
        if not path.exists():
            logger.debug("stats: no file for %r, starting fresh", task_name)
            stats = TaskStats()
            self._cache[task_name] = stats
            return stats

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("stats: cannot read %s (%s) — starting fresh", path, e)
            stats = TaskStats()
            self._cache[task_name] = stats
            return stats

        on_disk_fp = data.get("config_fingerprint")
        if on_disk_fp != fingerprint:
            logger.warning(
                "stats: %s fingerprint changed (was %r, now %r) — discarding old stats",
                task_name, on_disk_fp, fingerprint,
            )
            stats = TaskStats()
            self._cache[task_name] = stats
            return stats

        if data.get("version") != _FORMAT_VERSION:
            logger.warning("stats: %s has unsupported version — discarding", path)
            stats = TaskStats()
            self._cache[task_name] = stats
            return stats

        stats = TaskStats(
            cycle=int(data.get("cycle", 0)),
            per_other=dict(data.get("stats", {})),
        )
        logger.info(
            "stats: resumed %r from %s (cycle=%d, %d OTHERs)",
            task_name, path, stats.cycle, len(stats.per_other),
        )
        self._cache[task_name] = stats
        return stats

    def save(self, task_name: str) -> None:
        """Atomically write the cached stats for ``task_name`` to disk (persistent mode only)."""
        if not self._persistent:
            return  # in-memory mode — cache already updated by caller, nothing to write
        stats = self._cache.get(task_name)
        fingerprint = self._fingerprints.get(task_name)
        if stats is None or fingerprint is None:
            logger.debug("stats: nothing to save for %r", task_name)
            return

        path = self._file(task_name)
        payload = stats.to_json(task_name, fingerprint)
        try:
            # Atomic: write to .tmp in same dir, then os.replace().
            fd, tmp_path = tempfile.mkstemp(
                prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent),
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                os.replace(tmp_path, path)
            except Exception:
                # Clean up the tmp file if rename failed.
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
        except OSError as e:
            logger.warning("stats: could not save %s: %s", path, e)

    def reset(self, task_names: Iterable[str] | None = None) -> int:
        """Clear the in-memory cache and (in persistent mode) delete on-disk files.

        ``task_names=None`` wipes everything — that's the CLI ``--reset`` semantics.
        Returns the number of files actually removed (always 0 in in-memory mode).
        """
        self._cache.clear()
        if not self._persistent:
            return 0
        if task_names is None:
            paths = list(self._base.glob("*.stats.json"))
        else:
            paths = [self._file(n) for n in task_names]
        removed = 0
        for p in paths:
            try:
                p.unlink()
                removed += 1
            except FileNotFoundError:
                pass
            except OSError as e:
                logger.warning("stats: could not delete %s: %s", p, e)
        return removed

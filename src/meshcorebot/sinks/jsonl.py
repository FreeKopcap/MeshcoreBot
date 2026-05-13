"""Append-only JSONL sink with daily rotation."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

from ..config import JsonlSink as JsonlCfg
from .base import Record, Sink

logger = logging.getLogger(__name__)


class JsonlSinkImpl(Sink):
    name = "jsonl"

    def __init__(self, cfg: JsonlCfg) -> None:
        self._cfg = cfg
        self._fh: TextIO | None = None
        self._current_path: Path | None = None

    def _resolve(self) -> Path:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        return Path(self._cfg.path.format(date=date))

    def _open_if_rotated(self) -> None:
        target = self._resolve()
        if target == self._current_path and self._fh is not None:
            return
        # rotate
        if self._fh is not None:
            self._fh.close()
        target.parent.mkdir(parents=True, exist_ok=True)
        self._fh = target.open("a", encoding="utf-8")
        self._current_path = target

    async def start(self) -> None:
        self._open_if_rotated()
        logger.info("jsonl sink → %s", self._current_path)

    async def stop(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    async def write(self, record: Record) -> None:
        if self._cfg.events is not None and record.event not in self._cfg.events:
            return
        self._open_if_rotated()
        assert self._fh is not None
        self._fh.write(json.dumps(record.to_dict(), ensure_ascii=False) + "\n")
        self._fh.flush()

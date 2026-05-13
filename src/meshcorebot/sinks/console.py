"""Human-readable console output."""

from __future__ import annotations

import sys

from ..config import ConsoleSink as ConsoleCfg
from .base import Record, Sink

_RESET = "\033[0m"
_DIM = "\033[2m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_CYAN = "\033[36m"


def _color_for(event: str) -> str:
    if event.endswith("_error") or event.endswith("_timeout"):
        return _RED
    if event.endswith("_sent") or event == "trace_data":
        return _GREEN
    if event == "status":
        return _CYAN
    return _YELLOW


class ConsoleSinkImpl(Sink):
    name = "console"

    def __init__(self, cfg: ConsoleCfg) -> None:
        self._color = cfg.color and sys.stdout.isatty()

    async def write(self, record: Record) -> None:
        ts = record.ts.split("T", 1)[1].rstrip("Z+0:")[:8]  # HH:MM:SS
        head = f"[{ts}] {record.task} {record.event}"
        body = " ".join(f"{k}={v!r}" for k, v in record.data.items())
        if self._color:
            c = _color_for(record.event)
            line = f"{_DIM}[{ts}]{_RESET} {_CYAN}{record.task}{_RESET} {c}{record.event}{_RESET}"
            if body:
                line += f" {body}"
        else:
            line = head + (f" {body}" if body else "")
        print(line, flush=True)

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
    if event.endswith("_summary"):
        return _CYAN
    return _YELLOW


# Fields rendered specially / suppressed from the inline body.
# `text` → printed as an indented multi-line block below the header.
# `rows` → structured data for jsonl/mqtt; too verbose for console.
_INLINE_SUPPRESS = {"text", "rows"}


class ConsoleSinkImpl(Sink):
    name = "console"

    def __init__(self, cfg: ConsoleCfg) -> None:
        self._color = cfg.color and sys.stdout.isatty()

    async def write(self, record: Record) -> None:
        ts = record.ts.split("T", 1)[1].rstrip("Z+0:")[:8]  # HH:MM:SS
        text_block = record.data.get("text")
        body = " ".join(
            f"{k}={v!r}" for k, v in record.data.items() if k not in _INLINE_SUPPRESS
        )
        head = f"[{ts}] {record.task} {record.event}"
        if self._color:
            c = _color_for(record.event)
            line = f"{_DIM}[{ts}]{_RESET} {_CYAN}{record.task}{_RESET} {c}{record.event}{_RESET}"
            if body:
                line += f" {body}"
        else:
            line = head + (f" {body}" if body else "")
        print(line, flush=True)
        if isinstance(text_block, str) and text_block:
            for ln in text_block.splitlines():
                print(f"  {ln}", flush=True)

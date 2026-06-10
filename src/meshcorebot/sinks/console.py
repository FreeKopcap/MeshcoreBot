"""Human-readable console output."""

from __future__ import annotations

import sys
from datetime import datetime

from ..config import ConsoleSink as ConsoleCfg
from .base import Record, Sink


def _hhmmss(iso_ts: str) -> str:
    """Extract HH:MM:SS (local-tz) from an ISO 8601 timestamp string.

    Robust against trailing zeros — earlier ``rstrip("Z+0:")`` ate seconds
    that happened to end in 0 (rendering '14:37:20' as '14:37:2').
    """
    try:
        return datetime.fromisoformat(iso_ts).strftime("%H:%M:%S")
    except ValueError:
        # Fallback if the string is somehow malformed; show what we can.
        return iso_ts.split("T", 1)[-1][:8]

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
        ts = _hhmmss(record.ts)
        text_block = record.data.get("text")

        # Compact header for trace_cycle_summary: "[ts] task (cycle N)" or
        # "(FINAL cycle N)" — folds the otherwise-redundant title from the
        # text block into the standard header line.
        if record.event == "trace_cycle_summary":
            cycle = record.data.get("cycle", "?")
            final = record.data.get("final", False)
            label = f"FINAL cycle {cycle}" if final else f"cycle {cycle}"
            if self._color:
                c = _color_for(record.event)
                line = f"{_DIM}[{ts}]{_RESET} {_CYAN}{record.task}{_RESET} {c}({label}){_RESET}"
            else:
                line = f"[{ts}] {record.task} ({label})"
            print(line, flush=True)
            if isinstance(text_block, str) and text_block:
                for ln in text_block.splitlines():
                    print(f"  {ln}", flush=True)
            return

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

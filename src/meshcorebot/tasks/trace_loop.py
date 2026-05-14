"""Periodic trace task — round-robin (or random) through a list of paths.

Each cycle: pick the next path, ``send_trace(path=...)`` and wait for the
``TRACE_DATA`` event filtered by the returned tag. Times out per task config.
"""

from __future__ import annotations

import asyncio
import itertools
import logging
import random
from typing import Iterator

from meshcore import EventType

from ..config import TraceLoop, TracePath
from .base import BaseTask, safe_sleep

logger = logging.getLogger(__name__)


def _flatten_trace_payload(payload: dict) -> dict:
    """Normalize TRACE_DATA dict for the sinks — path becomes a JSON-friendly list."""
    out = {
        "tag": payload.get("tag"),
        "auth": payload.get("auth"),
        "flags": payload.get("flags"),
        "path_len": payload.get("path_len"),
    }
    nodes = payload.get("path") or []
    # nodes is a list of {hash: bytes|str, snr: float}
    flat = []
    for n in nodes:
        h = n.get("hash")
        if isinstance(h, (bytes, bytearray)):
            h = h.hex()
        flat.append({"hash": h, "snr": n.get("snr")})
    out["path"] = flat
    if "final_snr" in payload:
        out["final_snr"] = payload["final_snr"]
    return out


class TraceLoopTask(BaseTask):
    cfg: TraceLoop

    def _iter(self) -> Iterator[TracePath]:
        if self.cfg.mode == "round_robin":
            return itertools.cycle(self.cfg.paths)
        # random: a generator that yields a fresh random pick each time
        def gen() -> Iterator[TracePath]:
            while True:
                yield random.choice(self.cfg.paths)
        return gen()

    async def run(self, mc) -> None:
        await self.emit(
            "trace_loop_ready",
            mode=self.cfg.mode,
            interval_sec=self.cfg.interval,
            timeout_sec=self.cfg.timeout,
            paths=[p.name for p in self.cfg.paths],
        )

        it = self._iter()
        cycle = 0
        while True:
            cycle += 1
            route = next(it)
            # See trace_matrix._do_one_trace: lib generates a tag if not given
            # but doesn't echo it back, so we own the tag ourselves.
            tag = random.randint(1, 0xFFFFFFFF)
            send_kwargs = {"auth_code": self.cfg.auth_code, "tag": tag, "path": route.path}
            if self.cfg.flags is not None:
                send_kwargs["flags"] = self.cfg.flags
            send = await mc.commands.send_trace(**send_kwargs)
            if send.type == EventType.ERROR:
                await self.emit(
                    "trace_send_error", cycle=cycle, route=route.name,
                    path=route.path, tag=tag, error=str(send.payload),
                )
                await safe_sleep(self.cfg.interval)
                continue

            est_timeout_ms = send.payload.get("est_timeout", 0) if isinstance(send.payload, dict) else 0
            await self.emit(
                "trace_sent", cycle=cycle, route=route.name, path=route.path,
                tag=tag, est_timeout_ms=est_timeout_ms,
                tag_field=route.tag_field,
            )

            try:
                ev = await mc.wait_for_event(
                    EventType.TRACE_DATA,
                    attribute_filters={"tag": tag},
                    timeout=self.cfg.timeout,
                )
            except asyncio.TimeoutError:
                ev = None

            if ev is None:
                await self.emit(
                    "trace_timeout", cycle=cycle, route=route.name, path=route.path,
                    tag=tag, timeout_sec=self.cfg.timeout, tag_field=route.tag_field,
                )
            else:
                flat = _flatten_trace_payload(ev.payload)
                await self.emit(
                    "trace_data", cycle=cycle, route=route.name, path=route.path,
                    tag_field=route.tag_field, **flat,
                )

            await safe_sleep(self.cfg.interval)

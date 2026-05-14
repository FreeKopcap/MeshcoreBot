"""Cycle-based trace probing through MY repeater to a list of OTHER repeaters.

Each cycle: for each OTHER, send a trace ``MY,OTHER,MY`` (or longer in the
future), wait for ``TRACE_DATA`` filtered by the returned tag, record per-OTHER
stats (forward SNR at OTHER, return SNR at MY, success/attempts). At the end
of each cycle emit ``trace_cycle_summary`` with both structured rows and a
pretty text table. On normal completion (``cycles`` reached) the last summary
is marked ``final``. On Ctrl-C we cannot reliably round-trip the async sinks,
so the final table is also printed to stdout synchronously.

MY-end width follows OTHER's width: for OTHER ``"5A94"`` we use
``my_repeater[:4]``; for ``"5A"`` we use ``my_repeater[:2]``. Path validation
catches the case where ``my_repeater`` is too short.
"""

from __future__ import annotations

import asyncio
import logging
import random
import sys
from dataclasses import dataclass, field

from meshcore import EventType

from ..config import TraceMatrix
from .base import BaseTask, safe_sleep
from .trace_loop import _flatten_trace_payload

logger = logging.getLogger(__name__)


@dataclass
class _Stats:
    """Cumulative per-OTHER counters across the task's lifetime."""
    success: int = 0
    timeout: int = 0
    error: int = 0
    sum_fwd: float = 0.0      # SNR sum at OTHER (heard MY going out)
    cnt_fwd: int = 0
    sum_rev: float = 0.0      # SNR sum at MY (heard OTHER on return)
    cnt_rev: int = 0
    last_snrs: list = field(default_factory=list)   # full hop SNR list of last successful trace

    @property
    def attempts(self) -> int:
        return self.success + self.timeout + self.error

    @property
    def fwd_avg(self) -> float | None:
        return (self.sum_fwd / self.cnt_fwd) if self.cnt_fwd else None

    @property
    def rev_avg(self) -> float | None:
        return (self.sum_rev / self.cnt_rev) if self.cnt_rev else None


def _format_table(task_name: str, cycle: int, final: bool, others: list[str],
                  stats: dict[str, _Stats]) -> tuple[str, list[dict]]:
    """Render the analyzer-style summary table.

    Returns (text_block, structured_rows). Rows are also returned for JSONL/MQTT.
    """
    title = f"{task_name} — cycle {cycle}" + (" (FINAL)" if final else " (cumulative)")
    header = f"{'Test':<10} {'SNR→':>7} {'SNR←':>7} {'Trace':>9}"
    sep = "-" * len(header)
    lines = [title, header, sep]
    rows: list[dict] = []
    for o in others:
        s = stats[o]
        f = f"{s.fwd_avg:>7.2f}" if s.fwd_avg is not None else f"{'-':>7}"
        r = f"{s.rev_avg:>7.2f}" if s.rev_avg is not None else f"{'-':>7}"
        trc = f"{s.success}/{s.attempts}"
        lines.append(f"{o.upper():<10} {f} {r} {trc:>9}")
        rows.append({
            "test": o,
            "snr_fwd": s.fwd_avg,
            "snr_rev": s.rev_avg,
            "success": s.success,
            "timeout": s.timeout,
            "error": s.error,
            "attempts": s.attempts,
        })
    return "\n".join(lines), rows


class TraceMatrixTask(BaseTask):
    cfg: TraceMatrix

    def __init__(self, cfg, device_name, sinks) -> None:
        super().__init__(cfg, device_name, sinks)
        self._stats: dict[str, _Stats] = {o: _Stats() for o in cfg.others}
        self._cycle: int = 0

    def _build_path(self, other: str) -> str:
        width = len(other)
        my_end = self.cfg.my_repeater[:width]
        # current shape: MY,OTHER,MY (single-OTHER round-trip).
        # When richer multi-OTHER paths are added, this is the place to extend.
        return f"{my_end},{other},{my_end}"

    async def run(self, mc) -> None:
        await self.emit(
            "trace_matrix_ready",
            my_repeater=self.cfg.my_repeater,
            others=list(self.cfg.others),
            cycles=self.cfg.cycles,
            cycle_interval_sec=self.cfg.cycle_interval,
            trace_delay_sec=self.cfg.trace_delay,
            timeout_sec=self.cfg.timeout,
        )

        try:
            while self.cfg.cycles == 0 or self._cycle < self.cfg.cycles:
                self._cycle += 1
                await self._run_cycle(mc, self._cycle)
                done = self.cfg.cycles != 0 and self._cycle >= self.cfg.cycles
                await self._emit_summary(self._cycle, final=done)
                if done:
                    return
                await safe_sleep(self.cfg.cycle_interval)
        except asyncio.CancelledError:
            # Best-effort: print the cumulative table to stdout synchronously.
            # Async sinks are likely shutting down, so we don't try to await them.
            text, _ = _format_table(self.name, self._cycle, final=True,
                                    others=list(self.cfg.others), stats=self._stats)
            print("\n" + text, file=sys.stderr, flush=True)
            raise

    async def _run_cycle(self, mc, cycle: int) -> None:
        for i, other in enumerate(self.cfg.others):
            if i > 0:
                await safe_sleep(self.cfg.trace_delay)
            await self._do_one_trace(mc, cycle, other)

    async def _do_one_trace(self, mc, cycle: int, other: str) -> None:
        path = self._build_path(other)
        # meshcore lib generates a random tag if not supplied but does NOT
        # echo it back in the returned Event payload — so we generate it
        # ourselves to correlate with the eventual TRACE_DATA event.
        tag = random.randint(1, 0xFFFFFFFF)
        send_kwargs = {"auth_code": self.cfg.auth_code, "tag": tag, "path": path}
        if self.cfg.flags is not None:
            send_kwargs["flags"] = self.cfg.flags
        send = await mc.commands.send_trace(**send_kwargs)
        if send.type == EventType.ERROR:
            self._stats[other].error += 1
            await self.emit(
                "trace_send_error", cycle=cycle, other=other, path=path,
                tag=tag, error=str(send.payload),
            )
            return

        est_timeout_ms = send.payload.get("est_timeout", 0) if isinstance(send.payload, dict) else 0
        await self.emit(
            "trace_sent", cycle=cycle, other=other, path=path,
            tag=tag, est_timeout_ms=est_timeout_ms,
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
            self._stats[other].timeout += 1
            await self.emit(
                "trace_timeout", cycle=cycle, other=other, path=path,
                tag=tag, timeout_sec=self.cfg.timeout,
            )
            return

        flat = _flatten_trace_payload(ev.payload)
        hops = flat.get("path") or []
        # The lib's hops list trails one extra record with hash=None — that's our
        # companion node's own reception of the return packet (useful as a
        # "companion ↔ MY return-link SNR", but NOT the OTHER-side SNR we want).
        # Strip null-hash hops; the remainder is "who-recorded" the per-link SNR.
        real_hops = [h for h in hops if h.get("hash")]
        snrs = [h.get("snr") for h in real_hops]
        # For path MY, …, OTHER, …, MY with N entries:
        #   real_hops[0]  — MY heard us going out (companion → MY)
        #   real_hops[1]  — OTHER heard MY        (SNR→ at the first OTHER)
        #   real_hops[-1] — MY heard OTHER return (SNR← at MY from last OTHER)
        snr_fwd = snrs[1] if len(snrs) > 1 else None
        snr_rev = snrs[-1] if len(snrs) > 2 else None
        # Keep the trailing companion-side return SNR available for jsonl/mqtt consumers.
        companion_rx_snr = next((h.get("snr") for h in hops if not h.get("hash")), None)

        s = self._stats[other]
        s.success += 1
        s.last_snrs = [x for x in snrs if x is not None]
        if isinstance(snr_fwd, (int, float)):
            s.sum_fwd += float(snr_fwd); s.cnt_fwd += 1
        if isinstance(snr_rev, (int, float)):
            s.sum_rev += float(snr_rev); s.cnt_rev += 1

        # _flatten_trace_payload uses key "path" for the hops list — we already
        # use `path` for the route string we sent, so rename to `hops` on the wire.
        hops_flat = flat.pop("path", None)
        await self.emit(
            "trace_data", cycle=cycle, other=other, path=path,
            snr_fwd=snr_fwd, snr_rev=snr_rev, snrs=snrs,
            companion_rx_snr=companion_rx_snr,
            hops=hops_flat, **flat,
        )

    async def _emit_summary(self, cycle: int, final: bool) -> None:
        text, rows = _format_table(self.name, cycle, final,
                                   list(self.cfg.others), self._stats)
        await self.emit(
            "trace_cycle_summary", cycle=cycle, final=final,
            rows=rows, text=text,
        )

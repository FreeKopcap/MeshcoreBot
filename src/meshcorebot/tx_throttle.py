"""Cooperative async throttle for trace TX events.

Shared across all trace_matrix tasks running under one supervise loop. Ensures
a minimum time gap between successive ``send_trace`` BLE calls — useful when
two tasks issue traces concurrently on the same companion and you want the
LoRa side to have a quiet window between TXs so the repeater can finish its
relay + flip back to RX before the next packet hits the air.

The gate is purely a sleep before BLE-send: it doesn't track on-air time
(firmware does that). Granularity is therefore approximate — actual on-air
spacing also depends on firmware queue + CSMA back-off — but the floor is
respected at the application layer.
"""

from __future__ import annotations

import asyncio
import math


class TxThrottle:
    """Minimum-gap throttle over BLE send_trace events."""

    def __init__(self, min_gap_sec: float) -> None:
        if min_gap_sec <= 0:
            raise ValueError(f"min_gap_sec must be > 0, got {min_gap_sec}")
        self._min_gap = float(min_gap_sec)
        self._lock = asyncio.Lock()
        # Sentinel: first call to gate() should never wait.
        self._last_tx = -math.inf

    @property
    def min_gap(self) -> float:
        return self._min_gap

    async def gate(self) -> None:
        """Acquire the throttle, sleep enough to honor min_gap, return.

        The lock serializes the check+update; the actual send_trace call
        happens OUTSIDE this method, so multiple tasks can have ongoing BLE
        sends in flight as long as they entered the gate at least
        ``min_gap`` seconds apart.
        """
        loop = asyncio.get_event_loop()
        async with self._lock:
            now = loop.time()
            elapsed = now - self._last_tx
            if elapsed < self._min_gap:
                await asyncio.sleep(self._min_gap - elapsed)
            self._last_tx = loop.time()

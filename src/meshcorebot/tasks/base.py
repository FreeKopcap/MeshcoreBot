"""Task base class + dispatcher to concrete implementations."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from ..config import ChannelMessage, Task, TraceLoop, TraceMatrix

if TYPE_CHECKING:
    from meshcore import MeshCore

    from ..sinks.base import Fanout

logger = logging.getLogger(__name__)


class BaseTask:
    """One declarative task = one infinite asyncio coroutine."""

    def __init__(self, cfg, device_name: str, sinks: "Fanout") -> None:
        self.cfg = cfg
        self.device = device_name
        self.sinks = sinks
        self.name: str = cfg.name

    async def run(self, mc: "MeshCore") -> None:
        """Override in subclasses. Loop forever; let CancelledError propagate."""
        raise NotImplementedError

    async def emit(self, event: str, **data) -> None:
        from ..sinks.base import Record  # late import to avoid cycle
        await self.sinks.write(Record(event=event, task=self.name, device=self.device, data=data))


def build_task(cfg: Task, device_name: str, sinks: "Fanout") -> BaseTask:
    """Map a config task variant to its implementation."""
    from .chan_msg import ChanMsgTask
    from .trace_loop import TraceLoopTask
    from .trace_matrix import TraceMatrixTask

    if isinstance(cfg, ChannelMessage):
        return ChanMsgTask(cfg, device_name, sinks)
    if isinstance(cfg, TraceLoop):
        return TraceLoopTask(cfg, device_name, sinks)
    if isinstance(cfg, TraceMatrix):
        return TraceMatrixTask(cfg, device_name, sinks)
    raise TypeError(f"unknown task type: {type(cfg).__name__}")


async def safe_sleep(seconds: float) -> None:
    """asyncio.sleep that's safe to call with non-positive durations."""
    if seconds > 0:
        await asyncio.sleep(seconds)

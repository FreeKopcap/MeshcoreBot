"""Sink interface and fan-out aggregator.

A `Record` is the canonical event shape produced by tasks. Sinks decide how to
render it; the aggregator just forwards each record to every enabled sink.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..config import Sinks as SinksCfg

logger = logging.getLogger(__name__)


@dataclass
class Record:
    """One observable event from a task. Keep keys stable — JSONL+MQTT both consume it."""
    event: str                            # e.g. "trace_data" | "trace_timeout" | "chan_msg_sent"
    task: str                             # task name from config
    device: str                           # bot.device_name
    ts: str = field(                       # ISO-8601 in the LOCAL timezone
        default_factory=lambda: datetime.now().astimezone().isoformat(timespec="seconds")
    )
    data: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        # flatten data into root so consumers see flat JSON
        data = d.pop("data") or {}
        d.update(data)
        return d


class Sink:
    """Sinks live for the bot's lifetime. `write` may be sync; it must not block long."""
    name: str = "sink"

    async def start(self) -> None:
        """Acquire resources (open files, connect to broker, …)."""

    async def stop(self) -> None:
        """Release resources. Best-effort; should not raise on already-closed."""

    async def write(self, record: Record) -> None:  # pragma: no cover — abstract
        raise NotImplementedError


class Fanout:
    """Holds N sinks; broadcasts every record to all of them. Per-sink errors are logged, not raised."""

    def __init__(self, sinks: list[Sink]) -> None:
        self._sinks = sinks

    async def start(self) -> None:
        for s in self._sinks:
            try:
                await s.start()
            except Exception as e:  # noqa: BLE001
                logger.error("sink %s start failed: %s", s.name, e)

    async def stop(self) -> None:
        for s in self._sinks:
            try:
                await s.stop()
            except Exception as e:  # noqa: BLE001
                logger.error("sink %s stop failed: %s", s.name, e)

    async def write(self, record: Record) -> None:
        for s in self._sinks:
            try:
                await s.write(record)
            except Exception as e:  # noqa: BLE001
                logger.error("sink %s write failed: %s", s.name, e)


def build_sinks(cfg: SinksCfg) -> Fanout:
    """Instantiate enabled sinks from config."""
    from .console import ConsoleSinkImpl
    from .jsonl import JsonlSinkImpl
    from .mqtt import MqttSinkImpl

    sinks: list[Sink] = []
    if cfg.console.enabled:
        sinks.append(ConsoleSinkImpl(cfg.console))
    if cfg.jsonl.enabled:
        sinks.append(JsonlSinkImpl(cfg.jsonl))
    if cfg.mqtt.enabled:
        sinks.append(MqttSinkImpl(cfg.mqtt))
    return Fanout(sinks)

"""YAML configuration model for MeshcoreBot.

Single source of truth for what the bot does. Validated with pydantic; durations
accept ``5m``/``30s``/``2h`` shorthand and normalize to seconds.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Literal, Union

import yaml
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator


# -- Duration -----------------------------------------------------------------

_DURATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(s|sec|m|min|h|hr|hour)?\s*$", re.I)
_DURATION_MULT = {
    None: 1.0, "s": 1.0, "sec": 1.0,
    "m": 60.0, "min": 60.0,
    "h": 3600.0, "hr": 3600.0, "hour": 3600.0,
}


def _parse_duration(value: object) -> float:
    """Accept ``"5m"`` / ``"30s"`` / ``"2h"`` / plain seconds. Returns seconds."""
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        raise TypeError(f"duration must be str|int|float, got {type(value).__name__}")
    m = _DURATION_RE.match(value)
    if not m:
        raise ValueError(f"bad duration {value!r}; expected like '5m', '30s', '2h'")
    n, unit = m.groups()
    return float(n) * _DURATION_MULT[unit.lower() if unit else None]


Duration = Annotated[float, BeforeValidator(_parse_duration)]


# -- Transport ----------------------------------------------------------------

class SerialTransport(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["serial"]
    port: str
    baudrate: int = 115200


class BleTransport(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["ble"]
    address: str | None = None  # MAC / UUID, or None → first MeshCore-* device
    pin: int | None = None
    scan_timeout: Duration = 5.0


Transport = Annotated[
    Union[SerialTransport, BleTransport],
    Field(discriminator="type"),
]


# -- Sinks --------------------------------------------------------------------

class ConsoleSink(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    color: bool = True


class JsonlSink(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    # {date} → YYYY-MM-DD UTC at time of write
    path: str = "./logs/meshcorebot-{date}.jsonl"
    events: list[str] | None = None  # filter; None → all


class MqttSink(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    broker: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    topic_prefix: str = "meshcore/bot"
    qos: int = 0
    retain: bool = False
    client_id: str | None = None
    keepalive: int = 60


class Sinks(BaseModel):
    model_config = ConfigDict(extra="forbid")
    console: ConsoleSink = ConsoleSink()
    jsonl: JsonlSink = JsonlSink()
    mqtt: MqttSink = MqttSink()


# -- Bot block ----------------------------------------------------------------

class BotSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    device_name: str = "meshcorebot"
    reconnect: bool = True
    reconnect_delay: Duration = 10.0


# -- Tasks --------------------------------------------------------------------

class ChannelMessage(BaseModel):
    """Periodic probe-style channel messages — covers the old meshcore-probe.py."""
    model_config = ConfigDict(extra="forbid")
    type: Literal["chan_msg"]
    name: str
    enabled: bool = True
    interval: Duration
    channel: str  # name (e.g. "#connections") or numeric index as str
    messages: list[str]
    message_delay: Duration = 10.0


class TracePath(BaseModel):
    """One route to probe in a trace_loop. ``path`` is comma-separated hex hashes."""
    model_config = ConfigDict(extra="forbid")
    name: str
    path: str
    tag_field: str | None = None  # optional label attached to TRACE_DATA records

    @field_validator("path")
    @classmethod
    def _check_path(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("path must not be empty")
        for chunk in v.split(","):
            chunk = chunk.strip()
            if not re.fullmatch(r"[0-9a-fA-F]+", chunk):
                raise ValueError(f"path segment {chunk!r} is not hex")
        return v


class TraceLoop(BaseModel):
    """Round-robin / random trace through a list of known paths."""
    model_config = ConfigDict(extra="forbid")
    type: Literal["trace_loop"]
    name: str
    enabled: bool = True
    interval: Duration
    mode: Literal["round_robin", "random"] = "round_robin"
    timeout: Duration = 5.0
    auth_code: int = 0
    flags: int = 0
    paths: list[TracePath]

    @field_validator("paths")
    @classmethod
    def _nonempty(cls, v: list[TracePath]) -> list[TracePath]:
        if not v:
            raise ValueError("paths must contain at least one entry")
        return v


Task = Annotated[
    Union[ChannelMessage, TraceLoop],
    Field(discriminator="type"),
]


# -- Root ---------------------------------------------------------------------

class Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    transport: Transport
    sinks: Sinks = Sinks()
    bot: BotSettings = BotSettings()
    tasks: list[Task] = Field(default_factory=list)


def load_config(path: str | Path) -> Config:
    """Load and validate a YAML config file."""
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if raw is None:
        raise ValueError(f"{p}: empty config")
    return Config.model_validate(raw)

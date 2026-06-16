"""YAML configuration model for MeshcoreBot.

Single source of truth for what the bot does. Validated with pydantic; durations
accept ``5m``/``30s``/``2h`` shorthand and normalize to seconds.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Literal, Union

import yaml
from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, field_validator, model_validator


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
    # Exact BLE address (MAC with ":" on Linux/Win, UUID on macOS). When set, used
    # directly with no scan. Set this OR `name`, not both.
    address: str | None = None
    # Substring match (case-insensitive) against the device's local_name. Useful
    # when several MeshCore-* nodes are in range. Example: "MyNode".
    # When None AND multiple MeshCore-* found, the bot will scan and prompt
    # interactively (if stdin is a TTY) or use the first match (if not).
    name: str | None = None
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
    # Minimum gap (seconds) between any two trace TX events across all
    # trace_matrix tasks. Acts as a global throttle before every send_trace
    # — within-task spacing still comes from `trace_delay`; this kicks in
    # when two tasks would otherwise fire back-to-back. Default 2s gives the
    # repeater quiet time between packets without bloating cycles. Set to
    # null or 0 to disable throttling entirely.
    cross_task_delay: Duration | None = 2.0


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
    # flags encodes path-hash byte width: 0→1B, 1→2B, 2→4B, 3→8B (bits 0-1).
    # When None, meshcore lib infers it from the first hex segment of `path`.
    flags: int | None = None
    paths: list[TracePath]

    @field_validator("paths")
    @classmethod
    def _nonempty(cls, v: list[TracePath]) -> list[TracePath]:
        if not v:
            raise ValueError("paths must contain at least one entry")
        return v


_HEX_RE = re.compile(r"[0-9a-f]+")


class TraceMatrix(BaseModel):
    """Cycle-based trace probing through MY repeater to a list of OTHER repeaters.

    Each cycle does one ``send_trace`` per OTHER (``MY,OTHER,MY``-shaped path),
    waits ``trace_delay`` between them, then prints a cumulative summary table:
    SNR→ (OTHER heard MY going out), SNR← (MY heard OTHER on the return),
    success/attempts. Multi-hop paths (``MY,O1,O2,MY``) are accepted at the
    protocol level — the table just shows endpoint SNRs; richer per-hop
    columns are deferred to a future revision.

    The MY-end token width follows OTHER's hex width: OTHER ``"5A94"`` ⇒
    MY end ``my_repeater[:4]``; OTHER ``"5A"`` ⇒ MY end ``my_repeater[:2]``.
    Hence ``my_repeater`` must be at least as long as the longest OTHER.
    """
    model_config = ConfigDict(extra="forbid")
    type: Literal["trace_matrix"]
    name: str
    enabled: bool = True

    # --- editable parameters (top of YAML for visibility) ---
    cycles: int = 0                       # 0 = run forever until Ctrl-C
    cycle_interval: Duration              # seconds between cycle starts
    trace_delay: Duration = 5.0           # seconds between traces within one cycle
    my_repeater: str                      # hex prefix of MY repeater, e.g. "3333" or "333333"
    others: list[str]                     # OTHER repeaters to probe (each one trace per cycle)

    # --- trace knobs ---
    timeout: Duration = 8.0
    auth_code: int = 0
    # flags encodes path-hash byte width: 0→1B, 1→2B, 2→4B, 3→8B (bits 0-1).
    # When None, meshcore lib infers it from the first hex segment of the path.
    flags: int | None = None

    @field_validator("cycles")
    @classmethod
    def _check_cycles(cls, v: int) -> int:
        if v < 0:
            raise ValueError("cycles must be >= 0 (0 = run until Ctrl-C)")
        return v

    @field_validator("my_repeater")
    @classmethod
    def _check_my(cls, v: str) -> str:
        v = v.strip().lower()
        if not _HEX_RE.fullmatch(v):
            raise ValueError(f"my_repeater {v!r} is not hex")
        if len(v) == 0 or len(v) % 2 != 0:
            raise ValueError(f"my_repeater {v!r} must have even hex length (whole bytes)")
        return v

    @field_validator("others")
    @classmethod
    def _check_others(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("others must contain at least one entry")
        out: list[str] = []
        seen: set[str] = set()
        for o in v:
            n = o.strip().lower()
            if not _HEX_RE.fullmatch(n):
                raise ValueError(f"other {o!r} is not hex")
            if len(n) == 0 or len(n) % 2 != 0:
                raise ValueError(f"other {o!r} must have even hex length (whole bytes)")
            if n in seen:
                raise ValueError(f"duplicate other {o!r}")
            seen.add(n)
            out.append(n)
        return out

    @model_validator(mode="after")
    def _my_long_enough(self) -> "TraceMatrix":
        max_other = max(len(o) for o in self.others)
        if len(self.my_repeater) < max_other:
            raise ValueError(
                f"my_repeater {self.my_repeater!r} (len {len(self.my_repeater)}) "
                f"shorter than longest other (len {max_other}); MY-end width follows OTHER"
            )
        return self


Task = Annotated[
    Union[ChannelMessage, TraceLoop, TraceMatrix],
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
    """Load and validate a YAML config file.

    Top-level keys starting with ``_`` are stripped before validation — that
    gives you free scratch space for YAML anchors (``_repeaters_pool: &pool …``)
    that pydantic's ``extra="forbid"`` would otherwise reject.
    """
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    if raw is None:
        raise ValueError(f"{p}: empty config")
    if isinstance(raw, dict):
        for k in list(raw.keys()):
            if isinstance(k, str) and k.startswith("_"):
                raw.pop(k)
    return Config.model_validate(raw)

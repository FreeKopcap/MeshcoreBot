"""Output sinks — every task event fans out to all configured sinks."""

from .base import Record, Sink, build_sinks

__all__ = ["Record", "Sink", "build_sinks"]

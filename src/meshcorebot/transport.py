"""Connect a MeshCore companion via the transport described in config.

Wraps the three factory functions from the upstream `meshcore` library so the
rest of the bot does not care whether it's USB serial or BLE.
"""

from __future__ import annotations

import logging
from typing import Optional

from meshcore import MeshCore

from .config import BleTransport, SerialTransport, Transport

logger = logging.getLogger(__name__)


async def connect(cfg: Transport) -> MeshCore:
    """Open a connection to the node according to *cfg*. Raises on failure."""
    if isinstance(cfg, SerialTransport):
        logger.info("Connecting via serial %s @ %d", cfg.port, cfg.baudrate)
        mc = await MeshCore.create_serial(port=cfg.port, baudrate=cfg.baudrate)
        if mc is None:
            raise ConnectionError(f"create_serial({cfg.port}) returned None")
        return mc

    if isinstance(cfg, BleTransport):
        # `meshcore.create_ble` accepts address|None and scans for MeshCore-*.
        # Optional pairing pin is forwarded when provided.
        kwargs: dict = {}
        if cfg.address:
            kwargs["address"] = cfg.address
        if cfg.pin is not None:
            kwargs["pin"] = cfg.pin
        logger.info(
            "Connecting via BLE address=%s scan_timeout=%.1fs",
            cfg.address or "<first MeshCore-*>", cfg.scan_timeout,
        )
        mc = await MeshCore.create_ble(**kwargs)
        if mc is None:
            raise ConnectionError("create_ble returned None (no device found?)")
        return mc

    raise TypeError(f"unsupported transport: {type(cfg).__name__}")


async def disconnect(mc: Optional[MeshCore]) -> None:
    if mc is None:
        return
    try:
        await mc.disconnect()
    except Exception as e:  # noqa: BLE001 — best-effort cleanup
        logger.warning("disconnect raised: %s", e)

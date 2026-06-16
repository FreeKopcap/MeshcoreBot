"""Connect a MeshCore companion via the transport described in config.

Wraps the upstream `meshcore` factory functions. BLE has extra logic:
when no exact address is given we scan the neighbourhood, optionally filter by
a name substring, and prompt interactively when multiple candidates remain.
"""

from __future__ import annotations

import asyncio
import logging
import sys
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
        return await _connect_ble(cfg)

    raise TypeError(f"unsupported transport: {type(cfg).__name__}")


async def _connect_ble(cfg: BleTransport) -> MeshCore:
    kwargs: dict = {}
    if cfg.pin is not None:
        kwargs["pin"] = cfg.pin

    # 1) Exact address → pass-through, the lib handles it.
    if cfg.address and ":" in cfg.address:
        logger.info("Connecting via BLE address=%s (no scan)", cfg.address)
        mc = await MeshCore.create_ble(address=cfg.address, **kwargs)
        if mc is None:
            raise ConnectionError(f"create_ble(address={cfg.address}) returned None")
        return mc

    # 2) Scan ourselves so we can filter by name and/or prompt interactively.
    devs = await _scan_meshcore(cfg.scan_timeout)
    if not devs:
        raise ConnectionError(
            f"No MeshCore-* BLE devices found in {cfg.scan_timeout:.1f}s scan"
        )

    chosen = await _pick_ble_device(devs, name_filter=cfg.name)
    # Pass address only (not device=) — there's a bug in meshcore 2.3.7's
    # BLEConnection.connect: when given a `device`, it forgets to set
    # `self.address`, so it returns None on success and the higher-level
    # connect raises ConnectionError("Failed to connect to device") even
    # though the BLE link came up. Passing address triggers the lib's own
    # by-filter scan, which sets address properly.
    chosen_addr = chosen[0].address
    logger.info("Connecting via BLE device=%s address=%s", chosen[1], chosen_addr)
    mc = await MeshCore.create_ble(address=chosen_addr, **kwargs)
    if mc is None:
        raise ConnectionError(f"create_ble(address={chosen_addr!r}) returned None")
    return mc


async def _scan_meshcore(timeout: float) -> list[tuple]:
    """Return list of (BLEDevice, local_name) for MeshCore-* devices in range.

    Uses bleak directly (instead of meshcore's `find_device_by_filter` which
    stops at the first match) so we can present all options. ``scanning_mode``
    is forced to ``"active"`` so we also pick up Scan Response packets — that's
    where the friendly long names typically arrive (the AD packet usually
    carries only a short MAC-suffix name like "MeshCore-XXXXXXXX"). When
    several names show up for the same address, we keep the longest one,
    which is usually the friendly variant.
    """
    from bleak import BleakScanner  # local import — only needed if BLE is in use

    logger.info("Scanning BLE for MeshCore-* devices (%.1fs, active scan)...", timeout)
    seen_names: dict[str, set[str]] = {}      # address → set of advertised names
    devices: dict[str, object] = {}            # address → BLEDevice (last seen wins; same hw either way)

    def cb(device, adv):
        for n in (adv.local_name, device.name):
            if not n or not (n.startswith("MeshCore") or n.startswith("Meshcore")):
                continue
            seen_names.setdefault(device.address, set()).add(n)
            devices[device.address] = device

    scanner = BleakScanner(detection_callback=cb, scanning_mode="active")
    await scanner.start()
    try:
        await asyncio.sleep(timeout)
    finally:
        await scanner.stop()

    out: list[tuple] = []
    for addr, names in seen_names.items():
        # Prefer the longest name we've seen — it's the friendly Scan Response one.
        best = max(names, key=len)
        out.append((devices[addr], best))
    logger.info("Scan found %d MeshCore-* device(s)", len(out))
    return out


async def _pick_ble_device(devs: list[tuple], name_filter: str | None) -> tuple:
    """Apply name filter, then auto-pick / interactively prompt."""
    if name_filter:
        nf = name_filter.lower().strip()
        matched = [(d, n) for d, n in devs if nf in n.lower()]
        if len(matched) == 1:
            logger.info("Name filter %r matched exactly one device: %s", name_filter, matched[0][1])
            return matched[0]
        if len(matched) == 0:
            logger.warning(
                "Name filter %r matched none of %d found devices — falling back to selection prompt",
                name_filter, len(devs),
            )
        else:
            logger.info("Name filter %r matched %d devices — prompting to choose", name_filter, len(matched))
            devs = matched

    if len(devs) == 1:
        return devs[0]

    # Multiple candidates remain. Prompt only if we have a real terminal.
    if not sys.stdin.isatty():
        first = devs[0]
        logger.warning(
            "%d MeshCore devices found and stdin is not a TTY — defaulting to first: %s",
            len(devs), first[1],
        )
        return first

    print("\nMultiple MeshCore BLE devices found:", file=sys.stderr)
    for i, (d, ln) in enumerate(devs, 1):
        print(f"  {i}. {ln:<32}  {d.address}", file=sys.stderr)

    # Use prompt_toolkit's async prompt — plain input() in run_in_executor
    # cannot be cancelled by Ctrl-C (the executor thread blocks on read).
    from prompt_toolkit import PromptSession
    from prompt_toolkit.patch_stdout import patch_stdout

    session: PromptSession = PromptSession()
    prompt = f"Pick [1-{len(devs)}] (Ctrl-C to abort, Ctrl-D for default 1): "
    while True:
        try:
            with patch_stdout():
                raw = (await session.prompt_async(prompt)).strip()
        except EOFError as e:
            raise ConnectionError("BLE device selection aborted (EOF)") from e
        # KeyboardInterrupt from prompt_toolkit propagates naturally — asyncio.run
        # will catch it at the top level and shut down with cleanup. We DON'T
        # want to wrap it in ConnectionError, because supervise() would then
        # treat it as a retryable error and re-enter the scan loop.
        if raw.isdigit():
            n = int(raw)
            if 1 <= n <= len(devs):
                return devs[n - 1]
        print(f"  ↳ {raw!r} is not a valid choice; pick 1..{len(devs)}", file=sys.stderr)


async def disconnect(mc: Optional[MeshCore]) -> None:
    if mc is None:
        return
    try:
        # macOS BLE stack can hang on stop_notify / characteristic-disconnect
        # during shutdown; cap it so Ctrl-C reliably exits the process.
        await asyncio.wait_for(mc.disconnect(), timeout=3.0)
    except asyncio.TimeoutError:
        logger.warning("mc.disconnect timed out after 3s; proceeding with shutdown")
    except Exception as e:  # noqa: BLE001 — best-effort cleanup
        logger.warning("disconnect raised: %s", e)

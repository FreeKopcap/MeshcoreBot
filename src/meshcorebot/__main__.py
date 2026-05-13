"""``meshcorebot`` CLI entrypoint.

Usage:
    meshcorebot path/to/config.yaml
    meshcorebot --check config.yaml         # validate and dump the parsed config
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys

from . import __version__
from .config import load_config


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="meshcorebot",
        description="Declarative MeshCore bot — scheduled tasks over USB serial or BLE.",
    )
    p.add_argument("config", help="Path to YAML config file")
    p.add_argument("--check", action="store_true",
                   help="Validate config and print the parsed result, then exit")
    p.add_argument("-v", "--verbose", action="count", default=0,
                   help="-v: INFO, -vv: DEBUG")
    p.add_argument("-V", "--version", action="version", version=f"meshcorebot {__version__}")
    return p


def cli() -> None:
    args = _build_parser().parse_args()
    level = logging.WARNING - 10 * min(args.verbose, 2)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    try:
        cfg = load_config(args.config)
    except Exception as e:  # noqa: BLE001 — surface validation errors clearly
        print(f"config error: {e}", file=sys.stderr)
        sys.exit(2)

    if args.check:
        import json
        print(json.dumps(cfg.model_dump(mode="json"), indent=2, default=str))
        return

    # Late import — pulls meshcore/bleak, only needed when actually running
    from .scheduler import supervise

    async def _runner() -> None:
        loop = asyncio.get_running_loop()
        stop = asyncio.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # windows
                pass

        run_task = asyncio.create_task(supervise(cfg), name="meshcorebot.supervise")
        stop_task = asyncio.create_task(stop.wait(), name="meshcorebot.stop")
        done, pending = await asyncio.wait(
            {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        # propagate exceptions if supervise() finished with one
        for t in done:
            if t is run_task and t.exception() is not None:
                raise t.exception()  # type: ignore[misc]

    try:
        asyncio.run(_runner())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()

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
    p.add_argument("-p", "--persistence", action="store_true",
                   help="Persist trace_matrix stats to disk (./logs/<task>.stats.json) "
                        "across bot restarts. Without this flag, stats live only in memory "
                        "and survive BLE reconnects but reset on Ctrl-C / process exit.")
    p.add_argument("-r", "--reset", action="store_true",
                   help="Wipe stats files (./logs/*.stats.json) before starting. "
                        "Requires --persistence; without --persistence there's nothing on disk.")
    p.add_argument("-v", "--verbose", action="count", default=0,
                   help="-v: INFO, -vv: DEBUG")
    p.add_argument("-V", "--version", action="version", version=f"meshcorebot {__version__}")
    return p


def cli() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    level = logging.WARNING - 10 * min(args.verbose, 2)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.reset and not args.persistence:
        parser.error("--reset only makes sense with --persistence (no on-disk stats to wipe otherwise)")

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
    from .stats_store import StatsStore

    stats_store = StatsStore(persistent=args.persistence)
    if args.reset:
        removed = stats_store.reset()
        print(f"--reset: wiped {removed} stats file(s) under ./logs/", file=sys.stderr)

    async def _runner() -> None:
        # SIGINT: leave it to Python's default — that surfaces as KeyboardInterrupt
        # which asyncio.run catches and translates into a task-cancel with a
        # short grace period. Doing our own loop.add_signal_handler(SIGINT, ...)
        # conflicts with prompt_toolkit (it manipulates SIGINT around its
        # async prompts, and sometimes leaves us without a working handler).
        # SIGTERM we still install — for systemd/CI use cases where SIGTERM is
        # the canonical shutdown signal and Python's default would just exit.
        loop = asyncio.get_running_loop()
        stop = asyncio.Event()
        try:
            loop.add_signal_handler(signal.SIGTERM, stop.set)
        except NotImplementedError:  # windows
            pass

        run_task = asyncio.create_task(supervise(cfg, stats_store=stats_store),
                                        name="meshcorebot.supervise")
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

"""``meshcorebot`` CLI entrypoint.

Usage:
    meshcorebot path/to/config.yaml                         # one config
    meshcorebot a.yaml b.yaml                               # merge tasks from both
                                                            #   onto the same companion
    meshcorebot --check a.yaml [b.yaml...]                  # validate, print, exit

Multi-config merge: all configs must share identical ``transport:`` (they target
the same BLE/serial companion). Globals (sinks/bot) are taken from the first;
``tasks:`` lists from every config are concatenated under one supervise loop.
Task ``name:`` must be unique across all configs.
"""

from __future__ import annotations

import argparse
import asyncio
import faulthandler
import logging
import signal
import sys

from . import __version__
from .config import load_config


# Stacktrace dumper for debugging hangs. Send `kill -USR1 <PID>` from another
# terminal and Python writes every thread's stack to stderr — bot keeps
# running, purely diagnostic. On macOS/Linux only.
try:
    faulthandler.register(signal.SIGUSR1, file=sys.stderr, all_threads=True)
except (AttributeError, ValueError):
    pass


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="meshcorebot",
        description="Declarative MeshCore bot — scheduled tasks over USB serial or BLE.",
    )
    p.add_argument("config", nargs="+",
                   help="One or more YAML config files. With several, their `tasks:` lists "
                        "are merged onto a single companion connection — they must share the "
                        "same `transport:` block and have unique task names across files.")
    p.add_argument("--check", action="store_true",
                   help="Validate config(s) and print the merged result, then exit")
    p.add_argument("-p", "--persistence", action="store_true",
                   help="Persist trace_matrix stats to disk (./logs/<task>.stats.json) across "
                        "bot restarts. Without this flag, stats live only in memory and survive "
                        "BLE reconnects but reset on Ctrl-C / process exit.")
    p.add_argument("-r", "--reset", action="store_true",
                   help="Wipe stats files (./logs/*.stats.json) before starting. Requires --persistence.")
    p.add_argument("-v", "--verbose", action="count", default=0,
                   help="-v: INFO, -vv: DEBUG")
    p.add_argument("-V", "--version", action="version", version=f"meshcorebot {__version__}")
    return p


def _merge_configs(configs: list[tuple[str, object]]):
    """Return a single Config that is configs[0] with subsequent configs' tasks appended.

    Validates that every config's ``transport`` matches the first one, and
    that task names are unique across the merged set. ``sinks`` is taken
    from the first config (warn on mismatch). ``bot`` is mostly taken from
    the first config (warn on mismatch), except ``cross_task_delay`` which
    is merged as MAX across all configs — it's a "minimum gap" constraint
    and the strictest value should win, not the first one.
    """
    log = logging.getLogger(__name__)
    first_path, first_cfg = configs[0]
    merged = first_cfg.model_copy(deep=True)
    seen_names: dict[str, str] = {t.name: first_path for t in merged.tasks}

    for path, cfg in configs[1:]:
        if cfg.transport != first_cfg.transport:
            raise ValueError(
                f"{path}: transport block differs from {first_path}. "
                "All merged configs must target the same companion."
            )
        for t in cfg.tasks:
            if t.name in seen_names:
                # Reconcile by enabled flag — common when configs were forked
                # from the same template and each file disables the other's tasks:
                #   both off          → silently dedupe (no-op pair)
                #   existing off, new on → new wins, drop the disabled placeholder
                #   existing on, new off → keep existing, drop the disabled new copy
                #   both on           → genuine conflict, error
                existing = next((x for x in merged.tasks if x.name == t.name), None)
                existing_on = getattr(existing, "enabled", True) if existing else False
                new_on = getattr(t, "enabled", True)
                if not existing_on and not new_on:
                    log.info("dedup'd disabled task %r (also defined in %s)",
                             t.name, seen_names[t.name])
                    continue
                if not existing_on and new_on:
                    log.info("task %r: enabled copy from %s replaces disabled copy from %s",
                             t.name, path, seen_names[t.name])
                    merged.tasks.remove(existing)
                    merged.tasks.append(t)
                    seen_names[t.name] = path
                    continue
                if existing_on and not new_on:
                    log.info("task %r: keeping enabled copy from %s; disabled copy in %s ignored",
                             t.name, seen_names[t.name], path)
                    continue
                # both enabled — genuine conflict
                raise ValueError(
                    f"{path}: task name {t.name!r} is enabled in both {seen_names[t.name]} "
                    f"and this file. Disable one or rename."
                )
            seen_names[t.name] = path
            merged.tasks.append(t)

        if cfg.sinks != first_cfg.sinks:
            log.warning("%s: sinks differ from %s; using first config's sinks", path, first_path)

        # Special-case cross_task_delay: take MAX across all configs (strictest gap wins).
        a = merged.bot.cross_task_delay or 0.0
        b = cfg.bot.cross_task_delay or 0.0
        if a != b:
            chosen = max(a, b)
            log.info("%s: cross_task_delay=%.1fs differs from current %.1fs; using max=%.1fs",
                     path, b, a, chosen)
            merged.bot.cross_task_delay = chosen if chosen > 0 else None

        # For OTHER bot fields, first wins. Detect mismatch by comparing
        # everything except cross_task_delay (which we've already reconciled).
        a_dict = merged.bot.model_dump(); a_dict.pop("cross_task_delay", None)
        b_dict = cfg.bot.model_dump();    b_dict.pop("cross_task_delay", None)
        if a_dict != b_dict:
            log.warning("%s: bot block (excluding cross_task_delay) differs from %s; "
                        "using first config's values", path, first_path)
    return merged


def cli() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    level = logging.WARNING - 10 * min(args.verbose, 2)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.reset and not args.persistence:
        parser.error("--reset only makes sense with --persistence (no on-disk stats to wipe otherwise)")

    # Load and validate every config up front — fail fast if any is bad.
    configs: list[tuple[str, object]] = []
    for cfg_path in args.config:
        try:
            cfg = load_config(cfg_path)
        except Exception as e:  # noqa: BLE001 — surface validation errors clearly
            print(f"config error in {cfg_path}: {e}", file=sys.stderr)
            sys.exit(2)
        configs.append((cfg_path, cfg))

    try:
        merged_cfg = _merge_configs(configs)
    except ValueError as e:
        print(f"merge error: {e}", file=sys.stderr)
        sys.exit(2)

    if args.check:
        import json
        if len(configs) > 1:
            print(
                f"# merged from {len(configs)} configs: "
                + ", ".join(p for p, _ in configs),
                file=sys.stderr,
            )
        print(json.dumps(merged_cfg.model_dump(mode="json"), indent=2, default=str))
        return

    # Late import — pulls meshcore/bleak, only needed when actually running
    from .scheduler import supervise
    from .stats_store import StatsStore

    stats_store = StatsStore(persistent=args.persistence)
    if args.reset:
        removed = stats_store.reset()
        print(f"--reset: wiped {removed} stats file(s) under ./logs/", file=sys.stderr)

    if len(configs) > 1:
        print(
            f"merged {len(configs)} configs: "
            + ", ".join(p for p, _ in configs)
            + f" — {len(merged_cfg.tasks)} tasks total",
            file=sys.stderr,
        )

    async def _runner() -> None:
        # See git log: SIGINT stays with Python's default so asyncio.run catches
        # KeyboardInterrupt and cleanly cancels everything; SIGTERM goes through
        # the stop event for systemd-style shutdowns.
        loop = asyncio.get_running_loop()
        stop = asyncio.Event()
        try:
            loop.add_signal_handler(signal.SIGTERM, stop.set)
        except NotImplementedError:  # windows
            pass

        run_task = asyncio.create_task(
            supervise(merged_cfg, stats_store=stats_store),
            name="meshcorebot.supervise",
        )
        stop_task = asyncio.create_task(stop.wait(), name="meshcorebot.stop")
        done, pending = await asyncio.wait(
            {run_task, stop_task}, return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()
        for t in done:
            if t is run_task and t.exception() is not None:
                raise t.exception()  # type: ignore[misc]

    try:
        asyncio.run(_runner())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    cli()

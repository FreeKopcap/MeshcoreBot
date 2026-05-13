"""Connect once, spawn one asyncio task per declared task, supervise.

If `bot.reconnect` is on and the connection dies, all tasks are cancelled and
the supervisor reconnects after `bot.reconnect_delay` and starts them again.
"""

from __future__ import annotations

import asyncio
import logging

from .config import Config
from .sinks.base import Fanout, Record
from .tasks.base import BaseTask, build_task
from .transport import connect, disconnect

logger = logging.getLogger(__name__)


async def _run_one(task: BaseTask, mc, sinks: Fanout) -> None:
    """Wrap a task so a crash kills only its own coroutine and is reported via sinks."""
    try:
        await task.run(mc)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # noqa: BLE001
        logger.exception("task %s crashed", task.name)
        await sinks.write(Record(
            event="task_crashed", task=task.name, device=task.device,
            data={"error": repr(e)},
        ))


async def supervise(cfg: Config) -> None:
    """Main bot loop. Returns on KeyboardInterrupt/cancellation."""
    sinks = __import__("meshcorebot.sinks", fromlist=["build_sinks"]).build_sinks(cfg.sinks)
    await sinks.start()
    try:
        while True:
            mc = None
            try:
                await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                          data={"state": "connecting"}))
                mc = await connect(cfg.transport)
                await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                          data={"state": "connected"}))

                # Build task implementations
                impls: list[BaseTask] = []
                for t in cfg.tasks:
                    if not getattr(t, "enabled", True):
                        continue
                    impls.append(build_task(t, cfg.bot.device_name, sinks))

                if not impls:
                    await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                              data={"state": "idle", "reason": "no enabled tasks"}))

                # Run them all in parallel; if any crashes, we keep the rest going,
                # but if the *connection* dies we want to tear down and reconnect.
                aws = [asyncio.create_task(_run_one(t, mc, sinks), name=f"task:{t.name}") for t in impls]
                try:
                    await asyncio.gather(*aws)
                except asyncio.CancelledError:
                    raise
                # If we reach here all tasks returned (rare).
                await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                          data={"state": "all_tasks_finished"}))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — surface anything as an event
                logger.exception("supervisor error")
                await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                          data={"state": "error", "error": repr(e)}))
            finally:
                await disconnect(mc)

            if not cfg.bot.reconnect:
                return
            await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                      data={"state": "reconnecting", "delay_sec": cfg.bot.reconnect_delay}))
            await asyncio.sleep(cfg.bot.reconnect_delay)
    finally:
        await sinks.stop()

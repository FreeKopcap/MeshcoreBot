"""Connect once, spawn one asyncio task per declared task, supervise.

If `bot.reconnect` is on and the connection dies, all tasks are cancelled and
the supervisor reconnects after `bot.reconnect_delay` and starts them again.

Disconnect detection: we subscribe to the meshcore lib's ``DISCONNECTED``
event. If it fires, we cancel all task workers immediately and fall through
to the reconnect loop — otherwise the tasks would keep firing send_trace
calls against a dead BLE link, accumulating fake `trace_send_error` counts.
"""

from __future__ import annotations

import asyncio
import logging

from meshcore import EventType

from .config import Config, TraceMatrix
from .sinks.base import Fanout, Record
from .stats_store import StatsStore
from .tasks.base import BaseTask, build_task
from .transport import connect, disconnect
from .tx_throttle import TxThrottle

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


async def supervise(
    cfg: Config,
    stats_store: StatsStore | None = None,
) -> None:
    """Main bot loop. Returns on KeyboardInterrupt/cancellation.

    ``stats_store`` is shared across reconnects so trace_matrix tasks resume
    their cumulative counters instead of restarting at zero on every BLE
    re-link. Cross-task TX throttling (``bot.cross_task_delay``) is wired
    here too — one ``TxThrottle`` per supervise, passed to every
    ``trace_matrix`` task so back-to-back BLE-send events are paced."""
    sinks = __import__("meshcorebot.sinks", fromlist=["build_sinks"]).build_sinks(cfg.sinks)
    if stats_store is None:
        stats_store = StatsStore()
    tx_throttle = TxThrottle(cfg.bot.cross_task_delay) if cfg.bot.cross_task_delay else None
    if tx_throttle is not None:
        logger.info("cross-task TX throttle active: min %.1fs between any two send_trace calls",
                    tx_throttle.min_gap)
    await sinks.start()
    try:
        while True:
            mc = None
            disconnected = asyncio.Event()
            disconnect_sub = None
            try:
                await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                          data={"state": "connecting"}))
                mc = await connect(cfg.transport)
                await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                          data={"state": "connected"}))

                # Subscribe to disconnect: lib emits this when the BLE/serial
                # client loses contact. Callback just flips the event; the
                # main loop below picks it up via asyncio.wait and tears down.
                def _on_disconnect(_event):
                    if not disconnected.is_set():
                        logger.warning("DISCONNECTED event received from meshcore lib")
                        disconnected.set()
                try:
                    disconnect_sub = mc.subscribe(EventType.DISCONNECTED, _on_disconnect)
                except Exception as e:  # noqa: BLE001
                    logger.warning("could not subscribe to DISCONNECTED (%s) — disconnect detection degraded", e)

                # Build task implementations.
                # Cycle barrier only useful when 2+ trace_matrix tasks run together —
                # syncs their end-of-cycle so summaries print one-after-another
                # without intervening trace events.
                n_trace_matrix = sum(1 for t in cfg.tasks
                                     if isinstance(t, TraceMatrix) and getattr(t, "enabled", True))
                cycle_barrier = asyncio.Barrier(n_trace_matrix) if n_trace_matrix >= 2 else None
                # `force_reconnect` is the task-side hook to kick the supervisor
                # out of its current connection when BLE is observed to be wedged
                # (e.g. write_gatt_char hanging past BLE_WRITE_TIMEOUT_SEC).
                # Setting `disconnected` makes the asyncio.wait below return,
                # which cascades into the tear-down → reconnect path.
                impls: list[BaseTask] = []
                for t in cfg.tasks:
                    if not getattr(t, "enabled", True):
                        continue
                    impls.append(build_task(t, cfg.bot.device_name, sinks,
                                            stats_store=stats_store, tx_throttle=tx_throttle,
                                            cycle_barrier=cycle_barrier,
                                            force_reconnect=disconnected.set))

                if not impls:
                    await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                              data={"state": "idle", "reason": "no enabled tasks"}))

                # Run them all in parallel. Watch two things at once: the gather
                # of all task workers, and the disconnect event. Whoever
                # completes first wins — typically the disconnect event, since
                # task workers loop forever.
                aws = [asyncio.create_task(_run_one(t, mc, sinks), name=f"task:{t.name}") for t in impls]
                disconnect_wait = asyncio.create_task(disconnected.wait(), name="disconnect-wait")
                # `aws + [disconnect_wait]` — first one of these to finish
                # wins. Tasks loop forever, so the typical winner is the
                # disconnect-wait. If a task crashes, gather collects via
                # return_exceptions=True and we surface that as task_crashed.
                try:
                    done, _pending = await asyncio.wait(
                        set(aws) | {disconnect_wait},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                except asyncio.CancelledError:
                    for t in aws:
                        t.cancel()
                    disconnect_wait.cancel()
                    raise

                if disconnect_wait in done:
                    # BLE/serial link dropped — tear down the task workers.
                    await sinks.write(Record(
                        event="status", task="-", device=cfg.bot.device_name,
                        data={"state": "disconnected", "reason": "lib_event"},
                    ))
                    for t in aws:
                        t.cancel()
                    # Drain so each task's finally-block (e.g. final summary
                    # print) runs before we move on to mc.disconnect().
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*aws, return_exceptions=True),
                            timeout=5.0,
                        )
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        pass
                else:
                    # One of the tasks ended first — rare (cycles cap reached
                    # or task crashed). Wait briefly for the rest to settle,
                    # then proceed. The disconnect-wait gets cancelled too.
                    disconnect_wait.cancel()
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*aws, return_exceptions=True),
                            timeout=2.0,
                        )
                    except (asyncio.TimeoutError, asyncio.CancelledError):
                        for t in aws:
                            if not t.done():
                                t.cancel()
                    await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                              data={"state": "all_tasks_finished"}))
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001 — surface anything as an event
                logger.exception("supervisor error")
                await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                          data={"state": "error", "error": repr(e)}))
            finally:
                if disconnect_sub is not None and mc is not None:
                    try:
                        mc.unsubscribe(disconnect_sub)
                    except Exception:  # noqa: BLE001
                        pass
                await disconnect(mc)

            if not cfg.bot.reconnect:
                return
            await sinks.write(Record(event="status", task="-", device=cfg.bot.device_name,
                                      data={"state": "reconnecting", "delay_sec": cfg.bot.reconnect_delay}))
            await asyncio.sleep(cfg.bot.reconnect_delay)
    finally:
        await sinks.stop()

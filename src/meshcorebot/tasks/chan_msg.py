"""Periodic channel message task — replacement for the old meshcore-probe.py."""

from __future__ import annotations

import asyncio
import logging

from meshcore import EventType

from ..config import ChannelMessage
from .base import BaseTask, safe_sleep

logger = logging.getLogger(__name__)


class ChanMsgTask(BaseTask):
    cfg: ChannelMessage

    async def _resolve_channel(self, mc) -> int | None:
        """Accept ``channel`` as either a numeric index string or a channel name."""
        raw = self.cfg.channel
        try:
            return int(raw)
        except ValueError:
            pass

        # Probe up to max_channels and match by name
        q = await mc.commands.send_device_query()
        max_channels = 40
        if q.type != EventType.ERROR:
            max_channels = q.payload.get("max_channels", 40)

        for i in range(max_channels):
            r = await mc.commands.get_channel(i)
            if r.type == EventType.ERROR:
                break
            ch_name = r.payload.get("channel_name", "")
            if not ch_name:
                break
            if ch_name == raw:
                return i
        return None

    async def run(self, mc) -> None:
        idx = await self._resolve_channel(mc)
        if idx is None:
            await self.emit("chan_msg_error", reason="channel_not_found", channel=self.cfg.channel)
            return
        await self.emit("chan_msg_ready", channel=self.cfg.channel, index=idx,
                        interval_sec=self.cfg.interval, messages=self.cfg.messages)

        cycle = 0
        while True:
            cycle += 1
            for msg in self.cfg.messages:
                r = await mc.commands.send_chan_msg(idx, msg)
                if r.type == EventType.ERROR:
                    await self.emit("chan_msg_error", cycle=cycle, message=msg, error=str(r.payload))
                else:
                    await self.emit("chan_msg_sent", cycle=cycle, message=msg, index=idx)
                await safe_sleep(self.cfg.message_delay)
            await safe_sleep(self.cfg.interval)

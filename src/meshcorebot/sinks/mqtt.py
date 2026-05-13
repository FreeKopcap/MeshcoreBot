"""MQTT publisher sink.

Publishes each record to ``<topic_prefix>/<event>`` as a JSON payload. Uses
paho-mqtt's asyncio-friendly threaded loop; we don't block the bot's event
loop on broker I/O.

NOTE: target broker address is supplied by the user; until that happens the
sink stays disabled in the example config. Stub here is fully functional.
"""

from __future__ import annotations

import asyncio
import json
import logging

from ..config import MqttSink as MqttCfg
from .base import Record, Sink

logger = logging.getLogger(__name__)

try:
    import paho.mqtt.client as mqtt  # type: ignore[import-untyped]
    _PAHO_AVAILABLE = True
except ImportError:  # pragma: no cover
    _PAHO_AVAILABLE = False


class MqttSinkImpl(Sink):
    name = "mqtt"

    def __init__(self, cfg: MqttCfg) -> None:
        if not _PAHO_AVAILABLE:
            raise RuntimeError("paho-mqtt not installed; pip install paho-mqtt")
        self._cfg = cfg
        self._client: mqtt.Client | None = None
        self._connected = asyncio.Event()

    async def start(self) -> None:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._cfg.client_id or "",
            protocol=mqtt.MQTTv5,
        )
        if self._cfg.username:
            client.username_pw_set(self._cfg.username, self._cfg.password or "")

        loop = asyncio.get_running_loop()

        def _on_connect(c, userdata, flags, reason_code, properties=None):  # noqa: ANN001
            if reason_code == 0 or getattr(reason_code, "is_failure", False) is False:
                loop.call_soon_threadsafe(self._connected.set)
                logger.info("mqtt connected → %s:%d", self._cfg.broker, self._cfg.port)
            else:
                logger.error("mqtt connect failed: %s", reason_code)

        def _on_disconnect(*_args, **_kwargs):
            loop.call_soon_threadsafe(self._connected.clear)
            logger.warning("mqtt disconnected")

        client.on_connect = _on_connect
        client.on_disconnect = _on_disconnect
        try:
            client.connect_async(self._cfg.broker, self._cfg.port, keepalive=self._cfg.keepalive)
            client.loop_start()
        except Exception as e:  # noqa: BLE001
            logger.error("mqtt connect_async failed: %s", e)
            return
        self._client = client

    async def stop(self) -> None:
        if self._client is None:
            return
        try:
            self._client.loop_stop()
            self._client.disconnect()
        except Exception as e:  # noqa: BLE001
            logger.warning("mqtt stop: %s", e)
        self._client = None

    async def write(self, record: Record) -> None:
        if self._client is None:
            return
        topic = f"{self._cfg.topic_prefix.rstrip('/')}/{record.event}"
        payload = json.dumps(record.to_dict(), ensure_ascii=False)
        # paho's publish is non-blocking — it queues to its IO thread
        info = self._client.publish(topic, payload, qos=self._cfg.qos, retain=self._cfg.retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning("mqtt publish rc=%s topic=%s", info.rc, topic)

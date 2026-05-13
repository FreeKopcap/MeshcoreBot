# MeshcoreBot

Декларативный бот для [MeshCore](https://github.com/meshcore-dev/MeshCore) companion-нод: один YAML-файл описывает что и как часто делать (периодические сообщения в каналы, periodic trace по списку маршрутов, и т.д.), а бот это выполняет.

Транспорт — USB serial или BLE. Результаты пишутся одновременно в консоль, в JSONL-файл с дневной ротацией и (опционально) публикуются в MQTT.

Сделан как форк [`meshcore-dev/meshcore-cli`](https://github.com/meshcore-dev/meshcore-cli) — родной REPL `meshcli` сохранён рядом для интерактивной отладки.

## Состояние

`v0.1.0` MVP — первая работающая версия:

- ✅ YAML-конфигурация с pydantic-валидацией
- ✅ Транспорты: USB serial, BLE
- ✅ Task `chan_msg` — периодическая отправка в канал (замена `meshcore-probe.py`)
- ✅ Task `trace_loop` — round-robin / random trace по списку path
- ✅ Sinks: console, JSONL (дневная ротация), MQTT
- ✅ Авто-reconnect

В roadmap (после MVP):

- Task `contact_msg` — приватные сообщения по имени контакта
- Task `discover_path` — periodic path discovery
- Триггеры на входящие сообщения (keyword-response, как у [agessaman/meshcore-bot](https://github.com/agessaman/meshcore-bot))
- SQLite-хранилище истории
- Web-дашборд

## Установка

```bash
cd ~/Projects/MeshcoreBot
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
```

Или с [uv](https://docs.astral.sh/uv/):

```bash
uv venv && uv pip install -e .
```

## Использование

```bash
# Валидация конфига без подключения к ноде
meshcorebot --check examples/meshcorebot.example.yaml

# Запуск
meshcorebot examples/meshcorebot.example.yaml

# С подробным логированием
meshcorebot -vv my-config.yaml
```

См. [examples/meshcorebot.example.yaml](examples/meshcorebot.example.yaml) — полный пример со всеми опциями.

## Минимальный конфиг

```yaml
transport:
  type: serial
  port: /dev/cu.usbmodemXXXXXXXXX

tasks:
  - name: trace-neighbors
    type: trace_loop
    interval: 15m
    paths:
      - { name: r1, path: "e2,4a,e2" }
      - { name: r2, path: "5f,3a,5f" }
```

## Формат событий

Каждый sink получает плоский dict. Пример JSONL-строки для успешного trace:

```json
{"event":"trace_data","task":"trace-known-routes","device":"probe-east","ts":"2026-05-14T10:32:15+00:00","cycle":3,"route":"to-ber-via-a-b","path":"e2,4a,e2","tag_field":"ber-ab","tag":3847291,"auth":0,"flags":0,"path_len":3,"path":[{"hash":"e2","snr":7.5},{"hash":"4a","snr":5.0},{"hash":"e2","snr":6.5}],"final_snr":8.0}
```

Типы событий:

| event | когда |
|---|---|
| `status` | состояние коннекта (`connecting` / `connected` / `error` / `reconnecting`) |
| `trace_loop_ready` | task `trace_loop` стартанул |
| `trace_sent` | trace отправлен, ждём ответа |
| `trace_data` | пришёл `TRACE_DATA` |
| `trace_timeout` | таймаут ожидания `TRACE_DATA` |
| `trace_send_error` | нода вернула ошибку на send |
| `chan_msg_ready` | task `chan_msg` нашёл канал и стартанул |
| `chan_msg_sent` | сообщение в канал отправлено |
| `chan_msg_error` | нода вернула ошибку на send |
| `task_crashed` | необработанное исключение в task — supervisor перезапустит коннект |

## Legacy REPL

Оригинальный `meshcli` (интерактивный shell из upstream meshcore-cli) сохранён:

```bash
meshcli -s /dev/cu.usbmodemXXXXXXXXX
```

Полезен для ручной отладки команд, поиска hash маршрутов через `contacts`/`path`, тестов одиночного `trace`. Полная документация — [REPEATER_COMMANDS.md](REPEATER_COMMANDS.md) и upstream [README](https://github.com/meshcore-dev/meshcore-cli).

## Происхождение и лицензия

Это форк [`meshcore-dev/meshcore-cli`](https://github.com/meshcore-dev/meshcore-cli) v1.5.7 by Florent de Lamotte. Сохранена оригинальная MIT-лицензия ([LICENSE](LICENSE)). Новый код в `src/meshcorebot/` — также MIT.

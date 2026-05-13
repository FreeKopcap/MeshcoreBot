# MeshcoreBot — context for Claude Code

Декларативный бот для [MeshCore](https://github.com/meshcore-dev/MeshCore) companion-нод. Один YAML описывает, что и как часто делать; бот это выполняет, результаты пишет в console + JSONL + MQTT.

Этот файл — постоянный контекст проекта. Конкретику запуска/использования смотри в [README.md](README.md).

## Происхождение

Проект — **форк [`meshcore-dev/meshcore-cli`](https://github.com/meshcore-dev/meshcore-cli) v1.5.7**, склонированный в этот репозиторий 2026-05-14 и переименованный.

- Оригинал был импортирован одним коммитом (см. `git log --oneline` → `Import meshcore-cli v1.5.7 as baseline`); поверх него — наши изменения.
- **`src/meshcore_cli/`** — нетронутый исходник upstream-CLI; точка входа `meshcli` сохранена для интерактивной отладки.
- **`src/meshcorebot/`** — новый код проекта.
- LICENSE остался MIT (как у upstream).

Предшественник — `Meshcore Probe` в `~/etc/scripts/Meshcore Probe/` (отдельный git-репо, оставлен как исторический). Его периодическая логика отправки в канал перенесена сюда как task `chan_msg`. Обратной совместимости с тем скриптом нет — мы делаем полный переход на YAML.

## Структура

```
pyproject.toml                     # name: meshcorebot 0.1.0
src/
  meshcore_cli/                    # форк upstream CLI (НЕ трогать без явной причины)
    meshcore_cli.py                # 4744 строки, REPL на prompt_toolkit
  meshcorebot/                     # наш код
    __main__.py                    # CLI: meshcorebot [--check] [-vv] config.yaml
    config.py                      # pydantic Config + YAML loader + duration parser
    transport.py                   # connect(cfg) → MeshCore (serial|ble)
    scheduler.py                   # supervisor: connect, fan-out tasks, auto-reconnect
    tasks/
      base.py                      # BaseTask, build_task(), safe_sleep
      chan_msg.py                  # periodic channel message (бывший probe)
      trace_loop.py                # round-robin/random trace, wait_for_event(TRACE_DATA, tag)
    sinks/
      base.py                      # Sink ABC, Fanout, Record (canonical event shape)
      console.py                   # цветной лог в stdout
      jsonl.py                     # ./logs/meshcorebot-{date}.jsonl с дневной ротацией
      mqtt.py                      # paho-mqtt MQTTv5, threaded loop_start()
examples/
  meshcorebot.example.yaml         # эталонный конфиг со всеми опциями
logs/                              # gitignored
```

## Зависимости

Из `pyproject.toml`: `meshcore >= 2.3.7`, `bleak >= 0.22`, `prompt_toolkit` (для legacy REPL), `requests` (upstream), `pyyaml >= 6.0`, `pydantic >= 2.5`, `paho-mqtt >= 2.0`. Python `>= 3.10` (фактически dev-окружение на 3.14).

`.venv/` уже создан в корне проекта; пакет установлен как `pip install -e .`.

## Ключевые архитектурные решения

- **Конфиг через pydantic с discriminated union** — `transport.type` и `tasks[*].type` дискриминаторы. Это даёт строгие ошибки на этапе `--check` и хорошие подсказки в IDE.
- **Duration shorthand** в YAML (`5m`, `30s`, `2h`, или числа в секундах) реализован через `BeforeValidator` в `config.py`; в моделях типы — `float` (секунды).
- **Sinks — fan-out**. `tasks` не знают о форматах вывода; они шлют структурированный `Record(event, task, device, ts, data)`, `Fanout` раздаёт во все включённые sinks. Ошибки одного sink не валят остальных.
- **Task supervision**: каждая task — отдельный asyncio.Task. Падение одной не валит другие. Падение коннекта валит весь блок и `scheduler.py` переподключается через `bot.reconnect_delay`.
- **Trace correlation**: `send_trace()` возвращает `tag`; ответ ловится через `mc.wait_for_event(EventType.TRACE_DATA, attribute_filters={"tag": tag}, timeout=...)`. Это нативный механизм upstream meshcore lib >= 2.3.
- **Path в trace** — comma-separated hex (`"e2,4a,e2"` или `"a1b2,c3d4,a1b2"`); парсинг и валидация в `config.TracePath.path`.

## Что НЕ делать без явной причины

- Не редактировать `src/meshcore_cli/meshcore_cli.py` — это форкнутый upstream, всякое изменение усложнит синк с upstream. Если нужна функция оттуда — переноси/обёртывай в `src/meshcorebot/`.
- Не использовать `getopt` (как в upstream); новый код на `argparse`.
- Не блокировать event loop в sinks — paho-mqtt уже на своём треде через `loop_start()`.

## Запуск / smoke-test

```bash
. .venv/bin/activate
meshcorebot --check examples/meshcorebot.example.yaml   # dry-run валидация
meshcorebot examples/meshcorebot.example.yaml           # боевой запуск
meshcli -s /dev/cu.usbmodemXXXXXXXXX                    # legacy REPL для отладки
```

`meshcli` полезен для поиска path-хешей: `contacts` → `path <name>`.

## Состояние / roadmap

См. README.md раздел «Состояние». Открытые вопросы для пользователя зафиксированы в `~/.claude/projects/-Users-sk-Projects-MeshcoreBot/memory/`.

# Crypto Scalper AI

Локальный форк [HyperData Terminal](https://github.com/Co-Messi/HyperData-Terminal) на ветке `crypto-scalper`. Базовый коммит `5fd1f0264391de1b65b18014049ea61161d82d51` закреплён тегом `scalper-upstream-base`.

## Единое управление

`start.ps1` управляет native API, скринером и Market Pulse вместе.
Используйте `--status`, `--restart`, `--stop`; обычный запуск работает в фоне,
а `--tui` — в текущем терминале.

Есть один runtime-контур:

- native Hub владеет биржевыми сокетами и L2;
- Market Pulse владеет durable-лентой исполнений;
- screener — единственный автор плана attention;
- направленный legacy AI и его очереди отсутствуют;
- Telegram-карточки идут через единый `CardOutbox`.

`--demo` запускает синтетические данные без AI, Telegram и Market Pulse.

## Быстрый старт

Нужен Python 3.12 или новее. Из каталога форка:

```powershell
.\start.ps1
```

Скрипт подготовит окружение, установит пакет, скопирует `.env.example` в `.env`,
запустит native API `8420`, screener `8421` и Market Pulse `8431`.
Панель: [http://127.0.0.1:8421/scalper](http://127.0.0.1:8421/scalper).
Полная инструкция — [unified-project.md](unified-project.md).

### TUI (`--tui`)

Экран показывает health, order flow, native context, ликвидации, danger zone,
крупнейшие позиции, HLP, heatmap и smart money. На невысоких терминалах панели
разбиты на страницы MARKET / RISK / VAULT; переключение — клавишами `1`/`2`/`3`/`4`
или `Tab`, авто-ротация включается `--tui-rotate N`.

### Внешний screener

Для ручного запуска без supervisor:

```powershell
$env:PULSE_API_URL="http://127.0.0.1:8431"
uv run hyperdata api --port 8420
uv run hyperdata-screener --api-url http://127.0.0.1:8420 --port 8421
```

Screener читает `/v1/attention`, `/v1/market`, `/v1/snapshot` и `/v1/candles`,
не открывает собственные биржевые сокеты. Без доступного native attention
screener завершается с явной ошибкой, а не переходит на Top-20.

## Архитектура

- `hyperdata api` — единственный источник биржевых данных. Native Hub держит L2
  и широкий обзор, а исполнения читает из Market Pulse (`PulseObservation`);
  собственные trade-подписки Hyperliquid/Binance отключены. Наружу отдаются
  `/v1/*` и WebSocket, включая `/v1/attention` с планом.
- `hyperdata-screener` — независимый анализ. Читает план attention и рыночные
  снимки, ведёт журнал, контекстные разборы, Telegram и панель `/scalper`.

Каждый показатель сохраняет источник, время наблюдения, единицу и качество.
Неизвестные и устаревшие данные не подменяются нулями. Восстановленные исполнения
ленты не становятся live-триггерами.

## AI

Отбор и разбор отделены от направленного AI. Работают:

- автоматический разбор существенных изменений контекста;
- бесплатный маршрут `nvidia/nemotron-3.5-super-vl:free` с отдельным бюджетом;
- платный маршрут для сильных эпизодов и ручных запросов через общий лимит;
- межэпизодный метаобзор при существенных изменениях минимум двух эпизодов.

Ответ проверяется относительно неизменяемого пакета фактов; гипотеза не становится
измерением. `/analyze SYMBOL WINDOW` запускает контекстный разбор, `/report`
показывает фактологический отчёт. Отсутствие ключа не создаёт AI-задание.

## Telegram

Карточки связаны с эпизодом и `message_id`, очередь хранится в SQLite, учитывает
срок актуальности и общую паузу после 429. Неоднозначная первая отправка не
повторяется автоматически; редактирование использует существующий `message_id`.
Аналитический канал задаётся отдельно от оперативного чата. Подробности:
[telegram-ux.md](telegram-ux.md).

## История и проверки

Архив L2, исполнений, назначений и решений attention сохраняется отдельно от
SQLite-журнала. Replay, восстановление неполного архива и оценка качества описаны
в [attention-operations.md](attention-operations.md) и [fact-consistency.md](fact-consistency.md).

```powershell
uv run pytest -q
uv run ruff check hyperdata_terminal extensions apps tests
```

Dev-зависимости объявлены в `pyproject.toml` (`[dependency-groups] dev`).

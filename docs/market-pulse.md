# Пульс рынка: P0–P2

Реализация первых трёх пакетов плана [market-pulse-plan-v2.md](market-pulse-plan-v2.md)
на BTC/XRP: контракты, устойчивый приём и обработка пропусков/времени.
P3 (суточная приёмка), P4 (исторические модели), P5 (интеграция со screener)
и P6 (включение уведомлений) намеренно не заявляются выполненными.

## Что уже есть

| Пакет | Реализация | Где |
|---|---|---|
| P0. Контракты | Версионируемые `InstrumentSpec`, `ExecutionEvent`, `WindowAggregate`; Decimal/NUMERIC; канонический канал на инструмент; разделение execution/aggregated; явный fingerprint и ключ повтора | `hyperdata_terminal/market_pulse/contracts.py` |
| P1. Устойчивый приём | Два независимых collector-контейнера, raw-архив, HTTP-ingest, PostgreSQL-схема, один writer, advisory lock, commit-курсор, durable очередь пересчёта | `collectors/`, `ingest.py`, `postgres.py`, `deploy/market-pulse/` |
| P2. Пропуски и время | Гарантированно сохраняемый gap при разрыве, recovering/reconciled, восстановление из raw только при покрытии границ, календарные минуты UTC, provisional/закрытие через 5 с, поздние события с новой версией | `gaps.py`, `recovery.py`, `windows.py` |

Дополнительно реализованы read-only API `/health`, `/coverage`, `/windows`,
`/events` с устойчивым курсором `event_seq` и метаданными версий/покрытия.

## Запуск через Docker Compose

```bash
cd deploy/market-pulse
cp .env.example .env
docker compose up -d --build
docker compose ps
curl http://127.0.0.1:8431/health
```

Сервисы:

- `postgres` — журнал событий, окон, конфликтов, gap-ов и курсора;
- `ingest` — единственный writer, приём батчей и gap-сообщений;
- `api` — read-only API на `127.0.0.1:8431`;
- `collector-hyperliquid` и `collector-okx` — отдельные процессы и отдельные
  raw-тома; рестарт одной биржи не затрагивает другую.

Наружу опубликованы только loopback-порты. Ingest дополнительно защищён
`PULSE_INGEST_TOKEN`.

## Запуск без Docker

```bash
pip install -e ".[pulse]"
export PULSE_DATABASE_URL=postgresql://marketpulse:marketpulse@127.0.0.1:5432/marketpulse
hyperdata-pulse init-db
hyperdata-pulse ingest
hyperdata-pulse api
hyperdata-pulse collector --venue hyperliquid --markets BTC,XRP
hyperdata-pulse collector --venue okx --markets BTC,XRP
```

Проверка покрытия и пропусков:

```bash
hyperdata-pulse status
curl "http://127.0.0.1:8431/coverage"
curl "http://127.0.0.1:8431/windows?market=BTC&from_ms=1700000000000"
curl "http://127.0.0.1:8431/events?cursor=0&limit=100"
```

Восстановление участка из raw:

```bash
hyperdata-pulse recover --venue hyperliquid --market BTC \
  --from 2026-11-06T10:00:00Z --to 2026-11-06T10:15:00Z
```

## Контракт данных

- `schema_version = market-pulse.event.v1`, окна — `market-pulse.window.v1`.
- Цены, размеры и notional — `Decimal`/PostgreSQL `NUMERIC`; время — целые
  миллисекунды UTC с суффиксом `_ms`.
- Ключ повтора: `(venue, native_market_id, channel, aggregation_kind, trade_id)`.
- Совпадение ключа с другой ценой, размером, стороной или временем — конфликт.
  Обе версии сохраняются в `event_conflicts`, затронутое окно получает
  `gap_detected` и не используется как норма.
- OKX `trades-all` — канонический канал V1; агрегированный OKX `trades`
  допустим только для сверки и имеет другой `aggregation_kind`. Каналы живут
  на разных эндпоинтах: `trades-all` — `wss://ws.okx.com:443/ws/v5/business`,
  `trades` — `.../ws/v5/public`. Старый порт 8443 выводится из эксплуатации.
  Строки `trades-all` не содержат `count`: одна строка — одно исполнение.
- Для OKX `sz` — число контрактов; размер контракта берётся из публичного
  `/api/v5/public/instruments` и версионируется. Если единица неизвестна,
  событие сохраняется, но notional блокируется.
- Hyperliquid `sz` — базовые единицы; контрактный множитель 1.

## Время и качество окон

Окно V1 — `[t, t+60_000)` UTC. До `t+65_000` окно `provisional` и имеет
`warming_up`; затем закрывается. Позднее валидное исполнение не отбрасывается:
версия окна увеличивается, `revised_at_ms` фиксирует исправление. Пропуск не
заполняется нулём.

Состояния: `warming_up`, `observed_no_detected_gap`, `gap_detected`,
`recovering`, `reconciled`, `declared_outage`, `unavailable`.

`observed_no_detected_gap` означает только отсутствие обнаруженного разрыва.
Оно не доказывает полноту биржевого потока. Свежий стакан и heartbeat сделок
учитываются отдельно.

Перезапуск коллектора (в том числе чистый, без ошибки сокета) создаёт новый
`source_epoch`, и интервал от последней полученной сделки до нового подключения
фиксируется как `gap_detected`. Replay из raw не закрывает такой gap, если
архив не покрывает обе границы: одна пограничная запись не считается
доказательством непрерывности.

Плановый отказной интервал объявляется явно, а не удаляется:

```bash
hyperdata-pulse declare-outage --gap-id 12 --to 2026-11-06T10:05:00Z \
  --reason "planned redeploy"
```

Gap остаётся в журнале со состоянием `declared_outage`; окна этого интервала
получают `declared_outage` и исключаются из приёмки, но не считаются
восстановленными и не заполняются нулём. Эта команда не доказывает полноту —
она фиксирует известную потерю.

## Что важно знать перед P3

- 24-часовой приёмки ещё не было; 99% пригодных минут — будущий критерий, а не
  измеренный результат.
- Коллекторы переподключаются с нарастающей задержкой и jitter; после сбоя
  gap остаётся открытым до восстановления из raw/истории.
- **Плановые отказы объявляются через `declare-outage`.** После перезапуска
  коллектора остаются открытые gap; если интервал был плановым, его закрывают
  командой, и окна получают `declared_outage`. Иначе gap остаётся открытым и
  метрика 99% считается с ним как с потерей.
- Raw-архив ротируется по 5 GiB на площадку, возрастное удаление 48 часов
  предусмотрено API `prune`, но операционный планировщик ещё не включён.
- PostgreSQL-бэкап и проверка восстановления в отдельной БД должны быть
  настроены на хосте; код их не заменяет.
- Screener, Telegram и карточки пока не читают новый API. Это P5; старый путь
  уведомлений не изменён.
- Binance/Bybit не включены. Расширение набора 2 → 10 → 20 инструментов
  выполняется только после P3 и повторной ресурсной/отказной приёмки.

## Проверено на живом контуре

Локальный Docker Compose с PostgreSQL 16, реальными потоками Hyperliquid и OKX
(BTC/XRP) подтвердил: `init-db`, приём сделок, конвертацию OKX `sz` через
`ctVal`, закрытие/ревизию окон, отсутствие смешения `trades-all` и `trades`,
запись raw с правами непривилегированного пользователя, фиксацию gap при
перезапуске коллектора и идемпотентность повторного replay
(`accepted=0`, только `duplicates`). Живой прогон выявил и исправил: неверный
эндпоинт OKX, падение на heartbeat-словаре, race между данными и ack,
несовпадение ключей gap/coverage с native id и ложное закрытие gap одной
пограничной записью.

## Тесты

```bash
python -m pytest tests/test_market_pulse.py -q
ruff check hyperdata_terminal/market_pulse tests/test_market_pulse.py
```

Тесты покрывают контракты, идемпотентность/конфликты, закрытие и ревизию окон,
недоступность вместо нуля, жизненный цикл gap, запись нового epoch как gap,
объявленный отказ без нулевого заполнения, raw-архив с читаемым префиксом,
восстановление (включая неполное покрытие, оставляющее gap открытым), раздельные
namespace `trades-all`/`trades` у OKX и read-only API. PostgreSQL-путь требует
отдельного запуска `init-db`; unit-тесты используют эталонный in-memory store с
той же семантикой.

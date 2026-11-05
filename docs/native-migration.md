# Переход на нативный HyperData + внешний screener

Статус: **код миграции реализован, parity-прогон на live-данных ещё не выполнен.**
Форк остаётся эталоном до прохождения shadow-недели.

## Архитектура

```text
hyperdata api --port 8420          # полный HyperDataHub, REST + WS
        |
        |  /v1/market, /v1/snapshot, /v1/candles
        v
hyperdata-screener --port 8421     # AI, журнал, Telegram, web-панель
```

Screener не запускает биржевые сокеты и не дублирует `OrderFlowEngine`.
Он читает батч-снимок и использует тот же `Screener`, `Journal`, `phase`,
`profile`, `shadow` и `quality`, что и форк.

## Быстрый запуск

```powershell
# 1. Нативный API (live)
uv run hyperdata api --port 8420

# 2. Внешний screener
uv run hyperdata-screener --api-url http://127.0.0.1:8420 --port 8421
```

Откройте `http://127.0.0.1:8421/scalper`.

Демо-режим:

```powershell
uv run hyperdata api --demo --port 8420
uv run hyperdata-screener --api-url http://127.0.0.1:8420 --port 8421
```

TUI и Telegram:

```powershell
uv run hyperdata-screener --api-url http://127.0.0.1:8420 --tui
uv run hyperdata-screener --api-url http://127.0.0.1:8420 --notify
```

## Порты и каталоги

| Процесс | Порт | Состояние |
|---|---|---|
| `hyperdata api` | 8420 | `HYPERDATA_DATA_DIR` / `hyperdata.db` |
| `hyperdata-screener` | 8421 | `HYPERDATA_SCREENER_DATA_DIR` / `scalper.sqlite` |
| форк в shadow | 8422 | отдельный journal |

Нельзя указывать оба процесса на один каталог данных.

## Контракт API

### `GET /v1/snapshot`

Параметры: `symbols` (через запятую, default top-20, max 40), `windows`
(`1m`, `5m`, `15m`; default `1m,5m`), `include_candles` (`1`/`0`).

Строка содержит:

- `market`: price, volume_24h, open_interest, funding_rate, `observed_fields`, quality;
- `flow`: cvd, ofi, volume, trade_count, trades_per_second, coverage, warming_up,
  signal, first/last trade price, connected_at, last_trade_at, quality, venue;
- `book`: spread, imbalance, best_bid, best_ask, timestamp, quality;
- `candles`: natr, range_ratio, tape_baseline, closed_at, quality.

`flow` относится к Hyperliquid. Нативный full-hub хранит combined-окна и
per-venue копии для `1m`, `5m`, `15m`; screener читает Hyperliquid, чтобы
молчащий Binance не менял сигнал. Более длинные окна остаются только combined,
чтобы не утраивать буфер сделок за 24 часа.

Свечи в `/v1/snapshot` кешируются на 30 секунд на пару `(symbol, window)`.
Screener запрашивает `include_candles=0` и берёт NATR из отдельного
`/v1/candles`, как форк.

### `GET /v1/candles`

Параметры: `symbols` (обязателен), `interval` (`1m`, `5m`, `15m`), `bars` (2–500).
Возвращает сырые закрытые свечи для корреляции с BTC и отладки.

## Parity-прогон

Оба приложения умеют писать по одной JSONL-строке на скан:

```powershell
# эталон
uv run crypto-scalper --port 8422 --record baseline.jsonl

# кандидат
uv run hyperdata-screener --api-url http://127.0.0.1:8420 --port 8421 --record candidate.jsonl

# отчёт
uv run python -m extensions.scalper.parity baseline.jsonl candidate.jsonl --report parity-report.json
```

`parity` сравнивает quality-метки, flow/market/book-значения и сигналы,
выравнивая сканы по 5-секундным окнам. Код возврата `0` — паритет, `1` — есть
расхождения.

## Что уже реализовано

- per-venue окна в `OrderFlowEngine` (Hyperliquid / Binance / combined);
- `/v1/snapshot` и `/v1/candles` с quality-метками;
- `hyperdata api --demo`;
- `RemoteHub` — адаптер нативного API под интерфейс `HyperDataHub`;
- внешний сервис `hyperdata-screener`;
- запись сканов и parity-отчёт;
- тесты на контракт, per-venue окна, адаптер и parity.

## Что осталось до cutover

1. Прогнать shadow 24 часа на live и добиться `parity-report.json` без расхождений
   два дня подряд.
2. Проверить latency: p95 `/v1/snapshot?include_candles=0` ≤ 200 мс,
   интервал скана ≤ 6 с.
3. Проверить 24-часовой soak: нет `429`, нет необработанных исключений,
   рост RSS ≤ 50 МБ.
4. Отдельным этапом добавить контекст: ликвидации, whales, HLP, funding,
   L/S, basis, IV.
5. После parity-недели удалить `ScalperHub` и `ScalperAPI`-наследник.

## Откат

До cutover: остановить `hyperdata-screener`, продолжить `crypto-scalper`.
Тег `scalper-baseline` фиксирует состояние форка до миграции.

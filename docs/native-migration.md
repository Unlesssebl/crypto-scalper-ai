# Переход на нативный HyperData + внешний screener

Статус: **код миграции реализован, production-лаунчер и supervisor готовы.**
Короткий live-shadow пройден; 24-часовой parity-прогон ещё не выполнен.
Форк остаётся эталоном до прохождения shadow-недели.

Эксплуатация, автозапуск и откат: [operations.md](operations.md).

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
.\start.ps1
```

Лаунчер вызывает `scripts/serve.py`: он запускает нативный API на `8420`,
ждёт `/v1/live`, запускает screener на `8421`, перезапускает API при падении
и останавливает оба процесса по Ctrl+C. Логи API — в `.runtime/native.log`.

Прямой запуск без supervisor:

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
Первый запрос с `include_candles=1` прогревает кэш и может занять несколько
секунд; screener запрашивает `include_candles=0` и берёт NATR из отдельного
`/v1/candles`, как форк.

### `GET /v1/candles`

Параметры: `symbols` (обязателен), `interval` (`1m`, `5m`, `15m`), `bars` (2–500).
Возвращает сырые закрытые свечи для корреляции с BTC и отладки.

## Parity-прогон

Screener пишет по одной JSONL-строке на скан. Исторический shadow-прогон
сравнивал запись форка с записью screener; после удаления legacy эталоном
служит предыдущая запись screener (регрессия логики сигналов):

```powershell
# эталон: запись до изменения (или сохранённый артефакт shadow-прогона)
uv run hyperdata-screener --api-url http://127.0.0.1:8420 --port 8421 --record baseline.jsonl

# кандидат: запись после изменения
uv run hyperdata-screener --api-url http://127.0.0.1:8420 --port 8422 --record candidate.jsonl

# отчёт
uv run hyperdata-parity baseline.jsonl candidate.jsonl --report parity-report.json
```

`parity` выравнивает сканы по ближайшему времени (±2.5 с), пропускает
`warming_up`/`stale` как переходные состояния и сравнивает quality-метки,
flow/market/book-значения и сигналы. Код возврата `0` — паритет, `1` — есть
расхождения.

Пороговые значения (эмпирические, после live-shadow):

- core (`flow.*`, `signal`, `direction`) — не более **10%** сравнений;
- secondary (`market.*`, `book.*`, `candles.*`) — не более **30%**;
- согласие по `signal`/`direction` — не ниже **95%**.

Причина: два независимых live-фида не могут совпасть тик-в-тик; 1m-окна
различаются на границах сделок. Пятиминутный shadow (60 сканов, 2360 строк)
дал core 2.44%, secondary 4.24% и signal agreement 97.18% — паритет
пройден. Это всё ещё не заменяет 24-часовой прогон.

## Что уже реализовано

- per-venue окна в `OrderFlowEngine` (Hyperliquid / Binance / combined);
- `/v1/snapshot` и `/v1/candles` с quality-метками;
- `hyperdata api --demo`;
- prefetch Top-60 по объёму до старта API (orderflow и orderbook);
- регистронезависимые тикеры (`kPEPE`, `kSHIB`, ...);
- `RemoteHub` — адаптер нативного API под интерфейс `HyperDataHub`;
- внешний сервис `hyperdata-screener`;
- запись сканов и parity-отчёт;
- тесты на контракт, per-venue окна, адаптер, parity и сквозной путь.
- рыночный контекст в `/v1/snapshot`: L/S, basis, IV, ликвидации, whales,
  extreme funding (`context`-блок), доступен screener, AI и дашборду;
- риск-аппетит-заметки (`market_risk_context`) в находках и уведомлениях;
- периодический Telegram-дайджест рынка (`run_digest`, env
  `HYPERDATA_DIGEST_SECONDS`, по умолчанию 0 — выключен);
- карточки риск-аппетита (L/S BTC, IV BTC, базис BTC, ликвидации) в
  `dashboard.html`.
- детект дрейфа universe: `/v1/snapshot` сообщает в `warnings`, если
  top-volume-активы не имеют orderflow-подписки (`universe drifted`);
- нативный TUI: health badge и метка venue, панели ORDER FLOW, NATIVE CONTEXT,
  LIQUIDATIONS, DANGER ZONE, LARGEST POSITIONS, HLP VAULT, LIQ HEATMAP,
  SMART MONEY;
- REST для этих панелей: `/v1/hlp`, `/v1/positions/heatmap`, `/v1/smart-money`
  (агрегаты без адресов кошельков).

## Universe: ротация и рестарт

Orderflow shard plan фиксируется на `start()` (см. `OrderFlowEngine.add_symbol`),
поэтому **новый листинг внутри сессии подхватится только рестартом** native
API. Это осознанный компромисс: горячее переразбиение шардов сбрасывает все
окна и ломает parity. Сейчас `/v1/snapshot` явно предупреждает о дрейфе
(`universe drifted: ... (restart to pick them up)`), чтобы оператор видел
проблему сразу, а не через молчащий CVD.

## Что осталось до cutover

1. Прогнать shadow 24 часа на live и добиться core ≤ 10% / secondary ≤ 30% /
   signal agreement ≥ 95% в `parity-report.json` два дня подряд.
2. Проверить latency: p95 `/v1/snapshot?include_candles=0` ≤ 200 мс,
   интервал скана ≤ 6 с.
3. Проверить 24-часовой soak: нет `429`, нет необработанных исключений,
   рост RSS ≤ 50 МБ.

Старый legacy web-стек (`ScalperHub`, `ScalperAPI`, `apps/scalper_web`, `--legacy`)
удалён. Явный legacy-режим совместимости (`--without-pulse` / `PULSE_UNIFIED=0`)
остаётся для проверки старых сценариев.

## Откат

Код откатывается через git: `git switch --detach scalper-baseline`.
Тег `scalper-baseline` фиксирует последнее состояние форка до миграции.

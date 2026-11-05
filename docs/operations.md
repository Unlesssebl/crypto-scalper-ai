# Эксплуатация

## Запуск

```powershell
.\start.ps1
```

Лаунчер:

1. готовит `.venv` или использует `uv`;
2. создаёт `.env` из `.env.example`;
3. запускает `scripts/serve.py`;
4. supervisor поднимает native API на `8420`, ждёт `/v1/live`;
5. запускает screener на `8421`;
6. перезапускает native API при падении;
7. останавливает оба процесса по Ctrl+C.

Linux/macOS: `./start.sh`.

Старый форк: `.\start.ps1 --legacy` или `uv run crypto-scalper`.

## Порты и проверки

| Что | Адрес |
|---|---|
| Native API | `http://127.0.0.1:8420/v1/health` |
| Liveness | `http://127.0.0.1:8420/v1/live` |
| Screener | `http://127.0.0.1:8421/scalper` |
| Screener health | `http://127.0.0.1:8421/healthz` |

`/healthz` возвращает `200` только когда native snapshot свежий и без
`last_error`. `503` означает, что screener жив, но данные недоступны.

## Логи и состояние

- native API: `.runtime/native.log`;
- screener: stdout/stderr процесса (в systemd — journal);
- native SQLite: `HYPERDATA_DATA_DIR` (`hyperdata.db`);
- screener journal: `HYPERDATA_SCREENER_DATA_DIR` (`scalper.sqlite`).

Каталоги должны быть разными. Оба SQLite работают в WAL; для бэкапа
останавливайте сервис или используйте `sqlite3 .backup`.

## Windows: автозапуск

Task Scheduler:

```powershell
schtasks /Create /TN HyperData /SC ONSTART /RU SYSTEM ^
  /TR "pwsh -NoProfile -File C:\hyperdata\start.ps1"
```

Для сервиса без консоли удобнее NSSM:

```powershell
nssm install HyperData "C:\hyperdata\.venv\Scripts\python.exe" "C:\hyperdata\scripts\serve.py"
nssm set HyperData AppDirectory "C:\hyperdata"
nssm start HyperData
```

## Linux: systemd

```ini
[Unit]
Description=HyperData native API + screener
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=/opt/hyperdata
EnvironmentFile=/opt/hyperdata/.env
ExecStart=/opt/hyperdata/.venv/bin/python /opt/hyperdata/scripts/serve.py
Restart=on-failure
RestartSec=10
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now hyperdata
journalctl -u hyperdata -f
```

## Обновление

```powershell
git pull
uv sync                 # или: .\.venv\Scripts\python.exe -m pip install -e .
.\start.ps1
```

Схемы SQLite обновляются при старте. Перед обновлением сделайте бэкап
`hyperdata.db` и `scalper.sqlite`.

## Откат

1. Остановить supervisor.
2. Запустить `.\start.ps1 --legacy` — старый `crypto-scalper`.
3. Тег `scalper-baseline` фиксирует состояние форка до миграции.

## Безопасность

- Оба процесса слушают `127.0.0.1`; наружу не выставляются.
- Если нужен удалённый доступ к native API: `HYPERDATA_API_HOST=0.0.0.0` и
  обязательный `HYPERDATA_API_KEY`; CORS — только через
  `HYPERDATA_CORS_ORIGINS`.
- Ключи Gemini/OpenRouter/Telegram храните в `.env`, не в git.
- Не запускайте два экземпляра на одном каталоге данных.

## Что смотреть при сбоях

1. `/healthz` — 503 и `last_error`.
2. `/v1/health` native API — какие feeds в `degraded`/`stale`.
3. `.runtime/native.log` — ошибки Hyperliquid/сети.
4. Логи screener — ошибки AI/Telegram/журнала.
5. `parity-report.json` — если меняли версию данных/логику сигналов.

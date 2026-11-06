# Market Pulse deployment

```bash
cp .env.example .env
# edit POSTGRES_PASSWORD and PULSE_INGEST_TOKEN
docker compose up -d --build
docker compose ps
curl -i http://127.0.0.1:8431/health
```

`init-db` is optional: the ingest and API apply the schema on connect. Run it
explicitly when you want an empty, verified database before starting collectors:

```bash
docker compose run --rm ingest init-db
```

`/health` возвращает `200` когда все каналы наблюдаются без открытых gap и
`503` при открытом gap, недоступном или устаревшем канале. `degraded` после
перезапуска коллектора ожидаем, пока gap не согласован или не объявлен; это не
падение процесса.

Two collectors are separate containers with separate raw volumes.  The ingest
container mounts both volumes read-only in practice (it only reads them for the
`recover` command), so a restart of one exchange cannot interrupt the other.

Recovery from a raw interval:

```bash
docker compose run --rm ingest recover --venue hyperliquid --market BTC \
  --from 2026-11-06T10:00:00Z --to 2026-11-06T10:15:00Z
```

Recovery is idempotent: replaying an interval that is already ingested returns
`accepted: 0` and only counts `duplicates`. It does not reconcile a gap whose
raw archive does not cover both boundaries.

Declare a planned hole (for example a collector redeploy) so it is audited but
excluded from the acceptance metric:

```bash
docker compose run --rm ingest declare-outage --gap-id 12 \
  --to 2026-11-06T10:05:00Z --reason "planned redeploy"
```

The gap keeps the `declared_outage` state and its windows are never zero-filled
or counted as recovered.

Acceptance report for a 24-hour run:

```bash
docker compose run --rm ingest acceptance \
  --from 2026-11-06T10:00:00Z --to 2026-11-07T10:00:00Z \
  --required-pct 99 --format markdown
```

Only closed windows count. Minutes before the channel's first observed window
are reported as `outside`, quiet minutes are not treated as losses, and any
minute the channel cannot vouch for fails the run. A short run cannot pass a
long window: the observed span must also reach the required share.

The API and ingest ports are published on loopback only.  Do not expose them
directly; put an authenticated proxy in front if remote access is required.

Note: `docker compose down -v` deletes the database and both raw volumes.

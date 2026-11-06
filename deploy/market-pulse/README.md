# Market Pulse deployment

```bash
cp .env.example .env
# edit POSTGRES_PASSWORD and PULSE_INGEST_TOKEN
docker compose up -d --build
docker compose ps
curl http://127.0.0.1:8431/health
```

Two collectors are separate containers with separate raw volumes.  The ingest
container mounts both volumes read-only in practice (it only reads them for the
`recover` command), so a restart of one exchange cannot interrupt the other.

Recovery from a raw interval:

```bash
docker compose run --rm ingest recover --venue hyperliquid --market BTC \
  --from 2026-11-06T10:00:00Z --to 2026-11-06T10:15:00Z
```

The API and ingest ports are published on loopback only.  Do not expose them
directly; put an authenticated proxy in front if remote access is required.

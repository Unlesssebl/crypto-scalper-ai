"""``python -m hyperdata_terminal.market_pulse`` command line."""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import time

from . import service


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hyperdata-pulse", description="Market Pulse BTC/XRP capture")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="create the PostgreSQL schema and register V1 instruments")

    ingest = sub.add_parser("ingest", help="run the single-writer ingest service")
    ingest.add_argument("--forever", action="store_true", default=True, help=argparse.SUPPRESS)

    api = sub.add_parser("api", help="run the read-only facts API")
    api.add_argument("--forever", action="store_true", default=True, help=argparse.SUPPRESS)

    collector = sub.add_parser("collector", help="run one exchange collector")
    collector.add_argument("--venue", choices=("hyperliquid", "okx"), required=True)
    collector.add_argument("--markets", default="BTC,XRP", help="comma-separated V1 markets")
    collector.add_argument(
        "--no-refresh-okx",
        action="store_true",
        help="use the checked contract-size defaults instead of fetching OKX public instruments",
    )

    recover = sub.add_parser("recover", help="replay raw archives into the ingest service")
    recover.add_argument("--venue", choices=("hyperliquid", "okx"), required=True)
    recover.add_argument("--market", required=True, help="BTC or XRP")
    recover.add_argument("--from", dest="start", default=None, help="ISO-8601 or epoch ms")
    recover.add_argument("--to", dest="end", default=None, help="ISO-8601 or epoch ms")

    sub.add_parser("status", help="print storage, coverage and open gaps")

    outage = sub.add_parser(
        "declare-outage",
        help="close a gap as a declared outage excluded from acceptance",
    )
    outage.add_argument("--gap-id", type=int, required=True)
    outage.add_argument(
        "--to",
        dest="end",
        default=None,
        help="ISO-8601 or epoch ms; defaults to now",
    )
    outage.add_argument("--reason", required=True)

    accept = sub.add_parser(
        "acceptance",
        help="build a P3 acceptance report from closed minute windows",
    )
    accept.add_argument("--from", dest="start", default=None, help="ISO-8601 or epoch ms")
    accept.add_argument("--to", dest="end", default=None, help="ISO-8601 or epoch ms")
    accept.add_argument("--required-pct", type=float, default=99.0)
    accept.add_argument("--format", choices=("json", "markdown"), default="json")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        if args.command == "init-db":
            print(service.dumps(asyncio.run(service.init_db())))
        elif args.command == "ingest":
            asyncio.run(service.run_ingest_service())
        elif args.command == "api":
            asyncio.run(service.run_api_service())
        elif args.command == "collector":
            selected = tuple(item.strip().upper() for item in args.markets.split(",") if item.strip())
            asyncio.run(
                service.run_collector(
                    args.venue,
                    markets_override=selected,
                    refresh_okx=not args.no_refresh_okx,
                )
            )
        elif args.command == "recover":
            now = int(time.time() * 1_000)
            start = service.parse_boundary(args.start, default_ms=now - 60 * 60 * 1_000)
            end = service.parse_boundary(args.end, default_ms=now)
            if end <= start:
                parser.error("--to must be after --from")
            print(
                service.dumps(
                    asyncio.run(
                        service.recover(
                            args.venue,
                            args.market,
                            start_ms=start,
                            end_ms=end,
                        )
                    )
                )
            )
        elif args.command == "status":
            print(service.dumps(asyncio.run(service.status())))
        elif args.command == "declare-outage":
            now = int(time.time() * 1_000)
            end = service.parse_boundary(args.end, default_ms=now)
            print(
                service.dumps(
                    asyncio.run(
                        service.declare_outage(
                            args.gap_id,
                            end_ms=end,
                            reason=args.reason,
                        )
                    )
                )
            )
        elif args.command == "acceptance":
            now = int(time.time() * 1_000)
            start = service.parse_boundary(args.start, default_ms=now - 24 * 60 * 60 * 1_000)
            end = service.parse_boundary(args.end, default_ms=now)
            if end <= start:
                parser.error("--to must be after --from")
            print(
                asyncio.run(
                    service.acceptance(
                        start_ms=start,
                        end_ms=end,
                        required_pct=args.required_pct,
                        report_format=args.format,
                    )
                )
            )
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        logging.getLogger(__name__).error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())

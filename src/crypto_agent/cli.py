from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import replace
from typing import Sequence

from .factory import build_orchestrator
from .narrator import OpenAINarrator
from .resource_paths import default_migration_directory


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crypto Research Agent V1.1 (read-only)")
    subparsers = parser.add_subparsers(dest="command", required=True)
    analyze = subparsers.add_parser("analyze", help="Create a read-only research report")
    analyze.add_argument("--symbol", default="BTC/USD", choices=("BTC/USD", "ETH/USD"))
    analyze.add_argument("--interval", type=int, default=1440, choices=(240, 1440, 10080))
    analyze.add_argument(
        "--provider",
        default="synthetic",
        choices=("synthetic", "kraken", "coinbase", "consensus"),
        help=(
            "Standalone feeds are diagnostic-only and always veto ALERT; "
            "use consensus for two-source analysis"
        ),
    )
    analyze.add_argument("--narrate", action="store_true")

    database = subparsers.add_parser(
        "db",
        help="Plan, apply or verify the PostgreSQL schema without exposing the DSN",
    )
    database.add_argument(
        "action",
        choices=("plan", "migrate", "health"),
        help="Database operation to run",
    )
    database.add_argument(
        "--migrations",
        default=None,
        help="Directory containing checksummed SQL migrations",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "analyze":
        report = build_orchestrator(args.provider).analyze(
            symbol=args.symbol,
            interval_minutes=args.interval,
        )
        if args.narrate:
            report = replace(report, narrative=asyncio.run(OpenAINarrator().explain(report)))
        print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False))
        return 0
    if args.command == "db":
        return _run_database_command(args.action, args.migrations)
    return 2


def _run_database_command(action: str, migration_directory: str | None) -> int:
    # Keep the optional driver and every DSN-bearing value outside normal CLI output.
    from .postgres import (
        PostgresError,
        PostgresSettings,
        PsycopgConnectionFactory,
        apply_migrations,
        check_postgres_health,
        discover_migrations,
        plan_migrations,
    )

    try:
        migrations = discover_migrations(
            migration_directory or default_migration_directory()
        )
        connection_factory = PsycopgConnectionFactory(PostgresSettings.from_env())
        if action == "plan":
            plan = plan_migrations(connection_factory, migrations)
            payload: dict[str, object] = {
                "status": "ok",
                "applied": list(plan.applied),
                "pending": [item.version for item in plan.pending],
            }
            exit_code = 0
        elif action == "migrate":
            applied = apply_migrations(connection_factory, migrations)
            payload = {"status": "ok", "applied_now": list(applied)}
            exit_code = 0
        else:
            health = check_postgres_health(
                connection_factory,
                expected_migrations=migrations,
            )
            payload = {
                "status": health.status_code,
                "healthy": health.healthy,
                "database_reachable": health.database_reachable,
                "base_schema_ready": health.base_schema_ready,
                "migrations_current": health.migrations_current,
                "server_version": health.server_version,
                "missing_migrations": list(health.missing_migrations),
            }
            exit_code = 0 if health.healthy else 1
    except (PostgresError, ValueError) as exc:
        payload = {"status": "error", "error": str(exc)}
        exit_code = 1
    print(json.dumps(payload, indent=2, ensure_ascii=False), file=sys.stdout)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

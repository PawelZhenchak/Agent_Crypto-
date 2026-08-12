from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import threading
from dataclasses import replace
from datetime import datetime
from typing import Sequence

from .factory import build_orchestrator
from .narrator import OpenAINarrator
from .providers.t4 import Plus500T4Provider
from .resource_paths import (
    default_migration_directory,
    default_risk_policy_path,
    default_v1_seed_path,
)
from .t4_ingest import (
    T4IngestionReceipt,
    T4IngestionScheduler,
    T4IngestRepository,
    T4ReplayProvider,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Plus500 T4 Research Agent V1 (read-only)")
    subparsers = parser.add_subparsers(dest="command", required=True)
    analyze = subparsers.add_parser("analyze", help="Create a read-only research report")
    analyze.add_argument("--symbol", default="BTC/USD", choices=("BTC/USD", "ETH/USD"))
    analyze.add_argument("--interval", type=int, default=1440, choices=(240, 1440, 10080))
    analyze.add_argument(
        "--provider",
        default="synthetic",
        choices=("synthetic", "t4"),
        help="Use t4 for the isolated Plus500 Futures market-data bridge",
    )
    analyze.add_argument("--narrate", action="store_true")

    database = subparsers.add_parser(
        "db",
        help="Plan, apply or verify the PostgreSQL schema without exposing the DSN",
    )
    database.add_argument(
        "action",
        choices=("plan", "migrate", "seed", "health"),
        help="Database operation to run",
    )
    database.add_argument(
        "--migrations",
        default=None,
        help="Directory containing checksummed SQL migrations",
    )

    ingest = subparsers.add_parser(
        "ingest", help="Persist read-only T4 bridge batches in PostgreSQL"
    )
    ingest.add_argument("--symbol", default="BTC/USD", choices=("BTC/USD", "ETH/USD"))
    ingest.add_argument("--interval", type=int, default=1440, choices=(240, 1440, 10080))
    ingest.add_argument("--limit", type=int, default=120)
    ingest.add_argument("--watch", action="store_true")
    ingest.add_argument("--poll-seconds", type=float, default=300.0)

    replay = subparsers.add_parser(
        "replay", help="Replay immutable T4 data from PostgreSQL at an exact cutoff"
    )
    replay.add_argument("--symbol", default="BTC/USD", choices=("BTC/USD", "ETH/USD"))
    replay.add_argument("--interval", type=int, default=1440, choices=(240, 1440, 10080))
    replay.add_argument("--limit", type=int, default=120)
    replay.add_argument("--as-of", required=True)

    analyze_replay = subparsers.add_parser(
        "analyze-replay",
        help="Create a deterministic futures report from PostgreSQL at an exact cutoff",
    )
    analyze_replay.add_argument(
        "--symbol", default="BTC/USD", choices=("BTC/USD", "ETH/USD")
    )
    analyze_replay.add_argument(
        "--interval", type=int, default=1440, choices=(240, 1440, 10080)
    )
    analyze_replay.add_argument("--limit", type=int, default=120)
    analyze_replay.add_argument("--as-of", required=True)
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
    if args.command == "ingest":
        return _run_ingest_command(args)
    if args.command == "replay":
        return _run_replay_command(args)
    if args.command == "analyze-replay":
        return _run_analyze_replay_command(args)
    return 2


def _t4_repository() -> T4IngestRepository:
    from .postgres import PostgresSettings, PsycopgConnectionFactory

    return T4IngestRepository(PsycopgConnectionFactory(PostgresSettings.from_env()))


def _t4_provider() -> Plus500T4Provider:
    return Plus500T4Provider(
        bridge_url=os.getenv("CRYPTO_AGENT_T4_BRIDGE_URL", "http://127.0.0.1:8784"),
        bridge_token=os.getenv("CRYPTO_AGENT_T4_BRIDGE_TOKEN", ""),
        timeout_seconds=float(os.getenv("CRYPTO_AGENT_T4_TIMEOUT_SECONDS", "10")),
    )


def _run_ingest_command(args: argparse.Namespace) -> int:
    from .postgres import PostgresError
    from .providers.base import ProviderError
    try:
        provider = _t4_provider()
        repository = _t4_repository()

        def fetch_and_persist(
            symbol: str, interval: int, cutoff: datetime
        ) -> T4IngestionReceipt:
            batch = provider.fetch_batch(
                symbol=symbol,
                interval_minutes=interval,
                as_of=cutoff,
                limit=args.limit,
            )
            return repository.ingest(batch, requested_as_of=cutoff)

        scheduler = T4IngestionScheduler(
            fetch_and_persist, poll_seconds=args.poll_seconds
        )
        if args.watch:
            stop_event = threading.Event()
            try:
                scheduler.run_forever(
                    stop_event,
                    symbols=(args.symbol,),
                    intervals=(args.interval,),
                )
            except KeyboardInterrupt:
                stop_event.set()
            return 0
        receipt = scheduler.run_once(
            symbols=(args.symbol,), intervals=(args.interval,)
        )[0]
        payload: dict[str, object] = {
            "status": "stored" if receipt.inserted else "duplicate",
            "batch_id": receipt.batch_id,
            "payload_sha256": receipt.payload_sha256,
            "candle_count": receipt.candle_count,
            "read_only": True,
        }
        exit_code = 0
    except (PostgresError, ProviderError, RuntimeError, ValueError) as exc:
        payload = {"status": "error", "error": str(exc), "read_only": True}
        exit_code = 1
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return exit_code


def _run_replay_command(args: argparse.Namespace) -> int:
    from .postgres import PostgresError
    from .providers.base import ProviderError

    try:
        cutoff = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
        result = _t4_repository().replay(
            symbol=args.symbol,
            interval_minutes=args.interval,
            as_of=cutoff,
            limit=args.limit,
        )
        payload: dict[str, object] = {
            "status": "ok",
            "symbol": result.symbol,
            "interval_minutes": result.interval_minutes,
            "as_of": result.as_of.isoformat(),
            "candle_count": len(result.candles),
            "contract_id": result.contract_id,
            "bridge_schema_version": result.bridge_schema_version,
            "futures_evidence_present": result.futures_evidence is not None,
            "source_batch_hashes": list(result.source_batch_hashes),
            "replay_fingerprint_sha256": result.replay_fingerprint_sha256,
            "read_only": True,
        }
        exit_code = 0
    except (PostgresError, ProviderError, RuntimeError, ValueError) as exc:
        payload = {"status": "error", "error": str(exc), "read_only": True}
        exit_code = 1
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return exit_code


def _run_analyze_replay_command(args: argparse.Namespace) -> int:
    from .orchestrator import ResearchOrchestrator
    from .policy import RiskPolicy
    from .postgres import PostgresError
    from .providers.base import ProviderError

    try:
        cutoff = datetime.fromisoformat(args.as_of.replace("Z", "+00:00"))
        report = ResearchOrchestrator(
            provider=T4ReplayProvider(_t4_repository()),
            policy=RiskPolicy.load(default_risk_policy_path()),
        ).analyze(
            symbol=args.symbol,
            interval_minutes=args.interval,
            as_of=cutoff,
            limit=args.limit,
        )
        payload: dict[str, object] = report.to_dict()
        exit_code = 0
    except (PostgresError, ProviderError, RuntimeError, ValueError) as exc:
        payload = {"status": "error", "error": str(exc), "read_only": True}
        exit_code = 1
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return exit_code


def _run_database_command(action: str, migration_directory: str | None) -> int:
    # Keep the optional driver and every DSN-bearing value outside normal CLI output.
    from .postgres import (
        PostgresError,
        PostgresSettings,
        PsycopgConnectionFactory,
        apply_migrations,
        apply_v1_seeds,
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
        elif action == "seed":
            apply_v1_seeds(connection_factory, default_v1_seed_path())
            health = check_postgres_health(
                connection_factory,
                expected_migrations=migrations,
            )
            payload = {
                "status": health.status_code,
                "healthy": health.healthy,
                "seeds_ready": health.seeds_ready,
                "missing_seeds": list(health.missing_seeds),
            }
            exit_code = 0 if health.healthy else 1
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
                "schema_ready": health.schema_ready,
                "triggers_ready": health.triggers_ready,
                "seeds_ready": health.seeds_ready,
                "server_version": health.server_version,
                "missing_migrations": list(health.missing_migrations),
                "unexpected_migrations": list(health.unexpected_migrations),
                "missing_schema_objects": list(health.missing_schema_objects),
                "missing_triggers": list(health.missing_triggers),
                "missing_seeds": list(health.missing_seeds),
            }
            exit_code = 0 if health.healthy else 1
    except (PostgresError, ValueError) as exc:
        payload = {"status": "error", "error": str(exc)}
        exit_code = 1
    print(json.dumps(payload, indent=2, ensure_ascii=False), file=sys.stdout)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import argparse
import asyncio
import hashlib
import io
import json
import os
import re
import sys
import threading
import time
from collections.abc import Sequence
from contextlib import redirect_stdout, suppress
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid5

from .factory import build_monitoring_repository, build_orchestrator
from .monitoring import run_monitored_analysis
from .narrator import OpenAINarrator
from .providers.base import ProviderBatch, ProviderError
from .providers.t4 import Plus500T4Provider
from .resource_paths import (
    default_migration_directory,
    default_observation_policy_path,
    default_risk_policy_path,
    default_v1_seed_path,
)
from .t4_ingest import (
    T4IngestionReceipt,
    T4IngestionScheduler,
    T4IngestRepository,
    T4ReplayProvider,
)

if TYPE_CHECKING:
    from .evidence_verifier import EvidenceVerifierClient
    from .observation import ObservationRepository
    from .observation_scenarios import (
        ScenarioCheckpointReceipt,
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

    monitor = subparsers.add_parser(
        "monitor",
        help="Run a resilient read-only T4 analysis and persist operational traces",
    )
    monitor.add_argument("--symbol", default="BTC/USD", choices=("BTC/USD", "ETH/USD"))
    monitor.add_argument("--interval", type=int, default=1440, choices=(240, 1440, 10080))
    monitor.add_argument("--limit", type=int, default=120)
    monitor.add_argument("--watch", action="store_true")
    monitor.add_argument("--poll-seconds", type=float, default=300.0)

    delivery = subparsers.add_parser(
        "deliver-alerts",
        help="Deliver immutable research alerts as canonical stdout JSON",
    )
    delivery.add_argument("--watch", action="store_true")
    delivery.add_argument("--poll-seconds", type=float, default=5.0)

    subparsers.add_parser(
        "monitoring-status",
        help="Read the local monitoring dashboard projection as JSON",
    )

    observation_start = subparsers.add_parser(
        "observe-start",
        help="Freeze and start the real-time 28-day live-T4 observation baseline",
    )
    observation_start.add_argument("--campaign-id", required=True)
    observation_start.add_argument("--cycle-interval-seconds", type=int, default=300)
    observation_start.add_argument(
        "--scope",
        action="append",
        default=None,
        help="Repeat for each frozen scope (default: BTC/USD:240m and ETH/USD:240m)",
    )
    observation_start.add_argument("--code-commit-hash", default=None)
    observation_start.add_argument("--t4-protocol-commit-hash", default=None)
    observation_start.add_argument("--runtime-config-hash", default=None)

    observation_status = subparsers.add_parser(
        "observe-status",
        help="Evaluate a campaign at the PostgreSQL server's current time",
    )
    observation_status.add_argument("--campaign-id", required=True)

    observation_report = subparsers.add_parser(
        "observe-report",
        help="Store the immutable final V1 quality report after the real 28-day window",
    )
    observation_report.add_argument("--campaign-id", required=True)

    observation_run = subparsers.add_parser(
        "observe-run",
        help="Run exactly one due frozen-scope live-T4 observation cycle",
    )
    observation_run.add_argument("--campaign-id", required=True)
    observation_run.add_argument("--scope", required=True)
    observation_run.add_argument("--limit", type=int, default=120)

    observation_scenarios = subparsers.add_parser(
        "observe-scenarios",
        help="Collect one DB-verified signed T4 scenario without caller PASS input",
    )
    observation_scenarios.add_argument("--campaign-id", required=True)
    observation_scenarios.add_argument(
        "--scenario",
        required=True,
        choices=(
            "bridge_restart",
            "missing_data",
            "rate_limit",
            "reconnect",
            "replay_blocked",
            "roll_transition",
            "stale_data",
        ),
    )
    observation_scenarios.add_argument("--scope", default="BTC/USD:240m")
    observation_scenarios.add_argument("--limit", type=int, default=120)
    observation_scenarios.add_argument("--timeout-seconds", type=float, default=180.0)
    observation_scenarios.add_argument("--poll-seconds", type=float, default=0.5)

    evidence_verifier = subparsers.add_parser(
        "evidence-verifier",
        help="Run the isolated loopback T4 evidence verifier service",
    )
    evidence_verifier.add_argument(
        "--host",
        choices=("127.0.0.1", "::1"),
        default="127.0.0.1",
    )
    evidence_verifier.add_argument("--port", type=int, default=8791)
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
    if args.command == "monitor":
        return _run_monitor_command(args)
    if args.command == "deliver-alerts":
        return _run_alert_delivery_command(args)
    if args.command == "monitoring-status":
        return _run_monitoring_status_command()
    if args.command == "observe-start":
        return _run_observation_start_command(args)
    if args.command == "observe-status":
        return _run_observation_status_command(args)
    if args.command == "observe-report":
        return _run_observation_report_command(args)
    if args.command == "observe-run":
        return _run_observation_cycle_command(args)
    if args.command == "observe-scenarios":
        return _run_observation_scenario_command(args)
    if args.command == "evidence-verifier":
        return _run_evidence_verifier_command(args)
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
        orchestrator = ResearchOrchestrator(
            provider=T4ReplayProvider(_t4_repository()),
            policy=RiskPolicy.load(default_risk_policy_path()),
        )
        report, _ = run_monitored_analysis(
            orchestrator.analyze,
            build_monitoring_repository(),
            operation="analyze_replay",
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


def _run_monitor_command(args: argparse.Namespace) -> int:
    if not _valid_poll_seconds(args.poll_seconds, minimum=1.0):
        print(
            json.dumps({"status": "error", "error_code": "POLL_INTERVAL_INVALID"}),
            file=sys.stderr,
        )
        return 2
    stop_event = threading.Event()
    exit_code = 0
    while not stop_event.is_set():
        try:
            orchestrator = build_orchestrator("t4")
            report, receipt = run_monitored_analysis(
                orchestrator.analyze,
                build_monitoring_repository(),
                operation="live_t4_analysis",
                symbol=args.symbol,
                interval_minutes=args.interval,
                limit=args.limit,
            )
            payload = {
                "status": receipt.status,
                "trace_id": receipt.trace_id,
                "decision": report.decision.value,
                "reason_codes": list(report.reason_codes),
                "alert_enqueued": receipt.alert_enqueued,
                "read_only": True,
            }
            cycle_exit_code = 0
        except Exception:
            payload = {
                "status": "error",
                "error_code": "MONITORING_CYCLE_FAILED",
                "read_only": True,
            }
            cycle_exit_code = 1
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)
        exit_code = max(exit_code, cycle_exit_code)
        if not args.watch:
            return cycle_exit_code
        try:
            if stop_event.wait(float(args.poll_seconds)):
                break
        except KeyboardInterrupt:
            stop_event.set()
    return exit_code


def _run_alert_delivery_command(args: argparse.Namespace) -> int:
    if not _valid_poll_seconds(args.poll_seconds, minimum=0.5):
        print(
            json.dumps({"status": "error", "error_code": "POLL_INTERVAL_INVALID"}),
            file=sys.stderr,
        )
        return 2
    stop_event = threading.Event()
    exit_code = 0
    while not stop_event.is_set():
        try:
            result = build_monitoring_repository().deliver_one(sys.stdout)
            cycle_exit_code = (
                0 if result.status in {"idle", "delivered"} else 1
            )
            if result.status != "idle":
                print(
                    json.dumps(
                        {
                            "delivery_status": result.status,
                            "alert_key": result.alert_key,
                            "attempt_no": result.attempt_no,
                            "error_code": result.error_code,
                        },
                        ensure_ascii=False,
                    ),
                    file=sys.stderr,
                )
        except Exception:
            cycle_exit_code = 1
            print(
                json.dumps(
                    {"status": "error", "error_code": "ALERT_DELIVERY_FAILED"}
                ),
                file=sys.stderr,
            )
        exit_code = max(exit_code, cycle_exit_code)
        if not args.watch:
            return cycle_exit_code
        try:
            if stop_event.wait(float(args.poll_seconds)):
                break
        except KeyboardInterrupt:
            stop_event.set()
    return exit_code


def _run_monitoring_status_command() -> int:
    try:
        data = build_monitoring_repository().dashboard()
        payload = {
            "summary": data.summary,
            "alerts": list(data.alerts),
            "incidents": list(data.incidents),
            "read_only": True,
        }
        exit_code = 0
    except Exception:
        payload = {"status": "error", "error_code": "MONITORING_UNAVAILABLE"}
        exit_code = 1
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return exit_code


def _observation_repository() -> ObservationRepository:
    from .observation import ObservationRepository
    from .observation_policy import ObservationPolicy
    from .postgres import PostgresSettings, PsycopgConnectionFactory

    configured_path = os.getenv("CRYPTO_AGENT_OBSERVATION_POLICY_PATH")
    policy_path = (
        Path(configured_path)
        if configured_path is not None
        else default_observation_policy_path()
    )
    return ObservationRepository(
        PsycopgConnectionFactory(PostgresSettings.from_env()),
        ObservationPolicy.load(policy_path),
    )


def _scenario_evidence_repository() -> EvidenceVerifierClient:
    from .evidence_verifier import EvidenceVerifierClient

    return EvidenceVerifierClient.from_env()


def _run_evidence_verifier_command(args: argparse.Namespace) -> int:
    from .evidence_verifier import run_evidence_verifier_service

    try:
        run_evidence_verifier_service(host=args.host, port=args.port)
    except Exception:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error_code": "EVIDENCE_VERIFIER_START_FAILED",
                    "execution_enabled": False,
                }
            ),
            file=sys.stderr,
        )
        return 1
    return 0


def _observation_database_now(repository: ObservationRepository) -> datetime:
    """Use PostgreSQL time so the CLI cannot accept a caller-supplied cutoff."""

    from .postgres import cursor, transaction

    with (
        transaction(repository.connection_factory) as connection,
        cursor(connection) as db_cursor,
    ):
        db_cursor.execute("SET TRANSACTION READ ONLY")
        db_cursor.execute("SELECT CURRENT_TIMESTAMP")
        row = db_cursor.fetchone()
    if (
        not isinstance(row, Sequence)
        or isinstance(row, (str, bytes))
        or len(row) != 1
        or not isinstance(row[0], datetime)
        or row[0].tzinfo is None
        or row[0].utcoffset() is None
    ):
        raise RuntimeError("Observation database clock is unavailable")
    return row[0].astimezone(UTC)


def _observation_scope_is_due(
    repository: ObservationRepository,
    *,
    campaign_id: str,
    scope_key: str,
) -> bool:
    """Read-only preflight so a fault is never armed without a writable due slot."""

    from .postgres import cursor, transaction

    with (
        transaction(repository.connection_factory) as connection,
        cursor(connection) as db_cursor,
    ):
        db_cursor.execute("SET TRANSACTION READ ONLY")
        db_cursor.execute(
            """
            WITH campaign AS (
                SELECT started_at, planned_ends_at, cycle_interval_seconds,
                       clock_timestamp() AS database_now,
                       COALESCE(MAX(cycle.sequence_no), 0) + 1 AS next_sequence
                FROM crypto_agent.t4_observation_campaigns item
                LEFT JOIN crypto_agent.t4_observation_cycles cycle
                  ON cycle.campaign_id = item.campaign_id
                 AND cycle.scope_key = %s
                WHERE item.campaign_id = %s
                  AND item.scope_manifest ? %s
                GROUP BY item.started_at, item.planned_ends_at,
                         item.cycle_interval_seconds
            )
            SELECT database_now >= started_at
                       + make_interval(secs => cycle_interval_seconds * next_sequence)
               AND started_at
                       + make_interval(secs => cycle_interval_seconds * next_sequence)
                       <= planned_ends_at
               AND database_now <= planned_ends_at
                       + make_interval(secs => cycle_interval_seconds)
            FROM campaign
            """,
            (scope_key, campaign_id, scope_key),
        )
        row = db_cursor.fetchone()
    return (
        isinstance(row, Sequence)
        and not isinstance(row, (str, bytes))
        and len(row) == 1
        and row[0] is True
    )


def _sync_bridge_observation_events(campaign_id: str) -> int:
    """Ask the isolated verifier to fetch, verify and persist the journal."""

    return _scenario_evidence_repository().sync_bridge_events(
        campaign_id=campaign_id
    )


def _checkpoint_and_sync_bridge_observation_events(
    campaign_id: str,
) -> ScenarioCheckpointReceipt:
    """Ask the isolated verifier to sign and persist the exact journal head."""

    return _scenario_evidence_repository().record_campaign_checkpoint(
        campaign_id=campaign_id
    )


def _run_observation_start_command(args: argparse.Namespace) -> int:
    from .observation import ObservationCampaign

    try:
        repository = _observation_repository()
        policy = repository.policy
        started_at = _observation_database_now(repository)
        scopes = tuple(sorted(args.scope or ("BTC/USD:240m", "ETH/USD:240m")))
        evidence_repository = _scenario_evidence_repository()
        evidence_registration = evidence_repository.register_campaign(
            args.campaign_id
        )
        campaign = ObservationCampaign(
            campaign_id=args.campaign_id,
            started_at=started_at,
            planned_ends_at=started_at
            + timedelta(seconds=policy.minimum_elapsed_seconds),
            cycle_interval_seconds=args.cycle_interval_seconds,
            code_commit_hash=(
                args.code_commit_hash
                or os.getenv("CRYPTO_AGENT_CODE_COMMIT_HASH", "")
            ),
            t4_protocol_commit_hash=(
                args.t4_protocol_commit_hash
                or os.getenv("CRYPTO_AGENT_T4_PROTOCOL_COMMIT_HASH", "")
            ),
            runtime_config_hash=(
                args.runtime_config_hash
                or os.getenv("CRYPTO_AGENT_RUNTIME_CONFIG_HASH", "")
            ),
            bridge_evidence_key_fingerprint=(
                evidence_registration.evidence_key_fingerprint_sha256
            ),
            scope_manifest=scopes,
        )
        campaign.validate(policy)
        if evidence_registration.campaign_id != campaign.campaign_id:
            raise RuntimeError("bridge evidence key registration mismatch")
        live_provider = _t4_provider()
        for scope_key in campaign.scope_manifest:
            symbol, interval_minutes = _parse_observation_scope(scope_key)
            preflight_batch = live_provider.fetch_batch(
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=started_at,
                limit=60,
            )
            _validate_live_observation_batch(preflight_batch)
        frozen_baseline_hash = repository.create_campaign(campaign)
        evidence_repository.sync_bridge_events(
            campaign_id=campaign.campaign_id,
        )
        payload: dict[str, object] = {
            "schema_version": 1,
            "status": "started",
            "campaign_id": campaign.campaign_id,
            "started_at": campaign.started_at.isoformat(),
            "planned_ends_at": campaign.planned_ends_at.isoformat(),
            "cycle_interval_seconds": campaign.cycle_interval_seconds,
            "minimum_elapsed_hours": policy.minimum_elapsed_hours,
            "scope_manifest": list(campaign.scope_manifest),
            "live_t4_preflight_scope_count": len(campaign.scope_manifest),
            "policy_id": policy.policy_id,
            "policy_hash_sha256": policy.policy_hash_sha256,
            "frozen_baseline_hash_sha256": frozen_baseline_hash,
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 0
    except Exception:
        payload = {
            "schema_version": 1,
            "status": "error",
            "error_code": "OBSERVATION_START_FAILED",
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 1
    _print_observation_json(payload)
    return exit_code


class _SingleObservationBatchProvider:
    """Analysis adapter that can return only the already-fetched batch once."""

    source_id = Plus500T4Provider.source_id

    def __init__(
        self,
        batch: ProviderBatch,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> None:
        self._batch = batch
        self._symbol = symbol
        self._interval_minutes = interval_minutes
        self._as_of = as_of
        self._limit = limit
        self.calls = 0

    def fetch_batch(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> ProviderBatch:
        self.calls += 1
        if (
            self.calls != 1
            or symbol != self._symbol
            or interval_minutes != self._interval_minutes
            or as_of != self._as_of
            or limit != self._limit
        ):
            raise ProviderError(
                "Observation batch request does not match its frozen slot",
                code="T4_OBSERVATION_BATCH_MISMATCH",
            )
        return self._batch


def _run_observation_cycle_command(args: argparse.Namespace) -> int:
    from .observation import (
        ObservationCycleExecution,
        ObservationCyclePlan,
    )
    from .orchestrator import ResearchOrchestrator
    from .policy import RiskPolicy

    try:
        if type(args.limit) is not int or not 60 <= args.limit <= 720:
            raise ValueError("invalid observation limit")
        symbol, interval_minutes = _parse_observation_scope(args.scope)
        observation_repository = _observation_repository()

        def execute(plan: ObservationCyclePlan) -> ObservationCycleExecution:
            provider = _t4_provider()
            fetch_started = time.monotonic_ns()
            try:
                batch = provider.fetch_batch(
                    symbol=symbol,
                    interval_minutes=interval_minutes,
                    as_of=plan.expected_at,
                    limit=args.limit,
                )
            except ProviderError as exc:
                # The immutable observation cycle remains the primary
                # fail-closed record if monitoring storage is unavailable.
                with suppress(Exception):
                    build_monitoring_repository().record_failure(
                        trace_id=plan.trace_id,
                        operation="live_t4_analysis",
                        error_code=exc.code,
                        component="t4_provider",
                        scope=f"{symbol}:{interval_minutes}",
                    )
                raise
            bridge_rtt_milliseconds = max(
                0, (time.monotonic_ns() - fetch_started) // 1_000_000
            )
            _validate_live_observation_batch(batch)

            ingestion = _t4_repository().ingest(
                batch,
                requested_as_of=plan.expected_at,
            )
            if ingestion.payload_sha256 != batch.raw_payload_sha256:
                raise RuntimeError("observation ingestion linkage mismatch")

            sealed_provider = _SingleObservationBatchProvider(
                batch,
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=plan.expected_at,
                limit=args.limit,
            )
            provider.fetch_batch = sealed_provider.fetch_batch  # type: ignore[method-assign]
            orchestrator = ResearchOrchestrator(
                provider=provider,
                policy=RiskPolicy.load(default_risk_policy_path()),
            )

            def analyze_exact_batch(**kwargs: object):  # type: ignore[no-untyped-def]
                report = orchestrator.analyze(**kwargs)  # type: ignore[arg-type]
                return replace(
                    report,
                    decision_id=str(
                        uuid5(UUID(plan.trace_id), "observation-research-decision")
                    ),
                    risk=replace(
                        report.risk,
                        assessment_id=str(
                            uuid5(
                                UUID(plan.trace_id),
                                "observation-risk-assessment",
                            )
                        ),
                    ),
                )

            report, monitoring_receipt = run_monitored_analysis(
                analyze_exact_batch,
                build_monitoring_repository(),
                operation="live_t4_analysis",
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=plan.expected_at,
                limit=args.limit,
                trace_id=plan.trace_id,
            )
            if (
                sealed_provider.calls != 1
                or monitoring_receipt.trace_id != plan.trace_id
                or monitoring_receipt.status != "completed"
                or report.trace_id != plan.trace_id
                or report.as_of != plan.expected_at
                or report.metadata.get("t4_bridge_schema_version") != 5
                or report.metadata.get("t4_environment") != "live_t4"
                or report.metadata.get("plus500_t4_source_attested") is not True
                or report.metadata.get("external_delivery_eligible") is not True
                or report.metadata.get("provider_error_code") is not None
            ):
                raise RuntimeError("observation analysis linkage mismatch")
            analysis_input_hash = report.metadata.get("input_fingerprint_sha256")
            if (
                not isinstance(analysis_input_hash, str)
                or re.fullmatch(r"[0-9a-f]{64}", analysis_input_hash) is None
                or report.data_snapshot_id != f"sha256:{analysis_input_hash}"
            ):
                raise RuntimeError("observation analysis input attestation mismatch")
            metadata = batch.metadata
            return ObservationCycleExecution(
                t4_batch_id=ingestion.batch_id,
                research_run_id=monitoring_receipt.research_run_id,
                analysis_input_hash=analysis_input_hash,
                bridge_schema_version=int(metadata["t4_bridge_schema_version"]),
                raw_payload_hash=ingestion.payload_sha256,
                bridge_rtt_milliseconds=int(bridge_rtt_milliseconds),
                trace_id=plan.trace_id,
                environment=str(metadata["t4_environment"]),
                external_delivery_eligible=batch.external_delivery_eligible,
                replay=metadata.get("t4_replay") is True,
                synthetic=any(
                    source.get("kind") == "synthetic_fixture"
                    for source in batch.sources
                ),
            )

        result = observation_repository.run_cycle(
            args.campaign_id,
            args.scope,
            execute,
        )
        payload: dict[str, object] = {
            "schema_version": 1,
            "status": "recorded",
            "campaign_id": result.campaign_id,
            "scope": result.scope_key,
            "sequence_no": result.sequence_no,
            "expected_at": (
                None if result.expected_at is None else result.expected_at.isoformat()
            ),
            "outcome": result.outcome,
            "trace_id": result.trace_id,
            "t4_batch_id": result.t4_batch_id,
            "research_run_id": result.research_run_id,
            "error_code": result.error_code,
            "cycle_content_hash": result.content_hash,
            "missed_cycles_recorded": result.missed_cycles_recorded,
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 0 if result.outcome in {"success", "missed"} else 1
    except Exception as exc:
        from .observation import ObservationError

        payload = {
            "schema_version": 1,
            "status": "error",
            "error_code": (
                exc.code
                if isinstance(exc, ObservationError)
                else "OBSERVATION_CYCLE_FAILED"
            ),
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 1
    _print_observation_json(payload)
    return exit_code


def _run_observation_scenario_command(args: argparse.Namespace) -> int:
    from .observation_scenarios import (
        CONTROLLED_SCENARIOS,
        PASSIVE_SCENARIOS,
        ScenarioEvidenceError,
    )

    try:
        if type(args.limit) is not int or not 60 <= args.limit <= 720:
            raise ScenarioEvidenceError(
                "Scenario recovery limit is invalid",
                code="T4_SCENARIO_LIMIT_INVALID",
            )
        if not _valid_poll_seconds(args.poll_seconds, minimum=0.01):
            raise ScenarioEvidenceError(
                "Scenario polling interval is invalid",
                code="T4_SCENARIO_TIMING_INVALID",
            )
        if (
            isinstance(args.timeout_seconds, bool)
            or not isinstance(args.timeout_seconds, (int, float))
            or not 1.0 <= float(args.timeout_seconds) <= 300.0
            or float(args.poll_seconds) > float(args.timeout_seconds)
        ):
            raise ScenarioEvidenceError(
                "Scenario timeout is invalid",
                code="T4_SCENARIO_TIMING_INVALID",
            )
        symbol, interval_minutes = _parse_observation_scope(args.scope)
        repository = _scenario_evidence_repository()
        if args.scenario in PASSIVE_SCENARIOS:
            result = repository.verify_passive_trial(
                campaign_id=args.campaign_id,
                scenario_code=args.scenario,
            )
            result.validate()
        elif args.scenario in CONTROLLED_SCENARIOS:
            def before_trigger() -> None:
                if args.scenario not in {
                    "missing_data",
                    "rate_limit",
                    "stale_data",
                }:
                    return
                observation_repository = _observation_repository()
                if not _observation_scope_is_due(
                    observation_repository,
                    campaign_id=args.campaign_id,
                    scope_key=args.scope,
                ):
                    raise ScenarioEvidenceError(
                        "Controlled fault requires a currently due frozen slot",
                        code="T4_SCENARIO_SLOT_NOT_DUE",
                    )

            def exercise(_run: object) -> None:
                expected_code = {
                    "missing_data": "T4_MISSING_DATA",
                    "rate_limit": "T4_RATE_LIMITED",
                    "stale_data": "T4_STALE_DATA",
                }.get(args.scenario)
                if expected_code is None:
                    return
                cycle_args = argparse.Namespace(
                    campaign_id=args.campaign_id,
                    scope=args.scope,
                    limit=args.limit,
                )
                captured = io.StringIO()
                with redirect_stdout(captured):
                    exit_code = _run_observation_cycle_command(cycle_args)
                try:
                    cycle_payload = json.loads(captured.getvalue())
                except (TypeError, ValueError, json.JSONDecodeError):
                    raise ScenarioEvidenceError(
                        "Controlled failure cycle result is invalid",
                        code="T4_SCENARIO_FAILURE_NOT_RECORDED",
                    ) from None
                if (
                    exit_code != 1
                    or not isinstance(cycle_payload, dict)
                    or cycle_payload.get("status") != "recorded"
                    or cycle_payload.get("outcome") != "failure"
                    or cycle_payload.get("error_code") != expected_code
                ):
                    raise ScenarioEvidenceError(
                        "Controlled failure cycle was not recorded with its typed code",
                        code="T4_SCENARIO_FAILURE_NOT_RECORDED",
                    )

            def recovery(_run: object) -> None:
                observation_repository = _observation_repository()
                deadline = time.monotonic() + min(
                    60.0, float(args.timeout_seconds)
                )
                while time.monotonic() < deadline:
                    try:
                        cutoff = _observation_database_now(observation_repository)
                        batch = _t4_provider().fetch_batch(
                            symbol=symbol,
                            interval_minutes=interval_minutes,
                            as_of=cutoff,
                            limit=args.limit,
                        )
                        _validate_live_observation_batch(batch)
                        receipt = _t4_repository().ingest(
                            batch, requested_as_of=cutoff
                        )
                        if receipt.payload_sha256 != batch.raw_payload_sha256:
                            raise ScenarioEvidenceError(
                                "Scenario recovery batch linkage is invalid",
                                code="T4_SCENARIO_RECOVERY_INVALID",
                            )
                        return
                    except ProviderError:
                        time.sleep(max(0.05, float(args.poll_seconds)))
                raise ScenarioEvidenceError(
                    "Live T4 recovery was not observed before the deadline",
                    code="T4_SCENARIO_RECOVERY_TIMEOUT",
                )

            result = repository.run_controlled(
                campaign_id=args.campaign_id,
                scenario_code=args.scenario,
                scope_key=args.scope,
                before_trigger=before_trigger,
                exercise=exercise,
                recovery=recovery,
                timeout_seconds=float(args.timeout_seconds),
                poll_seconds=float(args.poll_seconds),
            )
        else:
            raise ScenarioEvidenceError(
                "Scenario is not approved",
                code="T4_SCENARIO_INVALID",
            )
        payload: dict[str, object] = {
            "schema_version": 1,
            "status": "verified",
            "campaign_id": result.campaign_id,
            "scenario": result.scenario_code,
            "outcome": result.outcome,
            "trial_id": result.trial_id,
            "completed_at": result.completed_at.isoformat(),
            "content_hash_sha256": result.content_hash_sha256,
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 0 if result.outcome == "pass" else 3
    except Exception as exc:
        payload = {
            "schema_version": 1,
            "status": "error",
            "error_code": (
                exc.code
                if isinstance(exc, ScenarioEvidenceError)
                else "T4_SCENARIO_VERIFICATION_FAILED"
            ),
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 1
    _print_observation_json(payload)
    return exit_code


def _parse_observation_scope(scope_key: object) -> tuple[str, int]:
    if not isinstance(scope_key, str):
        raise ValueError("invalid observation scope")
    try:
        symbol, interval_text = scope_key.rsplit(":", 1)
        if not interval_text.endswith("m"):
            raise ValueError
        interval_minutes = int(interval_text[:-1])
    except (TypeError, ValueError):
        raise ValueError("invalid observation scope") from None
    if symbol not in {"BTC/USD", "ETH/USD"} or interval_minutes not in {
        240,
        1440,
        10080,
    }:
        raise ValueError("invalid observation scope")
    return symbol, interval_minutes


def _validate_live_observation_batch(batch: object) -> ProviderBatch:
    if not isinstance(batch, ProviderBatch):
        raise RuntimeError("live T4 preflight failed")
    metadata = batch.metadata
    payload = batch.raw_payload
    payload_hash = batch.raw_payload_sha256
    if (
        metadata.get("t4_bridge_schema_version") != 5
        or metadata.get("t4_environment") != "live_t4"
        or metadata.get("t4_read_only_attested") is not True
        or metadata.get("t4_order_routes_exposed") is not False
        or metadata.get("t4_source_id") != Plus500T4Provider.source_id
        or metadata.get("t4_venue_id") != Plus500T4Provider.venue_id
        or metadata.get("t4_replay") is True
        or tuple(source.get("id") for source in batch.sources)
        != (Plus500T4Provider.source_id,)
        or batch.external_delivery_eligible is not True
        or not isinstance(payload, bytes)
        or not isinstance(payload_hash, str)
        or hashlib.sha256(payload).hexdigest() != payload_hash
    ):
        raise RuntimeError("live T4 preflight failed")
    return batch


def _run_observation_status_command(args: argparse.Namespace) -> int:
    try:
        repository = _observation_repository()
        observed_until = _observation_database_now(repository)
        report = repository.build_quality_report(
            args.campaign_id,
            observed_until=observed_until,
        )
        observation_remaining_seconds = max(
            0,
            repository.policy.minimum_elapsed_seconds - report.elapsed_seconds,
        )
        finalization_remaining_seconds = repository.finalization_remaining_seconds(
            args.campaign_id,
            observed_at=observed_until,
        )
        remaining_seconds = max(
            observation_remaining_seconds,
            finalization_remaining_seconds,
        )
        payload: dict[str, object] = {
            "schema_version": 1,
            "status": "ok",
            "campaign_id": report.campaign_id,
            "observed_until": report.observed_until.isoformat(),
            "elapsed_seconds": report.elapsed_seconds,
            "remaining_seconds": remaining_seconds,
            "observation_remaining_seconds": observation_remaining_seconds,
            "finalization_grace_remaining_seconds": (
                finalization_remaining_seconds
            ),
            "overall_status": report.overall_status.value,
            "v1_gate_passed": report.v1_gate_passed,
            "final_report_eligible": remaining_seconds == 0,
            "report_hash_sha256": report.report_hash_sha256,
            "criteria": [criterion.as_payload() for criterion in report.criteria],
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 0
    except Exception:
        payload = {
            "schema_version": 1,
            "status": "error",
            "error_code": "OBSERVATION_STATUS_UNAVAILABLE",
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 1
    _print_observation_json(payload)
    return exit_code


def _run_observation_report_command(args: argparse.Namespace) -> int:
    from .observation import ObservationBridgeCheckpoint

    try:
        repository = _observation_repository()
        # Freeze the database cutoff first.  Before the window ends, report
        # status needs no bridge mutation.  At finalization the signed
        # checkpoint closes one journal prefix, which is imported and then
        # evaluated again against this same immutable cutoff.
        observed_until = _observation_database_now(repository)
        report = repository.build_quality_report(
            args.campaign_id,
            observed_until=observed_until,
        )
        observation_remaining_seconds = max(
            0,
            repository.policy.minimum_elapsed_seconds - report.elapsed_seconds,
        )
        finalization_remaining_seconds = repository.finalization_remaining_seconds(
            args.campaign_id,
            observed_at=observed_until,
        )
        remaining_seconds = max(
            observation_remaining_seconds,
            finalization_remaining_seconds,
        )
        if remaining_seconds:
            payload: dict[str, object] = {
                "schema_version": 1,
                "status": "error",
                "error_code": (
                    "OBSERVATION_WINDOW_INCOMPLETE"
                    if observation_remaining_seconds
                    else "OBSERVATION_FINALIZATION_GRACE_INCOMPLETE"
                ),
                "campaign_id": report.campaign_id,
                "observed_until": report.observed_until.isoformat(),
                "elapsed_seconds": report.elapsed_seconds,
                "remaining_seconds": remaining_seconds,
                "observation_remaining_seconds": observation_remaining_seconds,
                "finalization_grace_remaining_seconds": (
                    finalization_remaining_seconds
                ),
                "overall_status": report.overall_status.value,
                "v1_gate_passed": False,
                "read_only": True,
                "execution_enabled": False,
            }
            exit_code = 1
        else:
            receipt = _checkpoint_and_sync_bridge_observation_events(
                args.campaign_id
            )
            report = repository.build_quality_report(
                args.campaign_id,
                observed_until=observed_until,
            )
            checkpoint = ObservationBridgeCheckpoint(
                campaign_id=receipt.campaign_id,
                action_request_id=receipt.action_request_id,
                sequence_no=receipt.event_sequence_no,
                event_hash_sha256=receipt.event_hash_sha256,
            )
            stored_hash = repository.store_final_report(
                report,
                bridge_checkpoint=checkpoint,
            )
            payload = {
                "schema_version": 1,
                "status": "stored",
                "campaign_id": report.campaign_id,
                "observed_until": report.observed_until.isoformat(),
                "elapsed_seconds": report.elapsed_seconds,
                "overall_status": report.overall_status.value,
                "v1_gate_passed": report.v1_gate_passed,
                "policy_id": report.policy_id,
                "policy_hash_sha256": report.policy_hash_sha256,
                "frozen_baseline_hash_sha256": (
                    report.frozen_baseline_hash_sha256
                ),
                "report_hash_sha256": stored_hash,
                "criteria": [
                    criterion.as_payload() for criterion in report.criteria
                ],
                "read_only": True,
                "execution_enabled": False,
            }
            exit_code = 0 if report.v1_gate_passed else 3
    except Exception:
        payload = {
            "schema_version": 1,
            "status": "error",
            "error_code": "OBSERVATION_REPORT_FAILED",
            "read_only": True,
            "execution_enabled": False,
        }
        exit_code = 1
    _print_observation_json(payload)
    return exit_code


def _print_observation_json(payload: dict[str, object]) -> None:
    print(
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
    )


def _valid_poll_seconds(value: object, *, minimum: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return False
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return False
    return minimum <= numeric <= 86_400 and numeric == numeric


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

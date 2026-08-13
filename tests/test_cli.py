from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from unittest.mock import call as mock_call

from crypto_agent.cli import (
    _observation_scope_is_due,
    _scenario_evidence_repository,
    build_parser,
    main,
)
from crypto_agent.evidence_verifier import EvidenceRegistration
from crypto_agent.monitoring import DeliveryResult, MonitoringError
from crypto_agent.observation import (
    CriterionResult,
    CriterionStatus,
    ObservationBridgeCheckpoint,
    ObservationCyclePlan,
    ObservationCycleRunResult,
    ObservationQualityReport,
)
from crypto_agent.observation_policy import ObservationPolicy
from crypto_agent.observation_scenarios import (
    ScenarioCheckpointReceipt,
    ScenarioEvidenceError,
    ScenarioTrialResult,
)
from crypto_agent.postgres import PostgresUnavailableError
from crypto_agent.providers.base import ProviderBatch


class _BrokenDriver:
    @staticmethod
    def connect(*args: object, **kwargs: object) -> object:
        raise RuntimeError("opaque-driver-sensitive-marker")


_OBSERVATION_POLICY = ObservationPolicy.load("configs/observation_policy.v1.json")
_CAMPAIGN_ID = "6d18efb9-c1ee-47bd-a122-351482b1410e"
_DATABASE_NOW = datetime(2026, 8, 12, 16, 0, tzinfo=UTC)
_CHECKPOINT_ACTION_REQUEST_ID = "31f5ef33-d35e-4d97-a599-f861eaa9ed4f"
_CHECKPOINT_EVENT_HASH = "f" * 64


def _checkpoint_receipt() -> ScenarioCheckpointReceipt:
    return ScenarioCheckpointReceipt(
        campaign_id=_CAMPAIGN_ID,
        action_request_id=_CHECKPOINT_ACTION_REQUEST_ID,
        event_sequence_no=19,
        event_hash_sha256=_CHECKPOINT_EVENT_HASH,
    )


def _bridge_checkpoint() -> ObservationBridgeCheckpoint:
    return ObservationBridgeCheckpoint(
        campaign_id=_CAMPAIGN_ID,
        action_request_id=_CHECKPOINT_ACTION_REQUEST_ID,
        sequence_no=19,
        event_hash_sha256=_CHECKPOINT_EVENT_HASH,
    )


@dataclass(frozen=True)
class _MinimalObservationRisk:
    assessment_id: str


@dataclass(frozen=True)
class _MinimalObservationReport:
    decision_id: str
    trace_id: str
    as_of: datetime
    data_snapshot_id: str
    metadata: dict[str, object]
    risk: _MinimalObservationRisk


def _quality_report(
    *,
    elapsed_seconds: int,
    overall_status: CriterionStatus = CriterionStatus.NOT_OBSERVED,
    gate_passed: bool = False,
) -> ObservationQualityReport:
    return ObservationQualityReport(
        campaign_id=_CAMPAIGN_ID,
        generated_at=_DATABASE_NOW,
        observed_until=_DATABASE_NOW,
        elapsed_seconds=elapsed_seconds,
        overall_status=overall_status,
        v1_gate_passed=gate_passed,
        policy_id=_OBSERVATION_POLICY.policy_id,
        policy_hash_sha256=_OBSERVATION_POLICY.policy_hash_sha256,
        frozen_baseline_hash_sha256="d" * 64,
        criteria=(
            CriterionResult(
                criterion_id="real_elapsed_time",
                status=overall_status,
                actual=elapsed_seconds,
                threshold=_OBSERVATION_POLICY.minimum_elapsed_seconds,
            ),
        ),
    )


def _live_preflight_batch() -> ProviderBatch:
    raw_payload = b'{"schema_version":5,"environment":"live_t4"}'
    import hashlib

    return ProviderBatch(
        candles=(),
        input_candles=(),
        sources=(
            {
                "id": "plus500_t4_futures_v1",
                "kind": "futures_market_data",
                "trust": "authenticated_external_data",
            },
        ),
        metadata={
            "t4_bridge_schema_version": 5,
            "t4_environment": "live_t4",
            "t4_read_only_attested": True,
            "t4_order_routes_exposed": False,
            "t4_source_id": "plus500_t4_futures_v1",
            "t4_venue_id": "plus500_t4",
        },
        raw_payload=raw_payload,
        raw_payload_sha256=hashlib.sha256(raw_payload).hexdigest(),
        external_delivery_eligible=True,
    )


class CliTests(unittest.TestCase):
    def test_due_preflight_allows_backlog_through_exact_final_grace(self) -> None:
        class DueCursor:
            def __init__(self) -> None:
                self.query = ""
                self.params: object = None

            def execute(self, query: str, params: object = None) -> None:
                if "WITH campaign" in query:
                    self.query = query
                    self.params = params

            def fetchone(self) -> object:
                return (True,)

            def close(self) -> None:
                return None

        class DueConnection:
            def __init__(self) -> None:
                self.database_cursor = DueCursor()

            def cursor(self) -> DueCursor:
                return self.database_cursor

            def commit(self) -> None:
                return None

            def rollback(self) -> None:
                return None

            def close(self) -> None:
                return None

        connection = DueConnection()
        repository = SimpleNamespace(connection_factory=lambda: connection)

        self.assertTrue(
            _observation_scope_is_due(
                repository,
                campaign_id=_CAMPAIGN_ID,
                scope_key="BTC/USD:240m",
            )
        )
        self.assertIn("database_now <= planned_ends_at", connection.database_cursor.query)
        self.assertIn("cycle_interval_seconds", connection.database_cursor.query)
        self.assertNotIn("LEAST", connection.database_cursor.query)
        self.assertEqual(
            connection.database_cursor.params,
            ("BTC/USD:240m", _CAMPAIGN_ID, "BTC/USD:240m"),
        )

    def test_parser_exposes_only_synthetic_and_t4(self) -> None:
        parser = build_parser()
        for provider in ("synthetic", "t4"):
            args = parser.parse_args(["analyze", "--provider", provider])
            self.assertEqual(args.provider, provider)
        for removed in ("kraken", "coinbase", "consensus"):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser.parse_args(["analyze", "--provider", removed])

    def test_parser_exposes_read_only_ingest_and_exact_cutoff_replay(self) -> None:
        parser = build_parser()
        ingest = parser.parse_args(
            ["ingest", "--symbol", "ETH/USD", "--interval", "240", "--watch"]
        )
        self.assertEqual(ingest.command, "ingest")
        self.assertTrue(ingest.watch)
        replay = parser.parse_args(
            ["replay", "--as-of", "2026-08-11T00:00:00+00:00"]
        )
        self.assertEqual(replay.command, "replay")
        self.assertEqual(replay.limit, 120)

    def test_parser_exposes_monitoring_and_delivery_commands(self) -> None:
        parser = build_parser()

        monitor = parser.parse_args(
            [
                "monitor",
                "--symbol",
                "ETH/USD",
                "--interval",
                "240",
                "--limit",
                "180",
                "--watch",
                "--poll-seconds",
                "60",
            ]
        )
        self.assertEqual(monitor.command, "monitor")
        self.assertEqual(monitor.symbol, "ETH/USD")
        self.assertEqual(monitor.interval, 240)
        self.assertEqual(monitor.limit, 180)
        self.assertTrue(monitor.watch)
        self.assertEqual(monitor.poll_seconds, 60.0)

        delivery = parser.parse_args(
            ["deliver-alerts", "--watch", "--poll-seconds", "2.5"]
        )
        self.assertEqual(delivery.command, "deliver-alerts")
        self.assertTrue(delivery.watch)
        self.assertEqual(delivery.poll_seconds, 2.5)

        status = parser.parse_args(["monitoring-status"])
        self.assertEqual(status.command, "monitoring-status")

    def test_parser_exposes_observation_commands_without_caller_cutoff(self) -> None:
        parser = build_parser()

        start = parser.parse_args(
            ["observe-start", "--campaign-id", _CAMPAIGN_ID]
        )
        self.assertEqual(start.command, "observe-start")
        self.assertEqual(start.cycle_interval_seconds, 300)
        status = parser.parse_args(
            ["observe-status", "--campaign-id", _CAMPAIGN_ID]
        )
        self.assertEqual(status.command, "observe-status")
        report = parser.parse_args(
            ["observe-report", "--campaign-id", _CAMPAIGN_ID]
        )
        self.assertEqual(report.command, "observe-report")
        run = parser.parse_args(
            [
                "observe-run",
                "--campaign-id",
                _CAMPAIGN_ID,
                "--scope",
                "BTC/USD:240m",
            ]
        )
        self.assertEqual(run.command, "observe-run")
        self.assertEqual(run.limit, 120)
        scenario = parser.parse_args(
            [
                "observe-scenarios",
                "--campaign-id",
                _CAMPAIGN_ID,
                "--scenario",
                "rate_limit",
            ]
        )
        self.assertEqual(scenario.scenario, "rate_limit")
        self.assertEqual(scenario.scope, "BTC/USD:240m")
        supervisor = parser.parse_args(
            [
                "observe-supervise",
                "--campaign-id",
                _CAMPAIGN_ID,
                "--process-manager",
                "systemd",
            ]
        )
        self.assertEqual(supervisor.command, "observe-supervise")
        self.assertEqual(supervisor.poll_seconds, 15.0)
        self.assertEqual(supervisor.cycle_timeout_seconds, 120.0)
        self.assertEqual(supervisor.command_retries, 2)
        self.assertEqual(supervisor.limit, 120)
        supervisor_status = parser.parse_args(
            ["observe-supervisor-status", "--campaign-id", _CAMPAIGN_ID]
        )
        self.assertEqual(
            supervisor_status.command, "observe-supervisor-status"
        )

        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(
                [
                    "observe-start",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--started-at",
                    "2020-01-01T00:00:00Z",
                ]
            )
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(["observe-start"])
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args(
                [
                    "observe-scenarios",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--scenario",
                    "rate_limit",
                    "--outcome",
                    "pass",
                ]
            )

    def test_supervisor_status_reads_only_safe_local_projection(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / _CAMPAIGN_ID
            directory.mkdir()
            (directory / "status.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "campaign_id": _CAMPAIGN_ID,
                        "status": "running",
                        "read_only": True,
                        "execution_enabled": False,
                    }
                ),
                encoding="utf-8",
            )
            output = io.StringIO()
            with redirect_stdout(output):
                exit_code = main(
                    [
                        "observe-supervisor-status",
                        "--campaign-id",
                        _CAMPAIGN_ID,
                        "--state-directory",
                        str(root),
                    ]
                )
        payload = json.loads(output.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["status"], "running")
        self.assertTrue(payload["read_only"])
        self.assertFalse(payload["execution_enabled"])

    def test_observation_start_uses_database_time_and_freezes_baseline(self) -> None:
        repository = SimpleNamespace(
            policy=_OBSERVATION_POLICY,
            create_campaign=Mock(return_value="e" * 64),
        )
        provider = SimpleNamespace(fetch_batch=Mock(return_value=_live_preflight_batch()))
        evidence_repository = SimpleNamespace(
            register_campaign=Mock(
                return_value=EvidenceRegistration(
                    campaign_id=_CAMPAIGN_ID,
                    evidence_key_fingerprint_sha256="d" * 64,
                    boot_id="cd6b270c-7a99-402c-8e24-c4e7cd59db84",
                    verified_event_count=1,
                )
            ),
            sync_bridge_events=Mock(return_value=1),
        )
        output = io.StringIO()
        with (
            patch(
                "crypto_agent.cli._observation_repository",
                return_value=repository,
            ),
            patch(
                "crypto_agent.cli._observation_database_now",
                return_value=_DATABASE_NOW,
            ),
            patch("crypto_agent.cli._t4_provider", return_value=provider),
            patch(
                "crypto_agent.cli._scenario_evidence_repository",
                return_value=evidence_repository,
            ),
            redirect_stdout(output),
        ):
            exit_code = main(
                [
                    "observe-start",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--code-commit-hash",
                    "a" * 64,
                    "--t4-protocol-commit-hash",
                    "b" * 64,
                    "--runtime-config-hash",
                    "c" * 64,
                ]
            )

        self.assertEqual(exit_code, 0)
        campaign = repository.create_campaign.call_args.args[0]
        self.assertEqual(campaign.started_at, _DATABASE_NOW)
        self.assertEqual(
            int((campaign.planned_ends_at - campaign.started_at).total_seconds()),
            _OBSERVATION_POLICY.minimum_elapsed_seconds,
        )
        self.assertEqual(
            campaign.scope_manifest,
            ("BTC/USD:240m", "ETH/USD:240m"),
        )
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "started")
        self.assertEqual(payload["minimum_elapsed_hours"], 672)
        self.assertEqual(payload["live_t4_preflight_scope_count"], 2)
        self.assertTrue(payload["read_only"])
        self.assertFalse(payload["execution_enabled"])
        self.assertEqual(provider.fetch_batch.call_count, 2)
        evidence_repository.register_campaign.assert_called_once_with(
            _CAMPAIGN_ID
        )
        evidence_repository.sync_bridge_events.assert_called_once_with(
            campaign_id=_CAMPAIGN_ID
        )
        for call in provider.fetch_batch.call_args_list:
            self.assertEqual(call.kwargs["as_of"], _DATABASE_NOW)
            self.assertEqual(call.kwargs["limit"], 60)

    def test_observation_start_rejects_non_live_preflight_before_campaign(self) -> None:
        repository = SimpleNamespace(
            policy=_OBSERVATION_POLICY,
            create_campaign=Mock(),
        )
        simulator = _live_preflight_batch()
        simulator.metadata["t4_environment"] = "t4_simulator"
        simulator = ProviderBatch(
            candles=simulator.candles,
            input_candles=simulator.input_candles,
            sources=simulator.sources,
            metadata=simulator.metadata,
            raw_payload=simulator.raw_payload,
            raw_payload_sha256=simulator.raw_payload_sha256,
            external_delivery_eligible=False,
        )
        provider = SimpleNamespace(fetch_batch=Mock(return_value=simulator))
        evidence_repository = SimpleNamespace(
            register_campaign=Mock(
                return_value=EvidenceRegistration(
                    campaign_id=_CAMPAIGN_ID,
                    evidence_key_fingerprint_sha256="d" * 64,
                    boot_id="cd6b270c-7a99-402c-8e24-c4e7cd59db84",
                    verified_event_count=1,
                )
            ),
        )
        output = io.StringIO()

        with (
            patch("crypto_agent.cli._observation_repository", return_value=repository),
            patch(
                "crypto_agent.cli._observation_database_now",
                return_value=_DATABASE_NOW,
            ),
            patch("crypto_agent.cli._t4_provider", return_value=provider),
            patch(
                "crypto_agent.cli._scenario_evidence_repository",
                return_value=evidence_repository,
            ),
            redirect_stdout(output),
        ):
            exit_code = main(
                [
                    "observe-start",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--code-commit-hash",
                    "a" * 64,
                    "--t4-protocol-commit-hash",
                    "b" * 64,
                    "--runtime-config-hash",
                    "c" * 64,
                ]
            )

        self.assertEqual(exit_code, 1)
        repository.create_campaign.assert_not_called()
        provider.fetch_batch.assert_called_once()
        self.assertEqual(
            json.loads(output.getvalue())["error_code"],
            "OBSERVATION_START_FAILED",
        )

    def test_observation_start_rejects_journal_not_bound_to_campaign(self) -> None:
        repository = SimpleNamespace(
            policy=_OBSERVATION_POLICY,
            create_campaign=Mock(),
        )
        evidence_repository = SimpleNamespace(
            register_campaign=Mock(
                side_effect=ScenarioEvidenceError(
                    "campaign mismatch",
                    code="T4_EVIDENCE_CAMPAIGN_MISMATCH",
                )
            ),
        )
        output = io.StringIO()

        with (
            patch("crypto_agent.cli._observation_repository", return_value=repository),
            patch(
                "crypto_agent.cli._observation_database_now",
                return_value=_DATABASE_NOW,
            ),
            patch(
                "crypto_agent.cli._scenario_evidence_repository",
                return_value=evidence_repository,
            ),
            redirect_stdout(output),
        ):
            exit_code = main(
                [
                    "observe-start",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--code-commit-hash",
                    "a" * 64,
                    "--t4-protocol-commit-hash",
                    "b" * 64,
                    "--runtime-config-hash",
                    "c" * 64,
                ]
            )

        self.assertEqual(exit_code, 1)
        repository.create_campaign.assert_not_called()
        evidence_repository.register_campaign.assert_called_once_with(_CAMPAIGN_ID)
        self.assertEqual(
            json.loads(output.getvalue())["error_code"],
            "OBSERVATION_START_FAILED",
        )

    def test_replay_scenario_uses_isolated_passive_verification(self) -> None:
        result = ScenarioTrialResult(
            trial_id="268d6bb9-c877-42c5-9a53-73b017607ee4",
            campaign_id=_CAMPAIGN_ID,
            scenario_code="replay_blocked",
            outcome="pass",
            completed_at=_DATABASE_NOW,
            content_hash_sha256="a" * 64,
        )
        repository = SimpleNamespace(
            verify_passive_trial=Mock(return_value=result)
        )
        output = io.StringIO()
        with (
            patch(
                "crypto_agent.cli._scenario_evidence_repository",
                return_value=repository,
            ),
            redirect_stdout(output),
        ):
            exit_code = main(
                [
                    "observe-scenarios",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--scenario",
                    "replay_blocked",
                ]
            )

        self.assertEqual(exit_code, 0)
        repository.verify_passive_trial.assert_called_once_with(
            campaign_id=_CAMPAIGN_ID,
            scenario_code="replay_blocked",
        )
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "verified")
        self.assertEqual(payload["outcome"], "pass")
        self.assertFalse(payload["execution_enabled"])

    def test_roll_scenario_syncs_signed_events_before_passive_verification(self) -> None:
        result = ScenarioTrialResult(
            trial_id="268d6bb9-c877-42c5-9a53-73b017607ee4",
            campaign_id=_CAMPAIGN_ID,
            scenario_code="roll_transition",
            outcome="pass",
            completed_at=_DATABASE_NOW,
            content_hash_sha256="a" * 64,
        )
        repository = SimpleNamespace(
            verify_passive_trial=Mock(return_value=result)
        )
        output = io.StringIO()
        with (
            patch(
                "crypto_agent.cli._scenario_evidence_repository",
                return_value=repository,
            ),
            redirect_stdout(output),
        ):
            exit_code = main(
                [
                    "observe-scenarios",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--scenario",
                    "roll_transition",
                ]
            )

        self.assertEqual(exit_code, 0)
        repository.verify_passive_trial.assert_called_once_with(
            campaign_id=_CAMPAIGN_ID,
            scenario_code="roll_transition",
        )

    def test_controlled_fault_records_typed_cycle_and_recovery_batch(self) -> None:
        result = ScenarioTrialResult(
            trial_id="268d6bb9-c877-42c5-9a53-73b017607ee4",
            campaign_id=_CAMPAIGN_ID,
            scenario_code="rate_limit",
            outcome="pass",
            completed_at=_DATABASE_NOW,
            content_hash_sha256="b" * 64,
        )
        verifier = Mock()

        def run_controlled(**kwargs: object) -> ScenarioTrialResult:
            exercise = kwargs["exercise"]
            recovery = kwargs["recovery"]
            assert callable(exercise)
            assert callable(recovery)
            exercise(object())
            recovery(object())
            return result

        verifier.run_controlled.side_effect = run_controlled
        recovery_batch = _live_preflight_batch()
        provider = SimpleNamespace(fetch_batch=Mock(return_value=recovery_batch))
        ingestion = SimpleNamespace(
            ingest=Mock(
                return_value=SimpleNamespace(
                    payload_sha256=recovery_batch.raw_payload_sha256
                )
            )
        )

        def controlled_cycle(_args: object) -> int:
            print(
                json.dumps(
                    {
                        "status": "recorded",
                        "outcome": "failure",
                        "error_code": "T4_RATE_LIMITED",
                    }
                )
            )
            return 1

        output = io.StringIO()
        with (
            patch(
                "crypto_agent.cli._scenario_evidence_repository",
                return_value=verifier,
            ),
            patch(
                "crypto_agent.cli._run_observation_cycle_command",
                side_effect=controlled_cycle,
            ) as cycle,
            patch("crypto_agent.cli._observation_repository"),
            patch(
                "crypto_agent.cli._observation_scope_is_due",
                return_value=True,
            ),
            patch(
                "crypto_agent.cli._observation_database_now",
                return_value=_DATABASE_NOW,
            ),
            patch("crypto_agent.cli._t4_provider", return_value=provider),
            patch("crypto_agent.cli._t4_repository", return_value=ingestion),
            redirect_stdout(output),
        ):
            exit_code = main(
                [
                    "observe-scenarios",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--scenario",
                    "rate_limit",
                ]
            )

        self.assertEqual(exit_code, 0)
        self.assertTrue(cycle.called)
        provider.fetch_batch.assert_called_once_with(
            symbol="BTC/USD",
            interval_minutes=240,
            as_of=_DATABASE_NOW,
            limit=120,
        )
        ingestion.ingest.assert_called_once_with(
            recovery_batch,
            requested_as_of=_DATABASE_NOW,
        )
        self.assertEqual(json.loads(output.getvalue())["status"], "verified")

    def test_observe_run_ingests_and_analyzes_the_exact_single_fetched_batch(self) -> None:
        batch = _live_preflight_batch()
        provider_fetch = Mock(return_value=batch)
        provider = SimpleNamespace(fetch_batch=provider_fetch)
        ingestion = SimpleNamespace(
            batch_id=41,
            payload_sha256=batch.raw_payload_sha256,
        )
        ingestion_repository = SimpleNamespace(ingest=Mock(return_value=ingestion))
        observed: dict[str, object] = {}
        trace_id = "11d7fe5d-f946-571a-8107-e31b872f96d7"
        plan = ObservationCyclePlan(
            campaign_id=_CAMPAIGN_ID,
            scope_key="BTC/USD:240m",
            sequence_no=1,
            expected_at=_DATABASE_NOW,
            started_at=_DATABASE_NOW,
            trace_id=trace_id,
        )

        def orchestrator_factory(*, provider, policy):  # type: ignore[no-untyped-def]
            del policy

            def analyze(**kwargs):  # type: ignore[no-untyped-def]
                observed["analysis_batch"] = provider.fetch_batch(
                    symbol=kwargs["symbol"],
                    interval_minutes=kwargs["interval_minutes"],
                    as_of=kwargs["as_of"],
                    limit=kwargs["limit"],
                )
                return _MinimalObservationReport(
                    decision_id="old",
                    trace_id=kwargs["trace_id"],
                    as_of=kwargs["as_of"],
                    data_snapshot_id=f"sha256:{'c' * 64}",
                    metadata={
                        "t4_bridge_schema_version": 5,
                        "t4_environment": "live_t4",
                        "plus500_t4_source_attested": True,
                        "external_delivery_eligible": True,
                        "provider_error_code": None,
                        "input_fingerprint_sha256": "c" * 64,
                    },
                    risk=_MinimalObservationRisk(assessment_id="old"),
                )

            return SimpleNamespace(analyze=analyze)

        def monitored(
            analyze,
            monitoring,
            **kwargs,
        ):  # type: ignore[no-untyped-def]
            del monitoring
            report = analyze(**{
                "symbol": kwargs["symbol"],
                "interval_minutes": kwargs["interval_minutes"],
                "as_of": kwargs["as_of"],
                "limit": kwargs["limit"],
                "trace_id": kwargs["trace_id"],
            })
            observed["report"] = report
            return report, SimpleNamespace(
                trace_id=kwargs["trace_id"],
                research_run_id=73,
                status="completed",
            )

        def run_cycle(campaign_id, scope, execute):  # type: ignore[no-untyped-def]
            self.assertEqual(campaign_id, _CAMPAIGN_ID)
            self.assertEqual(scope, "BTC/USD:240m")
            evidence = execute(plan)
            observed["evidence"] = evidence
            return ObservationCycleRunResult(
                campaign_id=campaign_id,
                scope_key=scope,
                sequence_no=1,
                expected_at=plan.expected_at,
                outcome="success",
                trace_id=trace_id,
                content_hash="f" * 64,
                missed_cycles_recorded=0,
                t4_batch_id=evidence.t4_batch_id,
                research_run_id=evidence.research_run_id,
            )

        observation_repository = SimpleNamespace(run_cycle=Mock(side_effect=run_cycle))
        output = io.StringIO()
        with (
            patch("crypto_agent.cli._observation_repository", return_value=observation_repository),
            patch("crypto_agent.cli._t4_provider", return_value=provider),
            patch("crypto_agent.cli._t4_repository", return_value=ingestion_repository),
            patch(
                "crypto_agent.orchestrator.ResearchOrchestrator",
                side_effect=orchestrator_factory,
            ),
            patch("crypto_agent.cli.build_monitoring_repository", return_value=object()),
            patch("crypto_agent.cli.run_monitored_analysis", side_effect=monitored),
            redirect_stdout(output),
        ):
            exit_code = main(
                [
                    "observe-run",
                    "--campaign-id",
                    _CAMPAIGN_ID,
                    "--scope",
                    "BTC/USD:240m",
                ]
            )

        self.assertEqual(exit_code, 0)
        provider_fetch.assert_called_once_with(
            symbol="BTC/USD",
            interval_minutes=240,
            as_of=_DATABASE_NOW,
            limit=120,
        )
        ingestion_repository.ingest.assert_called_once_with(
            batch,
            requested_as_of=_DATABASE_NOW,
        )
        self.assertIs(observed["analysis_batch"], batch)
        monitored_report = observed["report"]
        self.assertNotEqual(monitored_report.decision_id, "old")  # type: ignore[union-attr]
        self.assertNotEqual(
            monitored_report.risk.assessment_id,  # type: ignore[union-attr]
            "old",
        )
        evidence = observed["evidence"]
        self.assertEqual(evidence.t4_batch_id, 41)  # type: ignore[union-attr]
        self.assertEqual(evidence.research_run_id, 73)  # type: ignore[union-attr]
        self.assertEqual(evidence.analysis_input_hash, "c" * 64)  # type: ignore[union-attr]
        self.assertEqual(json.loads(output.getvalue())["outcome"], "success")

    def test_observation_status_reports_remaining_real_time(self) -> None:
        report = _quality_report(elapsed_seconds=3600)
        repository = SimpleNamespace(
            policy=_OBSERVATION_POLICY,
            build_quality_report=Mock(return_value=report),
            finalization_remaining_seconds=Mock(return_value=0),
        )
        output = io.StringIO()
        with (
            patch(
                "crypto_agent.cli._observation_repository",
                return_value=repository,
            ),
            patch(
                "crypto_agent.cli._observation_database_now",
                return_value=_DATABASE_NOW,
            ),
            redirect_stdout(output),
        ):
            exit_code = main(
                ["observe-status", "--campaign-id", _CAMPAIGN_ID]
            )

        self.assertEqual(exit_code, 0)
        repository.build_quality_report.assert_called_once_with(
            _CAMPAIGN_ID,
            observed_until=_DATABASE_NOW,
        )
        payload = json.loads(output.getvalue())
        self.assertEqual(
            payload["remaining_seconds"],
            _OBSERVATION_POLICY.minimum_elapsed_seconds - 3600,
        )
        self.assertFalse(payload["final_report_eligible"])
        self.assertEqual(payload["overall_status"], "NOT_OBSERVED")

    def test_final_observation_report_is_blocked_before_28_real_days(self) -> None:
        report = _quality_report(elapsed_seconds=3600)
        repository = SimpleNamespace(
            policy=_OBSERVATION_POLICY,
            build_quality_report=Mock(return_value=report),
            finalization_remaining_seconds=Mock(return_value=0),
            store_final_report=Mock(),
        )
        output = io.StringIO()
        with (
            patch(
                "crypto_agent.cli._observation_repository",
                return_value=repository,
            ),
            patch(
                "crypto_agent.cli._observation_database_now",
                return_value=_DATABASE_NOW,
            ),
            patch(
                "crypto_agent.cli._checkpoint_and_sync_bridge_observation_events"
            ) as sync,
            redirect_stdout(output),
        ):
            exit_code = main(
                ["observe-report", "--campaign-id", _CAMPAIGN_ID]
            )

        self.assertEqual(exit_code, 1)
        repository.store_final_report.assert_not_called()
        sync.assert_not_called()
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["error_code"], "OBSERVATION_WINDOW_INCOMPLETE")
        self.assertGreater(payload["remaining_seconds"], 0)

    def test_final_observation_report_waits_for_final_slot_grace(self) -> None:
        report = _quality_report(
            elapsed_seconds=_OBSERVATION_POLICY.minimum_elapsed_seconds,
        )
        repository = SimpleNamespace(
            policy=_OBSERVATION_POLICY,
            build_quality_report=Mock(return_value=report),
            finalization_remaining_seconds=Mock(return_value=300),
            store_final_report=Mock(),
        )
        output = io.StringIO()
        with (
            patch(
                "crypto_agent.cli._observation_repository",
                return_value=repository,
            ),
            patch(
                "crypto_agent.cli._observation_database_now",
                return_value=_DATABASE_NOW,
            ),
            patch(
                "crypto_agent.cli._checkpoint_and_sync_bridge_observation_events"
            ) as sync,
            redirect_stdout(output),
        ):
            exit_code = main(["observe-report", "--campaign-id", _CAMPAIGN_ID])

        self.assertEqual(exit_code, 1)
        repository.store_final_report.assert_not_called()
        sync.assert_not_called()
        payload = json.loads(output.getvalue())
        self.assertEqual(
            payload["error_code"],
            "OBSERVATION_FINALIZATION_GRACE_INCOMPLETE",
        )
        self.assertEqual(payload["remaining_seconds"], 300)
        self.assertEqual(payload["observation_remaining_seconds"], 0)
        self.assertEqual(payload["finalization_grace_remaining_seconds"], 300)

    def test_final_observation_report_stores_quality_failure_and_returns_nonzero(
        self,
    ) -> None:
        report = _quality_report(
            elapsed_seconds=_OBSERVATION_POLICY.minimum_elapsed_seconds,
            overall_status=CriterionStatus.FAIL,
        )
        repository = SimpleNamespace(
            policy=_OBSERVATION_POLICY,
            build_quality_report=Mock(return_value=report),
            finalization_remaining_seconds=Mock(return_value=0),
            store_final_report=Mock(return_value=report.report_hash_sha256),
        )
        output = io.StringIO()
        with (
            patch(
                "crypto_agent.cli._observation_repository",
                return_value=repository,
            ),
            patch(
                "crypto_agent.cli._observation_database_now",
                return_value=_DATABASE_NOW,
            ),
            patch(
                "crypto_agent.cli._checkpoint_and_sync_bridge_observation_events",
                return_value=_checkpoint_receipt(),
            ) as sync,
            redirect_stdout(output),
        ):
            exit_code = main(
                ["observe-report", "--campaign-id", _CAMPAIGN_ID]
            )

        self.assertEqual(exit_code, 3)
        repository.store_final_report.assert_called_once_with(
            report,
            bridge_checkpoint=_bridge_checkpoint(),
        )
        sync.assert_called_once_with(_CAMPAIGN_ID)
        self.assertEqual(repository.build_quality_report.call_count, 2)
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["status"], "stored")
        self.assertEqual(payload["overall_status"], "FAIL")
        self.assertFalse(payload["v1_gate_passed"])

    def test_final_report_freezes_database_cutoff_before_bridge_sync(self) -> None:
        calls: list[str] = []
        report = _quality_report(
            elapsed_seconds=_OBSERVATION_POLICY.minimum_elapsed_seconds,
            overall_status=CriterionStatus.FAIL,
        )
        repository = SimpleNamespace(
            policy=_OBSERVATION_POLICY,
            build_quality_report=Mock(return_value=report),
            finalization_remaining_seconds=Mock(return_value=0),
            store_final_report=Mock(return_value=report.report_hash_sha256),
        )
        with (
            patch(
                "crypto_agent.cli._observation_repository",
                return_value=repository,
            ),
            patch(
                "crypto_agent.cli._observation_database_now",
                side_effect=lambda _repository: (
                    calls.append("cutoff") or _DATABASE_NOW
                ),
            ),
            patch(
                "crypto_agent.cli._checkpoint_and_sync_bridge_observation_events",
                side_effect=lambda _campaign_id: (
                    calls.append("sync") or _checkpoint_receipt()
                ),
            ),
            redirect_stdout(io.StringIO()),
        ):
            exit_code = main(["observe-report", "--campaign-id", _CAMPAIGN_ID])

        self.assertEqual(exit_code, 3)
        self.assertEqual(calls, ["cutoff", "sync"])
        self.assertEqual(
            repository.build_quality_report.call_args_list,
            [
                mock_call(_CAMPAIGN_ID, observed_until=_DATABASE_NOW),
                mock_call(_CAMPAIGN_ID, observed_until=_DATABASE_NOW),
            ],
        )

    def test_observation_runtime_failures_are_secret_safe_stable_json(self) -> None:
        for command in (
            ["observe-start", "--campaign-id", _CAMPAIGN_ID],
            ["observe-status", "--campaign-id", _CAMPAIGN_ID],
            ["observe-report", "--campaign-id", _CAMPAIGN_ID],
        ):
            with self.subTest(command=command[0]):
                output = io.StringIO()
                with (
                    patch(
                        "crypto_agent.cli._observation_repository",
                        side_effect=RuntimeError("opaque-secret-marker"),
                    ),
                    redirect_stdout(output),
                ):
                    exit_code = main(command)

                self.assertNotEqual(exit_code, 0)
                payload = json.loads(output.getvalue())
                self.assertEqual(payload["status"], "error")
                self.assertTrue(payload["error_code"].startswith("OBSERVATION_"))
                self.assertNotIn("opaque", output.getvalue())
                self.assertNotIn("secret", output.getvalue())

    def test_invalid_monitor_poll_interval_fails_before_runtime_access(self) -> None:
        for value in ("nan", "inf", "0", "86401"):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                self.subTest(value=value),
                patch("crypto_agent.cli.build_orchestrator") as build_orchestrator,
                patch(
                    "crypto_agent.cli.build_monitoring_repository"
                ) as build_monitoring_repository,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                exit_code = main(["monitor", "--poll-seconds", value])

            self.assertEqual(exit_code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("POLL_INTERVAL_INVALID", stderr.getvalue())
            build_orchestrator.assert_not_called()
            build_monitoring_repository.assert_not_called()

    def test_invalid_delivery_poll_interval_fails_before_repository_access(self) -> None:
        for value in ("nan", "inf", "0", "0.49", "86401"):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                self.subTest(value=value),
                patch(
                    "crypto_agent.cli.build_monitoring_repository"
                ) as build_monitoring_repository,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                exit_code = main(["deliver-alerts", "--poll-seconds", value])

            self.assertEqual(exit_code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("POLL_INTERVAL_INVALID", stderr.getvalue())
            build_monitoring_repository.assert_not_called()

    def test_delivery_status_uses_stderr_without_contaminating_alert_stdout(self) -> None:
        class DeliveryRepository:
            @staticmethod
            def deliver_one(output: io.StringIO) -> SimpleNamespace:
                print('{"schema_version":1,"alert_key":"alert-1"}', file=output)
                return SimpleNamespace(
                    status="delivered",
                    alert_key="alert-1",
                    attempt_no=1,
                    error_code=None,
                )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch(
                "crypto_agent.cli.build_monitoring_repository",
                return_value=DeliveryRepository(),
            ),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            exit_code = main(["deliver-alerts"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            stdout.getvalue(), '{"schema_version":1,"alert_key":"alert-1"}\n'
        )
        self.assertNotIn("delivery_status", stdout.getvalue())
        self.assertIn('"delivery_status": "delivered"', stderr.getvalue())
        self.assertIn('"attempt_no": 1', stderr.getvalue())

    def test_delivery_and_status_factory_failures_are_stable_json(self) -> None:
        cases = (
            (
                "deliver-alerts",
                PostgresUnavailableError("opaque-postgres-secret-marker"),
                "stderr",
                "ALERT_DELIVERY_FAILED",
            ),
            (
                "deliver-alerts",
                MonitoringError("opaque-monitoring-secret-marker"),
                "stderr",
                "ALERT_DELIVERY_FAILED",
            ),
            (
                "monitoring-status",
                PostgresUnavailableError("opaque-postgres-secret-marker"),
                "stdout",
                "MONITORING_UNAVAILABLE",
            ),
            (
                "monitoring-status",
                MonitoringError("opaque-monitoring-secret-marker"),
                "stdout",
                "MONITORING_UNAVAILABLE",
            ),
        )
        for command, exception, output_channel, expected_code in cases:
            with self.subTest(command=command, exception=type(exception).__name__):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with (
                    patch(
                        "crypto_agent.cli.build_monitoring_repository",
                        side_effect=exception,
                    ),
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                ):
                    exit_code = main([command])

                self.assertNotEqual(exit_code, 0)
                selected_output = stdout if output_channel == "stdout" else stderr
                payload = json.loads(selected_output.getvalue())
                self.assertEqual(payload["status"], "error")
                self.assertEqual(payload["error_code"], expected_code)
                combined = stdout.getvalue() + stderr.getvalue()
                self.assertNotIn("opaque", combined)
                self.assertNotIn("secret", combined)

    def test_non_delivered_outcomes_never_return_cli_success(self) -> None:
        class DeliveryRepository:
            def __init__(self, result: DeliveryResult) -> None:
                self.result = result

            def deliver_one(self, output: io.StringIO) -> DeliveryResult:
                del output
                return self.result

        for outcome in ("retryable_failure", "permanent_failure", "expired"):
            with self.subTest(outcome=outcome):
                stdout = io.StringIO()
                stderr = io.StringIO()
                result = DeliveryResult(
                    status=outcome,
                    alert_key="alert-1",
                    attempt_no=1,
                    error_code="STABLE_DELIVERY_FAILURE",
                )
                with (
                    patch(
                        "crypto_agent.cli.build_monitoring_repository",
                        return_value=DeliveryRepository(result),
                    ),
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                ):
                    exit_code = main(["deliver-alerts"])

                self.assertEqual(exit_code, 1)
                self.assertEqual(stdout.getvalue(), "")
                status_payload = json.loads(stderr.getvalue())
                self.assertEqual(status_payload["delivery_status"], outcome)
                self.assertEqual(
                    status_payload["error_code"], "STABLE_DELIVERY_FAILURE"
                )

    def test_database_failure_never_prints_dsn(self) -> None:
        secret = "cli-secret-value"
        output = io.StringIO()
        environment = {"CRYPTO_AGENT_POSTGRES_DSN": f"opaque-{secret}-dsn"}
        with (
            patch.dict(os.environ, environment, clear=False),
            patch("crypto_agent.postgres.importlib.import_module", return_value=_BrokenDriver),
            redirect_stdout(output),
        ):
            exit_code = main(["db", "plan"])

        payload = output.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertIn('"status": "error"', payload)
        self.assertIn("PostgreSQL is unavailable", payload)
        self.assertNotIn(secret, payload)
        self.assertNotIn("opaque-driver-sensitive-marker", payload)

    def test_runtime_cli_never_reads_scenario_verifier_database_dsn(self) -> None:
        cli_source = Path("src/crypto_agent/cli.py").read_text(encoding="utf-8")
        self.assertNotIn("CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN", cli_source)
        with patch.dict(
            os.environ,
            {
                "CRYPTO_AGENT_EVIDENCE_VERIFIER_TOKEN": "v" * 32,
                "CRYPTO_AGENT_EVIDENCE_VERIFIER_URL": "http://127.0.0.1:8791",
            },
            clear=False,
        ):
            client = _scenario_evidence_repository()
        self.assertEqual(type(client).__name__, "EvidenceVerifierClient")

    def test_compose_bootstrap_does_not_bypass_migration_ledger(self) -> None:
        compose = Path("compose.yaml").read_text(encoding="utf-8")
        self.assertIn("127.0.0.1:5432:5432", compose)
        self.assertIn("db/schema.sql:/docker-entrypoint-initdb.d", compose)
        self.assertNotIn("0011_canonical_candle_provenance.sql:/docker-entrypoint", compose)

        cli_source = Path("src/crypto_agent/cli.py").read_text(encoding="utf-8")
        self.assertIn('"missing_seeds": list(health.missing_seeds)', cli_source)
        self.assertIn('"missing_triggers": list(health.missing_triggers)', cli_source)


if __name__ == "__main__":
    unittest.main()

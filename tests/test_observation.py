from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from crypto_agent.observation import (
    CriterionResult,
    CriterionStatus,
    ObservationBridgeCheckpoint,
    ObservationCampaign,
    ObservationCycle,
    ObservationCycleExecution,
    ObservationError,
    ObservationEvidence,
    ObservationQualityReport,
    ObservationRepository,
    ObservationSessionEvent,
    deterministic_observation_trace_id,
    evaluate_observation,
)
from crypto_agent.observation_policy import ObservationPolicy
from tests.db_fakes import FakeConnection, SQLStep

NOW = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
CAMPAIGN_ID = "7bf3c831-ae63-4d32-b750-1d694c1de236"
CHECKPOINT_EVENT_ID = "6e1808e6-f5cb-43e2-aef4-9f1470c6f510"


def _policy() -> ObservationPolicy:
    return ObservationPolicy.load(Path("configs/observation_policy.v1.json"))


def _campaign() -> ObservationCampaign:
    return ObservationCampaign(
        campaign_id=CAMPAIGN_ID,
        started_at=NOW,
        planned_ends_at=NOW + timedelta(hours=672),
        cycle_interval_seconds=3600,
        code_commit_hash="a" * 64,
        t4_protocol_commit_hash="b" * 64,
        runtime_config_hash="c" * 64,
        bridge_evidence_key_fingerprint="e" * 64,
        scope_manifest=("BTC/USD:240m", "ETH/USD:240m"),
    )


def _campaign_row(database_now: datetime) -> tuple[object, ...]:
    campaign = _campaign()
    policy = _policy()
    return (
        "live_t4",
        campaign.started_at,
        campaign.planned_ends_at,
        campaign.cycle_interval_seconds,
        policy.policy_id,
        policy.policy_hash_sha256,
        campaign.code_commit_hash,
        campaign.t4_protocol_commit_hash,
        campaign.runtime_config_hash,
        campaign.bridge_evidence_key_fingerprint,
        list(campaign.scope_manifest),
        campaign.scope_manifest_hash_sha256,
        campaign.frozen_baseline_hash_sha256(policy),
        database_now,
    )


def _complete_aggregate() -> tuple[object, ...]:
    return (1344, 1344, 1344, 1344, 1344, 2, 3600.0, 1.2, 2.1, 0, 0, 0, 0, 0, 0)


def _final_store_steps(*, include_insert: bool) -> list[SQLStep]:
    steps = [
        SQLStep("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
        SQLStep("SELECT pg_advisory_xact_lock"),
        SQLStep(
            "FROM crypto_agent.t4_observation_campaigns",
            [_campaign_row(NOW + timedelta(hours=673))],
        ),
        SQLStep("WITH campaign AS", [_complete_aggregate()]),
        SQLStep("SELECT scenario_code, BOOL_AND", []),
        SQLStep(
            "FROM crypto_agent.t4_bridge_observation_events",
            [(CHECKPOINT_EVENT_ID,)],
        ),
    ]
    if include_insert:
        steps.append(SQLStep("INSERT INTO crypto_agent.t4_observation_quality_reports"))
    return steps


def _checkpoint() -> ObservationBridgeCheckpoint:
    return ObservationBridgeCheckpoint(
        campaign_id=CAMPAIGN_ID,
        action_request_id="31f5ef33-d35e-4d97-a599-f861eaa9ed4f",
        sequence_no=19,
        event_hash_sha256="f" * 64,
    )


def _canonical_final_report() -> ObservationQualityReport:
    policy = _policy()
    campaign = _campaign()
    return evaluate_observation(
        replace(
            _evidence(),
            frozen_baseline_hash_sha256=campaign.frozen_baseline_hash_sha256(policy),
            scenario_results=(),
        ),
        policy,
    )


def _evidence() -> ObservationEvidence:
    policy = _policy()
    return ObservationEvidence(
        campaign_id=CAMPAIGN_ID,
        environment="live_t4",
        started_at=NOW,
        observed_until=NOW + timedelta(hours=672),
        frozen_baseline_hash_sha256="d" * 64,
        baseline_hash_matches=True,
        policy_hash_matches=True,
        cycle_interval_seconds=3600,
        scope_count=2,
        covered_scope_count=2,
        expected_cycles=1344,
        attempted_cycles=1344,
        successful_cycles=1344,
        linked_successful_cycles=1344,
        schema_valid_successful_cycles=1344,
        maximum_unexplained_gap_seconds=3600.0,
        bridge_rtt_p95_seconds=1.2,
        bridge_rtt_p99_seconds=2.1,
        read_only_violations=0,
        order_route_attempts=0,
        secret_leaks=0,
        integrity_failures=0,
        fail_closed_violations=0,
        prohibited_data_mode_violations=0,
        scenario_results=tuple((name, True) for name in policy.mandatory_scenarios),
    )


class ObservationEvaluationTests(unittest.TestCase):
    def test_gate_passes_only_when_every_mandatory_criterion_passes(self) -> None:
        report = evaluate_observation(_evidence(), _policy())

        self.assertEqual(report.overall_status, CriterionStatus.PASS)
        self.assertTrue(report.v1_gate_passed)
        self.assertGreater(len(report.criteria), 20)
        self.assertTrue(
            all(item.status is CriterionStatus.PASS for item in report.criteria)
        )
        self.assertEqual(len(report.report_hash_sha256), 64)
        self.assertEqual(report.report_hash_sha256, report.report_hash_sha256)

    def test_real_time_cannot_be_backfilled_or_declared_complete_early(self) -> None:
        early = replace(_evidence(), observed_until=NOW + timedelta(hours=671, minutes=59))
        report = evaluate_observation(early, _policy())
        elapsed = next(
            item for item in report.criteria if item.criterion_id == "real_elapsed_time"
        )

        self.assertEqual(elapsed.status, CriterionStatus.NOT_OBSERVED)
        self.assertEqual(report.overall_status, CriterionStatus.NOT_OBSERVED)
        self.assertFalse(report.v1_gate_passed)

    def test_missing_mandatory_scenario_is_not_observed_not_silently_passed(self) -> None:
        evidence = replace(
            _evidence(),
            scenario_results=tuple(
                item for item in _evidence().scenario_results if item[0] != "rate_limit"
            ),
        )
        report = evaluate_observation(evidence, _policy())
        scenario = next(
            item for item in report.criteria if item.criterion_id == "scenario_rate_limit"
        )

        self.assertEqual(scenario.status, CriterionStatus.NOT_OBSERVED)
        self.assertEqual(report.overall_status, CriterionStatus.NOT_OBSERVED)
        self.assertFalse(report.v1_gate_passed)

    def test_any_safety_or_integrity_violation_hard_fails_the_gate(self) -> None:
        cases = (
            {"environment": "t4_simulator"},
            {"baseline_hash_matches": False},
            {"read_only_violations": 1},
            {"order_route_attempts": 1},
            {"secret_leaks": 1},
            {"integrity_failures": 1},
            {"fail_closed_violations": 1},
            {"prohibited_data_mode_violations": 1},
            {"linked_successful_cycles": 1343},
            {"schema_valid_successful_cycles": 1343},
            {"maximum_unexplained_gap_seconds": 10_801.0},
            {"bridge_rtt_p99_seconds": 10.001},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                report = evaluate_observation(replace(_evidence(), **changes), _policy())
                self.assertEqual(report.overall_status, CriterionStatus.FAIL)
                self.assertFalse(report.v1_gate_passed)

    def test_rates_and_empty_measurements_are_evaluated_fail_closed(self) -> None:
        low_rates = replace(
            _evidence(),
            attempted_cycles=1330,
            successful_cycles=1310,
            linked_successful_cycles=1310,
            schema_valid_successful_cycles=1310,
        )
        self.assertEqual(
            evaluate_observation(low_rates, _policy()).overall_status,
            CriterionStatus.FAIL,
        )
        empty = replace(
            _evidence(),
            scope_count=0,
            covered_scope_count=0,
            expected_cycles=0,
            attempted_cycles=0,
            successful_cycles=0,
            linked_successful_cycles=0,
            schema_valid_successful_cycles=0,
            maximum_unexplained_gap_seconds=None,
            bridge_rtt_p95_seconds=None,
            bridge_rtt_p99_seconds=None,
        )
        self.assertEqual(
            evaluate_observation(empty, _policy()).overall_status,
            CriterionStatus.NOT_OBSERVED,
        )


class ObservationRepositoryTests(unittest.TestCase):
    def test_finalization_remaining_seconds_includes_final_slot_grace(self) -> None:
        observed_at = NOW + timedelta(hours=672)
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("planned_ends_at", [(3600,)]),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())

        remaining = repository.finalization_remaining_seconds(
            CAMPAIGN_ID,
            observed_at=observed_at,
        )

        self.assertEqual(remaining, 3600)
        query, params = connection.scripted_cursor.executions[-1]
        self.assertIn("make_interval(secs => cycle_interval_seconds)", query)
        self.assertIn("planned_ends_at", query)
        self.assertEqual(params, (observed_at, CAMPAIGN_ID))

    def test_observation_trace_is_stable_per_frozen_slot(self) -> None:
        first = deterministic_observation_trace_id(
            CAMPAIGN_ID, "BTC/USD:240m", 7
        )

        self.assertEqual(
            first,
            deterministic_observation_trace_id(
                CAMPAIGN_ID, "BTC/USD:240m", 7
            ),
        )
        self.assertNotEqual(
            first,
            deterministic_observation_trace_id(
                CAMPAIGN_ID, "BTC/USD:240m", 8
            ),
        )

    def test_campaign_persists_only_a_frozen_hash_baseline(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SELECT clock_timestamp()", [(NOW,)]),
                SQLStep("INSERT INTO crypto_agent.t4_observation_campaigns"),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())

        baseline_hash = repository.create_campaign(_campaign())

        self.assertEqual(baseline_hash, _campaign().frozen_baseline_hash_sha256(_policy()))
        self.assertTrue(connection.committed)
        _, params = connection.scripted_cursor.executions[1]
        self.assertNotIn("password", str(params).lower())

    def test_campaign_rejects_a_caller_supplied_historical_start(self) -> None:
        connection = FakeConnection(
            [SQLStep("SELECT clock_timestamp()", [(NOW + timedelta(days=28),)])]
        )
        repository = ObservationRepository(lambda: connection, _policy())

        with self.assertRaisesRegex(ObservationError, "database clock"):
            repository.create_campaign(_campaign())

        self.assertTrue(connection.rolled_back)

    def test_campaign_rejects_simulator_and_short_planned_window(self) -> None:
        repository = ObservationRepository(lambda: FakeConnection([]), _policy())
        for campaign in (
            replace(_campaign(), environment="t4_simulator"),
            replace(_campaign(), planned_ends_at=NOW + timedelta(hours=671)),
        ):
            with self.subTest(campaign=campaign), self.assertRaises(ObservationError):
                repository.create_campaign(campaign)

    def test_quality_report_is_assembled_from_database_ledger_only(self) -> None:
        campaign = _campaign()
        policy = _policy()
        baseline = campaign.frozen_baseline_hash_sha256(policy)
        campaign_row = (
            "live_t4",
            campaign.started_at,
            campaign.planned_ends_at,
            campaign.cycle_interval_seconds,
            policy.policy_id,
            policy.policy_hash_sha256,
            campaign.code_commit_hash,
            campaign.t4_protocol_commit_hash,
            campaign.runtime_config_hash,
            campaign.bridge_evidence_key_fingerprint,
            list(campaign.scope_manifest),
            campaign.scope_manifest_hash_sha256,
            baseline,
            NOW + timedelta(hours=673),
        )
        aggregate = (
            1344,
            1344,
            1344,
            1344,
            1344,
            2,
            3600.0,
            1.2,
            2.1,
            0,
            0,
            0,
            0,
            0,
            0,
        )
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("FROM crypto_agent.t4_observation_campaigns", [campaign_row]),
                SQLStep("WITH campaign AS", [aggregate]),
                SQLStep(
                    "SELECT scenario_code, BOOL_AND",
                    [(scenario, True) for scenario in policy.mandatory_scenarios],
                ),
            ]
        )
        repository = ObservationRepository(lambda: connection, policy)

        report = repository.build_quality_report(
            CAMPAIGN_ID,
            observed_until=NOW + timedelta(hours=672),
        )

        self.assertEqual(report.overall_status, CriterionStatus.PASS)
        self.assertTrue(report.v1_gate_passed)
        self.assertEqual(report.frozen_baseline_hash_sha256, baseline)
        aggregate_query = connection.scripted_cursor.executions[2][0]
        self.assertIn("bridge_safety AS", aggregate_query)
        self.assertIn("scenario_safety AS", aggregate_query)
        self.assertIn("event_type = 'outbound_rejected'", aggregate_query)
        self.assertIn("campaign_id IS DISTINCT FROM", aggregate_query)
        self.assertIn("alert_delivery_attempts", aggregate_query)
        self.assertTrue(connection.committed)

    def test_database_time_prevents_a_future_cutoff_from_faking_elapsed_time(self) -> None:
        campaign = _campaign()
        policy = _policy()
        row = (
            "live_t4",
            campaign.started_at,
            campaign.planned_ends_at,
            campaign.cycle_interval_seconds,
            policy.policy_id,
            policy.policy_hash_sha256,
            campaign.code_commit_hash,
            campaign.t4_protocol_commit_hash,
            campaign.runtime_config_hash,
            campaign.bridge_evidence_key_fingerprint,
            list(campaign.scope_manifest),
            campaign.scope_manifest_hash_sha256,
            campaign.frozen_baseline_hash_sha256(policy),
            NOW + timedelta(hours=100),
        )
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("FROM crypto_agent.t4_observation_campaigns", [row]),
            ]
        )
        repository = ObservationRepository(lambda: connection, policy)

        with self.assertRaisesRegex(ObservationError, "future"):
            repository.build_quality_report(
                CAMPAIGN_ID,
                observed_until=NOW + timedelta(hours=672),
            )

        self.assertTrue(connection.rolled_back)

    def test_cycle_record_uses_per_scope_hash_chain_and_exact_links(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("FROM crypto_agent.t4_observation_cycles", rows=[]),
                SQLStep("INSERT INTO crypto_agent.t4_observation_research_inputs"),
                SQLStep("INSERT INTO crypto_agent.t4_observation_cycles"),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())
        cycle = ObservationCycle(
            campaign_id=CAMPAIGN_ID,
            scope_key="BTC/USD:240m",
            sequence_no=1,
            expected_at=NOW,
            started_at=NOW,
            finished_at=NOW + timedelta(seconds=1),
            outcome="success",
            t4_batch_id=41,
            research_run_id=73,
            analysis_input_hash="d" * 64,
            bridge_schema_version=5,
            raw_payload_hash="e" * 64,
            bridge_rtt_milliseconds=900,
            trace_id=deterministic_observation_trace_id(
                CAMPAIGN_ID, "BTC/USD:240m", 1
            ),
        )

        content_hash = repository.record_cycle(cycle)

        self.assertEqual(len(content_hash), 64)
        self.assertTrue(connection.committed)
        link_params = connection.scripted_cursor.executions[3][1]
        insert_params = connection.scripted_cursor.executions[4][1]
        self.assertIn(41, link_params)
        self.assertIn(73, link_params)
        self.assertIn("d" * 64, link_params)
        self.assertIn(41, insert_params)
        self.assertIn(73, insert_params)
        self.assertIn("e" * 64, insert_params)

    def test_due_cycle_uses_database_schedule_and_exact_success_links(self) -> None:
        database_now = NOW + timedelta(hours=1, minutes=5)
        connection = FakeConnection(
            [
                SQLStep("SET LOCAL lock_timeout"),
                SQLStep("SET LOCAL idle_in_transaction_session_timeout"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns",
                    [
                        (
                            "live_t4",
                            NOW,
                            NOW + timedelta(hours=672),
                            3600,
                            ["BTC/USD:240m", "ETH/USD:240m"],
                            database_now,
                        )
                    ],
                ),
                SQLStep("FROM crypto_agent.t4_observation_cycles", rows=[]),
                SQLStep("SELECT clock_timestamp()", [(database_now + timedelta(seconds=1),)]),
                SQLStep("INSERT INTO crypto_agent.t4_observation_research_inputs"),
                SQLStep("INSERT INTO crypto_agent.t4_observation_cycles"),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())
        plans = []

        def execute(plan):  # type: ignore[no-untyped-def]
            plans.append(plan)
            return ObservationCycleExecution(
                t4_batch_id=41,
                research_run_id=73,
                analysis_input_hash="d" * 64,
                bridge_schema_version=5,
                raw_payload_hash="e" * 64,
                bridge_rtt_milliseconds=900,
                trace_id=plan.trace_id,
                environment="live_t4",
                external_delivery_eligible=True,
            )

        result = repository.run_cycle(CAMPAIGN_ID, "BTC/USD:240m", execute)

        self.assertEqual(result.outcome, "success")
        self.assertEqual(result.sequence_no, 1)
        self.assertEqual(result.expected_at, NOW + timedelta(hours=1))
        self.assertEqual(result.t4_batch_id, 41)
        self.assertEqual(result.research_run_id, 73)
        self.assertEqual(len(plans), 1)
        insert_params = connection.scripted_cursor.executions[-1][1]
        self.assertIn(plans[0].trace_id, insert_params)
        self.assertIn("e" * 64, insert_params)

    def test_same_cutoff_batches_keep_distinct_exact_research_run_links(self) -> None:
        links: list[tuple[object, ...]] = []
        for scope_key, batch_id, run_id, input_hash in (
            ("BTC/USD:240m", 41, 73, "d" * 64),
            ("ETH/USD:240m", 42, 74, "c" * 64),
        ):
            connection = FakeConnection(
                [
                    SQLStep("SELECT pg_advisory_xact_lock"),
                    SQLStep("SELECT pg_advisory_xact_lock"),
                    SQLStep("FROM crypto_agent.t4_observation_cycles", rows=[]),
                    SQLStep("INSERT INTO crypto_agent.t4_observation_research_inputs"),
                    SQLStep("INSERT INTO crypto_agent.t4_observation_cycles"),
                ]
            )
            repository = ObservationRepository(
                lambda current=connection: current,
                _policy(),
            )
            repository.record_cycle(
                ObservationCycle(
                    campaign_id=CAMPAIGN_ID,
                    scope_key=scope_key,
                    sequence_no=1,
                    expected_at=NOW,
                    started_at=NOW,
                    finished_at=NOW + timedelta(seconds=1),
                    outcome="success",
                    t4_batch_id=batch_id,
                    research_run_id=run_id,
                    analysis_input_hash=input_hash,
                    bridge_schema_version=5,
                    raw_payload_hash=("a" if batch_id == 41 else "b") * 64,
                    bridge_rtt_milliseconds=100,
                    trace_id=deterministic_observation_trace_id(
                        CAMPAIGN_ID, scope_key, 1
                    ),
                )
            )
            params = connection.scripted_cursor.executions[3][1]
            assert isinstance(params, tuple)
            links.append(params)

        self.assertEqual(
            [(params[1], params[4], params[5], params[7]) for params in links],
            [
                ("BTC/USD:240m", 41, 73, "d" * 64),
                ("ETH/USD:240m", 42, 74, "c" * 64),
            ],
        )

    def test_retry_before_next_slot_never_executes_or_duplicates_cycle(self) -> None:
        database_now = NOW + timedelta(hours=1, minutes=10)
        connection = FakeConnection(
            [
                SQLStep("SET LOCAL lock_timeout"),
                SQLStep("SET LOCAL idle_in_transaction_session_timeout"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns",
                    [
                        (
                            "live_t4",
                            NOW,
                            NOW + timedelta(hours=672),
                            3600,
                            ["BTC/USD:240m"],
                            database_now,
                        )
                    ],
                ),
                SQLStep(
                    "FROM crypto_agent.t4_observation_cycles",
                    [(1, "f" * 64)],
                ),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())
        executed = []

        with self.assertRaisesRegex(ObservationError, "not due") as raised:
            repository.run_cycle(
                CAMPAIGN_ID,
                "BTC/USD:240m",
                lambda plan: executed.append(plan),  # type: ignore[arg-type,return-value]
            )

        self.assertEqual(raised.exception.code, "OBSERVATION_CYCLE_NOT_DUE")
        self.assertEqual(executed, [])
        self.assertTrue(connection.rolled_back)

    def test_cycle_preserves_a_safe_typed_provider_failure_code(self) -> None:
        database_now = NOW + timedelta(hours=1, minutes=5)
        connection = FakeConnection(
            [
                SQLStep("SET LOCAL lock_timeout"),
                SQLStep("SET LOCAL idle_in_transaction_session_timeout"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns",
                    [
                        (
                            "live_t4",
                            NOW,
                            NOW + timedelta(hours=672),
                            3600,
                            ["BTC/USD:240m"],
                            database_now,
                        )
                    ],
                ),
                SQLStep("FROM crypto_agent.t4_observation_cycles", rows=[]),
                SQLStep(
                    "SELECT clock_timestamp()",
                    [(database_now + timedelta(seconds=1),)],
                ),
                SQLStep("INSERT INTO crypto_agent.t4_observation_cycles"),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())

        def execute(plan):  # type: ignore[no-untyped-def]
            del plan
            raise ObservationError("safe failure", code="T4_STALE_DATA")

        result = repository.run_cycle(CAMPAIGN_ID, "BTC/USD:240m", execute)

        self.assertEqual(result.outcome, "failure")
        self.assertEqual(result.error_code, "T4_STALE_DATA")
        insert_params = connection.scripted_cursor.executions[-1][1]
        self.assertIn("T4_STALE_DATA", insert_params)

    def test_scope_outside_frozen_manifest_fails_before_execution(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SET LOCAL lock_timeout"),
                SQLStep("SET LOCAL idle_in_transaction_session_timeout"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns",
                    [
                        (
                            "live_t4",
                            NOW,
                            NOW + timedelta(hours=672),
                            3600,
                            ["BTC/USD:240m"],
                            NOW + timedelta(hours=1),
                        )
                    ],
                )
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())

        with self.assertRaises(ObservationError) as raised:
            repository.run_cycle(
                CAMPAIGN_ID,
                "ETH/USD:240m",
                lambda plan: None,  # type: ignore[arg-type,return-value]
            )

        self.assertEqual(raised.exception.code, "OBSERVATION_SCOPE_NOT_FROZEN")

    def test_recovery_appends_missed_slots_before_current_live_attempt(self) -> None:
        database_now = NOW + timedelta(hours=3, minutes=5)
        connection = FakeConnection(
            [
                SQLStep("SET LOCAL lock_timeout"),
                SQLStep("SET LOCAL idle_in_transaction_session_timeout"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns",
                    [
                        (
                            "live_t4",
                            NOW,
                            NOW + timedelta(hours=672),
                            3600,
                            ["BTC/USD:240m"],
                            database_now,
                        )
                    ],
                ),
                SQLStep("FROM crypto_agent.t4_observation_cycles", rows=[]),
                SQLStep("INSERT INTO crypto_agent.t4_observation_cycles"),
                SQLStep("INSERT INTO crypto_agent.t4_observation_cycles"),
                SQLStep("SELECT clock_timestamp()", [(database_now + timedelta(seconds=1),)]),
                SQLStep("INSERT INTO crypto_agent.t4_observation_research_inputs"),
                SQLStep("INSERT INTO crypto_agent.t4_observation_cycles"),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())

        def execute(plan):  # type: ignore[no-untyped-def]
            return ObservationCycleExecution(
                t4_batch_id=42,
                research_run_id=74,
                analysis_input_hash="b" * 64,
                bridge_schema_version=5,
                raw_payload_hash="a" * 64,
                bridge_rtt_milliseconds=100,
                trace_id=plan.trace_id,
                environment="live_t4",
                external_delivery_eligible=True,
            )

        result = repository.run_cycle(CAMPAIGN_ID, "BTC/USD:240m", execute)

        self.assertEqual(result.outcome, "success")
        self.assertEqual(result.sequence_no, 3)
        self.assertEqual(result.missed_cycles_recorded, 2)
        inserts = [
            params
            for query, params in connection.scripted_cursor.executions
            if "INSERT INTO crypto_agent.t4_observation_cycles" in query
        ]
        self.assertEqual([params[6] for params in inserts], ["missed", "missed", "success"])
        self.assertEqual([params[2] for params in inserts], [1, 2, 3])
        self.assertEqual([params[8] for params in inserts[:2]], [None, None])

    def test_cycle_sequence_conflict_rolls_back_without_inserting(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("FROM crypto_agent.t4_observation_cycles", rows=[(2, "f" * 64)]),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())
        missed = ObservationCycle(
            campaign_id=CAMPAIGN_ID,
            scope_key="BTC/USD:240m",
            sequence_no=2,
            expected_at=NOW,
            outcome="missed",
        )

        with self.assertRaisesRegex(ObservationError, "sequence"):
            repository.record_cycle(missed)
        self.assertTrue(connection.rolled_back)
        self.assertFalse(connection.committed)

    def test_session_scenarios_are_append_only_hash_chained_events(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep("FROM crypto_agent.t4_observation_session_events", rows=[]),
                SQLStep("INSERT INTO crypto_agent.t4_observation_session_events"),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())
        event = ObservationSessionEvent(
            campaign_id=CAMPAIGN_ID,
            sequence_no=1,
            event_at=NOW,
            event_type="reconnect_succeeded",
            outcome="fail",
            scenario_code="reconnect",
            detail_code="EVIDENCE_REFERENCE_UNAVAILABLE",
        )

        content_hash = repository.record_session_event(event)

        self.assertEqual(len(content_hash), 64)
        self.assertTrue(connection.committed)

    def test_scenario_pass_is_rejected_until_objective_evidence_is_verifiable(self) -> None:
        repository = ObservationRepository(lambda: FakeConnection([]), _policy())
        event = ObservationSessionEvent(
            campaign_id=CAMPAIGN_ID,
            sequence_no=1,
            event_at=NOW,
            event_type="reconnect_succeeded",
            outcome="pass",
            scenario_code="reconnect",
            detail_code="SESSION_RESTORED",
        )

        with self.assertRaisesRegex(ObservationError, "objective references"):
            repository.record_session_event(event)

    def test_final_report_cannot_be_persisted_before_real_672_hours(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns",
                    [_campaign_row(NOW + timedelta(hours=672))],
                ),
                SQLStep("WITH campaign AS", [_complete_aggregate()]),
                SQLStep("SELECT scenario_code, BOOL_AND", []),
            ]
        )
        repository = ObservationRepository(lambda: connection, _policy())
        early = evaluate_observation(
            replace(_evidence(), observed_until=NOW + timedelta(hours=671)),
            _policy(),
        )

        with self.assertRaisesRegex(ObservationError, "final-slot grace"):
            repository.store_final_report(early, bridge_checkpoint=_checkpoint())
        self.assertTrue(connection.rolled_back)

    def test_final_report_storage_preserves_not_observed_as_gate_false(self) -> None:
        connection = FakeConnection(_final_store_steps(include_insert=True))
        repository = ObservationRepository(lambda: connection, _policy())
        report = _canonical_final_report()

        report_hash = repository.store_final_report(
            report,
            bridge_checkpoint=_checkpoint(),
        )

        self.assertEqual(report_hash, report.report_hash_sha256)
        self.assertEqual(report.overall_status, CriterionStatus.NOT_OBSERVED)
        self.assertFalse(report.v1_gate_passed)
        self.assertTrue(connection.committed)

    def test_final_report_rejects_a_forged_public_pass_payload(self) -> None:
        connection = FakeConnection(_final_store_steps(include_insert=False))
        repository = ObservationRepository(lambda: connection, _policy())
        forged = ObservationQualityReport(
            campaign_id=CAMPAIGN_ID,
            generated_at=NOW + timedelta(hours=672),
            observed_until=NOW + timedelta(hours=672),
            elapsed_seconds=_policy().minimum_elapsed_seconds,
            overall_status=CriterionStatus.PASS,
            v1_gate_passed=True,
            policy_id=_policy().policy_id,
            policy_hash_sha256=_policy().policy_hash_sha256,
            frozen_baseline_hash_sha256="0" * 64,
            criteria=(
                CriterionResult(
                    "trust_me",
                    CriterionStatus.PASS,
                    True,
                    True,
                ),
            ),
        )

        with self.assertRaisesRegex(ObservationError, "canonical database ledger"):
            repository.store_final_report(forged, bridge_checkpoint=_checkpoint())

        self.assertTrue(connection.rolled_back)

    def test_final_store_delegates_verified_gate_enforcement_to_database(
        self,
    ) -> None:
        policy = _policy()
        campaign = _campaign()
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"),
                SQLStep("SELECT pg_advisory_xact_lock"),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns",
                    [_campaign_row(NOW + timedelta(hours=673))],
                ),
                SQLStep("WITH campaign AS", [_complete_aggregate()]),
                SQLStep(
                    "SELECT scenario_code, BOOL_AND",
                    [(scenario, True) for scenario in policy.mandatory_scenarios],
                ),
                SQLStep(
                    "FROM crypto_agent.t4_bridge_observation_events",
                    [(CHECKPOINT_EVENT_ID,)],
                ),
                SQLStep("INSERT INTO crypto_agent.t4_observation_quality_reports"),
            ]
        )
        repository = ObservationRepository(lambda: connection, policy)
        report = evaluate_observation(
            replace(
                _evidence(),
                frozen_baseline_hash_sha256=(
                    campaign.frozen_baseline_hash_sha256(policy)
                ),
            ),
            policy,
        )

        report_hash = repository.store_final_report(
            report,
            bridge_checkpoint=_checkpoint(),
        )

        self.assertEqual(report_hash, report.report_hash_sha256)
        self.assertTrue(connection.committed)


class ObservationMigrationContractTests(unittest.TestCase):
    def test_schema_v5_separates_registry_product_and_opaque_market_identity(self) -> None:
        sql = Path("db/migrations/0017_t4_observation_campaign.sql").read_text(
            encoding="utf-8"
        )

        self.assertIn("CHECK (bridge_schema_version IN (2, 3, 4, 5))", sql)
        self.assertIn("ADD COLUMN environment text", sql)
        self.assertIn("ADD COLUMN exchange_id text", sql)
        self.assertIn("ADD COLUMN active_market_id text", sql)
        self.assertIn("ADD COLUMN rolled_from_market_id text", sql)
        self.assertIn("RENAME COLUMN market_id TO registry_market_id", sql)
        self.assertIn("ADD COLUMN market_id text", sql)
        self.assertIn("ADD COLUMN product_contract_id text", sql)
        self.assertIn("ADD COLUMN from_market_id text", sql)
        self.assertIn("ADD COLUMN to_market_id text", sql)
        self.assertIn("active_market_id !~ '[[:cntrl:]]'", sql)
        self.assertIn("market_id !~ '[[:cntrl:]]'", sql)
        self.assertIn("environment IN ('t4_simulator', 'live_t4')", sql)
        self.assertIn("observation cycle cannot be predeclared or backfilled", sql)
        self.assertIn("trace_id                    uuid", sql)
        self.assertIn("campaign_row.scope_manifest ? NEW.scope_key", sql)
        self.assertIn("missed observation cycle is not yet elapsed", sql)
        self.assertIn("ingestion_row.requested_as_of IS DISTINCT FROM NEW.expected_at", sql)
        self.assertIn("ingestion_row.raw_payload_hash IS DISTINCT FROM NEW.raw_payload_hash", sql)
        self.assertIn("research_row.run_kind IS DISTINCT FROM 'live_t4_analysis'", sql)
        self.assertIn("CREATE TABLE crypto_agent.t4_observation_research_inputs", sql)
        self.assertIn(
            "research_row.dataset_manifest_hash IS DISTINCT FROM NEW.analysis_input_hash",
            sql,
        )
        self.assertIn("observation exact batch-to-run linkage is invalid", sql)
        self.assertIn("AND gap_explanation_code IS NULL", sql)

    def test_observation_ledger_is_append_only_and_final_gate_is_fail_closed(self) -> None:
        sql = Path("db/migrations/0017_t4_observation_campaign.sql").read_text(
            encoding="utf-8"
        )

        for table_name in (
            "t4_observation_campaigns",
            "t4_observation_cycles",
            "t4_observation_session_events",
            "t4_observation_quality_reports",
        ):
            self.assertIn(f"CREATE TABLE crypto_agent.{table_name}", sql)
        self.assertIn("planned_ends_at >= started_at + interval '672 hours'", sql)
        self.assertIn("(report->>'elapsed_seconds')::bigint >= 2419200", sql)
        self.assertIn("overall_status IN ('PASS', 'FAIL', 'NOT_OBSERVED')", sql)
        self.assertIn("overall_status = 'PASS' AND v1_gate_passed", sql)
        self.assertIn("observation campaign start must use the current database clock", sql)
        self.assertIn("observation campaign is already finalized", sql)
        self.assertIn("scenario_code IS NULL OR outcome = 'fail'", sql)
        self.assertIn("required_criterion_ids constant text[]", sql)
        self.assertIn("criterion_ids IS DISTINCT FROM required_criterion_ids", sql)
        self.assertIn("verified scenario evidence is not implemented", sql)
        self.assertIn("NEW.observation_policy_hash IS DISTINCT FROM", sql)
        self.assertIn("NEW.frozen_baseline_hash IS DISTINCT FROM", sql)
        self.assertIn("campaign_row.planned_ends_at", sql)
        self.assertIn("campaign_row.cycle_interval_seconds", sql)
        self.assertIn("forbid_append_only_change", sql)


if __name__ == "__main__":
    unittest.main()

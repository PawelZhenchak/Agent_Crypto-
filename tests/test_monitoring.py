from __future__ import annotations

import io
import json
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from crypto_agent.deadline import (
    AnalysisDeadlineExceeded,
    analysis_deadline_scope,
    ensure_analysis_deadline,
    remaining_analysis_timeout,
)
from crypto_agent.domain import (
    DataQualityReport,
    Decision,
    ResearchReport,
    RiskAssessment,
)
from crypto_agent.monitoring import (
    MonitoringError,
    MonitoringReceipt,
    MonitoringRepository,
    _alert_payload,
    _eligible_for_delivery,
    _hash,
    _retry_delay,
    _safe_report_artifact,
    run_monitored_analysis,
)
from crypto_agent.monitoring_policy import MonitoringPolicy
from tests.db_fakes import FakeConnection, SQLStep

NOW = datetime(2026, 8, 12, 12, 0, tzinfo=UTC)
TRACE_ID = "7bf3c831-ae63-4d32-b750-1d694c1de236"
SECRET_MARKER = "opaque-monitoring-secret-marker"


def _policy() -> MonitoringPolicy:
    return MonitoringPolicy.load(Path("configs/monitoring_policy.v1.json"))


def _report(
    *,
    decision: Decision = Decision.ALERT,
    risk_decision: Decision = Decision.ALERT,
    vetoed: bool = False,
    expires_at: datetime | None = None,
    instrument_id: str = "plus500_t4_futures_v1:BTC/USD:1440m",
    provider_error_code: str | None = None,
    trace_id: str = TRACE_ID,
) -> ResearchReport:
    expiry = expires_at or NOW + timedelta(hours=1)
    risk = RiskAssessment(
        assessment_id="risk-assessment-v1",
        policy_id="v1-read-only-plus500-t4-2026-08-11",
        policy_hash="a" * 64,
        as_of=NOW - timedelta(minutes=1),
        expires_at=expiry,
        input_fingerprint_sha256="b" * 64,
        decision=risk_decision,
        vetoed=vetoed,
        flags=("SAFE_TEST_FLAG",) if vetoed else (),
        reasons=(),
    )
    return ResearchReport(
        decision_id="97a18460-b73e-47b3-b74a-02c881cd92e5",
        trace_id=trace_id,
        as_of=NOW - timedelta(minutes=1),
        expires_at=expiry,
        asset_id="bip122:000000000019d6689c085ae165831e93:native",
        instrument_id=instrument_id,
        horizon="1d",
        decision=decision,
        reason_codes=("VOLUME_ANOMALY",),
        regime_probabilities=(),
        model_version="deterministic-futures-research-v2",
        policy_version="v1-read-only-plus500-t4-2026-08-11",
        data_snapshot_id=f"sha256:{'b' * 64}",
        thesis=f"untrusted thesis {SECRET_MARKER}",
        counter_evidence=(f"untrusted counter {SECRET_MARKER}",),
        scenarios=({"untrusted": SECRET_MARKER},),
        invalidation_conditions=(f"untrusted invalidation {SECRET_MARKER}",),
        data_quality=DataQualityReport(
            score=1.0,
            sample_count=120,
            flags=(),
            critical_flags=(),
            newest_observed_at=NOW - timedelta(minutes=2),
            newest_available_at=NOW - timedelta(minutes=1),
        ),
        risk=risk,
        sources=(),
        metrics=None,
        futures_metrics=None,
        narrative=f"untrusted narrative {SECRET_MARKER}",
        metadata={
            "system_version": "0.7.0-plus500-t4-v1",
            "mode": "V1_READ_ONLY",
            "execution_enabled": False,
            "not_financial_advice": True,
            "v1_gate_passed": False,
            "plus500_t4_source_attested": True,
            "external_delivery_eligible": True,
            "t4_bridge_schema_version": 5,
            "t4_environment": "live_t4",
            "futures_gate_passed": True,
            "futures_policy_id": "futures-analysis-v1-2026-08-12",
            "futures_policy_hash_sha256": "c" * 64,
            "input_fingerprint_sha256": "b" * 64,
            "input_candle_count": 120,
            "analysis_candle_count": 120,
            "provider_error_code": provider_error_code,
            "provider_diagnostics": {"bridge_token": SECRET_MARKER},
            "raw_payload": SECRET_MARKER,
            "authorization": SECRET_MARKER,
        },
    )


class _MonitoringStub:
    def __init__(self) -> None:
        self.reports: list[tuple[ResearchReport, str]] = []
        self.failures: list[dict[str, object]] = []

    def record_report(
        self,
        report: ResearchReport,
        *,
        operation: str,
    ) -> MonitoringReceipt:
        self.reports.append((report, operation))
        return MonitoringReceipt(report.trace_id, 17, "completed", False, None)

    def record_failure(self, **kwargs: object) -> MonitoringReceipt:
        self.failures.append(dict(kwargs))
        return MonitoringReceipt(str(kwargs["trace_id"]), 18, "failed", False, None)


class _DeadlineAwareMonitoringStub(_MonitoringStub):
    def __init__(self) -> None:
        super().__init__()
        self.failure_budget: float | None = None

    def record_failure(self, **kwargs: object) -> MonitoringReceipt:
        ensure_analysis_deadline()
        self.failure_budget = remaining_analysis_timeout()
        return super().record_failure(**kwargs)


def _stored_delivery_row(
    payload: dict[str, object],
    *,
    payload_hash: str | None = None,
    now: datetime,
    policy: MonitoringPolicy | None = None,
) -> tuple[object, ...]:
    policy = policy or _policy()
    alert_key = f"research-alert:{'a' * 64}"
    resolved_payload_hash = payload_hash or _hash(payload)
    idempotency_key = _hash(
        {
            "alert_key": alert_key,
            "channel": "stdout_json",
            "destination": "process_stdout",
            "payload_hash": resolved_payload_hash,
        }
    )
    available_at = now - timedelta(seconds=1)
    expires_at = now + timedelta(hours=1)
    outbox_hash = _hash(
        {
            "alert_id": 202,
            "idempotency_key": idempotency_key,
            "payload_hash": resolved_payload_hash,
            "monitoring_policy_id": policy.policy_id,
            "monitoring_policy_hash": policy.policy_hash_sha256,
            "retention_days": policy.retention_days,
            "available_at": available_at.isoformat(),
            "expires_at": expires_at.isoformat(),
            "max_attempts": policy.max_attempts,
        }
    )
    return (
        101,
        202,
        alert_key,
        "stdout_json",
        "process_stdout",
        idempotency_key,
        policy.policy_id,
        policy.policy_hash_sha256,
        policy.retention_days,
        payload,
        resolved_payload_hash,
        available_at,
        expires_at,
        3,
        None,
        None,
        None,
        None,
        outbox_hash,
    )


class _PartialWriteStream:
    def __init__(self) -> None:
        self.written = ""

    def write(self, value: str) -> int:
        self.written += value[:12]
        raise OSError("opaque-partial-write-secret-marker")

    def flush(self) -> None:
        raise AssertionError("flush must not follow a failed write")


class _ShortWriteStream:
    def __init__(self) -> None:
        self.written = ""

    def write(self, value: str) -> int:
        self.written = value[:-1]
        return len(value) - 1

    def flush(self) -> None:
        raise AssertionError("flush must not follow a partial write")


class _SlowWriteStream:
    def write(self, value: str) -> int:
        time.sleep(5)
        return len(value)

    def flush(self) -> None:
        raise AssertionError("flush must not follow a timed-out write")


class MonitoringTests(unittest.TestCase):
    def test_only_eligible_live_t4_alert_is_deliverable(self) -> None:
        live = _report()
        self.assertTrue(_eligible_for_delivery(live, operation="live_t4_analysis", now=NOW))

        cases = {
            "no_signal": replace(
                live,
                decision=Decision.NO_SIGNAL,
                risk=replace(live.risk, decision=Decision.NO_SIGNAL),
            ),
            "veto": replace(live, risk=replace(live.risk, vetoed=True)),
            "synthetic": replace(
                live,
                instrument_id="synthetic_fixture_v1:BTC/USD:1440m",
            ),
            "expired": _report(expires_at=NOW),
            "provider_error": _report(provider_error_code="T4_BRIDGE_UNAVAILABLE"),
            "missing_external_delivery_eligibility": replace(
                live,
                metadata={
                    key: value
                    for key, value in live.metadata.items()
                    if key != "external_delivery_eligible"
                },
            ),
            "legacy_schema_v3": replace(
                live,
                metadata={**live.metadata, "t4_bridge_schema_version": 4},
            ),
            "fixture_environment": replace(
                live,
                metadata={**live.metadata, "t4_environment": "fixture"},
            ),
            "malformed_instrument": replace(
                live,
                instrument_id="plus500_t4_futures_v1:BTC/USD:1440m:forged",
            ),
            "instrument_horizon_mismatch": replace(live, horizon="4h"),
            "malformed_horizon": replace(live, horizon="30m"),
        }
        for name, report in cases.items():
            with self.subTest(name=name):
                self.assertFalse(
                    _eligible_for_delivery(
                        report,
                        operation="live_t4_analysis",
                        now=NOW,
                    )
                )

        self.assertFalse(_eligible_for_delivery(live, operation="analyze_replay", now=NOW))

    def test_safe_report_artifact_uses_redacted_allowlist(self) -> None:
        artifact = _safe_report_artifact(_report())
        serialized = json.dumps(artifact, sort_keys=True)

        self.assertTrue(artifact["read_only"])
        self.assertNotIn(SECRET_MARKER, serialized)
        self.assertNotIn("provider_diagnostics", serialized)
        self.assertNotIn("raw_payload", serialized)
        self.assertNotIn("narrative", serialized)
        self.assertNotIn("authorization", serialized)

    def test_alert_payload_has_an_exact_safe_allowlist(self) -> None:
        payload = _alert_payload(_report(), _policy())
        expected_keys = {
            "schema_version",
            "alert_type",
            "decision",
            "decision_id",
            "trace_id",
            "asset_id",
            "instrument_id",
            "horizon",
            "as_of",
            "expires_at",
            "reason_codes",
            "data_snapshot_id",
            "policy_id",
            "policy_hash_sha256",
            "futures_policy_id",
            "futures_policy_hash_sha256",
            "environment",
            "external_delivery",
            "read_only",
            "execution_enabled",
            "not_financial_advice",
            "monitoring_policy_id",
            "monitoring_policy_hash_sha256",
            "retention_days",
        }
        self.assertEqual(set(payload), expected_keys)
        self.assertIs(payload["read_only"], True)
        self.assertIs(payload["execution_enabled"], False)
        self.assertIs(payload["external_delivery"], False)
        self.assertEqual(payload["environment"], "live_t4")
        self.assertNotIn(SECRET_MARKER, json.dumps(payload, sort_keys=True))

    def test_retry_delay_is_deterministic_and_bounded(self) -> None:
        policy = _policy()
        for attempt_no in range(1, policy.max_attempts + 3):
            with self.subTest(attempt_no=attempt_no):
                first = _retry_delay(
                    policy,
                    attempt_no=attempt_no,
                    idempotency_key="d" * 64,
                )
                second = _retry_delay(
                    policy,
                    attempt_no=attempt_no,
                    idempotency_key="d" * 64,
                )
                self.assertEqual(first, second)
                self.assertGreaterEqual(first, policy.retry_base_seconds)
                self.assertLessEqual(first, policy.retry_max_seconds)

    def test_stored_alert_payload_is_validated_before_stdout_write(self) -> None:
        for name in ("unexpected_field", "payload_hash_mismatch"):
            with self.subTest(name=name):
                now = datetime.now(UTC)
                valid_payload = _alert_payload(
                    _report(expires_at=now + timedelta(hours=1)),
                    _policy(),
                )
                payload = (
                    {**valid_payload, "raw_secret": SECRET_MARKER}
                    if name == "unexpected_field"
                    else valid_payload
                )
                payload_hash = _hash(payload) if name == "unexpected_field" else "0" * 64
                connection = FakeConnection(
                    [
                        SQLStep("SELECT set_config"),
                        SQLStep(
                            "FROM crypto_agent.alert_delivery_outbox outbox",
                            [
                                _stored_delivery_row(
                                    payload,
                                    payload_hash=payload_hash,
                                    now=now,
                                )
                            ],
                        ),
                    ]
                )
                output = io.StringIO()
                repository = MonitoringRepository(
                    lambda current=connection: current,
                    _policy(),
                )

                with self.assertRaises(MonitoringError):
                    repository.deliver_one(output, now=now)

                self.assertEqual(output.getvalue(), "")
                self.assertTrue(connection.rolled_back)
                self.assertFalse(connection.committed)
                self.assertFalse(connection.scripted_cursor.steps)

    def test_terminal_previous_attempt_is_rejected_before_stdout_write(self) -> None:
        now = datetime.now(UTC)
        payload = _alert_payload(
            _report(expires_at=now + timedelta(hours=1)),
            _policy(),
        )
        row = list(_stored_delivery_row(payload, now=now))
        row[14] = 1
        row[15] = "delivered"
        row[16] = None
        row[17] = "d" * 64
        connection = FakeConnection(
            [
                SQLStep("SELECT set_config"),
                SQLStep(
                    "FROM crypto_agent.alert_delivery_outbox outbox",
                    [tuple(row)],
                ),
            ]
        )
        output = io.StringIO()
        repository = MonitoringRepository(lambda: connection, _policy())

        with self.assertRaises(MonitoringError):
            repository.deliver_one(output, now=now)

        self.assertEqual(output.getvalue(), "")
        self.assertTrue(connection.rolled_back)

    def test_due_retry_preserves_hash_chain_and_can_be_delivered(self) -> None:
        now = datetime.now(UTC)
        policy = _policy()
        payload = _alert_payload(
            _report(expires_at=now + timedelta(hours=1)),
            policy,
        )
        row = list(_stored_delivery_row(payload, now=now, policy=policy))
        row[14] = 1
        row[15] = "retryable_failure"
        row[16] = now - timedelta(seconds=1)
        row[17] = "d" * 64
        connection = FakeConnection(
            [
                SQLStep("SELECT set_config"),
                SQLStep(
                    "FROM crypto_agent.alert_delivery_outbox outbox",
                    [tuple(row)],
                ),
                SQLStep("INSERT INTO crypto_agent.alert_delivery_attempts"),
                SQLStep("SELECT source_id FROM crypto_agent.data_sources", [(17,)]),
                SQLStep("INSERT INTO crypto_agent.alert_events"),
            ]
        )
        output = io.StringIO()
        repository = MonitoringRepository(lambda: connection, policy)

        result = repository.deliver_one(output, now=now)

        self.assertEqual(result.status, "delivered")
        self.assertEqual(result.attempt_no, 2)
        attempt_params = connection.scripted_cursor.executions[2][1]
        self.assertIsInstance(attempt_params, tuple)
        self.assertEqual(attempt_params[1], 2)
        self.assertEqual(attempt_params[8], "d" * 64)
        self.assertTrue(output.getvalue().endswith("\n"))
        self.assertTrue(connection.committed)

    def test_partial_stdout_write_is_never_recorded_as_delivered(self) -> None:
        now = datetime.now(UTC)
        payload = _alert_payload(
            _report(expires_at=now + timedelta(hours=1)),
            _policy(),
        )
        connection = FakeConnection(
            [
                SQLStep("SELECT set_config"),
                SQLStep(
                    "FROM crypto_agent.alert_delivery_outbox outbox",
                    [_stored_delivery_row(payload, now=now)],
                ),
                SQLStep("INSERT INTO crypto_agent.alert_delivery_attempts"),
                SQLStep("SELECT source_id FROM crypto_agent.data_sources", [(17,)]),
                SQLStep("INSERT INTO crypto_agent.alert_events"),
            ]
        )
        stream = _PartialWriteStream()
        repository = MonitoringRepository(lambda: connection, _policy())

        result = repository.deliver_one(stream, now=now)  # type: ignore[arg-type]

        self.assertEqual(result.status, "retryable_failure")
        self.assertNotEqual(result.status, "delivered")
        self.assertNotIn("\n", stream.written)
        attempt_params = connection.scripted_cursor.executions[2][1]
        self.assertIsInstance(attempt_params, tuple)
        self.assertEqual(attempt_params[2], "retryable_failure")
        self.assertTrue(connection.committed)

    def test_short_write_without_exception_is_never_recorded_as_delivered(self) -> None:
        now = datetime.now(UTC)
        policy = _policy()
        payload = _alert_payload(
            _report(expires_at=now + timedelta(hours=1)),
            policy,
        )
        connection = FakeConnection(
            [
                SQLStep("SELECT set_config"),
                SQLStep(
                    "FROM crypto_agent.alert_delivery_outbox outbox",
                    [_stored_delivery_row(payload, now=now, policy=policy)],
                ),
                SQLStep("INSERT INTO crypto_agent.alert_delivery_attempts"),
                SQLStep("SELECT source_id FROM crypto_agent.data_sources", [(17,)]),
                SQLStep("INSERT INTO crypto_agent.alert_events"),
            ]
        )
        stream = _ShortWriteStream()
        repository = MonitoringRepository(lambda: connection, policy)

        result = repository.deliver_one(stream, now=now)  # type: ignore[arg-type]

        self.assertEqual(result.status, "retryable_failure")
        self.assertNotEqual(result.status, "delivered")
        self.assertFalse(stream.written.endswith("\n"))
        attempt_params = connection.scripted_cursor.executions[2][1]
        self.assertIsInstance(attempt_params, tuple)
        self.assertEqual(attempt_params[2], "retryable_failure")
        self.assertTrue(connection.committed)

    def test_closed_stream_value_error_records_bounded_failure(self) -> None:
        now = datetime.now(UTC)
        policy = _policy()
        payload = _alert_payload(
            _report(expires_at=now + timedelta(hours=1)),
            policy,
        )
        connection = FakeConnection(
            [
                SQLStep("SELECT set_config"),
                SQLStep(
                    "FROM crypto_agent.alert_delivery_outbox outbox",
                    [_stored_delivery_row(payload, now=now, policy=policy)],
                ),
                SQLStep("INSERT INTO crypto_agent.alert_delivery_attempts"),
                SQLStep("SELECT source_id FROM crypto_agent.data_sources", [(17,)]),
                SQLStep("INSERT INTO crypto_agent.alert_events"),
            ]
        )
        stream = io.StringIO()
        stream.close()
        repository = MonitoringRepository(lambda: connection, policy)

        result = repository.deliver_one(stream, now=now)

        self.assertEqual(result.status, "retryable_failure")
        self.assertEqual(result.error_code, "STDOUT_WRITE_FAILED")
        attempt_params = connection.scripted_cursor.executions[2][1]
        self.assertIsInstance(attempt_params, tuple)
        self.assertEqual(attempt_params[2], "retryable_failure")
        self.assertTrue(connection.committed)

    def test_slow_stream_is_bounded_by_attempt_timeout(self) -> None:
        now = datetime.now(UTC)
        policy = replace(_policy(), attempt_timeout_seconds=0.1)
        payload = _alert_payload(
            _report(expires_at=now + timedelta(hours=1)),
            policy,
        )
        connection = FakeConnection(
            [
                SQLStep("SELECT set_config"),
                SQLStep(
                    "FROM crypto_agent.alert_delivery_outbox outbox",
                    [_stored_delivery_row(payload, now=now, policy=policy)],
                ),
                SQLStep("INSERT INTO crypto_agent.alert_delivery_attempts"),
                SQLStep("SELECT source_id FROM crypto_agent.data_sources", [(17,)]),
                SQLStep("INSERT INTO crypto_agent.alert_events"),
            ]
        )
        repository = MonitoringRepository(lambda: connection, policy)

        started = time.monotonic()
        result = repository.deliver_one(_SlowWriteStream(), now=now)  # type: ignore[arg-type]
        elapsed = time.monotonic() - started

        self.assertLess(elapsed, 1.0)
        self.assertEqual(result.status, "retryable_failure")
        self.assertEqual(result.error_code, "STDOUT_WRITE_FAILED")
        attempt_params = connection.scripted_cursor.executions[2][1]
        self.assertIsInstance(attempt_params, tuple)
        self.assertEqual(attempt_params[2], "retryable_failure")
        self.assertTrue(connection.committed)

    def test_monitored_analysis_propagates_trace_and_records_report(self) -> None:
        monitoring = _MonitoringStub()
        calls: list[dict[str, Any]] = []

        def analyze(**kwargs: Any) -> ResearchReport:
            calls.append(dict(kwargs))
            return _report(trace_id=str(kwargs["trace_id"]))

        report, receipt = run_monitored_analysis(
            analyze,
            monitoring,  # type: ignore[arg-type]
            operation="live_t4_analysis",
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=NOW,
            limit=120,
            trace_id=TRACE_ID,
        )

        self.assertEqual(calls[0]["trace_id"], TRACE_ID)
        self.assertEqual(report.trace_id, TRACE_ID)
        self.assertEqual(receipt.trace_id, TRACE_ID)
        self.assertEqual(monitoring.reports, [(report, "live_t4_analysis")])
        self.assertEqual(monitoring.failures, [])

    def test_monitored_analysis_rejects_report_trace_mismatch(self) -> None:
        monitoring = _MonitoringStub()
        different_trace = "d3786f00-56d5-4fcb-956c-ee7eaf7890e8"

        def analyze(**kwargs: Any) -> ResearchReport:
            del kwargs
            return _report(trace_id=different_trace)

        with self.assertRaises(MonitoringError):
            run_monitored_analysis(
                analyze,
                monitoring,  # type: ignore[arg-type]
                operation="live_t4_analysis",
                symbol="BTC/USD",
                interval_minutes=1440,
                trace_id=TRACE_ID,
            )

        self.assertEqual(monitoring.reports, [])
        self.assertEqual(len(monitoring.failures), 1)
        self.assertEqual(monitoring.failures[0]["error_code"], "ANALYSIS_TRACE_MISMATCH")
        self.assertEqual(monitoring.failures[0]["trace_id"], TRACE_ID)

    def test_unexpected_key_error_records_stable_failure(self) -> None:
        monitoring = _MonitoringStub()

        def analyze(**kwargs: Any) -> ResearchReport:
            del kwargs
            raise KeyError("opaque-analysis-secret-marker")

        with self.assertRaises(KeyError):
            run_monitored_analysis(
                analyze,
                monitoring,  # type: ignore[arg-type]
                operation="live_t4_analysis",
                symbol="BTC/USD",
                interval_minutes=1440,
                trace_id=TRACE_ID,
            )

        self.assertEqual(monitoring.reports, [])
        self.assertEqual(len(monitoring.failures), 1)
        self.assertEqual(monitoring.failures[0]["error_code"], "ANALYSIS_UNEXPECTED_FAILURE")
        self.assertNotIn(
            "opaque-analysis-secret-marker",
            json.dumps(monitoring.failures[0], sort_keys=True),
        )

    def test_monitored_analysis_records_stable_failure_code(self) -> None:
        cases = (
            (TimeoutError("sensitive-timeout"), "ANALYSIS_TIMEOUT"),
            (RuntimeError("sensitive-runtime"), "ANALYSIS_FAILED"),
            (ValueError("sensitive-value"), "ANALYSIS_FAILED"),
        )
        for exception, expected_code in cases:
            with self.subTest(exception=type(exception).__name__):
                monitoring = _MonitoringStub()

                def analyze(
                    _exception: Exception = exception,
                    **kwargs: Any,
                ) -> ResearchReport:
                    del kwargs
                    raise _exception

                with self.assertRaises(type(exception)):
                    run_monitored_analysis(
                        analyze,
                        monitoring,  # type: ignore[arg-type]
                        operation="live_t4_analysis",
                        symbol="BTC/USD",
                        interval_minutes=1440,
                        trace_id=TRACE_ID,
                    )

                self.assertEqual(monitoring.reports, [])
                self.assertEqual(len(monitoring.failures), 1)
                failure = monitoring.failures[0]
                self.assertEqual(failure["trace_id"], TRACE_ID)
                self.assertEqual(failure["error_code"], expected_code)
                self.assertEqual(failure["component"], "analysis")
                self.assertEqual(failure["scope"], "BTC/USD:1440")
                self.assertNotIn("sensitive", json.dumps(failure, sort_keys=True))

    def test_expired_analysis_uses_reserved_total_budget_to_record_timeout(
        self,
    ) -> None:
        monitoring = _DeadlineAwareMonitoringStub()
        started = time.monotonic()
        work_deadline = started + 0.01
        failure_deadline = started + 0.25

        def analyze(**kwargs: Any) -> ResearchReport:
            del kwargs
            time.sleep(0.02)
            ensure_analysis_deadline()
            raise AssertionError("expired analysis should not continue")

        with (
            analysis_deadline_scope(work_deadline),
            self.assertRaises(AnalysisDeadlineExceeded),
        ):
            run_monitored_analysis(
                analyze,
                monitoring,  # type: ignore[arg-type]
                operation="live_t4_analysis",
                symbol="BTC/USD",
                interval_minutes=1440,
                trace_id=TRACE_ID,
                failure_deadline_monotonic=failure_deadline,
            )

        self.assertEqual(len(monitoring.failures), 1)
        self.assertEqual(monitoring.failures[0]["error_code"], "ANALYSIS_TIMEOUT")
        self.assertIsNotNone(monitoring.failure_budget)
        assert monitoring.failure_budget is not None
        self.assertGreater(monitoring.failure_budget, 0)
        self.assertLess(time.monotonic(), failure_deadline)

    def test_timeout_recording_failure_never_replaces_primary_timeout(self) -> None:
        primary = AnalysisDeadlineExceeded("primary analysis timeout")

        class _UnavailableMonitoring(_MonitoringStub):
            def record_failure(self, **kwargs: object) -> MonitoringReceipt:
                del kwargs
                raise MonitoringError("monitoring unavailable")

        def analyze(**kwargs: Any) -> ResearchReport:
            del kwargs
            raise primary

        with self.assertRaises(AnalysisDeadlineExceeded) as raised:
            run_monitored_analysis(
                analyze,
                _UnavailableMonitoring(),  # type: ignore[arg-type]
                operation="live_t4_analysis",
                symbol="BTC/USD",
                interval_minutes=1440,
                trace_id=TRACE_ID,
                failure_deadline_monotonic=time.monotonic() + 0.25,
            )

        self.assertIs(raised.exception, primary)


if __name__ == "__main__":
    unittest.main()

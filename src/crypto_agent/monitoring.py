from __future__ import annotations

import hashlib
import json
import os
import re
import select
import signal
import stat
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TextIO, cast
from uuid import UUID, uuid4

from . import __version__
from .deadline import (
    ANALYSIS_TIMEOUT_RECORDING_RESERVE_SECONDS,
    analysis_deadline_scope,
    bounded_analysis_timeout,
    ensure_analysis_deadline,
)
from .domain import Decision, ResearchReport
from .monitoring_policy import MonitoringPolicy
from .postgres import (
    ConnectionFactory,
    DBCursor,
    PostgresError,
    PostgresOperationError,
    cursor,
    transaction,
)

_INTERNAL_SOURCE = "crypto_agent_registry_v1"
_T4_SOURCE = "plus500_t4_futures_v1"
_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_HORIZON_SECONDS = {"4h": 14_400, "1d": 86_400, "1w": 604_800}
_INTERVAL_HORIZONS = {240: "4h", 1440: "1d", 10080: "1w"}
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_ALERT_PAYLOAD_KEYS = frozenset(
    {
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
)
_DELIVERY_SELECT = """
    SELECT outbox.alert_delivery_outbox_id, outbox.alert_id,
           alert.alert_key, outbox.channel, outbox.destination,
           outbox.idempotency_key, outbox.monitoring_policy_id,
           outbox.monitoring_policy_hash, outbox.retention_days,
           outbox.payload, outbox.payload_hash, outbox.available_at,
           outbox.expires_at, outbox.max_attempts, previous.attempt_no,
           previous.outcome, previous.next_attempt_at, previous.content_hash,
           outbox.content_hash
    FROM crypto_agent.alert_delivery_outbox outbox
    JOIN crypto_agent.alerts alert ON alert.alert_id = outbox.alert_id
    LEFT JOIN LATERAL (
        SELECT attempt_no, outcome, next_attempt_at, content_hash
        FROM crypto_agent.alert_delivery_attempts
        WHERE alert_delivery_outbox_id = outbox.alert_delivery_outbox_id
        ORDER BY attempt_no DESC
        LIMIT 1
    ) previous ON TRUE
"""


class MonitoringError(RuntimeError):
    """A monitoring failure whose message is safe for CLI/API output."""


@dataclass(frozen=True, slots=True)
class MonitoringReceipt:
    trace_id: str
    research_run_id: int
    status: str
    alert_enqueued: bool
    alert_key: str | None


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    status: str
    alert_key: str | None = None
    idempotency_key: str | None = None
    attempt_no: int | None = None
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class DashboardData:
    summary: dict[str, object]
    alerts: tuple[dict[str, object], ...]
    incidents: tuple[dict[str, object], ...]


class MonitoringRepository:
    """PostgreSQL source of truth for V1 traces, incidents and alert delivery.

    The repository intentionally activates the event-sourced tables already present
    in ``schema.sql``.  Only the stdout delivery outbox is introduced by migration
    0016.  No raw provider payload, credential, URL, header or exception is accepted
    by this boundary.
    """

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        policy: MonitoringPolicy,
    ) -> None:
        policy.validate()
        self.connection_factory = connection_factory
        self.policy = policy

    def record_report(
        self,
        report: ResearchReport,
        *,
        operation: str,
        recorded_at: datetime | None = None,
    ) -> MonitoringReceipt:
        now = _utc(recorded_at or datetime.now(UTC), "recorded_at")
        operation = _operation(operation)
        _uuid_text(report.trace_id, "trace_id")
        artifact = _safe_report_artifact(report)
        provider_code = _safe_optional_code(report.metadata.get("provider_error_code"))
        status = "degraded" if provider_code else "completed"
        ensure_analysis_deadline()

        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                _begin_monitoring_transaction(db_cursor)
                sources = _source_ids(db_cursor)
                ensure_analysis_deadline()
                run_id = self._insert_run(
                    db_cursor,
                    report=report,
                    operation=operation,
                    status=status,
                    artifact=artifact,
                    source_id=sources[_INTERNAL_SOURCE],
                    recorded_at=now,
                )
                if provider_code:
                    self._open_incident(
                        db_cursor,
                        trace_id=report.trace_id,
                        component="t4_provider",
                        code=provider_code,
                        scope=f"{report.instrument_id}:{report.horizon}",
                        source_id=sources[_INTERNAL_SOURCE],
                        affected_source_id=sources[_T4_SOURCE],
                        detected_at=now,
                    )
                alert_key = None
                alert_enqueued = False
                if _eligible_for_delivery(report, operation=operation, now=now):
                    alert_key, alert_enqueued = self._enqueue_alert(
                        db_cursor,
                        report=report,
                        run_id=run_id,
                        source_id=sources[_INTERNAL_SOURCE],
                        recorded_at=now,
                    )
                ensure_analysis_deadline()
                _refresh_monitoring_transaction_timeout(db_cursor)
        except (MonitoringError, TimeoutError, ValueError):
            raise
        except PostgresError:
            ensure_analysis_deadline()
            raise MonitoringError("Operational monitoring storage is unavailable") from None
        except Exception:
            ensure_analysis_deadline()
            raise MonitoringError("Operational monitoring storage is unavailable") from None

        return MonitoringReceipt(
            trace_id=report.trace_id,
            research_run_id=run_id,
            status=status,
            alert_enqueued=alert_enqueued,
            alert_key=alert_key,
        )

    def record_failure(
        self,
        *,
        trace_id: str,
        operation: str,
        error_code: str,
        component: str,
        scope: str,
        occurred_at: datetime | None = None,
    ) -> MonitoringReceipt:
        _uuid_text(trace_id, "trace_id")
        operation = _operation(operation)
        code = _required_code(error_code)
        component = _bounded_identifier(component, "component")
        scope = _bounded_text(scope, "scope", 200)
        now = _utc(occurred_at or datetime.now(UTC), "occurred_at")
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                _begin_monitoring_transaction(db_cursor)
                sources = _source_ids(db_cursor)
                run_id = self._insert_failed_run(
                    db_cursor,
                    trace_id=trace_id,
                    operation=operation,
                    code=code,
                    source_id=sources[_INTERNAL_SOURCE],
                    occurred_at=now,
                )
                self._open_incident(
                    db_cursor,
                    trace_id=trace_id,
                    component=component,
                    code=code,
                    scope=scope,
                    source_id=sources[_INTERNAL_SOURCE],
                    affected_source_id=(
                        sources[_T4_SOURCE] if component.startswith("t4") else None
                    ),
                    detected_at=now,
                )
        except (MonitoringError, TimeoutError, ValueError):
            raise
        except Exception:
            ensure_analysis_deadline()
            raise MonitoringError("Operational monitoring storage is unavailable") from None
        return MonitoringReceipt(trace_id, run_id, "failed", False, None)

    def dashboard(self, *, limit: int | None = None) -> DashboardData:
        page_size = self._page_size(limit)
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute("SET TRANSACTION READ ONLY")
                _begin_monitoring_transaction(db_cursor)
                db_cursor.execute(
                    """
                        SELECT
                          (SELECT COUNT(*) FROM crypto_agent.research_runs),
                          (SELECT COUNT(*) FROM crypto_agent.alerts
                           WHERE alert_type = 'research_alert_v1'),
                          (SELECT COUNT(*) FROM crypto_agent.alert_delivery_outbox outbox
                           WHERE NOT EXISTS (
                             SELECT 1 FROM crypto_agent.alert_delivery_attempts attempt
                             WHERE attempt.alert_delivery_outbox_id =
                                   outbox.alert_delivery_outbox_id
                               AND attempt.outcome IN (
                                 'delivered', 'permanent_failure', 'expired'
                               )
                           )),
                          (SELECT COUNT(*) FROM crypto_agent.data_quality_incidents),
                          (SELECT MAX(ingested_at)
                           FROM crypto_agent.t4_ingestion_batches),
                          (SELECT MAX(event_at) FROM crypto_agent.research_run_events
                           WHERE event_type = 'completed')
                        """
                )
                summary_row = db_cursor.fetchone()
                if summary_row is None:
                    raise MonitoringError("Monitoring summary is unavailable")
                summary = {
                    "mode": "V1_READ_ONLY",
                    "version": __version__,
                    "v1_gate_passed": False,
                    "execution_enabled": False,
                    "research_runs": _as_int(_value(summary_row, 0) or 0),
                    "research_alerts": _as_int(_value(summary_row, 1) or 0),
                    "pending_delivery": _as_int(_value(summary_row, 2) or 0),
                    "incidents": _as_int(_value(summary_row, 3) or 0),
                    "last_t4_ingest_at": _display(_value(summary_row, 4)),
                    "last_completed_analysis_at": _display(_value(summary_row, 5)),
                }
                alerts = self._read_alerts(db_cursor, page_size)
                incidents = self._read_incidents(db_cursor, page_size)
        except MonitoringError:
            raise
        except Exception:
            raise MonitoringError("Operational monitoring storage is unavailable") from None
        return DashboardData(summary, alerts, incidents)

    def trace(self, trace_id: str) -> dict[str, object] | None:
        _uuid_text(trace_id, "trace_id")
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute("SET TRANSACTION READ ONLY")
                _begin_monitoring_transaction(db_cursor)
                db_cursor.execute(
                    """
                        SELECT research_run_id, run_key, run_kind, as_of_at,
                               requested_at, agent_version
                        FROM crypto_agent.research_runs
                        WHERE trace_key = %s
                        ORDER BY research_run_id DESC
                        LIMIT 1
                        """,
                    (trace_id,),
                )
                run = db_cursor.fetchone()
                if run is None:
                    return None
                run_id = _as_int(_value(run, 0))
                db_cursor.execute(
                    """
                        SELECT event_type, event_at, details
                        FROM crypto_agent.research_run_events
                        WHERE research_run_id = %s
                        ORDER BY event_at, research_run_event_id
                        """,
                    (run_id,),
                )
                events = tuple(
                    {
                        "event_type": str(_value(row, 0)),
                        "event_at": _display(_value(row, 1)),
                        "details": _json_object(_value(row, 2)),
                    }
                    for row in db_cursor.fetchall()
                )
        except ValueError:
            raise
        except Exception:
            raise MonitoringError("Operational monitoring storage is unavailable") from None
        return {
            "research_run_id": run_id,
            "trace_id": trace_id,
            "run_key": str(_value(run, 1)),
            "operation": str(_value(run, 2)),
            "as_of": _display(_value(run, 3)),
            "requested_at": _display(_value(run, 4)),
            "agent_version": str(_value(run, 5)),
            "events": list(events),
            "read_only": True,
        }

    def deliver_one(
        self,
        stream: TextIO,
        *,
        now: datetime | None = None,
    ) -> DeliveryResult:
        attempted_at = _utc(now or datetime.now(UTC), "now")
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                _begin_monitoring_transaction(db_cursor)
                row = self._select_due_delivery(db_cursor, attempted_at)
                if row is None:
                    return DeliveryResult("idle")
                delivery = _DeliveryRow.from_row(
                    row,
                    policy=self.policy,
                    require_due=True,
                )
                attempt_no = delivery.previous_attempt_no + 1
                if attempted_at >= delivery.expires_at:
                    return self._record_delivery_outcome(
                        db_cursor,
                        delivery=delivery,
                        attempt_no=attempt_no,
                        started_at=attempted_at,
                        finished_at=attempted_at,
                        outcome="expired",
                        error_code="ALERT_EXPIRED",
                        next_attempt_at=None,
                    )

                seconds_remaining = (delivery.expires_at - attempted_at).total_seconds()
                safety_margin = 0.05
                if seconds_remaining <= safety_margin:
                    return DeliveryResult("idle", alert_key=delivery.alert_key)
                write_timeout = min(
                    self.policy.attempt_timeout_seconds,
                    seconds_remaining - safety_margin,
                )

                line = _canonical_json(
                    {
                        "channel": delivery.channel,
                        "destination": delivery.destination,
                        "idempotency_key": delivery.idempotency_key,
                        "payload_hash": delivery.payload_hash,
                        "payload": delivery.payload,
                    }
                )
                try:
                    _bounded_stream_write(stream, line + "\n", write_timeout)
                except Exception:
                    finished_at = datetime.now(UTC)
                    delay = _retry_delay(
                        self.policy,
                        attempt_no=attempt_no,
                        idempotency_key=delivery.idempotency_key,
                    )
                    retry_at = finished_at + timedelta(seconds=delay)
                    retryable = (
                        attempt_no < delivery.max_attempts
                        and retry_at < delivery.expires_at
                        and finished_at < delivery.expires_at
                    )
                    return self._record_delivery_outcome(
                        db_cursor,
                        delivery=delivery,
                        attempt_no=attempt_no,
                        started_at=attempted_at,
                        finished_at=finished_at,
                        outcome=("retryable_failure" if retryable else "permanent_failure"),
                        error_code="STDOUT_WRITE_FAILED",
                        next_attempt_at=retry_at if retryable else None,
                    )

                finished_at = datetime.now(UTC)
                if finished_at > delivery.expires_at:
                    return self._record_delivery_outcome(
                        db_cursor,
                        delivery=delivery,
                        attempt_no=attempt_no,
                        started_at=attempted_at,
                        finished_at=finished_at,
                        outcome="permanent_failure",
                        error_code="DELIVERY_WINDOW_EXCEEDED",
                        next_attempt_at=None,
                    )
                return self._record_delivery_outcome(
                    db_cursor,
                    delivery=delivery,
                    attempt_no=attempt_no,
                    started_at=attempted_at,
                    finished_at=finished_at,
                    outcome="delivered",
                    error_code=None,
                    next_attempt_at=None,
                )
        except MonitoringError:
            raise
        except Exception:
            raise MonitoringError("Alert delivery storage is unavailable") from None

    def _insert_run(
        self,
        db_cursor: DBCursor,
        *,
        report: ResearchReport,
        operation: str,
        status: str,
        artifact: dict[str, object],
        source_id: int,
        recorded_at: datetime,
    ) -> int:
        run_key = f"monitoring:{report.trace_id}"
        run_document = {
            "run_key": run_key,
            "operation": operation,
            "as_of": report.as_of.isoformat(),
            "expires_at": report.expires_at.isoformat(),
            "trace_id": report.trace_id,
            "decision_id": report.decision_id,
            "decision": report.decision.value,
            "status": status,
            "snapshot_id": report.data_snapshot_id,
            "policy_hash": report.risk.policy_hash,
            "artifact_hash": _hash(artifact),
            "agent_version": __version__,
        }
        run_hash = _hash(run_document)
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.research_runs (
                run_key, run_kind, as_of_at, requested_at, requested_by,
                agent_version, model_version, model_parameters, code_version,
                prompt_hash, dataset_manifest_hash, trace_key, source_id,
                source_record_key, source_version, revision_no, observed_at,
                available_at, content_hash
            ) VALUES (
                %s, %s, %s, %s, 'crypto-agent-monitor', %s, %s,
                '{}'::jsonb, %s, %s, %s, %s, %s, %s, 'v1', 1, %s, %s, %s
            )
            ON CONFLICT (run_key) DO NOTHING
            RETURNING research_run_id, content_hash
            """,
            (
                run_key,
                operation,
                report.as_of,
                max(report.as_of, recorded_at),
                __version__,
                report.model_version,
                __version__,
                report.risk.policy_hash,
                _sha_from_identifier(report.data_snapshot_id),
                report.trace_id,
                source_id,
                f"monitoring-run:{report.trace_id}",
                report.as_of,
                report.as_of,
                run_hash,
            ),
        )
        row = db_cursor.fetchone()
        if row is None:
            db_cursor.execute(
                """
                SELECT research_run_id, content_hash
                FROM crypto_agent.research_runs WHERE run_key = %s
                """,
                (run_key,),
            )
            row = db_cursor.fetchone()
            if row is None or str(_value(row, 1)) != run_hash:
                raise MonitoringError("Monitoring trace idempotency conflict")
        run_id = _as_int(_value(row, 0))
        self._insert_run_event(
            db_cursor,
            run_id=run_id,
            trace_id=report.trace_id,
            event_type="started",
            details={"operation": operation, "status": "started"},
            source_id=source_id,
            event_at=report.as_of,
        )
        self._insert_artifact(
            db_cursor,
            run_id=run_id,
            trace_id=report.trace_id,
            artifact=artifact,
            source_id=source_id,
            observed_at=report.as_of,
        )
        self._insert_run_event(
            db_cursor,
            run_id=run_id,
            trace_id=report.trace_id,
            event_type="completed",
            details={
                "status": status,
                "decision": report.decision.value,
                "reason_codes": list(report.reason_codes[:50]),
            },
            source_id=source_id,
            event_at=recorded_at,
        )
        return run_id

    def _insert_failed_run(
        self,
        db_cursor: DBCursor,
        *,
        trace_id: str,
        operation: str,
        code: str,
        source_id: int,
        occurred_at: datetime,
    ) -> int:
        run_key = f"monitoring:{trace_id}"
        run_hash = _hash(
            {
                "run_key": run_key,
                "operation": operation,
                "as_of": occurred_at.isoformat(),
                "trace_id": trace_id,
                "failure_code": code,
                "agent_version": __version__,
            }
        )
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.research_runs (
                run_key, run_kind, as_of_at, requested_at, requested_by,
                agent_version, model_parameters, code_version, prompt_hash,
                trace_key, source_id, source_record_key, source_version,
                revision_no, observed_at, available_at, content_hash
            ) VALUES (
                %s, %s, %s, %s, 'crypto-agent-monitor', %s, '{}'::jsonb,
                %s, %s, %s, %s, %s, 'v1', 1, %s, %s, %s
            )
            ON CONFLICT (run_key) DO NOTHING
            RETURNING research_run_id, content_hash
            """,
            (
                run_key,
                operation,
                occurred_at,
                occurred_at,
                __version__,
                __version__,
                "0" * 64,
                trace_id,
                source_id,
                f"monitoring-run:{trace_id}",
                occurred_at,
                occurred_at,
                run_hash,
            ),
        )
        row = db_cursor.fetchone()
        if row is None:
            db_cursor.execute(
                "SELECT research_run_id, content_hash FROM crypto_agent.research_runs "
                "WHERE run_key = %s",
                (run_key,),
            )
            row = db_cursor.fetchone()
            if row is None or str(_value(row, 1)) != run_hash:
                raise MonitoringError("Monitoring trace idempotency conflict")
        run_id = _as_int(_value(row, 0))
        self._insert_run_event(
            db_cursor,
            run_id=run_id,
            trace_id=trace_id,
            event_type="started",
            details={"operation": operation, "status": "started"},
            source_id=source_id,
            event_at=occurred_at,
        )
        self._insert_run_event(
            db_cursor,
            run_id=run_id,
            trace_id=trace_id,
            event_type="failed",
            details={"status": "failed", "error_code": code},
            source_id=source_id,
            event_at=occurred_at,
        )
        return run_id

    @staticmethod
    def _insert_run_event(
        db_cursor: DBCursor,
        *,
        run_id: int,
        trace_id: str,
        event_type: str,
        details: dict[str, object],
        source_id: int,
        event_at: datetime,
    ) -> None:
        event_hash = _hash(
            {
                "run_id": run_id,
                "trace_id": trace_id,
                "event_type": event_type,
                "event_at": event_at.isoformat(),
                "details": details,
            }
        )
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.research_run_events (
                research_run_id, event_type, event_at, details, source_id,
                source_record_key, source_version, revision_no, observed_at,
                available_at, content_hash
            ) VALUES (%s, %s, %s, %s::jsonb, %s, %s, 'v1', 1, %s, %s, %s)
            ON CONFLICT (source_id, source_record_key, revision_no) DO NOTHING
            RETURNING content_hash
            """,
            (
                run_id,
                event_type,
                event_at,
                _canonical_json(details),
                source_id,
                f"monitoring-run-event:{trace_id}:{event_type}",
                event_at,
                event_at,
                event_hash,
            ),
        )
        inserted = db_cursor.fetchone()
        if inserted is None:
            db_cursor.execute(
                """
                SELECT content_hash FROM crypto_agent.research_run_events
                WHERE source_id = %s AND source_record_key = %s AND revision_no = 1
                """,
                (source_id, f"monitoring-run-event:{trace_id}:{event_type}"),
            )
            existing = db_cursor.fetchone()
            if existing is None or str(_value(existing, 0)) != event_hash:
                raise MonitoringError("Monitoring run event idempotency conflict")

    @staticmethod
    def _insert_artifact(
        db_cursor: DBCursor,
        *,
        run_id: int,
        trace_id: str,
        artifact: dict[str, object],
        source_id: int,
        observed_at: datetime,
    ) -> None:
        content_hash = _hash(artifact)
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.research_artifacts (
                research_run_id, artifact_key, artifact_type, schema_version,
                artifact_json, source_id, source_record_key, source_version,
                revision_no, observed_at, available_at, content_hash
            ) VALUES (
                %s, 'safe-report', 'research_report', 'monitoring-v1', %s::jsonb,
                %s, %s, 'v1', 1, %s, %s, %s
            )
            ON CONFLICT (research_run_id, artifact_key, revision_no) DO NOTHING
            RETURNING content_hash
            """,
            (
                run_id,
                _canonical_json(artifact),
                source_id,
                f"monitoring-artifact:{trace_id}",
                observed_at,
                observed_at,
                content_hash,
            ),
        )
        inserted = db_cursor.fetchone()
        if inserted is None:
            db_cursor.execute(
                """
                SELECT content_hash FROM crypto_agent.research_artifacts
                WHERE research_run_id = %s AND artifact_key = 'safe-report'
                  AND revision_no = 1
                """,
                (run_id,),
            )
            existing = db_cursor.fetchone()
            if existing is None or str(_value(existing, 0)) != content_hash:
                raise MonitoringError("Monitoring artifact idempotency conflict")

    def _open_incident(
        self,
        db_cursor: DBCursor,
        *,
        trace_id: str,
        component: str,
        code: str,
        scope: str,
        source_id: int,
        affected_source_id: int | None,
        detected_at: datetime,
    ) -> None:
        fingerprint = _hash({"component": component, "code": code, "scope": scope})
        incident_key = f"operational:{fingerprint}"
        evidence = {"error_code": code, "component": component, "scope": scope}
        content_hash = _hash(
            {
                "incident_key": incident_key,
                "severity": _incident_severity(code),
                "evidence": evidence,
            }
        )
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.data_quality_incidents (
                incident_key, severity, dataset_name, rule_key,
                affected_source_id, affected_record_key, detected_at, summary,
                evidence, source_id, source_record_key, source_version,
                revision_no, observed_at, available_at, content_hash
            ) VALUES (
                %s, %s, 'operational_monitoring', %s, %s, %s, %s, %s,
                %s::jsonb, %s, %s, 'v1', 1, %s, %s, %s
            )
            ON CONFLICT (incident_key) DO NOTHING
            RETURNING data_quality_incident_id
            """,
            (
                incident_key,
                _incident_severity(code),
                code,
                affected_source_id,
                scope,
                detected_at,
                _incident_summary(code),
                _canonical_json(evidence),
                source_id,
                f"operational-incident:{fingerprint}",
                detected_at,
                detected_at,
                content_hash,
            ),
        )
        inserted = db_cursor.fetchone()
        if inserted is None:
            db_cursor.execute(
                """
                SELECT data_quality_incident_id
                FROM crypto_agent.data_quality_incidents
                WHERE incident_key = %s
                """,
                (incident_key,),
            )
            existing = db_cursor.fetchone()
            if existing is None:
                raise MonitoringError("Operational incident could not be recorded")
            incident_id = _as_int(_value(existing, 0))
            event_type = "commented"
        else:
            incident_id = _as_int(_value(inserted, 0))
            event_type = "opened"
        details = {"error_code": code, "trace_id": trace_id}
        event_hash = _hash(
            {
                "incident_id": incident_id,
                "event_type": event_type,
                "trace_id": trace_id,
                "event_at": detected_at.isoformat(),
            }
        )
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.data_quality_incident_events (
                data_quality_incident_id, event_type, event_at, actor_key,
                details, source_id, source_record_key, source_version,
                revision_no, observed_at, available_at, content_hash
            ) VALUES (
                %s, %s, %s, 'crypto-agent-monitor', %s::jsonb, %s, %s,
                'v1', 1, %s, %s, %s
            )
            ON CONFLICT (source_id, source_record_key, revision_no) DO NOTHING
            """,
            (
                incident_id,
                event_type,
                detected_at,
                _canonical_json(details),
                source_id,
                f"operational-incident-event:{incident_key}:{trace_id}",
                detected_at,
                detected_at,
                event_hash,
            ),
        )

    def _enqueue_alert(
        self,
        db_cursor: DBCursor,
        *,
        report: ResearchReport,
        run_id: int,
        source_id: int,
        recorded_at: datetime,
    ) -> tuple[str, bool]:
        symbol = _report_symbol(report)
        db_cursor.execute(
            """
            SELECT market_id, base_asset_id FROM crypto_agent.markets
            WHERE market_key = %s
            """,
            (f"plus500_t4:{symbol.split('/')[0].lower()}-usd-front",),
        )
        scope = db_cursor.fetchone()
        if scope is None:
            raise MonitoringError("Operational alert market scope is unavailable")
        market_id = _as_int(_value(scope, 0))
        asset_id = _as_int(_value(scope, 1))
        payload = _alert_payload(report, self.policy)
        payload_hash = _hash(payload)
        dedupe_hash = _hash(
            {
                "data_snapshot_id": report.data_snapshot_id,
                "policy_hash": report.risk.policy_hash,
                "asset_id": report.asset_id,
                "horizon": report.horizon,
                "reason_codes": list(report.reason_codes),
                "alert_type": "research_alert_v1",
            }
        )
        alert_key = f"research-alert:{dedupe_hash}"
        alert_content_hash = _hash(
            {
                "alert_key": alert_key,
                "run_id": run_id,
                "payload_hash": payload_hash,
                "expires_at": report.expires_at.isoformat(),
            }
        )
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.alerts (
                alert_key, research_run_id, asset_id, market_id, alert_type,
                severity, title, message, dedupe_key, expires_at, payload,
                source_id, source_record_key, source_version, revision_no,
                observed_at, available_at, content_hash
            ) VALUES (
                %s, %s, %s, %s, 'research_alert_v1', 'warning', %s, %s, %s,
                %s, %s::jsonb, %s, %s, 'v1', 1, %s, %s, %s
            )
            ON CONFLICT (alert_key) DO NOTHING
            RETURNING alert_id
            """,
            (
                alert_key,
                run_id,
                asset_id,
                market_id,
                f"Research alert: {symbol} {report.horizon}"[:200],
                "Deterministic futures thresholds produced a read-only research alert.",
                dedupe_hash,
                report.expires_at,
                _canonical_json(payload),
                source_id,
                alert_key,
                report.as_of,
                report.as_of,
                alert_content_hash,
            ),
        )
        inserted = db_cursor.fetchone()
        if inserted is None:
            self._validate_existing_alert(
                db_cursor,
                report=report,
                alert_key=alert_key,
                dedupe_hash=dedupe_hash,
                asset_id=asset_id,
                market_id=market_id,
            )
            return alert_key, False
        alert_id = _as_int(_value(inserted, 0))
        self._insert_alert_event(
            db_cursor,
            alert_id=alert_id,
            event_type="created",
            event_at=recorded_at,
            source_id=source_id,
            source_record_key=f"alert-event:{alert_key}:created",
            channel=None,
            details={"trace_id": report.trace_id, "read_only": True},
        )
        idempotency_key = _hash(
            {
                "alert_key": alert_key,
                "channel": self.policy.channel,
                "destination": self.policy.destination,
                "payload_hash": payload_hash,
            }
        )
        outbox_hash = _hash(
            {
                "alert_id": alert_id,
                "idempotency_key": idempotency_key,
                "payload_hash": payload_hash,
                "monitoring_policy_id": self.policy.policy_id,
                "monitoring_policy_hash": self.policy.policy_hash_sha256,
                "retention_days": self.policy.retention_days,
                "available_at": recorded_at.isoformat(),
                "expires_at": report.expires_at.isoformat(),
                "max_attempts": self.policy.max_attempts,
            }
        )
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.alert_delivery_outbox (
                alert_id, channel, destination, idempotency_key,
                monitoring_policy_id, monitoring_policy_hash, retention_days,
                payload, payload_hash, available_at, expires_at, max_attempts,
                content_hash
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s
            )
            """,
            (
                alert_id,
                self.policy.channel,
                self.policy.destination,
                idempotency_key,
                self.policy.policy_id,
                self.policy.policy_hash_sha256,
                self.policy.retention_days,
                _canonical_json(payload),
                payload_hash,
                recorded_at,
                report.expires_at,
                self.policy.max_attempts,
                outbox_hash,
            ),
        )
        return alert_key, True

    def _validate_existing_alert(
        self,
        db_cursor: DBCursor,
        *,
        report: ResearchReport,
        alert_key: str,
        dedupe_hash: str,
        asset_id: int,
        market_id: int,
    ) -> None:
        db_cursor.execute(
            """
            SELECT alert_id, research_run_id, asset_id, market_id, dedupe_key,
                   expires_at, payload, content_hash
            FROM crypto_agent.alerts WHERE alert_key = %s
            """,
            (alert_key,),
        )
        row = db_cursor.fetchone()
        if row is None:
            raise MonitoringError("Operational alert idempotency conflict")
        alert_id = _as_int(_value(row, 0))
        run_id = _as_int(_value(row, 1))
        stored_payload = _json_object(_value(row, 6))
        stored_payload_hash = _hash(stored_payload)
        expires_at = _database_utc(_value(row, 5), "expires_at")
        expected_content_hash = _hash(
            {
                "alert_key": alert_key,
                "run_id": run_id,
                "payload_hash": stored_payload_hash,
                "expires_at": expires_at.isoformat(),
            }
        )
        if (
            _as_int(_value(row, 2)) != asset_id
            or _as_int(_value(row, 3)) != market_id
            or str(_value(row, 4)) != dedupe_hash
            or str(_value(row, 7)) != expected_content_hash
            or stored_payload.get("data_snapshot_id") != report.data_snapshot_id
            or stored_payload.get("policy_hash_sha256") != report.risk.policy_hash
            or stored_payload.get("instrument_id") != report.instrument_id
            or stored_payload.get("horizon") != report.horizon
            or stored_payload.get("reason_codes") != list(report.reason_codes[:50])
        ):
            raise MonitoringError("Operational alert idempotency conflict")
        delivery_row = self._select_alert_delivery(db_cursor, alert_id)
        if delivery_row is None:
            raise MonitoringError("Operational alert outbox is incomplete")
        delivery = _DeliveryRow.from_row(
            delivery_row,
            policy=self.policy,
            require_due=False,
        )
        if delivery.alert_key != alert_key:
            raise MonitoringError("Operational alert outbox is inconsistent")

    @staticmethod
    def _select_alert_delivery(db_cursor: DBCursor, alert_id: int) -> object | None:
        db_cursor.execute(
            _DELIVERY_SELECT
            + """
            WHERE outbox.alert_id = %s
            FOR UPDATE OF outbox
            LIMIT 1
            """,
            (alert_id,),
        )
        return db_cursor.fetchone()

    @staticmethod
    def _insert_alert_event(
        db_cursor: DBCursor,
        *,
        alert_id: int,
        event_type: str,
        event_at: datetime,
        source_id: int,
        source_record_key: str,
        channel: str | None,
        details: dict[str, object],
    ) -> None:
        content_hash = _hash(
            {
                "alert_id": alert_id,
                "event_type": event_type,
                "event_at": event_at.isoformat(),
                "channel": channel,
                "details": details,
            }
        )
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.alert_events (
                alert_id, event_type, event_at, channel, actor_key, details,
                source_id, source_record_key, source_version, revision_no,
                observed_at, available_at, ingested_at, content_hash
            ) VALUES (
                %s, %s, %s, %s, 'crypto-agent-monitor', %s::jsonb, %s, %s,
                'v1', 1, %s, %s, %s, %s
            )
            ON CONFLICT (source_id, source_record_key, revision_no) DO NOTHING
            """,
            (
                alert_id,
                event_type,
                event_at,
                channel,
                _canonical_json(details),
                source_id,
                source_record_key,
                event_at,
                event_at,
                event_at,
                content_hash,
            ),
        )

    @staticmethod
    def _select_due_delivery(
        db_cursor: DBCursor,
        now: datetime,
    ) -> object | None:
        db_cursor.execute(
            _DELIVERY_SELECT
            + """
            WHERE outbox.available_at <= %s
              AND (
                previous.attempt_no IS NULL
                OR (
                  previous.outcome = 'retryable_failure'
                  AND (
                    previous.next_attempt_at <= %s
                    OR outbox.expires_at <= %s
                  )
                )
              )
            ORDER BY outbox.available_at, outbox.alert_delivery_outbox_id
            FOR UPDATE OF outbox SKIP LOCKED
            LIMIT 1
            """,
            (now, now, now),
        )
        return db_cursor.fetchone()

    def _record_delivery_outcome(
        self,
        db_cursor: DBCursor,
        *,
        delivery: _DeliveryRow,
        attempt_no: int,
        started_at: datetime,
        finished_at: datetime,
        outcome: str,
        error_code: str | None,
        next_attempt_at: datetime | None,
    ) -> DeliveryResult:
        attempt_document = {
            "outbox_id": delivery.outbox_id,
            "attempt_no": attempt_no,
            "outcome": outcome,
            "started_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "next_attempt_at": (
                next_attempt_at.isoformat() if next_attempt_at is not None else None
            ),
            "error_code": error_code,
            "request_payload_hash": delivery.payload_hash,
            "previous_attempt_hash": delivery.previous_attempt_hash,
        }
        content_hash = _hash(attempt_document)
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.alert_delivery_attempts (
                alert_delivery_outbox_id, attempt_no, outcome, started_at,
                finished_at, next_attempt_at, error_code, request_payload_hash,
                previous_attempt_hash, content_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                delivery.outbox_id,
                attempt_no,
                outcome,
                started_at,
                finished_at,
                next_attempt_at,
                error_code,
                delivery.payload_hash,
                delivery.previous_attempt_hash,
                content_hash,
            ),
        )
        event_type = {
            "delivered": "delivered",
            "expired": "expired",
            "retryable_failure": "delivery_failed",
            "permanent_failure": "delivery_failed",
        }[outcome]
        self._insert_alert_event(
            db_cursor,
            alert_id=delivery.alert_id,
            event_type=event_type,
            event_at=finished_at,
            source_id=_single_source_id(db_cursor, _INTERNAL_SOURCE),
            source_record_key=(f"alert-delivery-event:{delivery.outbox_id}:{attempt_no}:{outcome}"),
            channel=delivery.channel,
            details={
                "attempt_no": attempt_no,
                "outcome": outcome,
                "error_code": error_code,
                "idempotency_key": delivery.idempotency_key,
            },
        )
        return DeliveryResult(
            status=outcome,
            alert_key=delivery.alert_key,
            idempotency_key=delivery.idempotency_key,
            attempt_no=attempt_no,
            error_code=error_code,
        )

    @staticmethod
    def _read_alerts(
        db_cursor: DBCursor,
        limit: int,
    ) -> tuple[dict[str, object], ...]:
        db_cursor.execute(
            """
            SELECT alert.alert_key, alert.severity, alert.title,
                   alert.observed_at, alert.expires_at,
                   COALESCE(latest.event_type, 'created')
            FROM crypto_agent.alerts alert
            LEFT JOIN LATERAL (
                SELECT event_type FROM crypto_agent.alert_events event
                WHERE event.alert_id = alert.alert_id
                ORDER BY event.event_at DESC, event.alert_event_id DESC LIMIT 1
            ) latest ON TRUE
            WHERE alert.alert_type = 'research_alert_v1'
            ORDER BY alert.observed_at DESC, alert.alert_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return tuple(
            {
                "alert_key": str(_value(row, 0)),
                "severity": str(_value(row, 1)),
                "title": str(_value(row, 2)),
                "observed_at": _display(_value(row, 3)),
                "expires_at": _display(_value(row, 4)),
                "status": str(_value(row, 5)),
            }
            for row in db_cursor.fetchall()
        )

    @staticmethod
    def _read_incidents(
        db_cursor: DBCursor,
        limit: int,
    ) -> tuple[dict[str, object], ...]:
        db_cursor.execute(
            """
            SELECT incident.incident_key, incident.severity,
                   incident.rule_key, incident.summary, incident.detected_at,
                   COALESCE(latest.event_type, 'opened')
            FROM crypto_agent.data_quality_incidents incident
            LEFT JOIN LATERAL (
                SELECT event_type
                FROM crypto_agent.data_quality_incident_events event
                WHERE event.data_quality_incident_id =
                      incident.data_quality_incident_id
                ORDER BY event.event_at DESC,
                         event.data_quality_incident_event_id DESC LIMIT 1
            ) latest ON TRUE
            WHERE incident.dataset_name = 'operational_monitoring'
            ORDER BY incident.detected_at DESC,
                     incident.data_quality_incident_id DESC
            LIMIT %s
            """,
            (limit,),
        )
        return tuple(
            {
                "incident_key": str(_value(row, 0)),
                "severity": str(_value(row, 1)),
                "error_code": str(_value(row, 2)),
                "summary": str(_value(row, 3)),
                "detected_at": _display(_value(row, 4)),
                "status": str(_value(row, 5)),
            }
            for row in db_cursor.fetchall()
        )

    def _page_size(self, limit: int | None) -> int:
        if limit is None:
            return self.policy.max_page_size
        if type(limit) is not int or not 1 <= limit <= self.policy.max_page_size:
            raise ValueError(f"limit must be in [1, {self.policy.max_page_size}]")
        return limit


@dataclass(frozen=True, slots=True)
class _DeliveryRow:
    outbox_id: int
    alert_id: int
    alert_key: str
    channel: str
    destination: str
    idempotency_key: str
    payload: dict[str, object]
    payload_hash: str
    available_at: datetime
    expires_at: datetime
    max_attempts: int
    previous_attempt_no: int
    previous_outcome: str | None
    previous_next_attempt_at: datetime | None
    previous_attempt_hash: str | None

    @classmethod
    def from_row(
        cls,
        row: object,
        *,
        policy: MonitoringPolicy,
        require_due: bool,
    ) -> _DeliveryRow:
        channel = str(_value(row, 3))
        destination = str(_value(row, 4))
        if channel != "stdout_json" or destination != "process_stdout":
            raise MonitoringError("Alert delivery route is not approved")
        outbox_id = _as_int(_value(row, 0))
        alert_id = _as_int(_value(row, 1))
        alert_key = str(_value(row, 2))
        idempotency_key = str(_value(row, 5))
        monitoring_policy_id = str(_value(row, 6))
        monitoring_policy_hash = str(_value(row, 7))
        retention_days = _as_int(_value(row, 8))
        payload = _json_object(_value(row, 9))
        payload_hash = str(_value(row, 10))
        available_at = _database_utc(_value(row, 11), "available_at")
        expires_at = _database_utc(_value(row, 12), "expires_at")
        max_attempts = _as_int(_value(row, 13))
        previous_attempt_no = _as_int(_value(row, 14) or 0)
        previous_outcome = None if _value(row, 15) is None else str(_value(row, 15))
        previous_next_attempt_at = (
            None
            if _value(row, 16) is None
            else _database_utc(_value(row, 16), "previous_next_attempt_at")
        )
        previous_attempt_hash = None if _value(row, 17) is None else str(_value(row, 17))
        outbox_content_hash = str(_value(row, 18))
        if (
            re.fullmatch(r"research-alert:[0-9a-f]{64}", alert_key) is None
            or _SHA256.fullmatch(idempotency_key) is None
            or _SHA256.fullmatch(payload_hash) is None
            or monitoring_policy_id != policy.policy_id
            or monitoring_policy_hash != policy.policy_hash_sha256
            or retention_days != policy.retention_days
            or max_attempts != policy.max_attempts
            or previous_attempt_no > max_attempts
            or (
                previous_attempt_no == 0
                and (
                    previous_outcome is not None
                    or previous_next_attempt_at is not None
                    or previous_attempt_hash is not None
                )
            )
            or (
                previous_attempt_no > 0
                and (
                    previous_outcome
                    not in {
                        "delivered",
                        "retryable_failure",
                        "permanent_failure",
                        "expired",
                    }
                    or previous_attempt_hash is None
                    or (
                        previous_outcome == "retryable_failure"
                        and previous_next_attempt_at is None
                    )
                    or (
                        previous_outcome != "retryable_failure"
                        and previous_next_attempt_at is not None
                    )
                )
            )
            or (
                require_due
                and previous_attempt_no > 0
                and previous_outcome != "retryable_failure"
            )
            or (
                previous_outcome == "retryable_failure"
                and previous_attempt_no >= max_attempts
            )
            or (
                previous_attempt_hash is not None
                and _SHA256.fullmatch(previous_attempt_hash) is None
            )
        ):
            raise MonitoringError("Stored alert delivery envelope is invalid")
        _validate_alert_payload(
            payload,
            policy=policy,
            available_at=available_at,
            expires_at=expires_at,
        )
        if _hash(payload) != payload_hash:
            raise MonitoringError("Stored alert payload hash is invalid")
        expected_idempotency_key = _hash(
            {
                "alert_key": alert_key,
                "channel": channel,
                "destination": destination,
                "payload_hash": payload_hash,
            }
        )
        expected_outbox_hash = _hash(
            {
                "alert_id": alert_id,
                "idempotency_key": idempotency_key,
                "payload_hash": payload_hash,
                "monitoring_policy_id": monitoring_policy_id,
                "monitoring_policy_hash": monitoring_policy_hash,
                "retention_days": retention_days,
                "available_at": available_at.isoformat(),
                "expires_at": expires_at.isoformat(),
                "max_attempts": max_attempts,
            }
        )
        if (
            idempotency_key != expected_idempotency_key
            or outbox_content_hash != expected_outbox_hash
        ):
            raise MonitoringError("Stored alert outbox integrity check failed")
        return cls(
            outbox_id=outbox_id,
            alert_id=alert_id,
            alert_key=alert_key,
            channel=channel,
            destination=destination,
            idempotency_key=idempotency_key,
            payload=payload,
            payload_hash=payload_hash,
            available_at=available_at,
            expires_at=expires_at,
            max_attempts=max_attempts,
            previous_attempt_no=previous_attempt_no,
            previous_outcome=previous_outcome,
            previous_next_attempt_at=previous_next_attempt_at,
            previous_attempt_hash=previous_attempt_hash,
        )


def run_monitored_analysis(
    analyze: Callable[..., ResearchReport],
    monitoring: MonitoringRepository,
    *,
    operation: str,
    symbol: str,
    interval_minutes: int,
    as_of: datetime | None = None,
    limit: int = 120,
    trace_id: str | None = None,
    failure_deadline_monotonic: float | None = None,
) -> tuple[ResearchReport, MonitoringReceipt]:
    trace = trace_id or str(uuid4())
    _uuid_text(trace, "trace_id")
    try:
        report = analyze(
            symbol=symbol,
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
            trace_id=trace,
        )
    except TimeoutError:
        try:
            deadline_scope = (
                analysis_deadline_scope(
                    min(
                        failure_deadline_monotonic,
                        time.monotonic()
                        + ANALYSIS_TIMEOUT_RECORDING_RESERVE_SECONDS,
                    )
                )
                if failure_deadline_monotonic is not None
                else nullcontext()
            )
            with deadline_scope:
                monitoring.record_failure(
                    trace_id=trace,
                    operation=operation,
                    error_code="ANALYSIS_TIMEOUT",
                    component="analysis",
                    scope=f"{symbol}:{interval_minutes}",
                )
        except Exception:
            # The timeout is the primary failure. Bounded telemetry cleanup is
            # best effort, so a database outage must not turn HTTP 504 into an
            # unrelated monitoring error or expose driver details.
            pass
        raise
    except (RuntimeError, ValueError):
        monitoring.record_failure(
            trace_id=trace,
            operation=operation,
            error_code="ANALYSIS_FAILED",
            component="analysis",
            scope=f"{symbol}:{interval_minutes}",
        )
        raise
    except Exception:
        monitoring.record_failure(
            trace_id=trace,
            operation=operation,
            error_code="ANALYSIS_UNEXPECTED_FAILURE",
            component="analysis",
            scope=f"{symbol}:{interval_minutes}",
        )
        raise
    if report.trace_id != trace:
        monitoring.record_failure(
            trace_id=trace,
            operation=operation,
            error_code="ANALYSIS_TRACE_MISMATCH",
            component="analysis",
            scope=f"{symbol}:{interval_minutes}",
        )
        raise MonitoringError("Analysis trace attestation failed")
    ensure_analysis_deadline()
    receipt = monitoring.record_report(report, operation=operation)
    return report, receipt


def _safe_report_artifact(report: ResearchReport) -> dict[str, object]:
    metadata_keys = (
        "system_version",
        "mode",
        "execution_enabled",
        "not_financial_advice",
        "v1_gate_passed",
        "plus500_t4_source_attested",
        "external_delivery_eligible",
        "t4_bridge_schema_version",
        "t4_environment",
        "futures_gate_passed",
        "futures_policy_id",
        "futures_policy_hash_sha256",
        "input_fingerprint_sha256",
        "input_candle_count",
        "analysis_candle_count",
        "provider_error_code",
    )
    return {
        "schema_version": 1,
        "decision_id": report.decision_id,
        "trace_id": report.trace_id,
        "as_of": report.as_of.isoformat(),
        "expires_at": report.expires_at.isoformat(),
        "asset_id": report.asset_id,
        "instrument_id": report.instrument_id,
        "horizon": report.horizon,
        "decision": report.decision.value,
        "reason_codes": list(report.reason_codes[:50]),
        "model_version": report.model_version,
        "policy_version": report.policy_version,
        "data_snapshot_id": report.data_snapshot_id,
        "data_quality": {
            "score": report.data_quality.score,
            "sample_count": report.data_quality.sample_count,
            "flags": list(report.data_quality.flags[:50]),
            "critical_flags": list(report.data_quality.critical_flags[:50]),
        },
        "risk": {
            "assessment_id": report.risk.assessment_id,
            "policy_id": report.risk.policy_id,
            "policy_hash": report.risk.policy_hash,
            "decision": report.risk.decision.value,
            "vetoed": report.risk.vetoed,
            "flags": list(report.risk.flags[:50]),
        },
        "metadata": {
            key: _safe_scalar(report.metadata.get(key))
            for key in metadata_keys
            if key in report.metadata
        },
        "read_only": True,
    }


def _alert_payload(
    report: ResearchReport,
    policy: MonitoringPolicy,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "alert_type": "research_alert_v1",
        "decision": "ALERT",
        "decision_id": report.decision_id,
        "trace_id": report.trace_id,
        "asset_id": report.asset_id,
        "instrument_id": report.instrument_id,
        "horizon": report.horizon,
        "as_of": report.as_of.isoformat(),
        "expires_at": report.expires_at.isoformat(),
        "reason_codes": list(report.reason_codes[:50]),
        "data_snapshot_id": report.data_snapshot_id,
        "policy_id": report.risk.policy_id,
        "policy_hash_sha256": report.risk.policy_hash,
        "futures_policy_id": _safe_scalar(report.metadata.get("futures_policy_id")),
        "futures_policy_hash_sha256": _safe_scalar(
            report.metadata.get("futures_policy_hash_sha256")
        ),
        "environment": "live_t4",
        "external_delivery": False,
        "read_only": True,
        "execution_enabled": False,
        "not_financial_advice": True,
        "monitoring_policy_id": policy.policy_id,
        "monitoring_policy_hash_sha256": policy.policy_hash_sha256,
        "retention_days": policy.retention_days,
    }


def _eligible_for_delivery(
    report: ResearchReport,
    *,
    operation: str,
    now: datetime,
) -> bool:
    metadata = report.metadata
    exact_instrument = any(
        report.instrument_id == f"{_T4_SOURCE}:{symbol}:{interval}m" and report.horizon == horizon
        for interval, horizon in _INTERVAL_HORIZONS.items()
        for symbol in ("BTC/USD", "ETH/USD")
    )
    return bool(
        operation == "live_t4_analysis"
        and report.decision is Decision.ALERT
        and report.risk.decision is Decision.ALERT
        and report.risk.vetoed is False
        and report.as_of <= now < report.expires_at
        and metadata.get("plus500_t4_source_attested") is True
        and metadata.get("external_delivery_eligible") is True
        and metadata.get("t4_bridge_schema_version") == 4
        and metadata.get("t4_environment") == "live_t4"
        and metadata.get("futures_gate_passed") is True
        and metadata.get("provider_error_code") is None
        and metadata.get("execution_enabled") is False
        and exact_instrument
    )


def _validate_alert_payload(
    payload: dict[str, object],
    *,
    policy: MonitoringPolicy,
    available_at: datetime,
    expires_at: datetime,
) -> None:
    if set(payload) != _ALERT_PAYLOAD_KEYS:
        raise MonitoringError("Stored alert payload fields are invalid")
    instrument_id = payload.get("instrument_id")
    horizon = payload.get("horizon")
    instrument_match: tuple[str, str] | None = None
    if isinstance(instrument_id, str) and isinstance(horizon, str):
        for interval, expected_horizon in _INTERVAL_HORIZONS.items():
            for symbol in ("BTC/USD", "ETH/USD"):
                if (
                    instrument_id == f"{_T4_SOURCE}:{symbol}:{interval}m"
                    and horizon == expected_horizon
                ):
                    instrument_match = (symbol, expected_horizon)
    reason_codes = payload.get("reason_codes")
    try:
        as_of = _parse_utc_timestamp(payload.get("as_of"), "as_of")
        payload_expires_at = _parse_utc_timestamp(payload.get("expires_at"), "expires_at")
        _uuid_text(cast(str, payload.get("decision_id")), "decision_id")
        _uuid_text(cast(str, payload.get("trace_id")), "trace_id")
    except (TypeError, ValueError):
        raise MonitoringError("Stored alert payload identity is invalid") from None
    expected_asset = {
        "BTC/USD": "bip122:000000000019d6689c085ae165831e93:native",
        "ETH/USD": "eip155:1:native",
    }.get(instrument_match[0] if instrument_match is not None else "")
    hashes = (
        payload.get("policy_hash_sha256"),
        payload.get("futures_policy_hash_sha256"),
        payload.get("monitoring_policy_hash_sha256"),
    )
    if (
        payload.get("schema_version") != 1
        or payload.get("alert_type") != "research_alert_v1"
        or payload.get("decision") != "ALERT"
        or instrument_match is None
        or payload.get("asset_id") != expected_asset
        or horizon not in _HORIZON_SECONDS
        or not isinstance(reason_codes, list)
        or len(reason_codes) > 50
        or any(
            not isinstance(code, str) or _SAFE_CODE.fullmatch(code) is None for code in reason_codes
        )
        or not isinstance(payload.get("policy_id"), str)
        or not isinstance(payload.get("futures_policy_id"), str)
        or any(not isinstance(value, str) or _SHA256.fullmatch(value) is None for value in hashes)
        or not isinstance(payload.get("data_snapshot_id"), str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", cast(str, payload.get("data_snapshot_id"))) is None
        or payload.get("environment") != "live_t4"
        or payload.get("external_delivery") is not False
        or payload.get("read_only") is not True
        or payload.get("execution_enabled") is not False
        or payload.get("not_financial_advice") is not True
        or payload.get("monitoring_policy_id") != policy.policy_id
        or payload.get("monitoring_policy_hash_sha256") != policy.policy_hash_sha256
        or payload.get("retention_days") != policy.retention_days
        or not as_of <= available_at < expires_at
        or payload_expires_at != expires_at
    ):
        raise MonitoringError("Stored alert payload attestation failed")


def _source_ids(db_cursor: DBCursor) -> dict[str, int]:
    db_cursor.execute(
        """
        SELECT source_key, source_id FROM crypto_agent.data_sources
        WHERE source_key IN ('crypto_agent_registry_v1', 'plus500_t4_futures_v1')
        """
    )
    result = {str(_value(row, 0)): _as_int(_value(row, 1)) for row in db_cursor.fetchall()}
    if set(result) != {_INTERNAL_SOURCE, _T4_SOURCE}:
        raise MonitoringError("Operational monitoring registry is incomplete")
    return result


def _single_source_id(db_cursor: DBCursor, source_key: str) -> int:
    db_cursor.execute(
        "SELECT source_id FROM crypto_agent.data_sources WHERE source_key = %s",
        (source_key,),
    )
    row = db_cursor.fetchone()
    if row is None:
        raise MonitoringError("Operational monitoring registry is incomplete")
    return _as_int(_value(row, 0))


def _retry_delay(
    policy: MonitoringPolicy,
    *,
    attempt_no: int,
    idempotency_key: str,
) -> float:
    base = min(
        policy.retry_max_seconds,
        policy.retry_base_seconds * (2 ** max(0, attempt_no - 1)),
    )
    digest = hashlib.sha256(f"{idempotency_key}:{attempt_no}".encode()).digest()
    jitter = int.from_bytes(digest[:2], "big") / 65_535 * min(base * 0.25, 5.0)
    return float(min(policy.retry_max_seconds, base + jitter))


def _report_symbol(report: ResearchReport) -> str:
    try:
        symbol = report.instrument_id.split(":", 2)[1]
    except (AttributeError, IndexError):
        raise MonitoringError("Operational alert instrument is invalid") from None
    if symbol not in {"BTC/USD", "ETH/USD"}:
        raise MonitoringError("Operational alert instrument is invalid")
    return symbol


def _sha_from_identifier(value: str) -> str:
    candidate = value.removeprefix("sha256:")
    if re.fullmatch(r"[0-9a-f]{64}", candidate):
        return candidate
    raise MonitoringError("Report data snapshot hash is invalid")


def _operation(value: str) -> str:
    allowed = {"analysis", "live_t4_analysis", "analyze_replay"}
    if value not in allowed:
        raise ValueError("Unsupported monitoring operation")
    return value


def _uuid_text(value: str, name: str) -> str:
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError):
        raise ValueError(f"{name} must be a UUID") from None
    if str(parsed) != value.lower():
        raise ValueError(f"{name} must use canonical UUID text")
    return str(parsed)


def _required_code(value: str) -> str:
    if not isinstance(value, str) or not _SAFE_CODE.fullmatch(value):
        raise ValueError("error_code must be a stable uppercase identifier")
    return value


def _safe_optional_code(value: object) -> str | None:
    if value is None:
        return None
    return _required_code(str(value))


def _bounded_identifier(value: str, name: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", value):
        raise ValueError(f"{name} must be a stable lowercase identifier")
    return value


def _bounded_text(value: str, name: str, maximum: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise ValueError(f"{name} must be non-empty and at most {maximum} characters")
    return value


def _incident_severity(code: str) -> str:
    if code in {"SECURITY_BOUNDARY_FAILED", "DATABASE_INTEGRITY_FAILED"}:
        return "critical"
    if code in {"ANALYSIS_TIMEOUT", "POSTGRES_UNAVAILABLE"}:
        return "error"
    return "warning"


def _incident_summary(code: str) -> str:
    summaries = {
        "T4_APPLICATION_REGISTRATION_REQUIRED": (
            "Official T4 application registration is still required."
        ),
        "ANALYSIS_TIMEOUT": "The read-only analysis exceeded its bounded deadline.",
        "ANALYSIS_FAILED": "The read-only analysis failed safely.",
        "POSTGRES_UNAVAILABLE": "PostgreSQL monitoring storage is unavailable.",
    }
    return summaries.get(code, "A monitored read-only component reported a failure.")


def _safe_scalar(value: object) -> str | int | float | bool | None:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float) and value == value and abs(value) != float("inf"):
        return value
    return None


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_json_default,
    )


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _json_object(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return {str(key): item for key, item in value.items()}
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            raise MonitoringError("Stored monitoring JSON is invalid") from None
        if isinstance(decoded, dict):
            return cast(dict[str, object], decoded)
    raise MonitoringError("Stored monitoring JSON is invalid")


def _begin_monitoring_transaction(db_cursor: DBCursor) -> None:
    timeout_ms = max(1, int(bounded_analysis_timeout(5.0) * 1000))
    lock_timeout_ms = min(timeout_ms, 2_000)
    db_cursor.execute(
        """
        SELECT set_config('TimeZone', 'UTC', true),
               set_config('statement_timeout', %s, true),
               set_config('lock_timeout', %s, true)
        """,
        (f"{timeout_ms}ms", f"{lock_timeout_ms}ms"),
    )


def _refresh_monitoring_transaction_timeout(db_cursor: DBCursor) -> None:
    timeout_ms = max(1, int(bounded_analysis_timeout(5.0) * 1000))
    db_cursor.execute(
        "SELECT set_config('statement_timeout', %s, true)",
        (f"{timeout_ms}ms",),
    )


def _bounded_stream_write(stream: TextIO, value: str, timeout_seconds: float) -> None:
    if timeout_seconds <= 0:
        raise TimeoutError("stdout write deadline elapsed")
    encoded = value.encode("utf-8")
    try:
        file_descriptor = stream.fileno()
    except (AttributeError, OSError, ValueError):
        _bounded_python_stream_write(stream, value, timeout_seconds)
        return
    mode = os.fstat(file_descriptor).st_mode
    if stat.S_ISFIFO(mode):
        pipe_buf = os.fpathconf(file_descriptor, "PC_PIPE_BUF")
        if len(encoded) > pipe_buf:
            raise OSError("alert line exceeds atomic stdout pipe capacity")
    deadline = time.monotonic() + timeout_seconds
    was_blocking = os.get_blocking(file_descriptor)
    try:
        os.set_blocking(file_descriptor, False)
        while True:
            try:
                written = os.write(file_descriptor, encoded)
            except BlockingIOError:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("stdout write timed out") from None
                _, writable, _ = select.select([], [file_descriptor], [], remaining)
                if not writable:
                    raise TimeoutError("stdout write timed out") from None
                continue
            if written != len(encoded):
                raise OSError("stdout write was partial")
            return
    finally:
        os.set_blocking(file_descriptor, was_blocking)


def _bounded_python_stream_write(
    stream: TextIO,
    value: str,
    timeout_seconds: float,
) -> None:
    if threading.current_thread() is not threading.main_thread() or not hasattr(
        signal, "setitimer"
    ):
        raise OSError("stdout stream does not support bounded writes")
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_delay, previous_interval = signal.getitimer(signal.ITIMER_REAL)
    if previous_delay > 0:
        raise OSError("stdout write timer is unavailable")

    def timeout_handler(signum: int, frame: object) -> None:
        del signum, frame
        raise TimeoutError("stdout write timed out")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        written = stream.write(value)
        if written != len(value):
            raise OSError("stdout write was partial")
        stream.flush()
    finally:
        signal.setitimer(signal.ITIMER_REAL, previous_delay, previous_interval)
        signal.signal(signal.SIGALRM, previous_handler)


def _parse_utc_timestamp(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be an ISO timestamp")
    return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")), name)


def _database_utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise MonitoringError(f"Stored {name} timestamp is invalid")
    return value.astimezone(UTC)


def _utc(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be timezone-aware UTC")
    if value.utcoffset() != timedelta(0):
        raise ValueError(f"{name} must be normalized to UTC")
    return value


def _display(value: object) -> object:
    return value.isoformat() if isinstance(value, datetime) else value


def _value(row: object, index: int) -> object:
    if isinstance(row, Mapping):
        return tuple(row.values())[index]
    if isinstance(row, (tuple, list)):
        return row[index]
    raise PostgresOperationError("PostgreSQL returned an unsupported row shape")


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        raise MonitoringError("PostgreSQL returned an invalid integer")
    try:
        return int(cast(Any, value))
    except (TypeError, ValueError, OverflowError):
        raise MonitoringError("PostgreSQL returned an invalid integer") from None

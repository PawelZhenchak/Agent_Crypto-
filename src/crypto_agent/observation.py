from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from .observation_policy import ObservationPolicy
from .postgres import ConnectionFactory, DBCursor, cursor, transaction

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_UPPER_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_SAFE_LOWER_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_CAMPAIGN_START_TOLERANCE = timedelta(minutes=5)


class ObservationError(RuntimeError):
    """Safe boundary error for campaign persistence and evaluation."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "OBSERVATION_ERROR",
    ) -> None:
        super().__init__(message)
        self.code = code


class CriterionStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_OBSERVED = "NOT_OBSERVED"


@dataclass(frozen=True, slots=True)
class ObservationCampaign:
    campaign_id: str
    started_at: datetime
    planned_ends_at: datetime
    cycle_interval_seconds: int
    code_commit_hash: str
    t4_protocol_commit_hash: str
    runtime_config_hash: str
    bridge_evidence_key_fingerprint: str
    scope_manifest: tuple[str, ...]
    environment: str = "live_t4"

    def validate(self, policy: ObservationPolicy) -> None:
        _uuid(self.campaign_id, "campaign_id")
        started_at = _utc(self.started_at, "started_at")
        planned_ends_at = _utc(self.planned_ends_at, "planned_ends_at")
        if self.environment != "live_t4":
            raise ObservationError("Observation campaign must use live_t4")
        if planned_ends_at - started_at < timedelta(
            seconds=policy.minimum_elapsed_seconds
        ):
            raise ObservationError("Observation campaign must cover the full minimum window")
        if (
            type(self.cycle_interval_seconds) is not int
            or not 60 <= self.cycle_interval_seconds <= 86_400
        ):
            raise ObservationError("Observation cycle interval is invalid")
        for field_name in (
            "code_commit_hash",
            "t4_protocol_commit_hash",
            "runtime_config_hash",
            "bridge_evidence_key_fingerprint",
        ):
            _sha256(getattr(self, field_name), field_name)
        if (
            not isinstance(self.scope_manifest, tuple)
            or not self.scope_manifest
            or any(
                not isinstance(scope, str) or not scope.strip() or len(scope) > 256
                for scope in self.scope_manifest
            )
            or len(set(self.scope_manifest)) != len(self.scope_manifest)
            or tuple(sorted(self.scope_manifest)) != self.scope_manifest
        ):
            raise ObservationError(
                "Observation scope manifest must be a sorted non-empty unique tuple"
            )

    @property
    def scope_manifest_hash_sha256(self) -> str:
        return _hash(list(self.scope_manifest))

    def frozen_baseline_hash_sha256(self, policy: ObservationPolicy) -> str:
        self.validate(policy)
        return _hash(
            {
                "campaign_id": self.campaign_id,
                "environment": self.environment,
                "started_at": _iso(self.started_at),
                "planned_ends_at": _iso(self.planned_ends_at),
                "cycle_interval_seconds": self.cycle_interval_seconds,
                "observation_policy_id": policy.policy_id,
                "observation_policy_hash_sha256": policy.policy_hash_sha256,
                "code_commit_hash": self.code_commit_hash,
                "t4_protocol_commit_hash": self.t4_protocol_commit_hash,
                "runtime_config_hash": self.runtime_config_hash,
                "bridge_evidence_key_fingerprint": (
                    self.bridge_evidence_key_fingerprint
                ),
                "scope_manifest_hash_sha256": self.scope_manifest_hash_sha256,
                "read_only": True,
                "execution_enabled": False,
            }
        )


@dataclass(frozen=True, slots=True)
class ObservationCycle:
    campaign_id: str
    scope_key: str
    sequence_no: int
    expected_at: datetime
    outcome: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error_code: str | None = None
    gap_explanation_code: str | None = None
    t4_batch_id: int | None = None
    research_run_id: int | None = None
    analysis_input_hash: str | None = None
    bridge_schema_version: int | None = None
    raw_payload_hash: str | None = None
    bridge_rtt_milliseconds: int | None = None
    trace_id: str | None = None


@dataclass(frozen=True, slots=True)
class ObservationCyclePlan:
    """Frozen database-derived inputs for exactly one live observation slot."""

    campaign_id: str
    scope_key: str
    sequence_no: int
    expected_at: datetime
    started_at: datetime
    trace_id: str


@dataclass(frozen=True, slots=True)
class ObservationCycleExecution:
    """Safe links returned by the live fetch -> ingest -> analysis pipeline."""

    t4_batch_id: int
    research_run_id: int
    analysis_input_hash: str
    bridge_schema_version: int
    raw_payload_hash: str
    bridge_rtt_milliseconds: int
    trace_id: str
    environment: str
    external_delivery_eligible: bool
    replay: bool = False
    synthetic: bool = False


@dataclass(frozen=True, slots=True)
class ObservationCycleRunResult:
    campaign_id: str
    scope_key: str
    sequence_no: int | None
    expected_at: datetime | None
    outcome: str
    trace_id: str | None
    content_hash: str | None
    missed_cycles_recorded: int
    t4_batch_id: int | None = None
    research_run_id: int | None = None
    error_code: str | None = None


@dataclass(frozen=True, slots=True)
class ObservationSessionEvent:
    campaign_id: str
    sequence_no: int
    event_at: datetime
    event_type: str
    outcome: str
    scenario_code: str | None = None
    detail_code: str | None = None


@dataclass(frozen=True, slots=True)
class ObservationEvidence:
    campaign_id: str
    environment: str
    started_at: datetime
    observed_until: datetime
    frozen_baseline_hash_sha256: str
    baseline_hash_matches: bool
    policy_hash_matches: bool
    cycle_interval_seconds: int
    scope_count: int
    covered_scope_count: int
    expected_cycles: int
    attempted_cycles: int
    successful_cycles: int
    linked_successful_cycles: int
    schema_valid_successful_cycles: int
    maximum_unexplained_gap_seconds: float | None
    bridge_rtt_p95_seconds: float | None
    bridge_rtt_p99_seconds: float | None
    read_only_violations: int
    order_route_attempts: int
    secret_leaks: int
    integrity_failures: int
    fail_closed_violations: int
    prohibited_data_mode_violations: int
    scenario_results: tuple[tuple[str, bool], ...]


@dataclass(frozen=True, slots=True)
class CriterionResult:
    criterion_id: str
    status: CriterionStatus
    actual: object
    threshold: object
    mandatory: bool = True

    def as_payload(self) -> dict[str, object]:
        return {
            "criterion_id": self.criterion_id,
            "status": self.status.value,
            "actual": self.actual,
            "threshold": self.threshold,
            "mandatory": self.mandatory,
        }


@dataclass(frozen=True, slots=True)
class ObservationQualityReport:
    campaign_id: str
    generated_at: datetime
    observed_until: datetime
    elapsed_seconds: int
    overall_status: CriterionStatus
    v1_gate_passed: bool
    policy_id: str
    policy_hash_sha256: str
    frozen_baseline_hash_sha256: str
    criteria: tuple[CriterionResult, ...]

    def as_payload(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "campaign_id": self.campaign_id,
            "generated_at": _iso(self.generated_at),
            "observed_until": _iso(self.observed_until),
            "elapsed_seconds": self.elapsed_seconds,
            "overall_status": self.overall_status.value,
            "v1_gate_passed": self.v1_gate_passed,
            "policy_id": self.policy_id,
            "policy_hash_sha256": self.policy_hash_sha256,
            "frozen_baseline_hash_sha256": self.frozen_baseline_hash_sha256,
            "criteria": [criterion.as_payload() for criterion in self.criteria],
        }

    @property
    def report_hash_sha256(self) -> str:
        return _hash(self.as_payload())


@dataclass(frozen=True, slots=True)
class ObservationBridgeCheckpoint:
    campaign_id: str
    action_request_id: str
    sequence_no: int
    event_hash_sha256: str

    def validate(self) -> None:
        _uuid(self.campaign_id, "campaign_id")
        _uuid(self.action_request_id, "action_request_id")
        if type(self.sequence_no) is not int or self.sequence_no <= 0:
            raise ObservationError("Observation checkpoint sequence is invalid")
        _sha256(self.event_hash_sha256, "event_hash_sha256")


def evaluate_observation(
    evidence: ObservationEvidence,
    policy: ObservationPolicy,
) -> ObservationQualityReport:
    """Produce a deterministic PASS/FAIL/NOT_OBSERVED acceptance report."""

    policy.validate()
    _validate_evidence(evidence)
    elapsed_seconds = max(
        0,
        int(
            (
                _utc(evidence.observed_until, "observed_until")
                - _utc(evidence.started_at, "started_at")
            ).total_seconds()
        ),
    )
    attempted_rate = _rate(evidence.attempted_cycles, evidence.expected_cycles)
    success_rate = _rate(evidence.successful_cycles, evidence.attempted_cycles)
    gap_limit = (
        evidence.cycle_interval_seconds * policy.maximum_unexplained_gap_multiplier
    )

    criteria: list[CriterionResult] = [
        _minimum(
            "real_elapsed_time",
            elapsed_seconds,
            policy.minimum_elapsed_seconds,
            incomplete_is_not_observed=True,
        ),
        _boolean("live_environment_only", evidence.environment == "live_t4", "live_t4"),
        _boolean(
            "frozen_baseline",
            evidence.baseline_hash_matches and evidence.policy_hash_matches,
            "all frozen hashes match",
        ),
        _coverage(evidence.scope_count, evidence.covered_scope_count),
        _minimum_optional(
            "cycle_attempt_rate",
            attempted_rate,
            policy.minimum_cycle_attempt_rate,
        ),
        _minimum_optional(
            "cycle_success_rate",
            success_rate,
            policy.minimum_cycle_success_rate,
        ),
        _count_match(
            "exact_batch_analysis_linkage",
            evidence.linked_successful_cycles,
            evidence.successful_cycles,
        ),
        _count_match(
            "bridge_schema_conformance",
            evidence.schema_valid_successful_cycles,
            evidence.successful_cycles,
        ),
        _maximum_optional(
            "maximum_unexplained_gap_seconds",
            evidence.maximum_unexplained_gap_seconds,
            gap_limit,
        ),
        _latency(evidence, policy),
        _zero("read_only_violations", evidence.read_only_violations),
        _zero("order_route_attempts", evidence.order_route_attempts),
        _zero("secret_leaks", evidence.secret_leaks),
        _zero("integrity_failures", evidence.integrity_failures),
        _zero("fail_closed_violations", evidence.fail_closed_violations),
        _zero(
            "prohibited_data_mode_violations",
            evidence.prohibited_data_mode_violations,
        ),
    ]
    scenario_results = dict(evidence.scenario_results)
    for scenario in policy.mandatory_scenarios:
        result = scenario_results.get(scenario)
        criteria.append(
            CriterionResult(
                criterion_id=f"scenario_{scenario}",
                status=(
                    CriterionStatus.NOT_OBSERVED
                    if result is None
                    else CriterionStatus.PASS
                    if result
                    else CriterionStatus.FAIL
                ),
                actual="not_observed" if result is None else result,
                threshold=True,
            )
        )

    mandatory = tuple(item for item in criteria if item.mandatory)
    if any(item.status is CriterionStatus.FAIL for item in mandatory):
        overall = CriterionStatus.FAIL
    elif any(item.status is CriterionStatus.NOT_OBSERVED for item in mandatory):
        overall = CriterionStatus.NOT_OBSERVED
    else:
        overall = CriterionStatus.PASS
    gate = overall is CriterionStatus.PASS and all(
        item.status is CriterionStatus.PASS for item in mandatory
    )
    return ObservationQualityReport(
        campaign_id=evidence.campaign_id,
        generated_at=evidence.observed_until,
        observed_until=evidence.observed_until,
        elapsed_seconds=elapsed_seconds,
        overall_status=overall,
        v1_gate_passed=gate,
        policy_id=policy.policy_id,
        policy_hash_sha256=policy.policy_hash_sha256,
        frozen_baseline_hash_sha256=evidence.frozen_baseline_hash_sha256,
        criteria=tuple(criteria),
    )


class ObservationRepository:
    """Append-only PostgreSQL persistence for campaign acceptance evidence."""

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        policy: ObservationPolicy,
    ) -> None:
        policy.validate()
        self.connection_factory = connection_factory
        self.policy = policy

    def create_campaign(self, campaign: ObservationCampaign) -> str:
        campaign.validate(self.policy)
        baseline_hash = campaign.frozen_baseline_hash_sha256(self.policy)
        content_hash = _hash(
            {
                "campaign_id": campaign.campaign_id,
                "frozen_baseline_hash_sha256": baseline_hash,
            }
        )
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute("SELECT clock_timestamp()")
                clock_row = db_cursor.fetchone()
                database_now = _datetime_value(clock_row, 0, "database_now")
                started_at = _utc(campaign.started_at, "started_at")
                if not database_now - _CAMPAIGN_START_TOLERANCE <= started_at <= database_now:
                    raise ObservationError(
                        "Observation campaign start must come from the current database clock"
                    )
                db_cursor.execute(
                    """
                    INSERT INTO crypto_agent.t4_observation_campaigns (
                        campaign_id, environment, started_at, planned_ends_at,
                        cycle_interval_seconds, observation_policy_id,
                        observation_policy_hash, code_commit_hash,
                        t4_protocol_commit_hash, runtime_config_hash,
                        bridge_evidence_key_fingerprint, scope_manifest,
                        scope_manifest_hash, frozen_baseline_hash,
                        read_only, execution_enabled, content_hash
                    ) VALUES (
                        %s, 'live_t4', %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s::jsonb, %s, %s, TRUE, FALSE, %s
                    )
                    """,
                    (
                        campaign.campaign_id,
                        campaign.started_at,
                        campaign.planned_ends_at,
                        campaign.cycle_interval_seconds,
                        self.policy.policy_id,
                        self.policy.policy_hash_sha256,
                        campaign.code_commit_hash,
                        campaign.t4_protocol_commit_hash,
                        campaign.runtime_config_hash,
                        campaign.bridge_evidence_key_fingerprint,
                        _canonical_json(list(campaign.scope_manifest)),
                        campaign.scope_manifest_hash_sha256,
                        baseline_hash,
                        content_hash,
                    ),
                )
        except ObservationError:
            raise
        except Exception:
            raise ObservationError("Observation campaign storage is unavailable") from None
        return baseline_hash

    def build_quality_report(
        self,
        campaign_id: str,
        *,
        observed_until: datetime,
    ) -> ObservationQualityReport:
        """Build acceptance evidence only from the immutable database ledger."""

        _uuid(campaign_id, "campaign_id")
        observed_until = _utc(observed_until, "observed_until")
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute("SET TRANSACTION READ ONLY")
                report, _, _ = self._build_quality_report_with_cursor(
                    db_cursor,
                    campaign_id,
                    observed_until=observed_until,
                    lock_campaign=False,
                )
        except ObservationError:
            raise
        except Exception:
            raise ObservationError("Observation evidence storage is unavailable") from None
        return report

    def finalization_remaining_seconds(
        self,
        campaign_id: str,
        *,
        observed_at: datetime,
    ) -> int:
        """Return database-derived time until the final-slot grace has elapsed."""

        _uuid(campaign_id, "campaign_id")
        observed_at = _utc(observed_at, "observed_at")
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute("SET TRANSACTION READ ONLY")
                db_cursor.execute(
                    """
                    SELECT GREATEST(
                        0,
                        CEIL(EXTRACT(EPOCH FROM (
                            planned_ends_at
                            + make_interval(secs => cycle_interval_seconds)
                            - %s::timestamptz
                        )))::bigint
                    )
                    FROM crypto_agent.t4_observation_campaigns
                    WHERE campaign_id = %s
                    """,
                    (observed_at, campaign_id),
                )
                row = db_cursor.fetchone()
        except ObservationError:
            raise
        except Exception:
            raise ObservationError("Observation evidence storage is unavailable") from None
        if row is None:
            raise ObservationError("Observation campaign does not exist")
        remaining = _int_value(row, 0)
        if remaining < 0:
            raise ObservationError("Observation finalization time is invalid")
        return remaining

    def _build_quality_report_with_cursor(
        self,
        db_cursor: DBCursor,
        campaign_id: str,
        *,
        observed_until: datetime | None,
        lock_campaign: bool,
    ) -> tuple[ObservationQualityReport, ObservationCampaign, datetime]:
        """Derive one report from a caller-owned transaction and database clock."""

        campaign_query = """
            SELECT environment, started_at, planned_ends_at,
                   cycle_interval_seconds, observation_policy_id,
                   observation_policy_hash, code_commit_hash,
                   t4_protocol_commit_hash, runtime_config_hash,
                   bridge_evidence_key_fingerprint, scope_manifest,
                   scope_manifest_hash,
                   frozen_baseline_hash, CURRENT_TIMESTAMP
            FROM crypto_agent.t4_observation_campaigns
            WHERE campaign_id = %s
        """
        if lock_campaign:
            db_cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"t4-observation-campaign:{campaign_id}",),
            )
        db_cursor.execute(campaign_query, (campaign_id,))
        row = db_cursor.fetchone()
        if row is None:
            raise ObservationError("Observation campaign does not exist")
        database_now = _datetime_value(row, 13, "database_now")
        requested_until = (
            database_now
            if observed_until is None
            else _utc(observed_until, "observed_until")
        )
        if requested_until > database_now:
            raise ObservationError("Observation cutoff cannot be in the future")
        planned_ends_at = _datetime_value(row, 2, "planned_ends_at")
        effective_until = min(requested_until, planned_ends_at)
        scopes = _scope_manifest(_value(row, 10))
        campaign = ObservationCampaign(
            campaign_id=campaign_id,
            environment=str(_value(row, 0)),
            started_at=_datetime_value(row, 1, "started_at"),
            planned_ends_at=planned_ends_at,
            cycle_interval_seconds=_int_value(row, 3),
            code_commit_hash=str(_value(row, 6)),
            t4_protocol_commit_hash=str(_value(row, 7)),
            runtime_config_hash=str(_value(row, 8)),
            bridge_evidence_key_fingerprint=str(_value(row, 9)),
            scope_manifest=scopes,
        )
        stored_scope_hash = str(_value(row, 11))
        stored_baseline_hash = str(_value(row, 12))
        policy_matches = bool(
            _value(row, 4) == self.policy.policy_id
            and _value(row, 5) == self.policy.policy_hash_sha256
        )
        baseline_matches = bool(
            stored_scope_hash == campaign.scope_manifest_hash_sha256
            and stored_baseline_hash
            == campaign.frozen_baseline_hash_sha256(self.policy)
        )
        db_cursor.execute(
            """
            WITH campaign AS (
                SELECT campaign_id, started_at, planned_ends_at,
                       cycle_interval_seconds, scope_manifest,
                       bridge_evidence_key_fingerprint
                FROM crypto_agent.t4_observation_campaigns
                WHERE campaign_id = %s
            ), scopes AS (
                SELECT jsonb_array_elements_text(scope_manifest) AS scope_key,
                       started_at, cycle_interval_seconds
                FROM campaign
            ), expected_slots AS (
                SELECT scope_key, cycle_interval_seconds,
                       generate_series(
                           started_at + make_interval(secs => cycle_interval_seconds),
                           %s,
                           make_interval(secs => cycle_interval_seconds)
                       ) AS expected_at
                FROM scopes
            ), slot_evidence AS (
                SELECT slot.scope_key, slot.expected_at,
                       slot.cycle_interval_seconds,
                       cycle.observation_cycle_id, cycle.outcome,
                       cycle.gap_explanation_code,
                       cycle.t4_batch_id, cycle.research_run_id,
                       cycle.bridge_schema_version,
                       cycle.bridge_rtt_milliseconds,
                       (
                           cycle.observation_cycle_id IS NULL
                           OR (cycle.outcome = 'missed'
                               AND cycle.gap_explanation_code IS NULL)
                       ) AS unexplained
                FROM expected_slots slot
                LEFT JOIN crypto_agent.t4_observation_cycles cycle
                  ON cycle.campaign_id = %s
                 AND cycle.scope_key = slot.scope_key
                 AND cycle.expected_at = slot.expected_at
            ), marked AS (
                SELECT *, SUM(CASE WHEN unexplained THEN 0 ELSE 1 END)
                    OVER (PARTITION BY scope_key ORDER BY expected_at) AS gap_group
                FROM slot_evidence
            ), unexplained_runs AS (
                SELECT scope_key, gap_group,
                       COUNT(*) * MAX(cycle_interval_seconds) AS gap_seconds
                FROM marked WHERE unexplained
                GROUP BY scope_key, gap_group
            ), safety_bounds AS (
                SELECT campaign.*,
                       COALESCE(checkpoint.event_at, %s) AS bridge_cutoff_at,
                       checkpoint.sequence_no AS bridge_checkpoint_sequence_no,
                       CASE
                           WHEN checkpoint.sequence_no IS NULL THEN %s
                           ELSE LEAST(
                               checkpoint.event_at,
                               campaign.planned_ends_at + make_interval(
                                   secs => campaign.cycle_interval_seconds
                               )
                           )
                       END AS session_cutoff_at
                FROM campaign
                LEFT JOIN LATERAL (
                    SELECT event.event_at, event.sequence_no
                    FROM crypto_agent.t4_bridge_observation_events event
                    WHERE event.campaign_id = campaign.campaign_id
                      AND event.evidence_key_fingerprint_sha256 =
                            campaign.bridge_evidence_key_fingerprint
                      AND event.event_type = 'campaign_checkpoint'
                      AND event.reason_code =
                            'OBSERVATION_CAMPAIGN_CHECKPOINT'
                    ORDER BY event.sequence_no DESC
                    LIMIT 1
                ) checkpoint ON TRUE
            ), session_safety AS (
                SELECT
                    COUNT(*) FILTER (WHERE event_type = 'read_only_violation')
                        AS read_only_violations,
                    COUNT(*) FILTER (WHERE event_type = 'order_route_attempt')
                        AS order_route_attempts,
                    COUNT(*) FILTER (WHERE event_type = 'secret_leak_detected')
                        AS secret_leaks,
                    COUNT(*) FILTER (WHERE event_type = 'integrity_failure')
                        AS integrity_failures,
                    COUNT(*) FILTER (WHERE event_type = 'fail_closed_violation')
                        AS fail_closed_violations,
                    COUNT(*) FILTER (
                        WHERE event_type IN (
                            'fixture_data_attempt', 'replay_data_attempt',
                            'backfill_attempt'
                        )
                    ) AS prohibited_data_mode_violations
                FROM crypto_agent.t4_observation_session_events event
                CROSS JOIN safety_bounds bounds
                WHERE event.campaign_id = bounds.campaign_id
                  AND event.event_at BETWEEN bounds.started_at
                                         AND bounds.session_cutoff_at
            ), bridge_safety AS (
                SELECT
                    COUNT(*) FILTER (WHERE NOT event.read_only)
                        AS read_only_violations,
                    COUNT(*) FILTER (
                        WHERE event.order_routes_exposed
                           OR event.event_type = 'outbound_rejected'
                    ) AS order_route_attempts,
                    COUNT(*) FILTER (
                        WHERE event.campaign_id IS DISTINCT FROM
                                bounds.campaign_id
                           OR event.environment <> 'live_t4'
                           OR event.bridge_schema_version <> 5
                           OR NOT event.signature_verified
                    ) AS integrity_failures
                FROM crypto_agent.t4_bridge_observation_events event
                CROSS JOIN safety_bounds bounds
                WHERE event.evidence_key_fingerprint_sha256 =
                        bounds.bridge_evidence_key_fingerprint
                  AND event.event_at BETWEEN bounds.started_at
                                         AND bounds.bridge_cutoff_at
                  AND (
                      bounds.bridge_checkpoint_sequence_no IS NULL
                      OR event.sequence_no <=
                            bounds.bridge_checkpoint_sequence_no
                  )
            ), scenario_safety AS (
                SELECT
                    (
                        SELECT COUNT(*)
                        FROM crypto_agent.t4_observation_scenario_trials trial
                        JOIN crypto_agent.alerts alert
                          ON alert.research_run_id = trial.research_run_id
                        WHERE trial.campaign_id = bounds.campaign_id
                          AND trial.outcome = 'pass'
                          AND trial.scenario_code IN (
                              'missing_data', 'rate_limit', 'stale_data',
                              'replay_blocked'
                          )
                    ) + (
                        SELECT COUNT(*)
                        FROM crypto_agent.t4_observation_scenario_trials trial
                        JOIN crypto_agent.alerts alert
                          ON alert.research_run_id = trial.research_run_id
                        JOIN crypto_agent.alert_delivery_outbox outbox
                          ON outbox.alert_id = alert.alert_id
                        WHERE trial.campaign_id = bounds.campaign_id
                          AND trial.outcome = 'pass'
                          AND trial.scenario_code IN (
                              'missing_data', 'rate_limit', 'stale_data',
                              'replay_blocked'
                          )
                    ) + (
                        SELECT COUNT(*)
                        FROM crypto_agent.t4_observation_scenario_trials trial
                        JOIN crypto_agent.alerts alert
                          ON alert.research_run_id = trial.research_run_id
                        JOIN crypto_agent.alert_delivery_outbox outbox
                          ON outbox.alert_id = alert.alert_id
                        JOIN crypto_agent.alert_delivery_attempts attempt
                          ON attempt.alert_delivery_outbox_id =
                                outbox.alert_delivery_outbox_id
                        WHERE trial.campaign_id = bounds.campaign_id
                          AND trial.outcome = 'pass'
                          AND trial.scenario_code IN (
                              'missing_data', 'rate_limit', 'stale_data',
                              'replay_blocked'
                          )
                    ) AS fail_closed_violations
                FROM safety_bounds bounds
            ), safety AS (
                SELECT
                    session_safety.read_only_violations
                        + bridge_safety.read_only_violations
                            AS read_only_violations,
                    session_safety.order_route_attempts
                        + bridge_safety.order_route_attempts
                            AS order_route_attempts,
                    session_safety.secret_leaks AS secret_leaks,
                    session_safety.integrity_failures
                        + bridge_safety.integrity_failures
                            AS integrity_failures,
                    session_safety.fail_closed_violations
                        + scenario_safety.fail_closed_violations
                        AS fail_closed_violations,
                    session_safety.prohibited_data_mode_violations
                        AS prohibited_data_mode_violations
                FROM session_safety
                CROSS JOIN bridge_safety
                CROSS JOIN scenario_safety
            ), cycle_stats AS (
                SELECT
                    COUNT(*) AS expected_cycles,
                    COUNT(*) FILTER (
                        WHERE outcome IN ('success', 'failure')
                    ) AS attempted_cycles,
                    COUNT(*) FILTER (WHERE outcome = 'success')
                        AS successful_cycles,
                    COUNT(*) FILTER (
                        WHERE outcome = 'success'
                          AND t4_batch_id IS NOT NULL
                          AND research_run_id IS NOT NULL
                    ) AS linked_successful_cycles,
                    COUNT(*) FILTER (
                        WHERE outcome = 'success'
                          AND bridge_schema_version = %s
                    ) AS schema_valid_successful_cycles,
                    COUNT(DISTINCT scope_key) FILTER (
                        WHERE outcome = 'success'
                    ) AS covered_scope_count,
                    COALESCE(
                        (SELECT MAX(gap_seconds)::double precision
                         FROM unexplained_runs),
                        0.0
                    ) AS maximum_unexplained_gap_seconds,
                    percentile_cont(0.95) WITHIN GROUP (
                        ORDER BY bridge_rtt_milliseconds
                    ) FILTER (WHERE outcome = 'success') / 1000.0 AS rtt_p95,
                    percentile_cont(0.99) WITHIN GROUP (
                        ORDER BY bridge_rtt_milliseconds
                    ) FILTER (WHERE outcome = 'success') / 1000.0 AS rtt_p99
                FROM slot_evidence
            )
            SELECT
                cycle_stats.expected_cycles,
                cycle_stats.attempted_cycles,
                cycle_stats.successful_cycles,
                cycle_stats.linked_successful_cycles,
                cycle_stats.schema_valid_successful_cycles,
                cycle_stats.covered_scope_count,
                cycle_stats.maximum_unexplained_gap_seconds,
                cycle_stats.rtt_p95, cycle_stats.rtt_p99,
                safety.read_only_violations, safety.order_route_attempts,
                safety.secret_leaks, safety.integrity_failures,
                safety.fail_closed_violations,
                safety.prohibited_data_mode_violations
            FROM cycle_stats CROSS JOIN safety
            """,
            (
                campaign_id,
                effective_until,
                campaign_id,
                effective_until,
                effective_until,
                self.policy.required_bridge_schema_version,
            ),
        )
        aggregate = db_cursor.fetchone()
        if aggregate is None:
            raise ObservationError("Observation campaign evidence is unavailable")
        db_cursor.execute(
            """
            WITH scenario_outcomes AS (
                SELECT scenario_code, outcome = 'pass' AS verified
                FROM crypto_agent.t4_observation_scenario_trials
                WHERE campaign_id = %s AND completed_at <= %s
                UNION ALL
                SELECT request.scenario_code, FALSE
                FROM crypto_agent.t4_observation_scenario_trial_requests request
                WHERE request.campaign_id = %s
                  AND request.started_at <= %s
                  AND NOT EXISTS (
                      SELECT 1
                      FROM crypto_agent.t4_observation_scenario_trials trial
                      WHERE trial.trial_id = request.trial_id
                  )
            )
            SELECT scenario_code, BOOL_AND(verified)
            FROM scenario_outcomes
            GROUP BY scenario_code ORDER BY scenario_code
            """,
            (campaign_id, effective_until, campaign_id, effective_until),
        )
        scenarios = tuple(
            (str(_value(item, 0)), bool(_value(item, 1)))
            for item in db_cursor.fetchall()
        )
        evidence = ObservationEvidence(
            campaign_id=campaign_id,
            environment=campaign.environment,
            started_at=campaign.started_at,
            observed_until=effective_until,
            frozen_baseline_hash_sha256=stored_baseline_hash,
            baseline_hash_matches=baseline_matches,
            policy_hash_matches=policy_matches,
            cycle_interval_seconds=campaign.cycle_interval_seconds,
            scope_count=len(scopes),
            covered_scope_count=_int_value(aggregate, 5),
            expected_cycles=_int_value(aggregate, 0),
            attempted_cycles=_int_value(aggregate, 1),
            successful_cycles=_int_value(aggregate, 2),
            linked_successful_cycles=_int_value(aggregate, 3),
            schema_valid_successful_cycles=_int_value(aggregate, 4),
            maximum_unexplained_gap_seconds=_optional_float_value(aggregate, 6),
            bridge_rtt_p95_seconds=_optional_float_value(aggregate, 7),
            bridge_rtt_p99_seconds=_optional_float_value(aggregate, 8),
            read_only_violations=_int_value(aggregate, 9),
            order_route_attempts=_int_value(aggregate, 10),
            secret_leaks=_int_value(aggregate, 11),
            integrity_failures=_int_value(aggregate, 12),
            fail_closed_violations=_int_value(aggregate, 13),
            prohibited_data_mode_violations=_int_value(aggregate, 14),
            scenario_results=scenarios,
        )
        return evaluate_observation(evidence, self.policy), campaign, database_now

    def record_cycle(self, record: ObservationCycle) -> str:
        _validate_cycle(record)
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"t4-observation-campaign:{record.campaign_id}",),
                )
                db_cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"t4-observation:{record.campaign_id}:{record.scope_key}",),
                )
                db_cursor.execute(
                    """
                    SELECT sequence_no, content_hash
                    FROM crypto_agent.t4_observation_cycles
                    WHERE campaign_id = %s AND scope_key = %s
                    ORDER BY sequence_no DESC LIMIT 1
                    """,
                    (record.campaign_id, record.scope_key),
                )
                previous = db_cursor.fetchone()
                previous_sequence = 0 if previous is None else int(_value(previous, 0))
                previous_hash = None if previous is None else str(_value(previous, 1))
                if record.sequence_no != previous_sequence + 1:
                    raise ObservationError("Observation cycle sequence is invalid")
                content_hash = _insert_cycle(db_cursor, record, previous_hash)
        except ObservationError:
            raise
        except Exception:
            raise ObservationError("Observation cycle storage is unavailable") from None
        return content_hash

    def run_cycle(
        self,
        campaign_id: str,
        scope_key: str,
        execute: Callable[[ObservationCyclePlan], ObservationCycleExecution],
    ) -> ObservationCycleRunResult:
        """Run one due slot from a PostgreSQL-frozen schedule.

        Transaction-scoped advisory locks serialize finalization first and then
        retries for this frozen scope. Expired slots are represented only by
        honest ``missed`` records; success and failure can never be backfilled.
        """

        _uuid(campaign_id, "campaign_id")
        if not isinstance(scope_key, str) or not scope_key.strip() or len(scope_key) > 256:
            raise ObservationError(
                "Observation cycle scope is invalid",
                code="OBSERVATION_SCOPE_INVALID",
            )
        if not callable(execute):
            raise ObservationError(
                "Observation cycle executor is invalid",
                code="OBSERVATION_EXECUTOR_INVALID",
            )
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                # Keep one in-flight executor per frozen scope.  The transaction
                # still spans the external read-only fetch, so bound both lock
                # acquisition and an accidentally abandoned idle transaction.
                db_cursor.execute("SET LOCAL lock_timeout = '5s'")
                db_cursor.execute(
                    "SET LOCAL idle_in_transaction_session_timeout = '180s'"
                )
                db_cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"t4-observation-campaign:{campaign_id}",),
                )
                db_cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"t4-observation:{campaign_id}:{scope_key}",),
                )
                db_cursor.execute(
                    """
                    SELECT environment, started_at, planned_ends_at,
                           cycle_interval_seconds, scope_manifest,
                           clock_timestamp()
                    FROM crypto_agent.t4_observation_campaigns
                    WHERE campaign_id = %s
                    """,
                    (campaign_id,),
                )
                campaign_row = db_cursor.fetchone()
                if campaign_row is None:
                    raise ObservationError(
                        "Observation campaign does not exist",
                        code="OBSERVATION_CAMPAIGN_NOT_FOUND",
                    )
                if str(_value(campaign_row, 0)) != "live_t4":
                    raise ObservationError(
                        "Observation campaign is not live T4",
                        code="OBSERVATION_ENVIRONMENT_INVALID",
                    )
                started_at = _datetime_value(campaign_row, 1, "started_at")
                planned_ends_at = _datetime_value(
                    campaign_row, 2, "planned_ends_at"
                )
                interval_seconds = _int_value(campaign_row, 3)
                scopes = _scope_manifest(_value(campaign_row, 4))
                database_now = _datetime_value(campaign_row, 5, "database_now")
                if scope_key not in scopes:
                    raise ObservationError(
                        "Observation scope is outside the frozen manifest",
                        code="OBSERVATION_SCOPE_NOT_FROZEN",
                    )

                db_cursor.execute(
                    """
                    SELECT sequence_no, content_hash
                    FROM crypto_agent.t4_observation_cycles
                    WHERE campaign_id = %s AND scope_key = %s
                    ORDER BY sequence_no DESC LIMIT 1
                    """,
                    (campaign_id, scope_key),
                )
                previous = db_cursor.fetchone()
                sequence_no = 1 if previous is None else int(_value(previous, 0)) + 1
                previous_hash = None if previous is None else str(_value(previous, 1))
                missed_count = 0
                last_missed_hash: str | None = None
                last_missed_expected_at: datetime | None = None

                while True:
                    expected_at = started_at + timedelta(
                        seconds=interval_seconds * sequence_no
                    )
                    if expected_at > planned_ends_at:
                        if missed_count:
                            return ObservationCycleRunResult(
                                campaign_id=campaign_id,
                                scope_key=scope_key,
                                sequence_no=sequence_no - 1,
                                expected_at=last_missed_expected_at,
                                outcome="missed",
                                trace_id=None,
                                content_hash=last_missed_hash,
                                missed_cycles_recorded=missed_count,
                            )
                        raise ObservationError(
                            "Observation campaign schedule is complete",
                            code="OBSERVATION_SCHEDULE_COMPLETE",
                        )
                    slot_end = expected_at + timedelta(seconds=interval_seconds)
                    if database_now <= slot_end:
                        break
                    missed = ObservationCycle(
                        campaign_id=campaign_id,
                        scope_key=scope_key,
                        sequence_no=sequence_no,
                        expected_at=expected_at,
                        outcome="missed",
                    )
                    last_missed_hash = _insert_cycle(
                        db_cursor, missed, previous_hash
                    )
                    last_missed_expected_at = expected_at
                    previous_hash = last_missed_hash
                    sequence_no += 1
                    missed_count += 1

                if database_now < expected_at:
                    if missed_count:
                        return ObservationCycleRunResult(
                            campaign_id=campaign_id,
                            scope_key=scope_key,
                            sequence_no=sequence_no - 1,
                            expected_at=last_missed_expected_at,
                            outcome="missed",
                            trace_id=None,
                            content_hash=last_missed_hash,
                            missed_cycles_recorded=missed_count,
                        )
                    raise ObservationError(
                        "Observation cycle is not due",
                        code="OBSERVATION_CYCLE_NOT_DUE",
                    )

                trace_id = deterministic_observation_trace_id(
                    campaign_id, scope_key, sequence_no
                )
                plan = ObservationCyclePlan(
                    campaign_id=campaign_id,
                    scope_key=scope_key,
                    sequence_no=sequence_no,
                    expected_at=expected_at,
                    started_at=database_now,
                    trace_id=trace_id,
                )
                execution: ObservationCycleExecution | None = None
                failure_code: str | None = None
                try:
                    execution = execute(plan)
                    _validate_cycle_execution(execution, plan)
                except Exception as exc:
                    candidate_code = getattr(exc, "code", None)
                    failure_code = (
                        candidate_code
                        if isinstance(candidate_code, str)
                        and _SAFE_UPPER_CODE.fullmatch(candidate_code) is not None
                        else "OBSERVATION_CYCLE_EXECUTION_FAILED"
                    )

                db_cursor.execute("SELECT clock_timestamp()")
                finished_row = db_cursor.fetchone()
                finished_at = _datetime_value(
                    finished_row, 0, "observation_finished_at"
                )
                if failure_code is not None or execution is None:
                    failure = ObservationCycle(
                        campaign_id=campaign_id,
                        scope_key=scope_key,
                        sequence_no=sequence_no,
                        expected_at=expected_at,
                        started_at=database_now,
                        finished_at=finished_at,
                        outcome="failure",
                        error_code=failure_code,
                        trace_id=trace_id,
                    )
                    content_hash = _insert_cycle(
                        db_cursor, failure, previous_hash
                    )
                    return ObservationCycleRunResult(
                        campaign_id=campaign_id,
                        scope_key=scope_key,
                        sequence_no=sequence_no,
                        expected_at=expected_at,
                        outcome="failure",
                        trace_id=trace_id,
                        content_hash=content_hash,
                        missed_cycles_recorded=missed_count,
                        error_code=failure_code,
                    )

                success = ObservationCycle(
                    campaign_id=campaign_id,
                    scope_key=scope_key,
                    sequence_no=sequence_no,
                    expected_at=expected_at,
                    started_at=database_now,
                    finished_at=finished_at,
                    outcome="success",
                    t4_batch_id=execution.t4_batch_id,
                    research_run_id=execution.research_run_id,
                    analysis_input_hash=execution.analysis_input_hash,
                    bridge_schema_version=execution.bridge_schema_version,
                    raw_payload_hash=execution.raw_payload_hash,
                    bridge_rtt_milliseconds=execution.bridge_rtt_milliseconds,
                    trace_id=trace_id,
                )
                content_hash = _insert_cycle(db_cursor, success, previous_hash)
                return ObservationCycleRunResult(
                    campaign_id=campaign_id,
                    scope_key=scope_key,
                    sequence_no=sequence_no,
                    expected_at=expected_at,
                    outcome="success",
                    trace_id=trace_id,
                    content_hash=content_hash,
                    missed_cycles_recorded=missed_count,
                    t4_batch_id=execution.t4_batch_id,
                    research_run_id=execution.research_run_id,
                )
        except ObservationError:
            raise
        except Exception:
            raise ObservationError(
                "Observation cycle execution is unavailable",
                code="OBSERVATION_CYCLE_UNAVAILABLE",
            ) from None

    def record_session_event(self, event: ObservationSessionEvent) -> str:
        _validate_event(event)
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                    (f"t4-observation-campaign:{event.campaign_id}",),
                )
                db_cursor.execute(
                    """
                    SELECT sequence_no, content_hash
                    FROM crypto_agent.t4_observation_session_events
                    WHERE campaign_id = %s
                    ORDER BY sequence_no DESC LIMIT 1
                    """,
                    (event.campaign_id,),
                )
                previous = db_cursor.fetchone()
                previous_sequence = 0 if previous is None else int(_value(previous, 0))
                previous_hash = None if previous is None else str(_value(previous, 1))
                if event.sequence_no != previous_sequence + 1:
                    raise ObservationError("Observation event sequence is invalid")
                content_hash = _hash(
                    {
                        **_event_payload(event),
                        "previous_event_hash": previous_hash,
                    }
                )
                db_cursor.execute(
                    """
                    INSERT INTO crypto_agent.t4_observation_session_events (
                        campaign_id, sequence_no, event_at, event_type, outcome,
                        scenario_code, detail_code, read_only, execution_enabled,
                        previous_event_hash, content_hash
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, FALSE, %s, %s)
                    """,
                    (
                        event.campaign_id,
                        event.sequence_no,
                        event.event_at,
                        event.event_type,
                        event.outcome,
                        event.scenario_code,
                        event.detail_code,
                        previous_hash,
                        content_hash,
                    ),
                )
        except ObservationError:
            raise
        except Exception:
            raise ObservationError("Observation event storage is unavailable") from None
        return content_hash

    def store_final_report(
        self,
        report: ObservationQualityReport,
        *,
        bridge_checkpoint: ObservationBridgeCheckpoint,
    ) -> str:
        """Persist only the canonical ledger report rebuilt under one DB lock."""

        _validate_report(report)
        bridge_checkpoint.validate()
        if bridge_checkpoint.campaign_id != report.campaign_id:
            raise ObservationError("Observation checkpoint campaign does not match")
        if report.policy_id != self.policy.policy_id or (
            report.policy_hash_sha256 != self.policy.policy_hash_sha256
        ):
            raise ObservationError("Final report policy baseline does not match")
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                canonical, campaign, database_now = (
                    self._build_quality_report_with_cursor(
                        db_cursor,
                        report.campaign_id,
                        observed_until=None,
                        lock_campaign=True,
                    )
                )
                finalization_not_before = campaign.planned_ends_at + timedelta(
                    seconds=campaign.cycle_interval_seconds
                )
                if database_now < finalization_not_before:
                    raise ObservationError(
                        "Final report requires the full window and final-slot grace"
                    )
                _validate_report(canonical)
                if canonical.elapsed_seconds < self.policy.minimum_elapsed_seconds:
                    raise ObservationError(
                        "Final report requires the full real observation window"
                    )
                if report.as_payload() != canonical.as_payload():
                    raise ObservationError(
                        "Final report must exactly match the canonical database ledger"
                    )
                db_cursor.execute(
                    """
                    SELECT event_id
                    FROM crypto_agent.t4_bridge_observation_events
                    WHERE campaign_id = %s
                      AND event_type = 'campaign_checkpoint'
                      AND reason_code = 'OBSERVATION_CAMPAIGN_CHECKPOINT'
                      AND action_request_id = %s
                      AND sequence_no = %s
                      AND event_hash_sha256 = %s
                      AND scope_key IS NULL
                      AND scenario_code IS NULL
                      AND control_action = 'checkpoint'
                      AND control_step = 'consumed'
                    """,
                    (
                        bridge_checkpoint.campaign_id,
                        bridge_checkpoint.action_request_id,
                        bridge_checkpoint.sequence_no,
                        bridge_checkpoint.event_hash_sha256,
                    ),
                )
                checkpoint_row = db_cursor.fetchone()
                if checkpoint_row is None:
                    raise ObservationError(
                        "Final report requires the verified bridge checkpoint"
                    )
                checkpoint_event_id = str(_value(checkpoint_row, 0))
                _uuid(checkpoint_event_id, "bridge_checkpoint_event_id")
                payload = canonical.as_payload()
                report_hash = canonical.report_hash_sha256
                content_hash = _hash(
                    {
                        "campaign_id": canonical.campaign_id,
                        "report_hash_sha256": report_hash,
                    }
                )
                db_cursor.execute(
                    """
                    INSERT INTO crypto_agent.t4_observation_quality_reports (
                        campaign_id, generated_at, observed_until, overall_status,
                        v1_gate_passed, observation_policy_id,
                        observation_policy_hash, frozen_baseline_hash, report,
                        report_hash, content_hash, bridge_checkpoint_event_id,
                        bridge_checkpoint_sequence_no,
                        bridge_checkpoint_event_hash,
                        bridge_checkpoint_action_request_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s,
                        %s, %s, %s, %s, %s
                    )
                    """,
                    (
                        canonical.campaign_id,
                        canonical.generated_at,
                        canonical.observed_until,
                        canonical.overall_status.value,
                        canonical.v1_gate_passed,
                        canonical.policy_id,
                        canonical.policy_hash_sha256,
                        canonical.frozen_baseline_hash_sha256,
                        _canonical_json(payload),
                        report_hash,
                        content_hash,
                        checkpoint_event_id,
                        bridge_checkpoint.sequence_no,
                        bridge_checkpoint.event_hash_sha256,
                        bridge_checkpoint.action_request_id,
                    ),
                )
        except ObservationError:
            raise
        except Exception:
            raise ObservationError("Observation quality report storage is unavailable") from None
        return report_hash


def _validate_evidence(evidence: ObservationEvidence) -> None:
    _uuid(evidence.campaign_id, "campaign_id")
    _utc(evidence.started_at, "started_at")
    _utc(evidence.observed_until, "observed_until")
    _sha256(evidence.frozen_baseline_hash_sha256, "frozen_baseline_hash_sha256")
    integer_fields = (
        evidence.cycle_interval_seconds,
        evidence.scope_count,
        evidence.covered_scope_count,
        evidence.expected_cycles,
        evidence.attempted_cycles,
        evidence.successful_cycles,
        evidence.linked_successful_cycles,
        evidence.schema_valid_successful_cycles,
        evidence.read_only_violations,
        evidence.order_route_attempts,
        evidence.secret_leaks,
        evidence.integrity_failures,
        evidence.fail_closed_violations,
        evidence.prohibited_data_mode_violations,
    )
    if any(type(value) is not int or value < 0 for value in integer_fields):
        raise ObservationError("Observation counters must be non-negative integers")
    if evidence.cycle_interval_seconds < 60:
        raise ObservationError("Observation cycle interval is invalid")
    if type(evidence.baseline_hash_matches) is not bool or (
        type(evidence.policy_hash_matches) is not bool
    ):
        raise ObservationError("Observation baseline attestations must be booleans")
    if evidence.covered_scope_count > evidence.scope_count:
        raise ObservationError("Covered observation scope count is invalid")
    if evidence.successful_cycles > evidence.attempted_cycles:
        raise ObservationError("Successful observation cycle count is invalid")
    if evidence.linked_successful_cycles > evidence.successful_cycles or (
        evidence.schema_valid_successful_cycles > evidence.successful_cycles
    ):
        raise ObservationError("Successful observation evidence counts are invalid")
    if evidence.observed_until < evidence.started_at:
        raise ObservationError("Observation end precedes its start")
    scenario_names = tuple(name for name, _ in evidence.scenario_results)
    if (
        len(set(scenario_names)) != len(scenario_names)
        or any(_SAFE_LOWER_CODE.fullmatch(name) is None for name in scenario_names)
        or any(type(result) is not bool for _, result in evidence.scenario_results)
    ):
        raise ObservationError("Observation scenario results are invalid")
    for value in (
        evidence.maximum_unexplained_gap_seconds,
        evidence.bridge_rtt_p95_seconds,
        evidence.bridge_rtt_p99_seconds,
    ):
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value < 0
        ):
            raise ObservationError("Observation duration metric is invalid")


def deterministic_observation_trace_id(
    campaign_id: str,
    scope_key: str,
    sequence_no: int,
) -> str:
    """Stable UUID for retry-safe monitoring linkage without caller entropy."""

    _uuid(campaign_id, "campaign_id")
    if not isinstance(scope_key, str) or not scope_key.strip() or len(scope_key) > 256:
        raise ObservationError("Observation cycle scope is invalid")
    if type(sequence_no) is not int or sequence_no <= 0:
        raise ObservationError("Observation cycle sequence is invalid")
    return str(
        uuid5(
            NAMESPACE_URL,
            f"crypto-agent:t4-observation:{campaign_id}:{scope_key}:{sequence_no}",
        )
    )


def _validate_cycle_execution(
    execution: ObservationCycleExecution,
    plan: ObservationCyclePlan,
) -> None:
    if not isinstance(execution, ObservationCycleExecution):
        raise ObservationError("Observation execution evidence is invalid")
    if execution.trace_id != plan.trace_id:
        raise ObservationError("Observation monitoring trace does not match")
    _uuid(execution.trace_id, "trace_id")
    _sha256(execution.raw_payload_hash, "raw_payload_hash")
    _sha256(execution.analysis_input_hash, "analysis_input_hash")
    if (
        type(execution.t4_batch_id) is not int
        or execution.t4_batch_id <= 0
        or type(execution.research_run_id) is not int
        or execution.research_run_id <= 0
        or execution.bridge_schema_version != 5
        or type(execution.bridge_rtt_milliseconds) is not int
        or execution.bridge_rtt_milliseconds < 0
    ):
        raise ObservationError("Observation execution links are invalid")
    if (
        execution.environment != "live_t4"
        or execution.external_delivery_eligible is not True
        or execution.replay is not False
        or execution.synthetic is not False
    ):
        raise ObservationError("Observation execution is not eligible live T4 data")


def _insert_cycle(
    db_cursor: DBCursor,
    record: ObservationCycle,
    previous_hash: str | None,
) -> str:
    _validate_cycle(record)
    if record.outcome == "success":
        _insert_research_input_link(db_cursor, record)
    content_hash = _hash(
        {
            **_cycle_payload(record),
            "previous_cycle_hash": previous_hash,
        }
    )
    db_cursor.execute(
        """
        INSERT INTO crypto_agent.t4_observation_cycles (
            campaign_id, scope_key, sequence_no, expected_at,
            started_at, finished_at, outcome, error_code,
            gap_explanation_code, t4_batch_id,
            research_run_id, analysis_input_hash, bridge_schema_version,
            raw_payload_hash,
            bridge_rtt_milliseconds, trace_id, read_only, execution_enabled,
            previous_cycle_hash, content_hash
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, TRUE, FALSE, %s, %s
        )
        """,
        (
            record.campaign_id,
            record.scope_key,
            record.sequence_no,
            record.expected_at,
            record.started_at,
            record.finished_at,
            record.outcome,
            record.error_code,
            record.gap_explanation_code,
            record.t4_batch_id,
            record.research_run_id,
            record.analysis_input_hash,
            record.bridge_schema_version,
            record.raw_payload_hash,
            record.bridge_rtt_milliseconds,
            record.trace_id,
            previous_hash,
            content_hash,
        ),
    )
    return content_hash


def _insert_research_input_link(
    db_cursor: DBCursor,
    record: ObservationCycle,
) -> None:
    """Persist the exact immutable T4 batch consumed by one research run."""

    if (
        record.t4_batch_id is None
        or record.research_run_id is None
        or record.analysis_input_hash is None
        or record.raw_payload_hash is None
        or record.trace_id is None
    ):
        raise ObservationError("Observation research input link is incomplete")
    content_hash = _hash(
        {
            "campaign_id": record.campaign_id,
            "scope_key": record.scope_key,
            "sequence_no": record.sequence_no,
            "expected_at": _iso(record.expected_at),
            "t4_batch_id": record.t4_batch_id,
            "research_run_id": record.research_run_id,
            "raw_payload_hash": record.raw_payload_hash,
            "analysis_input_hash": record.analysis_input_hash,
            "trace_id": record.trace_id,
        }
    )
    db_cursor.execute(
        """
        INSERT INTO crypto_agent.t4_observation_research_inputs (
            campaign_id, scope_key, sequence_no, expected_at, t4_batch_id,
            research_run_id, raw_payload_hash, analysis_input_hash, trace_id,
            content_hash
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            record.campaign_id,
            record.scope_key,
            record.sequence_no,
            record.expected_at,
            record.t4_batch_id,
            record.research_run_id,
            record.raw_payload_hash,
            record.analysis_input_hash,
            record.trace_id,
            content_hash,
        ),
    )


def _validate_cycle(record: ObservationCycle) -> None:
    _uuid(record.campaign_id, "campaign_id")
    _utc(record.expected_at, "expected_at")
    if not record.scope_key.strip() or len(record.scope_key) > 256:
        raise ObservationError("Observation cycle scope is invalid")
    if type(record.sequence_no) is not int or record.sequence_no <= 0:
        raise ObservationError("Observation cycle sequence is invalid")
    if record.outcome not in {"success", "failure", "missed"}:
        raise ObservationError("Observation cycle outcome is invalid")
    if record.error_code is not None and _SAFE_UPPER_CODE.fullmatch(record.error_code) is None:
        raise ObservationError("Observation cycle error code is invalid")
    if record.gap_explanation_code is not None and (
        _SAFE_UPPER_CODE.fullmatch(record.gap_explanation_code) is None
    ):
        raise ObservationError("Observation gap explanation is invalid")
    if record.outcome == "success":
        if (
            record.started_at is None
            or record.finished_at is None
            or record.error_code is not None
            or record.gap_explanation_code is not None
            or record.t4_batch_id is None
            or record.research_run_id is None
            or record.analysis_input_hash is None
            or record.bridge_schema_version is None
            or record.raw_payload_hash is None
            or record.bridge_rtt_milliseconds is None
            or record.trace_id is None
        ):
            raise ObservationError("Successful observation cycle evidence is incomplete")
        _sha256(record.raw_payload_hash, "raw_payload_hash")
        _sha256(record.analysis_input_hash, "analysis_input_hash")
        if (
            type(record.t4_batch_id) is not int
            or record.t4_batch_id <= 0
            or type(record.research_run_id) is not int
            or record.research_run_id <= 0
            or type(record.bridge_schema_version) is not int
            or record.bridge_schema_version <= 0
            or type(record.bridge_rtt_milliseconds) is not int
            or record.bridge_rtt_milliseconds < 0
        ):
            raise ObservationError("Successful observation cycle links are invalid")
        _uuid(record.trace_id, "trace_id")
    elif record.outcome == "failure":
        if (
            record.started_at is None
            or record.finished_at is None
            or record.error_code is None
            or record.gap_explanation_code is not None
            or record.t4_batch_id is not None
            or record.research_run_id is not None
            or record.analysis_input_hash is not None
            or record.bridge_schema_version is not None
            or record.raw_payload_hash is not None
            or record.trace_id is None
        ):
            raise ObservationError("Failed observation cycle evidence is incomplete")
        _uuid(record.trace_id, "trace_id")
    elif any(
        value is not None
        for value in (
            record.started_at,
            record.finished_at,
            record.error_code,
            record.gap_explanation_code,
            record.t4_batch_id,
            record.research_run_id,
            record.analysis_input_hash,
            record.bridge_schema_version,
            record.raw_payload_hash,
            record.bridge_rtt_milliseconds,
            record.trace_id,
        )
    ):
        raise ObservationError("Missed observation cycle evidence is invalid")
    if record.started_at is not None:
        _utc(record.started_at, "started_at")
    if record.finished_at is not None:
        _utc(record.finished_at, "finished_at")
    if (
        record.started_at is not None
        and record.finished_at is not None
        and record.finished_at < record.started_at
    ):
        raise ObservationError("Observation cycle end precedes its start")


def _validate_event(event: ObservationSessionEvent) -> None:
    _uuid(event.campaign_id, "campaign_id")
    _utc(event.event_at, "event_at")
    if type(event.sequence_no) is not int or event.sequence_no <= 0:
        raise ObservationError("Observation event sequence is invalid")
    if _SAFE_LOWER_CODE.fullmatch(event.event_type) is None:
        raise ObservationError("Observation event type is invalid")
    if event.outcome not in {"pass", "fail"}:
        raise ObservationError("Observation event outcome is invalid")
    if event.scenario_code is not None and (
        _SAFE_LOWER_CODE.fullmatch(event.scenario_code) is None
    ):
        raise ObservationError("Observation scenario code is invalid")
    if event.scenario_code is not None and event.outcome == "pass":
        raise ObservationError(
            "Passing scenario evidence is disabled until objective references are verified"
        )
    if event.detail_code is not None and _SAFE_UPPER_CODE.fullmatch(event.detail_code) is None:
        raise ObservationError("Observation event detail code is invalid")


def _cycle_payload(record: ObservationCycle) -> dict[str, object]:
    return {
        "campaign_id": record.campaign_id,
        "scope_key": record.scope_key,
        "sequence_no": record.sequence_no,
        "expected_at": _iso(record.expected_at),
        "started_at": None if record.started_at is None else _iso(record.started_at),
        "finished_at": None if record.finished_at is None else _iso(record.finished_at),
        "outcome": record.outcome,
        "error_code": record.error_code,
        "gap_explanation_code": record.gap_explanation_code,
        "t4_batch_id": record.t4_batch_id,
        "research_run_id": record.research_run_id,
        "analysis_input_hash": record.analysis_input_hash,
        "bridge_schema_version": record.bridge_schema_version,
        "raw_payload_hash": record.raw_payload_hash,
        "bridge_rtt_milliseconds": record.bridge_rtt_milliseconds,
        "trace_id": record.trace_id,
        "read_only": True,
        "execution_enabled": False,
    }


def _event_payload(event: ObservationSessionEvent) -> dict[str, object]:
    return {
        "campaign_id": event.campaign_id,
        "sequence_no": event.sequence_no,
        "event_at": _iso(event.event_at),
        "event_type": event.event_type,
        "outcome": event.outcome,
        "scenario_code": event.scenario_code,
        "detail_code": event.detail_code,
        "read_only": True,
        "execution_enabled": False,
    }


def _validate_report(report: ObservationQualityReport) -> None:
    _uuid(report.campaign_id, "campaign_id")
    _utc(report.generated_at, "generated_at")
    _utc(report.observed_until, "observed_until")
    _sha256(report.policy_hash_sha256, "policy_hash_sha256")
    _sha256(report.frozen_baseline_hash_sha256, "frozen_baseline_hash_sha256")
    if type(report.elapsed_seconds) is not int or report.elapsed_seconds < 0:
        raise ObservationError("Observation report elapsed time is invalid")
    if report.generated_at != report.observed_until:
        raise ObservationError("Observation report generation boundary is invalid")
    criterion_ids = tuple(item.criterion_id for item in report.criteria)
    if (
        not isinstance(report.overall_status, CriterionStatus)
        or not report.criteria
        or len(set(criterion_ids)) != len(criterion_ids)
        or any(_SAFE_LOWER_CODE.fullmatch(value) is None for value in criterion_ids)
        or any(
            not isinstance(item.status, CriterionStatus)
            or type(item.mandatory) is not bool
            for item in report.criteria
        )
    ):
        raise ObservationError("Observation report criteria are invalid")
    mandatory = tuple(item for item in report.criteria if item.mandatory)
    gate_expected = bool(
        mandatory
        and report.overall_status is CriterionStatus.PASS
        and all(item.status is CriterionStatus.PASS for item in mandatory)
    )
    if type(report.v1_gate_passed) is not bool or report.v1_gate_passed != gate_expected:
        raise ObservationError("Observation report gate is inconsistent")


def _minimum(
    criterion_id: str,
    actual: int,
    threshold: int,
    *,
    incomplete_is_not_observed: bool = False,
) -> CriterionResult:
    status = CriterionStatus.PASS if actual >= threshold else CriterionStatus.FAIL
    if incomplete_is_not_observed and actual < threshold:
        status = CriterionStatus.NOT_OBSERVED
    return CriterionResult(criterion_id, status, actual, threshold)


def _minimum_optional(
    criterion_id: str,
    actual: float | None,
    threshold: float,
) -> CriterionResult:
    if actual is None:
        return CriterionResult(criterion_id, CriterionStatus.NOT_OBSERVED, None, threshold)
    return CriterionResult(
        criterion_id,
        CriterionStatus.PASS if actual >= threshold else CriterionStatus.FAIL,
        round(actual, 8),
        threshold,
    )


def _maximum_optional(
    criterion_id: str,
    actual: float | None,
    threshold: float,
) -> CriterionResult:
    if actual is None:
        return CriterionResult(criterion_id, CriterionStatus.NOT_OBSERVED, None, threshold)
    return CriterionResult(
        criterion_id,
        CriterionStatus.PASS if actual <= threshold else CriterionStatus.FAIL,
        actual,
        threshold,
    )


def _boolean(criterion_id: str, actual: bool, threshold: object) -> CriterionResult:
    return CriterionResult(
        criterion_id,
        CriterionStatus.PASS if actual else CriterionStatus.FAIL,
        actual,
        threshold,
    )


def _zero(criterion_id: str, actual: int) -> CriterionResult:
    return CriterionResult(
        criterion_id,
        CriterionStatus.PASS if actual == 0 else CriterionStatus.FAIL,
        actual,
        0,
    )


def _coverage(scope_count: int, covered_scope_count: int) -> CriterionResult:
    if scope_count == 0:
        status = CriterionStatus.NOT_OBSERVED
    else:
        status = (
            CriterionStatus.PASS
            if covered_scope_count == scope_count
            else CriterionStatus.FAIL
        )
    return CriterionResult(
        "configured_scope_coverage",
        status,
        {"covered": covered_scope_count, "configured": scope_count},
        "all configured scopes covered",
    )


def _count_match(criterion_id: str, actual: int, expected: int) -> CriterionResult:
    status = (
        CriterionStatus.NOT_OBSERVED
        if expected == 0
        else CriterionStatus.PASS
        if actual == expected
        else CriterionStatus.FAIL
    )
    return CriterionResult(criterion_id, status, actual, expected)


def _latency(
    evidence: ObservationEvidence,
    policy: ObservationPolicy,
) -> CriterionResult:
    actual = {
        "p95_seconds": evidence.bridge_rtt_p95_seconds,
        "p99_seconds": evidence.bridge_rtt_p99_seconds,
    }
    threshold = {
        "p95_seconds": policy.maximum_bridge_rtt_p95_seconds,
        "p99_seconds": policy.maximum_bridge_rtt_p99_seconds,
    }
    if evidence.bridge_rtt_p95_seconds is None or evidence.bridge_rtt_p99_seconds is None:
        status = CriterionStatus.NOT_OBSERVED
    elif (
        evidence.bridge_rtt_p95_seconds <= policy.maximum_bridge_rtt_p95_seconds
        and evidence.bridge_rtt_p99_seconds <= policy.maximum_bridge_rtt_p99_seconds
    ):
        status = CriterionStatus.PASS
    else:
        status = CriterionStatus.FAIL
    return CriterionResult("bridge_round_trip_latency", status, actual, threshold)


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return min(1.0, numerator / denominator)


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _iso(value: datetime) -> str:
    return _utc(value, "timestamp").isoformat().replace("+00:00", "Z")


def _utc(value: datetime, field_name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ObservationError(f"{field_name} must be timezone-aware")
    return value.astimezone(UTC)


def _uuid(value: str, field_name: str) -> None:
    try:
        parsed = UUID(value)
    except (TypeError, ValueError, AttributeError):
        raise ObservationError(f"{field_name} must be a UUID") from None
    if str(parsed) != value:
        raise ObservationError(f"{field_name} must use canonical UUID text")


def _sha256(value: str, field_name: str) -> None:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ObservationError(f"{field_name} must be lowercase SHA-256")


def _value(row: object, index: int) -> Any:
    try:
        return row[index]  # type: ignore[index]
    except (IndexError, KeyError, TypeError):
        raise ObservationError("Observation storage returned an invalid row") from None


def _int_value(row: object, index: int) -> int:
    value = _value(row, index)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ObservationError("Observation storage returned an invalid counter")
    return int(value)


def _optional_float_value(row: object, index: int) -> float | None:
    value = _value(row, index)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ObservationError("Observation storage returned an invalid duration")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise ObservationError("Observation storage returned an invalid duration")
    return result


def _datetime_value(row: object, index: int, field_name: str) -> datetime:
    value = _value(row, index)
    if not isinstance(value, datetime):
        raise ObservationError(f"Observation storage returned an invalid {field_name}")
    return _utc(value, field_name)


def _scope_manifest(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            raise ObservationError("Observation scope manifest is invalid") from None
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ObservationError("Observation scope manifest is invalid")
    scopes = tuple(value)
    if (
        not scopes
        or len(set(scopes)) != len(scopes)
        or tuple(sorted(scopes)) != scopes
    ):
        raise ObservationError("Observation scope manifest is invalid")
    return scopes

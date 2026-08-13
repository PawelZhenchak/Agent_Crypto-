from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import re
import threading
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol, cast
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import UUID, uuid5

from fastapi import FastAPI, Response, status
from fastapi import Request as FastAPIRequest
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict
from starlette.middleware.trustedhost import TrustedHostMiddleware

from .observation_scenarios import (
    CONTROLLED_SCENARIOS,
    BridgeEventCursor,
    BridgeEventPage,
    ControlledScenarioRun,
    PostgresScenarioEvidenceRepository,
    ReplayAttestationEvidence,
    ReplayAttestationReceipt,
    ScenarioCheckpointReceipt,
    ScenarioControlReceipt,
    ScenarioEvidenceError,
    ScenarioEvidenceRunner,
    ScenarioTrialEvidence,
    ScenarioTrialHandle,
    ScenarioTrialResult,
    SignedBridgeEvent,
    T4BridgeObservationClient,
)
from .postgres import ConnectionFactory

_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})
_LOOPBACK_CLIENTS = frozenset({"127.0.0.1", "::1", "testclient"})
_MAX_RESPONSE_BYTES = 65_536


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


class _StrictRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RegisterRequest(_StrictRequest):
    campaign_id: str


class SyncRequest(_StrictRequest):
    campaign_id: str
    operation: Literal["sync", "checkpoint"] = "sync"
    action_request_id: str | None = None


class BeginRequest(_StrictRequest):
    campaign_id: str
    scenario_code: str
    action_request_id: str
    scope_key: str
    preflight_complete: bool = False


class CompleteRequest(_StrictRequest):
    trial_id: str
    campaign_id: str
    scenario_code: str
    action_request_id: str
    scope_key: str
    timeout_seconds: float = 60.0
    poll_seconds: float = 0.5


class PassiveRequest(_StrictRequest):
    campaign_id: str
    scenario_code: str


class _DirectEvidenceRepository(Protocol):
    def register_bridge_evidence_key(self, public_key_spki_der: bytes) -> str: ...

    def store_bridge_event_page(
        self,
        *,
        campaign_id: str,
        page: BridgeEventPage,
    ) -> None: ...

    def begin_trial(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
        action_request_id: str | None,
        scope_key: str,
    ) -> ScenarioTrialHandle: ...

    def submit_trial_evidence(
        self,
        evidence: ScenarioTrialEvidence,
    ) -> ScenarioTrialResult: ...

    def verify_passive_trial(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
    ) -> ScenarioTrialResult: ...

    def record_replay_attestation(
        self,
        evidence: ReplayAttestationEvidence,
    ) -> ReplayAttestationReceipt: ...


@dataclass(frozen=True, slots=True)
class EvidenceRegistration:
    campaign_id: str
    evidence_key_fingerprint_sha256: str
    boot_id: str
    verified_event_count: int


@dataclass(frozen=True, slots=True)
class RemoteControlledScenarioRun:
    handle: ScenarioTrialHandle
    control_receipt: ScenarioControlReceipt
    evidence_already_complete: bool


@dataclass(frozen=True, slots=True)
class _VerifiedJournal:
    boot_id: str
    public_key_spki_der: bytes
    events: tuple[SignedBridgeEvent, ...]
    cursor: BridgeEventCursor | None


@dataclass(frozen=True, slots=True)
class ReplayCampaignPlan:
    campaign_id: str
    replay_as_of: datetime
    scopes: tuple[str, ...]


class PostgresReplayEvidenceReader:
    """Read frozen replay inputs through a separate, non-verifier DB login."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        if not callable(connection_factory):
            raise ScenarioEvidenceError(
                "Replay evidence reader database is invalid",
                code="T4_REPLAY_READER_INVALID",
            )
        self.connection_factory = connection_factory

    def campaign_plan(self, campaign_id: str) -> ReplayCampaignPlan:
        from .postgres import cursor, transaction

        campaign_id = _uuid(campaign_id, "campaign_id")
        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute("SET TRANSACTION READ ONLY")
                db_cursor.execute("SET LOCAL search_path TO pg_catalog")
                db_cursor.execute(
                    """
                    SELECT session_user = current_user,
                           role.rolcanlogin, role.rolinherit,
                           NOT role.rolsuper, NOT role.rolcreaterole,
                           NOT role.rolcreatedb, NOT role.rolreplication,
                           NOT role.rolbypassrls,
                           pg_has_role(current_user,
                               'crypto_agent_evidence_reader', 'MEMBER'),
                           NOT pg_has_role(current_user,
                               'crypto_agent_evidence_verifier', 'MEMBER'),
                           NOT pg_has_role(current_user,
                               'crypto_agent_evidence_owner', 'MEMBER'),
                           NOT EXISTS (
                               SELECT 1 FROM pg_catalog.pg_database database
                               WHERE database.datname = current_database()
                                 AND database.datdba = role.oid
                           ),
                           NOT EXISTS (
                               SELECT 1 FROM pg_catalog.pg_namespace namespace
                               WHERE namespace.nspname = 'crypto_agent'
                                 AND namespace.nspowner = role.oid
                           ),
                           NOT EXISTS (
                               SELECT 1
                               FROM pg_catalog.pg_class relation
                               JOIN pg_catalog.pg_namespace namespace
                                 ON namespace.oid = relation.relnamespace
                               WHERE namespace.nspname = 'crypto_agent'
                                 AND relation.relowner = role.oid
                           ),
                           NOT EXISTS (
                               SELECT 1
                               FROM pg_catalog.pg_proc routine
                               JOIN pg_catalog.pg_namespace namespace
                                 ON namespace.oid = routine.pronamespace
                               WHERE namespace.nspname = 'crypto_agent'
                                 AND routine.proowner = role.oid
                           ),
                           NOT EXISTS (
                               SELECT 1
                               FROM pg_catalog.pg_class relation
                               JOIN pg_catalog.pg_namespace namespace
                                 ON namespace.oid = relation.relnamespace
                               WHERE namespace.nspname = 'crypto_agent'
                                 AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                                 AND (
                                     has_table_privilege(
                                         current_user, relation.oid, 'INSERT'
                                     )
                                     OR has_table_privilege(
                                         current_user, relation.oid, 'UPDATE'
                                     )
                                     OR has_table_privilege(
                                         current_user, relation.oid, 'DELETE'
                                     )
                                     OR has_table_privilege(
                                         current_user, relation.oid, 'TRUNCATE'
                                     )
                                 )
                           )
                    FROM pg_catalog.pg_roles role
                    WHERE role.rolname = current_user
                    """
                )
                safety = db_cursor.fetchone()
                if (
                    not isinstance(safety, Sequence)
                    or isinstance(safety, (str, bytes, bytearray))
                    or len(safety) != 16
                    or any(value is not True for value in safety)
                ):
                    raise ScenarioEvidenceError(
                        "Replay evidence reader login is not isolated",
                        code="T4_REPLAY_READER_UNSAFE",
                    )
                db_cursor.execute(
                    """
                    SELECT planned_ends_at, cycle_interval_seconds,
                           scope_manifest, environment, read_only,
                           execution_enabled, clock_timestamp()
                    FROM crypto_agent.t4_observation_campaigns
                    WHERE campaign_id = %s
                    """,
                    (campaign_id,),
                )
                row = db_cursor.fetchone()
        except ScenarioEvidenceError:
            raise
        except Exception:
            raise ScenarioEvidenceError(
                "Replay evidence reader is unavailable",
                code="T4_REPLAY_READER_UNAVAILABLE",
            ) from None
        return _replay_campaign_plan(row, campaign_id=campaign_id)


class ReplayEvidenceAttestor:
    """Run deterministic replays in memory and persist verifier attestations."""

    def __init__(
        self,
        reader: PostgresReplayEvidenceReader,
        repository: _DirectEvidenceRepository,
    ) -> None:
        self._reader = reader
        self._repository = repository

    def attest_campaign(self, campaign_id: str) -> tuple[ReplayAttestationReceipt, ...]:
        from .monitoring import _eligible_for_delivery
        from .orchestrator import ResearchOrchestrator
        from .policy import RiskPolicy
        from .resource_paths import default_risk_policy_path
        from .t4_ingest import T4IngestRepository, T4ReplayProvider

        plan = self._reader.campaign_plan(campaign_id)
        provider = T4ReplayProvider(
            T4IngestRepository(self._reader.connection_factory)
        )
        orchestrator = ResearchOrchestrator(
            provider=provider,
            policy=RiskPolicy.load(default_risk_policy_path()),
        )
        receipts: list[ReplayAttestationReceipt] = []
        for scope_key in plan.scopes:
            symbol, interval_minutes = _parse_scope(scope_key)
            report = orchestrator.analyze(
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=plan.replay_as_of,
                limit=120,
            )
            metadata = report.metadata
            if (
                report.as_of != plan.replay_as_of
                or metadata.get("t4_replay") is not True
                or _parsed_utc(metadata.get("t4_replay_as_of"))
                != plan.replay_as_of
                or metadata.get("t4_bridge_schema_version") != 5
                or metadata.get("t4_environment") != "live_t4"
                or metadata.get("plus500_t4_source_attested") is not True
                or metadata.get("external_delivery_eligible") is not False
                or metadata.get("execution_enabled") is not False
                or metadata.get("provider_error_code") is not None
                or metadata.get("input_candle_count") != 120
                or _eligible_for_delivery(
                    report,
                    operation="analyze_replay",
                    now=datetime.now(UTC),
                )
            ):
                raise ScenarioEvidenceError(
                    "Replay analysis safety attestation failed",
                    code="T4_REPLAY_ATTESTATION_INVALID",
                )
            fingerprint = _sha256_value(
                metadata.get("t4_replay_fingerprint_sha256"),
                "replay_fingerprint_sha256",
            )
            source_batch_ids = _source_batch_ids(
                metadata.get("t4_replay_source_batch_ids")
            )
            source_batch_hashes = _source_batch_hashes(
                metadata.get("t4_replay_source_batch_hashes")
            )
            provenance = _sha256_value(
                metadata.get("t4_replay_provenance_sha256"),
                "provenance_sha256",
            )
            if provenance != _replay_provenance_hash(
                source_batch_ids,
                source_batch_hashes,
            ):
                raise ScenarioEvidenceError(
                    "Replay analysis provenance is invalid",
                    code="T4_REPLAY_ATTESTATION_INVALID",
                )
            evidence = ReplayAttestationEvidence(
                attestation_id=str(
                    uuid5(
                        UUID(plan.campaign_id),
                        f"t4-replay-verifier-attestation:{scope_key}",
                    )
                ),
                campaign_id=plan.campaign_id,
                scope_key=scope_key,
                replay_as_of=plan.replay_as_of,
                replay_fingerprint_sha256=fingerprint,
                source_batch_ids=source_batch_ids,
                source_batch_hashes=source_batch_hashes,
                provenance_sha256=provenance,
            )
            evidence.validate()
            receipts.append(self._repository.record_replay_attestation(evidence))
        return tuple(receipts)


class EvidenceVerifierService:
    """Own the verifier credentials and persist only self-fetched signed evidence."""

    def __init__(
        self,
        bridge_client: T4BridgeObservationClient,
        repository: _DirectEvidenceRepository,
        replay_attestor: ReplayEvidenceAttestor | None = None,
    ) -> None:
        self._bridge_client = bridge_client
        self._repository = repository
        self._replay_attestor = replay_attestor
        self._lock = threading.RLock()

    def register_campaign(self, campaign_id: str) -> EvidenceRegistration:
        campaign_id = _uuid(campaign_id, "campaign_id")
        with self._lock:
            journal = self._fetch_journal(campaign_id=campaign_id, store=False)
            if (
                not journal.events
                or journal.events[0].sequence_no != 1
                or any(event.campaign_id != campaign_id for event in journal.events)
                or not any(
                    event.event_type == "bridge_started"
                    and event.boot_id == journal.boot_id
                    for event in journal.events
                )
            ):
                raise ScenarioEvidenceError(
                    "Bridge evidence journal is not bound to the campaign",
                    code="T4_EVIDENCE_CAMPAIGN_MISMATCH",
                )
            fingerprint = self._repository.register_bridge_evidence_key(
                journal.public_key_spki_der
            )
            expected = self._bridge_client.verifier.evidence_key_fingerprint_sha256
            if fingerprint != expected:
                raise ScenarioEvidenceError(
                    "Registered bridge evidence key does not match the pin",
                    code="T4_EVIDENCE_KEY_MISMATCH",
                )
            return EvidenceRegistration(
                campaign_id=campaign_id,
                evidence_key_fingerprint_sha256=fingerprint,
                boot_id=journal.boot_id,
                verified_event_count=len(journal.events),
            )

    def sync_bridge_events(self, campaign_id: str) -> int:
        campaign_id = _uuid(campaign_id, "campaign_id")
        with self._lock:
            return len(self._fetch_journal(campaign_id=campaign_id, store=True).events)

    def record_campaign_checkpoint(
        self,
        campaign_id: str,
        *,
        action_request_id: str | None = None,
    ) -> ScenarioCheckpointReceipt:
        campaign_id = _uuid(campaign_id, "campaign_id")
        with self._lock:
            self._register_current_key()
            return ScenarioEvidenceRunner(
                self._bridge_client,
                self._repository,
            ).record_campaign_checkpoint(
                campaign_id=campaign_id,
                action_request_id=action_request_id,
            )

    def begin_controlled(self, request: BeginRequest) -> RemoteControlledScenarioRun:
        campaign_id = _uuid(request.campaign_id, "campaign_id")
        action_request_id = _uuid(request.action_request_id, "action_request_id")

        def require_preflight() -> None:
            if not request.preflight_complete:
                raise ScenarioEvidenceError(
                    "Controlled scenario requires a runtime preflight",
                    code="T4_SCENARIO_PREFLIGHT_REQUIRED",
                )

        with self._lock:
            self._register_current_key()
            run = ScenarioEvidenceRunner(
                self._bridge_client,
                self._repository,
            ).start_controlled(
                campaign_id=campaign_id,
                scenario_code=request.scenario_code,
                action_request_id=action_request_id,
                scope_key=request.scope_key,
                before_trigger=require_preflight,
            )
        return RemoteControlledScenarioRun(
            handle=run.handle,
            control_receipt=run.control_receipt,
            evidence_already_complete=run.evidence_already_complete,
        )

    def complete_controlled(self, request: CompleteRequest) -> ScenarioTrialResult:
        campaign_id = _uuid(request.campaign_id, "campaign_id")
        action_request_id = _uuid(request.action_request_id, "action_request_id")
        action = CONTROLLED_SCENARIOS.get(request.scenario_code)
        if action is None:
            raise ScenarioEvidenceError(
                "Scenario is not an approved controlled scenario",
                code="T4_SCENARIO_NOT_CONTROLLED",
            )
        _timing(request.timeout_seconds, request.poll_seconds)
        with self._lock:
            # Complete is a resume-only operation.  First fetch and verify the
            # signed journal without touching PostgreSQL.  An unknown request
            # must never open a trial, persist a page or arm bridge control.
            journal = self._fetch_journal(campaign_id=campaign_id, store=False)
            self._control_receipt_from_journal(
                journal,
                campaign_id=campaign_id,
                scenario_code=request.scenario_code,
                action_request_id=action_request_id,
                scope_key=request.scope_key,
                action=action,
            )

            # A matching signed control_applied receipt makes an idempotent DB
            # resume safe.  Re-fetch before persistence so every stored page is
            # independently verified in this service invocation.
            journal = self._fetch_journal(campaign_id=campaign_id, store=True)
            receipt = self._control_receipt_from_journal(
                journal,
                campaign_id=campaign_id,
                scenario_code=request.scenario_code,
                action_request_id=action_request_id,
                scope_key=request.scope_key,
                action=action,
            )
            handle = self._repository.begin_trial(
                campaign_id=campaign_id,
                scenario_code=request.scenario_code,
                action_request_id=action_request_id,
                scope_key=request.scope_key,
            )
            if (
                not isinstance(handle, ScenarioTrialHandle)
                or handle.trial_id != _uuid(request.trial_id, "trial_id")
                or handle.campaign_id != campaign_id
                or handle.scenario_code != request.scenario_code
                or handle.action_request_id != action_request_id
                or handle.scope_key != request.scope_key
            ):
                raise ScenarioEvidenceError(
                    "Controlled scenario handle does not match",
                    code="T4_SCENARIO_HANDLE_MISMATCH",
                )
            _utc(handle.started_at, "started_at")
            runner = ScenarioEvidenceRunner(
                self._bridge_client,
                self._repository,
            )
            run = ControlledScenarioRun(
                handle=handle,
                action=action,
                control_receipt=receipt,
                baseline_cursor=journal.cursor,
                initial_events=journal.events,
                evidence_already_complete=False,
            )
            return runner.collect_controlled(
                run,
                timeout_seconds=float(request.timeout_seconds),
                poll_seconds=float(request.poll_seconds),
            )

    def verify_passive(self, request: PassiveRequest) -> ScenarioTrialResult:
        campaign_id = _uuid(request.campaign_id, "campaign_id")
        with self._lock:
            if request.scenario_code == "replay_blocked":
                if self._replay_attestor is None:
                    raise ScenarioEvidenceError(
                        "Replay evidence attestor is not configured",
                        code="T4_REPLAY_READER_UNAVAILABLE",
                    )
                self._replay_attestor.attest_campaign(campaign_id)
            else:
                self._fetch_journal(campaign_id=campaign_id, store=True)
            return ScenarioEvidenceRunner(
                self._bridge_client,
                self._repository,
            ).verify_passive(
                campaign_id=campaign_id,
                scenario_code=request.scenario_code,
            )

    def _register_current_key(self) -> str:
        page = self._bridge_client.list_events(limit=1)
        fingerprint = self._repository.register_bridge_evidence_key(
            page.public_key_spki_der
        )
        if fingerprint != page.evidence_key_fingerprint_sha256:
            raise ScenarioEvidenceError(
                "Registered bridge evidence key does not match the verified page",
                code="T4_EVIDENCE_KEY_MISMATCH",
            )
        return fingerprint

    @staticmethod
    def _control_receipt_from_journal(
        journal: _VerifiedJournal,
        *,
        campaign_id: str,
        scenario_code: str,
        action_request_id: str,
        scope_key: str,
        action: str,
    ) -> ScenarioControlReceipt:
        matches = tuple(
            event
            for event in journal.events
            if event.event_type == "control_applied"
            and event.campaign_id == campaign_id
            and event.scenario_code == scenario_code
            and event.action_request_id == action_request_id
            and event.scope_key == scope_key
            and event.payload.action == action
            and event.payload.control_step == "applied"
        )
        if not matches:
            raise ScenarioEvidenceError(
                "Controlled scenario receipt is absent from the signed journal",
                code="T4_SCENARIO_RECEIPT_MISSING",
            )
        if len(matches) != 1:
            raise ScenarioEvidenceError(
                "Controlled scenario receipt identifier was reused",
                code="T4_SCENARIO_RECEIPT_COLLISION",
            )
        event = matches[0]
        return ScenarioControlReceipt(
            action=action,
            campaign_id=campaign_id,
            action_request_id=action_request_id,
            scope_key=scope_key,
            event_sequence_no=event.sequence_no,
            event_hash_sha256=event.event_hash_sha256,
        )

    def _fetch_journal(self, *, campaign_id: str, store: bool) -> _VerifiedJournal:
        cursor: BridgeEventCursor | None = None
        events: list[SignedBridgeEvent] = []
        public_key_spki_der: bytes | None = None
        boot_id: str | None = None
        page_limit = 512
        maximum_pages = 100_000 // page_limit + 2
        for _ in range(maximum_pages):
            # T4BridgeObservationClient performs the ECDSA, canonical projection,
            # key-pin and chain verification before this method sees the page.
            page = self._bridge_client.list_events(cursor=cursor, limit=page_limit)
            if public_key_spki_der is None:
                public_key_spki_der = page.public_key_spki_der
                if store:
                    fingerprint = self._repository.register_bridge_evidence_key(
                        public_key_spki_der
                    )
                    if fingerprint != page.evidence_key_fingerprint_sha256:
                        raise ScenarioEvidenceError(
                            "Registered bridge evidence key does not match the verified page",
                            code="T4_EVIDENCE_KEY_MISMATCH",
                        )
            elif page.public_key_spki_der != public_key_spki_der:
                raise ScenarioEvidenceError(
                    "Bridge evidence key changed within the journal",
                    code="T4_EVIDENCE_KEY_MISMATCH",
                )
            boot_id = page.boot_id
            if page.events:
                if any(event.campaign_id not in {None, campaign_id} for event in page.events):
                    raise ScenarioEvidenceError(
                        "Bridge event belongs to another campaign",
                        code="T4_EVIDENCE_CAMPAIGN_MISMATCH",
                    )
                if store:
                    self._repository.store_bridge_event_page(
                        campaign_id=campaign_id,
                        page=page,
                    )
                events.extend(page.events)
                cursor = page.cursor
            if len(page.events) < page_limit:
                if public_key_spki_der is None or boot_id is None:
                    break
                return _VerifiedJournal(
                    boot_id=boot_id,
                    public_key_spki_der=public_key_spki_der,
                    events=tuple(events),
                    cursor=cursor,
                )
        raise ScenarioEvidenceError(
            "Bridge event journal exceeds the bounded evidence window",
            code="T4_EVIDENCE_JOURNAL_LIMIT_EXCEEDED",
        )


class EvidenceVerifierClient:
    """Narrow loopback client; it never accepts signed pages or a PASS outcome."""

    def __init__(
        self,
        *,
        verifier_url: str,
        verifier_token: str,
        timeout_seconds: float = 10.0,
    ) -> None:
        parsed = urlsplit(verifier_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in _LOOPBACK_HOSTS
            or parsed.port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ScenarioEvidenceError(
                "Evidence verifier must be an explicit loopback HTTP origin",
                code="T4_EVIDENCE_VERIFIER_ORIGIN_INVALID",
            )
        self._verifier_url = verifier_url.rstrip("/")
        self._verifier_token = _token(verifier_token, "verifier_token")
        if not _finite(timeout_seconds, 1.0, 30.0):
            raise ScenarioEvidenceError(
                "Evidence verifier timeout is invalid",
                code="T4_EVIDENCE_VERIFIER_TIMEOUT_INVALID",
            )
        self._timeout_seconds = float(timeout_seconds)

    @classmethod
    def from_env(cls) -> EvidenceVerifierClient:
        timeout_raw = os.getenv("CRYPTO_AGENT_EVIDENCE_VERIFIER_TIMEOUT_SECONDS", "10")
        try:
            timeout = float(timeout_raw)
        except ValueError:
            raise ScenarioEvidenceError(
                "Evidence verifier timeout is invalid",
                code="T4_EVIDENCE_VERIFIER_TIMEOUT_INVALID",
            ) from None
        return cls(
            verifier_url=os.getenv(
                "CRYPTO_AGENT_EVIDENCE_VERIFIER_URL",
                "http://127.0.0.1:8791",
            ),
            verifier_token=os.getenv("CRYPTO_AGENT_EVIDENCE_VERIFIER_TOKEN", ""),
            timeout_seconds=timeout,
        )

    def register_campaign(self, campaign_id: str) -> EvidenceRegistration:
        campaign_id = _uuid(campaign_id, "campaign_id")
        payload = self._post("register", {"campaign_id": campaign_id})
        _exact_keys(
            payload,
            {
                "schema_version",
                "status",
                "campaign_id",
                "evidence_key_fingerprint_sha256",
                "boot_id",
                "verified_event_count",
                "execution_enabled",
            },
        )
        fingerprint = _sha256_text(payload.get("evidence_key_fingerprint_sha256"))
        result = EvidenceRegistration(
            campaign_id=_uuid(payload.get("campaign_id"), "campaign_id"),
            evidence_key_fingerprint_sha256=fingerprint,
            boot_id=_uuid(payload.get("boot_id"), "boot_id"),
            verified_event_count=_positive_int(payload.get("verified_event_count")),
        )
        if (
            payload.get("schema_version") != 1
            or payload.get("status") != "registered"
            or payload.get("execution_enabled") is not False
            or result.campaign_id != campaign_id
        ):
            raise _invalid_response()
        return result

    def sync_bridge_events(self, *, campaign_id: str) -> int:
        campaign_id = _uuid(campaign_id, "campaign_id")
        payload = self._post(
            "sync",
            {"campaign_id": campaign_id, "operation": "sync"},
        )
        _exact_keys(
            payload,
            {"schema_version", "status", "campaign_id", "stored", "execution_enabled"},
        )
        stored = _nonnegative_int(payload.get("stored"))
        if (
            payload.get("schema_version") != 1
            or payload.get("status") != "synced"
            or payload.get("campaign_id") != campaign_id
            or payload.get("execution_enabled") is not False
        ):
            raise _invalid_response()
        return stored

    def record_campaign_checkpoint(
        self,
        *,
        campaign_id: str,
        action_request_id: str | None = None,
    ) -> ScenarioCheckpointReceipt:
        campaign_id = _uuid(campaign_id, "campaign_id")
        if action_request_id is not None:
            action_request_id = _uuid(action_request_id, "action_request_id")
        payload = self._post(
            "sync",
            {
                "campaign_id": campaign_id,
                "operation": "checkpoint",
                "action_request_id": action_request_id,
            },
        )
        _exact_keys(
            payload,
            {
                "schema_version",
                "status",
                "campaign_id",
                "action_request_id",
                "event_sequence_no",
                "event_hash_sha256",
                "execution_enabled",
            },
        )
        receipt = ScenarioCheckpointReceipt(
            campaign_id=_uuid(payload.get("campaign_id"), "campaign_id"),
            action_request_id=_uuid(
                payload.get("action_request_id"), "action_request_id"
            ),
            event_sequence_no=_positive_int(payload.get("event_sequence_no")),
            event_hash_sha256=_sha256_text(payload.get("event_hash_sha256")),
        )
        if (
            payload.get("schema_version") != 1
            or payload.get("status") != "checkpointed"
            or payload.get("execution_enabled") is not False
            or receipt.campaign_id != campaign_id
            or (
                action_request_id is not None
                and receipt.action_request_id != action_request_id
            )
        ):
            raise _invalid_response()
        return receipt

    def run_controlled(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
        scope_key: str,
        action_request_id: str | None = None,
        before_trigger: Callable[[], object] | None = None,
        exercise: Callable[[RemoteControlledScenarioRun], object] | None = None,
        recovery: Callable[[RemoteControlledScenarioRun], object] | None = None,
        timeout_seconds: float = 60.0,
        poll_seconds: float = 0.5,
    ) -> ScenarioTrialResult:
        campaign_id = _uuid(campaign_id, "campaign_id")
        request_id = _uuid(
            action_request_id
            or str(
                uuid5(
                    UUID(campaign_id),
                    f"t4-observation-scenario:{scenario_code}:{scope_key}",
                )
            ),
            "action_request_id",
        )
        preflight_complete = before_trigger is None
        try:
            run = self._begin(
                campaign_id=campaign_id,
                scenario_code=scenario_code,
                action_request_id=request_id,
                scope_key=scope_key,
                preflight_complete=preflight_complete,
            )
        except ScenarioEvidenceError as exc:
            if exc.code != "T4_SCENARIO_PREFLIGHT_REQUIRED" or before_trigger is None:
                raise
            before_trigger()
            run = self._begin(
                campaign_id=campaign_id,
                scenario_code=scenario_code,
                action_request_id=request_id,
                scope_key=scope_key,
                preflight_complete=True,
            )
        if exercise is not None and not run.evidence_already_complete:
            exercise(run)
        if recovery is not None:
            recovery(run)
        return self._complete(
            run,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )

    def verify_passive_trial(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
    ) -> ScenarioTrialResult:
        campaign_id = _uuid(campaign_id, "campaign_id")
        payload = self._post(
            "passive",
            {"campaign_id": campaign_id, "scenario_code": scenario_code},
        )
        return _result_from_response(
            payload,
            campaign_id=campaign_id,
            scenario_code=scenario_code,
        )

    def _begin(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
        action_request_id: str,
        scope_key: str,
        preflight_complete: bool,
    ) -> RemoteControlledScenarioRun:
        payload = self._post(
            "begin",
            {
                "campaign_id": campaign_id,
                "scenario_code": scenario_code,
                "action_request_id": action_request_id,
                "scope_key": scope_key,
                "preflight_complete": preflight_complete,
            },
        )
        _exact_keys(
            payload,
            {
                "schema_version",
                "status",
                "trial_id",
                "campaign_id",
                "scenario_code",
                "action_request_id",
                "scope_key",
                "started_at",
                "control_receipt_sequence_no",
                "control_receipt_event_hash",
                "evidence_already_complete",
                "execution_enabled",
            },
        )
        handle = ScenarioTrialHandle(
            trial_id=_uuid(payload.get("trial_id"), "trial_id"),
            campaign_id=_uuid(payload.get("campaign_id"), "campaign_id"),
            scenario_code=_text(payload.get("scenario_code")),
            action_request_id=_uuid(
                payload.get("action_request_id"), "action_request_id"
            ),
            scope_key=_text(payload.get("scope_key")),
            started_at=_utc(payload.get("started_at"), "started_at"),
        )
        action = CONTROLLED_SCENARIOS.get(scenario_code)
        if action is None:
            raise _invalid_response()
        receipt = ScenarioControlReceipt(
            action=action,
            campaign_id=handle.campaign_id,
            action_request_id=cast(str, handle.action_request_id),
            scope_key=handle.scope_key,
            event_sequence_no=_positive_int(
                payload.get("control_receipt_sequence_no")
            ),
            event_hash_sha256=_sha256_text(
                payload.get("control_receipt_event_hash")
            ),
        )
        already_complete = payload.get("evidence_already_complete")
        if (
            payload.get("schema_version") != 1
            or payload.get("status") != "started"
            or payload.get("execution_enabled") is not False
            or type(already_complete) is not bool
            or handle.campaign_id != campaign_id
            or handle.scenario_code != scenario_code
            or handle.action_request_id != action_request_id
            or handle.scope_key != scope_key
        ):
            raise _invalid_response()
        return RemoteControlledScenarioRun(
            handle=handle,
            control_receipt=receipt,
            evidence_already_complete=already_complete,
        )

    def _complete(
        self,
        run: RemoteControlledScenarioRun,
        *,
        timeout_seconds: float,
        poll_seconds: float,
    ) -> ScenarioTrialResult:
        _timing(timeout_seconds, poll_seconds)
        handle = run.handle
        payload = self._post(
            "complete",
            {
                "trial_id": handle.trial_id,
                "campaign_id": handle.campaign_id,
                "scenario_code": handle.scenario_code,
                "action_request_id": handle.action_request_id,
                "scope_key": handle.scope_key,
                "timeout_seconds": float(timeout_seconds),
                "poll_seconds": float(poll_seconds),
            },
            timeout_seconds=float(timeout_seconds) + self._timeout_seconds,
        )
        result = _result_from_response(
            payload,
            campaign_id=handle.campaign_id,
            scenario_code=handle.scenario_code,
        )
        if result.trial_id != handle.trial_id:
            raise _invalid_response()
        return result

    def _post(
        self,
        operation: str,
        payload: Mapping[str, object],
        *,
        timeout_seconds: float | None = None,
    ) -> Mapping[str, object]:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        request = Request(
            f"{self._verifier_url}/v1/evidence/{operation}",
            data=body,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Crypto-Agent-Evidence-Token": self._verifier_token,
            },
            method="POST",
        )
        return _request_json(
            request,
            timeout_seconds=(
                self._timeout_seconds
                if timeout_seconds is None
                else timeout_seconds
            ),
        )


def create_evidence_verifier_app(
    service: EvidenceVerifierService,
    *,
    api_token: str,
) -> FastAPI:
    expected_token = _token(api_token, "verifier_token")
    app = FastAPI(
        title="Crypto Agent T4 Evidence Verifier",
        version="1",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["127.0.0.1", "localhost", "[::1]", "testserver"],
    )

    @app.middleware("http")
    async def authorize_loopback(
        request: FastAPIRequest,
        call_next: Callable[[FastAPIRequest], Awaitable[Response]],
    ) -> Response:
        rejection = _authorization_rejection(request, expected_token)
        if rejection is not None:
            return rejection
        response = await call_next(request)
        for name, value in _security_headers().items():
            response.headers[name] = value
        return response

    @app.exception_handler(ScenarioEvidenceError)
    async def scenario_error(
        _request: FastAPIRequest,
        exc: ScenarioEvidenceError,
    ) -> JSONResponse:
        status_code = (
            status.HTTP_409_CONFLICT
            if exc.code == "T4_SCENARIO_PREFLIGHT_REQUIRED"
            else status.HTTP_503_SERVICE_UNAVAILABLE
            if exc.code
            in {
                "T4_OBSERVATION_BRIDGE_UNAVAILABLE",
                "T4_SCENARIO_DATABASE_UNAVAILABLE",
            }
            else status.HTTP_422_UNPROCESSABLE_CONTENT
        )
        return JSONResponse(
            status_code=status_code,
            content={"detail": exc.code},
            headers=_security_headers(),
        )

    @app.post("/v1/evidence/register")
    def register(request: RegisterRequest) -> dict[str, object]:
        result = service.register_campaign(request.campaign_id)
        return {
            "schema_version": 1,
            "status": "registered",
            "campaign_id": result.campaign_id,
            "evidence_key_fingerprint_sha256": (
                result.evidence_key_fingerprint_sha256
            ),
            "boot_id": result.boot_id,
            "verified_event_count": result.verified_event_count,
            "execution_enabled": False,
        }

    @app.post("/v1/evidence/sync")
    def sync(request: SyncRequest) -> dict[str, object]:
        if request.operation == "sync":
            if request.action_request_id is not None:
                raise ScenarioEvidenceError(
                    "Sync request contains an invalid action identifier",
                    code="T4_EVIDENCE_FIELDS_INVALID",
                )
            return {
                "schema_version": 1,
                "status": "synced",
                "campaign_id": request.campaign_id,
                "stored": service.sync_bridge_events(request.campaign_id),
                "execution_enabled": False,
            }
        receipt = service.record_campaign_checkpoint(
            request.campaign_id,
            action_request_id=request.action_request_id,
        )
        return {
            "schema_version": 1,
            "status": "checkpointed",
            "campaign_id": receipt.campaign_id,
            "action_request_id": receipt.action_request_id,
            "event_sequence_no": receipt.event_sequence_no,
            "event_hash_sha256": receipt.event_hash_sha256,
            "execution_enabled": False,
        }

    @app.post("/v1/evidence/begin")
    def begin(request: BeginRequest) -> dict[str, object]:
        run = service.begin_controlled(request)
        handle = run.handle
        receipt = run.control_receipt
        return {
            "schema_version": 1,
            "status": "started",
            "trial_id": handle.trial_id,
            "campaign_id": handle.campaign_id,
            "scenario_code": handle.scenario_code,
            "action_request_id": handle.action_request_id,
            "scope_key": handle.scope_key,
            "started_at": _isoformat(handle.started_at),
            "control_receipt_sequence_no": receipt.event_sequence_no,
            "control_receipt_event_hash": receipt.event_hash_sha256,
            "evidence_already_complete": run.evidence_already_complete,
            "execution_enabled": False,
        }

    @app.post("/v1/evidence/complete")
    def complete(request: CompleteRequest) -> dict[str, object]:
        return _result_payload(service.complete_controlled(request))

    @app.post("/v1/evidence/passive")
    def passive(request: PassiveRequest) -> dict[str, object]:
        return _result_payload(service.verify_passive(request))

    return app


def build_evidence_verifier_service() -> tuple[EvidenceVerifierService, str]:
    """Read privileged verifier settings only inside the verifier process."""

    from .postgres import PostgresSettings, PostgresUnavailableError, PsycopgConnectionFactory

    evidence_dsn = os.getenv("CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN", "")
    if not evidence_dsn:
        raise PostgresUnavailableError(
            "Scenario verifier PostgreSQL is not configured"
        )
    evidence_read_dsn = os.getenv(
        "CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN", ""
    )
    if not evidence_read_dsn:
        raise PostgresUnavailableError(
            "Replay evidence reader PostgreSQL is not configured"
        )
    runtime_dsn = os.getenv("CRYPTO_AGENT_POSTGRES_DSN", "")
    if runtime_dsn:
        raise PostgresUnavailableError(
            "Runtime PostgreSQL must not be configured in the verifier process"
        )
    if hmac.compare_digest(evidence_dsn.strip(), evidence_read_dsn.strip()):
        raise PostgresUnavailableError(
            "Replay reader PostgreSQL login must be separate from verifier"
        )
    timeout_raw = os.getenv("CRYPTO_AGENT_POSTGRES_CONNECT_TIMEOUT", "5")
    try:
        connect_timeout = int(timeout_raw)
    except ValueError:
        raise PostgresUnavailableError(
            "PostgreSQL connect timeout configuration is invalid"
        ) from None
    api_token = _token(
        os.getenv("CRYPTO_AGENT_EVIDENCE_VERIFIER_TOKEN", ""),
        "verifier_token",
    )
    bridge_token = os.getenv("CRYPTO_AGENT_T4_BRIDGE_TOKEN", "")
    control_token = os.getenv("CRYPTO_AGENT_T4_OBSERVATION_CONTROL_TOKEN") or None
    if hmac.compare_digest(api_token, bridge_token) or (
        control_token is not None and hmac.compare_digest(api_token, control_token)
    ):
        raise ScenarioEvidenceError(
            "Evidence verifier API token must be separately scoped",
            code="T4_EVIDENCE_VERIFIER_TOKEN_REUSED",
        )
    bridge = T4BridgeObservationClient(
        bridge_url=os.getenv(
            "CRYPTO_AGENT_T4_BRIDGE_URL", "http://127.0.0.1:8784"
        ),
        bridge_token=bridge_token,
        observation_control_token=control_token,
        evidence_key_fingerprint_sha256=os.getenv(
            "CRYPTO_AGENT_T4_EVIDENCE_KEY_FINGERPRINT", ""
        ),
        evidence_public_key_spki_base64=os.getenv(
            "CRYPTO_AGENT_T4_EVIDENCE_PUBLIC_KEY_SPKI_BASE64"
        ),
        timeout_seconds=float(os.getenv("CRYPTO_AGENT_T4_TIMEOUT_SECONDS", "10")),
    )
    repository = PostgresScenarioEvidenceRepository(
        PsycopgConnectionFactory(
            PostgresSettings(
                dsn=evidence_dsn,
                connect_timeout_seconds=connect_timeout,
            )
        )
    )
    reader = PostgresReplayEvidenceReader(
        PsycopgConnectionFactory(
            PostgresSettings(
                dsn=evidence_read_dsn,
                connect_timeout_seconds=connect_timeout,
            )
        )
    )
    return (
        EvidenceVerifierService(
            bridge,
            repository,
            ReplayEvidenceAttestor(reader, repository),
        ),
        api_token,
    )


def run_evidence_verifier_service(*, host: str, port: int) -> None:
    if host not in {"127.0.0.1", "::1"} or type(port) is not int or not 1024 <= port <= 65535:
        raise ValueError("Evidence verifier bind address is invalid")
    service, api_token = build_evidence_verifier_service()
    import uvicorn

    uvicorn.run(
        create_evidence_verifier_app(service, api_token=api_token),
        host=host,
        port=port,
        access_log=False,
        server_header=False,
    )


def _replay_campaign_plan(
    row: object,
    *,
    campaign_id: str,
) -> ReplayCampaignPlan:
    if (
        not isinstance(row, Sequence)
        or isinstance(row, (str, bytes, bytearray))
        or len(row) != 7
    ):
        raise ScenarioEvidenceError(
            "Replay campaign is unavailable",
            code="T4_REPLAY_CAMPAIGN_INVALID",
        )
    replay_as_of = _utc(row[0], "planned_ends_at")
    interval = row[1]
    raw_scopes = row[2]
    if isinstance(raw_scopes, str):
        try:
            raw_scopes = json.loads(raw_scopes)
        except json.JSONDecodeError:
            raw_scopes = None
    if (
        type(interval) is not int
        or not 1 <= interval <= 86_400
        or not isinstance(raw_scopes, list)
        or not raw_scopes
        or any(not isinstance(scope, str) for scope in raw_scopes)
        or tuple(sorted(set(raw_scopes))) != tuple(raw_scopes)
        or row[3] != "live_t4"
        or row[4] is not True
        or row[5] is not False
    ):
        raise ScenarioEvidenceError(
            "Replay campaign is invalid",
            code="T4_REPLAY_CAMPAIGN_INVALID",
        )
    for scope in raw_scopes:
        _parse_scope(scope)
    scopes = tuple(raw_scopes)
    database_now = _utc(row[6], "database_now")
    if database_now < replay_as_of:
        raise ScenarioEvidenceError(
            "Replay attestation is before the frozen campaign cutoff",
            code="T4_REPLAY_ATTESTATION_WINDOW_INVALID",
        )
    return ReplayCampaignPlan(
        campaign_id=campaign_id,
        replay_as_of=replay_as_of,
        scopes=scopes,
    )


def _parse_scope(scope_key: str) -> tuple[str, int]:
    match = re.fullmatch(r"((?:BTC|ETH)/USD):(240|1440|10080)m", scope_key)
    if match is None:
        raise ScenarioEvidenceError(
            "Replay campaign scope is invalid",
            code="T4_REPLAY_CAMPAIGN_INVALID",
        )
    return match.group(1), int(match.group(2))


def _parsed_utc(value: object) -> datetime | None:
    try:
        return _utc(value, "replay_as_of")
    except ScenarioEvidenceError:
        return None


def _sha256_value(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ScenarioEvidenceError(
            f"Replay {field} is invalid",
            code="T4_REPLAY_ATTESTATION_INVALID",
        )
    return value


def _source_batch_ids(value: object) -> tuple[int, ...]:
    if (
        not isinstance(value, (list, tuple))
        or len(value) != 1
        or type(value[0]) is not int
        or value[0] <= 0
    ):
        raise ScenarioEvidenceError(
            "Replay source batch identifiers are invalid",
            code="T4_REPLAY_ATTESTATION_INVALID",
        )
    return (value[0],)


def _source_batch_hashes(value: object) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or len(value) != 1:
        raise ScenarioEvidenceError(
            "Replay source batch hashes are invalid",
            code="T4_REPLAY_ATTESTATION_INVALID",
        )
    return (_sha256_value(value[0], "source_batch_hash"),)


def _replay_provenance_hash(
    source_batch_ids: tuple[int, ...],
    source_batch_hashes: tuple[str, ...],
) -> str:
    framed = "t4-replay-provenance-v1\n" + "".join(
        f"{batch_id}:{batch_hash}\n"
        for batch_id, batch_hash in zip(
            source_batch_ids,
            source_batch_hashes,
            strict=True,
        )
    )
    return hashlib.sha256(framed.encode("ascii")).hexdigest()


def _authorization_rejection(
    request: FastAPIRequest,
    expected_token: str,
) -> JSONResponse | None:
    client_host = request.client.host if request.client is not None else ""
    if client_host not in _LOOPBACK_CLIENTS:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": "LOOPBACK_REQUIRED"},
            headers=_security_headers(),
        )
    supplied = request.headers.get("X-Crypto-Agent-Evidence-Token", "")
    if not supplied or not hmac.compare_digest(supplied, expected_token):
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": "EVIDENCE_TOKEN_REQUIRED"},
            headers=_security_headers(),
        )
    return None


def _security_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store",
        "Content-Security-Policy": "default-src 'none'; frame-ancestors 'none'",
        "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
    }


def _request_json(request: Request, *, timeout_seconds: float) -> Mapping[str, object]:
    try:
        response = build_opener(ProxyHandler({}), _NoRedirectHandler()).open(
            request,
            timeout=timeout_seconds,
        )
        try:
            status_code = getattr(response, "status", None)
            content_type = _content_type(getattr(response, "headers", None))
            body = response.read(_MAX_RESPONSE_BYTES + 1)
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()
    except HTTPError as exc:
        try:
            body = exc.read(_MAX_RESPONSE_BYTES + 1)
        except Exception:
            body = b""
        error_code = _error_code(body)
        raise ScenarioEvidenceError(
            "Evidence verifier rejected the request",
            code=error_code or "T4_EVIDENCE_VERIFIER_REJECTED",
        ) from None
    except Exception:
        raise ScenarioEvidenceError(
            "Evidence verifier request failed safely",
            code="T4_EVIDENCE_VERIFIER_UNAVAILABLE",
        ) from None
    if (
        status_code != 200
        or content_type != "application/json"
        or not isinstance(body, bytes)
        or not body
        or len(body) > _MAX_RESPONSE_BYTES
    ):
        raise _invalid_response()
    try:
        decoded = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _invalid_response() from None
    if not isinstance(decoded, dict) or not all(isinstance(key, str) for key in decoded):
        raise _invalid_response()
    return cast(Mapping[str, object], decoded)


def _content_type(headers: object) -> str:
    getter = getattr(headers, "get_content_type", None)
    return str(getter()) if callable(getter) else ""


def _error_code(body: bytes) -> str | None:
    if not body or len(body) > _MAX_RESPONSE_BYTES:
        return None
    try:
        value = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    detail = value.get("detail") if isinstance(value, dict) else None
    return detail if isinstance(detail, str) and _SAFE_CODE.fullmatch(detail) else None


def _result_payload(result: ScenarioTrialResult) -> dict[str, object]:
    result.validate()
    return {
        "schema_version": 1,
        "status": "verified",
        "trial_id": result.trial_id,
        "campaign_id": result.campaign_id,
        "scenario_code": result.scenario_code,
        "outcome": result.outcome,
        "completed_at": _isoformat(result.completed_at),
        "content_hash_sha256": result.content_hash_sha256,
        "execution_enabled": False,
    }


def _result_from_response(
    payload: Mapping[str, object],
    *,
    campaign_id: str,
    scenario_code: str,
) -> ScenarioTrialResult:
    _exact_keys(
        payload,
        {
            "schema_version",
            "status",
            "trial_id",
            "campaign_id",
            "scenario_code",
            "outcome",
            "completed_at",
            "content_hash_sha256",
            "execution_enabled",
        },
    )
    result = ScenarioTrialResult(
        trial_id=_uuid(payload.get("trial_id"), "trial_id"),
        campaign_id=_uuid(payload.get("campaign_id"), "campaign_id"),
        scenario_code=_text(payload.get("scenario_code")),
        outcome=_text(payload.get("outcome")),
        completed_at=_utc(payload.get("completed_at"), "completed_at"),
        content_hash_sha256=_sha256_text(payload.get("content_hash_sha256")),
    )
    result.validate()
    if (
        payload.get("schema_version") != 1
        or payload.get("status") != "verified"
        or payload.get("execution_enabled") is not False
        or result.campaign_id != campaign_id
        or result.scenario_code != scenario_code
    ):
        raise _invalid_response()
    return result


def _exact_keys(payload: Mapping[str, object], expected: set[str]) -> None:
    if set(payload) != expected:
        raise _invalid_response()


def _uuid(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ScenarioEvidenceError(
            f"{field} is invalid", code="T4_EVIDENCE_ID_INVALID"
        )
    try:
        parsed = UUID(value)
    except ValueError:
        raise ScenarioEvidenceError(
            f"{field} is invalid", code="T4_EVIDENCE_ID_INVALID"
        ) from None
    if str(parsed) != value:
        raise ScenarioEvidenceError(
            f"{field} is invalid", code="T4_EVIDENCE_ID_INVALID"
        )
    return value


def _utc(value: object, field: str) -> datetime:
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
    elif isinstance(value, datetime):
        parsed = value
    else:
        parsed = None
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ScenarioEvidenceError(
            f"{field} is invalid", code="T4_EVIDENCE_TIME_INVALID"
        )
    return parsed.astimezone(UTC)


def _isoformat(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _token(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not 32 <= len(value) <= 256
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        raise ScenarioEvidenceError(
            f"{field} is invalid", code="T4_EVIDENCE_VERIFIER_TOKEN_INVALID"
        )
    return value


def _timing(timeout_seconds: object, poll_seconds: object) -> None:
    timeout_valid = _finite(timeout_seconds, 1.0, 300.0)
    timeout = (
        float(timeout_seconds)
        if timeout_valid and isinstance(timeout_seconds, (int, float))
        else 0.0
    )
    if not timeout_valid or not _finite(
        poll_seconds, 0.01, min(30.0, timeout)
    ):
        raise ScenarioEvidenceError(
            "Scenario collection timing is invalid",
            code="T4_SCENARIO_TIMING_INVALID",
        )


def _finite(value: object, lower: float, upper: float) -> bool:
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and lower <= float(value) <= upper
    )


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise _invalid_response()
    return value


def _sha256_text(value: object) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise _invalid_response()
    return value


def _positive_int(value: object) -> int:
    if type(value) is not int or value <= 0:
        raise _invalid_response()
    return value


def _nonnegative_int(value: object) -> int:
    if type(value) is not int or value < 0:
        raise _invalid_response()
    return value


def _invalid_response() -> ScenarioEvidenceError:
    return ScenarioEvidenceError(
        "Evidence verifier response is invalid",
        code="T4_EVIDENCE_VERIFIER_RESPONSE_INVALID",
    )

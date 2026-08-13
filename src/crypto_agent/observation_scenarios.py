from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import re
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Protocol, TypeAlias, cast
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener
from uuid import UUID, uuid4, uuid5

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)

from .postgres import ConnectionFactory

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_UTC_MICROSECOND = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$"
)
_SIGNATURE_ALGORITHM = "ecdsa-p256-sha256-der"
_EVENT_RESPONSE_LIMIT_BYTES = 2_000_000
_CONTROL_RESPONSE_LIMIT_BYTES = 16_384

BRIDGE_EVENT_REASONS: Mapping[str, str] = {
    "bridge_started": "BRIDGE_STARTED",
    "campaign_checkpoint": "OBSERVATION_CAMPAIGN_CHECKPOINT",
    "session_connecting": "T4_SESSION_CONNECTING",
    "session_authenticated": "T4_SESSION_AUTHENTICATED",
    "session_ready": "READY",
    "session_disconnected": "T4_SESSION_DISCONNECTED",
    "cache_cleared": "T4_CACHE_CLEARED",
    "missing_data_detected": "T4_MISSING_DATA",
    "stale_data_detected": "T4_STALE_DATA",
    "rate_limited": "T4_RATE_LIMITED",
    "contract_roll_observed": "T4_CONTRACT_ROLL_OBSERVED",
    "control_requested": "OBSERVATION_CONTROL_REQUESTED",
    "control_applied": "OBSERVATION_CONTROL_APPLIED",
    "bridge_stopping": "BRIDGE_STOPPING",
    "outbound_rejected": "T4_OUTBOUND_REJECTED",
}

CONTROL_ACTION_SCENARIOS: Mapping[str, str] = {
    "reconnect": "reconnect",
    "missing_data": "missing_data",
    "stale_data": "stale_data",
    "rate_limit": "rate_limit",
    "restart": "bridge_restart",
}
CONTROLLED_SCENARIOS: Mapping[str, str] = {
    scenario: action for action, scenario in CONTROL_ACTION_SCENARIOS.items()
}
PASSIVE_SCENARIOS = frozenset({"replay_blocked", "roll_transition"})
MANDATORY_SCENARIOS = frozenset((*CONTROLLED_SCENARIOS, *PASSIVE_SCENARIOS))
CONTROL_STEPS = frozenset({"requested", "applied", "consumed"})

_SIGNED_CLAIM_KEYS = (
    "schema_version",
    "event_id",
    "sequence_no",
    "event_at",
    "event_type",
    "reason_code",
    "campaign_id",
    "boot_id",
    "session_generation",
    "reconnect_count",
    "environment",
    "bridge_schema_version",
    "read_only",
    "order_routes_exposed",
    "scope_key",
    "scenario_code",
    "action_request_id",
    "previous_event_hash",
    "payload",
    "payload_hash_sha256",
)
_EVENT_KEYS = frozenset(
    (
        *_SIGNED_CLAIM_KEYS,
        "event_hash_sha256",
        "canonical_payload_base64",
        "signature_algorithm",
        "evidence_key_fingerprint_sha256",
        "signature_base64",
    )
)
_PAGE_KEYS = frozenset(
    {
        "schema_version",
        "boot_id",
        "environment",
        "read_only",
        "order_routes_exposed",
        "signature_algorithm",
        "evidence_key_fingerprint_sha256",
        "public_key_spki_base64",
        "events",
    }
)
_CONTROL_RECEIPT_SCOPED_KEYS = frozenset(
    {
        "schema_version",
        "status",
        "action",
        "campaign_id",
        "action_request_id",
        "scope_key",
        "event_sequence_no",
        "event_hash_sha256",
        "read_only",
        "execution_enabled",
    }
)
_CHECKPOINT_RECEIPT_KEYS = frozenset(
    {
        "schema_version",
        "status",
        "campaign_id",
        "action_request_id",
        "event_sequence_no",
        "event_hash_sha256",
        "read_only",
        "execution_enabled",
    }
)
_PAYLOAD_KEY_ORDER = (
    "action",
    "control_step",
    "from_market_id",
    "to_market_id",
)
_SAFE_SCOPE_INTERVALS = frozenset({240, 1440, 10080})


class ScenarioEvidenceError(RuntimeError):
    """A secret-safe, fail-closed observation-scenario error."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


@dataclass(frozen=True, slots=True)
class BridgeEventPayload:
    action: str | None = None
    control_step: str | None = None
    from_market_id: str | None = None
    to_market_id: str | None = None

    def as_claim(self) -> dict[str, str]:
        values = {
            "action": self.action,
            "control_step": self.control_step,
            "from_market_id": self.from_market_id,
            "to_market_id": self.to_market_id,
        }
        return {key: value for key, value in values.items() if value is not None}


@dataclass(frozen=True, slots=True)
class SignedBridgeEvent:
    schema_version: int
    event_id: str
    sequence_no: int
    event_at: datetime
    event_type: str
    reason_code: str
    campaign_id: str | None
    boot_id: str
    session_generation: int
    reconnect_count: int
    environment: str
    bridge_schema_version: int
    read_only: bool
    order_routes_exposed: bool
    scope_key: str | None
    scenario_code: str | None
    action_request_id: str | None
    previous_event_hash: str | None
    payload: BridgeEventPayload
    payload_hash_sha256: str
    event_hash_sha256: str
    canonical_payload: bytes
    signature_der: bytes
    evidence_key_fingerprint_sha256: str

    @property
    def cursor(self) -> BridgeEventCursor:
        return BridgeEventCursor(
            boot_id=self.boot_id,
            sequence_no=self.sequence_no,
            event_hash_sha256=self.event_hash_sha256,
        )


@dataclass(frozen=True, slots=True)
class BridgeEventCursor:
    boot_id: str
    sequence_no: int
    event_hash_sha256: str

    def validate(self) -> None:
        _uuid(self.boot_id, "boot_id")
        if type(self.sequence_no) is not int or self.sequence_no <= 0:
            raise ScenarioEvidenceError(
                "Bridge event cursor sequence is invalid",
                code="T4_EVIDENCE_CURSOR_INVALID",
            )
        _sha256(self.event_hash_sha256, "event_hash_sha256")


@dataclass(frozen=True, slots=True)
class BridgeEventPage:
    boot_id: str
    environment: str
    evidence_key_fingerprint_sha256: str
    public_key_spki_der: bytes
    events: tuple[SignedBridgeEvent, ...]

    @property
    def cursor(self) -> BridgeEventCursor | None:
        return None if not self.events else self.events[-1].cursor


@dataclass(frozen=True, slots=True)
class ScenarioControlReceipt:
    action: str
    campaign_id: str
    action_request_id: str
    scope_key: str
    event_sequence_no: int
    event_hash_sha256: str


@dataclass(frozen=True, slots=True)
class ScenarioCheckpointReceipt:
    campaign_id: str
    action_request_id: str
    event_sequence_no: int
    event_hash_sha256: str


@dataclass(frozen=True, slots=True)
class ScenarioTrialHandle:
    trial_id: str
    campaign_id: str
    scenario_code: str
    action_request_id: str | None
    scope_key: str
    started_at: datetime


@dataclass(frozen=True, slots=True)
class ControlledScenarioRun:
    handle: ScenarioTrialHandle
    action: str
    control_receipt: ScenarioControlReceipt
    baseline_cursor: BridgeEventCursor | None
    initial_events: tuple[SignedBridgeEvent, ...]
    evidence_already_complete: bool


@dataclass(frozen=True, slots=True)
class ScenarioTrialEvidence:
    """References only; PostgreSQL must derive the trial outcome itself."""

    trial_id: str
    campaign_id: str
    scenario_code: str
    action_request_id: str | None
    bridge_event_hashes: tuple[str, ...]
    bridge_boot_ids: tuple[str, ...]
    control_receipt_event_hash: str | None = None

    def validate(self) -> None:
        _uuid(self.trial_id, "trial_id")
        _uuid(self.campaign_id, "campaign_id")
        _scenario(self.scenario_code)
        if self.action_request_id is not None:
            _uuid(self.action_request_id, "action_request_id")
        if (
            not self.bridge_event_hashes
            or len(set(self.bridge_event_hashes)) != len(self.bridge_event_hashes)
            or any(_SHA256.fullmatch(item) is None for item in self.bridge_event_hashes)
        ):
            raise ScenarioEvidenceError(
                "Scenario bridge event references are invalid",
                code="T4_SCENARIO_REFERENCE_INVALID",
            )
        if (
            not self.bridge_boot_ids
            or len(set(self.bridge_boot_ids)) != len(self.bridge_boot_ids)
        ):
            raise ScenarioEvidenceError(
                "Scenario bridge boot references are invalid",
                code="T4_SCENARIO_REFERENCE_INVALID",
            )
        for boot_id in self.bridge_boot_ids:
            _uuid(boot_id, "bridge_boot_id")
        if self.control_receipt_event_hash is not None:
            _sha256(
                self.control_receipt_event_hash,
                "control_receipt_event_hash",
            )


@dataclass(frozen=True, slots=True)
class ScenarioTrialResult:
    """Database-derived result; callers never submit this outcome to the bridge."""

    trial_id: str
    campaign_id: str
    scenario_code: str
    outcome: str
    completed_at: datetime
    content_hash_sha256: str

    def validate(self) -> None:
        _uuid(self.trial_id, "trial_id")
        _uuid(self.campaign_id, "campaign_id")
        _scenario(self.scenario_code)
        if self.outcome not in {"pass", "fail"}:
            raise ScenarioEvidenceError(
                "Scenario trial result is invalid",
                code="T4_SCENARIO_RESULT_INVALID",
            )
        _utc(self.completed_at, "completed_at")
        _sha256(self.content_hash_sha256, "content_hash_sha256")


@dataclass(frozen=True, slots=True)
class ReplayAttestationEvidence:
    attestation_id: str
    campaign_id: str
    scope_key: str
    replay_as_of: datetime
    replay_fingerprint_sha256: str
    source_batch_ids: tuple[int, ...]
    source_batch_hashes: tuple[str, ...]
    provenance_sha256: str

    def validate(self) -> None:
        _uuid(self.attestation_id, "attestation_id")
        _uuid(self.campaign_id, "campaign_id")
        _required_scope(self.scope_key)
        _utc(self.replay_as_of, "replay_as_of")
        _sha256(self.replay_fingerprint_sha256, "replay_fingerprint_sha256")
        if (
            len(self.source_batch_ids) != 1
            or len(self.source_batch_hashes) != 1
            or type(self.source_batch_ids[0]) is not int
            or self.source_batch_ids[0] <= 0
        ):
            raise ScenarioEvidenceError(
                "Replay attestation source batches are invalid",
                code="T4_REPLAY_ATTESTATION_INVALID",
            )
        _sha256(self.source_batch_hashes[0], "source_batch_hash")
        _sha256(self.provenance_sha256, "provenance_sha256")


@dataclass(frozen=True, slots=True)
class ReplayAttestationReceipt:
    replay_attestation_id: int
    content_hash_sha256: str

    def validate(self) -> None:
        if (
            type(self.replay_attestation_id) is not int
            or self.replay_attestation_id <= 0
        ):
            raise ScenarioEvidenceError(
                "Replay attestation result is invalid",
                code="T4_REPLAY_ATTESTATION_INVALID",
            )
        _sha256(self.content_hash_sha256, "content_hash_sha256")


class ScenarioEvidenceRepository(Protocol):
    """Persistence boundary whose implementation must use DB-side PASS validators."""

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


class PostgresScenarioEvidenceRepository:
    """Persist only locally verified references through restricted DB functions."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        if not callable(connection_factory):
            raise ScenarioEvidenceError(
                "Scenario evidence database is invalid",
                code="T4_SCENARIO_DATABASE_INVALID",
            )
        self.connection_factory = connection_factory

    def register_bridge_evidence_key(self, public_key_spki_der: bytes) -> str:
        if not isinstance(public_key_spki_der, bytes) or not public_key_spki_der:
            raise ScenarioEvidenceError(
                "Bridge evidence key is invalid",
                code="T4_EVIDENCE_KEY_INVALID",
            )
        expected = hashlib.sha256(public_key_spki_der).hexdigest()
        row = self._fetch_one(
            "SELECT crypto_agent.register_t4_bridge_evidence_key(%s)",
            (base64.b64encode(public_key_spki_der).decode("ascii"),),
        )
        fingerprint = _row_text(row, 0, "evidence key fingerprint")
        if fingerprint != expected:
            raise ScenarioEvidenceError(
                "Database evidence key fingerprint does not match",
                code="T4_EVIDENCE_KEY_MISMATCH",
            )
        return fingerprint

    def store_bridge_event_page(
        self,
        *,
        campaign_id: str,
        page: BridgeEventPage,
    ) -> None:
        campaign_id = _uuid(campaign_id, "campaign_id")
        if (
            not isinstance(page, BridgeEventPage)
            or page.environment != "live_t4"
            or not page.events
        ):
            raise ScenarioEvidenceError(
                "Bridge evidence page is not eligible for a live campaign",
                code="T4_EVIDENCE_PAGE_INVALID",
            )
        _sha256(
            page.evidence_key_fingerprint_sha256,
            "evidence_key_fingerprint_sha256",
        )
        for event in page.events:
            if event.campaign_id not in {None, campaign_id}:
                raise ScenarioEvidenceError(
                    "Bridge event belongs to another campaign",
                    code="T4_EVIDENCE_CAMPAIGN_MISMATCH",
                )
        statements = tuple(
            (
                """
                SELECT crypto_agent.record_verified_t4_bridge_observation_event(
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    event.event_id,
                    event.campaign_id,
                    event.sequence_no,
                    event.event_at,
                    event.event_type,
                    event.reason_code,
                    event.boot_id,
                    event.session_generation,
                    event.reconnect_count,
                    event.scenario_code,
                    event.action_request_id,
                    event.previous_event_hash,
                    event.payload_hash_sha256,
                    event.event_hash_sha256,
                    base64.b64encode(event.canonical_payload).decode("ascii"),
                    event.evidence_key_fingerprint_sha256,
                    base64.b64encode(event.signature_der).decode("ascii"),
                ),
            )
            for event in page.events
        )
        self._execute_many(statements)

    def begin_trial(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
        action_request_id: str | None,
        scope_key: str,
    ) -> ScenarioTrialHandle:
        campaign_id = _uuid(campaign_id, "campaign_id")
        scenario_code = _scenario(scenario_code)
        if scenario_code not in CONTROLLED_SCENARIOS:
            raise ScenarioEvidenceError(
                "Only a controlled scenario can be started",
                code="T4_SCENARIO_NOT_CONTROLLED",
            )
        if action_request_id is None:
            raise ScenarioEvidenceError(
                "Controlled scenario action identifier is required",
                code="T4_EVIDENCE_ID_INVALID",
            )
        action_request_id = _uuid(action_request_id, "action_request_id")
        scope_key = _required_scope(scope_key)
        row = self._fetch_one(
            """
            SELECT trial_id, started_at
            FROM crypto_agent.begin_t4_observation_scenario_trial(%s, %s, %s, %s)
            """,
            (campaign_id, scenario_code, action_request_id, scope_key),
        )
        return ScenarioTrialHandle(
            trial_id=_row_uuid(row, 0, "trial_id"),
            campaign_id=campaign_id,
            scenario_code=scenario_code,
            action_request_id=action_request_id,
            scope_key=scope_key,
            started_at=_row_utc(row, 1, "started_at"),
        )

    def submit_trial_evidence(
        self,
        evidence: ScenarioTrialEvidence,
    ) -> ScenarioTrialResult:
        evidence.validate()
        if evidence.scenario_code not in CONTROLLED_SCENARIOS:
            raise ScenarioEvidenceError(
                "Only controlled evidence can complete a started trial",
                code="T4_SCENARIO_NOT_CONTROLLED",
            )
        if evidence.control_receipt_event_hash is None:
            raise ScenarioEvidenceError(
                "Controlled scenario receipt reference is required",
                code="T4_SCENARIO_REFERENCE_INVALID",
            )
        row = self._fetch_one(
            """
            SELECT trial_id, campaign_id, scenario_code, outcome,
                   completed_at, content_hash
            FROM crypto_agent.complete_t4_observation_scenario_trial(%s, %s, %s)
            """,
            (
                evidence.trial_id,
                list(evidence.bridge_event_hashes),
                evidence.control_receipt_event_hash,
            ),
        )
        return _scenario_result(row)

    def verify_passive_trial(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
    ) -> ScenarioTrialResult:
        campaign_id = _uuid(campaign_id, "campaign_id")
        scenario_code = _scenario(scenario_code)
        if scenario_code not in PASSIVE_SCENARIOS:
            raise ScenarioEvidenceError(
                "Only passive evidence can be verified without control",
                code="T4_SCENARIO_NOT_PASSIVE",
            )
        row = self._fetch_one(
            """
            SELECT trial_id, campaign_id, scenario_code, outcome,
                   completed_at, content_hash
            FROM crypto_agent.verify_passive_t4_observation_scenario(%s, %s)
            """,
            (campaign_id, scenario_code),
        )
        return _scenario_result(row)

    def record_replay_attestation(
        self,
        evidence: ReplayAttestationEvidence,
    ) -> ReplayAttestationReceipt:
        if not isinstance(evidence, ReplayAttestationEvidence):
            raise ScenarioEvidenceError(
                "Replay attestation is invalid",
                code="T4_REPLAY_ATTESTATION_INVALID",
            )
        evidence.validate()
        row = self._fetch_one(
            """
            SELECT replay_attestation_id, content_hash
            FROM crypto_agent.record_verified_t4_replay_attestation(
                %s::uuid, %s::uuid, %s::text, %s::timestamptz,
                %s::crypto_agent.sha256_hex, %s::bigint[],
                %s::crypto_agent.sha256_hex[], %s::crypto_agent.sha256_hex
            )
            """,
            (
                evidence.attestation_id,
                evidence.campaign_id,
                evidence.scope_key,
                evidence.replay_as_of,
                evidence.replay_fingerprint_sha256,
                list(evidence.source_batch_ids),
                list(evidence.source_batch_hashes),
                evidence.provenance_sha256,
            ),
        )
        receipt = ReplayAttestationReceipt(
            replay_attestation_id=_row_positive_int(
                row, 0, "replay_attestation_id"
            ),
            content_hash_sha256=_row_text(row, 1, "content_hash"),
        )
        receipt.validate()
        return receipt

    def _fetch_one(self, query: str, params: Sequence[object]) -> object:
        from .postgres import cursor, transaction

        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute(query, params)
                row = db_cursor.fetchone()
        except ScenarioEvidenceError:
            raise
        except Exception:
            raise ScenarioEvidenceError(
                "Scenario evidence database operation failed safely",
                code="T4_SCENARIO_DATABASE_UNAVAILABLE",
            ) from None
        if row is None:
            raise ScenarioEvidenceError(
                "Scenario evidence database returned no result",
                code="T4_SCENARIO_DATABASE_INVALID",
            )
        return row

    def _execute_many(
        self,
        statements: Sequence[tuple[str, Sequence[object]]],
    ) -> None:
        from .postgres import cursor, transaction

        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                for query, params in statements:
                    db_cursor.execute(query, params)
                    if db_cursor.fetchone() is None:
                        raise ScenarioEvidenceError(
                            "Scenario event storage returned no result",
                            code="T4_SCENARIO_DATABASE_INVALID",
                        )
        except ScenarioEvidenceError:
            raise
        except Exception:
            raise ScenarioEvidenceError(
                "Scenario evidence database operation failed safely",
                code="T4_SCENARIO_DATABASE_UNAVAILABLE",
            ) from None

class BridgeObservationControl(Protocol):
    def list_events(
        self,
        *,
        cursor: BridgeEventCursor | None = None,
        limit: int = 512,
    ) -> BridgeEventPage: ...

    def trigger(
        self,
        action: str,
        *,
        campaign_id: str,
        action_request_id: str,
        scope_key: str,
    ) -> ScenarioControlReceipt: ...

    def checkpoint(
        self,
        *,
        campaign_id: str,
        action_request_id: str,
    ) -> ScenarioCheckpointReceipt: ...


class BridgeEvidenceVerifier:
    """Verify a pinned ECDSA P-256 bridge evidence key and signed event pages."""

    def __init__(
        self,
        *,
        evidence_key_fingerprint_sha256: str,
        evidence_public_key_spki_base64: str | None = None,
    ) -> None:
        fingerprint = _sha256(
            evidence_key_fingerprint_sha256,
            "evidence_key_fingerprint_sha256",
        )
        self.evidence_key_fingerprint_sha256 = fingerprint
        self.public_key: ec.EllipticCurvePublicKey | None = None
        self.public_key_spki_der: bytes | None = None
        if evidence_public_key_spki_base64 is None:
            return
        spki_der = _decode_base64(
            evidence_public_key_spki_base64,
            "public_key_spki_base64",
            maximum_bytes=512,
        )
        self._pin_page_key(spki_der)

    def _pin_page_key(self, spki_der: bytes) -> ec.EllipticCurvePublicKey:
        if hashlib.sha256(spki_der).hexdigest() != self.evidence_key_fingerprint_sha256:
            raise ScenarioEvidenceError(
                "Bridge evidence key fingerprint does not match the frozen value",
                code="T4_EVIDENCE_KEY_INVALID",
            )
        if self.public_key_spki_der is not None and spki_der != self.public_key_spki_der:
            raise ScenarioEvidenceError(
                "Bridge evidence key changed after it was pinned",
                code="T4_EVIDENCE_KEY_MISMATCH",
            )
        try:
            key = serialization.load_der_public_key(spki_der)
        except (TypeError, ValueError):
            raise ScenarioEvidenceError(
                "Pinned bridge evidence key is invalid",
                code="T4_EVIDENCE_KEY_INVALID",
            ) from None
        if not isinstance(key, ec.EllipticCurvePublicKey) or not isinstance(
            key.curve, ec.SECP256R1
        ):
            raise ScenarioEvidenceError(
                "Pinned bridge evidence key must use P-256",
                code="T4_EVIDENCE_KEY_INVALID",
            )
        self.public_key = key
        self.public_key_spki_der = spki_der
        return key

    def verify_page(
        self,
        payload: object,
        *,
        cursor: BridgeEventCursor | None = None,
    ) -> BridgeEventPage:
        item = _object(payload, "event page")
        _exact_keys(item, _PAGE_KEYS, "event page")
        if (
            item.get("schema_version") != 1
            or item.get("read_only") is not True
            or item.get("order_routes_exposed") is not False
            or item.get("signature_algorithm") != _SIGNATURE_ALGORITHM
            or item.get("evidence_key_fingerprint_sha256")
            != self.evidence_key_fingerprint_sha256
        ):
            raise ScenarioEvidenceError(
                "Bridge event page attestation is invalid",
                code="T4_EVIDENCE_PAGE_INVALID",
            )
        boot_id = _uuid(item.get("boot_id"), "boot_id")
        environment = _environment(item.get("environment"))
        response_spki = _decode_base64(
            item.get("public_key_spki_base64"),
            "public_key_spki_base64",
            maximum_bytes=512,
        )
        self._pin_page_key(response_spki)
        raw_events = item.get("events")
        if not isinstance(raw_events, list) or len(raw_events) > 4096:
            raise ScenarioEvidenceError(
                "Bridge event page events are invalid",
                code="T4_EVIDENCE_PAGE_INVALID",
            )
        if cursor is not None:
            cursor.validate()
        events: list[SignedBridgeEvent] = []
        expected_sequence = 1 if cursor is None else cursor.sequence_no + 1
        previous_hash = None if cursor is None else cursor.event_hash_sha256
        for raw_event in raw_events:
            event = self._verify_event(raw_event)
            if (
                event.environment != environment
                or event.sequence_no != expected_sequence
                or event.previous_event_hash != previous_hash
            ):
                raise ScenarioEvidenceError(
                    "Bridge event page hash chain is invalid",
                    code="T4_EVIDENCE_CHAIN_INVALID",
                )
            events.append(event)
            expected_sequence += 1
            previous_hash = event.event_hash_sha256
        return BridgeEventPage(
            boot_id=boot_id,
            environment=environment,
            evidence_key_fingerprint_sha256=(
                self.evidence_key_fingerprint_sha256
            ),
            public_key_spki_der=response_spki,
            events=tuple(events),
        )

    def _verify_event(self, payload: object) -> SignedBridgeEvent:
        item = _object(payload, "bridge event")
        _exact_keys(item, _EVENT_KEYS, "bridge event")
        if (
            item.get("signature_algorithm") != _SIGNATURE_ALGORITHM
            or item.get("evidence_key_fingerprint_sha256")
            != self.evidence_key_fingerprint_sha256
        ):
            raise ScenarioEvidenceError(
                "Bridge event signature attestation is invalid",
                code="T4_EVIDENCE_SIGNATURE_INVALID",
            )
        canonical_payload = _decode_base64(
            item.get("canonical_payload_base64"),
            "canonical_payload_base64",
            maximum_bytes=65_536,
        )
        signature = _decode_base64(
            item.get("signature_base64"),
            "signature_base64",
            maximum_bytes=256,
        )
        event_hash = _sha256(item.get("event_hash_sha256"), "event_hash_sha256")
        if hashlib.sha256(canonical_payload).hexdigest() != event_hash:
            raise ScenarioEvidenceError(
                "Bridge event content hash does not match",
                code="T4_EVIDENCE_HASH_MISMATCH",
            )
        try:
            r_value, s_value = decode_dss_signature(signature)
            if encode_dss_signature(r_value, s_value) != signature:
                raise ValueError
            public_key = self.public_key
            if public_key is None:
                raise ValueError
            public_key.verify(
                signature,
                canonical_payload,
                ec.ECDSA(hashes.SHA256()),
            )
        except (InvalidSignature, TypeError, ValueError):
            raise ScenarioEvidenceError(
                "Bridge event signature verification failed",
                code="T4_EVIDENCE_SIGNATURE_INVALID",
            ) from None
        claims = _load_json(canonical_payload, "canonical bridge event")
        claim = _object(claims, "canonical bridge event")
        if tuple(claim) != _SIGNED_CLAIM_KEYS:
            raise ScenarioEvidenceError(
                "Canonical bridge event field order is invalid",
                code="T4_EVIDENCE_CANONICAL_INVALID",
            )
        projected = {key: item.get(key) for key in _SIGNED_CLAIM_KEYS}
        if claim != projected:
            raise ScenarioEvidenceError(
                "Canonical bridge event differs from its projection",
                code="T4_EVIDENCE_PROJECTION_MISMATCH",
            )
        return _validated_event(item, canonical_payload, signature)


class T4BridgeObservationClient:
    """Loopback-only client for the signed, read-only bridge evidence surface."""

    _allowed_hosts = frozenset({"127.0.0.1", "localhost", "::1"})

    def __init__(
        self,
        *,
        bridge_url: str,
        bridge_token: str,
        observation_control_token: str | None = None,
        evidence_key_fingerprint_sha256: str,
        evidence_public_key_spki_base64: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        parsed = urlsplit(bridge_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in self._allowed_hosts
            or parsed.port is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ScenarioEvidenceError(
                "T4 observation bridge must be an explicit loopback HTTP origin",
                code="T4_OBSERVATION_ORIGIN_INVALID",
            )
        _token(bridge_token, "bridge_token")
        if observation_control_token is not None:
            _token(observation_control_token, "observation_control_token")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(float(timeout_seconds))
            or not 1 <= float(timeout_seconds) <= 30
        ):
            raise ScenarioEvidenceError(
                "T4 observation timeout is invalid",
                code="T4_OBSERVATION_TIMEOUT_INVALID",
            )
        self._bridge_url = bridge_url.rstrip("/")
        self._bridge_token = bridge_token
        self._observation_control_token = observation_control_token
        self._timeout_seconds = float(timeout_seconds)
        self.verifier = BridgeEvidenceVerifier(
            evidence_key_fingerprint_sha256=evidence_key_fingerprint_sha256,
            evidence_public_key_spki_base64=evidence_public_key_spki_base64,
        )

    def list_events(
        self,
        *,
        cursor: BridgeEventCursor | None = None,
        limit: int = 512,
    ) -> BridgeEventPage:
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ScenarioEvidenceError(
                "Bridge event page limit is invalid",
                code="T4_EVIDENCE_LIMIT_INVALID",
            )
        if cursor is not None:
            cursor.validate()
        query: dict[str, str | int] = {"limit": limit}
        if cursor is not None:
            query["after_sequence"] = cursor.sequence_no
        payload = self._request_json(
            Request(
                f"{self._bridge_url}/v1/observation/events?{urlencode(query)}",
                headers={
                    "Accept": "application/json",
                    "X-Crypto-Agent-Bridge-Token": self._bridge_token,
                },
                method="GET",
            ),
            expected_status=200,
            maximum_bytes=_EVENT_RESPONSE_LIMIT_BYTES,
        )
        return self.verifier.verify_page(payload, cursor=cursor)

    def trigger(
        self,
        action: str,
        *,
        campaign_id: str,
        action_request_id: str,
        scope_key: str,
    ) -> ScenarioControlReceipt:
        if self._observation_control_token is None:
            raise ScenarioEvidenceError(
                "T4 observation control is not configured",
                code="T4_OBSERVATION_CONTROL_NOT_CONFIGURED",
            )
        if action not in CONTROL_ACTION_SCENARIOS:
            raise ScenarioEvidenceError(
                "Observation control action is not allowed",
                code="T4_OBSERVATION_ACTION_INVALID",
            )
        campaign_id = _uuid(campaign_id, "campaign_id")
        action_request_id = _uuid(action_request_id, "action_request_id")
        scope_key = _required_scope(scope_key)
        body = _canonical_json(
            {
                "campaign_id": campaign_id,
                "action_request_id": action_request_id,
                "scope_key": scope_key,
            }
        )
        payload = self._request_json(
            Request(
                f"{self._bridge_url}/v1/observation/control/{action}",
                data=body,
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Crypto-Agent-Bridge-Token": self._bridge_token,
                    "X-Crypto-Agent-Observation-Control-Token": (
                        self._observation_control_token
                    ),
                },
                method="POST",
            ),
            expected_status=202,
            maximum_bytes=_CONTROL_RESPONSE_LIMIT_BYTES,
        )
        return _control_receipt(
            payload,
            expected_action=action,
            expected_campaign_id=campaign_id,
            expected_action_request_id=action_request_id,
            expected_scope_key=scope_key,
        )

    def checkpoint(
        self,
        *,
        campaign_id: str,
        action_request_id: str,
    ) -> ScenarioCheckpointReceipt:
        campaign_id = _uuid(campaign_id, "campaign_id")
        action_request_id = _uuid(action_request_id, "action_request_id")
        payload = self._request_json(
            Request(
                f"{self._bridge_url}/v1/observation/checkpoint",
                data=_canonical_json(
                    {
                        "campaign_id": campaign_id,
                        "action_request_id": action_request_id,
                    }
                ),
                headers={
                    "Accept": "application/json",
                    "Content-Type": "application/json",
                    "X-Crypto-Agent-Bridge-Token": self._bridge_token,
                },
                method="POST",
            ),
            expected_status=201,
            maximum_bytes=_CONTROL_RESPONSE_LIMIT_BYTES,
        )
        return _checkpoint_receipt(
            payload,
            expected_campaign_id=campaign_id,
            expected_action_request_id=action_request_id,
        )

    def _request_json(
        self,
        request: Request,
        *,
        expected_status: int,
        maximum_bytes: int,
    ) -> object:
        try:
            response = build_opener(ProxyHandler({}), _NoRedirectHandler()).open(
                request,
                timeout=self._timeout_seconds,
            )
            try:
                status = getattr(response, "status", None)
                headers = getattr(response, "headers", None)
                content_type = (
                    headers.get_content_type()
                    if headers is not None and callable(getattr(headers, "get_content_type", None))
                    else ""
                )
                if status != expected_status or content_type != "application/json":
                    raise ValueError
                body = response.read(maximum_bytes + 1)
            finally:
                close = getattr(response, "close", None)
                if callable(close):
                    close()
        except Exception:
            raise ScenarioEvidenceError(
                "T4 observation bridge request failed safely",
                code="T4_OBSERVATION_BRIDGE_UNAVAILABLE",
            ) from None
        if not isinstance(body, bytes) or not body or len(body) > maximum_bytes:
            raise ScenarioEvidenceError(
                "T4 observation bridge response is invalid",
                code="T4_OBSERVATION_RESPONSE_INVALID",
            )
        return _load_json(body, "bridge response")


class ScenarioEvidenceRunner:
    """Collect signed references; the repository alone decides PASS or FAIL."""

    def __init__(
        self,
        client: BridgeObservationControl,
        repository: ScenarioEvidenceRepository,
    ) -> None:
        self.client = client
        self.repository = repository

    def sync_bridge_events(
        self,
        *,
        campaign_id: str,
        page_limit: int = 512,
        required_checkpoint: ScenarioCheckpointReceipt | None = None,
    ) -> int:
        """Verify and persist the current signed journal without using control."""

        campaign_id = _uuid(campaign_id, "campaign_id")
        if type(page_limit) is not int or not 1 <= page_limit <= 1000:
            raise ScenarioEvidenceError(
                "Bridge event page limit is invalid",
                code="T4_EVIDENCE_LIMIT_INVALID",
            )
        cursor: BridgeEventCursor | None = None
        stored = 0
        checkpoint_seen = required_checkpoint is None
        # The bridge journal is capped at 100,000 events.  The explicit bound
        # keeps a compromised peer from turning synchronization into an endless loop.
        maximum_pages = 100_000 // page_limit + 2
        for _ in range(maximum_pages):
            page = self.client.list_events(cursor=cursor, limit=page_limit)
            if page.events:
                self.repository.store_bridge_event_page(
                    campaign_id=campaign_id,
                    page=page,
                )
                stored += len(page.events)
                cursor = page.cursor
                if required_checkpoint is not None and any(
                    _matches_checkpoint(event, required_checkpoint)
                    for event in page.events
                ):
                    checkpoint_seen = True
            if len(page.events) < page_limit:
                if not checkpoint_seen:
                    raise ScenarioEvidenceError(
                        "Signed campaign checkpoint is absent from the journal",
                        code="T4_EVIDENCE_CHECKPOINT_MISSING",
                    )
                return stored
        raise ScenarioEvidenceError(
            "Bridge event journal exceeds the bounded evidence window",
            code="T4_EVIDENCE_JOURNAL_LIMIT_EXCEEDED",
        )

    def record_campaign_checkpoint(
        self,
        *,
        campaign_id: str,
        action_request_id: str | None = None,
    ) -> ScenarioCheckpointReceipt:
        campaign_id = _uuid(campaign_id, "campaign_id")
        request_id = _uuid(
            action_request_id or str(uuid4()),
            "action_request_id",
        )
        receipt = self.client.checkpoint(
            campaign_id=campaign_id,
            action_request_id=request_id,
        )
        self.sync_bridge_events(
            campaign_id=campaign_id,
            required_checkpoint=receipt,
        )
        return receipt

    def start_controlled(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
        action_request_id: str | None = None,
        scope_key: str,
        before_trigger: Callable[[], object] | None = None,
    ) -> ControlledScenarioRun:
        campaign_id = _uuid(campaign_id, "campaign_id")
        action = CONTROLLED_SCENARIOS.get(scenario_code)
        if action is None:
            raise ScenarioEvidenceError(
                "Scenario is not an approved controlled scenario",
                code="T4_SCENARIO_NOT_CONTROLLED",
            )
        request_id = _uuid(
            action_request_id or str(uuid4()),
            "action_request_id",
        )
        scope_key = _required_scope(scope_key)
        journal_events: list[SignedBridgeEvent] = []
        baseline_cursor: BridgeEventCursor | None = None
        maximum_pages = 102
        for _ in range(maximum_pages):
            page = self.client.list_events(cursor=baseline_cursor, limit=1000)
            if page.events:
                self.repository.store_bridge_event_page(
                    campaign_id=campaign_id,
                    page=page,
                )
                journal_events.extend(page.events)
                baseline_cursor = page.cursor
            if len(page.events) < 1000:
                break
        else:
            raise ScenarioEvidenceError(
                "Bridge event journal exceeds the bounded evidence window",
                code="T4_EVIDENCE_JOURNAL_LIMIT_EXCEEDED",
            )
        matching_receipts = tuple(
            event
            for event in journal_events
            if event.event_type == "control_applied"
            and event.campaign_id == campaign_id
            and event.scenario_code == scenario_code
            and event.action_request_id == request_id
            and event.scope_key == scope_key
            and event.payload.action == action
            and event.payload.control_step == "applied"
        )
        if len(matching_receipts) > 1:
            raise ScenarioEvidenceError(
                "Controlled scenario receipt identifier was reused",
                code="T4_SCENARIO_RECEIPT_COLLISION",
            )
        if not matching_receipts and before_trigger is not None:
            # Perform schedule/due checks before the append-only DB request is
            # opened.  A bridge receipt already present in the verified journal
            # means this is a resume and must not repeat a transient precheck.
            before_trigger()
        handle = self.repository.begin_trial(
            campaign_id=campaign_id,
            scenario_code=scenario_code,
            action_request_id=request_id,
            scope_key=scope_key,
        )
        _validate_handle(
            handle,
            campaign_id=campaign_id,
            scenario_code=scenario_code,
            action_request_id=request_id,
            scope_key=scope_key,
        )
        if matching_receipts:
            signed_receipt = matching_receipts[0]
            receipt = ScenarioControlReceipt(
                action=action,
                campaign_id=campaign_id,
                action_request_id=request_id,
                scope_key=scope_key,
                event_sequence_no=signed_receipt.sequence_no,
                event_hash_sha256=signed_receipt.event_hash_sha256,
            )
        else:
            receipt = self.client.trigger(
                action,
                campaign_id=campaign_id,
                action_request_id=request_id,
                scope_key=scope_key,
            )
        run = ControlledScenarioRun(
            handle=handle,
            action=action,
            control_receipt=receipt,
            baseline_cursor=baseline_cursor,
            initial_events=tuple(journal_events),
            evidence_already_complete=False,
        )
        return ControlledScenarioRun(
            handle=run.handle,
            action=run.action,
            control_receipt=run.control_receipt,
            baseline_cursor=run.baseline_cursor,
            initial_events=run.initial_events,
            evidence_already_complete=(
                _controlled_evidence_fragment(run, run.initial_events) is not None
            ),
        )

    def collect_controlled(
        self,
        run: ControlledScenarioRun,
        *,
        recovery: Callable[[ControlledScenarioRun], object] | None = None,
        timeout_seconds: float = 60.0,
        poll_seconds: float = 0.5,
    ) -> ScenarioTrialResult:
        _validate_run(run)
        if not _finite_range(timeout_seconds, 1.0, 300.0) or not _finite_range(
            poll_seconds, 0.01, min(30.0, float(timeout_seconds))
        ):
            raise ScenarioEvidenceError(
                "Scenario collection timing is invalid",
                code="T4_SCENARIO_TIMING_INVALID",
            )
        deadline = time.monotonic() + float(timeout_seconds)
        evidence_by_hash: dict[str, SignedBridgeEvent] = {
            event.event_hash_sha256: event for event in run.initial_events
        }
        cursor = run.baseline_cursor

        while time.monotonic() < deadline:
            try:
                page = self.client.list_events(cursor=cursor)
            except ScenarioEvidenceError as exc:
                if exc.code != "T4_OBSERVATION_BRIDGE_UNAVAILABLE":
                    raise
                time.sleep(float(poll_seconds))
                continue

            if page.events:
                self.repository.store_bridge_event_page(
                    campaign_id=run.handle.campaign_id,
                    page=page,
                )
            if page.cursor is not None:
                cursor = page.cursor
            for event in page.events:
                evidence_by_hash[event.event_hash_sha256] = event
            ordered = tuple(
                sorted(
                    evidence_by_hash.values(),
                    key=lambda event: event.sequence_no,
                )
            )
            fragment = _controlled_evidence_fragment(run, ordered)
            if fragment is not None:
                if recovery is not None:
                    recovery(run)
                    recovery = None
                references = tuple(
                    dict.fromkeys(
                        (
                            run.control_receipt.event_hash_sha256,
                            *(event.event_hash_sha256 for event in fragment),
                        )
                    )
                )
                evidence = ScenarioTrialEvidence(
                    trial_id=run.handle.trial_id,
                    campaign_id=run.handle.campaign_id,
                    scenario_code=run.handle.scenario_code,
                    action_request_id=run.handle.action_request_id,
                    bridge_event_hashes=references,
                    bridge_boot_ids=tuple(
                        dict.fromkeys(event.boot_id for event in fragment)
                    ),
                    control_receipt_event_hash=(
                        run.control_receipt.event_hash_sha256
                    ),
                )
                evidence.validate()
                result = self.repository.submit_trial_evidence(evidence)
                result.validate()
                if (
                    result.trial_id != run.handle.trial_id
                    or result.campaign_id != run.handle.campaign_id
                    or result.scenario_code != run.handle.scenario_code
                ):
                    raise ScenarioEvidenceError(
                        "Scenario repository returned a mismatched result",
                        code="T4_SCENARIO_RESULT_MISMATCH",
                    )
                return result
            time.sleep(float(poll_seconds))
        raise ScenarioEvidenceError(
            "Objective scenario evidence was not observed before the deadline",
            code="T4_SCENARIO_EVIDENCE_TIMEOUT",
        )

    def run_controlled(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
        scope_key: str,
        action_request_id: str | None = None,
        before_trigger: Callable[[], object] | None = None,
        exercise: Callable[[ControlledScenarioRun], object] | None = None,
        recovery: Callable[[ControlledScenarioRun], object] | None = None,
        timeout_seconds: float = 60.0,
        poll_seconds: float = 0.5,
    ) -> ScenarioTrialResult:
        campaign_id = _uuid(campaign_id, "campaign_id")
        request_id = action_request_id or str(
            uuid5(
                UUID(campaign_id),
                f"t4-observation-scenario:{scenario_code}:{scope_key}",
            )
        )
        run = self.start_controlled(
            campaign_id=campaign_id,
            scenario_code=scenario_code,
            action_request_id=request_id,
            scope_key=scope_key,
            before_trigger=before_trigger,
        )
        if exercise is not None and not run.evidence_already_complete:
            exercise(run)
        return self.collect_controlled(
            run,
            recovery=recovery,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )

    def verify_passive(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
    ) -> ScenarioTrialResult:
        campaign_id = _uuid(campaign_id, "campaign_id")
        if scenario_code not in PASSIVE_SCENARIOS:
            raise ScenarioEvidenceError(
                "Scenario is not an approved passive scenario",
                code="T4_SCENARIO_NOT_PASSIVE",
            )
        result = self.repository.verify_passive_trial(
            campaign_id=campaign_id,
            scenario_code=scenario_code,
        )
        result.validate()
        if result.campaign_id != campaign_id or result.scenario_code != scenario_code:
            raise ScenarioEvidenceError(
                "Scenario repository returned a mismatched passive result",
                code="T4_SCENARIO_RESULT_MISMATCH",
            )
        return result


def _validated_event(
    item: Mapping[str, object],
    canonical_payload: bytes,
    signature_der: bytes,
) -> SignedBridgeEvent:
    if item.get("schema_version") != 1:
        raise _event_invalid()
    event_id = _uuid(item.get("event_id"), "event_id")
    sequence_no = _nonnegative_int(item.get("sequence_no"), "sequence_no", positive=True)
    event_at = _utc_timestamp(item.get("event_at"), "event_at")
    event_type = item.get("event_type")
    reason_code = item.get("reason_code")
    if (
        not isinstance(event_type, str)
        or BRIDGE_EVENT_REASONS.get(event_type) != reason_code
    ):
        raise ScenarioEvidenceError(
            "Bridge event type and safe reason do not match",
            code="T4_EVIDENCE_REASON_INVALID",
        )
    campaign_id = _optional_uuid(item.get("campaign_id"), "campaign_id")
    boot_id = _uuid(item.get("boot_id"), "boot_id")
    session_generation = _nonnegative_int(
        item.get("session_generation"), "session_generation"
    )
    reconnect_count = _nonnegative_int(item.get("reconnect_count"), "reconnect_count")
    environment = _environment(item.get("environment"))
    if (
        item.get("bridge_schema_version") != 5
        or item.get("read_only") is not True
        or item.get("order_routes_exposed") is not False
    ):
        raise _event_invalid()
    scope_key = _optional_scope(item.get("scope_key"))
    scenario_code = _optional_scenario(item.get("scenario_code"))
    action_request_id = _optional_uuid(
        item.get("action_request_id"), "action_request_id"
    )
    previous_event_hash = _optional_sha256(
        item.get("previous_event_hash"), "previous_event_hash"
    )
    payload = _event_payload(item.get("payload"))
    payload_hash = _sha256(item.get("payload_hash_sha256"), "payload_hash_sha256")
    if hashlib.sha256(_canonical_json(payload.as_claim())).hexdigest() != payload_hash:
        raise ScenarioEvidenceError(
            "Bridge event payload hash does not match",
            code="T4_EVIDENCE_PAYLOAD_HASH_MISMATCH",
        )
    event_hash = _sha256(item.get("event_hash_sha256"), "event_hash_sha256")
    _validate_event_semantics(
        event_type=event_type,
        campaign_id=campaign_id,
        scenario_code=scenario_code,
        action_request_id=action_request_id,
        scope_key=scope_key,
        payload=payload,
    )
    return SignedBridgeEvent(
        schema_version=1,
        event_id=event_id,
        sequence_no=sequence_no,
        event_at=event_at,
        event_type=event_type,
        reason_code=cast(str, reason_code),
        campaign_id=campaign_id,
        boot_id=boot_id,
        session_generation=session_generation,
        reconnect_count=reconnect_count,
        environment=environment,
        bridge_schema_version=5,
        read_only=True,
        order_routes_exposed=False,
        scope_key=scope_key,
        scenario_code=scenario_code,
        action_request_id=action_request_id,
        previous_event_hash=previous_event_hash,
        payload=payload,
        payload_hash_sha256=payload_hash,
        event_hash_sha256=event_hash,
        canonical_payload=canonical_payload,
        signature_der=signature_der,
        evidence_key_fingerprint_sha256=cast(
            str, item.get("evidence_key_fingerprint_sha256")
        ),
    )


def _validate_event_semantics(
    *,
    event_type: str,
    campaign_id: str | None,
    scenario_code: str | None,
    action_request_id: str | None,
    scope_key: str | None,
    payload: BridgeEventPayload,
) -> None:
    action = payload.action
    step = payload.control_step
    if action is not None and action not in {*CONTROL_ACTION_SCENARIOS, "checkpoint"}:
        raise _event_invalid()
    if step is not None and step not in CONTROL_STEPS:
        raise _event_invalid()
    if (action is None) != (step is None):
        raise _event_invalid()
    if event_type == "campaign_checkpoint":
        if (
            campaign_id is None
            or scenario_code is not None
            or action_request_id is None
            or scope_key is not None
            or action != "checkpoint"
            or step != "consumed"
        ):
            raise _event_invalid()
    elif action == "checkpoint":
        raise _event_invalid()
    if event_type == "control_requested":
        if step != "requested":
            raise _event_invalid()
    elif event_type == "control_applied" and step not in {"applied", "consumed"}:
        raise _event_invalid()
    if scenario_code == "roll_transition":
        if (
            event_type != "contract_roll_observed"
            or action_request_id is not None
            or action is not None
        ):
            raise _event_invalid()
    elif scenario_code is not None:
        allowed_correlated_types = {
            "reconnect": {
                "control_requested",
                "control_applied",
                "cache_cleared",
                "session_ready",
            },
            "missing_data": {
                "control_requested",
                "control_applied",
                "cache_cleared",
                "missing_data_detected",
            },
            "stale_data": {
                "control_requested",
                "control_applied",
                "stale_data_detected",
            },
            "rate_limit": {
                "control_requested",
                "control_applied",
                "rate_limited",
            },
            "bridge_restart": {
                "control_requested",
                "control_applied",
                "bridge_stopping",
            },
        }.get(scenario_code)
        if (
            campaign_id is None
            or action_request_id is None
            or scope_key is None
            or allowed_correlated_types is None
            or event_type not in allowed_correlated_types
            or (
                action is not None
                and CONTROL_ACTION_SCENARIOS.get(action) != scenario_code
            )
            or (event_type != "cache_cleared" and action is None)
        ):
            raise _event_invalid()
    elif event_type != "campaign_checkpoint" and (
        action_request_id is not None or action is not None
    ):
        raise _event_invalid()
    if event_type in {"control_requested", "control_applied"} and (
        campaign_id is None or scenario_code is None or action_request_id is None
    ):
        raise _event_invalid()
    if event_type in {
        "missing_data_detected",
        "stale_data_detected",
        "rate_limited",
    } and scope_key is None:
        raise _event_invalid()
    has_roll_ids = payload.from_market_id is not None or payload.to_market_id is not None
    if event_type == "contract_roll_observed":
        if (
            scope_key is None
            or payload.from_market_id is None
            or payload.to_market_id is None
            or payload.from_market_id == payload.to_market_id
            or scenario_code != "roll_transition"
        ):
            raise _event_invalid()
    elif has_roll_ids:
        raise _event_invalid()


def _event_payload(value: object) -> BridgeEventPayload:
    item = _object(value, "bridge event payload")
    if any(key not in _PAYLOAD_KEY_ORDER for key in item) or tuple(item) != tuple(
        key for key in _PAYLOAD_KEY_ORDER if key in item
    ):
        raise ScenarioEvidenceError(
            "Bridge event payload fields are invalid",
            code="T4_EVIDENCE_PAYLOAD_INVALID",
        )
    action = _optional_text(item.get("action"), "action", maximum=32)
    control_step = _optional_text(
        item.get("control_step"), "control_step", maximum=32
    )
    from_market_id = _optional_opaque_id(
        item.get("from_market_id"), "from_market_id", maximum=256
    )
    to_market_id = _optional_opaque_id(
        item.get("to_market_id"), "to_market_id", maximum=256
    )
    return BridgeEventPayload(
        action=action,
        control_step=control_step,
        from_market_id=from_market_id,
        to_market_id=to_market_id,
    )


def _control_receipt(
    payload: object,
    *,
    expected_action: str,
    expected_campaign_id: str,
    expected_action_request_id: str,
    expected_scope_key: str,
) -> ScenarioControlReceipt:
    item = _object(payload, "control receipt")
    if frozenset(item) != _CONTROL_RECEIPT_SCOPED_KEYS:
        raise ScenarioEvidenceError(
            "Control receipt fields are invalid",
            code="T4_EVIDENCE_FIELDS_INVALID",
        )
    response_scope = item.get("scope_key")
    if (
        item.get("schema_version") != 1
        or item.get("status") != "accepted"
        or item.get("action") != expected_action
        or item.get("campaign_id") != expected_campaign_id
        or item.get("action_request_id") != expected_action_request_id
        or item.get("read_only") is not True
        or item.get("execution_enabled") is not False
        or _required_scope(response_scope) != expected_scope_key
    ):
        raise ScenarioEvidenceError(
            "Observation control receipt is invalid",
            code="T4_OBSERVATION_RECEIPT_INVALID",
        )
    sequence_no = _nonnegative_int(
        item.get("event_sequence_no"), "event_sequence_no", positive=True
    )
    event_hash = _sha256(item.get("event_hash_sha256"), "event_hash_sha256")
    return ScenarioControlReceipt(
        action=expected_action,
        campaign_id=expected_campaign_id,
        action_request_id=expected_action_request_id,
        scope_key=expected_scope_key,
        event_sequence_no=sequence_no,
        event_hash_sha256=event_hash,
    )


def _checkpoint_receipt(
    payload: object,
    *,
    expected_campaign_id: str,
    expected_action_request_id: str,
) -> ScenarioCheckpointReceipt:
    item = _object(payload, "checkpoint receipt")
    if frozenset(item) != _CHECKPOINT_RECEIPT_KEYS:
        raise ScenarioEvidenceError(
            "Checkpoint receipt fields are invalid",
            code="T4_EVIDENCE_FIELDS_INVALID",
        )
    if (
        item.get("schema_version") != 1
        or item.get("status") != "recorded"
        or item.get("campaign_id") != expected_campaign_id
        or item.get("action_request_id") != expected_action_request_id
        or item.get("read_only") is not True
        or item.get("execution_enabled") is not False
    ):
        raise ScenarioEvidenceError(
            "Observation checkpoint receipt is invalid",
            code="T4_OBSERVATION_CHECKPOINT_INVALID",
        )
    return ScenarioCheckpointReceipt(
        campaign_id=expected_campaign_id,
        action_request_id=expected_action_request_id,
        event_sequence_no=_nonnegative_int(
            item.get("event_sequence_no"),
            "event_sequence_no",
            positive=True,
        ),
        event_hash_sha256=_sha256(
            item.get("event_hash_sha256"),
            "event_hash_sha256",
        ),
    )


def _matches_checkpoint(
    event: SignedBridgeEvent,
    receipt: ScenarioCheckpointReceipt,
) -> bool:
    return (
        event.event_type == "campaign_checkpoint"
        and event.reason_code == "OBSERVATION_CAMPAIGN_CHECKPOINT"
        and event.campaign_id == receipt.campaign_id
        and event.action_request_id == receipt.action_request_id
        and event.sequence_no == receipt.event_sequence_no
        and event.event_hash_sha256 == receipt.event_hash_sha256
        and event.scope_key is None
        and event.scenario_code is None
        and event.payload.action == "checkpoint"
        and event.payload.control_step == "consumed"
    )


def _controlled_evidence_fragment(
    run: ControlledScenarioRun,
    events: Sequence[SignedBridgeEvent],
) -> tuple[SignedBridgeEvent, ...] | None:
    def linked(event: SignedBridgeEvent) -> bool:
        return (
            event.campaign_id == run.handle.campaign_id
            and event.scenario_code == run.handle.scenario_code
            and event.action_request_id == run.handle.action_request_id
            and event.scope_key == run.handle.scope_key
        )

    requested = next(
        (
            index
            for index, event in enumerate(events)
            if linked(event)
            and event.event_type == "control_requested"
            and event.payload.action == run.action
            and event.payload.control_step == "requested"
        ),
        None,
    )
    if requested is None:
        return None
    applied = next(
        (
            index
            for index in range(requested + 1, len(events))
            if linked(events[index])
            and events[index].event_type == "control_applied"
            and events[index].payload.action == run.action
            and events[index].payload.control_step == "applied"
            and events[index].event_hash_sha256
            == run.control_receipt.event_hash_sha256
            and events[index].sequence_no
            == run.control_receipt.event_sequence_no
        ),
        None,
    )
    if applied is None:
        return None
    scenario = run.handle.scenario_code
    if scenario == "reconnect":
        if not any(
            linked(event) and event.event_type == "cache_cleared"
            for event in events[requested + 1 : applied]
        ):
            return None
        terminal = next(
            (
                index
                for index in range(applied + 1, len(events))
                if linked(events[index])
                and events[index].event_type == "session_ready"
                and events[index].payload.action == "reconnect"
                and events[index].payload.control_step == "consumed"
            ),
            None,
        )
        if terminal is None:
            return None
        event_types = {event.event_type for event in events[applied + 1 : terminal + 1]}
        if not {
            "session_disconnected",
            "cache_cleared",
            "session_connecting",
            "session_authenticated",
            "session_ready",
        }.issubset(event_types):
            return None
        return tuple(events[requested : terminal + 1])
    if scenario == "bridge_restart":
        stopping = next(
            (
                index
                for index in range(applied + 1, len(events))
                if linked(events[index])
                and events[index].event_type == "bridge_stopping"
                and events[index].payload.action == "restart"
                and events[index].payload.control_step == "consumed"
            ),
            None,
        )
        if stopping is None:
            return None
        old_boot = events[stopping].boot_id
        started = next(
            (
                index
                for index in range(stopping + 1, len(events))
                if events[index].event_type == "bridge_started"
                and events[index].boot_id != old_boot
            ),
            None,
        )
        if started is None:
            return None
        new_boot = events[started].boot_id
        ready = next(
            (
                index
                for index in range(started + 1, len(events))
                if events[index].event_type == "session_ready"
                and events[index].boot_id == new_boot
            ),
            None,
        )
        if ready is None:
            return None
        new_boot_types = {
            event.event_type
            for event in events[started : ready + 1]
            if event.boot_id == new_boot
        }
        if not {
            "bridge_started",
            "session_connecting",
            "session_authenticated",
            "session_ready",
        }.issubset(new_boot_types):
            return None
        return tuple(events[requested : ready + 1])
    required = {
        "missing_data": "missing_data_detected",
        "stale_data": "stale_data_detected",
        "rate_limit": "rate_limited",
    }.get(scenario)
    if required is None:
        return None
    terminal = next(
        (
            index
            for index in range(applied + 1, len(events))
            if linked(events[index])
            and events[index].event_type == required
            and events[index].payload.action == run.action
            and events[index].payload.control_step == "consumed"
        ),
        None,
    )
    return None if terminal is None else tuple(events[requested : terminal + 1])


def _validate_handle(
    handle: ScenarioTrialHandle,
    *,
    campaign_id: str,
    scenario_code: str,
    action_request_id: str,
    scope_key: str,
) -> None:
    if not isinstance(handle, ScenarioTrialHandle):
        raise ScenarioEvidenceError(
            "Scenario repository returned an invalid handle",
            code="T4_SCENARIO_HANDLE_INVALID",
        )
    _uuid(handle.trial_id, "trial_id")
    _utc(handle.started_at, "started_at")
    if (
        handle.campaign_id != campaign_id
        or handle.scenario_code != scenario_code
        or handle.action_request_id != action_request_id
        or handle.scope_key != scope_key
    ):
        raise ScenarioEvidenceError(
            "Scenario repository returned a mismatched handle",
            code="T4_SCENARIO_HANDLE_MISMATCH",
        )


def _validate_run(run: ControlledScenarioRun) -> None:
    if not isinstance(run, ControlledScenarioRun):
        raise ScenarioEvidenceError(
            "Controlled scenario run is invalid",
            code="T4_SCENARIO_RUN_INVALID",
        )
    expected_action = CONTROLLED_SCENARIOS.get(run.handle.scenario_code)
    if (
        expected_action != run.action
        or run.control_receipt.action != run.action
        or run.control_receipt.campaign_id != run.handle.campaign_id
        or run.control_receipt.action_request_id != run.handle.action_request_id
        or run.control_receipt.scope_key != run.handle.scope_key
    ):
        raise ScenarioEvidenceError(
            "Controlled scenario run linkage is invalid",
            code="T4_SCENARIO_RUN_INVALID",
        )
    _sha256(run.control_receipt.event_hash_sha256, "event_hash_sha256")


def _row_item(row: object, index: int, label: str) -> object:
    if (
        not isinstance(row, Sequence)
        or isinstance(row, (str, bytes, bytearray))
        or len(row) <= index
    ):
        raise ScenarioEvidenceError(
            f"Scenario database {label} is invalid",
            code="T4_SCENARIO_DATABASE_INVALID",
        )
    return row[index]


def _row_text(row: object, index: int, label: str) -> str:
    value = _row_item(row, index, label)
    if not isinstance(value, str):
        raise ScenarioEvidenceError(
            f"Scenario database {label} is invalid",
            code="T4_SCENARIO_DATABASE_INVALID",
        )
    return value


def _row_uuid(row: object, index: int, label: str) -> str:
    value = _row_item(row, index, label)
    return _uuid(str(value), label)


def _row_positive_int(row: object, index: int, label: str) -> int:
    value = _row_item(row, index, label)
    if type(value) is not int or value <= 0:
        raise ScenarioEvidenceError(
            f"Scenario database {label} is invalid",
            code="T4_SCENARIO_DATABASE_INVALID",
        )
    return value


def _row_utc(row: object, index: int, label: str) -> datetime:
    value = _row_item(row, index, label)
    if not isinstance(value, datetime):
        raise ScenarioEvidenceError(
            f"Scenario database {label} is invalid",
            code="T4_SCENARIO_DATABASE_INVALID",
        )
    return _utc(value, label)


def _scenario_result(row: object) -> ScenarioTrialResult:
    result = ScenarioTrialResult(
        trial_id=_row_uuid(row, 0, "trial_id"),
        campaign_id=_row_uuid(row, 1, "campaign_id"),
        scenario_code=_scenario(_row_text(row, 2, "scenario_code")),
        outcome=_row_text(row, 3, "outcome"),
        completed_at=_row_utc(row, 4, "completed_at"),
        content_hash_sha256=_sha256(
            _row_text(row, 5, "content_hash"),
            "content_hash",
        ),
    )
    result.validate()
    return result


class _StrictObject(dict[str, object]):
    pass


JsonBytes: TypeAlias = bytes | bytearray


def _load_json(value: JsonBytes, label: str) -> object:
    def object_pairs(pairs: list[tuple[str, object]]) -> _StrictObject:
        result = _StrictObject()
        for key, item in pairs:
            if not isinstance(key, str) or key in result:
                raise ValueError
            result[key] = item
        return result

    try:
        return json.loads(
            bytes(value),
            object_pairs_hook=object_pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise ScenarioEvidenceError(
            f"{label.capitalize()} JSON is invalid",
            code="T4_EVIDENCE_JSON_INVALID",
        ) from None


def _object(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or not all(
        isinstance(key, str) for key in value
    ):
        raise ScenarioEvidenceError(
            f"{label.capitalize()} must be an object",
            code="T4_EVIDENCE_JSON_INVALID",
        )
    return cast(Mapping[str, object], value)


def _exact_keys(
    item: Mapping[str, object],
    expected: frozenset[str],
    label: str,
) -> None:
    if frozenset(item) != expected:
        raise ScenarioEvidenceError(
            f"{label.capitalize()} fields are invalid",
            code="T4_EVIDENCE_FIELDS_INVALID",
        )


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _decode_base64(value: object, field: str, *, maximum_bytes: int) -> bytes:
    if not isinstance(value, str) or not value or len(value) > maximum_bytes * 2:
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_ENCODING_INVALID",
        )
    try:
        decoded = base64.b64decode(value.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error):
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_ENCODING_INVALID",
        ) from None
    if not decoded or len(decoded) > maximum_bytes:
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_ENCODING_INVALID",
        )
    return decoded


def _token(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not 32 <= len(value) <= 256
        or any(character.isspace() or _is_control(character) for character in value)
    ):
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_OBSERVATION_TOKEN_INVALID",
        )
    return value


def _uuid(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_ID_INVALID",
        )
    try:
        parsed = UUID(value)
    except ValueError:
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_ID_INVALID",
        ) from None
    if str(parsed) != value:
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_ID_INVALID",
        )
    return value


def _optional_uuid(value: object, field: str) -> str | None:
    return None if value is None else _uuid(value, field)


def _sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_HASH_INVALID",
        )
    return value


def _optional_sha256(value: object, field: str) -> str | None:
    return None if value is None else _sha256(value, field)


def _utc_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or _UTC_MICROSECOND.fullmatch(value) is None:
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_TIME_INVALID",
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_TIME_INVALID",
        ) from None
    return _utc(parsed, field)


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_TIME_INVALID",
        )
    return value


def _nonnegative_int(value: object, field: str, *, positive: bool = False) -> int:
    if type(value) is not int or value < int(positive):
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_COUNTER_INVALID",
        )
    return value


def _environment(value: object) -> str:
    if value not in {"live_t4", "t4_simulator"}:
        raise ScenarioEvidenceError(
            "Bridge evidence environment is invalid",
            code="T4_EVIDENCE_ENVIRONMENT_INVALID",
        )
    return value


def _optional_scope(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > 256:
        raise _event_invalid()
    try:
        symbol, interval = value.rsplit(":", 1)
        minutes = int(interval[:-1]) if interval.endswith("m") else -1
    except (TypeError, ValueError):
        raise _event_invalid() from None
    if symbol not in {"BTC/USD", "ETH/USD"} or minutes not in _SAFE_SCOPE_INTERVALS:
        raise _event_invalid()
    return value


def _required_scope(value: object) -> str:
    try:
        scope = _optional_scope(value)
    except ScenarioEvidenceError:
        scope = None
    if scope is None:
        raise ScenarioEvidenceError(
            "Scenario scope is invalid",
            code="T4_SCENARIO_SCOPE_INVALID",
        )
    return scope


def _scenario(value: object) -> str:
    if not isinstance(value, str) or value not in MANDATORY_SCENARIOS:
        raise ScenarioEvidenceError(
            "Scenario code is invalid",
            code="T4_SCENARIO_CODE_INVALID",
        )
    return value


def _optional_scenario(value: object) -> str | None:
    return None if value is None else _scenario(value)


def _optional_text(value: object, field: str, *, maximum: int) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or any(_is_control(character) for character in value)
    ):
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_PAYLOAD_INVALID",
        )
    return value


def _optional_opaque_id(value: object, field: str, *, maximum: int) -> str | None:
    result = _optional_text(value, field, maximum=maximum)
    if result is not None and result != result.strip():
        raise ScenarioEvidenceError(
            f"{field} is invalid",
            code="T4_EVIDENCE_PAYLOAD_INVALID",
        )
    return result


def _finite_range(value: object, lower: float, upper: float) -> bool:
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and lower <= float(value) <= upper
    )


def _is_control(character: str) -> bool:
    codepoint = ord(character)
    return codepoint < 32 or 127 <= codepoint <= 159


def _event_invalid() -> ScenarioEvidenceError:
    return ScenarioEvidenceError(
        "Bridge event semantics are invalid",
        code="T4_EVIDENCE_EVENT_INVALID",
    )

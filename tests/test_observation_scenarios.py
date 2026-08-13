from __future__ import annotations

import base64
import hashlib
import json
import unittest
from dataclasses import dataclass
from datetime import UTC, datetime
from email.message import Message
from typing import Any
from unittest.mock import patch

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from crypto_agent.observation_scenarios import (
    BRIDGE_EVENT_REASONS,
    BridgeEventCursor,
    BridgeEventPage,
    BridgeEvidenceVerifier,
    PostgresScenarioEvidenceRepository,
    ReplayAttestationEvidence,
    ScenarioCheckpointReceipt,
    ScenarioControlReceipt,
    ScenarioEvidenceError,
    ScenarioEvidenceRunner,
    ScenarioTrialEvidence,
    ScenarioTrialHandle,
    ScenarioTrialResult,
    T4BridgeObservationClient,
)

CAMPAIGN_ID = "7bf3c831-ae63-4d32-b750-1d694c1de236"
ACTION_REQUEST_ID = "cb133687-a5ba-48ad-bfe1-0a5617c64f0a"
TRIAL_ID = "6ef48452-c5bd-4b12-911f-c91e8dc6bab7"
OLD_BOOT_ID = "83f9aaf3-61ee-4ae5-91c0-dcfc0a9fab0d"
NEW_BOOT_ID = "8b730112-297c-4021-ac3f-56358b03b92d"
NOW = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)


class _RepositoryCursor:
    def __init__(self, rows: list[object]) -> None:
        self.rows = rows
        self.executions: list[tuple[str, object]] = []

    def execute(self, query: str, params: object = None) -> None:
        self.executions.append((query, params))

    def fetchone(self) -> object:
        return self.rows.pop(0)

    def close(self) -> None:
        return None


class _RepositoryConnection:
    def __init__(self, cursor: _RepositoryCursor) -> None:
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _RepositoryCursor:
        return self._cursor

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        return None

_CLAIM_KEYS = (
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


class EvidenceFactory:
    def __init__(self) -> None:
        self.private_key = ec.generate_private_key(ec.SECP256R1())
        self.spki = self.private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        self.spki_base64 = base64.b64encode(self.spki).decode("ascii")
        self.fingerprint = hashlib.sha256(self.spki).hexdigest()

    def event(
        self,
        sequence_no: int,
        previous_event_hash: str | None,
        event_type: str,
        *,
        boot_id: str = OLD_BOOT_ID,
        campaign_id: str | None = CAMPAIGN_ID,
        scenario_code: str | None = None,
        action_request_id: str | None = None,
        payload: dict[str, str] | None = None,
        scope_key: str | None = None,
        reason_code: str | None = None,
        event_at: str | None = None,
    ) -> dict[str, Any]:
        payload = {} if payload is None else payload
        payload_bytes = _json_bytes(payload)
        values: dict[str, Any] = {
            "schema_version": 1,
            "event_id": f"00000000-0000-4000-8000-{sequence_no:012d}",
            "sequence_no": sequence_no,
            "event_at": event_at
            or f"2026-08-13T10:00:{sequence_no % 60:02d}.000000Z",
            "event_type": event_type,
            "reason_code": reason_code or BRIDGE_EVENT_REASONS[event_type],
            "campaign_id": campaign_id,
            "boot_id": boot_id,
            "session_generation": 1,
            "reconnect_count": 0,
            "environment": "live_t4",
            "bridge_schema_version": 5,
            "read_only": True,
            "order_routes_exposed": False,
            "scope_key": scope_key,
            "scenario_code": scenario_code,
            "action_request_id": action_request_id,
            "previous_event_hash": previous_event_hash,
            "payload": payload,
            "payload_hash_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        }
        claims = {key: values[key] for key in _CLAIM_KEYS}
        canonical = _json_bytes(claims)
        signature = self.private_key.sign(canonical, ec.ECDSA(hashes.SHA256()))
        return {
            **claims,
            "event_hash_sha256": hashlib.sha256(canonical).hexdigest(),
            "canonical_payload_base64": base64.b64encode(canonical).decode("ascii"),
            "signature_algorithm": "ecdsa-p256-sha256-der",
            "evidence_key_fingerprint_sha256": self.fingerprint,
            "signature_base64": base64.b64encode(signature).decode("ascii"),
        }

    def chain(self, specs: list[dict[str, Any]]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        previous: str | None = None
        for sequence_no, spec in enumerate(specs, start=1):
            item = self.event(sequence_no, previous, **spec)
            result.append(item)
            previous = item["event_hash_sha256"]
        return result

    def page(
        self,
        events: list[dict[str, Any]],
        *,
        outer_boot_id: str = OLD_BOOT_ID,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "boot_id": outer_boot_id,
            "environment": "live_t4",
            "read_only": True,
            "order_routes_exposed": False,
            "signature_algorithm": "ecdsa-p256-sha256-der",
            "evidence_key_fingerprint_sha256": self.fingerprint,
            "public_key_spki_base64": self.spki_base64,
            "events": events,
        }

    def verifier(self, *, explicit_key: bool = False) -> BridgeEvidenceVerifier:
        return BridgeEvidenceVerifier(
            evidence_key_fingerprint_sha256=self.fingerprint,
            evidence_public_key_spki_base64=(
                self.spki_base64 if explicit_key else None
            ),
        )


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


class BridgeEvidenceVerifierTests(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = EvidenceFactory()

    def test_fetches_key_from_page_but_requires_frozen_fingerprint(self) -> None:
        events = self.factory.chain(
            [{"event_type": "bridge_started", "campaign_id": None}]
        )

        page = self.factory.verifier().verify_page(self.factory.page(events))

        self.assertEqual(page.events[0].event_type, "bridge_started")
        self.assertEqual(page.public_key_spki_der, self.factory.spki)

    def test_global_chain_may_cross_boots_and_outer_boot_is_current(self) -> None:
        events = self.factory.chain(
            [
                {
                    "event_type": "bridge_stopping",
                    "scenario_code": "bridge_restart",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "restart", "control_step": "consumed"},
                },
                {
                    "event_type": "bridge_started",
                    "boot_id": NEW_BOOT_ID,
                    "campaign_id": None,
                },
            ]
        )

        page = self.factory.verifier().verify_page(
            self.factory.page(events, outer_boot_id=NEW_BOOT_ID)
        )

        self.assertEqual([item.boot_id for item in page.events], [OLD_BOOT_ID, NEW_BOOT_ID])
        self.assertEqual(page.boot_id, NEW_BOOT_ID)

    def test_cursor_verifies_sequence_and_hash_without_boot_equality(self) -> None:
        events = self.factory.chain(
            [
                {"event_type": "bridge_started", "campaign_id": None},
                {
                    "event_type": "bridge_started",
                    "boot_id": NEW_BOOT_ID,
                    "campaign_id": None,
                },
            ]
        )
        verifier = self.factory.verifier()
        first = verifier.verify_page(self.factory.page(events[:1]))

        second = verifier.verify_page(
            self.factory.page(events[1:], outer_boot_id=NEW_BOOT_ID),
            cursor=first.cursor,
        )

        self.assertEqual(second.events[0].sequence_no, 2)
        self.assertEqual(second.events[0].boot_id, NEW_BOOT_ID)

    def test_rejects_projection_tampering(self) -> None:
        events = self.factory.chain(
            [{"event_type": "bridge_started", "campaign_id": None}]
        )
        payload = self.factory.page(events)
        payload["events"][0]["reason_code"] = "READY"

        with self.assertRaisesRegex(
            ScenarioEvidenceError, "differs from its projection"
        ):
            self.factory.verifier().verify_page(payload)

    def test_rejects_validly_signed_but_wrong_reason_pair(self) -> None:
        events = self.factory.chain(
            [
                {
                    "event_type": "bridge_started",
                    "campaign_id": None,
                    "reason_code": "T4_SESSION_CONNECTING",
                }
            ]
        )

        with self.assertRaisesRegex(ScenarioEvidenceError, "safe reason"):
            self.factory.verifier().verify_page(self.factory.page(events))

    def test_rejects_seven_digit_timestamp_before_postgres_projection(self) -> None:
        events = self.factory.chain(
            [
                {
                    "event_type": "bridge_started",
                    "campaign_id": None,
                    "event_at": "2026-08-13T10:00:01.1234567Z",
                }
            ]
        )

        with self.assertRaisesRegex(ScenarioEvidenceError, "event_at is invalid"):
            self.factory.verifier().verify_page(self.factory.page(events))

    def test_rejects_broken_hash_chain(self) -> None:
        events = self.factory.chain(
            [
                {"event_type": "bridge_started", "campaign_id": None},
                {"event_type": "session_connecting", "campaign_id": None},
            ]
        )
        events[1] = self.factory.event(
            2,
            "f" * 64,
            "session_connecting",
            campaign_id=None,
        )

        with self.assertRaisesRegex(ScenarioEvidenceError, "hash chain"):
            self.factory.verifier().verify_page(self.factory.page(events))

    def test_rejects_key_not_matching_frozen_fingerprint(self) -> None:
        other = EvidenceFactory()
        events = other.chain(
            [{"event_type": "bridge_started", "campaign_id": None}]
        )
        verifier = BridgeEvidenceVerifier(
            evidence_key_fingerprint_sha256=self.factory.fingerprint
        )
        payload = other.page(events)
        payload["evidence_key_fingerprint_sha256"] = self.factory.fingerprint

        with self.assertRaisesRegex(ScenarioEvidenceError, "frozen value"):
            verifier.verify_page(payload)

    def test_accepts_roll_without_action_request_and_correlated_empty_cache_payload(self) -> None:
        events = self.factory.chain(
            [
                {
                    "event_type": "contract_roll_observed",
                    "scenario_code": "roll_transition",
                    "scope_key": "BTC/USD:240m",
                    "payload": {
                        "from_market_id": "old-market",
                        "to_market_id": "new-market",
                    },
                },
                {
                    "event_type": "cache_cleared",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {},
                },
            ]
        )

        page = self.factory.verifier().verify_page(self.factory.page(events))

        self.assertIsNone(page.events[0].action_request_id)
        self.assertEqual(page.events[1].scenario_code, "reconnect")

    def test_campaign_checkpoint_is_exact_and_campaign_bound(self) -> None:
        events = self.factory.chain(
            [
                {
                    "event_type": "campaign_checkpoint",
                    "action_request_id": ACTION_REQUEST_ID,
                    "payload": {
                        "action": "checkpoint",
                        "control_step": "consumed",
                    },
                }
            ]
        )

        page = self.factory.verifier().verify_page(self.factory.page(events))

        checkpoint = page.events[0]
        self.assertEqual(checkpoint.campaign_id, CAMPAIGN_ID)
        self.assertIsNone(checkpoint.scope_key)
        self.assertIsNone(checkpoint.scenario_code)


class _Response:
    def __init__(self, payload: object, status: int) -> None:
        self.status = status
        self.body = _json_bytes(payload)
        self.headers = Message()
        self.headers["Content-Type"] = "application/json"

    def read(self, amount: int) -> bytes:
        return self.body[:amount]

    def close(self) -> None:
        pass


class _Opener:
    def __init__(self, responses: list[_Response]) -> None:
        self.responses = responses
        self.requests: list[Any] = []

    def open(self, request: Any, *, timeout: float) -> _Response:
        self.requests.append(request)
        return self.responses.pop(0)


class T4BridgeObservationClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = EvidenceFactory()
        self.bridge_token = "b" * 32
        self.control_token = "c" * 32

    def client(self) -> T4BridgeObservationClient:
        return T4BridgeObservationClient(
            bridge_url="http://127.0.0.1:8080",
            bridge_token=self.bridge_token,
            observation_control_token=self.control_token,
            evidence_key_fingerprint_sha256=self.factory.fingerprint,
        )

    def test_rejects_non_loopback_origin(self) -> None:
        with self.assertRaisesRegex(ScenarioEvidenceError, "loopback"):
            T4BridgeObservationClient(
                bridge_url="http://example.com:8080",
                bridge_token=self.bridge_token,
                observation_control_token=self.control_token,
                evidence_key_fingerprint_sha256=self.factory.fingerprint,
            )

    def test_get_uses_only_after_sequence_and_limit(self) -> None:
        events = self.factory.chain(
            [{"event_type": "bridge_started", "campaign_id": None}]
        )
        opener = _Opener([_Response(self.factory.page(events), 200)])

        with patch(
            "crypto_agent.observation_scenarios.build_opener",
            return_value=opener,
        ):
            self.client().list_events(limit=100)

        request = opener.requests[0]
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:8080/v1/observation/events?limit=100",
        )
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(headers["x-crypto-agent-bridge-token"], self.bridge_token)
        self.assertNotIn("x-crypto-agent-observation-control-token", headers)

    def test_post_uses_action_path_exact_body_and_separate_control_token(self) -> None:
        receipt = {
            "schema_version": 1,
            "status": "accepted",
            "action": "reconnect",
            "campaign_id": CAMPAIGN_ID,
            "action_request_id": ACTION_REQUEST_ID,
            "scope_key": "BTC/USD:240m",
            "event_sequence_no": 9,
            "event_hash_sha256": "a" * 64,
            "read_only": True,
            "execution_enabled": False,
        }
        opener = _Opener([_Response(receipt, 202)])

        with patch(
            "crypto_agent.observation_scenarios.build_opener",
            return_value=opener,
        ):
            result = self.client().trigger(
                "reconnect",
                campaign_id=CAMPAIGN_ID,
                action_request_id=ACTION_REQUEST_ID,
                scope_key="BTC/USD:240m",
            )

        request = opener.requests[0]
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:8080/v1/observation/control/reconnect",
        )
        self.assertEqual(
            request.data,
            _json_bytes(
                {
                    "campaign_id": CAMPAIGN_ID,
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                }
            ),
        )
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(
            headers["x-crypto-agent-observation-control-token"],
            self.control_token,
        )
        self.assertEqual(result.event_hash_sha256, "a" * 64)
        self.assertEqual(result.scope_key, "BTC/USD:240m")

    def test_checkpoint_uses_bridge_token_without_fault_control_token(self) -> None:
        receipt = {
            "schema_version": 1,
            "status": "recorded",
            "campaign_id": CAMPAIGN_ID,
            "action_request_id": ACTION_REQUEST_ID,
            "event_sequence_no": 17,
            "event_hash_sha256": "f" * 64,
            "read_only": True,
            "execution_enabled": False,
        }
        opener = _Opener([_Response(receipt, 201)])

        with patch(
            "crypto_agent.observation_scenarios.build_opener",
            return_value=opener,
        ):
            result = self.client().checkpoint(
                campaign_id=CAMPAIGN_ID,
                action_request_id=ACTION_REQUEST_ID,
            )

        request = opener.requests[0]
        self.assertEqual(
            request.full_url,
            "http://127.0.0.1:8080/v1/observation/checkpoint",
        )
        self.assertEqual(
            request.data,
            _json_bytes(
                {
                    "campaign_id": CAMPAIGN_ID,
                    "action_request_id": ACTION_REQUEST_ID,
                }
            ),
        )
        headers = {key.lower(): value for key, value in request.header_items()}
        self.assertEqual(headers["x-crypto-agent-bridge-token"], self.bridge_token)
        self.assertNotIn("x-crypto-agent-observation-control-token", headers)
        self.assertEqual(result.event_sequence_no, 17)

    def test_control_post_rejects_missing_scope_before_network(self) -> None:
        opener = _Opener([])
        with (
            patch(
                "crypto_agent.observation_scenarios.build_opener",
                return_value=opener,
            ),
            self.assertRaises(ScenarioEvidenceError) as raised,
        ):
            self.client().trigger(
                "missing_data",
                campaign_id=CAMPAIGN_ID,
                action_request_id=ACTION_REQUEST_ID,
                scope_key="",
            )

        self.assertEqual(raised.exception.code, "T4_SCENARIO_SCOPE_INVALID")
        self.assertEqual(opener.requests, [])

    def test_read_only_event_client_does_not_require_control_token(self) -> None:
        client = T4BridgeObservationClient(
            bridge_url="http://127.0.0.1:8080",
            bridge_token=self.bridge_token,
            evidence_key_fingerprint_sha256=self.factory.fingerprint,
        )

        with self.assertRaisesRegex(ScenarioEvidenceError, "not configured") as raised:
            client.trigger(
                "reconnect",
                campaign_id=CAMPAIGN_ID,
                action_request_id=ACTION_REQUEST_ID,
                scope_key="BTC/USD:240m",
            )

        self.assertEqual(
            raised.exception.code,
            "T4_OBSERVATION_CONTROL_NOT_CONFIGURED",
        )


@dataclass
class _FakeClient:
    baseline: BridgeEventPage
    evidence_page: BridgeEventPage
    receipt: ScenarioControlReceipt
    calls: int = 0
    trigger_calls: int = 0

    def list_events(
        self,
        *,
        cursor: BridgeEventCursor | None = None,
        limit: int = 512,
    ) -> BridgeEventPage:
        self.calls += 1
        return self.baseline if self.calls == 1 else self.evidence_page

    def trigger(
        self,
        action: str,
        *,
        campaign_id: str,
        action_request_id: str,
        scope_key: str,
    ) -> ScenarioControlReceipt:
        self.trigger_calls += 1
        if (
            action != self.receipt.action
            or campaign_id != self.receipt.campaign_id
            or action_request_id != self.receipt.action_request_id
            or scope_key != "BTC/USD:240m"
        ):
            raise AssertionError("mismatched control request")
        return self.receipt

    def checkpoint(
        self,
        *,
        campaign_id: str,
        action_request_id: str,
    ) -> ScenarioCheckpointReceipt:
        del campaign_id, action_request_id
        raise AssertionError("unexpected checkpoint request")


class _EmptyPollingClient(_FakeClient):
    def list_events(
        self,
        *,
        cursor: BridgeEventCursor | None = None,
        limit: int = 512,
    ) -> BridgeEventPage:
        self.calls += 1
        if self.calls == 1:
            return self.baseline
        if self.calls < 4:
            return BridgeEventPage(
                boot_id=self.evidence_page.boot_id,
                environment=self.evidence_page.environment,
                evidence_key_fingerprint_sha256=(
                    self.evidence_page.evidence_key_fingerprint_sha256
                ),
                public_key_spki_der=self.evidence_page.public_key_spki_der,
                events=(),
            )
        return self.evidence_page


@dataclass
class _CheckpointClient:
    page: BridgeEventPage
    receipt: ScenarioCheckpointReceipt
    checkpoint_calls: int = 0

    def list_events(
        self,
        *,
        cursor: BridgeEventCursor | None = None,
        limit: int = 512,
    ) -> BridgeEventPage:
        del cursor, limit
        return self.page

    def checkpoint(
        self,
        *,
        campaign_id: str,
        action_request_id: str,
    ) -> ScenarioCheckpointReceipt:
        self.checkpoint_calls += 1
        if (
            campaign_id != self.receipt.campaign_id
            or action_request_id != self.receipt.action_request_id
        ):
            raise AssertionError("mismatched checkpoint request")
        return self.receipt

    def trigger(
        self,
        action: str,
        *,
        campaign_id: str,
        action_request_id: str,
        scope_key: str,
    ) -> ScenarioControlReceipt:
        del action, campaign_id, action_request_id, scope_key
        raise AssertionError("unexpected control request")


class _FakeRepository:
    def __init__(self, scenario_code: str) -> None:
        self.scenario_code = scenario_code
        self.pages: list[BridgeEventPage] = []
        self.evidence: ScenarioTrialEvidence | None = None

    def store_bridge_event_page(
        self,
        *,
        campaign_id: str,
        page: BridgeEventPage,
    ) -> None:
        self.pages.append(page)

    def begin_trial(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
        action_request_id: str | None,
        scope_key: str,
    ) -> ScenarioTrialHandle:
        return ScenarioTrialHandle(
            trial_id=TRIAL_ID,
            campaign_id=campaign_id,
            scenario_code=scenario_code,
            action_request_id=action_request_id,
            scope_key=scope_key,
            started_at=NOW,
        )

    def submit_trial_evidence(
        self,
        evidence: ScenarioTrialEvidence,
    ) -> ScenarioTrialResult:
        self.evidence = evidence
        return ScenarioTrialResult(
            trial_id=evidence.trial_id,
            campaign_id=evidence.campaign_id,
            scenario_code=evidence.scenario_code,
            outcome="pass",
            completed_at=NOW,
            content_hash_sha256="d" * 64,
        )

    def verify_passive_trial(
        self,
        *,
        campaign_id: str,
        scenario_code: str,
    ) -> ScenarioTrialResult:
        return ScenarioTrialResult(
            trial_id=TRIAL_ID,
            campaign_id=campaign_id,
            scenario_code=scenario_code,
            outcome="pass",
            completed_at=NOW,
            content_hash_sha256="e" * 64,
        )


class ScenarioEvidenceRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = EvidenceFactory()

    def _verified_pages(
        self, specs: list[dict[str, Any]], *, outer_boot_id: str = OLD_BOOT_ID
    ) -> tuple[BridgeEventPage, BridgeEventPage, list[dict[str, Any]]]:
        raw = self.factory.chain(specs)
        verifier = self.factory.verifier()
        baseline = verifier.verify_page(self.factory.page(raw[:1]))
        evidence = verifier.verify_page(
            self.factory.page(raw[1:], outer_boot_id=outer_boot_id),
            cursor=baseline.cursor,
        )
        return baseline, evidence, raw

    def test_checkpoint_receipt_must_match_verified_signed_journal_event(self) -> None:
        raw = self.factory.chain(
            [
                {"event_type": "bridge_started"},
                {
                    "event_type": "campaign_checkpoint",
                    "action_request_id": ACTION_REQUEST_ID,
                    "payload": {
                        "action": "checkpoint",
                        "control_step": "consumed",
                    },
                },
            ]
        )
        page = self.factory.verifier().verify_page(self.factory.page(raw))
        receipt = ScenarioCheckpointReceipt(
            campaign_id=CAMPAIGN_ID,
            action_request_id=ACTION_REQUEST_ID,
            event_sequence_no=2,
            event_hash_sha256=raw[1]["event_hash_sha256"],
        )
        client = _CheckpointClient(page, receipt)
        repository = _FakeRepository("roll_transition")

        result = ScenarioEvidenceRunner(client, repository).record_campaign_checkpoint(
            campaign_id=CAMPAIGN_ID,
            action_request_id=ACTION_REQUEST_ID,
        )

        self.assertEqual(result, receipt)
        self.assertEqual(client.checkpoint_calls, 1)
        self.assertEqual(repository.pages, [page])

    def test_reconnect_collects_unlinked_recovery_fragment_without_pass_input(self) -> None:
        baseline, evidence_page, raw = self._verified_pages(
            [
                {"event_type": "bridge_started", "campaign_id": None},
                {
                    "event_type": "control_requested",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "reconnect", "control_step": "requested"},
                },
                {
                    "event_type": "cache_cleared",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                },
                {
                    "event_type": "control_applied",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "reconnect", "control_step": "applied"},
                },
                {"event_type": "session_disconnected"},
                {"event_type": "cache_cleared"},
                {"event_type": "session_connecting"},
                {"event_type": "session_authenticated"},
                {
                    "event_type": "session_ready",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "reconnect", "control_step": "consumed"},
                },
            ]
        )
        receipt = ScenarioControlReceipt(
            action="reconnect",
            campaign_id=CAMPAIGN_ID,
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
            event_sequence_no=4,
            event_hash_sha256=raw[3]["event_hash_sha256"],
        )
        repository = _FakeRepository("reconnect")
        runner = ScenarioEvidenceRunner(
            _FakeClient(baseline, evidence_page, receipt), repository
        )

        run = runner.start_controlled(
            campaign_id=CAMPAIGN_ID,
            scenario_code="reconnect",
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
        )
        result = runner.collect_controlled(run, timeout_seconds=1, poll_seconds=0.01)

        self.assertEqual(result.outcome, "pass")
        self.assertIsNotNone(repository.evidence)
        assert repository.evidence is not None
        self.assertFalse(hasattr(repository.evidence, "outcome"))
        self.assertIn(raw[6]["event_hash_sha256"], repository.evidence.bridge_event_hashes)

    def test_lost_control_response_resumes_from_signed_receipt_without_retrigger(
        self,
    ) -> None:
        raw = self.factory.chain(
            [
                {"event_type": "bridge_started", "campaign_id": None},
                {
                    "event_type": "control_requested",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "reconnect", "control_step": "requested"},
                },
                {
                    "event_type": "cache_cleared",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                },
                {
                    "event_type": "control_applied",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "reconnect", "control_step": "applied"},
                },
                {"event_type": "session_disconnected", "campaign_id": None},
                {"event_type": "cache_cleared", "campaign_id": None},
                {"event_type": "session_connecting", "campaign_id": None},
                {"event_type": "session_authenticated", "campaign_id": None},
                {
                    "event_type": "session_ready",
                    "scenario_code": "reconnect",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "reconnect", "control_step": "consumed"},
                },
            ]
        )
        page = self.factory.verifier().verify_page(self.factory.page(raw))
        empty = BridgeEventPage(
            boot_id=page.boot_id,
            environment=page.environment,
            evidence_key_fingerprint_sha256=page.evidence_key_fingerprint_sha256,
            public_key_spki_der=page.public_key_spki_der,
            events=(),
        )
        receipt = ScenarioControlReceipt(
            action="reconnect",
            campaign_id=CAMPAIGN_ID,
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
            event_sequence_no=4,
            event_hash_sha256=raw[3]["event_hash_sha256"],
        )
        client = _FakeClient(page, empty, receipt)
        repository = _FakeRepository("reconnect")
        runner = ScenarioEvidenceRunner(client, repository)
        exercise = unittest.mock.Mock()

        result = runner.run_controlled(
            campaign_id=CAMPAIGN_ID,
            scenario_code="reconnect",
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
            exercise=exercise,
            timeout_seconds=1,
            poll_seconds=0.01,
        )

        self.assertEqual(result.outcome, "pass")
        self.assertEqual(client.trigger_calls, 0)
        exercise.assert_not_called()

    def test_empty_poll_pages_are_not_persisted_before_terminal_evidence(self) -> None:
        baseline, evidence_page, raw = self._verified_pages(
            [
                {"event_type": "bridge_started", "campaign_id": None},
                {
                    "event_type": "control_requested",
                    "scenario_code": "missing_data",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "missing_data", "control_step": "requested"},
                },
                {
                    "event_type": "control_applied",
                    "scenario_code": "missing_data",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "missing_data", "control_step": "applied"},
                },
                {
                    "event_type": "missing_data_detected",
                    "scenario_code": "missing_data",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "missing_data", "control_step": "consumed"},
                },
            ]
        )
        receipt = ScenarioControlReceipt(
            action="missing_data",
            campaign_id=CAMPAIGN_ID,
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
            event_sequence_no=3,
            event_hash_sha256=raw[2]["event_hash_sha256"],
        )
        repository = _FakeRepository("missing_data")
        runner = ScenarioEvidenceRunner(
            _EmptyPollingClient(baseline, evidence_page, receipt), repository
        )

        run = runner.start_controlled(
            campaign_id=CAMPAIGN_ID,
            scenario_code="missing_data",
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
        )
        runner.collect_controlled(run, timeout_seconds=1, poll_seconds=0.01)

        self.assertEqual(repository.pages, [baseline, evidence_page])

    def test_restart_accepts_unlinked_events_from_new_boot(self) -> None:
        baseline, evidence_page, raw = self._verified_pages(
            [
                {"event_type": "bridge_started", "campaign_id": None},
                {
                    "event_type": "control_requested",
                    "scenario_code": "bridge_restart",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "restart", "control_step": "requested"},
                },
                {
                    "event_type": "control_applied",
                    "scenario_code": "bridge_restart",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "restart", "control_step": "applied"},
                },
                {
                    "event_type": "bridge_stopping",
                    "scenario_code": "bridge_restart",
                    "action_request_id": ACTION_REQUEST_ID,
                    "scope_key": "BTC/USD:240m",
                    "payload": {"action": "restart", "control_step": "consumed"},
                },
                {
                    "event_type": "bridge_started",
                    "boot_id": NEW_BOOT_ID,
                    "campaign_id": None,
                },
                {
                    "event_type": "session_connecting",
                    "boot_id": NEW_BOOT_ID,
                    "campaign_id": None,
                },
                {
                    "event_type": "session_authenticated",
                    "boot_id": NEW_BOOT_ID,
                    "campaign_id": None,
                },
                {
                    "event_type": "session_ready",
                    "boot_id": NEW_BOOT_ID,
                    "campaign_id": None,
                },
            ],
            outer_boot_id=NEW_BOOT_ID,
        )
        receipt = ScenarioControlReceipt(
            action="restart",
            campaign_id=CAMPAIGN_ID,
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
            event_sequence_no=3,
            event_hash_sha256=raw[2]["event_hash_sha256"],
        )
        repository = _FakeRepository("bridge_restart")
        runner = ScenarioEvidenceRunner(
            _FakeClient(baseline, evidence_page, receipt), repository
        )

        run = runner.start_controlled(
            campaign_id=CAMPAIGN_ID,
            scenario_code="bridge_restart",
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
        )
        runner.collect_controlled(run, timeout_seconds=1, poll_seconds=0.01)

        assert repository.evidence is not None
        self.assertEqual(
            repository.evidence.bridge_boot_ids,
            (OLD_BOOT_ID, NEW_BOOT_ID),
        )

    def test_passive_result_can_only_come_from_repository_verification(self) -> None:
        repository = _FakeRepository("roll_transition")
        runner = ScenarioEvidenceRunner(unittest.mock.Mock(), repository)

        result = runner.verify_passive(
            campaign_id=CAMPAIGN_ID,
            scenario_code="roll_transition",
        )

        self.assertEqual(result.outcome, "pass")

    def test_passive_roll_syncs_signed_journal_without_control(self) -> None:
        raw = self.factory.chain(
            [
                {"event_type": "bridge_started", "campaign_id": CAMPAIGN_ID},
                {
                    "event_type": "contract_roll_observed",
                    "scenario_code": "roll_transition",
                    "scope_key": "BTC/USD:240m",
                    "payload": {
                        "from_market_id": "old-market",
                        "to_market_id": "new-market",
                    },
                },
            ]
        )
        page = self.factory.verifier().verify_page(self.factory.page(raw))
        client = unittest.mock.Mock()
        client.list_events.return_value = page
        repository = _FakeRepository("roll_transition")
        runner = ScenarioEvidenceRunner(client, repository)

        stored = runner.sync_bridge_events(
            campaign_id=CAMPAIGN_ID,
            page_limit=512,
        )

        self.assertEqual(stored, 2)
        self.assertEqual(repository.pages, [page])
        self.assertFalse(client.trigger.called)


class PostgresScenarioEvidenceRepositoryTests(unittest.TestCase):
    def test_replay_attestation_uses_exact_typed_verifier_function(self) -> None:
        cursor = _RepositoryCursor([(41, "d" * 64)])
        connection = _RepositoryConnection(cursor)
        repository = PostgresScenarioEvidenceRepository(lambda: connection)
        evidence = ReplayAttestationEvidence(
            attestation_id="fa2b3d0b-bc3a-4388-84b8-c14b94572bba",
            campaign_id=CAMPAIGN_ID,
            scope_key="BTC/USD:240m",
            replay_as_of=NOW,
            replay_fingerprint_sha256="a" * 64,
            source_batch_ids=(7,),
            source_batch_hashes=("b" * 64,),
            provenance_sha256="c" * 64,
        )

        receipt = repository.record_replay_attestation(evidence)

        self.assertEqual(receipt.replay_attestation_id, 41)
        self.assertEqual(receipt.content_hash_sha256, "d" * 64)
        query, params = cursor.executions[0]
        self.assertIn("record_verified_t4_replay_attestation", query)
        self.assertIn("%s::crypto_agent.sha256_hex[]", query)
        self.assertEqual(
            params,
            (
                evidence.attestation_id,
                CAMPAIGN_ID,
                "BTC/USD:240m",
                NOW,
                "a" * 64,
                [7],
                ["b" * 64],
                "c" * 64,
            ),
        )

    def test_complete_submits_only_trial_and_verified_hash_references(self) -> None:
        cursor = _RepositoryCursor(
            [(TRIAL_ID, CAMPAIGN_ID, "rate_limit", "fail", NOW, "d" * 64)]
        )
        connection = _RepositoryConnection(cursor)
        repository = PostgresScenarioEvidenceRepository(lambda: connection)
        evidence = ScenarioTrialEvidence(
            trial_id=TRIAL_ID,
            campaign_id=CAMPAIGN_ID,
            scenario_code="rate_limit",
            action_request_id=ACTION_REQUEST_ID,
            bridge_event_hashes=("a" * 64, "b" * 64),
            bridge_boot_ids=(OLD_BOOT_ID,),
            control_receipt_event_hash="a" * 64,
        )

        result = repository.submit_trial_evidence(evidence)

        self.assertEqual(result.outcome, "fail")
        query, params = cursor.executions[0]
        self.assertIn("complete_t4_observation_scenario_trial", query)
        self.assertEqual(params, (TRIAL_ID, ["a" * 64, "b" * 64], "a" * 64))
        self.assertNotIn(NOW, params)
        self.assertEqual(connection.commits, 1)

    def test_register_key_checks_database_fingerprint(self) -> None:
        cursor = _RepositoryCursor([("0" * 64,)])
        repository = PostgresScenarioEvidenceRepository(
            lambda: _RepositoryConnection(cursor)
        )

        with self.assertRaisesRegex(
            ScenarioEvidenceError, "fingerprint does not match"
        ):
            repository.register_bridge_evidence_key(b"spki")


if __name__ == "__main__":
    unittest.main()

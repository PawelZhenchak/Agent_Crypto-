from __future__ import annotations

import os
import unittest
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

from starlette.requests import Request

from crypto_agent.evidence_verifier import (
    BeginRequest,
    CompleteRequest,
    EvidenceVerifierClient,
    EvidenceVerifierService,
    PassiveRequest,
    PostgresReplayEvidenceReader,
    ReplayCampaignPlan,
    ReplayEvidenceAttestor,
    _authorization_rejection,
    build_evidence_verifier_service,
    create_evidence_verifier_app,
)
from crypto_agent.observation_scenarios import (
    ReplayAttestationReceipt,
    ScenarioEvidenceError,
)
from crypto_agent.postgres import PostgresUnavailableError
from tests.db_fakes import FakeConnection, SQLStep
from tests.test_observation_scenarios import (
    ACTION_REQUEST_ID,
    CAMPAIGN_ID,
    TRIAL_ID,
    EvidenceFactory,
)

TOKEN = "v" * 32


class EvidenceVerifierTests(unittest.TestCase):
    def setUp(self) -> None:
        factory = EvidenceFactory()
        raw_events = factory.chain(
            [{"event_type": "bridge_started", "campaign_id": CAMPAIGN_ID}]
        )
        verified_page = factory.verifier().verify_page(factory.page(raw_events))
        self.page = verified_page
        self.bridge = SimpleNamespace(
            verifier=SimpleNamespace(
                evidence_key_fingerprint_sha256=factory.fingerprint
            ),
            list_events=Mock(side_effect=[verified_page]),
        )
        self.repository = SimpleNamespace(
            register_bridge_evidence_key=Mock(return_value=factory.fingerprint),
            store_bridge_event_page=Mock(),
        )

    def test_register_fetches_verified_journal_without_caller_event_input(self) -> None:
        service = EvidenceVerifierService(self.bridge, self.repository)

        result = service.register_campaign(CAMPAIGN_ID)

        self.assertEqual(result.campaign_id, CAMPAIGN_ID)
        self.assertEqual(result.verified_event_count, 1)
        self.bridge.list_events.assert_called_once_with(cursor=None, limit=512)
        self.repository.register_bridge_evidence_key.assert_called()

    def test_sync_stores_only_page_fetched_by_service(self) -> None:
        service = EvidenceVerifierService(self.bridge, self.repository)

        stored = service.sync_bridge_events(CAMPAIGN_ID)

        self.assertEqual(stored, 1)
        self.repository.store_bridge_event_page.assert_called_once_with(
            campaign_id=CAMPAIGN_ID,
            page=self.page,
        )

    def test_begin_requires_preflight_before_opening_new_db_request(self) -> None:
        self.bridge.list_events.side_effect = [self.page, self.page]
        repository = SimpleNamespace(
            register_bridge_evidence_key=self.repository.register_bridge_evidence_key,
            store_bridge_event_page=Mock(),
            begin_trial=Mock(),
        )
        service = EvidenceVerifierService(self.bridge, repository)
        request = BeginRequest(
            campaign_id=CAMPAIGN_ID,
            scenario_code="rate_limit",
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
            preflight_complete=False,
        )

        with self.assertRaisesRegex(
            ScenarioEvidenceError, "requires a runtime preflight"
        ) as raised:
            service.begin_controlled(request)

        self.assertEqual(raised.exception.code, "T4_SCENARIO_PREFLIGHT_REQUIRED")
        repository.begin_trial.assert_not_called()

    def test_complete_unknown_request_is_read_only_and_never_arms_control(self) -> None:
        bridge = SimpleNamespace(
            verifier=self.bridge.verifier,
            list_events=Mock(side_effect=[self.page]),
            trigger=Mock(),
        )
        repository = SimpleNamespace(
            register_bridge_evidence_key=Mock(),
            store_bridge_event_page=Mock(),
            begin_trial=Mock(),
            submit_trial_evidence=Mock(),
        )
        service = EvidenceVerifierService(bridge, repository)
        request = CompleteRequest(
            trial_id=TRIAL_ID,
            campaign_id=CAMPAIGN_ID,
            scenario_code="rate_limit",
            action_request_id=ACTION_REQUEST_ID,
            scope_key="BTC/USD:240m",
        )

        with (
            patch(
                "crypto_agent.evidence_verifier.ScenarioEvidenceRunner.start_controlled"
            ) as start_controlled,
            self.assertRaisesRegex(ScenarioEvidenceError, "absent") as raised,
        ):
            service.complete_controlled(request)

        self.assertEqual(raised.exception.code, "T4_SCENARIO_RECEIPT_MISSING")
        bridge.trigger.assert_not_called()
        start_controlled.assert_not_called()
        repository.register_bridge_evidence_key.assert_not_called()
        repository.store_bridge_event_page.assert_not_called()
        repository.begin_trial.assert_not_called()
        repository.submit_trial_evidence.assert_not_called()

    def test_routes_have_no_event_pass_or_signature_input_model(self) -> None:
        service = Mock(spec=EvidenceVerifierService)
        app = create_evidence_verifier_app(service, api_token=TOKEN)
        paths = {
            route.path
            for route in app.routes
            if route.path.startswith("/v1/evidence/")
        }
        self.assertEqual(
            paths,
            {
                "/v1/evidence/register",
                "/v1/evidence/sync",
                "/v1/evidence/begin",
                "/v1/evidence/complete",
                "/v1/evidence/passive",
            },
        )
        source = __import__(
            "pathlib"
        ).Path("src/crypto_agent/evidence_verifier.py").read_text(encoding="utf-8")
        request_models = source[
            source.index("class RegisterRequest") :
            source.index("class _DirectEvidenceRepository")
        ]
        self.assertNotIn("BridgeEventPage", request_models)
        self.assertNotIn("signature_verified", request_models)
        self.assertNotIn("outcome", request_models)

    def test_middleware_rejects_non_loopback_and_wrong_token(self) -> None:
        remote = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/v1/evidence/sync",
                "headers": [],
                "client": ("192.0.2.5", 5000),
                "server": ("127.0.0.1", 8791),
                "scheme": "http",
                "query_string": b"",
            }
        )
        self.assertEqual(_authorization_rejection(remote, TOKEN).status_code, 403)
        local = Request(
            {
                "type": "http",
                "method": "POST",
                "path": "/v1/evidence/sync",
                "headers": [(b"x-crypto-agent-evidence-token", b"wrong")],
                "client": ("127.0.0.1", 5000),
                "server": ("127.0.0.1", 8791),
                "scheme": "http",
                "query_string": b"",
            }
        )
        self.assertEqual(_authorization_rejection(local, TOKEN).status_code, 401)

    def test_client_rejects_non_loopback_and_has_no_direct_store_method(self) -> None:
        with self.assertRaisesRegex(ScenarioEvidenceError, "loopback"):
            EvidenceVerifierClient(
                verifier_url="http://example.com:8791",
                verifier_token=TOKEN,
            )
        client = EvidenceVerifierClient(
            verifier_url="http://127.0.0.1:8791",
            verifier_token=TOKEN,
        )
        self.assertFalse(hasattr(client, "store_bridge_event_page"))
        self.assertFalse(hasattr(client, "submit_trial_evidence"))

    def test_service_builder_rejects_shared_runtime_and_evidence_dsn(self) -> None:
        shared = "postgresql://shared:secret@127.0.0.1/crypto_agent"
        with (
            patch.dict(
                os.environ,
                {
                    "CRYPTO_AGENT_POSTGRES_DSN": shared,
                    "CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN": shared,
                    "CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN": (
                        "postgresql://reader:secret@127.0.0.1/crypto_agent"
                    ),
                    "CRYPTO_AGENT_EVIDENCE_VERIFIER_TOKEN": TOKEN,
                },
                clear=False,
            ),
            self.assertRaisesRegex(
                PostgresUnavailableError,
                "Runtime PostgreSQL must not be configured",
            ),
        ):
            build_evidence_verifier_service()

    def test_service_builder_rejects_same_write_and_reader_login(self) -> None:
        shared = "postgresql://evidence:secret@127.0.0.1/crypto_agent"
        with (
            patch.dict(
                os.environ,
                {
                    "CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN": shared,
                    "CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN": shared,
                },
                clear=True,
            ),
            self.assertRaisesRegex(
                PostgresUnavailableError,
                "reader PostgreSQL login must be separate",
            ),
        ):
            build_evidence_verifier_service()

    def test_runtime_cli_never_references_privileged_evidence_dsns(self) -> None:
        source = __import__("pathlib").Path(
            "src/crypto_agent/cli.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn("CRYPTO_AGENT_POSTGRES_EVIDENCE_DSN", source)
        self.assertNotIn("CRYPTO_AGENT_POSTGRES_EVIDENCE_READ_DSN", source)

    def test_replay_reader_requires_safe_non_owner_login_and_allows_late_attestation(
        self,
    ) -> None:
        planned_end = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
        for unsafe_boundary_check in range(11, 16):
            safety = [True] * 16
            safety[unsafe_boundary_check] = False
            unsafe = FakeConnection(
                [
                    SQLStep("SET TRANSACTION READ ONLY"),
                    SQLStep("SET LOCAL search_path TO pg_catalog"),
                    SQLStep("FROM pg_catalog.pg_roles", [tuple(safety)]),
                ]
            )
            reader = PostgresReplayEvidenceReader(lambda unsafe=unsafe: unsafe)

            with (
                self.subTest(unsafe_boundary_check=unsafe_boundary_check),
                self.assertRaisesRegex(
                    ScenarioEvidenceError, "login is not isolated"
                ) as raised,
            ):
                reader.campaign_plan(CAMPAIGN_ID)

            self.assertEqual(raised.exception.code, "T4_REPLAY_READER_UNSAFE")

        safe = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("SET LOCAL search_path TO pg_catalog"),
                SQLStep("FROM pg_catalog.pg_roles", [(True,) * 16]),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns",
                    [(
                        planned_end,
                        300,
                        ["BTC/USD:240m", "ETH/USD:240m"],
                        "live_t4",
                        True,
                        False,
                        planned_end + timedelta(days=3),
                    )],
                ),
            ]
        )
        plan = PostgresReplayEvidenceReader(lambda: safe).campaign_plan(
            CAMPAIGN_ID
        )
        self.assertEqual(plan.replay_as_of, planned_end)
        self.assertEqual(plan.scopes, ("BTC/USD:240m", "ETH/USD:240m"))

    def test_replay_passive_never_verifies_before_self_attestation(self) -> None:
        attestor = SimpleNamespace(
            attest_campaign=Mock(
                side_effect=ScenarioEvidenceError(
                    "replay failed",
                    code="T4_REPLAY_ATTESTATION_INVALID",
                )
            )
        )
        repository = SimpleNamespace(verify_passive_trial=Mock())
        service = EvidenceVerifierService(
            self.bridge,
            repository,
            replay_attestor=attestor,
        )

        with self.assertRaisesRegex(ScenarioEvidenceError, "replay failed"):
            service.verify_passive(
                PassiveRequest(
                    campaign_id=CAMPAIGN_ID,
                    scenario_code="replay_blocked",
                )
            )

        attestor.attest_campaign.assert_called_once_with(CAMPAIGN_ID)
        repository.verify_passive_trial.assert_not_called()

    def test_replay_attestor_runs_in_memory_and_records_derived_metadata(self) -> None:
        planned_end = datetime(2026, 8, 13, 10, 0, tzinfo=UTC)
        provenance = __import__("hashlib").sha256(
            ("t4-replay-provenance-v1\n7:" + "b" * 64 + "\n").encode("ascii")
        ).hexdigest()
        metadata = {
            "t4_replay": True,
            "t4_replay_as_of": planned_end.isoformat(),
            "t4_bridge_schema_version": 5,
            "t4_environment": "live_t4",
            "plus500_t4_source_attested": True,
            "external_delivery_eligible": False,
            "execution_enabled": False,
            "provider_error_code": None,
            "input_candle_count": 120,
            "t4_replay_fingerprint_sha256": "a" * 64,
            "t4_replay_source_batch_ids": [7],
            "t4_replay_source_batch_hashes": ["b" * 64],
            "t4_replay_provenance_sha256": provenance,
        }
        reader = SimpleNamespace(
            connection_factory=Mock(),
            campaign_plan=Mock(
                return_value=ReplayCampaignPlan(
                    campaign_id=CAMPAIGN_ID,
                    replay_as_of=planned_end,
                    scopes=("BTC/USD:240m",),
                )
            ),
        )
        repository = SimpleNamespace(
            record_replay_attestation=Mock(
                return_value=ReplayAttestationReceipt(41, "c" * 64)
            )
        )
        report = SimpleNamespace(as_of=planned_end, metadata=metadata)
        orchestrator = SimpleNamespace(analyze=Mock(return_value=report))

        with (
            patch("crypto_agent.orchestrator.ResearchOrchestrator", return_value=orchestrator),
            patch("crypto_agent.policy.RiskPolicy.load"),
            patch("crypto_agent.t4_ingest.T4IngestRepository"),
            patch("crypto_agent.t4_ingest.T4ReplayProvider"),
            patch("crypto_agent.monitoring._eligible_for_delivery", return_value=False),
        ):
            receipts = ReplayEvidenceAttestor(reader, repository).attest_campaign(
                CAMPAIGN_ID
            )

        self.assertEqual(receipts[0].replay_attestation_id, 41)
        orchestrator.analyze.assert_called_once_with(
            symbol="BTC/USD",
            interval_minutes=240,
            as_of=planned_end,
            limit=120,
        )
        evidence = repository.record_replay_attestation.call_args.args[0]
        self.assertEqual(evidence.campaign_id, CAMPAIGN_ID)
        self.assertEqual(evidence.source_batch_ids, (7,))
        self.assertEqual(evidence.provenance_sha256, provenance)


if __name__ == "__main__":
    unittest.main()

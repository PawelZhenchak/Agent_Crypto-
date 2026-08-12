from __future__ import annotations

import unittest
from datetime import timedelta
from datetime import UTC, datetime
from unittest.mock import patch

from crypto_agent.orchestrator import ResearchOrchestrator
from crypto_agent.policy import RiskPolicy
from crypto_agent.providers.t4 import Plus500T4Provider
from crypto_agent.t4_ingest import T4ReplayProvider, T4ReplayResult
from tests.helpers import PROJECT_ROOT
from tests.test_t4 import _BRIDGE_TOKEN, _Opener, _payload

AS_OF = datetime(2026, 8, 11, tzinfo=UTC)


class _StaticReplayRepository:
    def __init__(self, result: T4ReplayResult) -> None:
        self.result = result

    def replay(self, **kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        return self.result


class FuturesOrchestratorTests(unittest.TestCase):
    def test_complete_live_fixture_and_replay_are_deterministically_equivalent(self) -> None:
        live_provider = Plus500T4Provider(bridge_token=_BRIDGE_TOKEN)
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(_payload()),
        ):
            batch = live_provider.fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
                limit=120,
            )
        metadata = batch.metadata
        assert batch.reference_price is not None
        replay_result = T4ReplayResult(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
            candles=batch.candles,
            reference_price=batch.reference_price,
            contract_id=str(metadata["t4_contract_id"]),
            contract_expires_at=datetime.fromisoformat(
                str(metadata["t4_contract_expires_at"])
            ),
            contract_roll_at=datetime.fromisoformat(
                str(metadata["t4_contract_roll_at"])
            ),
            contract_selection=str(metadata["t4_contract_selection"]),
            rolled_from_contract_id=None,
            bridge_schema_version=3,
            futures_evidence=batch.futures_evidence,
            source_batch_hashes=(batch.raw_payload_sha256 or "0" * 64,),
            replay_fingerprint_sha256="f" * 64,
        )
        selected_policy = RiskPolicy.load(
            PROJECT_ROOT / "configs" / "risk_policy.v1.json"
        )
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(_payload()),
        ):
            live = ResearchOrchestrator(
                provider=live_provider,
                policy=selected_policy,
            ).analyze(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
            )
        replay = ResearchOrchestrator(
            provider=T4ReplayProvider(_StaticReplayRepository(replay_result)),  # type: ignore[arg-type]
            policy=selected_policy,
        ).analyze(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
        )

        self.assertTrue(live.metadata["futures_gate_passed"])
        self.assertTrue(replay.metadata["futures_gate_passed"])
        self.assertEqual(live.data_snapshot_id, replay.data_snapshot_id)
        self.assertEqual(live.metrics, replay.metrics)
        self.assertEqual(live.futures_metrics, replay.futures_metrics)
        self.assertEqual(live.decision, replay.decision)
        self.assertEqual(live.reason_codes, replay.reason_codes)
        self.assertEqual(live.scenarios, replay.scenarios)
        self.assertEqual(live.invalidation_conditions, replay.invalidation_conditions)

    def test_fingerprint_changes_when_one_book_quantity_changes(self) -> None:
        original = _payload()
        changed = _payload()
        evidence = changed["futures_evidence"]
        assert isinstance(evidence, dict)
        bids = evidence["bids"]
        assert isinstance(bids, list) and isinstance(bids[0], dict)
        bids[0]["quantity"] = float(bids[0]["quantity"]) + 1.0
        reports = []
        for payload in (original, changed):
            provider = Plus500T4Provider(bridge_token=_BRIDGE_TOKEN)
            with patch(
                "crypto_agent.providers.t4.build_opener",
                return_value=_Opener(payload),
            ):
                reports.append(
                    ResearchOrchestrator(
                        provider=provider,
                        policy=RiskPolicy.load(
                            PROJECT_ROOT / "configs" / "risk_policy.v1.json"
                        ),
                    ).analyze(
                        symbol="BTC/USD",
                        interval_minutes=1440,
                        as_of=AS_OF,
                    )
                )
        self.assertNotEqual(reports[0].data_snapshot_id, reports[1].data_snapshot_id)

    def test_fingerprint_binds_analysis_cutoff(self) -> None:
        reports = []
        for cutoff in (AS_OF, AS_OF + timedelta(seconds=10)):
            provider = Plus500T4Provider(bridge_token=_BRIDGE_TOKEN)
            payload = _payload()
            with patch(
                "crypto_agent.providers.t4.build_opener",
                return_value=_Opener(payload),
            ):
                reports.append(
                    ResearchOrchestrator(
                        provider=provider,
                        policy=RiskPolicy.load(
                            PROJECT_ROOT / "configs" / "risk_policy.v1.json"
                        ),
                    ).analyze(
                        symbol="BTC/USD",
                        interval_minutes=1440,
                        as_of=cutoff,
                    )
                )
        self.assertNotEqual(reports[0].data_snapshot_id, reports[1].data_snapshot_id)


if __name__ == "__main__":
    unittest.main()

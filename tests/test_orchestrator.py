from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from crypto_agent.consensus_math import (
    CONSENSUS_ALGORITHM_VERSION,
    CONSENSUS_WINDOW_SIZE,
)
from crypto_agent.domain import Decision
from crypto_agent.orchestrator import ResearchOrchestrator, _consensus_attested
from crypto_agent.providers.base import ProviderBatch, ProviderError
from crypto_agent.providers.coinbase import CoinbaseExchangePublicProvider
from crypto_agent.providers.consensus import CrossExchangeConsensusProvider
from crypto_agent.providers.kraken import KrakenPublicProvider
from crypto_agent.providers.synthetic import SyntheticProvider
from crypto_agent.storage import ReportRepository

from tests.helpers import policy


AS_OF = datetime(2026, 8, 10, tzinfo=timezone.utc)


def _fixture_fetch(source_id: str, *, price_multiplier: float = 1.0):
    candles = SyntheticProvider().fetch_candles(
        symbol="BTC/USD",
        interval_minutes=1440,
        as_of=AS_OF,
        limit=CONSENSUS_WINDOW_SIZE,
    )
    rows = [
        replace(
            item,
            source=source_id,
            open=item.open * price_multiplier,
            high=item.high * price_multiplier,
            low=item.low * price_multiplier,
            close=item.close * price_multiplier,
        )
        for item in candles
    ]

    def fetch_candles(**kwargs):
        return list(rows[-kwargs["limit"] :])

    return fetch_candles


def _approved_consensus_fixture():
    loaded_policy = policy()
    kraken = KrakenPublicProvider()
    coinbase = CoinbaseExchangePublicProvider()
    kraken.fetch_candles = _fixture_fetch(kraken.source_id)  # type: ignore[method-assign]
    coinbase.fetch_candles = _fixture_fetch(  # type: ignore[method-assign]
        coinbase.source_id,
        price_multiplier=1.0005,
    )
    provider = CrossExchangeConsensusProvider((kraken, coinbase), loaded_policy)
    batch = provider.fetch_batch(
        symbol="BTC/USD",
        interval_minutes=1440,
        as_of=AS_OF,
        limit=CONSENSUS_WINDOW_SIZE,
    )
    return provider, batch, loaded_policy


def _attested(
    provider: CrossExchangeConsensusProvider,
    batch: ProviderBatch,
    loaded_policy,
) -> bool:
    return _consensus_attested(
        provider=provider,
        batch=batch,
        required_sources=loaded_policy.min_consensus_sources,
        required_policy_hash=loaded_policy.fingerprint(),
    )


class OrchestratorTests(unittest.TestCase):
    def test_consensus_attestation_accepts_only_the_complete_fixed_window(self) -> None:
        provider, batch, loaded_policy = _approved_consensus_fixture()

        self.assertTrue(_attested(provider, batch, loaded_policy))
        self.assertEqual(len(batch.candles), CONSENSUS_WINDOW_SIZE)
        self.assertEqual(
            len(batch.input_candles),
            loaded_policy.min_consensus_sources * CONSENSUS_WINDOW_SIZE,
        )

    def test_forged_consensus_metadata_cannot_attest(self) -> None:
        provider, batch, loaded_policy = _approved_consensus_fixture()
        for name, changes in (
            ("version", {"consensus_version": "forged_consensus_v99"}),
            ("window", {"consensus_window_size": CONSENSUS_WINDOW_SIZE - 1}),
            ("overlap_short", {"consensus_overlap_count": CONSENSUS_WINDOW_SIZE - 1}),
            ("overlap_extra", {"consensus_overlap_count": CONSENSUS_WINDOW_SIZE + 1}),
            (
                "source_count",
                {
                    "consensus_source_counts": {
                        source["id"]: CONSENSUS_WINDOW_SIZE - 1
                        for source in batch.sources
                    }
                },
            ),
        ):
            with self.subTest(name=name):
                forged = replace(batch, metadata={**batch.metadata, **changes})
                self.assertFalse(_attested(provider, forged, loaded_policy))

    def test_truncated_or_extra_canonical_window_cannot_attest(self) -> None:
        provider, batch, loaded_policy = _approved_consensus_fixture()
        forged_batches = (
            replace(batch, candles=batch.candles[:-1]),
            replace(batch, candles=(*batch.candles, batch.candles[0])),
        )
        for forged in forged_batches:
            with self.subTest(candle_count=len(forged.candles)):
                self.assertFalse(_attested(provider, forged, loaded_policy))

    def test_truncated_or_extra_raw_batch_cannot_attest(self) -> None:
        provider, batch, loaded_policy = _approved_consensus_fixture()
        forged_batches = (
            replace(batch, input_candles=batch.input_candles[:-1]),
            replace(
                batch,
                input_candles=(*batch.input_candles, batch.input_candles[0]),
            ),
        )
        for forged in forged_batches:
            with self.subTest(raw_count=len(forged.input_candles)):
                self.assertFalse(_attested(provider, forged, loaded_policy))

    def test_raw_sources_must_have_the_same_120_windows_as_canonical_data(self) -> None:
        provider, batch, loaded_policy = _approved_consensus_fixture()
        first = batch.input_candles[0]
        shifted = replace(
            first,
            open_time=first.open_time - timedelta(days=1),
            close_time=first.close_time - timedelta(days=1),
        )
        wrong_window = replace(
            batch,
            input_candles=(shifted, *batch.input_candles[1:]),
        )
        wrong_source = replace(
            batch,
            input_candles=(
                replace(first, source="spoofed_approved_source"),
                *batch.input_candles[1:],
            ),
        )
        for name, forged in (
            ("window", wrong_window),
            ("source", wrong_source),
        ):
            with self.subTest(name=name):
                self.assertFalse(_attested(provider, forged, loaded_policy))

    def test_batch_source_order_must_match_sealed_provider_composition(self) -> None:
        provider, batch, loaded_policy = _approved_consensus_fixture()
        reversed_sources = tuple(reversed(batch.sources))
        forged = replace(
            batch,
            sources=reversed_sources,
            metadata={
                **batch.metadata,
                "consensus_source_ids": [item["id"] for item in reversed_sources],
            },
        )
        self.assertFalse(_attested(provider, forged, loaded_policy))

    def test_volume_attestation_requires_full_consensus_attestation(self) -> None:
        provider, batch, loaded_policy = _approved_consensus_fixture()
        source_ids = [item["id"] for item in batch.sources]
        forged = replace(
            batch,
            metadata={
                **batch.metadata,
                "consensus_version": "forged_consensus_v99",
                "consensus_volume_zscores": {
                    source_id: 10.0 for source_id in source_ids
                },
            },
        )
        with patch.object(
            CrossExchangeConsensusProvider,
            "fetch_batch",
            return_value=forged,
        ):
            report = ResearchOrchestrator(
                provider=provider,
                policy=loaded_policy,
            ).analyze(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
            )

        self.assertFalse(report.metadata["v1_1_consensus_passed"])
        self.assertFalse(report.metadata["v1_1_volume_anomaly_attested"])
        self.assertIn("CONSENSUS_REQUIRED", report.risk.flags)

    def test_report_is_read_only_and_persisted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = ReportRepository(Path(directory) / "reports.db")
            orchestrator = ResearchOrchestrator(
                provider=SyntheticProvider(),
                policy=policy(),
                repository=repository,
            )
            report = orchestrator.analyze(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
            )
            self.assertEqual(report.decision, Decision.NO_SIGNAL)
            self.assertFalse(report.metadata["execution_enabled"])
            self.assertEqual(len(report.metadata["input_fingerprint_sha256"]), 64)
            self.assertIsNotNone(repository.latest(report.asset_id, "1d"))
            self.assertIsNotNone(repository.read_snapshot(report.data_snapshot_id))
            self.assertNotIn("BUY", str(report.to_dict()).upper())
            self.assertEqual(report.risk.input_fingerprint_sha256, report.data_snapshot_id[7:])

    def test_unapproved_asset_is_no_signal(self) -> None:
        orchestrator = ResearchOrchestrator(
            provider=SyntheticProvider(),
            policy=policy(),
        )
        report = orchestrator.analyze(
            symbol="DOGE/USD",
            as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertTrue(report.risk.vetoed)
        self.assertIn("ASSET_NOT_ALLOWED", report.risk.flags)

    def test_provider_failure_returns_no_signal(self) -> None:
        class FailingProvider:
            source_id = "failing_test_provider"

            def fetch_candles(self, **kwargs):
                raise ProviderError("offline")

        orchestrator = ResearchOrchestrator(provider=FailingProvider(), policy=policy())
        report = orchestrator.analyze(symbol="BTC/USD")
        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertIn("PROVIDER_UNAVAILABLE", report.risk.flags)

    def test_standalone_feed_can_never_emit_an_alert(self) -> None:
        class AlertingSingleFeed(SyntheticProvider):
            def fetch_candles(self, **kwargs):
                values = super().fetch_candles(**kwargs)
                last = values[-1]
                values[-1] = replace(
                    last,
                    open=last.open,
                    high=last.open * 1.07,
                    low=last.open * 0.999,
                    close=last.open * 1.06,
                )
                return values

        report = ResearchOrchestrator(
            provider=AlertingSingleFeed(), policy=policy()
        ).analyze(
            symbol="BTC/USD",
            as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
        )
        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertTrue(report.risk.vetoed)
        self.assertIn("CONSENSUS_REQUIRED", report.risk.flags)
        self.assertFalse(report.metadata["v1_1_consensus_passed"])

    def test_forged_batch_metadata_cannot_attest_single_source_consensus(self) -> None:
        class ForgedSingleSourceBatch:
            source_id = "forged_single_source"

            def fetch_batch(self, **kwargs):
                values = SyntheticProvider().fetch_candles(**kwargs)
                values = [replace(item, source=self.source_id) for item in values]
                last = values[-1]
                values[-1] = replace(
                    last,
                    high=last.open * 1.07,
                    low=last.open * 0.999,
                    close=last.open * 1.06,
                )
                return ProviderBatch(
                    candles=tuple(values),
                    input_candles=tuple(values),
                    sources=(
                        {
                            "id": self.source_id,
                            "kind": "market_data",
                            "trust": "untrusted_external_data",
                        },
                    ),
                    metadata={
                        "consensus_passed": True,
                        "consensus_source_ids": [self.source_id],
                        "consensus_overlap_count": len(values),
                    },
                )

            def fetch_candles(self, **kwargs):
                raise AssertionError("batch path must be used")

        report = ResearchOrchestrator(
            provider=ForgedSingleSourceBatch(), policy=policy()
        ).analyze(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
        )

        self.assertIn("LARGE_PERIOD_MOVE", report.reason_codes)
        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertTrue(report.risk.vetoed)
        self.assertIn("CONSENSUS_REQUIRED", report.risk.flags)
        self.assertFalse(report.metadata["v1_1_consensus_passed"])

    def test_future_as_of_is_no_signal_and_does_not_query_provider(self) -> None:
        class MustNotRunProvider:
            source_id = "must_not_run"

            def fetch_candles(self, **kwargs):
                raise AssertionError("provider must not be called")

        orchestrator = ResearchOrchestrator(provider=MustNotRunProvider(), policy=policy())
        report = orchestrator.analyze(
            symbol="BTC/USD",
            as_of=datetime(2035, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertIn("FUTURE_AS_OF", report.risk.flags)

    def test_same_as_of_different_horizons_are_both_stored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = ReportRepository(Path(directory) / "reports.db")
            orchestrator = ResearchOrchestrator(
                provider=SyntheticProvider(), policy=policy(), repository=repository
            )
            as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
            four_hour = orchestrator.analyze(
                symbol="BTC/USD", interval_minutes=240, as_of=as_of
            )
            daily = orchestrator.analyze(
                symbol="BTC/USD", interval_minutes=1440, as_of=as_of
            )
            self.assertIsNotNone(repository.latest(four_hour.asset_id, "4h"))
            self.assertIsNotNone(repository.latest(daily.asset_id, "1d"))

    def test_exact_duplicates_do_not_change_metrics(self) -> None:
        class DuplicateProvider(SyntheticProvider):
            def fetch_candles(self, **kwargs):
                values = super().fetch_candles(**kwargs)
                return [*values, values[-1]]

        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        baseline = ResearchOrchestrator(
            provider=SyntheticProvider(), policy=policy()
        ).analyze(symbol="BTC/USD", as_of=as_of)
        duplicate = ResearchOrchestrator(
            provider=DuplicateProvider(), policy=policy()
        ).analyze(symbol="BTC/USD", as_of=as_of)
        self.assertIn("DUPLICATE_EXACT", duplicate.data_quality.flags)
        self.assertEqual(duplicate.metrics, baseline.metrics)

    def test_local_storage_is_append_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            repository = ReportRepository(Path(directory) / "reports.db")
            report = ResearchOrchestrator(
                provider=SyntheticProvider(), policy=policy(), repository=repository
            ).analyze(
                symbol="BTC/USD",
                as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
            )
            with repository.connect() as connection:
                with self.assertRaises(Exception):
                    connection.execute(
                        "UPDATE research_reports SET decision = 'ALERT' WHERE decision_id = ?",
                        (report.decision_id,),
                    )


if __name__ == "__main__":
    unittest.main()

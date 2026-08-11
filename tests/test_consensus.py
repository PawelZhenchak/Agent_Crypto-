from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from tempfile import TemporaryDirectory
from unittest.mock import patch

from crypto_agent.consensus_math import (
    CONSENSUS_ALGORITHM_VERSION,
    CONSENSUS_WINDOW_SIZE,
    ConsensusInput,
    ConsensusMathError,
    ConsensusPolicyParameters,
    build_cross_exchange_consensus,
)
from crypto_agent.domain import Decision, ReferencePriceObservation
from crypto_agent.providers.base import ProviderError
from crypto_agent.providers.consensus import (
    CrossExchangeConsensusProvider,
    _build_reference_price_envelope,
)
from crypto_agent.providers.coinbase import CoinbaseExchangePublicProvider
from crypto_agent.providers.kraken import KrakenPublicProvider
from crypto_agent.providers.synthetic import SyntheticProvider
from crypto_agent.orchestrator import ResearchOrchestrator
from crypto_agent.storage import ReportRepository

from tests.helpers import policy


AS_OF = datetime(2026, 8, 10, tzinfo=timezone.utc)


def consensus_parameters() -> ConsensusPolicyParameters:
    selected = policy()
    return ConsensusPolicyParameters(
        min_overlap=selected.min_consensus_overlap,
        max_close_divergence_bps=(
            selected.max_pairwise_reference_price_divergence_bps
        ),
        max_ohlc_divergence_bps=selected.max_cross_source_ohlc_divergence_bps,
        max_volume_zscore_delta=selected.max_cross_source_volume_zscore_delta,
        max_divergent_fraction=selected.max_divergent_candle_fraction,
        max_market_price=selected.max_market_price,
        max_base_volume=selected.max_base_volume,
    )


def consensus_inputs(provider: "StaticProvider") -> tuple[ConsensusInput, ...]:
    return tuple(
        ConsensusInput(
            source_id=provider.source_id,
            open_time=item.open_time,
            close_time=item.close_time,
            open=item.open,
            high=item.high,
            low=item.low,
            close=item.close,
            volume=item.volume,
            available_at=item.available_at,
            ingested_at=item.ingested_at,
        )
        for item in provider.candles
    )


class StaticProvider:
    def __init__(
        self,
        source_id: str,
        *,
        price_multiplier: float = 1.0,
        count: int = CONSENSUS_WINDOW_SIZE,
    ) -> None:
        self.source_id = source_id
        source = SyntheticProvider().fetch_candles(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
            limit=count,
        )
        self.candles = [
            replace(
                candle,
                source=source_id,
                open=candle.open * price_multiplier,
                high=candle.high * price_multiplier,
                low=candle.low * price_multiplier,
                close=candle.close * price_multiplier,
            )
            for candle in source
        ]

    def fetch_candles(self, **kwargs):
        return list(self.candles[-kwargs["limit"] :])


class ConsensusProviderTests(unittest.TestCase):
    def provider(self, right_multiplier: float = 1.0005):
        return CrossExchangeConsensusProvider(
            (
                StaticProvider("venue_a"),
                StaticProvider("venue_b", price_multiplier=right_multiplier),
            ),
            policy(),
            allow_unapproved_for_testing=True,
        )

    def test_agreeing_sources_create_auditable_consensus(self) -> None:
        batch = self.provider().fetch_batch(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
            limit=120,
        )
        self.assertEqual(len(batch.candles), 120)
        self.assertEqual(len(batch.input_candles), 240)
        self.assertEqual({item["id"] for item in batch.sources}, {"venue_a", "venue_b"})
        self.assertTrue(batch.metadata["consensus_passed"])
        self.assertEqual(
            batch.metadata["consensus_overlap_count"], CONSENSUS_WINDOW_SIZE
        )
        self.assertEqual(
            batch.metadata["consensus_window_size"], CONSENSUS_WINDOW_SIZE
        )
        self.assertTrue(
            all(item.source == "cross_exchange_spot_consensus_v1" for item in batch.candles)
        )
        self.assertIsNone(batch.reference_price)

    def test_reference_snapshot_uses_exactly_the_two_approved_sources(self) -> None:
        providers = (KrakenPublicProvider(), CoinbaseExchangePublicProvider())
        observations = {
            KrakenPublicProvider.source_id: ReferencePriceObservation(
                symbol="BTC/USD",
                price=99.5,
                event_time=AS_OF,
                available_at=AS_OF,
                ingested_at=AS_OF,
                source=KrakenPublicProvider.source_id,
            ),
            CoinbaseExchangePublicProvider.source_id: ReferencePriceObservation(
                symbol="BTC/USD",
                price=100.5,
                event_time=AS_OF,
                available_at=AS_OF,
                ingested_at=AS_OF,
                source=CoinbaseExchangePublicProvider.source_id,
            ),
        }

        snapshot, result = _build_reference_price_envelope(
            providers,
            observations,
            symbol="BTC/USD",
            as_of=AS_OF,
            policy=policy(),
        )

        self.assertEqual(snapshot.symbol, "BTC/USD")
        self.assertEqual(snapshot.price, 100.0)
        self.assertEqual(
            tuple(item.source for item in snapshot.observations),
            tuple(item.source_id for item in providers),
        )
        self.assertEqual(set(result.venue_ids), {"kraken", "coinbase"})

    def test_provider_wrapper_matches_the_shared_pure_algorithm(self) -> None:
        left = StaticProvider("venue_a")
        right = StaticProvider("venue_b", price_multiplier=1.0005)
        pure = build_cross_exchange_consensus(
            {
                left.source_id: consensus_inputs(left),
                right.source_id: consensus_inputs(right),
            },
            consensus_parameters(),
            limit=CONSENSUS_WINDOW_SIZE,
        )
        batch = CrossExchangeConsensusProvider(
            (left, right),
            policy(),
            allow_unapproved_for_testing=True,
        ).fetch_batch(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
            limit=CONSENSUS_WINDOW_SIZE,
        )

        self.assertEqual(batch.metadata["consensus_version"], CONSENSUS_ALGORITHM_VERSION)
        self.assertEqual(len(batch.candles), len(pure.candles))
        for wrapped, expected in zip(batch.candles, pure.candles, strict=True):
            self.assertEqual(wrapped.open_time, expected.open_time)
            self.assertEqual(wrapped.close_time, expected.close_time)
            self.assertEqual(wrapped.open, expected.open)
            self.assertEqual(wrapped.high, expected.high)
            self.assertEqual(wrapped.low, expected.low)
            self.assertEqual(wrapped.close, expected.close)
            self.assertEqual(wrapped.volume, expected.normalized_volume)
            self.assertEqual(wrapped.available_at, expected.available_at)
            self.assertEqual(wrapped.ingested_at, expected.ingested_at)

    def test_119_aligned_inputs_are_rejected_by_the_fixed_window(self) -> None:
        left = StaticProvider("venue_a")
        right = StaticProvider("venue_b", price_multiplier=1.0005)
        right.candles = right.candles[1:]

        provider = CrossExchangeConsensusProvider(
            (left, right),
            policy(),
            allow_unapproved_for_testing=True,
        )

        with self.assertRaises(ProviderError) as raised:
            provider.fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
                limit=CONSENSUS_WINDOW_SIZE,
            )

        self.assertEqual(raised.exception.code, "INSUFFICIENT_SOURCE_OVERLAP")
        self.assertIsNotNone(raised.exception.evidence)
        assert raised.exception.evidence is not None
        self.assertEqual(
            raised.exception.evidence.metadata["consensus_window_size"],
            CONSENSUS_WINDOW_SIZE,
        )

    def test_caller_limit_does_not_change_the_fixed_consensus_window(self) -> None:
        def provider() -> CrossExchangeConsensusProvider:
            return CrossExchangeConsensusProvider(
                (
                    StaticProvider("venue_a", count=180),
                    StaticProvider("venue_b", price_multiplier=1.0005, count=180),
                ),
                policy(),
                allow_unapproved_for_testing=True,
            )

        small_request = provider().fetch_batch(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
            limit=1,
        )
        large_request = provider().fetch_batch(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
            limit=180,
        )

        self.assertEqual(small_request.candles, large_request.candles)
        self.assertEqual(len(small_request.candles), CONSENSUS_WINDOW_SIZE)
        self.assertEqual(len(large_request.candles), CONSENSUS_WINDOW_SIZE)
        self.assertEqual(len(small_request.input_candles), 2 * CONSENSUS_WINDOW_SIZE)
        self.assertEqual(len(large_request.input_candles), 2 * CONSENSUS_WINDOW_SIZE)

    def test_one_venue_gap_cannot_be_hidden_by_an_extra_older_candle(self) -> None:
        left = StaticProvider("venue_a", count=CONSENSUS_WINDOW_SIZE + 1)
        right = StaticProvider(
            "venue_b",
            price_multiplier=1.0005,
            count=CONSENSUS_WINDOW_SIZE + 1,
        )
        right.candles.pop(60)
        provider = CrossExchangeConsensusProvider(
            (left, right),
            policy(),
            allow_unapproved_for_testing=True,
        )

        with self.assertRaises(ProviderError) as raised:
            provider.fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
                limit=CONSENSUS_WINDOW_SIZE + 1,
            )

        self.assertEqual(raised.exception.code, "CONSENSUS_WINDOW_GAP")

    def test_common_gap_is_rejected_by_the_pure_algorithm(self) -> None:
        left = StaticProvider("venue_a", count=CONSENSUS_WINDOW_SIZE + 1)
        right = StaticProvider(
            "venue_b",
            price_multiplier=1.0005,
            count=CONSENSUS_WINDOW_SIZE + 1,
        )
        left.candles.pop(60)
        right.candles.pop(60)

        with self.assertRaises(ConsensusMathError) as raised:
            build_cross_exchange_consensus(
                {
                    left.source_id: consensus_inputs(left),
                    right.source_id: consensus_inputs(right),
                },
                replace(
                    consensus_parameters(),
                    min_overlap=CONSENSUS_WINDOW_SIZE,
                ),
                limit=CONSENSUS_WINDOW_SIZE,
            )

        self.assertEqual(raised.exception.code, "CONSENSUS_WINDOW_GAP")

    def test_aligned_duration_mismatch_is_rejected_by_pure_math(self) -> None:
        left = list(consensus_inputs(StaticProvider("venue_a")))
        right = list(
            consensus_inputs(
                StaticProvider("venue_b", price_multiplier=1.0005)
            )
        )
        for rows in (left, right):
            item = rows[60]
            rows[60] = replace(
                item,
                close_time=item.close_time - timedelta(hours=1),
            )

        with self.assertRaises(ConsensusMathError) as raised:
            build_cross_exchange_consensus(
                {"venue_a": tuple(left), "venue_b": tuple(right)},
                replace(
                    consensus_parameters(),
                    min_overlap=CONSENSUS_WINDOW_SIZE,
                ),
                limit=CONSENSUS_WINDOW_SIZE,
            )

        self.assertEqual(raised.exception.code, "CONSENSUS_DURATION_MISMATCH")

    def test_pure_algorithm_golden_volume_context_is_stable(self) -> None:
        start = datetime(2026, 8, 6, tzinfo=timezone.utc)
        inputs: dict[str, tuple[ConsensusInput, ...]] = {}
        for source_id, scale in (("venue_a", 1.0), ("venue_b", 10.0)):
            rows: list[ConsensusInput] = []
            for index in range(60):
                volume = float((index + 1) * 10)
                open_time = start + timedelta(days=index)
                close_time = open_time + timedelta(days=1)
                rows.append(
                    ConsensusInput(
                        source_id=source_id,
                        open_time=open_time,
                        close_time=close_time,
                        open=100.0,
                        high=110.0,
                        low=90.0,
                        close=104.0,
                        volume=volume * scale,
                        available_at=close_time,
                        ingested_at=close_time,
                    )
                )
            inputs[source_id] = tuple(rows)

        # The pure primitive remains reusable for small golden fixtures; the
        # production provider contract enforces the exact 120-candle window.
        parameters = replace(consensus_parameters(), min_overlap=60)
        result = build_cross_exchange_consensus(inputs, parameters)

        self.assertEqual(result.source_ids, ("venue_a", "venue_b"))
        self.assertEqual(result.overlap_count, 60)
        self.assertEqual(result.volume_scales, {"venue_a": 300.0, "venue_b": 3000.0})
        self.assertAlmostEqual(result.candles[0].normalized_volume, 1 / 30)
        self.assertEqual(result.candles[-1].normalized_volume, 2.0)
        self.assertEqual(result.candles[-1].close, 104.0)

    def test_configuration_properties_are_immutable(self) -> None:
        kraken = KrakenPublicProvider()
        coinbase = CoinbaseExchangePublicProvider()
        approved = CrossExchangeConsensusProvider((kraken, coinbase), policy())

        with self.assertRaises(AttributeError):
            approved.providers = (  # type: ignore[misc]
                StaticProvider("venue_a"),
                StaticProvider("venue_b"),
            )
        with self.assertRaises(AttributeError):
            approved.policy = replace(  # type: ignore[misc]
                policy(), max_cross_source_divergence_bps=1.0
            )
        with self.assertRaises(AttributeError):
            approved.approved_venue_pair = True  # type: ignore[misc]
        with self.assertRaises(AttributeError):
            del approved._providers  # type: ignore[attr-defined]
        for provider in (kraken, coinbase):
            with self.subTest(provider=type(provider).__name__):
                self.assertFalse(hasattr(provider, "__dict__"))
                with self.assertRaises(AttributeError):
                    provider.fetch_candles = lambda **_: []  # type: ignore[method-assign]
                with self.assertRaises(AttributeError):
                    object.__setattr__(provider, "fetch_candles", lambda **_: [])
                with self.assertRaises(AttributeError):
                    provider.fetch_reference_price = lambda **_: None  # type: ignore[method-assign]
                with self.assertRaises(AttributeError):
                    object.__setattr__(provider, "fetch_reference_price", lambda **_: None)

    def test_sealed_composition_detects_low_level_origin_or_timeout_tampering(self) -> None:
        mutations = (
            ("_base_url", "https://attacker.example"),
            ("_timeout_seconds", 99.0),
        )
        for attribute, value in mutations:
            with self.subTest(attribute=attribute):
                kraken = KrakenPublicProvider(timeout_seconds=7)
                coinbase = CoinbaseExchangePublicProvider(timeout_seconds=7)
                approved = CrossExchangeConsensusProvider((kraken, coinbase), policy())
                object.__setattr__(kraken, attribute, value)

                self.assertFalse(approved.approved_venue_pair)
                with self.assertRaises(ProviderError) as raised:
                    approved.fetch_batch(
                        symbol="BTC/USD",
                        interval_minutes=1440,
                        as_of=AS_OF,
                        limit=120,
                    )
                self.assertEqual(
                    raised.exception.code,
                    "CONSENSUS_CONFIGURATION_TAMPERED",
                )

    def test_in_place_provider_mapping_mutation_invalidates_integrity(self) -> None:
        approved = CrossExchangeConsensusProvider(
            (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
            policy(),
        )
        original_pair = KrakenPublicProvider._pairs["BTC/USD"]
        try:
            KrakenPublicProvider._pairs["BTC/USD"] = "ETHUSD"
            self.assertFalse(approved.approved_venue_pair)
            with self.assertRaises(ProviderError) as raised:
                approved.fetch_batch(
                    symbol="BTC/USD",
                    interval_minutes=1440,
                    as_of=AS_OF,
                    limit=CONSENSUS_WINDOW_SIZE,
                )
            self.assertEqual(
                raised.exception.code,
                "CONSENSUS_CONFIGURATION_TAMPERED",
            )
        finally:
            KrakenPublicProvider._pairs["BTC/USD"] = original_pair

        self.assertTrue(approved.approved_venue_pair)

    def test_mapping_subclass_cannot_lie_to_integrity_snapshot(self) -> None:
        class EvilDict(dict):
            def items(self):
                return dict.items(self)

            def get(self, key, default=None):
                if key == "BTC/USD":
                    return "ETHUSD"
                return dict.get(self, key, default)

        approved = CrossExchangeConsensusProvider(
            (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
            policy(),
        )
        original_pairs = KrakenPublicProvider._pairs
        try:
            KrakenPublicProvider._pairs = EvilDict(original_pairs)
            self.assertFalse(approved.approved_venue_pair)
            with self.assertRaises(ProviderError) as raised:
                approved.fetch_batch(
                    symbol="BTC/USD",
                    interval_minutes=1440,
                    as_of=AS_OF,
                    limit=CONSENSUS_WINDOW_SIZE,
                )
            self.assertEqual(
                raised.exception.code,
                "CONSENSUS_CONFIGURATION_TAMPERED",
            )
        finally:
            KrakenPublicProvider._pairs = original_pairs

        self.assertTrue(approved.approved_venue_pair)

    def test_string_subclass_cannot_spoof_the_pinned_origin(self) -> None:
        class EvilStr(str):
            def __format__(self, _spec):
                return "https://attacker.example"

        kraken = KrakenPublicProvider()
        approved = CrossExchangeConsensusProvider(
            (kraken, CoinbaseExchangePublicProvider()),
            policy(),
        )
        object.__setattr__(kraken, "_base_url", EvilStr("https://api.kraken.com"))

        self.assertFalse(approved.approved_venue_pair)
        with self.assertRaises(ProviderError) as raised:
            approved.fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
                limit=CONSENSUS_WINDOW_SIZE,
            )
        self.assertEqual(
            raised.exception.code,
            "CONSENSUS_CONFIGURATION_TAMPERED",
        )

    def test_low_level_composition_spoof_fails_before_fetch_and_cannot_alert(self) -> None:
        approved = CrossExchangeConsensusProvider(
            (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
            policy(),
        )
        orchestrator = ResearchOrchestrator(provider=approved, policy=policy())
        left = StaticProvider(KrakenPublicProvider.source_id)
        right = StaticProvider(CoinbaseExchangePublicProvider.source_id)
        for provider in (left, right):
            latest = provider.candles[-1]
            provider.candles[-1] = replace(
                latest,
                high=latest.open * 1.07,
                low=latest.open * 0.999,
                close=latest.open * 1.06,
            )
        object.__setattr__(approved, "_providers", (left, right))

        report = orchestrator.analyze(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
        )

        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertEqual(
            report.metadata["provider_error_code"],
            "CONSENSUS_CONFIGURATION_TAMPERED",
        )
        self.assertFalse(report.metadata["v1_1_consensus_passed"])
        self.assertFalse(approved.approved_venue_pair)

    def test_low_level_policy_spoof_is_revalidated_before_fetch(self) -> None:
        approved = CrossExchangeConsensusProvider(
            (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
            policy(),
        )
        object.__setattr__(
            approved,
            "_policy",
            replace(
                policy(),
                max_cross_source_divergence_bps=101.0,
                max_divergent_candle_fraction=0.01,
            ),
        )

        with self.assertRaises(ProviderError) as raised:
            approved.fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
                limit=120,
            )

        self.assertEqual(
            raised.exception.code,
            "CONSENSUS_CONFIGURATION_TAMPERED",
        )
        self.assertFalse(approved.approved_venue_pair)

    def test_class_method_substitution_fails_closed_and_cannot_alert(self) -> None:
        kraken = KrakenPublicProvider()
        coinbase = CoinbaseExchangePublicProvider()
        kraken_fixture = StaticProvider(kraken.source_id)
        coinbase_fixture = StaticProvider(coinbase.source_id, price_multiplier=1.0005)
        for provider in (kraken_fixture, coinbase_fixture):
            latest = provider.candles[-1]
            provider.candles[-1] = replace(
                latest,
                high=latest.open * 1.07,
                low=latest.open * 0.999,
                close=latest.open * 1.06,
            )
        approved = CrossExchangeConsensusProvider((kraken, coinbase), policy())
        with patch.object(
            KrakenPublicProvider,
            "fetch_candles",
            new=kraken_fixture.fetch_candles,
        ), patch.object(
            CoinbaseExchangePublicProvider,
            "fetch_candles",
            new=coinbase_fixture.fetch_candles,
        ):
            self.assertFalse(approved.approved_venue_pair)
            report = ResearchOrchestrator(provider=approved, policy=policy()).analyze(
                symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF
            )

        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertTrue(report.risk.vetoed)
        self.assertFalse(report.metadata["v1_1_consensus_passed"])
        self.assertEqual(
            report.metadata["provider_error_code"],
            "CONSENSUS_CONFIGURATION_TAMPERED",
        )
        self.assertNotEqual(report.decision, Decision.ALERT)

    def test_reference_price_method_substitution_fails_before_fetch(self) -> None:
        approved = CrossExchangeConsensusProvider(
            (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
            policy(),
        )

        with patch.object(
            KrakenPublicProvider,
            "fetch_reference_price",
            new=lambda *_args, **_kwargs: ReferencePriceObservation(
                symbol="BTC/USD",
                price=100.0,
                event_time=AS_OF,
                available_at=AS_OF,
                ingested_at=AS_OF,
                source=KrakenPublicProvider.source_id,
            ),
        ):
            self.assertFalse(approved.approved_venue_pair)
            with self.assertRaises(ProviderError) as raised:
                approved.fetch_batch(
                    symbol="BTC/USD",
                    interval_minutes=1440,
                    as_of=AS_OF,
                    limit=CONSENSUS_WINDOW_SIZE,
                )

        self.assertEqual(
            raised.exception.code,
            "CONSENSUS_CONFIGURATION_TAMPERED",
        )

    def test_in_place_method_code_mutation_fails_before_fetch(self) -> None:
        def forged_fetch(self, **_kwargs):
            del self
            return []

        methods = (
            (KrakenPublicProvider, "fetch_candles"),
            (CoinbaseExchangePublicProvider, "fetch_candles"),
            (KrakenPublicProvider, "fetch_reference_price"),
            (CoinbaseExchangePublicProvider, "fetch_reference_price"),
        )
        for owner, name in methods:
            with self.subTest(owner=owner.__name__, method=name):
                approved = CrossExchangeConsensusProvider(
                    (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
                    policy(),
                )
                function = getattr(owner, name)
                original_code = function.__code__
                try:
                    function.__code__ = forged_fetch.__code__
                    self.assertFalse(approved.approved_venue_pair)
                    with self.assertRaises(ProviderError) as raised:
                        approved.fetch_batch(
                            symbol="BTC/USD",
                            interval_minutes=1440,
                            as_of=AS_OF,
                            limit=CONSENSUS_WINDOW_SIZE,
                        )
                    self.assertEqual(
                        raised.exception.code,
                        "CONSENSUS_CONFIGURATION_TAMPERED",
                    )
                finally:
                    function.__code__ = original_code

                self.assertTrue(approved.approved_venue_pair)

    def test_transport_substitution_fails_before_fetch(self) -> None:
        approved = CrossExchangeConsensusProvider(
            (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
            policy(),
        )

        with patch("crypto_agent.providers.kraken.urlopen", new=lambda *_a, **_k: None):
            self.assertFalse(approved.approved_venue_pair)
            with self.assertRaises(ProviderError) as raised:
                approved.fetch_batch(
                    symbol="BTC/USD",
                    interval_minutes=1440,
                    as_of=AS_OF,
                    limit=CONSENSUS_WINDOW_SIZE,
                )

        self.assertEqual(
            raised.exception.code,
            "CONSENSUS_CONFIGURATION_TAMPERED",
        )

    def test_transport_dependency_substitution_fails_before_fetch(self) -> None:
        approved = CrossExchangeConsensusProvider(
            (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
            policy(),
        )

        with patch(
            "crypto_agent.providers.kraken.build_opener",
            new=lambda *_args, **_kwargs: None,
        ):
            self.assertFalse(approved.approved_venue_pair)
            with self.assertRaises(ProviderError) as raised:
                approved.fetch_batch(
                    symbol="BTC/USD",
                    interval_minutes=1440,
                    as_of=AS_OF,
                    limit=CONSENSUS_WINDOW_SIZE,
                )

        self.assertEqual(
            raised.exception.code,
            "CONSENSUS_CONFIGURATION_TAMPERED",
        )

    def test_stdlib_opener_method_substitution_fails_before_fetch(self) -> None:
        approved = CrossExchangeConsensusProvider(
            (KrakenPublicProvider(), CoinbaseExchangePublicProvider()),
            policy(),
        )

        with patch(
            "urllib.request.OpenerDirector.open",
            new=lambda *_args, **_kwargs: None,
        ):
            self.assertFalse(approved.approved_venue_pair)
            with self.assertRaises(ProviderError) as raised:
                approved.fetch_batch(
                    symbol="BTC/USD",
                    interval_minutes=1440,
                    as_of=AS_OF,
                    limit=CONSENSUS_WINDOW_SIZE,
                )

        self.assertEqual(
            raised.exception.code,
            "CONSENSUS_CONFIGURATION_TAMPERED",
        )

    def test_consensus_policy_must_match_orchestrator_policy(self) -> None:
        loose_policy = policy()
        strict_policy = replace(
            loose_policy,
            max_cross_source_divergence_bps=1.0,
            max_cross_source_ohlc_divergence_bps=1.0,
        )
        kraken = KrakenPublicProvider()
        coinbase = CoinbaseExchangePublicProvider()
        consensus = CrossExchangeConsensusProvider(
            (kraken, coinbase),
            loose_policy,
        )

        with self.assertRaisesRegex(ValueError, "same risk policy"):
            ResearchOrchestrator(provider=consensus, policy=strict_policy)

    def test_replaced_policy_cannot_weaken_consensus_safety(self) -> None:
        weakened = replace(
            policy(),
            max_cross_source_divergence_bps=101.0,
            max_divergent_candle_fraction=0.01,
        )

        with self.assertRaisesRegex(ValueError, "max_divergent_candle_fraction"):
            CrossExchangeConsensusProvider(
                (StaticProvider("venue_a"), StaticProvider("venue_b")),
                weakened,
                allow_unapproved_for_testing=True,
            )
        with self.assertRaisesRegex(ValueError, "max_divergent_candle_fraction"):
            ResearchOrchestrator(provider=SyntheticProvider(), policy=weakened)

    def test_testing_override_cannot_attest_consensus_or_emit_alert(self) -> None:
        left = StaticProvider("venue_a")
        right = StaticProvider("venue_b")
        for provider in (left, right):
            latest = provider.candles[-1]
            provider.candles[-1] = replace(
                latest,
                high=latest.open * 1.07,
                low=latest.open * 0.999,
                close=latest.open * 1.06,
            )

        testing_consensus = CrossExchangeConsensusProvider(
            (left, right), policy(), allow_unapproved_for_testing=True
        )
        report = ResearchOrchestrator(
            provider=testing_consensus, policy=policy()
        ).analyze(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
        )

        self.assertIn("LARGE_PERIOD_MOVE", report.reason_codes)
        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertTrue(report.risk.vetoed)
        self.assertIn("CONSENSUS_REQUIRED", report.risk.flags)
        self.assertFalse(report.metadata["v1_1_consensus_passed"])

    def test_latest_divergence_is_fail_closed(self) -> None:
        left = StaticProvider("venue_a")
        right = StaticProvider("venue_b")
        right.candles[-1] = replace(
            right.candles[-1],
            open=right.candles[-1].open * 1.02,
            high=right.candles[-1].high * 1.02,
            low=right.candles[-1].low * 1.02,
            close=right.candles[-1].close * 1.02,
        )
        consensus = CrossExchangeConsensusProvider(
            (left, right), policy(), allow_unapproved_for_testing=True
        )
        with self.assertRaises(ProviderError) as context:
            consensus.fetch_candles(
                symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=120
            )
        self.assertEqual(context.exception.code, "CROSS_SOURCE_DIVERGENCE")

    def test_failed_divergence_retains_raw_evidence_and_measurements(self) -> None:
        kraken_fixture = StaticProvider(KrakenPublicProvider.source_id)
        coinbase_fixture = StaticProvider(CoinbaseExchangePublicProvider.source_id)
        latest = coinbase_fixture.candles[-1]
        coinbase_fixture.candles[-1] = replace(
            latest,
            open=latest.open * 1.02,
            high=latest.high * 1.02,
            low=latest.low * 1.02,
            close=latest.close * 1.02,
        )
        consensus = CrossExchangeConsensusProvider(
            (kraken_fixture, coinbase_fixture),
            policy(),
            allow_unapproved_for_testing=True,
        )

        with TemporaryDirectory() as directory:
            repository = ReportRepository(f"{directory}/reports.db")
            report = ResearchOrchestrator(
                provider=consensus,
                policy=policy(),
                repository=repository,
            ).analyze(symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF)
            snapshot = repository.read_snapshot(report.data_snapshot_id)

        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertEqual(report.metadata["input_candle_count"], 240)
        self.assertEqual(len(snapshot or []), 240)
        self.assertEqual(
            {item["id"] for item in report.sources},
            {KrakenPublicProvider.source_id, CoinbaseExchangePublicProvider.source_id},
        )
        diagnostics = report.metadata["provider_diagnostics"]
        self.assertEqual(diagnostics["consensus_failure_code"], "CROSS_SOURCE_DIVERGENCE")
        self.assertGreater(diagnostics["consensus_max_close_divergence_bps"], 100.0)
        self.assertFalse(diagnostics["consensus_passed"])

    def test_consensus_subclass_cannot_forge_attestation(self) -> None:
        class ForgedConsensus(CrossExchangeConsensusProvider):
            def fetch_batch(self, **kwargs):
                batch = super().fetch_batch(**kwargs)
                candles = list(batch.candles)
                latest = candles[-1]
                candles[-1] = replace(
                    latest,
                    high=latest.open * 1.07,
                    low=latest.open * 0.999,
                    close=latest.open * 1.06,
                )
                return replace(batch, candles=tuple(candles))

        forged = ForgedConsensus(
            (
                StaticProvider("venue_a"),
                StaticProvider("venue_b", price_multiplier=1.0005),
            ),
            policy(),
            allow_unapproved_for_testing=True,
        )

        report = ResearchOrchestrator(provider=forged, policy=policy()).analyze(
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=AS_OF,
        )

        self.assertIn("LARGE_PERIOD_MOVE", report.reason_codes)
        self.assertEqual(report.decision, Decision.NO_SIGNAL)
        self.assertFalse(report.metadata["v1_1_consensus_passed"])
        self.assertIn("CONSENSUS_REQUIRED", report.risk.flags)

    def test_single_historical_divergence_is_fail_closed(self) -> None:
        for divergence_kind in ("close", "ohlc"):
            with self.subTest(divergence_kind=divergence_kind):
                left = StaticProvider("venue_a")
                right = StaticProvider("venue_b")
                historical_index = 40
                historical = right.candles[historical_index]
                if divergence_kind == "close":
                    divergent_close = historical.close * 1.02
                    right.candles[historical_index] = replace(
                        historical,
                        high=max(historical.high, divergent_close * 1.001),
                        close=divergent_close,
                    )
                else:
                    right.candles[historical_index] = replace(
                        historical,
                        high=historical.high * 1.10,
                    )

                consensus = CrossExchangeConsensusProvider(
                    (left, right), policy(), allow_unapproved_for_testing=True
                )
                with self.assertRaises(ProviderError) as context:
                    consensus.fetch_candles(
                        symbol="BTC/USD",
                        interval_minutes=1440,
                        as_of=AS_OF,
                        limit=120,
                    )
                self.assertEqual(context.exception.code, "CROSS_SOURCE_DIVERGENCE")

    def test_source_failure_is_fail_closed(self) -> None:
        class FailingProvider:
            source_id = "offline_venue"

            def fetch_candles(self, **kwargs):
                raise ProviderError("offline")

        consensus = CrossExchangeConsensusProvider(
            (StaticProvider("venue_a"), FailingProvider()),
            policy(),
            allow_unapproved_for_testing=True,
        )
        with self.assertRaises(ProviderError) as context:
            consensus.fetch_candles(
                symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=120
            )
        self.assertEqual(context.exception.code, "CONSENSUS_SOURCE_UNAVAILABLE")

    def test_single_source_volume_corruption_is_fail_closed(self) -> None:
        left = StaticProvider("venue_a")
        right = StaticProvider("venue_b")
        right.candles[-1] = replace(right.candles[-1], volume=1e10)
        consensus = CrossExchangeConsensusProvider(
            (left, right), policy(), allow_unapproved_for_testing=True
        )
        with self.assertRaises(ProviderError) as context:
            consensus.fetch_candles(
                symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=120
            )
        self.assertEqual(context.exception.code, "CROSS_SOURCE_VOLUME_DIVERGENCE")

    def test_single_source_historical_volume_corruption_cannot_emit_alert(self) -> None:
        kraken_fixture = StaticProvider(KrakenPublicProvider.source_id)
        coinbase_fixture = StaticProvider(
            CoinbaseExchangePublicProvider.source_id,
            price_multiplier=1.0005,
        )
        for index in range(-30, -5):
            candle = coinbase_fixture.candles[index]
            coinbase_fixture.candles[index] = replace(candle, volume=100_000.0)
        consensus = CrossExchangeConsensusProvider(
            (kraken_fixture, coinbase_fixture),
            policy(),
            allow_unapproved_for_testing=True,
        )

        report = ResearchOrchestrator(provider=consensus, policy=policy()).analyze(
            symbol="BTC/USD",
            interval_minutes=1_440,
            as_of=AS_OF,
        )

        self.assertIsNotNone(report.metrics)
        assert report.metrics is not None
        self.assertLess(report.metrics.volume_zscore, -2.0)
        self.assertFalse(report.metadata["v1_1_volume_anomaly_attested"])
        self.assertNotIn("VOLUME_ANOMALY", report.reason_codes)
        self.assertEqual(report.decision, Decision.NO_SIGNAL)

    def test_finite_but_extreme_volume_is_rejected_without_crashing(self) -> None:
        left = StaticProvider("venue_a")
        right = StaticProvider("venue_b")
        left.candles[-1] = replace(left.candles[-1], volume=1e308)
        right.candles[-1] = replace(right.candles[-1], volume=1e308)
        consensus = CrossExchangeConsensusProvider(
            (left, right), policy(), allow_unapproved_for_testing=True
        )
        with self.assertRaises(ProviderError) as context:
            consensus.fetch_candles(
                symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=120
            )
        self.assertEqual(context.exception.code, "SOURCE_VALUE_INVALID")

    def test_subnormal_volume_scale_is_rejected_before_normalization(self) -> None:
        left = StaticProvider("venue_a")
        right = StaticProvider("venue_b")
        for provider in (left, right):
            provider.candles = [
                replace(candle, volume=5e-324) for candle in provider.candles
            ]
            provider.candles[-1] = replace(provider.candles[-1], volume=1.0)
        consensus = CrossExchangeConsensusProvider(
            (left, right),
            policy(),
            allow_unapproved_for_testing=True,
        )

        with self.assertRaises(ProviderError) as context:
            consensus.fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
                limit=120,
            )
        self.assertEqual(context.exception.code, "SOURCE_VOLUME_INVALID")

    def test_insufficient_overlap_is_fail_closed(self) -> None:
        left = StaticProvider("venue_a")
        right = StaticProvider("venue_b")
        right.candles = right.candles[-59:]
        consensus = CrossExchangeConsensusProvider(
            (left, right), policy(), allow_unapproved_for_testing=True
        )
        with self.assertRaises(ProviderError) as context:
            consensus.fetch_candles(
                symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=120
            )
        self.assertEqual(context.exception.code, "INSUFFICIENT_SOURCE_OVERLAP")

    def test_missing_latest_window_cannot_hide_behind_staleness_tolerance(self) -> None:
        left = StaticProvider("venue_a", count=CONSENSUS_WINDOW_SIZE + 1)
        right = StaticProvider("venue_b", count=CONSENSUS_WINDOW_SIZE + 1)
        for provider in (left, right):
            latest_common = provider.candles[-2]
            provider.candles[-2] = replace(
                latest_common,
                high=latest_common.open * 1.07,
                low=latest_common.open * 0.999,
                close=latest_common.open * 1.06,
            )
        right.candles.pop()
        consensus = CrossExchangeConsensusProvider(
            (left, right), policy(), allow_unapproved_for_testing=True
        )
        with self.assertRaises(ProviderError) as context:
            consensus.fetch_candles(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=AS_OF,
                limit=CONSENSUS_WINDOW_SIZE + 1,
            )
        self.assertEqual(context.exception.code, "CONSENSUS_LATEST_WINDOW_MISSING")

    def test_duplicate_source_ids_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            CrossExchangeConsensusProvider(
                (StaticProvider("same"), StaticProvider("same")),
                policy(),
                allow_unapproved_for_testing=True,
            )

    def test_unapproved_or_synthetic_venues_cannot_attest_consensus(self) -> None:
        with self.assertRaises(ValueError):
            CrossExchangeConsensusProvider(
                (StaticProvider("synthetic_a"), StaticProvider("synthetic_b")), policy()
            )


if __name__ == "__main__":
    unittest.main()

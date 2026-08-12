from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from crypto_agent.domain import (
    BasisReference,
    ContractTransitionEvidence,
    FuturesEvidence,
    MetricStatus,
    OrderBookLevel,
    SessionStatus,
)
from crypto_agent.futures_analytics import analyze_futures
from crypto_agent.futures_policy import FuturesAnalysisPolicy
from crypto_agent.resource_paths import default_futures_policy_path
from tests.helpers import candles

AS_OF = datetime(2026, 8, 10, tzinfo=UTC)
CONTRACT_ID = "CME:MBT:202609"


def _policy() -> FuturesAnalysisPolicy:
    return FuturesAnalysisPolicy.load(default_futures_policy_path())


def _evidence(
    *,
    session_status: SessionStatus = SessionStatus.OPEN,
    observed_at: datetime | None = None,
    basis_source: str = "plus500_t4_index_v1",
    bids: tuple[OrderBookLevel, ...] | None = None,
    asks: tuple[OrderBookLevel, ...] | None = None,
    transition: ContractTransitionEvidence | None = None,
    contract_id: str = CONTRACT_ID,
) -> FuturesEvidence:
    observed = observed_at or AS_OF - timedelta(seconds=2)
    available = observed + timedelta(seconds=1)
    ingested = min(AS_OF, available + timedelta(seconds=1))
    return FuturesEvidence(
        contract_id=contract_id,
        source="plus500_t4_futures_v1",
        session_status=session_status,
        is_full_snapshot=True,
        observed_at=observed,
        available_at=available,
        ingested_at=ingested,
        bids=bids
        or tuple(
            OrderBookLevel(level, 100.0 - level / 10, float(level))
            for level in range(1, 6)
        ),
        asks=asks
        or tuple(
            OrderBookLevel(level, 100.0 + level / 10, float(level + 1))
            for level in range(1, 6)
        ),
        basis_reference=BasisReference(
            symbol="BTC/USD",
            reference_type="index",
            source=basis_source,
            price=99.0,
            observed_at=observed,
            available_at=available,
            ingested_at=ingested,
        ),
        contract_transition=transition,
    )


def _analyze(
    evidence: FuturesEvidence | None,
    *,
    as_of: datetime = AS_OF,
    contract_selection: str = "front_month",
    rolled_from_contract_id: str | None = None,
    data=None,
):
    return analyze_futures(
        list(data if data is not None else candles()),
        evidence,
        as_of=as_of,
        symbol="BTC/USD",
        contract_id=CONTRACT_ID,
        contract_expires_at=AS_OF + timedelta(days=30),
        contract_roll_at=AS_OF + timedelta(days=25),
        contract_selection=contract_selection,
        rolled_from_contract_id=rolled_from_contract_id,
        policy=_policy(),
    )


class FuturesAnalyticsTests(unittest.TestCase):
    def test_complete_evidence_calculates_spread_depth_basis_and_volume(self) -> None:
        result = _analyze(_evidence())
        metrics = result.metrics
        self.assertTrue(result.gate_passed)
        self.assertEqual(metrics.spread_status, MetricStatus.AVAILABLE)
        self.assertAlmostEqual(metrics.spread_bps or 0, 20.0)
        self.assertEqual(metrics.bid_depth, 15.0)
        self.assertEqual(metrics.ask_depth, 20.0)
        self.assertAlmostEqual(metrics.book_imbalance or 0, -5 / 35)
        self.assertAlmostEqual(metrics.basis_bps or 0, 10_000 * (100 / 99 - 1))
        self.assertEqual(metrics.volume_status, MetricStatus.AVAILABLE)
        self.assertEqual(metrics.roll_impact_status, MetricStatus.NOT_APPLICABLE)

    def test_schema_v2_style_missing_evidence_is_explicit_and_fails_closed(self) -> None:
        result = _analyze(None)
        self.assertFalse(result.gate_passed)
        self.assertEqual(result.metrics.spread_status, MetricStatus.UNAVAILABLE)
        self.assertEqual(result.metrics.basis_status, MetricStatus.UNAVAILABLE)
        self.assertIsNone(result.metrics.spread_bps)
        self.assertIn("FUTURES_EVIDENCE_INCOMPLETE", result.veto_flags)

    def test_crossed_book_is_invalid_instead_of_using_candle_range(self) -> None:
        bids = tuple(
            OrderBookLevel(level, 101.0 - level / 10, 1.0)
            for level in range(1, 6)
        )
        asks = tuple(
            OrderBookLevel(level, 100.0 + level / 10, 1.0)
            for level in range(1, 6)
        )
        result = _analyze(_evidence(bids=bids, asks=asks))
        self.assertFalse(result.gate_passed)
        self.assertEqual(result.metrics.spread_status, MetricStatus.INVALID)
        self.assertIn("ORDER_BOOK_INVALID", result.veto_flags)

    def test_stale_snapshot_and_future_timestamp_are_invalid(self) -> None:
        stale = _analyze(_evidence(observed_at=AS_OF - timedelta(minutes=2)))
        self.assertEqual(stale.metrics.spread_status, MetricStatus.INVALID)
        future = _analyze(_evidence(observed_at=AS_OF + timedelta(seconds=1)))
        self.assertEqual(future.metrics.spread_status, MetricStatus.INVALID)

    def test_anonymous_futures_price_cannot_be_used_as_basis_reference(self) -> None:
        result = _analyze(_evidence(basis_source="plus500_t4_futures_v1"))
        self.assertFalse(result.gate_passed)
        self.assertEqual(result.metrics.basis_status, MetricStatus.INVALID)
        self.assertIn("BASIS_REFERENCE_INVALID", result.veto_flags)

    def test_unapproved_basis_source_fails_closed(self) -> None:
        result = _analyze(_evidence(basis_source="totally_untrusted_fake_source"))
        self.assertFalse(result.gate_passed)
        self.assertEqual(result.metrics.basis_status, MetricStatus.INVALID)
        self.assertIn("BASIS_REFERENCE_INVALID", result.veto_flags)

    def test_extreme_finite_volume_fails_closed_without_overflow(self) -> None:
        data = [
            replace(item, volume=1.0 if index % 2 == 0 else 1e308)
            for index, item in enumerate(candles())
        ]
        result = _analyze(_evidence(), data=data)
        self.assertFalse(result.gate_passed)
        self.assertEqual(result.metrics.volume_status, MetricStatus.INVALID)
        self.assertIn("VOLUME_EVIDENCE_INVALID", result.veto_flags)

    def test_overflowing_book_depth_fails_closed(self) -> None:
        bids = tuple(
            OrderBookLevel(level, 100.0 - level / 10, 1e308)
            for level in range(1, 6)
        )
        asks = tuple(
            OrderBookLevel(level, 100.0 + level / 10, 1e308)
            for level in range(1, 6)
        )
        result = _analyze(_evidence(bids=bids, asks=asks))
        self.assertFalse(result.gate_passed)
        self.assertEqual(result.metrics.depth_status, MetricStatus.INVALID)
        self.assertIsNone(result.metrics.book_imbalance)
        self.assertIn("ORDER_BOOK_DEPTH_INVALID", result.veto_flags)

    def test_zero_volume_variance_is_unavailable_not_zero(self) -> None:
        flat = [replace(item, volume=100.0) for item in candles()]
        result = _analyze(_evidence(), data=flat)
        self.assertEqual(result.metrics.volume_status, MetricStatus.UNAVAILABLE)
        self.assertIsNone(result.metrics.volume_zscore)
        self.assertIn("VOLUME_VARIANCE_ZERO", result.reason_codes)

    def test_roll_boundary_is_invalid_for_the_old_contract(self) -> None:
        result = analyze_futures(
            list(candles()),
            _evidence(),
            as_of=AS_OF + timedelta(days=25),
            symbol="BTC/USD",
            contract_id=CONTRACT_ID,
            contract_expires_at=AS_OF + timedelta(days=30),
            contract_roll_at=AS_OF + timedelta(days=25),
            contract_selection="front_month",
            rolled_from_contract_id=None,
            policy=_policy(),
        )
        self.assertEqual(result.metrics.lifecycle_status, MetricStatus.INVALID)
        self.assertIn("CONTRACT_LIFECYCLE_INVALID", result.veto_flags)

    def test_controlled_roll_uses_synchronized_transition_prices(self) -> None:
        transition = ContractTransitionEvidence(
            from_contract_id="CME:MBT:202606",
            to_contract_id=CONTRACT_ID,
            price_type="mid",
            from_price=99.5,
            to_price=100.0,
            source="plus500_t4_futures_v1",
            observed_at=AS_OF - timedelta(seconds=2),
            available_at=AS_OF - timedelta(seconds=1),
            ingested_at=AS_OF,
        )
        result = _analyze(
            _evidence(transition=transition),
            contract_selection="rolled",
            rolled_from_contract_id="CME:MBT:202606",
        )
        self.assertTrue(result.gate_passed)
        self.assertEqual(result.metrics.roll_impact_status, MetricStatus.AVAILABLE)
        self.assertAlmostEqual(
            result.metrics.roll_impact_bps or 0,
            10_000 * (100 / 99.5 - 1),
        )

    def test_roll_transition_must_match_current_book_mid(self) -> None:
        transition = ContractTransitionEvidence(
            from_contract_id="CME:MBT:202606",
            to_contract_id=CONTRACT_ID,
            price_type="mid",
            from_price=99.5,
            to_price=101.0,
            source="plus500_t4_futures_v1",
            observed_at=AS_OF - timedelta(seconds=2),
            available_at=AS_OF - timedelta(seconds=1),
            ingested_at=AS_OF,
        )
        result = _analyze(
            _evidence(transition=transition),
            contract_selection="rolled",
            rolled_from_contract_id="CME:MBT:202606",
        )
        self.assertFalse(result.gate_passed)
        self.assertEqual(result.metrics.roll_impact_status, MetricStatus.INVALID)
        self.assertIn("ROLL_EVIDENCE_INVALID", result.veto_flags)


if __name__ == "__main__":
    unittest.main()

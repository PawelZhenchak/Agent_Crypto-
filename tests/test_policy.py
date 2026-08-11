from __future__ import annotations

import json
import math
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

from crypto_agent.domain import (
    DataQualityReport,
    Decision,
    MarketMetrics,
    ReferencePriceObservation,
    ReferencePriceSnapshot,
    Regime,
)
from crypto_agent.policy import PolicyConfigurationError, RiskGate, RiskPolicy

from tests.helpers import PROJECT_ROOT, policy


class PolicyTests(unittest.TestCase):
    def test_v1_policy_is_strictly_read_only(self) -> None:
        loaded = policy()
        self.assertFalse(loaded.execution_enabled)
        self.assertFalse(loaded.exchange_credentials_allowed)
        self.assertFalse(loaded.leverage_allowed)
        self.assertFalse(loaded.martingale_allowed)
        self.assertEqual(loaded.allowed_intervals_minutes, (240, 1440, 10080))

    def test_execution_cannot_be_enabled_by_config(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "configs/risk_policy.v1.json").read_text(encoding="utf-8")
        )
        payload["execution_enabled"] = True
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unsafe.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(PolicyConfigurationError):
                RiskPolicy.load(path)

    def test_safety_booleans_require_actual_json_booleans(self) -> None:
        loaded = policy()
        invalid_values = (
            ("execution_enabled", 0),
            ("leverage_allowed", 0),
            ("martingale_allowed", ""),
            ("exchange_credentials_allowed", None),
            ("human_approval_required_for_execution", 1),
        )
        for field_name, value in invalid_values:
            with self.subTest(field_name=field_name, value=value):
                with self.assertRaises(PolicyConfigurationError):
                    replace(loaded, **{field_name: value}).validate_v1_safety()

    def test_numeric_policy_fields_reject_bool_and_float_spoofing(self) -> None:
        with self.assertRaises(PolicyConfigurationError):
            replace(
                policy(), max_divergent_candle_fraction=False
            ).validate_v1_safety()
        with self.assertRaises(PolicyConfigurationError):
            replace(policy(), min_consensus_sources=2.0).validate_v1_safety()

    def test_every_integer_policy_field_rejects_boolean_values(self) -> None:
        loaded = policy()
        invalid_values = (
            ("allowed_intervals_minutes", (240, 1440, True)),
            ("policy_schema_version", True),
            ("min_samples", True),
            ("max_reference_price_age_seconds", True),
            ("report_ttl_seconds", True),
            ("max_clock_skew_seconds", False),
            ("min_consensus_sources", True),
            ("min_consensus_overlap", True),
        )
        for field_name, value in invalid_values:
            with self.subTest(field_name=field_name):
                with self.assertRaises(PolicyConfigurationError):
                    replace(loaded, **{field_name: value}).validate_v1_safety()

    def test_every_integer_policy_field_requires_exact_int_type(self) -> None:
        loaded = policy()
        invalid_values = (
            ("allowed_intervals_minutes", (240.0, 1440.0, 10080.0)),
            ("policy_schema_version", 2.0),
            ("min_samples", 60.0),
            ("max_reference_price_age_seconds", 300.0),
            ("report_ttl_seconds", 3600.0),
            ("max_clock_skew_seconds", 30.0),
            ("min_consensus_sources", 2.0),
            ("min_consensus_overlap", 60.0),
        )
        for field_name, value in invalid_values:
            with self.subTest(field_name=field_name):
                with self.assertRaises(PolicyConfigurationError):
                    replace(loaded, **{field_name: value}).validate_v1_safety()

    def test_hard_thresholds_cannot_be_weakened(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "configs/risk_policy.v1.json").read_text(encoding="utf-8")
        )
        payload["min_data_quality"] = 0.0
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "weak.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(PolicyConfigurationError):
                RiskPolicy.load(path)

    def test_non_finite_threshold_is_rejected(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "configs/risk_policy.v1.json").read_text(encoding="utf-8")
        )
        payload["max_staleness_multiplier"] = math.inf
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "infinite.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(PolicyConfigurationError):
                RiskPolicy.load(path)

    def test_consensus_thresholds_cannot_be_weakened(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "configs/risk_policy.v1.json").read_text(encoding="utf-8")
        )
        payload["max_cross_source_divergence_bps"] = 101
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "weak-consensus.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(PolicyConfigurationError):
                RiskPolicy.load(path)

    def test_reference_price_contract_is_explicit_and_cannot_be_weakened(self) -> None:
        loaded = policy()
        self.assertEqual(loaded.policy_schema_version, 2)
        self.assertEqual(loaded.max_reference_price_age_seconds, 300)
        self.assertEqual(
            loaded.max_reference_price_deviation_from_median_bps,
            50.0,
        )
        self.assertEqual(loaded.max_pairwise_reference_price_divergence_bps, 100.0)
        for changes in (
            {"max_reference_price_age_seconds": 301},
            {"max_reference_price_age_seconds": 299},
            {"max_reference_price_deviation_from_median_bps": 51.0},
            {
                "max_reference_price_deviation_from_median_bps": 40.0,
                "max_cross_source_divergence_bps": 100.0,
            },
        ):
            with self.subTest(changes=changes):
                with self.assertRaises(PolicyConfigurationError):
                    replace(loaded, **changes).validate_v1_safety()

    def test_two_source_pairwise_limit_equals_fifty_bps_from_median(self) -> None:
        loaded = policy()
        left, right = 99.5, 100.5
        midpoint_median = (left + right) / 2.0
        pairwise_bps = abs(left - right) / midpoint_median * 10_000.0
        per_source_bps = max(
            abs(left - midpoint_median),
            abs(right - midpoint_median),
        ) / midpoint_median * 10_000.0

        self.assertAlmostEqual(pairwise_bps, 100.0)
        self.assertAlmostEqual(per_source_bps, 50.0)
        self.assertEqual(
            pairwise_bps,
            loaded.max_pairwise_reference_price_divergence_bps,
        )
        self.assertEqual(
            per_source_bps,
            loaded.max_reference_price_deviation_from_median_bps,
        )

    def test_consensus_overlap_is_the_exact_versioned_window(self) -> None:
        loaded = policy()
        self.assertEqual(loaded.min_consensus_overlap, 120)
        for invalid in (119, 121):
            with self.subTest(invalid=invalid):
                with self.assertRaises(PolicyConfigurationError):
                    replace(
                        loaded, min_consensus_overlap=invalid
                    ).validate_v1_safety()

    def test_programmatic_policy_cannot_weaken_risk_gate(self) -> None:
        weakened = replace(
            policy(),
            max_cross_source_divergence_bps=1_000_000_000.0,
            max_cross_source_ohlc_divergence_bps=1_000_000_000.0,
            max_cross_source_volume_zscore_delta=1_000_000_000.0,
            max_divergent_candle_fraction=1.0,
        )
        with self.assertRaises(PolicyConfigurationError):
            RiskGate(weakened)

    def test_risk_gate_rejects_non_finite_inputs_independently(self) -> None:
        assessment = self._risk_gate_assessment(
            metrics=replace(self._valid_metrics(), volume_zscore=float("nan"))
        )
        self.assertTrue(assessment.vetoed)
        self.assertEqual(assessment.decision, Decision.NO_SIGNAL)
        self.assertIn("INVALID_RISK_INPUT", assessment.flags)

    def test_risk_gate_rejects_invalid_quality_score_independently(self) -> None:
        quality = replace(self._valid_quality(), score=float("nan"))
        assessment = self._risk_gate_assessment(quality=quality)
        self.assertTrue(assessment.vetoed)
        self.assertIn("INVALID_RISK_INPUT", assessment.flags)

    def test_risk_gate_rejects_bad_hash_expiry_and_decision(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        assessment = RiskGate(policy()).evaluate(
            symbol="BTC/USD",
            interval_minutes=1440,
            quality=self._valid_quality(),
            metrics=self._valid_metrics(),
            reference_price=self._valid_reference_price(as_of),
            as_of=as_of,
            expires_at=as_of,
            input_fingerprint_sha256="not-a-sha256",
            consensus_passed=True,
            proposed_decision="BUY",  # type: ignore[arg-type]
        )
        self.assertTrue(assessment.vetoed)
        self.assertEqual(assessment.decision, Decision.NO_SIGNAL)
        self.assertIn("INVALID_RISK_INPUT", assessment.flags)
        self.assertEqual(assessment.input_fingerprint_sha256, "0" * 64)
        self.assertGreater(assessment.expires_at, assessment.as_of)

    def test_risk_gate_rejects_expiry_beyond_policy_ttl(self) -> None:
        loaded = policy()
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        assessment = RiskGate(loaded).evaluate(
            symbol="BTC/USD",
            interval_minutes=1440,
            quality=self._valid_quality(),
            metrics=self._valid_metrics(),
            reference_price=self._valid_reference_price(as_of),
            as_of=as_of,
            expires_at=as_of + timedelta(seconds=loaded.report_ttl_seconds + 1),
            input_fingerprint_sha256="a" * 64,
            consensus_passed=True,
            proposed_decision=Decision.ALERT,
        )
        self.assertTrue(assessment.vetoed)
        self.assertEqual(assessment.decision, Decision.NO_SIGNAL)
        self.assertIn("INVALID_RISK_INPUT", assessment.flags)
        self.assertEqual(
            assessment.expires_at,
            as_of + timedelta(seconds=loaded.report_ttl_seconds),
        )

    def test_positive_history_requires_timestamp_evidence(self) -> None:
        quality_without_lineage = replace(
            self._valid_quality(),
            newest_observed_at=None,
            newest_available_at=None,
        )
        assessment = self._risk_gate_assessment(quality=quality_without_lineage)
        self.assertTrue(assessment.vetoed)
        self.assertEqual(assessment.decision, Decision.NO_SIGNAL)
        self.assertIn("INVALID_RISK_INPUT", assessment.flags)

    def test_malformed_quality_collections_fail_closed_without_crashing(self) -> None:
        malformed_reports = (
            replace(self._valid_quality(), flags=None),  # type: ignore[arg-type]
            replace(self._valid_quality(), critical_flags=None),  # type: ignore[arg-type]
            replace(
                self._valid_quality(),
                critical_flags=(["STALE_DATA"],),  # type: ignore[arg-type,list-item]
            ),
        )

        for quality in malformed_reports:
            with self.subTest(quality=quality):
                assessment = self._risk_gate_assessment(quality=quality)
                self.assertTrue(assessment.vetoed)
                self.assertEqual(assessment.decision, Decision.NO_SIGNAL)
                self.assertIn("INVALID_RISK_INPUT", assessment.flags)

    def test_risk_gate_independently_rejects_a_stale_reference_price(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        at_boundary = self._valid_reference_price(
            as_of,
            event_time=as_of - timedelta(seconds=300),
        )
        after_boundary = as_of + timedelta(microseconds=1)
        stale = self._valid_reference_price(
            after_boundary,
            event_time=as_of - timedelta(seconds=300),
        )

        accepted = self._risk_gate_assessment(reference_price=at_boundary)
        rejected = self._risk_gate_assessment(
            reference_price=stale,
            as_of=after_boundary,
        )

        self.assertNotIn("STALE_DATA", accepted.flags)
        self.assertIn("STALE_DATA", rejected.flags)
        self.assertTrue(rejected.vetoed)
        self.assertEqual(rejected.decision, Decision.NO_SIGNAL)

    def test_risk_gate_recomputes_two_source_median_threshold(self) -> None:
        accepted = self._risk_gate_assessment(
            reference_price=self._valid_reference_price(
                datetime(2026, 8, 10, tzinfo=timezone.utc),
                prices=(99.5, 100.5),
            )
        )
        rejected = self._risk_gate_assessment(
            reference_price=self._valid_reference_price(
                datetime(2026, 8, 10, tzinfo=timezone.utc),
                prices=(99.49, 100.51),
            )
        )

        self.assertNotIn("DATA_CONFLICT", accepted.flags)
        self.assertIn("DATA_CONFLICT", rejected.flags)
        self.assertTrue(rejected.vetoed)

    def test_risk_gate_requires_exact_reference_price_provenance(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        snapshot = self._valid_reference_price(as_of)
        wrong_source = replace(
            snapshot.observations[1],
            source="kraken_spot_rest_v1",
        )
        rejected = self._risk_gate_assessment(
            reference_price=replace(
                snapshot,
                observations=(snapshot.observations[0], wrong_source),
            )
        )
        missing = self._risk_gate_assessment(reference_price=None)

        self.assertIn("REFERENCE_PRICE_INVALID", rejected.flags)
        self.assertIn("REFERENCE_PRICE_MISSING", missing.flags)
        self.assertTrue(rejected.vetoed)
        self.assertTrue(missing.vetoed)

    def test_risk_gate_rejects_malformed_reference_fields_without_crashing(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        snapshot = self._valid_reference_price(as_of)
        malformed = replace(
            snapshot.observations[0],
            source=["kraken_spot_rest_v1"],  # type: ignore[arg-type]
        )

        rejected = self._risk_gate_assessment(
            reference_price=replace(
                snapshot,
                observations=(malformed, snapshot.observations[1]),
            )
        )

        self.assertTrue(rejected.vetoed)
        self.assertEqual(rejected.decision, Decision.NO_SIGNAL)
        self.assertIn("REFERENCE_PRICE_INVALID", rejected.flags)

    @staticmethod
    def _valid_quality() -> DataQualityReport:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        return DataQualityReport(
            score=1.0,
            sample_count=120,
            flags=(),
            critical_flags=(),
            newest_observed_at=as_of - timedelta(hours=12),
            newest_available_at=as_of - timedelta(seconds=1),
        )

    @staticmethod
    def _valid_metrics() -> MarketMetrics:
        return MarketMetrics(
            last_price=100,
            period_return=0.01,
            return_7_periods=0.03,
            annualized_volatility=0.3,
            max_drawdown=-0.1,
            sma_20=101,
            sma_50=100,
            volume_zscore=0.5,
            regime=Regime.RANGE,
        )

    def _risk_gate_assessment(
        self,
        *,
        quality: DataQualityReport | None = None,
        metrics: MarketMetrics | None = None,
        reference_price: ReferencePriceSnapshot | None | object = ..., 
        as_of: datetime | None = None,
    ):
        evaluation_time = as_of or datetime(2026, 8, 10, tzinfo=timezone.utc)
        selected_reference = (
            self._valid_reference_price(evaluation_time)
            if reference_price is ...
            else reference_price
        )
        return RiskGate(policy()).evaluate(
            symbol="BTC/USD",
            interval_minutes=1440,
            quality=quality or self._valid_quality(),
            metrics=metrics or self._valid_metrics(),
            reference_price=selected_reference,  # type: ignore[arg-type]
            as_of=evaluation_time,
            expires_at=evaluation_time + timedelta(hours=1),
            input_fingerprint_sha256="a" * 64,
            consensus_passed=True,
            proposed_decision=Decision.ALERT,
        )

    @staticmethod
    def _valid_reference_price(
        as_of: datetime,
        *,
        event_time: datetime | None = None,
        prices: tuple[float, float] = (99.5, 100.5),
    ) -> ReferencePriceSnapshot:
        observed_at = event_time or as_of - timedelta(minutes=1)
        available_at = min(as_of, observed_at + timedelta(seconds=1))
        return ReferencePriceSnapshot(
            symbol="BTC/USD",
            observations=(
                ReferencePriceObservation(
                    symbol="BTC/USD",
                    price=prices[0],
                    event_time=observed_at,
                    available_at=available_at,
                    ingested_at=available_at,
                    source="kraken_spot_rest_v1",
                ),
                ReferencePriceObservation(
                    symbol="BTC/USD",
                    price=prices[1],
                    event_time=observed_at,
                    available_at=available_at,
                    ingested_at=available_at,
                    source="coinbase_exchange_spot_rest_v1",
                ),
            ),
        )


if __name__ == "__main__":
    unittest.main()

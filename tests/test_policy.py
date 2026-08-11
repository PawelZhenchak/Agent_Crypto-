from __future__ import annotations

import json
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


class Plus500T4PolicyTests(unittest.TestCase):
    def test_policy_is_single_source_and_read_only(self) -> None:
        loaded = policy()
        self.assertEqual(loaded.policy_schema_version, 3)
        self.assertEqual(loaded.required_source_count, 1)
        self.assertEqual(loaded.required_history_candles, 120)
        self.assertFalse(loaded.execution_enabled)
        self.assertFalse(loaded.exchange_credentials_allowed)
        self.assertFalse(loaded.leverage_allowed)

    def test_source_count_cannot_be_weakened_or_changed(self) -> None:
        for value in (0, 2, True, 1.0):
            with self.subTest(value=value):
                with self.assertRaises(PolicyConfigurationError):
                    replace(policy(), required_source_count=value).validate_v1_safety()

    def test_execution_cannot_be_enabled(self) -> None:
        payload = json.loads(
            (PROJECT_ROOT / "configs/risk_policy.v1.json").read_text(encoding="utf-8")
        )
        payload["execution_enabled"] = True
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "unsafe.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaises(PolicyConfigurationError):
                RiskPolicy.load(path)

    def test_fresh_t4_reference_price_can_pass_gate(self) -> None:
        loaded = policy()
        now = datetime(2026, 8, 11, tzinfo=timezone.utc)
        quality = DataQualityReport(1.0, 120, (), (), now, now)
        metrics = MarketMetrics(
            100.0, 0.01, 0.02, 0.2, -0.1, 99.0, 98.0, 3.5, Regime.TREND_UP
        )
        reference = ReferencePriceSnapshot(
            symbol="BTC/USD",
            observations=(
                ReferencePriceObservation(
                    "BTC/USD", 100.0, now - timedelta(minutes=1), now, now,
                    "plus500_t4_futures_v1",
                ),
            ),
        )
        result = RiskGate(loaded).evaluate(
            symbol="BTC/USD",
            interval_minutes=1440,
            quality=quality,
            metrics=metrics,
            reference_price=reference,
            as_of=now,
            expires_at=now + timedelta(minutes=30),
            input_fingerprint_sha256="a" * 64,
            source_attested=True,
            proposed_decision=Decision.ALERT,
        )
        self.assertFalse(result.vetoed)
        self.assertEqual(result.decision, Decision.ALERT)

    def test_other_reference_source_is_rejected(self) -> None:
        loaded = policy()
        now = datetime(2026, 8, 11, tzinfo=timezone.utc)
        quality = DataQualityReport(1.0, 120, (), (), now, now)
        reference = ReferencePriceSnapshot(
            symbol="BTC/USD",
            observations=(
                ReferencePriceObservation(
                    "BTC/USD", 100.0, now, now, now, "unapproved_source"
                ),
            ),
        )
        result = RiskGate(loaded).evaluate(
            symbol="BTC/USD",
            interval_minutes=1440,
            quality=quality,
            metrics=None,
            reference_price=reference,
            as_of=now,
            expires_at=now + timedelta(minutes=30),
            input_fingerprint_sha256="a" * 64,
            source_attested=True,
        )
        self.assertTrue(result.vetoed)
        self.assertIn("REFERENCE_PRICE_INVALID", result.flags)


if __name__ == "__main__":
    unittest.main()

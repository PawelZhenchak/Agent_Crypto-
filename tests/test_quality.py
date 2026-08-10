from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from crypto_agent.quality import assess_data_quality

from tests.helpers import candles, policy


class DataQualityTests(unittest.TestCase):
    def assess(self, data, as_of):
        return assess_data_quality(
            data,
            as_of=as_of,
            interval_minutes=1440,
            expected_symbol="BTC/USD",
            expected_source="synthetic_fixture_v1",
            policy=policy(),
        )

    def test_clean_point_in_time_data_passes(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        result = self.assess(candles(), as_of)
        self.assertTrue(result.passed)
        self.assertGreaterEqual(result.score, policy().min_data_quality)

    def test_future_available_data_is_vetoed(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        data = candles()
        data[-1] = replace(data[-1], available_at=as_of + timedelta(days=1))
        result = self.assess(data, as_of)
        self.assertIn("LOOKAHEAD_DATA", result.critical_flags)
        self.assertFalse(result.passed)

    def test_stale_data_is_vetoed(self) -> None:
        as_of = datetime(2026, 8, 12, tzinfo=timezone.utc)
        result = self.assess(candles(), as_of)
        self.assertIn("STALE_DATA", result.critical_flags)

    def test_post_cutoff_ingestion_is_vetoed(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        data = candles()
        data[-1] = replace(data[-1], ingested_at=as_of + timedelta(microseconds=1))
        result = self.assess(data, as_of)
        self.assertIn("POST_CUTOFF_INGESTION", result.critical_flags)

    def test_mismatched_instrument_is_vetoed(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        data = candles()
        data[-1] = replace(data[-1], symbol="ETH/USD", interval_minutes=10080)
        result = self.assess(data, as_of)
        self.assertIn("UNEXPECTED_SYMBOL", result.critical_flags)
        self.assertIn("UNEXPECTED_INTERVAL", result.critical_flags)

    def test_mismatched_source_is_vetoed(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        data = candles()
        data[-1] = replace(data[-1], source="spoofed_source")
        result = self.assess(data, as_of)
        self.assertIn("UNEXPECTED_SOURCE", result.critical_flags)

    def test_non_finite_value_is_vetoed(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        data = candles()
        data[-1] = replace(data[-1], volume=float("nan"))
        result = self.assess(data, as_of)
        self.assertIn("NON_FINITE_VALUE", result.critical_flags)
        self.assertEqual(result.score, 0.0)

    def test_naive_timestamp_is_vetoed_without_crash(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        data = candles()
        data[-1] = replace(data[-1], open_time=data[-1].open_time.replace(tzinfo=None))
        result = self.assess(data, as_of)
        self.assertIn("INVALID_TIMEZONE", result.critical_flags)

    def test_invalid_timestamp_order_is_vetoed(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
        data = candles()
        data[-1] = replace(data[-1], available_at=data[-1].close_time - timedelta(seconds=1))
        result = self.assess(data, as_of)
        self.assertIn("INVALID_TIME_ORDER", result.critical_flags)


if __name__ == "__main__":
    unittest.main()

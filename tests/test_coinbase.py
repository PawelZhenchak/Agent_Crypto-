from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from crypto_agent.providers.base import ProviderError
from crypto_agent.providers.coinbase import (
    CoinbaseExchangePublicProvider,
    _NoRedirectHandler,
)


UTC = timezone.utc


def _row(
    open_time: datetime,
    *,
    low: str = "100",
    high: str = "120",
    open_price: str = "105",
    close_price: str = "115",
    volume: str = "10",
) -> list[object]:
    return [
        int(open_time.timestamp()),
        low,
        high,
        open_price,
        close_price,
        volume,
    ]


class _Response:
    def __init__(self, payload: object) -> None:
        self._raw = json.dumps(payload).encode("utf-8")
        self.headers = {"Content-Length": str(len(self._raw))}

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self, size: int) -> bytes:
        return self._raw[:size]


class _RedirectedResponse(_Response):
    @staticmethod
    def geturl() -> str:
        return "https://attacker.example/fake-coinbase-feed"


class CoinbaseExchangeProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.provider = CoinbaseExchangePublicProvider()

    def test_daily_parser_sorts_rows_and_drops_current_incomplete_bucket(self) -> None:
        payload = [
            _row(datetime(2026, 8, 10, tzinfo=UTC), high="140", close_price="130"),
            _row(datetime(2026, 8, 9, tzinfo=UTC), close_price="119"),
            _row(datetime(2026, 8, 8, tzinfo=UTC), close_price="118"),
        ]

        candles = self.provider.parse_payload(
            payload,
            symbol="BTC/USD",
            interval_minutes=1_440,
            as_of=datetime(2026, 8, 10, 12, tzinfo=UTC),
            limit=3,
        )

        self.assertEqual([candle.open_time.day for candle in candles], [8, 9])
        self.assertEqual(candles[-1].close, 119.0)
        self.assertTrue(all(candle.source == self.provider.source_id for candle in candles))

    def test_four_hour_candle_is_built_from_four_complete_hourly_buckets(self) -> None:
        start = datetime(2026, 8, 10, 4, tzinfo=UTC)
        payload = [
            _row(
                start + timedelta(hours=index),
                low=str(100 - index),
                high=str(120 + index),
                open_price=str(105 + index),
                close_price=str(110 + index),
                volume=str(index + 1),
            )
            for index in reversed(range(4))
        ]

        candles = self.provider.parse_payload(
            payload,
            symbol="ETH/USD",
            interval_minutes=240,
            as_of=datetime(2026, 8, 10, 8, 30, tzinfo=UTC),
            limit=1,
        )

        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].open_time, start)
        self.assertEqual(candles[0].close_time, start + timedelta(hours=4))
        self.assertEqual(candles[0].open, 105.0)
        self.assertEqual(candles[0].close, 113.0)
        self.assertEqual(candles[0].low, 97.0)
        self.assertEqual(candles[0].high, 123.0)
        self.assertEqual(candles[0].volume, 10.0)

    def test_missing_hour_is_preserved_as_a_gap(self) -> None:
        start = datetime(2026, 8, 10, 4, tzinfo=UTC)
        payload = [_row(start + timedelta(hours=index)) for index in (0, 1, 3)]

        candles = self.provider.parse_payload(
            payload,
            symbol="BTC/USD",
            interval_minutes=240,
            as_of=datetime(2026, 8, 10, 8, tzinfo=UTC),
            limit=1,
        )

        self.assertEqual(candles, [])

    def test_week_is_aligned_to_thursday_utc_and_requires_seven_days(self) -> None:
        thursday = datetime(2026, 8, 6, tzinfo=UTC)
        payload = [
            _row(
                thursday + timedelta(days=index),
                open_price=str(105 + index),
                close_price=str(106 + index),
                volume="2",
            )
            for index in range(7)
        ]

        candles = self.provider.parse_payload(
            payload,
            symbol="BTC/USD",
            interval_minutes=10_080,
            as_of=datetime(2026, 8, 13, 12, tzinfo=UTC),
            limit=1,
        )

        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].open_time, thursday)
        self.assertEqual(candles[0].close_time, thursday + timedelta(days=7))
        self.assertEqual(candles[0].open, 105.0)
        self.assertEqual(candles[0].close, 112.0)
        self.assertEqual(candles[0].volume, 14.0)

    def test_weekly_history_supplies_exact_120_kraken_aligned_windows(self) -> None:
        target_end = datetime(2026, 8, 13, tzinfo=UTC)
        first = target_end - timedelta(days=7 * 120)
        payload = [_row(first + timedelta(days=index)) for index in range(7 * 120)]

        candles = self.provider.parse_payload(
            payload,
            symbol="BTC/USD",
            interval_minutes=10_080,
            as_of=target_end + timedelta(hours=12),
            limit=120,
        )

        self.assertEqual(len(candles), 120)
        self.assertTrue(all(item.open_time.weekday() == 3 for item in candles))

    def test_conflicting_duplicate_is_rejected(self) -> None:
        open_time = datetime(2026, 8, 9, tzinfo=UTC)
        payload = [_row(open_time), _row(open_time, close_price="116")]

        with self.assertRaises(ProviderError):
            self.provider.parse_payload(
                payload,
                symbol="BTC/USD",
                interval_minutes=1_440,
                as_of=datetime(2026, 8, 10, tzinfo=UTC),
                limit=1,
            )

    def test_exact_duplicate_is_deduplicated_for_overlapping_pages(self) -> None:
        open_time = datetime(2026, 8, 9, tzinfo=UTC)
        row = _row(open_time)

        candles = self.provider.parse_payload(
            [row, list(row)],
            symbol="BTC/USD",
            interval_minutes=1_440,
            as_of=datetime(2026, 8, 10, tzinfo=UTC),
            limit=1,
        )

        self.assertEqual(len(candles), 1)
        self.assertEqual(candles[0].open_time, open_time)

    def test_non_finite_and_inconsistent_values_are_rejected(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=UTC)
        with self.assertRaises(ProviderError):
            self.provider.parse_payload(
                [_row(datetime(2026, 8, 9, tzinfo=UTC), high="nan")],
                symbol="BTC/USD",
                interval_minutes=1_440,
                as_of=as_of,
                limit=1,
            )
        with self.assertRaises(ProviderError):
            self.provider.parse_payload(
                [_row(datetime(2026, 8, 9, tzinfo=UTC), high="110", close_price="115")],
                symbol="BTC/USD",
                interval_minutes=1_440,
                as_of=as_of,
                limit=1,
            )

    def test_fetch_pages_ranges_at_no_more_than_300_source_buckets(self) -> None:
        requests: list[str] = []

        def fake_urlopen(request: object, timeout: float) -> _Response:
            del timeout
            requests.append(request.full_url)  # type: ignore[attr-defined]
            return _Response([])

        with patch("crypto_agent.providers.coinbase.urlopen", side_effect=fake_urlopen):
            candles = self.provider.fetch_candles(
                symbol="BTC/USD",
                interval_minutes=240,
                as_of=datetime(2026, 8, 10, tzinfo=UTC),
                limit=100,
            )

        self.assertEqual(candles, [])
        self.assertEqual(len(requests), 2)  # 100 x 4 hourly source buckets
        for url in requests:
            parsed = urlsplit(url)
            query = parse_qs(parsed.query)
            self.assertEqual(parsed.hostname, "api.exchange.coinbase.com")
            self.assertEqual(parsed.path, "/products/BTC-USD/candles")
            self.assertEqual(query["granularity"], ["3600"])
            start = datetime.fromisoformat(query["start"][0].replace("Z", "+00:00"))
            end = datetime.fromisoformat(query["end"][0].replace("Z", "+00:00"))
            self.assertLessEqual(end - start, timedelta(hours=299))

    def test_request_boundaries_fail_closed(self) -> None:
        as_of = datetime(2026, 8, 10, tzinfo=UTC)
        for symbol in ("SOL/USD", "BTC-USD"):
            with self.assertRaises(ProviderError):
                self.provider.fetch_candles(
                    symbol=symbol,
                    interval_minutes=1_440,
                    as_of=as_of,
                    limit=1,
                )
        for interval in (60, 360, 10_081):
            with self.assertRaises(ProviderError):
                self.provider.fetch_candles(
                    symbol="BTC/USD",
                    interval_minutes=interval,
                    as_of=as_of,
                    limit=1,
                )
        for limit in (0, 301, True):
            with self.assertRaises(ProviderError):
                self.provider.fetch_candles(
                    symbol="BTC/USD",
                    interval_minutes=1_440,
                    as_of=as_of,
                    limit=limit,  # type: ignore[arg-type]
                )

    def test_base_url_and_timeout_are_pinned(self) -> None:
        invalid_urls = (
            "http://api.exchange.coinbase.com",
            "https://example.com",
            "https://api.exchange.coinbase.com/private",
            "https://api.exchange.coinbase.com:443",
            "https://api.exchange.coinbase.com?redirect=https://example.com",
        )
        for url in invalid_urls:
            with self.assertRaises(ValueError):
                CoinbaseExchangePublicProvider(base_url=url)
        for timeout in (0, -1, float("nan"), True):
            with self.assertRaises(ValueError):
                CoinbaseExchangePublicProvider(timeout_seconds=timeout)

    def test_origin_and_timeout_cannot_be_mutated_after_construction(self) -> None:
        provider = CoinbaseExchangePublicProvider(timeout_seconds=7)

        with self.assertRaises(AttributeError):
            provider.base_url = "https://attacker.example"  # type: ignore[misc]
        with self.assertRaises(AttributeError):
            provider._base_url = "https://attacker.example"  # type: ignore[attr-defined]
        with self.assertRaises(AttributeError):
            provider.timeout_seconds = 99  # type: ignore[misc]
        with self.assertRaises(AttributeError):
            provider._immutable_configuration_attributes = frozenset()  # type: ignore[misc]
        with self.assertRaises(AttributeError):
            del provider.base_url  # type: ignore[misc]

        self.assertEqual(provider.base_url, "https://api.exchange.coinbase.com")
        self.assertEqual(provider.timeout_seconds, 7.0)

    def test_cross_origin_redirect_is_rejected_before_payload_read(self) -> None:
        with patch(
            "crypto_agent.providers.coinbase.urlopen",
            return_value=_RedirectedResponse([]),
        ):
            with self.assertRaises(ProviderError) as context:
                self.provider.fetch_candles(
                    symbol="BTC/USD",
                    interval_minutes=1_440,
                    as_of=datetime(2026, 8, 10, tzinfo=UTC),
                    limit=1,
                )
        self.assertEqual(context.exception.code, "SOURCE_ORIGIN_MISMATCH")

    def test_redirect_handler_never_creates_a_followup_request(self) -> None:
        handler = _NoRedirectHandler()
        redirected = handler.redirect_request(
            req=object(),  # type: ignore[arg-type]
            fp=None,
            code=302,
            msg="Found",
            headers={"Location": "https://attacker.example/internal"},
            newurl="https://attacker.example/internal",
        )
        self.assertIsNone(redirected)


if __name__ == "__main__":
    unittest.main()

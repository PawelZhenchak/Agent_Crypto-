from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from crypto_agent.providers.kraken import KrakenPublicProvider, _NoRedirectHandler
from crypto_agent.providers.base import ProviderError


class _RedirectedResponse:
    headers: dict[str, str] = {}

    def __enter__(self) -> _RedirectedResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    @staticmethod
    def geturl() -> str:
        return "https://attacker.example/fake-kraken-feed"

    @staticmethod
    def read(size: int) -> bytes:
        del size
        return b"{}"


class _Response:
    def __init__(self, payload: object, url: str) -> None:
        self._raw = json.dumps(payload).encode("utf-8")
        self._url = url
        self.headers = {"Content-Length": str(len(self._raw))}

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def geturl(self) -> str:
        return self._url

    def read(self, size: int) -> bytes:
        return self._raw[:size]


class KrakenProviderTests(unittest.TestCase):
    def test_reference_price_uses_a_separate_latest_closed_one_minute_candle(self) -> None:
        noon = datetime(2026, 8, 10, 12, tzinfo=timezone.utc)
        rows = [
            [
                int((noon - timedelta(minutes=2 - index)).timestamp()),
                "100",
                "120",
                "90",
                str(111 + index),
                "100",
                "1",
                1,
            ]
            for index in range(3)
        ]
        payload = {"error": [], "result": {"XXBTZUSD": rows, "last": rows[-1][0]}}

        observation = KrakenPublicProvider().parse_reference_price_payload(
            payload,
            symbol="BTC/USD",
            as_of=noon,
        )

        self.assertEqual(observation.price, 112.0)
        self.assertEqual(observation.event_time, noon)
        self.assertEqual(observation.symbol, "BTC/USD")
        self.assertEqual(observation.source, KrakenPublicProvider.source_id)
        self.assertLessEqual(observation.event_time, observation.available_at)
        self.assertLessEqual(observation.available_at, observation.ingested_at)

    def test_reference_price_request_is_pinned_to_one_minute_public_ohlc(self) -> None:
        noon = datetime(2026, 8, 10, 12, tzinfo=timezone.utc)
        current_open = noon
        rows = [
            [
                int((noon - timedelta(minutes=1)).timestamp()),
                "100",
                "120",
                "90",
                "115",
                "100",
                "1",
                1,
            ],
            [
                int(current_open.timestamp()),
                "100",
                "120",
                "90",
                "116",
                "100",
                "1",
                1,
            ],
        ]
        payload = {"error": [], "result": {"XXBTZUSD": rows, "last": rows[-1][0]}}
        requests: list[str] = []

        def fake_urlopen(request: object, timeout: float) -> _Response:
            del timeout
            url = request.full_url  # type: ignore[attr-defined]
            requests.append(url)
            return _Response(payload, url)

        with patch("crypto_agent.providers.kraken.urlopen", side_effect=fake_urlopen):
            observation = KrakenPublicProvider().fetch_reference_price(
                symbol="BTC/USD",
                as_of=noon,
            )

        self.assertEqual(observation.event_time, noon)
        self.assertEqual(len(requests), 1)
        parsed = urlsplit(requests[0])
        self.assertEqual(parsed.path, "/0/public/OHLC")
        self.assertEqual(parse_qs(parsed.query)["interval"], ["1"])

    def test_reference_price_does_not_fall_back_to_an_older_closed_minute(self) -> None:
        noon = datetime(2026, 8, 10, 12, tzinfo=timezone.utc)
        rows = [
            [
                int((noon - timedelta(minutes=4)).timestamp()),
                "100",
                "120",
                "90",
                "115",
                "100",
                "1",
                1,
            ],
            [
                int(noon.timestamp()),
                "100",
                "120",
                "90",
                "116",
                "100",
                "1",
                1,
            ],
        ]
        payload = {"error": [], "result": {"XXBTZUSD": rows, "last": rows[-1][0]}}

        with self.assertRaises(ProviderError) as raised:
            KrakenPublicProvider().parse_reference_price_payload(
                payload,
                symbol="BTC/USD",
                as_of=noon,
            )

        self.assertEqual(raised.exception.code, "REFERENCE_PRICE_UNAVAILABLE")

    def test_parser_drops_uncommitted_last_candle(self) -> None:
        payload = {
            "error": [],
            "result": {
                "XXBTZUSD": [
                    [1723075200, "55000", "56000", "54000", "55500", "55200", "10", 100],
                    [1723161600, "55500", "57000", "55000", "56500", "56000", "12", 120],
                    [1723248000, "56500", "58000", "56000", "57500", "57000", "8", 80],
                ],
                "last": 1723248000,
            },
        }
        provider = KrakenPublicProvider()
        candles = provider.parse_payload(
            payload,
            symbol="BTC/USD",
            interval_minutes=1440,
            as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
            limit=100,
        )
        self.assertEqual(len(candles), 2)
        self.assertEqual(candles[-1].close, 56500.0)

    def test_wrong_response_pair_is_rejected(self) -> None:
        payload = {
            "error": [],
            "result": {"XETHZUSD": [[1723075200, "1", "1", "1", "1", "1", "1", 1]]},
        }
        with self.assertRaises(ProviderError):
            KrakenPublicProvider().parse_payload(
                payload,
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
                limit=100,
            )

    def test_base_url_is_pinned(self) -> None:
        with self.assertRaises(ValueError):
            KrakenPublicProvider(base_url="http://api.kraken.com")
        with self.assertRaises(ValueError):
            KrakenPublicProvider(base_url="https://example.com")
        with self.assertRaises(ValueError):
            KrakenPublicProvider(base_url="https://api.kraken.com?redirect=example.com")
        for timeout in (0, -1, float("nan"), True):
            with self.assertRaises(ValueError):
                KrakenPublicProvider(timeout_seconds=timeout)

    def test_origin_and_timeout_cannot_be_mutated_after_construction(self) -> None:
        provider = KrakenPublicProvider(timeout_seconds=7)

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

        self.assertEqual(provider.base_url, "https://api.kraken.com")
        self.assertEqual(provider.timeout_seconds, 7.0)

    def test_cross_origin_redirect_is_rejected_before_payload_read(self) -> None:
        with patch(
            "crypto_agent.providers.kraken.urlopen",
            return_value=_RedirectedResponse(),
        ):
            with self.assertRaises(ProviderError) as context:
                KrakenPublicProvider().fetch_candles(
                    symbol="BTC/USD",
                    interval_minutes=1_440,
                    as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
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

    def test_native_weekly_candles_are_aligned_to_thursday_utc(self) -> None:
        thursday = datetime(2026, 7, 30, tzinfo=timezone.utc)
        rows = [
            [
                int((thursday + timedelta(days=7 * index)).timestamp()),
                "100",
                "120",
                "90",
                str(105 + index),
                "105",
                "10",
                1,
            ]
            for index in range(3)
        ]
        payload = {"error": [], "result": {"XXBTZUSD": rows, "last": rows[-1][0]}}
        candles = KrakenPublicProvider().parse_payload(
            payload,
            symbol="BTC/USD",
            interval_minutes=10080,
            source_interval_minutes=10080,
            as_of=datetime(2026, 8, 13, 12, tzinfo=timezone.utc),
            limit=2,
        )
        self.assertEqual(len(candles), 2)
        self.assertEqual(candles[0].open_time, thursday)
        self.assertEqual(candles[1].open_time, thursday + timedelta(days=7))
        self.assertTrue(all(item.interval_minutes == 10080 for item in candles))

    def test_native_weekly_series_preserves_a_missing_week(self) -> None:
        thursday = datetime(2026, 7, 16, tzinfo=timezone.utc)
        rows = [
            [
                int((thursday + timedelta(days=7 * index)).timestamp()),
                "100",
                "120",
                "90",
                "105",
                "105",
                "10",
                1,
            ]
            for index in (0, 2, 3)
        ]
        payload = {"error": [], "result": {"XXBTZUSD": rows, "last": rows[-1][0]}}
        candles = KrakenPublicProvider().parse_payload(
            payload,
            symbol="BTC/USD",
            interval_minutes=10080,
            source_interval_minutes=10080,
            as_of=datetime(2026, 8, 6, 12, tzinfo=timezone.utc),
            limit=3,
        )
        self.assertEqual(len(candles), 2)
        self.assertEqual(candles[1].open_time - candles[0].open_time, timedelta(days=14))

    def test_native_weekly_history_supplies_exact_120_aligned_windows(self) -> None:
        target_end = datetime(2026, 8, 13, tzinfo=timezone.utc)
        first = target_end - timedelta(days=7 * 719)
        rows = [
            [
                int((first + timedelta(days=7 * index)).timestamp()),
                "100",
                "120",
                "90",
                "105",
                "105",
                "10",
                1,
            ]
            for index in range(720)
        ]
        payload = {"error": [], "result": {"XXBTZUSD": rows, "last": rows[-1][0]}}
        candles = KrakenPublicProvider().parse_payload(
            payload,
            symbol="BTC/USD",
            interval_minutes=10080,
            source_interval_minutes=10080,
            as_of=target_end + timedelta(hours=12),
            limit=120,
        )
        self.assertEqual(len(candles), 120)
        self.assertTrue(all(item.open_time.weekday() == 3 for item in candles))


if __name__ == "__main__":
    unittest.main()

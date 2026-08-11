from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from crypto_agent.orchestrator import _source_attested
from crypto_agent.providers.base import ProviderError
from crypto_agent.providers.t4 import Plus500T4Provider


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = json.dumps(payload).encode("utf-8")

    def read(self, size: int) -> bytes:
        return self.payload[:size]


class _Opener:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def open(self, request, timeout):  # type: ignore[no-untyped-def]
        del request, timeout
        return _Response(self.payload)


def _payload() -> dict[str, object]:
    as_of = datetime(2026, 8, 11, tzinfo=timezone.utc)
    candles = []
    for index in range(120):
        close_time = as_of - timedelta(days=119 - index)
        candles.append(
            {
                "symbol": "BTC/USD",
                "interval_minutes": 1440,
                "open_time": (close_time - timedelta(days=1)).isoformat(),
                "close_time": close_time.isoformat(),
                "open": 100.0 + index,
                "high": 102.0 + index,
                "low": 99.0 + index,
                "close": 101.0 + index,
                "volume": 1000.0 + index,
                "source": "plus500_t4_futures_v1",
                "available_at": close_time.isoformat(),
                "ingested_at": as_of.isoformat(),
            }
        )
    return {
        "source_id": "plus500_t4_futures_v1",
        "venue_id": "plus500_t4",
        "read_only": True,
        "order_routes_exposed": False,
        "volume_zscore": 3.5,
        "candles": candles,
        "reference_price": {
            "source": "plus500_t4_futures_v1",
            "price": 220.0,
            "event_time": (as_of - timedelta(minutes=1)).isoformat(),
            "available_at": as_of.isoformat(),
            "ingested_at": as_of.isoformat(),
        },
    }


class Plus500T4ProviderTests(unittest.TestCase):
    def test_bridge_is_restricted_to_loopback(self) -> None:
        for url in (
            "https://example.com:8784",
            "http://example.com:8784",
            "http://user:pass@127.0.0.1:8784",
            "http://127.0.0.1",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                Plus500T4Provider(bridge_url=url)

    def test_valid_read_only_payload_is_parsed(self) -> None:
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(_payload()),
        ):
            batch = Plus500T4Provider().fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(len(batch.candles), 120)
        self.assertEqual(batch.sources[0]["id"], "plus500_t4_futures_v1")
        self.assertTrue(batch.metadata["t4_read_only_attested"])
        self.assertFalse(batch.metadata["t4_order_routes_exposed"])
        self.assertEqual(len(batch.reference_price.observations), 1)  # type: ignore[union-attr]

    def test_wrong_source_fails_closed(self) -> None:
        payload = _payload()
        payload["source_id"] = "forged"
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ):
            batch = Plus500T4Provider().fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(batch.metadata["t4_source_id"], "forged")
        self.assertFalse(
            _source_attested(
                provider=Plus500T4Provider(), batch=batch, required_sources=1
            )
        )

    def test_exact_read_only_t4_batch_is_attested(self) -> None:
        provider = Plus500T4Provider()
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(_payload()),
        ):
            batch = provider.fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertTrue(
            _source_attested(provider=provider, batch=batch, required_sources=1)
        )

    def test_incomplete_history_is_rejected(self) -> None:
        payload = _payload()
        payload["candles"] = payload["candles"][:119]  # type: ignore[index]
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider().fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_HISTORY_INCOMPLETE")


if __name__ == "__main__":
    unittest.main()

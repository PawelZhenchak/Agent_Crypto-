from __future__ import annotations

import hashlib
import json
import unittest
from datetime import datetime, timedelta, timezone
from email.message import Message
from unittest.mock import patch

from crypto_agent.orchestrator import _source_attested
from crypto_agent.providers.base import ProviderError
from crypto_agent.providers.t4 import Plus500T4Provider


_BRIDGE_TOKEN = "t" * 32


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = json.dumps(payload).encode("utf-8")
        self.status = 200
        self.headers = Message()
        self.headers["Content-Type"] = "application/json"

    def read(self, size: int) -> bytes:
        return self.payload[:size]


class _Opener:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def open(self, request, timeout):  # type: ignore[no-untyped-def]
        self.request = request
        del timeout
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
        "schema_version": 3,
        "source_id": "plus500_t4_futures_v1",
        "venue_id": "plus500_t4",
        "read_only": True,
        "order_routes_exposed": False,
        "logical_symbol": "BTC/USD",
        "interval_minutes": 1440,
        "contract_id": "CME:MBT:202609",
        "contract_expires_at": (as_of + timedelta(days=30)).isoformat(),
        "contract_roll_at": (as_of + timedelta(days=25)).isoformat(),
        "contract_selection": "front_month",
        "rolled_from_contract_id": None,
        "volume_zscore": 3.5,
        "candles": candles,
        "reference_price": {
            "symbol": "BTC/USD",
            "source": "plus500_t4_futures_v1",
            "price": 220.0,
            "event_time": (as_of - timedelta(minutes=1)).isoformat(),
            "available_at": as_of.isoformat(),
            "ingested_at": as_of.isoformat(),
        },
        "futures_evidence": {
            "contract_id": "CME:MBT:202609",
            "source_id": "plus500_t4_futures_v1",
            "session_status": "OPEN",
            "is_full_snapshot": True,
            "observed_at": (as_of - timedelta(seconds=2)).isoformat(),
            "available_at": (as_of - timedelta(seconds=1)).isoformat(),
            "ingested_at": as_of.isoformat(),
            "bids": [
                {"level": level, "price": 220.0 - level / 10, "quantity": 2.0 + level}
                for level in range(1, 6)
            ],
            "asks": [
                {"level": level, "price": 220.0 + level / 10, "quantity": 3.0 + level}
                for level in range(1, 6)
            ],
            "basis_reference": {
                "symbol": "BTC/USD",
                "reference_type": "index",
                "source": "plus500_t4_index_v1",
                "price": 219.5,
                "observed_at": (as_of - timedelta(seconds=2)).isoformat(),
                "available_at": (as_of - timedelta(seconds=1)).isoformat(),
                "ingested_at": as_of.isoformat(),
            },
            "contract_transition": None,
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
                Plus500T4Provider(bridge_url=url, bridge_token=_BRIDGE_TOKEN)

    def test_bridge_requires_a_strong_local_token(self) -> None:
        for token in ("", "short", "contains whitespace " + "x" * 32):
            with self.subTest(token=token), self.assertRaises(ValueError):
                Plus500T4Provider(bridge_token=token)

    def test_valid_read_only_payload_is_parsed(self) -> None:
        opener = _Opener(_payload())
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=opener,
        ):
            batch = Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(len(batch.candles), 120)
        self.assertEqual(batch.sources[0]["id"], "plus500_t4_futures_v1")
        self.assertTrue(batch.metadata["t4_read_only_attested"])
        self.assertFalse(batch.metadata["t4_order_routes_exposed"])
        self.assertIsInstance(batch.raw_payload, bytes)
        self.assertEqual(
            batch.raw_payload_sha256,
            hashlib.sha256(batch.raw_payload or b"").hexdigest(),
        )
        self.assertEqual(
            opener.request.get_header("X-crypto-agent-bridge-token"),
            _BRIDGE_TOKEN,
        )
        self.assertEqual(len(batch.reference_price.observations), 1)  # type: ignore[union-attr]

    def test_wrong_source_fails_closed(self) -> None:
        payload = _payload()
        payload["source_id"] = "forged"
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ):
            with self.assertRaises(ProviderError) as raised:
                Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                    symbol="BTC/USD",
                    interval_minutes=1440,
                    as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                    limit=120,
                )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_unapproved_basis_source_fails_closed(self) -> None:
        payload = _payload()
        evidence = payload["futures_evidence"]
        assert isinstance(evidence, dict)
        basis = evidence["basis_reference"]
        assert isinstance(basis, dict)
        basis["source"] = "totally_untrusted_fake_source"
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_old_bridge_schema_fails_closed(self) -> None:
        payload = _payload()
        payload["schema_version"] = 1
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_order_routes_attestation_fails_closed(self) -> None:
        payload = _payload()
        payload["order_routes_exposed"] = True
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_expired_contract_fails_closed(self) -> None:
        payload = _payload()
        payload["contract_expires_at"] = "2026-08-11T00:00:00+00:00"
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_contract_in_roll_window_fails_closed(self) -> None:
        payload = _payload()
        payload["contract_roll_at"] = "2026-08-11T00:00:00+00:00"
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_controlled_roll_provenance_is_retained(self) -> None:
        payload = _payload()
        payload["contract_id"] = "CME:MBT:202612"
        payload["contract_selection"] = "rolled"
        payload["rolled_from_contract_id"] = "CME:MBT:202609"
        evidence = payload["futures_evidence"]
        assert isinstance(evidence, dict)
        evidence["contract_id"] = "CME:MBT:202612"
        evidence["contract_transition"] = {
            "from_contract_id": "CME:MBT:202609",
            "to_contract_id": "CME:MBT:202612",
            "price_type": "mid",
            "from_price": 219.8,
            "to_price": 220.0,
            "source": "plus500_t4_futures_v1",
            "observed_at": "2026-08-10T23:59:58+00:00",
            "available_at": "2026-08-10T23:59:59+00:00",
            "ingested_at": "2026-08-11T00:00:00+00:00",
        }
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ):
            batch = Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(batch.metadata["t4_contract_selection"], "rolled")
        self.assertEqual(
            batch.metadata["t4_rolled_from_contract_id"], "CME:MBT:202609"
        )

    def test_forged_roll_provenance_fails_closed(self) -> None:
        payload = _payload()
        payload["contract_selection"] = "rolled"
        payload["rolled_from_contract_id"] = payload["contract_id"]
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_exact_read_only_t4_batch_is_attested(self) -> None:
        provider = Plus500T4Provider(bridge_token=_BRIDGE_TOKEN)
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
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_HISTORY_INCOMPLETE")


if __name__ == "__main__":
    unittest.main()

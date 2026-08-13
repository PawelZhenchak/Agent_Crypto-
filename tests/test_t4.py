from __future__ import annotations

import hashlib
import json
import unittest
from datetime import UTC, datetime, timedelta
from email.message import Message
from unittest.mock import patch
from urllib.error import HTTPError

from crypto_agent.orchestrator import _source_attested
from crypto_agent.providers.base import ProviderError
from crypto_agent.providers.t4 import Plus500T4Provider

_BRIDGE_TOKEN = "t" * 32


class _Response:
    def __init__(self, payload: object) -> None:
        self.payload = json.dumps(payload).encode("utf-8")
        self.status = 200
        self.closed = False
        self.headers = Message()
        self.headers["Content-Type"] = "application/json"

    def read(self, size: int) -> bytes:
        return self.payload[:size]

    def close(self) -> None:
        self.closed = True


class _Opener:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def open(self, request, timeout):  # type: ignore[no-untyped-def]
        self.request = request
        del timeout
        return _Response(self.payload)


class _ErrorOpener:
    def __init__(self, *, status: int, reason_code: str | None) -> None:
        self.status = status
        self.reason_code = reason_code

    def open(self, request, timeout):  # type: ignore[no-untyped-def]
        del request, timeout
        headers = Message()
        if self.reason_code is not None:
            headers["X-Crypto-Agent-T4-Reason-Code"] = self.reason_code
        raise HTTPError(
            "http://127.0.0.1:8784/v1/market-data",
            self.status,
            "bridge failure",
            headers,
            None,
        )


def _payload() -> dict[str, object]:
    as_of = datetime(2026, 8, 11, tzinfo=UTC)
    active_market_id = "MBT Sep26 (XCME)"
    previous_market_id = "MBT Jun26 (XCME)"
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
                "market_id": (
                    previous_market_id if index < 90 else active_market_id
                ),
                "available_at": close_time.isoformat(),
                "ingested_at": as_of.isoformat(),
            }
        )
    return {
        "schema_version": 5,
        "source_id": "plus500_t4_futures_v1",
        "venue_id": "plus500_t4",
        "read_only": True,
        "order_routes_exposed": False,
        "environment": "live_t4",
        "logical_symbol": "BTC/USD",
        "interval_minutes": 1440,
        "exchange_id": "CME",
        "contract_id": "MBT",
        "market_id": active_market_id,
        "contract_expires_at": (as_of + timedelta(days=30)).isoformat(),
        "contract_roll_at": (as_of + timedelta(days=25)).isoformat(),
        "contract_selection": "front_month",
        "rolled_from_market_id": None,
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
            "exchange_id": "CME",
            "contract_id": "MBT",
            "market_id": active_market_id,
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
                "exchange_id": "CME",
                "contract_id": "BTC-INDEX",
                "market_id": "BTC Index (CME)",
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

    def test_bridge_preserves_only_allowlisted_failure_reason(self) -> None:
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_ErrorOpener(
                status=503,
                reason_code="T4_STALE_DATA",
            ),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_STALE_DATA")

    def test_bridge_rejects_untrusted_failure_reason(self) -> None:
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_ErrorOpener(
                status=503,
                reason_code="DROP_DATABASE_NOW",
            ),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_BRIDGE_UNAVAILABLE")

    def test_valid_read_only_payload_is_parsed(self) -> None:
        opener = _Opener(_payload())
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=opener,
        ):
            batch = Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(len(batch.candles), 120)
        self.assertEqual(batch.sources[0]["id"], "plus500_t4_futures_v1")
        self.assertTrue(batch.metadata["t4_read_only_attested"])
        self.assertFalse(batch.metadata["t4_order_routes_exposed"])
        self.assertEqual(batch.metadata["t4_environment"], "live_t4")
        self.assertEqual(batch.metadata["t4_exchange_id"], "CME")
        self.assertEqual(batch.metadata["t4_contract_id"], "MBT")
        self.assertEqual(batch.metadata["t4_market_id"], "MBT Sep26 (XCME)")
        self.assertEqual(len(batch.metadata["t4_candle_market_ids"]), 120)
        self.assertTrue(batch.external_delivery_eligible)
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
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_unknown_environment_fails_closed(self) -> None:
        payload = _payload()
        payload["environment"] = "fixture"
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_simulator_is_accepted_but_never_delivery_eligible(self) -> None:
        payload = _payload()
        payload["environment"] = "t4_simulator"
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ):
            batch = Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(batch.metadata["t4_environment"], "t4_simulator")
        self.assertFalse(batch.external_delivery_eligible)

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
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_old_bridge_schema_fails_closed(self) -> None:
        for schema_version in (1, 2, 3, 4):
            with self.subTest(schema_version=schema_version):
                payload = _payload()
                payload["schema_version"] = schema_version
                with patch(
                    "crypto_agent.providers.t4.build_opener",
                    return_value=_Opener(payload),
                ), self.assertRaises(ProviderError) as raised:
                    Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                        symbol="BTC/USD",
                        interval_minutes=1440,
                        as_of=datetime(2026, 8, 11, tzinfo=UTC),
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
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
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
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
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
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_controlled_roll_provenance_is_retained(self) -> None:
        payload = _payload()
        payload["contract_selection"] = "rolled"
        payload["rolled_from_market_id"] = "MBT Jun26 (XCME)"
        evidence = payload["futures_evidence"]
        assert isinstance(evidence, dict)
        evidence["contract_transition"] = {
            "from_market_id": "MBT Jun26 (XCME)",
            "to_market_id": "MBT Sep26 (XCME)",
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
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(batch.metadata["t4_contract_selection"], "rolled")
        self.assertEqual(
            batch.metadata["t4_rolled_from_market_id"], "MBT Jun26 (XCME)"
        )

    def test_forged_roll_provenance_fails_closed(self) -> None:
        payload = _payload()
        payload["contract_selection"] = "rolled"
        payload["rolled_from_market_id"] = payload["market_id"]
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_blank_market_id_fails_closed(self) -> None:
        payload = _payload()
        payload["market_id"] = "  "
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_PAYLOAD_INVALID")

    def test_market_id_control_characters_fail_closed(self) -> None:
        payload = _payload()
        payload["market_id"] = "MBT\nSep26"
        with patch(
            "crypto_agent.providers.t4.build_opener",
            return_value=_Opener(payload),
        ), self.assertRaises(ProviderError) as raised:
            Plus500T4Provider(bridge_token=_BRIDGE_TOKEN).fetch_batch(
                symbol="BTC/USD",
                interval_minutes=1440,
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
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
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
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
                as_of=datetime(2026, 8, 11, tzinfo=UTC),
                limit=120,
            )
        self.assertEqual(raised.exception.code, "T4_HISTORY_INCOMPLETE")


if __name__ == "__main__":
    unittest.main()

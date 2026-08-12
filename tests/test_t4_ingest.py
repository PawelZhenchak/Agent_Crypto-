from __future__ import annotations

import hashlib
import json
import threading
import unittest
from datetime import datetime, timedelta, timezone

from crypto_agent.domain import Candle, ReferencePriceObservation, ReferencePriceSnapshot
from crypto_agent.providers.base import ProviderBatch, ProviderError
from crypto_agent.t4_ingest import (
    T4IngestionReceipt,
    T4IngestionScheduler,
    T4IngestRepository,
)

from tests.db_fakes import FakeConnection, SQLStep


AS_OF = datetime(2026, 8, 11, tzinfo=timezone.utc)


def _batch(*, tamper_normalized: bool = False) -> ProviderBatch:
    candles: list[Candle] = []
    raw_candles: list[dict[str, object]] = []
    for index in range(60):
        close_time = AS_OF - timedelta(days=59 - index)
        raw = {
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
            "ingested_at": AS_OF.isoformat(),
        }
        raw_candles.append(raw)
        candles.append(
            Candle(
                symbol="BTC/USD",
                interval_minutes=1440,
                open_time=close_time - timedelta(days=1),
                close_time=close_time,
                open=float(raw["open"]) + (1.0 if tamper_normalized and index == 0 else 0.0),
                high=float(raw["high"]),
                low=float(raw["low"]),
                close=float(raw["close"]),
                volume=float(raw["volume"]),
                source="plus500_t4_futures_v1",
                available_at=close_time,
                ingested_at=AS_OF,
            )
        )
    envelope = {
        "schema_version": 2,
        "source_id": "plus500_t4_futures_v1",
        "venue_id": "plus500_t4",
        "read_only": True,
        "order_routes_exposed": False,
        "logical_symbol": "BTC/USD",
        "interval_minutes": 1440,
        "contract_id": "CME:MBT:202609",
        "contract_expires_at": (AS_OF + timedelta(days=30)).isoformat(),
        "contract_roll_at": (AS_OF + timedelta(days=25)).isoformat(),
        "contract_selection": "front_month",
        "rolled_from_contract_id": None,
        "candles": raw_candles,
        "reference_price": {
            "symbol": "BTC/USD",
            "source": "plus500_t4_futures_v1",
            "price": 161.0,
            "event_time": (AS_OF - timedelta(minutes=1)).isoformat(),
            "available_at": AS_OF.isoformat(),
            "ingested_at": AS_OF.isoformat(),
        },
    }
    raw_payload = json.dumps(envelope, separators=(",", ":")).encode()
    reference = ReferencePriceSnapshot(
        symbol="BTC/USD",
        observations=(
            ReferencePriceObservation(
                symbol="BTC/USD",
                price=161.0,
                event_time=AS_OF - timedelta(minutes=1),
                available_at=AS_OF,
                ingested_at=AS_OF,
                source="plus500_t4_futures_v1",
            ),
        ),
    )
    return ProviderBatch(
        candles=tuple(candles),
        input_candles=tuple(candles),
        sources=(
            {
                "id": "plus500_t4_futures_v1",
                "kind": "futures_market_data",
                "trust": "authenticated_external_data",
            },
        ),
        metadata={
            "t4_read_only_attested": True,
            "t4_source_id": "plus500_t4_futures_v1",
            "t4_venue_id": "plus500_t4",
            "t4_order_routes_exposed": False,
            "t4_bridge_schema_version": 2,
            "t4_contract_id": "CME:MBT:202609",
            "t4_contract_expires_at": (AS_OF + timedelta(days=30)).isoformat(),
            "t4_contract_roll_at": (AS_OF + timedelta(days=25)).isoformat(),
            "t4_contract_selection": "front_month",
            "t4_rolled_from_contract_id": None,
        },
        reference_price=reference,
        raw_payload=raw_payload,
        raw_payload_sha256=hashlib.sha256(raw_payload).hexdigest(),
    )


def _replay_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for index in reversed(range(60)):
        close_time = AS_OF - timedelta(days=59 - index)
        rows.append(
            (
                close_time - timedelta(days=1),
                close_time,
                100 + index,
                102 + index,
                99 + index,
                101 + index,
                1000 + index,
                close_time,
                AS_OF,
                "a" * 64,
                "CME:MBT:202609",
                AS_OF + timedelta(days=30),
                AS_OF + timedelta(days=25),
                "front_month",
                None,
                161,
                AS_OF - timedelta(minutes=1),
                AS_OF,
                AS_OF,
                1,
            )
        )
    return rows


class T4OperationalIngestTests(unittest.TestCase):
    def test_ingest_is_atomic_and_persists_every_candle(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("FROM crypto_agent.data_sources", [(7,)]),
                SQLStep("FROM crypto_agent.market_data_source_bindings", [(11,)]),
                SQLStep("INSERT INTO crypto_agent.t4_ingestion_batches", [(42,)]),
                *[
                    SQLStep("INSERT INTO crypto_agent.t4_canonical_candles")
                    for _ in range(60)
                ],
            ]
        )
        receipt = T4IngestRepository(lambda: connection).ingest(
            _batch(), requested_as_of=AS_OF
        )
        self.assertEqual(receipt.batch_id, 42)
        self.assertEqual(receipt.candle_count, 60)
        self.assertTrue(receipt.inserted)
        self.assertTrue(connection.committed)
        self.assertFalse(connection.scripted_cursor.steps)

    def test_duplicate_payload_returns_existing_batch_without_duplicate_candles(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("FROM crypto_agent.data_sources", [(7,)]),
                SQLStep("FROM crypto_agent.market_data_source_bindings", [(11,)]),
                SQLStep("INSERT INTO crypto_agent.t4_ingestion_batches"),
                SQLStep("FROM crypto_agent.t4_ingestion_batches", [(42, 60)]),
            ]
        )
        receipt = T4IngestRepository(lambda: connection).ingest(
            _batch(), requested_as_of=AS_OF
        )
        self.assertFalse(receipt.inserted)
        self.assertEqual(receipt.batch_id, 42)
        self.assertFalse(
            any(
                "INSERT INTO crypto_agent.t4_canonical_candles" in query
                for query, _ in connection.scripted_cursor.executions
            )
        )

    def test_normalized_data_must_match_the_hashed_raw_payload(self) -> None:
        with self.assertRaisesRegex(ValueError, "differs from normalized candles"):
            T4IngestRepository(lambda: FakeConnection([])).ingest(
                _batch(tamper_normalized=True), requested_as_of=AS_OF
            )

    def test_replay_is_read_only_and_deterministic(self) -> None:
        first = FakeConnection(
            [SQLStep("SET TRANSACTION READ ONLY"), SQLStep("WITH ranked AS", _replay_rows())]
        )
        second = FakeConnection(
            [SQLStep("SET TRANSACTION READ ONLY"), SQLStep("WITH ranked AS", _replay_rows())]
        )
        connections = iter((first, second))
        repository = T4IngestRepository(lambda: next(connections))
        result_a = repository.replay(
            symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=60
        )
        result_b = repository.replay(
            symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=60
        )
        self.assertEqual(result_a.replay_fingerprint_sha256, result_b.replay_fingerprint_sha256)
        self.assertEqual(result_a.candles, result_b.candles)
        self.assertEqual(result_a.source_batch_hashes, ("a" * 64,))
        self.assertTrue(first.committed)

    def test_incomplete_replay_fails_closed(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("WITH ranked AS", _replay_rows()[:-1]),
            ]
        )
        with self.assertRaises(ProviderError) as raised:
            T4IngestRepository(lambda: connection).replay(
                symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=60
            )
        self.assertEqual(raised.exception.code, "T4_REPLAY_INCOMPLETE")

    def test_scheduler_has_stoppable_cycles(self) -> None:
        calls: list[tuple[str, int, datetime]] = []

        def persist(symbol: str, interval: int, cutoff: datetime) -> T4IngestionReceipt:
            calls.append((symbol, interval, cutoff))
            return T4IngestionReceipt(1, "b" * 64, 60, True)

        scheduler = T4IngestionScheduler(persist, poll_seconds=1)
        receipts = scheduler.run_once(
            symbols=("BTC/USD",), intervals=(1440,), as_of=AS_OF
        )
        self.assertEqual(len(receipts), 1)
        self.assertEqual(calls, [("BTC/USD", 1440, AS_OF)])
        stop = threading.Event()
        stop.set()
        scheduler.run_forever(stop)


if __name__ == "__main__":
    unittest.main()

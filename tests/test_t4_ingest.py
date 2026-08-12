from __future__ import annotations

import base64
import hashlib
import json
import threading
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from crypto_agent.domain import (
    BasisReference,
    Candle,
    FuturesEvidence,
    OrderBookLevel,
    ReferencePriceObservation,
    ReferencePriceSnapshot,
    SessionStatus,
)
from crypto_agent.futures_analytics import futures_evidence_to_dict
from crypto_agent.providers.base import ProviderBatch, ProviderError
from crypto_agent.t4_ingest import (
    T4IngestionReceipt,
    T4IngestionScheduler,
    T4IngestRepository,
    _candle_hash,
    _object_hash,
    _validated_envelope,
)
from tests.db_fakes import FakeConnection, SQLStep

AS_OF = datetime(2026, 8, 11, tzinfo=UTC)


def _batch(
    *, tamper_normalized: bool = False, schema_version: int = 2
) -> ProviderBatch:
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
    evidence = None
    if schema_version in {3, 5}:
        evidence = FuturesEvidence(
            contract_id=(
                "MBT Sep26 (XCME)" if schema_version == 5 else "CME:MBT:202609"
            ),
            source="plus500_t4_futures_v1",
            session_status=SessionStatus.OPEN,
            is_full_snapshot=True,
            observed_at=AS_OF - timedelta(seconds=2),
            available_at=AS_OF - timedelta(seconds=1),
            ingested_at=AS_OF,
            bids=tuple(
                OrderBookLevel(level, 161 - level / 10, float(level))
                for level in range(1, 6)
            ),
            asks=tuple(
                OrderBookLevel(level, 161 + level / 10, float(level + 1))
                for level in range(1, 6)
            ),
            basis_reference=BasisReference(
                symbol="BTC/USD",
                reference_type="index",
                source="plus500_t4_index_v1",
                price=160.5,
                observed_at=AS_OF - timedelta(seconds=2),
                available_at=AS_OF - timedelta(seconds=1),
                ingested_at=AS_OF,
            ),
        )
    envelope = {
        "schema_version": schema_version,
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
    if evidence is not None:
        raw_evidence = futures_evidence_to_dict(evidence)
        assert isinstance(raw_evidence, dict)
        if schema_version == 5:
            envelope.update(
                {
                    "environment": "live_t4",
                    "exchange_id": "CME",
                    "contract_id": "MBT",
                    "market_id": "MBT Sep26 (XCME)",
                    "rolled_from_market_id": None,
                }
            )
            envelope.pop("rolled_from_contract_id")
            for raw_candle in raw_candles:
                raw_candle["market_id"] = "MBT Sep26 (XCME)"
            raw_evidence.update(
                {
                    "exchange_id": "CME",
                    "contract_id": "MBT",
                    "market_id": "MBT Sep26 (XCME)",
                }
            )
            basis = raw_evidence["basis_reference"]
            assert isinstance(basis, dict)
            basis.update(
                {
                    "exchange_id": "CME",
                    "contract_id": "BTC-INDEX",
                    "market_id": "BTC Index (CME)",
                }
            )
        envelope["futures_evidence"] = raw_evidence
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
            "t4_bridge_schema_version": schema_version,
            "t4_contract_id": (
                "MBT" if schema_version == 5 else "CME:MBT:202609"
            ),
            "t4_contract_expires_at": (AS_OF + timedelta(days=30)).isoformat(),
            "t4_contract_roll_at": (AS_OF + timedelta(days=25)).isoformat(),
            "t4_contract_selection": "front_month",
            "t4_rolled_from_contract_id": None,
            "t4_futures_evidence_attested": evidence is not None,
            **(
                {
                    "t4_environment": "live_t4",
                    "t4_exchange_id": "CME",
                    "t4_market_id": "MBT Sep26 (XCME)",
                    "t4_basis_exchange_id": "CME",
                    "t4_basis_contract_id": "BTC-INDEX",
                    "t4_basis_market_id": "BTC Index (CME)",
                    "t4_candle_market_ids": ["MBT Sep26 (XCME)"] * 60,
                    "t4_rolled_from_market_id": None,
                }
                if schema_version == 5
                else {}
            ),
        },
        reference_price=reference,
        raw_payload=raw_payload,
        raw_payload_sha256=hashlib.sha256(raw_payload).hexdigest(),
        futures_evidence=evidence,
    )


def _replay_rows(*, schema_version: int = 2) -> list[tuple[object, ...]]:
    raw_payload = b'{"fixture":"replay"}'
    raw_payload_hash = hashlib.sha256(raw_payload).hexdigest()
    raw_payload_base64 = base64.b64encode(raw_payload).decode("ascii")
    rows: list[tuple[object, ...]] = []
    for index in reversed(range(60)):
        close_time = AS_OF - timedelta(days=59 - index)
        candle = Candle(
            symbol="BTC/USD",
            interval_minutes=1440,
            open_time=close_time - timedelta(days=1),
            close_time=close_time,
            open=float(100 + index),
            high=float(102 + index),
            low=float(99 + index),
            close=float(101 + index),
            volume=float(1000 + index),
            source="plus500_t4_futures_v1",
            available_at=close_time,
            ingested_at=AS_OF,
        )
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
                raw_payload_hash,
                "CME:MBT:202609",
                AS_OF + timedelta(days=30),
                AS_OF + timedelta(days=25),
                "front_month",
                None,
                161,
                AS_OF - timedelta(minutes=1),
                AS_OF,
                AS_OF,
                schema_version,
                1,
                raw_payload_base64,
                _candle_hash(candle),
            )
        )
    return rows


def _v3_snapshot_and_levels() -> tuple[tuple[object, ...], list[tuple[object, ...]]]:
    evidence = _batch(schema_version=3).futures_evidence
    assert isinstance(evidence, FuturesEvidence)
    snapshot = (
        77,
        "CME:MBT:202609",
        "OPEN",
        True,
        AS_OF - timedelta(seconds=2),
        AS_OF - timedelta(seconds=1),
        AS_OF,
        "BTC/USD",
        "index",
        "plus500_t4_index_v1",
        160.5,
        AS_OF - timedelta(seconds=2),
        AS_OF - timedelta(seconds=1),
        AS_OF,
        _object_hash(futures_evidence_to_dict(evidence)),
    )
    levels = [
        (
            side,
            level.level,
            level.price,
            level.quantity,
            _object_hash(
                {
                    "side": side,
                    "level": level.level,
                    "price": level.price,
                    "quantity": level.quantity,
                }
            ),
        )
        for side, side_levels in (("bid", evidence.bids), ("ask", evidence.asks))
        for level in side_levels
    ]
    return snapshot, levels


class T4OperationalIngestTests(unittest.TestCase):
    def test_schema_v5_envelope_preserves_official_opaque_market_ids(self) -> None:
        batch = _batch(schema_version=5)
        envelope = _validated_envelope(batch, requested_as_of=AS_OF)
        self.assertEqual(envelope["exchange_id"], "CME")
        self.assertEqual(envelope["contract_id"], "MBT")
        self.assertEqual(envelope["market_id"], "MBT Sep26 (XCME)")

    def test_schema_v5_simulator_envelope_is_ingestible_but_explicit(self) -> None:
        batch = _batch(schema_version=5)
        raw_envelope = json.loads(batch.raw_payload or b"")
        raw_envelope["environment"] = "t4_simulator"
        raw_payload = json.dumps(raw_envelope, separators=(",", ":")).encode()
        simulator = replace(
            batch,
            metadata={**batch.metadata, "t4_environment": "t4_simulator"},
            raw_payload=raw_payload,
            raw_payload_sha256=hashlib.sha256(raw_payload).hexdigest(),
        )
        envelope = _validated_envelope(simulator, requested_as_of=AS_OF)
        self.assertEqual(envelope["environment"], "t4_simulator")
        self.assertFalse(simulator.external_delivery_eligible)

    def test_schema_v5_environment_metadata_mismatch_fails_closed(self) -> None:
        batch = _batch(schema_version=5)
        mismatched = replace(
            batch,
            metadata={**batch.metadata, "t4_environment": "t4_simulator"},
        )
        with self.assertRaisesRegex(ValueError, "attestation mismatch"):
            _validated_envelope(mismatched, requested_as_of=AS_OF)

    def test_schema_v5_candle_market_id_mismatch_fails_closed(self) -> None:
        batch = _batch(schema_version=5)
        metadata = dict(batch.metadata)
        market_ids = list(metadata["t4_candle_market_ids"])
        market_ids[0] = "MBT Jun26 (XCME)"
        metadata["t4_candle_market_ids"] = market_ids
        with self.assertRaisesRegex(ValueError, "candle MarketID"):
            _validated_envelope(
                replace(batch, metadata=metadata), requested_as_of=AS_OF
            )

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

    def test_schema_v3_evidence_is_persisted_in_the_same_transaction(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("FROM crypto_agent.data_sources", [(7,)]),
                SQLStep("FROM crypto_agent.market_data_source_bindings", [(11,)]),
                SQLStep("INSERT INTO crypto_agent.t4_ingestion_batches", [(42,)]),
                *[
                    SQLStep("INSERT INTO crypto_agent.t4_canonical_candles")
                    for _ in range(60)
                ],
                SQLStep("INSERT INTO crypto_agent.t4_futures_snapshots", [(77,)]),
                *[
                    SQLStep("INSERT INTO crypto_agent.t4_orderbook_levels")
                    for _ in range(10)
                ],
            ]
        )
        receipt = T4IngestRepository(lambda: connection).ingest(
            _batch(schema_version=3), requested_as_of=AS_OF
        )
        self.assertTrue(receipt.inserted)
        self.assertTrue(connection.committed)
        self.assertFalse(connection.scripted_cursor.steps)

    def test_duplicate_payload_returns_existing_batch_without_duplicate_candles(self) -> None:
        batch = _batch()
        connection = FakeConnection(
            [
                SQLStep("FROM crypto_agent.data_sources", [(7,)]),
                SQLStep("FROM crypto_agent.market_data_source_bindings", [(11,)]),
                SQLStep("INSERT INTO crypto_agent.t4_ingestion_batches"),
                SQLStep(
                    "FROM crypto_agent.t4_ingestion_batches",
                    [(42, 60, 2, batch.raw_payload_sha256, 60, 0, 0, 0)],
                ),
            ]
        )
        receipt = T4IngestRepository(lambda: connection).ingest(
            batch, requested_as_of=AS_OF
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
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("WITH selected_batch AS", _replay_rows()),
            ]
        )
        second = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("WITH selected_batch AS", _replay_rows()),
            ]
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
        self.assertEqual(
            result_a.source_batch_hashes,
            (hashlib.sha256(b'{"fixture":"replay"}').hexdigest(),),
        )
        self.assertTrue(first.committed)

    def test_incomplete_replay_fails_closed(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("WITH selected_batch AS", _replay_rows()[:-1]),
            ]
        )
        with self.assertRaises(ProviderError) as raised:
            T4IngestRepository(lambda: connection).replay(
                symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=60
            )
        self.assertEqual(raised.exception.code, "T4_REPLAY_INCOMPLETE")

    def test_replay_rejects_corrupted_raw_or_candle_hashes(self) -> None:
        cases: list[tuple[str, int, object, bool]] = [
            (
                "raw_payload_base64",
                21,
                base64.b64encode(b'{"fixture":"corrupted"}').decode("ascii"),
                True,
            ),
            ("raw_payload_hash", 9, "0" * 64, True),
            ("candle_content_hash", 22, "0" * 64, False),
        ]
        for label, column, replacement, replace_all in cases:
            with self.subTest(label=label):
                rows = _replay_rows()
                targets = range(len(rows)) if replace_all else range(1)
                for row_index in targets:
                    mutable = list(rows[row_index])
                    mutable[column] = replacement
                    rows[row_index] = tuple(mutable)
                connection = FakeConnection(
                    [
                        SQLStep("SET TRANSACTION READ ONLY"),
                        SQLStep("WITH selected_batch AS", rows),
                    ]
                )
                with self.assertRaises(ProviderError) as raised:
                    T4IngestRepository(lambda connection=connection: connection).replay(
                        symbol="BTC/USD",
                        interval_minutes=1440,
                        as_of=AS_OF,
                        limit=60,
                    )
                self.assertEqual(
                    raised.exception.code,
                    "T4_REPLAY_INTEGRITY_FAILURE",
                )

    def test_replay_query_excludes_short_batches_before_selecting_one(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("WITH selected_batch AS", _replay_rows()),
            ]
        )
        T4IngestRepository(lambda: connection).replay(
            symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=60
        )
        query, params = next(
            (query, params)
            for query, params in connection.scripted_cursor.executions
            if "WITH selected_batch AS" in query
        )
        self.assertIn("candidate.record_count >= %s", query)
        self.assertIn(") >= %s ORDER BY candidate.available_at DESC", query)
        self.assertIsInstance(params, tuple)
        assert isinstance(params, tuple)
        self.assertEqual(params[5], 60)
        self.assertEqual(params[9], 60)
        self.assertEqual(params[13], 60)

    def test_schema_v3_replay_restores_exact_futures_evidence(self) -> None:
        snapshot, levels = _v3_snapshot_and_levels()
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep("WITH selected_batch AS", _replay_rows(schema_version=3)),
                SQLStep("FROM crypto_agent.t4_futures_snapshots", [snapshot]),
                SQLStep("FROM crypto_agent.t4_orderbook_levels", levels),
                SQLStep("FROM crypto_agent.t4_contract_transition_evidence"),
            ]
        )
        result = T4IngestRepository(lambda: connection).replay(
            symbol="BTC/USD", interval_minutes=1440, as_of=AS_OF, limit=60
        )
        self.assertEqual(result.bridge_schema_version, 3)
        self.assertEqual(result.futures_evidence, _batch(schema_version=3).futures_evidence)
        self.assertEqual(
            result.as_provider_batch().metadata["t4_futures_evidence_attested"],
            True,
        )
        self.assertFalse(connection.scripted_cursor.steps)

    def test_schema_v3_replay_rejects_corrupted_evidence_hashes(self) -> None:
        for label, target in (
            ("snapshot", "snapshot"),
            ("order_book_level", "level"),
        ):
            with self.subTest(label=label):
                snapshot, levels = _v3_snapshot_and_levels()
                if target == "snapshot":
                    mutable_snapshot = list(snapshot)
                    mutable_snapshot[14] = "0" * 64
                    snapshot = tuple(mutable_snapshot)
                else:
                    mutable_level = list(levels[0])
                    mutable_level[4] = "0" * 64
                    levels[0] = tuple(mutable_level)
                connection = FakeConnection(
                    [
                        SQLStep("SET TRANSACTION READ ONLY"),
                        SQLStep(
                            "WITH selected_batch AS",
                            _replay_rows(schema_version=3),
                        ),
                        SQLStep(
                            "FROM crypto_agent.t4_futures_snapshots",
                            [snapshot],
                        ),
                        SQLStep("FROM crypto_agent.t4_orderbook_levels", levels),
                        SQLStep("FROM crypto_agent.t4_contract_transition_evidence"),
                    ]
                )
                with self.assertRaises(ProviderError) as raised:
                    T4IngestRepository(lambda connection=connection: connection).replay(
                        symbol="BTC/USD",
                        interval_minutes=1440,
                        as_of=AS_OF,
                        limit=60,
                    )
                self.assertEqual(
                    raised.exception.code,
                    "T4_REPLAY_EVIDENCE_INVALID",
                )

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

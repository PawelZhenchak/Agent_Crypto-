from __future__ import annotations

import json
import unittest
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import crypto_agent.ingest as ingest_module
from crypto_agent.consensus_math import (
    CONSENSUS_WINDOW_SIZE,
    ConsensusPolicyParameters,
    build_cross_exchange_consensus,
)
from crypto_agent.domain import Candle
from crypto_agent.ingest import (
    NORMALIZED_VOLUME_UNIT,
    SUPPORTED_CANONICAL_ALGORITHM,
    CanonicalCandleDraft,
    IngestValidationError,
    PersistenceInvariantError,
    PointInTimeCandleRepository,
    source_candle_content_hash,
)
from crypto_agent.postgres import PostgresOperationError, PostgresUnavailableError
from crypto_agent.providers.consensus import CrossExchangeConsensusProvider
from crypto_agent.providers.coinbase import CoinbaseExchangePublicProvider
from crypto_agent.providers.kraken import KrakenPublicProvider

from tests.db_fakes import FakeConnection, SQLStep
from tests.helpers import policy


UTC = timezone.utc
OPEN_TIME = datetime(2026, 8, 9, tzinfo=UTC)
CLOSE_TIME = OPEN_TIME + timedelta(days=1)
AVAILABLE_AT = CLOSE_TIME + timedelta(minutes=1)
PROVIDER_INGESTED_AT = CLOSE_TIME + timedelta(minutes=2)
CAPTURED_AT = CLOSE_TIME + timedelta(minutes=4)
AS_OF = CLOSE_TIME + timedelta(minutes=3)
QUERY_AS_OF = CLOSE_TIME + timedelta(minutes=5)
POLICY_EFFECTIVE_FROM = OPEN_TIME - timedelta(days=365)
REGISTRY_AVAILABLE_AT = OPEN_TIME - timedelta(days=2)
REGISTRY_INGESTED_AT = OPEN_TIME - timedelta(days=1)
SOURCE_OPEN_TIME = OPEN_TIME + timedelta(hours=10)
SOURCE_CLOSE_TIME = SOURCE_OPEN_TIME + timedelta(hours=1)
SOURCE_AVAILABLE_AT = SOURCE_CLOSE_TIME + timedelta(minutes=1)
SOURCE_PROVIDER_INGESTED_AT = SOURCE_CLOSE_TIME + timedelta(minutes=2)
RISK_POLICY = policy()
POLICY_HASH = RISK_POLICY.fingerprint()
KRAKEN_SOURCE_IDS = tuple(range(1, CONSENSUS_WINDOW_SIZE + 1))
COINBASE_SOURCE_IDS = tuple(range(1_001, 1_001 + CONSENSUS_WINDOW_SIZE))
CANONICAL_CONTEXT_IDS = (*KRAKEN_SOURCE_IDS, *COINBASE_SOURCE_IDS)
CANONICAL_OBSERVATION_IDS = (
    KRAKEN_SOURCE_IDS[-1],
    COINBASE_SOURCE_IDS[-1],
)


def source_candle(
    *,
    source: str = "kraken",
    symbol: str = "BTC/USD",
    available_at: datetime = SOURCE_AVAILABLE_AT,
    ingested_at: datetime = SOURCE_PROVIDER_INGESTED_AT,
) -> Candle:
    return Candle(
        symbol=symbol,
        interval_minutes=60,
        open_time=SOURCE_OPEN_TIME,
        close_time=SOURCE_CLOSE_TIME,
        open=100.0,
        high=110.0,
        low=90.0,
        close=104.0,
        volume=10.0,
        source=source,
        available_at=available_at,
        ingested_at=ingested_at,
    )


def binding_row(source: str = "kraken") -> tuple[object, ...]:
    return (source, "BTC/USD", "XBTUSD", 1001, "kraken")


def series_row(
    *,
    selected_policy=RISK_POLICY,
    effective_from: datetime = POLICY_EFFECTIVE_FROM,
    effective_to: datetime | None = None,
    registry_available_at: datetime = REGISTRY_AVAILABLE_AT,
    registry_ingested_at: datetime = REGISTRY_INGESTED_AT,
) -> tuple[object, ...]:
    return (
        5,
        900,
        99,
        77,
        "BTC/USD",
        86_400,
        SUPPORTED_CANONICAL_ALGORITHM,
        selected_policy.fingerprint(),
        9000,
        1,
        2,
        "spot",
        asdict(selected_policy),
        effective_from,
        effective_to,
        registry_available_at,
        registry_ingested_at,
    )


def canonical_series_model(
    *,
    selected_policy=RISK_POLICY,
    effective_from: datetime = POLICY_EFFECTIVE_FROM,
    effective_to: datetime | None = None,
    registry_available_at: datetime = REGISTRY_AVAILABLE_AT,
    registry_ingested_at: datetime = REGISTRY_INGESTED_AT,
):
    return ingest_module._CanonicalSeries(
        canonical_series_id=5,
        canonical_market_id=900,
        canonical_source_id=99,
        risk_policy_id=77,
        canonical_symbol="BTC/USD",
        interval_seconds=86_400,
        algorithm_version=SUPPORTED_CANONICAL_ALGORITHM,
        policy_hash=selected_policy.fingerprint(),
        exchange_id=9000,
        base_asset_id=1,
        quote_asset_id=2,
        instrument_type="spot",
        policy_effective_from=effective_from,
        policy_effective_to=effective_to,
        registry_available_at=registry_available_at,
        registry_ingested_at=registry_ingested_at,
        risk_policy=selected_policy,
        policy_parameters=ConsensusPolicyParameters(
            min_overlap=CONSENSUS_WINDOW_SIZE,
            max_close_divergence_bps=(
                selected_policy.max_cross_source_divergence_bps
            ),
            max_ohlc_divergence_bps=(
                selected_policy.max_cross_source_ohlc_divergence_bps
            ),
            max_volume_zscore_delta=(
                selected_policy.max_cross_source_volume_zscore_delta
            ),
            max_divergent_fraction=(
                selected_policy.max_divergent_candle_fraction
            ),
            max_market_price=selected_policy.max_market_price,
            max_base_volume=selected_policy.max_base_volume,
        ),
    )


def canonical_draft(
    *,
    close_price: int | float | Decimal = Decimal("104.25"),
    policy_hash: str = POLICY_HASH,
    source_candle_ids: tuple[int, ...] = CANONICAL_OBSERVATION_IDS,
    context_candle_ids: tuple[int, ...] = CANONICAL_CONTEXT_IDS,
) -> CanonicalCandleDraft:
    return CanonicalCandleDraft.from_numbers(
        canonical_series_id=5,
        open_time=OPEN_TIME,
        close_time=CLOSE_TIME,
        open_price=Decimal("100.25"),
        high_price=Decimal("110.25"),
        low_price=Decimal("90.25"),
        close_price=close_price,
        base_volume=None,
        trade_count=None,
        algorithm_version=SUPPORTED_CANONICAL_ALGORITHM,
        policy_hash=policy_hash,
        source_candle_ids=source_candle_ids,
        context_candle_ids=context_candle_ids,
    )


def raw_row(
    candle_id: int,
    source_id: int,
    *,
    market_id: int,
    exchange_id: int,
    price_offset: int | float | Decimal,
    open_time: datetime = OPEN_TIME,
    available_at: datetime | None = None,
    ingested_at: datetime | None = None,
    canonical_symbol: str = "BTC/USD",
    source_key: str | None = None,
    exchange_key: str | None = None,
    revision_no: int = 1,
    source_record_key: str | None = None,
) -> dict[str, object]:
    close_time = open_time + timedelta(days=1)
    row_available_at = available_at or close_time + timedelta(minutes=1)
    row_ingested_at = ingested_at or close_time + timedelta(minutes=2)
    is_kraken = source_id == 1
    row = {
        "candle_id": candle_id,
        "market_id": market_id,
        "interval_seconds": 86_400,
        "open_time": open_time,
        "close_time": close_time,
        "open_price": Decimal(100 + price_offset),
        "high_price": Decimal(110 + price_offset),
        "low_price": Decimal(90 + price_offset),
        "close_price": Decimal(104 + price_offset),
        "base_volume": Decimal(10 + 2 * price_offset),
        "trade_count": None,
        "source_id": source_id,
        "source_record_key": source_record_key or f"ohlc:{source_id}:{open_time.isoformat()}",
        "source_version": "v1",
        "revision_no": revision_no,
        "content_hash": "0" * 64,
        "observed_at": close_time,
        "available_at": row_available_at,
        "ingested_at": row_ingested_at,
        "is_final": True,
        "exchange_id": exchange_id,
        "exchange_key": exchange_key or ("kraken" if is_kraken else "coinbase"),
        "base_asset_id": 1,
        "quote_asset_id": 2,
        "instrument_type": "spot",
        "source_key": source_key or (
            KrakenPublicProvider.source_id
            if is_kraken
            else CoinbaseExchangePublicProvider.source_id
        ),
        "canonical_symbol": canonical_symbol,
        "venue_symbol": "XBTUSD" if is_kraken else "BTC-USD",
    }
    row["content_hash"] = ingest_module._raw_source_candle_content_hash(
        ingest_module._raw_row(row)
    )
    return row


def independent_raw_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(CONSENSUS_WINDOW_SIZE):
        open_time = OPEN_TIME - timedelta(
            days=CONSENSUS_WINDOW_SIZE - 1 - index
        )
        rows.extend(
            (
                raw_row(
                    KRAKEN_SOURCE_IDS[index],
                    1,
                    market_id=101,
                    exchange_id=1001,
                    price_offset=0,
                    open_time=open_time,
                ),
                raw_row(
                    COINBASE_SOURCE_IDS[index],
                    2,
                    market_id=202,
                    exchange_id=2002,
                    price_offset=Decimal("0.5"),
                    open_time=open_time,
                ),
            )
        )
    return rows


def provenance_rows() -> list[tuple[int, str]]:
    observation_ids = set(CANONICAL_OBSERVATION_IDS)
    return [
        (
            candle_id,
            "observation" if candle_id in observation_ids else "context",
        )
        for candle_id in sorted(CANONICAL_CONTEXT_IDS)
    ]


def provenance_insert_steps(
    *, final_rows: list[tuple[int, str]] | None = None
) -> list[SQLStep]:
    return [
        SQLStep("FROM crypto_agent.canonical_candle_provenance", []),
        *(
            SQLStep("INSERT INTO crypto_agent.canonical_candle_provenance")
            for _ in CANONICAL_CONTEXT_IDS
        ),
        SQLStep(
            "FROM crypto_agent.canonical_candle_provenance",
            provenance_rows() if final_rows is None else final_rows,
        ),
    ]


def stored_canonical_row(
    *,
    normalized_volume_override: Decimal | None = None,
) -> tuple[object, ...]:
    series = canonical_series_model()
    draft = canonical_draft()
    raw_rows = tuple(ingest_module._raw_row(row) for row in independent_raw_rows())
    computed = ingest_module._recompute_canonical(draft, series, raw_rows)
    manifest_computed = (
        computed
        if normalized_volume_override is None
        else replace(computed, normalized_volume=normalized_volume_override)
    )
    inputs_hash = ingest_module._canonical_inputs_hash(
        raw_rows,
        series=series,
        observation_candle_ids=CANONICAL_OBSERVATION_IDS,
    )
    payload_hash = ingest_module._computed_payload_hash(
        draft,
        series,
        manifest_computed,
    )
    evidence_hash = ingest_module._canonical_evidence_hash(
        series=series,
        cutoff_as_of=AS_OF,
        inputs_hash=inputs_hash,
        computed_payload_hash=payload_hash,
        diagnostics_document=manifest_computed.diagnostics_document,
    )
    content_hash = ingest_module._canonical_content_hash(
        draft,
        series,
        inputs_hash=inputs_hash,
        computed_payload_hash=payload_hash,
        evidence_hash=evidence_hash,
    )
    return (
        90,
        900,
        86_400,
        OPEN_TIME,
        CLOSE_TIME,
        computed.open_price,
        computed.high_price,
        computed.low_price,
        computed.close_price,
        None,
        None,
        1,
        CLOSE_TIME,
        computed.available_at,
        CAPTURED_AT,
        content_hash,
        5,
        77,
        SUPPORTED_CANONICAL_ALGORITHM,
        POLICY_HASH,
        manifest_computed.normalized_volume,
        NORMALIZED_VOLUME_UNIT,
        evidence_hash,
        CONSENSUS_WINDOW_SIZE,
        AS_OF,
        inputs_hash,
        payload_hash,
        manifest_computed.diagnostics_document,
        ingest_module.canonical_candle_record_key(draft),
    )


def stored_provenance_evidence_rows() -> list[dict[str, object]]:
    roles = dict(provenance_rows())
    return [
        {
            **row,
            "canonical_candle_id": 90,
            "input_role": roles[int(row["candle_id"])],
        }
        for row in independent_raw_rows()
    ]


def canonical_persist_steps(
    raw_rows: list[dict[str, object]] | None = None,
    *,
    final_provenance: list[tuple[int, str]] | None = None,
) -> list[SQLStep]:
    return [
        SQLStep("canonical_candle_series", [series_row()]),
        SQLStep(
            "FROM crypto_agent.candles candle",
            independent_raw_rows() if raw_rows is None else raw_rows,
        ),
        SQLStep("pg_advisory_xact_lock"),
        SQLStep("FROM crypto_agent.candles", []),
        SQLStep("INSERT INTO crypto_agent.candles", [(90,)]),
        SQLStep("canonical_candle_manifests", []),
        SQLStep("INSERT INTO crypto_agent.canonical_candle_manifests"),
        *provenance_insert_steps(final_rows=final_provenance),
    ]


class IngestV11Tests(unittest.TestCase):
    def test_source_ingest_requires_explicit_market_binding(self) -> None:
        connection = FakeConnection([SQLStep("market_data_source_bindings", [])])
        repository = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        )
        with self.assertRaisesRegex(IngestValidationError, "not authorized"):
            repository.append_source_candles(
                market_id=101,
                source_id=1,
                source_key="kraken",
                source_version="v1",
                candles=[source_candle()],
                as_of=AS_OF,
            )
        self.assertTrue(connection.rolled_back)

    def test_source_ingest_rejects_symbol_not_bound_to_venue_market(self) -> None:
        connection = FakeConnection(
            [SQLStep("market_data_source_bindings", [binding_row()])]
        )
        repository = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        )
        with self.assertRaisesRegex(IngestValidationError, "symbol"):
            repository.append_source_candles(
                market_id=101,
                source_id=1,
                source_key="kraken",
                source_version="v1",
                candles=[source_candle(symbol="ETH/USD")],
                as_of=AS_OF,
            )

    def test_source_ingest_never_backdates_provider_ingested_at(self) -> None:
        future_provider_receipt = CAPTURED_AT + timedelta(seconds=1)
        connection = FakeConnection(
            [SQLStep("market_data_source_bindings", [binding_row()])]
        )
        repository = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        )
        with self.assertRaisesRegex(IngestValidationError, "lineage"):
            repository.append_source_candles(
                market_id=101,
                source_id=1,
                source_key="kraken",
                source_version="v1",
                candles=[source_candle(ingested_at=future_provider_receipt)],
                as_of=AS_OF,
            )
        self.assertTrue(connection.rolled_back)
        self.assertFalse(
            any(
                "INSERT INTO crypto_agent.candles" in query
                for query, _ in connection.scripted_cursor.executions
            )
        )

    def test_source_ingest_replay_is_idempotent_but_appends_receipt(self) -> None:
        candle = source_candle()
        content_hash = source_candle_content_hash(
            candle,
            market_id=101,
            source_version="v1",
        )
        connection = FakeConnection(
            [
                SQLStep("market_data_source_bindings", [binding_row()]),
                SQLStep("pg_advisory_xact_lock"),
                SQLStep("FROM crypto_agent.candles", [(42, 3, content_hash)]),
                SQLStep("INSERT INTO crypto_agent.source_candle_receipts"),
            ]
        )
        repository = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        )
        result = repository.append_source_candles(
            market_id=101,
            source_id=1,
            source_key="kraken",
            source_version="v1",
            candles=[candle],
            as_of=AS_OF,
        )
        self.assertEqual(result[0].candle_id, 42)
        self.assertEqual(result[0].revision_no, 3)
        self.assertFalse(result[0].inserted)
        self.assertTrue(connection.committed)
        self.assertTrue(
            any(
                "source_candle_receipts" in query
                for query, _ in connection.scripted_cursor.executions
            )
        )

    def test_source_hash_is_venue_scoped_and_ignores_retry_time(self) -> None:
        first = source_candle()
        second = source_candle(
            available_at=AVAILABLE_AT + timedelta(minutes=1),
            ingested_at=PROVIDER_INGESTED_AT + timedelta(minutes=1),
        )
        self.assertEqual(
            source_candle_content_hash(first, market_id=101, source_version="v1"),
            source_candle_content_hash(second, market_id=101, source_version="v1"),
        )
        self.assertNotEqual(
            source_candle_content_hash(first, market_id=101, source_version="v1"),
            source_candle_content_hash(first, market_id=202, source_version="v1"),
        )
        self.assertNotEqual(
            source_candle_content_hash(first, market_id=101, source_version="v1"),
            source_candle_content_hash(first, market_id=101, source_version="v2"),
        )

    def test_database_connection_error_is_sanitized(self) -> None:
        def unavailable() -> FakeConnection:
            raise RuntimeError("postgresql://root:do-not-log-this@host/db")

        repository = PointInTimeCandleRepository(unavailable, clock=lambda: CAPTURED_AT)
        with self.assertRaises(PostgresUnavailableError) as raised:
            repository.append_source_candles(
                market_id=101,
                source_id=1,
                source_key="kraken",
                source_version="v1",
                candles=[source_candle()],
                as_of=AS_OF,
            )
        self.assertEqual(str(raised.exception), "PostgreSQL is unavailable")
        self.assertNotIn("do-not-log-this", str(raised.exception))

    def test_database_operation_error_rolls_back_and_is_sanitized(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("market_data_source_bindings", [binding_row()]),
                SQLStep(
                    "pg_advisory_xact_lock",
                    error=RuntimeError("password=do-not-log-this"),
                ),
            ]
        )
        repository = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        )
        with self.assertRaises(PostgresOperationError) as raised:
            repository.append_source_candles(
                market_id=101,
                source_id=1,
                source_key="kraken",
                source_version="v1",
                candles=[source_candle()],
                as_of=AS_OF,
            )
        self.assertEqual(str(raised.exception), "PostgreSQL source candle ingest failed")
        self.assertNotIn("do-not-log-this", str(raised.exception))
        self.assertTrue(connection.rolled_back)
        self.assertTrue(connection.closed)

    def test_canonical_accepts_equivalent_markets_from_different_venues(self) -> None:
        connection = FakeConnection(canonical_persist_steps())
        repository = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        )
        persisted = repository.append_canonical_candle(
            canonical_draft(),
            as_of=AS_OF,
        )
        self.assertEqual(persisted.candle_id, 90)
        self.assertTrue(persisted.inserted)
        insert_query, insert_params = next(
            (query, params)
            for query, params in connection.scripted_cursor.executions
            if "INSERT INTO crypto_agent.candles" in query
        )
        self.assertIn("INSERT INTO crypto_agent.candles", insert_query)
        self.assertEqual(insert_params[0], 900)
        self.assertNotIn(insert_params[0], (101, 202))
        self.assertIn("NULL, NULL, %s, true", insert_query)
        self.assertIsNone(insert_params[8])
        raw_query, raw_params = next(
            (query, params)
            for query, params in connection.scripted_cursor.executions
            if "FROM crypto_agent.candles candle" in query
        )
        self.assertIn(
            "DISTINCT ON (candle.source_id, candle.source_record_key)",
            raw_query,
        )
        self.assertIn("candle.revision_no DESC", raw_query)
        self.assertNotIn("ANY(%s::bigint[])", raw_query)
        self.assertEqual(raw_params[0], 86_400)
        self.assertIn(KrakenPublicProvider.source_id, raw_params)
        self.assertIn(CoinbaseExchangePublicProvider.source_id, raw_params)

        manifest_params = next(
            params
            for query, params in connection.scripted_cursor.executions
            if "INSERT INTO crypto_agent.canonical_candle_manifests" in query
        )
        self.assertEqual(manifest_params[5], CONSENSUS_WINDOW_SIZE)
        self.assertEqual(manifest_params[6], AS_OF)
        self.assertRegex(manifest_params[7], r"^[0-9a-f]{64}$")
        self.assertRegex(manifest_params[8], r"^[0-9a-f]{64}$")
        self.assertEqual(manifest_params[9], Decimal("1"))
        self.assertEqual(manifest_params[10], NORMALIZED_VOLUME_UNIT)
        diagnostics = json.loads(manifest_params[11])
        self.assertEqual(
            diagnostics["consensus_overlap_count"],
            CONSENSUS_WINDOW_SIZE,
        )
        self.assertEqual(
            set(diagnostics["consensus_source_ids"]),
            {
                KrakenPublicProvider.source_id,
                CoinbaseExchangePublicProvider.source_id,
            },
        )
        self.assertRegex(manifest_params[12], r"^[0-9a-f]{64}$")
        replay_connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", independent_raw_rows()),
                SQLStep("pg_advisory_xact_lock"),
                SQLStep(
                    "FROM crypto_agent.candles",
                    [(90, 1, persisted.content_hash)],
                ),
                SQLStep("canonical_candle_manifests", [tuple(manifest_params[1:])]),
                SQLStep(
                    "FROM crypto_agent.canonical_candle_provenance",
                    provenance_rows(),
                ),
            ]
        )
        replayed = PointInTimeCandleRepository(
            lambda: replay_connection,
            clock=lambda: CAPTURED_AT,
        ).append_canonical_candle(canonical_draft(), as_of=AS_OF)
        self.assertFalse(replayed.inserted)
        self.assertFalse(
            any(
                "INSERT INTO crypto_agent.candles" in query
                for query, _ in replay_connection.scripted_cursor.executions
            )
        )

    def test_canonical_context_claim_must_equal_db_derived_universe(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", independent_raw_rows()),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "DB-derived raw universe"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(
                canonical_draft(context_candle_ids=CANONICAL_CONTEXT_IDS[1:]),
                as_of=AS_OF,
            )

    def test_canonical_missing_db_window_is_fail_closed(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep(
                    "FROM crypto_agent.candles candle",
                    independent_raw_rows()[:-1],
                ),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "DB-derived raw universe"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(canonical_draft(), as_of=AS_OF)

    def test_canonical_rejects_unapproved_source_identity(self) -> None:
        spoofed = [
            {**row, "source_key": "synthetic_fixture_v1"}
            if row["source_id"] == 2
            else row
            for row in independent_raw_rows()
        ]
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", spoofed),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "approved source"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(canonical_draft(), as_of=AS_OF)

    def test_durable_receipt_may_follow_market_cutoff_without_backdating(self) -> None:
        received_after_cutoff = [
            {**row, "ingested_at": CAPTURED_AT}
            for row in independent_raw_rows()
        ]
        connection = FakeConnection(
            canonical_persist_steps(received_after_cutoff)
        )
        result = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        ).append_canonical_candle(canonical_draft(), as_of=AS_OF)
        self.assertTrue(result.inserted)
        raw_params = next(
            params
            for query, params in connection.scripted_cursor.executions
            if "FROM crypto_agent.candles candle" in query
        )
        self.assertEqual(raw_params[-3:], (AS_OF, AS_OF, CAPTURED_AT))

    def test_canonical_recomputes_and_rejects_manipulated_price(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", independent_raw_rows()),
            ]
        )
        repository = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        )
        with self.assertRaisesRegex(IngestValidationError, "CANONICAL_VALUE_MISMATCH"):
            repository.append_canonical_candle(
                canonical_draft(close_price=106),
                as_of=AS_OF,
            )
        self.assertTrue(connection.rolled_back)
        self.assertFalse(
            any(
                "INSERT INTO crypto_agent.candles" in query
                for query, _ in connection.scripted_cursor.executions
            )
        )

    def test_canonical_rejects_extreme_cross_venue_divergence(self) -> None:
        divergent = [
            row if row["source_id"] == 1 else raw_row(
                int(row["candle_id"]),
                2,
                market_id=202,
                exchange_id=2002,
                price_offset=Decimal("100"),
                open_time=row["open_time"],
            )
            for row in independent_raw_rows()
        ]
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", divergent),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "CROSS_SOURCE_DIVERGENCE"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(canonical_draft(), as_of=AS_OF)
        self.assertTrue(connection.rolled_back)

    def test_canonical_consensus_enforces_close_divergence_policy(self) -> None:
        strict_policy = replace(RISK_POLICY, max_cross_source_divergence_bps=10.0)
        connection = FakeConnection(
            [
                SQLStep(
                    "canonical_candle_series",
                    [series_row(selected_policy=strict_policy)],
                ),
                SQLStep("FROM crypto_agent.candles candle", independent_raw_rows()),
            ]
        )
        with self.assertRaisesRegex(
            IngestValidationError, "CROSS_SOURCE_DIVERGENCE"
        ):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(
                canonical_draft(policy_hash=strict_policy.fingerprint()),
                as_of=AS_OF,
            )

    def test_canonical_consensus_enforces_ohlc_divergence_policy(self) -> None:
        strict_policy = replace(
            RISK_POLICY,
            max_cross_source_ohlc_divergence_bps=10.0,
        )
        connection = FakeConnection(
            [
                SQLStep(
                    "canonical_candle_series",
                    [
                        series_row(selected_policy=strict_policy)
                    ],
                ),
                SQLStep("FROM crypto_agent.candles candle", independent_raw_rows()),
            ]
        )
        with self.assertRaisesRegex(
            IngestValidationError, "CROSS_SOURCE_DIVERGENCE"
        ):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(
                canonical_draft(policy_hash=strict_policy.fingerprint()),
                as_of=AS_OF,
            )

    def test_canonical_persistence_uses_live_consensus_algorithm(self) -> None:
        self.assertEqual(
            SUPPORTED_CANONICAL_ALGORITHM,
            "cross_exchange_spot_consensus_v1",
        )
        migration = (
            Path(__file__).resolve().parents[1]
            / "db/migrations/0011_canonical_candle_provenance.sql"
        ).read_text(encoding="utf-8")
        self.assertIn(
            f"algorithm_version = '{SUPPORTED_CANONICAL_ALGORITHM}'",
            migration,
        )

    def test_canonical_requires_distinct_source_venues(self) -> None:
        same_venue = [
            {**row, "exchange_id": 1001, "exchange_key": "kraken"}
            if row["source_id"] == 2
            else row
            for row in independent_raw_rows()
        ]
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", same_venue),
            ]
        )
        repository = PointInTimeCandleRepository(
            lambda: connection,
            clock=lambda: CAPTURED_AT,
        )
        with self.assertRaisesRegex(
            IngestValidationError, "exact distinct venues|source-to-venue"
        ):
            repository.append_canonical_candle(canonical_draft(), as_of=AS_OF)

    def test_canonical_rejects_wrong_policy_hash(self) -> None:
        wrong_policy_draft = CanonicalCandleDraft.from_numbers(
            canonical_series_id=5,
            open_time=OPEN_TIME,
            close_time=CLOSE_TIME,
            open_price=101,
            high_price=111,
            low_price=91,
            close_price=105,
            base_volume=None,
            trade_count=None,
            algorithm_version=SUPPORTED_CANONICAL_ALGORITHM,
            policy_hash="e" * 64,
            source_candle_ids=CANONICAL_OBSERVATION_IDS,
            context_candle_ids=CANONICAL_CONTEXT_IDS,
        )
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", independent_raw_rows()),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "policy hash"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(wrong_policy_draft, as_of=AS_OF)

    def test_canonical_rejects_policy_weaker_than_live_v11_envelope(self) -> None:
        weak_policy = replace(RISK_POLICY, min_consensus_overlap=60)
        connection = FakeConnection(
            [
                SQLStep(
                    "canonical_candle_series",
                    [series_row(selected_policy=weak_policy)],
                ),
                SQLStep("FROM crypto_agent.candles candle", independent_raw_rows()),
            ]
        )
        with self.assertRaisesRegex(
            IngestValidationError,
            "fails V1 safety validation",
        ):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(
                canonical_draft(policy_hash=weak_policy.fingerprint()),
                as_of=AS_OF,
            )

    def test_caller_cannot_cherry_pick_the_derived_context(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", independent_raw_rows()),
            ]
        )
        cherry_picked_ids = tuple(
            candle_id
            for candle_id in CANONICAL_CONTEXT_IDS
            if candle_id != KRAKEN_SOURCE_IDS[0]
        )
        with self.assertRaisesRegex(IngestValidationError, "DB-derived raw universe"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(
                canonical_draft(context_candle_ids=cherry_picked_ids),
                as_of=AS_OF,
            )
        self.assertTrue(connection.rolled_back)

    def test_caller_cannot_pin_a_superseded_raw_revision(self) -> None:
        derived_rows = independent_raw_rows()
        superseded = derived_rows[0]
        latest = {
            **superseded,
            "candle_id": 9_999,
            "revision_no": 2,
            "content_hash": f"{9_999:064x}",
        }
        derived_rows[0] = latest
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", derived_rows),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "DB-derived raw universe"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(canonical_draft(), as_of=AS_OF)
        selection_query = connection.scripted_cursor.executions[1][0]
        self.assertIn(
            "DISTINCT ON (candle.source_id, candle.source_record_key)",
            selection_query,
        )
        self.assertIn("candle.revision_no DESC", selection_query)

    def test_gapped_or_misaligned_120_window_is_rejected(self) -> None:
        gapped_rows = independent_raw_rows()
        target = gapped_rows[20]
        one_hour = timedelta(hours=1)
        gapped_rows[20] = {
            **target,
            "open_time": target["open_time"] + one_hour,
            "close_time": target["close_time"] + one_hour,
            "observed_at": target["observed_at"] + one_hour,
            "available_at": target["available_at"] + one_hour,
            "ingested_at": target["ingested_at"] + one_hour,
        }
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", gapped_rows),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "aligned, and contiguous"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(canonical_draft(), as_of=AS_OF)

    def test_unapproved_source_identities_are_rejected(self) -> None:
        unapproved = [
            {
                **row,
                "source_key": (
                    "unapproved_alpha"
                    if row["source_id"] == 1
                    else "unapproved_beta"
                ),
            }
            for row in independent_raw_rows()
        ]
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", unapproved),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "approved source"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(canonical_draft(), as_of=AS_OF)

    def test_persistence_requires_the_full_shared_120_window(self) -> None:
        removed_ids = {KRAKEN_SOURCE_IDS[0], COINBASE_SOURCE_IDS[0]}
        short_rows = [
            row
            for row in independent_raw_rows()
            if row["candle_id"] not in removed_ids
        ]
        short_ids = tuple(
            candle_id
            for candle_id in CANONICAL_CONTEXT_IDS
            if candle_id not in removed_ids
        )
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("FROM crypto_agent.candles candle", short_rows),
            ]
        )
        with self.assertRaisesRegex(IngestValidationError, "exactly 240 raw candles"):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(
                canonical_draft(context_candle_ids=short_ids),
                as_of=AS_OF,
            )

    def test_provenance_conflict_after_on_conflict_is_detected(self) -> None:
        missing_final_link = provenance_rows()[:-1]
        connection = FakeConnection(
            canonical_persist_steps(final_provenance=missing_final_link)
        )
        with self.assertRaisesRegex(
            PersistenceInvariantError,
            "did not persist the exact mapping",
        ):
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(canonical_draft(), as_of=AS_OF)
        self.assertTrue(connection.rolled_back)

    def test_runtime_and_durable_replay_have_golden_numeric_parity(self) -> None:
        rows = independent_raw_rows()

        def provider_candles(source_id: int, source_key: str) -> list[Candle]:
            return [
                Candle(
                    symbol="BTC/USD",
                    interval_minutes=1_440,
                    open_time=row["open_time"],
                    close_time=row["close_time"],
                    open=float(row["open_price"]),
                    high=float(row["high_price"]),
                    low=float(row["low_price"]),
                    close=float(row["close_price"]),
                    volume=float(row["base_volume"]),
                    source=source_key,
                    available_at=row["available_at"],
                    ingested_at=row["ingested_at"],
                )
                for row in rows
                if row["source_id"] == source_id
            ]

        kraken = KrakenPublicProvider()
        coinbase = CoinbaseExchangePublicProvider()
        kraken_rows = provider_candles(1, kraken.source_id)
        coinbase_rows = provider_candles(2, coinbase.source_id)
        kraken.fetch_candles = lambda **_: list(kraken_rows)  # type: ignore[method-assign]
        coinbase.fetch_candles = lambda **_: list(coinbase_rows)  # type: ignore[method-assign]
        runtime = CrossExchangeConsensusProvider(
            (kraken, coinbase),
            RISK_POLICY,
        ).fetch_batch(
            symbol="BTC/USD",
            interval_minutes=1_440,
            as_of=AS_OF,
            limit=CONSENSUS_WINDOW_SIZE,
        )
        runtime_latest = runtime.candles[-1]

        connection = FakeConnection(canonical_persist_steps(rows))
        with patch(
            "crypto_agent.ingest.build_cross_exchange_consensus",
            wraps=build_cross_exchange_consensus,
        ) as durable_math:
            PointInTimeCandleRepository(
                lambda: connection,
                clock=lambda: CAPTURED_AT,
            ).append_canonical_candle(
                CanonicalCandleDraft.from_numbers(
                    canonical_series_id=5,
                    open_time=runtime_latest.open_time,
                    close_time=runtime_latest.close_time,
                    open_price=runtime_latest.open,
                    high_price=runtime_latest.high,
                    low_price=runtime_latest.low,
                    close_price=runtime_latest.close,
                    base_volume=None,
                    trade_count=None,
                    algorithm_version=SUPPORTED_CANONICAL_ALGORITHM,
                    policy_hash=POLICY_HASH,
                    source_candle_ids=CANONICAL_OBSERVATION_IDS,
                    context_candle_ids=CANONICAL_CONTEXT_IDS,
                ),
                as_of=AS_OF,
            )

        self.assertEqual(durable_math.call_args.kwargs["limit"], CONSENSUS_WINDOW_SIZE)
        insert_params = next(
            params
            for query, params in connection.scripted_cursor.executions
            if "INSERT INTO crypto_agent.candles" in query
        )
        self.assertEqual(
            insert_params[4:8],
            tuple(
                Decimal(str(value))
                for value in (
                    runtime_latest.open,
                    runtime_latest.high,
                    runtime_latest.low,
                    runtime_latest.close,
                )
            ),
        )
        manifest_params = next(
            params
            for query, params in connection.scripted_cursor.executions
            if "INSERT INTO crypto_agent.canonical_candle_manifests" in query
        )
        self.assertEqual(
            manifest_params[9],
            Decimal(str(runtime_latest.volume)),
        )
        durable_diagnostics = json.loads(manifest_params[11])
        self.assertEqual(
            durable_diagnostics["consensus_overlap_count"],
            runtime.metadata["consensus_overlap_count"],
        )
        self.assertEqual(
            durable_diagnostics["consensus_source_counts"],
            runtime.metadata["consensus_source_counts"],
        )

    def test_canonical_query_applies_available_and_ingested_cutoff(self) -> None:
        canonical_row = stored_canonical_row()
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("WITH eligible AS", [canonical_row]),
                SQLStep(
                    "WHERE provenance.canonical_candle_id = ANY",
                    stored_provenance_evidence_rows(),
                ),
            ]
        )
        records = PointInTimeCandleRepository(
            lambda: connection
        ).load_canonical_candles(
            canonical_series_id=5,
            as_of=QUERY_AS_OF,
            limit=100,
        )
        query, params = connection.scripted_cursor.executions[1]
        self.assertIn("c.available_at <= %s", query)
        self.assertIn("c.ingested_at <= %s", query)
        self.assertEqual(
            params,
            (5, QUERY_AS_OF, QUERY_AS_OF, QUERY_AS_OF, QUERY_AS_OF, 100),
        )
        provenance_query, provenance_params = connection.scripted_cursor.executions[2]
        self.assertIn("ANY(%s::bigint[])", provenance_query)
        self.assertIn("linked_at <= %s", provenance_query)
        self.assertEqual(provenance_params, ([90], QUERY_AS_OF))
        self.assertEqual(records[0].source_candle_ids, CANONICAL_OBSERVATION_IDS)
        self.assertEqual(records[0].context_candle_ids, CANONICAL_CONTEXT_IDS)
        self.assertEqual(records[0].canonical_series_id, 5)
        self.assertEqual(records[0].risk_policy_id, 77)
        self.assertIsNone(records[0].base_volume)
        self.assertEqual(records[0].normalized_volume, Decimal("1"))
        self.assertEqual(records[0].normalized_volume_unit, NORMALIZED_VOLUME_UNIT)
        self.assertEqual(records[0].consensus_window_size, CONSENSUS_WINDOW_SIZE)

    def test_canonical_query_rejects_incomplete_persisted_provenance(self) -> None:
        canonical_row = stored_canonical_row()
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("WITH eligible AS", [canonical_row]),
                SQLStep(
                    "WHERE provenance.canonical_candle_id = ANY",
                    [stored_provenance_evidence_rows()[0]],
                ),
            ]
        )
        with self.assertRaisesRegex(
            PersistenceInvariantError, "deterministic replay"
        ):
            PointInTimeCandleRepository(lambda: connection).load_canonical_candles(
                canonical_series_id=5,
                as_of=QUERY_AS_OF,
                limit=100,
            )

    def test_canonical_query_rejects_forged_but_hash_consistent_volume(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep(
                    "WITH eligible AS",
                    [
                        stored_canonical_row(
                            normalized_volume_override=Decimal("999999999")
                        )
                    ],
                ),
                SQLStep(
                    "WHERE provenance.canonical_candle_id = ANY",
                    stored_provenance_evidence_rows(),
                ),
            ]
        )
        with self.assertRaisesRegex(
            PersistenceInvariantError,
            "differ from deterministic replay",
        ):
            PointInTimeCandleRepository(lambda: connection).load_canonical_candles(
                canonical_series_id=5,
                as_of=QUERY_AS_OF,
                limit=100,
            )

    def test_canonical_query_rejects_raw_ohlcv_changed_without_source_hash(self) -> None:
        tampered_rows = stored_provenance_evidence_rows()
        earliest_open = min(row["open_time"] for row in tampered_rows)
        for row in tampered_rows:
            if row["open_time"] == earliest_open:
                for field in ("open_price", "high_price", "low_price", "close_price"):
                    row[field] = Decimal(row[field]) * 2

        connection = FakeConnection(
            [
                SQLStep("canonical_candle_series", [series_row()]),
                SQLStep("WITH eligible AS", [stored_canonical_row()]),
                SQLStep(
                    "WHERE provenance.canonical_candle_id = ANY",
                    tampered_rows,
                ),
            ]
        )
        with self.assertRaisesRegex(
            PersistenceInvariantError,
            "source candle content hash",
        ):
            PointInTimeCandleRepository(lambda: connection).load_canonical_candles(
                canonical_series_id=5,
                as_of=QUERY_AS_OF,
                limit=100,
            )

    def test_historical_canonical_remains_readable_after_policy_expiry(self) -> None:
        policy_expiry = QUERY_AS_OF - timedelta(minutes=1)
        connection = FakeConnection(
            [
                SQLStep(
                    "canonical_candle_series",
                    [series_row(effective_to=policy_expiry)],
                ),
                SQLStep("WITH eligible AS", [stored_canonical_row()]),
                SQLStep(
                    "WHERE provenance.canonical_candle_id = ANY",
                    stored_provenance_evidence_rows(),
                ),
            ]
        )
        records = PointInTimeCandleRepository(
            lambda: connection
        ).load_canonical_candles(
            canonical_series_id=5,
            as_of=QUERY_AS_OF,
            limit=100,
        )
        self.assertEqual(len(records), 1)

    def test_policy_expired_before_manifest_cutoff_is_rejected(self) -> None:
        connection = FakeConnection(
            [
                SQLStep(
                    "canonical_candle_series",
                    [series_row(effective_to=AS_OF - timedelta(seconds=1))],
                ),
                SQLStep("WITH eligible AS", [stored_canonical_row()]),
                SQLStep(
                    "WHERE provenance.canonical_candle_id = ANY",
                    stored_provenance_evidence_rows(),
                ),
            ]
        )
        with self.assertRaisesRegex(
            PersistenceInvariantError,
            "outside its policy validity interval",
        ):
            PointInTimeCandleRepository(lambda: connection).load_canonical_candles(
                canonical_series_id=5,
                as_of=QUERY_AS_OF,
                limit=100,
            )

    def test_manifest_cutoff_cannot_precede_registry_metadata(self) -> None:
        late_registry_time = AS_OF + timedelta(minutes=1)
        connection = FakeConnection(
            [
                SQLStep(
                    "canonical_candle_series",
                    [
                        series_row(
                            registry_available_at=late_registry_time,
                            registry_ingested_at=late_registry_time,
                        )
                    ],
                ),
                SQLStep("WITH eligible AS", [stored_canonical_row()]),
                SQLStep(
                    "WHERE provenance.canonical_candle_id = ANY",
                    stored_provenance_evidence_rows(),
                ),
            ]
        )
        with self.assertRaisesRegex(
            PersistenceInvariantError,
            "predates required registry metadata",
        ):
            PointInTimeCandleRepository(lambda: connection).load_canonical_candles(
                canonical_series_id=5,
                as_of=QUERY_AS_OF,
                limit=100,
            )

    def test_canonical_query_revalidates_policy_fingerprint(self) -> None:
        mismatched = list(series_row())
        mismatched[7] = "e" * 64
        connection = FakeConnection(
            [SQLStep("canonical_candle_series", [tuple(mismatched)])]
        )
        with self.assertRaisesRegex(IngestValidationError, "policy hash"):
            PointInTimeCandleRepository(lambda: connection).load_canonical_candles(
                canonical_series_id=5,
                as_of=QUERY_AS_OF,
                limit=100,
            )

    def test_nan_is_rejected_before_storage(self) -> None:
        with self.assertRaisesRegex(IngestValidationError, "finite"):
            CanonicalCandleDraft.from_numbers(
                canonical_series_id=5,
                open_time=OPEN_TIME,
                close_time=CLOSE_TIME,
                open_price=float("nan"),
                high_price=111,
                low_price=91,
                close_price=105,
                base_volume=None,
                trade_count=None,
                algorithm_version=SUPPORTED_CANONICAL_ALGORITHM,
                policy_hash=POLICY_HASH,
                source_candle_ids=CANONICAL_OBSERVATION_IDS,
                context_candle_ids=CANONICAL_CONTEXT_IDS,
            )


if __name__ == "__main__":
    unittest.main()

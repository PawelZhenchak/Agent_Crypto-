from __future__ import annotations

import base64
import binascii
import hashlib
import json
import math
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from .domain import (
    BasisReference,
    Candle,
    ContractTransitionEvidence,
    FuturesEvidence,
    OrderBookLevel,
    ReferencePriceObservation,
    ReferencePriceSnapshot,
    SessionStatus,
)
from .futures_analytics import (
    futures_evidence_to_dict,
    validate_futures_evidence_structure,
)
from .postgres import (
    ConnectionFactory,
    DBCursor,
    PostgresError,
    PostgresOperationError,
    cursor,
    transaction,
)
from .providers.base import ProviderBatch, ProviderError

T4_SOURCE_ID = "plus500_t4_futures_v1"
T4_VENUE_ID = "plus500_t4"
T4_SCHEMA_VERSION = 5
T4_SUPPORTED_SCHEMA_VERSIONS = frozenset({2, 3, 4, 5})
T4_MAX_RAW_PAYLOAD_BYTES = 4_000_000


@dataclass(frozen=True, slots=True)
class T4IngestionReceipt:
    batch_id: int
    payload_sha256: str
    candle_count: int
    inserted: bool


@dataclass(frozen=True, slots=True)
class T4ReplayResult:
    symbol: str
    interval_minutes: int
    as_of: datetime
    candles: tuple[Candle, ...]
    reference_price: ReferencePriceSnapshot
    contract_id: str
    contract_expires_at: datetime
    contract_roll_at: datetime
    contract_selection: str
    rolled_from_contract_id: str | None
    bridge_schema_version: int
    futures_evidence: FuturesEvidence | None
    source_batch_hashes: tuple[str, ...]
    replay_fingerprint_sha256: str
    source_batch_ids: tuple[int, ...] = ()
    environment: str | None = None
    exchange_id: str | None = None
    market_id: str | None = None
    basis_exchange_id: str | None = None
    basis_contract_id: str | None = None
    basis_market_id: str | None = None
    candle_market_ids: tuple[str, ...] = ()
    rolled_from_market_id: str | None = None

    def as_provider_batch(self) -> ProviderBatch:
        return ProviderBatch(
            candles=self.candles,
            input_candles=self.candles,
            sources=(
                {
                    "id": T4_SOURCE_ID,
                    "kind": "futures_market_data_replay",
                    "trust": "authenticated_postgres_replay",
                },
            ),
            metadata={
                "t4_read_only_attested": True,
                "t4_source_id": T4_SOURCE_ID,
                "t4_venue_id": T4_VENUE_ID,
                "t4_order_routes_exposed": False,
                "t4_bridge_schema_version": self.bridge_schema_version,
                "t4_contract_id": self.contract_id,
                "t4_contract_expires_at": self.contract_expires_at.isoformat(),
                "t4_contract_roll_at": self.contract_roll_at.isoformat(),
                "t4_contract_selection": self.contract_selection,
                "t4_rolled_from_contract_id": self.rolled_from_contract_id,
                **(
                    {
                        "t4_environment": self.environment,
                        "t4_exchange_id": self.exchange_id,
                        "t4_market_id": self.market_id,
                        "t4_basis_exchange_id": self.basis_exchange_id,
                        "t4_basis_contract_id": self.basis_contract_id,
                        "t4_basis_market_id": self.basis_market_id,
                        "t4_candle_market_ids": list(self.candle_market_ids),
                        "t4_rolled_from_market_id": self.rolled_from_market_id,
                    }
                    if self.bridge_schema_version == 5
                    else {}
                ),
                "t4_replay": True,
                "t4_replay_as_of": self.as_of.isoformat(),
                "t4_replay_fingerprint_sha256": self.replay_fingerprint_sha256,
                "t4_replay_source_batch_hashes": list(self.source_batch_hashes),
                "t4_replay_source_batch_ids": list(self.source_batch_ids),
                "t4_replay_provenance_sha256": _replay_provenance_hash(
                    self.source_batch_ids,
                    self.source_batch_hashes,
                ),
                "t4_futures_evidence_attested": bool(
                    self.bridge_schema_version in {3, 4, 5}
                    and self.futures_evidence is not None
                ),
            },
            reference_price=self.reference_price,
            futures_evidence=self.futures_evidence,
        )


class T4IngestRepository:
    """Atomic, idempotent storage and point-in-time replay for T4 batches."""

    def __init__(self, connection_factory: ConnectionFactory) -> None:
        self.connection_factory = connection_factory

    def ingest(
        self,
        batch: ProviderBatch,
        *,
        requested_as_of: datetime,
    ) -> T4IngestionReceipt:
        envelope = _validated_envelope(batch, requested_as_of=requested_as_of)
        payload_hash = _required_payload_hash(batch)
        candles = tuple(batch.candles)
        reference = _single_reference(batch)
        metadata = batch.metadata
        schema_version = int(envelope["schema_version"])
        candle_market_ids = (
            tuple(cast(list[str], metadata["t4_candle_market_ids"]))
            if schema_version == 5
            else (None,) * len(candles)
        )
        observed_at = max(item.close_time for item in candles)
        available_at = max(
            [*(item.available_at for item in candles), reference.available_at]
        )

        try:
            with transaction(self.connection_factory) as connection, cursor(
                connection
            ) as db_cursor:
                db_cursor.execute(
                    """
                    SELECT source_id
                    FROM crypto_agent.data_sources
                    WHERE source_key = %s
                    FOR SHARE
                    """,
                    (T4_SOURCE_ID,),
                )
                source_row = db_cursor.fetchone()
                if source_row is None:
                    raise PostgresOperationError("T4 source registry is missing")
                source_id = int(_row_value(source_row, "source_id", 0))

                db_cursor.execute(
                    """
                    SELECT binding.market_id
                    FROM crypto_agent.market_data_source_bindings AS binding
                    JOIN crypto_agent.markets AS market
                      ON market.market_id = binding.market_id
                    JOIN crypto_agent.exchanges AS exchange
                      ON exchange.exchange_id = market.exchange_id
                    WHERE binding.source_id = %s
                      AND binding.canonical_symbol = %s
                      AND exchange.exchange_key = %s
                    """,
                    (source_id, envelope["logical_symbol"], T4_VENUE_ID),
                )
                market_row = db_cursor.fetchone()
                if market_row is None:
                    raise PostgresOperationError("T4 market binding is missing")
                market_id = int(_row_value(market_row, "market_id", 0))

                db_cursor.execute(
                    """
                    INSERT INTO crypto_agent.t4_ingestion_batches (
                        source_id, market_id, logical_symbol, contract_id,
                        contract_expires_at, contract_roll_at, contract_selection,
                        rolled_from_contract_id, interval_seconds, requested_as_of,
                        bridge_schema_version, reference_price, reference_event_time,
                        reference_available_at, reference_ingested_at,
                        raw_payload_base64, raw_payload_hash, record_count, status,
                        observed_at, available_at, environment, exchange_id,
                        active_market_id, rolled_from_market_id,
                        basis_exchange_id, basis_contract_id, basis_market_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, 'completed', %s, %s
                        , %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (raw_payload_hash) DO NOTHING
                    RETURNING t4_batch_id
                    """,
                    (
                        source_id,
                        market_id,
                        envelope["logical_symbol"],
                        metadata["t4_contract_id"],
                        _utc_metadata(metadata, "t4_contract_expires_at"),
                        _utc_metadata(metadata, "t4_contract_roll_at"),
                        metadata["t4_contract_selection"],
                        (
                            None
                            if schema_version == 5
                            else metadata["t4_rolled_from_contract_id"]
                        ),
                        candles[0].interval_minutes * 60,
                        requested_as_of,
                        schema_version,
                        reference.price,
                        reference.event_time,
                        reference.available_at,
                        reference.ingested_at,
                        base64.b64encode(batch.raw_payload or b"").decode("ascii"),
                        payload_hash,
                        len(candles),
                        observed_at,
                        available_at,
                        metadata.get("t4_environment") if schema_version == 5 else None,
                        metadata.get("t4_exchange_id") if schema_version == 5 else None,
                        metadata.get("t4_market_id") if schema_version == 5 else None,
                        (
                            metadata.get("t4_rolled_from_market_id")
                            if schema_version == 5
                            else None
                        ),
                        (
                            metadata.get("t4_basis_exchange_id")
                            if schema_version == 5
                            else None
                        ),
                        (
                            metadata.get("t4_basis_contract_id")
                            if schema_version == 5
                            else None
                        ),
                        (
                            metadata.get("t4_basis_market_id")
                            if schema_version == 5
                            else None
                        ),
                    ),
                )
                inserted_row = db_cursor.fetchone()
                inserted = inserted_row is not None
                if inserted:
                    batch_id = int(_row_value(inserted_row, "t4_batch_id", 0))
                    for candle, t4_market_id in zip(
                        candles, candle_market_ids, strict=True
                    ):
                        db_cursor.execute(
                            """
                            INSERT INTO crypto_agent.t4_canonical_candles (
                                t4_batch_id, source_id, registry_market_id,
                                logical_symbol, contract_id, market_id,
                                interval_seconds, open_time, close_time,
                                open_price, high_price, low_price, close_price,
                                base_volume, available_at, provider_ingested_at,
                                content_hash
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            """,
                            (
                                batch_id,
                                source_id,
                                market_id,
                                candle.symbol,
                                metadata["t4_contract_id"],
                                t4_market_id,
                                candle.interval_minutes * 60,
                                candle.open_time,
                                candle.close_time,
                                candle.open,
                                candle.high,
                                candle.low,
                                candle.close,
                                candle.volume,
                                candle.available_at,
                                candle.ingested_at,
                                _candle_hash(candle, market_id=t4_market_id),
                            ),
                        )
                    if schema_version in {3, 4, 5}:
                        if batch.futures_evidence is None:
                            raise ValueError("T4 schema v3 futures evidence is missing")
                        _persist_futures_evidence(
                            db_cursor,
                            batch_id=batch_id,
                            source_id=source_id,
                            market_id=market_id,
                            logical_symbol=str(envelope["logical_symbol"]),
                            evidence=batch.futures_evidence,
                            schema_version=schema_version,
                            exchange_id=(
                                str(metadata["t4_exchange_id"])
                                if schema_version == 5
                                else None
                            ),
                            product_contract_id=(
                                str(metadata["t4_contract_id"])
                                if schema_version == 5
                                else None
                            ),
                            active_market_id=(
                                str(metadata["t4_market_id"])
                                if schema_version == 5
                                else None
                            ),
                            basis_exchange_id=(
                                str(metadata["t4_basis_exchange_id"])
                                if schema_version == 5
                                else None
                            ),
                            basis_contract_id=(
                                str(metadata["t4_basis_contract_id"])
                                if schema_version == 5
                                else None
                            ),
                            basis_market_id=(
                                str(metadata["t4_basis_market_id"])
                                if schema_version == 5
                                else None
                            ),
                        )
                else:
                    db_cursor.execute(
                        """
                        SELECT batch.t4_batch_id, batch.record_count,
                               batch.bridge_schema_version, batch.raw_payload_hash,
                               (SELECT count(*)
                                FROM crypto_agent.t4_canonical_candles AS candle
                                WHERE candle.t4_batch_id = batch.t4_batch_id)
                                   AS candle_count,
                               (SELECT count(*)
                                FROM crypto_agent.t4_futures_snapshots AS snapshot
                                WHERE snapshot.t4_batch_id = batch.t4_batch_id)
                                   AS snapshot_count,
                               (SELECT count(*)
                                FROM crypto_agent.t4_orderbook_levels AS level
                                JOIN crypto_agent.t4_futures_snapshots AS snapshot
                                  ON snapshot.t4_snapshot_id = level.t4_snapshot_id
                                WHERE snapshot.t4_batch_id = batch.t4_batch_id)
                                   AS level_count,
                               (SELECT count(*)
                                FROM crypto_agent.t4_contract_transition_evidence AS transition
                                WHERE transition.t4_batch_id = batch.t4_batch_id)
                                   AS transition_count
                        FROM crypto_agent.t4_ingestion_batches AS batch
                        WHERE batch.raw_payload_hash = %s
                        """,
                        (payload_hash,),
                    )
                    existing = db_cursor.fetchone()
                    expected_snapshot_count = 1 if schema_version in {3, 4, 5} else 0
                    expected_level_count = (
                        len(batch.futures_evidence.bids)
                        + len(batch.futures_evidence.asks)
                        if schema_version in {3, 4, 5}
                        and batch.futures_evidence is not None
                        else 0
                    )
                    expected_transition_count = int(
                        schema_version in {3, 4, 5}
                        and batch.futures_evidence is not None
                        and batch.futures_evidence.contract_transition is not None
                    )
                    if existing is None or not bool(
                        int(_row_value(existing, "record_count", 1)) == len(candles)
                        and int(_row_value(existing, "bridge_schema_version", 2))
                        == schema_version
                        and str(_row_value(existing, "raw_payload_hash", 3))
                        == payload_hash
                        and int(_row_value(existing, "candle_count", 4)) == len(candles)
                        and int(_row_value(existing, "snapshot_count", 5))
                        == expected_snapshot_count
                        and int(_row_value(existing, "level_count", 6))
                        == expected_level_count
                        and int(_row_value(existing, "transition_count", 7))
                        == expected_transition_count
                    ):
                        raise PostgresOperationError("T4 ingest idempotency check failed")
                    batch_id = int(_row_value(existing, "t4_batch_id", 0))
        except PostgresError:
            raise
        except Exception:
            raise PostgresOperationError("T4 operational ingest failed") from None

        return T4IngestionReceipt(
            batch_id=batch_id,
            payload_sha256=payload_hash,
            candle_count=len(candles),
            inserted=inserted,
        )

    def replay(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> T4ReplayResult:
        _validate_replay_request(symbol, interval_minutes, as_of, limit)
        snapshot_row: object | None = None
        level_rows: tuple[object, ...] = ()
        transition_row: object | None = None
        try:
            with transaction(self.connection_factory) as connection, cursor(
                connection
            ) as db_cursor:
                db_cursor.execute("SET TRANSACTION READ ONLY")
                db_cursor.execute(
                    """
                    WITH selected_batch AS (
                        SELECT candidate.*
                        FROM crypto_agent.t4_ingestion_batches AS candidate
                        WHERE candidate.logical_symbol = %s
                          AND candidate.interval_seconds = %s
                          AND candidate.requested_as_of <= %s
                          AND candidate.available_at <= %s
                          AND candidate.ingested_at <= %s
                          AND candidate.record_count >= %s
                          AND candidate.status = 'completed'
                          AND (
                              SELECT count(*)
                              FROM crypto_agent.t4_canonical_candles AS eligible
                              WHERE eligible.t4_batch_id = candidate.t4_batch_id
                                AND eligible.close_time <= %s
                                AND eligible.available_at <= %s
                                AND eligible.provider_ingested_at <= %s
                          ) >= %s
                        ORDER BY candidate.available_at DESC,
                                 candidate.ingested_at DESC,
                                 candidate.t4_batch_id DESC
                        LIMIT 1
                    ), ranked AS (
                        SELECT candle.open_time, candle.close_time,
                               candle.open_price, candle.high_price,
                               candle.low_price, candle.close_price,
                               candle.base_volume, candle.available_at,
                               candle.provider_ingested_at, batch.raw_payload_hash,
                               batch.contract_id, batch.contract_expires_at,
                               batch.contract_roll_at, batch.contract_selection,
                               batch.rolled_from_contract_id,
                               batch.reference_price, batch.reference_event_time,
                               batch.reference_available_at,
                               batch.reference_ingested_at,
                               batch.bridge_schema_version,
                               batch.t4_batch_id,
                               batch.raw_payload_base64,
                               candle.content_hash,
                               batch.environment,
                               batch.exchange_id,
                               batch.active_market_id,
                               batch.rolled_from_market_id,
                               batch.basis_exchange_id,
                               batch.basis_contract_id,
                               batch.basis_market_id,
                               candle.market_id AS candle_market_id,
                               row_number() OVER (
                                   PARTITION BY candle.open_time
                                   ORDER BY candle.available_at DESC,
                                            candle.t4_candle_id DESC
                               ) AS replay_rank
                        FROM crypto_agent.t4_canonical_candles AS candle
                        JOIN selected_batch AS batch
                          ON batch.t4_batch_id = candle.t4_batch_id
                        WHERE candle.close_time <= %s
                          AND candle.available_at <= %s
                          AND candle.provider_ingested_at <= %s
                    )
                    SELECT open_time, close_time, open_price, high_price, low_price,
                           close_price, base_volume, available_at,
                           provider_ingested_at, raw_payload_hash, contract_id,
                           contract_expires_at, contract_roll_at, contract_selection,
                           rolled_from_contract_id, reference_price,
                           reference_event_time, reference_available_at,
                           reference_ingested_at, bridge_schema_version, t4_batch_id
                           , raw_payload_base64, content_hash, environment,
                           exchange_id, active_market_id, rolled_from_market_id,
                           basis_exchange_id, basis_contract_id, basis_market_id,
                           candle_market_id
                    FROM ranked
                    WHERE replay_rank = 1
                    ORDER BY open_time DESC
                    LIMIT %s
                    """,
                    (
                        symbol,
                        interval_minutes * 60,
                        as_of,
                        as_of,
                        as_of,
                        limit,
                        as_of,
                        as_of,
                        as_of,
                        limit,
                        as_of,
                        as_of,
                        as_of,
                        limit,
                    ),
                )
                rows = tuple(db_cursor.fetchall())
                if len(rows) == limit:
                    newest_in_transaction = rows[0]
                    schema_version_in_transaction = int(
                        _row_value(
                            newest_in_transaction,
                            "bridge_schema_version",
                            19,
                        )
                    )
                    if schema_version_in_transaction in {3, 4, 5}:
                        batch_id = int(
                            _row_value(newest_in_transaction, "t4_batch_id", 20)
                        )
                        db_cursor.execute(
                            """
                            SELECT t4_snapshot_id, contract_id, session_status,
                                   is_full_snapshot, observed_at, available_at,
                                   provider_ingested_at, basis_reference_symbol,
                                   basis_reference_type, basis_reference_source,
                                   basis_reference_price, basis_observed_at,
                                   basis_available_at, basis_ingested_at,
                                   content_hash, exchange_id, product_contract_id,
                                   active_market_id, basis_exchange_id,
                                   basis_product_contract_id, basis_market_id
                            FROM crypto_agent.t4_futures_snapshots
                            WHERE t4_batch_id = %s
                              AND available_at <= %s
                              AND provider_ingested_at <= %s
                            """,
                            (batch_id, as_of, as_of),
                        )
                        snapshot_row = db_cursor.fetchone()
                        if snapshot_row is not None:
                            snapshot_id = int(
                                _row_value(snapshot_row, "t4_snapshot_id", 0)
                            )
                            db_cursor.execute(
                                """
                                SELECT side, level_no, price, quantity, content_hash
                                FROM crypto_agent.t4_orderbook_levels
                                WHERE t4_snapshot_id = %s
                                ORDER BY side, level_no
                                """,
                                (snapshot_id,),
                            )
                            level_rows = tuple(db_cursor.fetchall())
                        db_cursor.execute(
                            """
                            SELECT from_contract_id, to_contract_id, price_type,
                                   from_price, to_price, evidence_source,
                                   observed_at, available_at, provider_ingested_at,
                                   content_hash, exchange_id, product_contract_id,
                                   from_market_id, to_market_id
                            FROM crypto_agent.t4_contract_transition_evidence
                            WHERE t4_batch_id = %s
                              AND available_at <= %s
                              AND provider_ingested_at <= %s
                            """,
                            (batch_id, as_of, as_of),
                        )
                        transition_row = db_cursor.fetchone()
        except PostgresError:
            raise
        except Exception:
            raise PostgresOperationError("T4 replay failed") from None

        if len(rows) != limit:
            raise ProviderError(
                "T4 replay history is incomplete", code="T4_REPLAY_INCOMPLETE"
            )
        ordered = tuple(reversed(rows))
        newest = ordered[-1]
        contract_id = str(_row_value(newest, "contract_id", 10))
        bridge_schema_version = int(
            _row_value(newest, "bridge_schema_version", 19)
        )
        if bridge_schema_version not in T4_SUPPORTED_SCHEMA_VERSIONS:
            raise ProviderError(
                "T4 replay schema is unsupported",
                code="T4_REPLAY_SCHEMA_UNSUPPORTED",
            )
        candles = tuple(
            Candle(
                symbol=symbol,
                interval_minutes=interval_minutes,
                open_time=_as_utc(_row_value(row, "open_time", 0)),
                close_time=_as_utc(_row_value(row, "close_time", 1)),
                open=float(_row_value(row, "open_price", 2)),
                high=float(_row_value(row, "high_price", 3)),
                low=float(_row_value(row, "low_price", 4)),
                close=float(_row_value(row, "close_price", 5)),
                volume=float(_row_value(row, "base_volume", 6)),
                source=T4_SOURCE_ID,
                available_at=_as_utc(_row_value(row, "available_at", 7)),
                ingested_at=_as_utc(_row_value(row, "provider_ingested_at", 8)),
            )
            for row in ordered
        )
        _verify_replay_batch_integrity(ordered, candles)
        source_batches = tuple(
            sorted(
                {
                    (
                        int(_row_value(row, "t4_batch_id", 20)),
                        str(_row_value(row, "raw_payload_hash", 9)),
                    )
                    for row in ordered
                }
            )
        )
        source_batch_ids = tuple(item[0] for item in source_batches)
        source_hashes = tuple(item[1] for item in source_batches)
        reference = ReferencePriceSnapshot(
            symbol=symbol,
            observations=(
                ReferencePriceObservation(
                    symbol=symbol,
                    price=float(_row_value(newest, "reference_price", 15)),
                    event_time=_as_utc(_row_value(newest, "reference_event_time", 16)),
                    available_at=_as_utc(
                        _row_value(newest, "reference_available_at", 17)
                    ),
                    ingested_at=_as_utc(
                        _row_value(newest, "reference_ingested_at", 18)
                    ),
                    source=T4_SOURCE_ID,
                ),
            ),
        )
        contract_expires_at = _as_utc(
            _row_value(newest, "contract_expires_at", 11)
        )
        contract_roll_at = _as_utc(_row_value(newest, "contract_roll_at", 12))
        contract_selection = str(_row_value(newest, "contract_selection", 13))
        rolled_from_contract_id = _optional_text(
            _row_value(newest, "rolled_from_contract_id", 14)
        )
        environment = (
            _optional_text(_row_value(newest, "environment", 23))
            if bridge_schema_version == 5
            else None
        )
        exchange_id = (
            _optional_text(_row_value(newest, "exchange_id", 24))
            if bridge_schema_version == 5
            else None
        )
        active_market_id = (
            _optional_text(_row_value(newest, "active_market_id", 25))
            if bridge_schema_version == 5
            else None
        )
        rolled_from_market_id = (
            _optional_text(_row_value(newest, "rolled_from_market_id", 26))
            if bridge_schema_version == 5
            else None
        )
        basis_exchange_id = (
            _optional_text(_row_value(newest, "basis_exchange_id", 27))
            if bridge_schema_version == 5
            else None
        )
        basis_contract_id = (
            _optional_text(_row_value(newest, "basis_contract_id", 28))
            if bridge_schema_version == 5
            else None
        )
        basis_market_id = (
            _optional_text(_row_value(newest, "basis_market_id", 29))
            if bridge_schema_version == 5
            else None
        )
        candle_market_ids = (
            tuple(
                str(_row_value(row, "candle_market_id", 30))
                for row in ordered
            )
            if bridge_schema_version == 5
            else ()
        )
        if bridge_schema_version == 5 and (
            environment not in {"t4_simulator", "live_t4"}
            or exchange_id is None
            or active_market_id is None
            or basis_exchange_id is None
            or basis_contract_id is None
            or basis_market_id is None
            or any(value == "None" for value in candle_market_ids)
            or candle_market_ids[-1] != active_market_id
        ):
            raise ProviderError(
                "T4 replay schema-v5 identity is invalid",
                code="T4_REPLAY_EVIDENCE_INVALID",
            )
        futures_evidence = None
        if bridge_schema_version in {3, 4, 5}:
            futures_evidence = _replay_futures_evidence(
                snapshot_row=snapshot_row,
                level_rows=level_rows,
                transition_row=transition_row,
                as_of=as_of,
                expected_symbol=symbol,
                expected_contract_id=(
                    active_market_id
                    if bridge_schema_version == 5 and active_market_id is not None
                    else contract_id
                ),
                contract_selection=contract_selection,
                rolled_from_contract_id=(
                    rolled_from_market_id
                    if bridge_schema_version == 5
                    else rolled_from_contract_id
                ),
                schema_version=bridge_schema_version,
                expected_exchange_id=exchange_id,
                expected_product_contract_id=(
                    contract_id if bridge_schema_version == 5 else None
                ),
                expected_basis_exchange_id=basis_exchange_id,
                expected_basis_contract_id=basis_contract_id,
                expected_basis_market_id=basis_market_id,
            )
        fingerprint = _replay_hash(
            candles,
            source_hashes,
            as_of,
            contract_id=contract_id,
            contract_expires_at=contract_expires_at,
            contract_roll_at=contract_roll_at,
            contract_selection=contract_selection,
            rolled_from_contract_id=rolled_from_contract_id,
            bridge_schema_version=bridge_schema_version,
            futures_evidence=futures_evidence,
            environment=environment,
            exchange_id=exchange_id,
            market_id=active_market_id,
            basis_exchange_id=basis_exchange_id,
            basis_contract_id=basis_contract_id,
            basis_market_id=basis_market_id,
            candle_market_ids=candle_market_ids,
            rolled_from_market_id=rolled_from_market_id,
        )
        return T4ReplayResult(
            symbol=symbol,
            interval_minutes=interval_minutes,
            as_of=as_of,
            candles=candles,
            reference_price=reference,
            contract_id=contract_id,
            contract_expires_at=contract_expires_at,
            contract_roll_at=contract_roll_at,
            contract_selection=contract_selection,
            rolled_from_contract_id=rolled_from_contract_id,
            bridge_schema_version=bridge_schema_version,
            futures_evidence=futures_evidence,
            source_batch_hashes=source_hashes,
            replay_fingerprint_sha256=fingerprint,
            source_batch_ids=source_batch_ids,
            environment=environment,
            exchange_id=exchange_id,
            market_id=active_market_id,
            basis_exchange_id=basis_exchange_id,
            basis_contract_id=basis_contract_id,
            basis_market_id=basis_market_id,
            candle_market_ids=candle_market_ids,
            rolled_from_market_id=rolled_from_market_id,
        )

    def _legacy_replay(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> T4ReplayResult:
        _validate_replay_request(symbol, interval_minutes, as_of, limit)
        try:
            with transaction(self.connection_factory) as connection, cursor(
                connection
            ) as db_cursor:
                db_cursor.execute("SET TRANSACTION READ ONLY")
                db_cursor.execute(
                    """
                    WITH ranked AS (
                        SELECT candle.open_time, candle.close_time,
                               candle.open_price, candle.high_price,
                               candle.low_price, candle.close_price,
                               candle.base_volume, candle.available_at,
                               candle.provider_ingested_at, batch.raw_payload_hash,
                               batch.contract_id, batch.contract_expires_at,
                               batch.contract_roll_at, batch.contract_selection,
                               batch.rolled_from_contract_id,
                               batch.reference_price, batch.reference_event_time,
                               batch.reference_available_at,
                               batch.reference_ingested_at,
                               row_number() OVER (
                                   PARTITION BY candle.open_time
                                   ORDER BY candle.available_at DESC,
                                            batch.available_at DESC,
                                            batch.t4_batch_id DESC
                               ) AS replay_rank
                        FROM crypto_agent.t4_canonical_candles AS candle
                        JOIN crypto_agent.t4_ingestion_batches AS batch
                          ON batch.t4_batch_id = candle.t4_batch_id
                        WHERE candle.logical_symbol = %s
                          AND candle.interval_seconds = %s
                          AND candle.close_time <= %s
                          AND candle.available_at <= %s
                          AND candle.provider_ingested_at <= %s
                          AND batch.available_at <= %s
                          AND batch.ingested_at <= %s
                          AND batch.status = 'completed'
                    )
                    SELECT *
                    FROM ranked
                    WHERE replay_rank = 1
                    ORDER BY open_time DESC
                    LIMIT %s
                    """,
                    (
                        symbol,
                        interval_minutes * 60,
                        as_of,
                        as_of,
                        as_of,
                        as_of,
                        as_of,
                        limit,
                    ),
                )
                rows = tuple(db_cursor.fetchall())
        except PostgresError:
            raise
        except Exception:
            raise PostgresOperationError("T4 replay failed") from None

        if len(rows) != limit:
            raise ProviderError("T4 replay history is incomplete", code="T4_REPLAY_INCOMPLETE")
        ordered = tuple(reversed(rows))
        newest = ordered[-1]
        contract_id = str(_row_value(newest, "contract_id", 10))
        candles = tuple(
            Candle(
                symbol=symbol,
                interval_minutes=interval_minutes,
                open_time=_as_utc(_row_value(row, "open_time", 0)),
                close_time=_as_utc(_row_value(row, "close_time", 1)),
                open=float(_row_value(row, "open_price", 2)),
                high=float(_row_value(row, "high_price", 3)),
                low=float(_row_value(row, "low_price", 4)),
                close=float(_row_value(row, "close_price", 5)),
                volume=float(_row_value(row, "base_volume", 6)),
                source=T4_SOURCE_ID,
                available_at=_as_utc(_row_value(row, "available_at", 7)),
                ingested_at=_as_utc(_row_value(row, "provider_ingested_at", 8)),
            )
            for row in ordered
        )
        source_batches = tuple(
            sorted(
                {
                    (
                        int(_row_value(row, "t4_batch_id", 20)),
                        str(_row_value(row, "raw_payload_hash", 9)),
                    )
                    for row in ordered
                }
            )
        )
        source_batch_ids = tuple(item[0] for item in source_batches)
        source_hashes = tuple(item[1] for item in source_batches)
        reference = ReferencePriceSnapshot(
            symbol=symbol,
            observations=(
                ReferencePriceObservation(
                    symbol=symbol,
                    price=float(_row_value(newest, "reference_price", 15)),
                    event_time=_as_utc(_row_value(newest, "reference_event_time", 16)),
                    available_at=_as_utc(
                        _row_value(newest, "reference_available_at", 17)
                    ),
                    ingested_at=_as_utc(
                        _row_value(newest, "reference_ingested_at", 18)
                    ),
                    source=T4_SOURCE_ID,
                ),
            ),
        )
        fingerprint = _replay_hash(candles, source_hashes, as_of)
        return T4ReplayResult(
            symbol=symbol,
            interval_minutes=interval_minutes,
            as_of=as_of,
            candles=candles,
            reference_price=reference,
            contract_id=contract_id,
            contract_expires_at=_as_utc(_row_value(newest, "contract_expires_at", 11)),
            contract_roll_at=_as_utc(_row_value(newest, "contract_roll_at", 12)),
            contract_selection=str(_row_value(newest, "contract_selection", 13)),
            rolled_from_contract_id=_optional_text(
                _row_value(newest, "rolled_from_contract_id", 14)
            ),
            bridge_schema_version=2,
            futures_evidence=None,
            source_batch_hashes=source_hashes,
            replay_fingerprint_sha256=fingerprint,
            source_batch_ids=source_batch_ids,
        )


def _replay_futures_evidence(
    *,
    snapshot_row: object | None,
    level_rows: tuple[object, ...],
    transition_row: object | None,
    as_of: datetime,
    expected_symbol: str,
    expected_contract_id: str,
    contract_selection: str,
    rolled_from_contract_id: str | None,
    schema_version: int,
    expected_exchange_id: str | None = None,
    expected_product_contract_id: str | None = None,
    expected_basis_exchange_id: str | None = None,
    expected_basis_contract_id: str | None = None,
    expected_basis_market_id: str | None = None,
) -> FuturesEvidence:
    if snapshot_row is None or not level_rows:
        raise ProviderError(
            "T4 replay futures evidence is incomplete",
            code="T4_REPLAY_EVIDENCE_INCOMPLETE",
        )
    try:
        contract_id = (
            str(_row_value(snapshot_row, "active_market_id", 17))
            if schema_version == 5
            else str(_row_value(snapshot_row, "contract_id", 1))
        )
        if contract_id != expected_contract_id:
            raise ValueError("contract mismatch")
        if schema_version == 5 and (
            str(_row_value(snapshot_row, "exchange_id", 15))
            != expected_exchange_id
            or str(_row_value(snapshot_row, "product_contract_id", 16))
            != expected_product_contract_id
            or str(_row_value(snapshot_row, "basis_exchange_id", 18))
            != expected_basis_exchange_id
            or str(_row_value(snapshot_row, "basis_product_contract_id", 19))
            != expected_basis_contract_id
            or str(_row_value(snapshot_row, "basis_market_id", 20))
            != expected_basis_market_id
        ):
            raise ValueError("schema-v5 snapshot identity mismatch")
        bids = tuple(
            OrderBookLevel(
                level=int(_row_value(row, "level_no", 1)),
                price=float(_row_value(row, "price", 2)),
                quantity=float(_row_value(row, "quantity", 3)),
            )
            for row in sorted(
                (
                    item
                    for item in level_rows
                    if str(_row_value(item, "side", 0)) == "bid"
                ),
                key=lambda item: int(_row_value(item, "level_no", 1)),
            )
        )
        asks = tuple(
            OrderBookLevel(
                level=int(_row_value(row, "level_no", 1)),
                price=float(_row_value(row, "price", 2)),
                quantity=float(_row_value(row, "quantity", 3)),
            )
            for row in sorted(
                (
                    item
                    for item in level_rows
                    if str(_row_value(item, "side", 0)) == "ask"
                ),
                key=lambda item: int(_row_value(item, "level_no", 1)),
            )
        )
        if not bids or not asks or len(level_rows) != len(bids) + len(asks):
            raise ValueError("book side missing")
        basis = BasisReference(
            symbol=str(_row_value(snapshot_row, "basis_reference_symbol", 7)),
            reference_type=str(
                _row_value(snapshot_row, "basis_reference_type", 8)
            ),
            source=str(_row_value(snapshot_row, "basis_reference_source", 9)),
            price=float(_row_value(snapshot_row, "basis_reference_price", 10)),
            observed_at=_as_utc(_row_value(snapshot_row, "basis_observed_at", 11)),
            available_at=_as_utc(_row_value(snapshot_row, "basis_available_at", 12)),
            ingested_at=_as_utc(_row_value(snapshot_row, "basis_ingested_at", 13)),
        )
        transition = _replay_transition(transition_row, schema_version=schema_version)
        if (contract_selection == "front_month" and transition is not None) or (
            contract_selection == "rolled"
            and (
                transition is None
                or transition.from_contract_id != rolled_from_contract_id
                or transition.to_contract_id != expected_contract_id
            )
        ):
            raise ValueError("transition scope mismatch")
        evidence = FuturesEvidence(
            contract_id=contract_id,
            source=T4_SOURCE_ID,
            session_status=SessionStatus(
                str(_row_value(snapshot_row, "session_status", 2))
            ),
            is_full_snapshot=bool(
                _row_value(snapshot_row, "is_full_snapshot", 3)
            ),
            observed_at=_as_utc(_row_value(snapshot_row, "observed_at", 4)),
            available_at=_as_utc(_row_value(snapshot_row, "available_at", 5)),
            ingested_at=_as_utc(
                _row_value(snapshot_row, "provider_ingested_at", 6)
            ),
            bids=bids,
            asks=asks,
            basis_reference=basis,
            contract_transition=transition,
        )
        if not validate_futures_evidence_structure(
            evidence,
            as_of=as_of,
            symbol=expected_symbol,
            contract_id=expected_contract_id,
            contract_selection=contract_selection,
            rolled_from_contract_id=rolled_from_contract_id,
        ):
            raise ValueError("invalid futures evidence semantics")
        if str(_row_value(snapshot_row, "content_hash", 14)) != _object_hash(
            futures_evidence_to_dict(evidence)
        ):
            raise ValueError("snapshot hash mismatch")
        for row in level_rows:
            level_payload = {
                "side": str(_row_value(row, "side", 0)),
                "level": int(_row_value(row, "level_no", 1)),
                "price": float(_row_value(row, "price", 2)),
                "quantity": float(_row_value(row, "quantity", 3)),
            }
            if str(_row_value(row, "content_hash", 4)) != _object_hash(level_payload):
                raise ValueError("order-book hash mismatch")
        if (
            transition_row is not None
            and transition is not None
            and str(_row_value(transition_row, "content_hash", 9))
            != _object_hash(_transition_payload(transition))
        ):
            raise ValueError("transition hash mismatch")
        return evidence
    except (IndexError, KeyError, TypeError, ValueError):
        raise ProviderError(
            "T4 replay futures evidence is invalid",
            code="T4_REPLAY_EVIDENCE_INVALID",
        ) from None


def _replay_transition(
    row: object | None, *, schema_version: int
) -> ContractTransitionEvidence | None:
    if row is None:
        return None
    return ContractTransitionEvidence(
        from_contract_id=str(
            _row_value(
                row,
                "from_market_id" if schema_version == 5 else "from_contract_id",
                12 if schema_version == 5 else 0,
            )
        ),
        to_contract_id=str(
            _row_value(
                row,
                "to_market_id" if schema_version == 5 else "to_contract_id",
                13 if schema_version == 5 else 1,
            )
        ),
        price_type=str(_row_value(row, "price_type", 2)),
        from_price=float(_row_value(row, "from_price", 3)),
        to_price=float(_row_value(row, "to_price", 4)),
        source=str(_row_value(row, "evidence_source", 5)),
        observed_at=_as_utc(_row_value(row, "observed_at", 6)),
        available_at=_as_utc(_row_value(row, "available_at", 7)),
        ingested_at=_as_utc(_row_value(row, "provider_ingested_at", 8)),
    )


def _verify_replay_batch_integrity(
    rows: tuple[object, ...],
    candles: tuple[Candle, ...],
) -> None:
    try:
        batch_ids = {int(_row_value(row, "t4_batch_id", 20)) for row in rows}
        raw_pairs = {
            (
                str(_row_value(row, "raw_payload_hash", 9)),
                str(_row_value(row, "raw_payload_base64", 21)),
            )
            for row in rows
        }
        if len(batch_ids) != 1 or len(raw_pairs) != 1 or len(rows) != len(candles):
            raise ValueError("batch scope mismatch")
        raw_hash, raw_base64 = next(iter(raw_pairs))
        decoded = base64.b64decode(raw_base64.encode("ascii"), validate=True)
        if hashlib.sha256(decoded).hexdigest() != raw_hash:
            raise ValueError("raw payload hash mismatch")
        for row, candle in zip(rows, candles, strict=True):
            schema_version = int(
                _row_value(row, "bridge_schema_version", 19)
            )
            candle_market_id = (
                str(_row_value(row, "candle_market_id", 30))
                if schema_version == 5
                else None
            )
            if str(_row_value(row, "content_hash", 22)) != _candle_hash(
                candle,
                market_id=candle_market_id,
            ):
                raise ValueError("candle hash mismatch")
    except (binascii.Error, LookupError, TypeError, ValueError):
        raise ProviderError(
            "T4 replay integrity verification failed",
            code="T4_REPLAY_INTEGRITY_FAILURE",
        ) from None


def _persist_futures_evidence(
    db_cursor: DBCursor,
    *,
    batch_id: int,
    source_id: int,
    market_id: int,
    logical_symbol: str,
    evidence: FuturesEvidence,
    schema_version: int,
    exchange_id: str | None,
    product_contract_id: str | None,
    active_market_id: str | None,
    basis_exchange_id: str | None,
    basis_contract_id: str | None,
    basis_market_id: str | None,
) -> None:
    basis = evidence.basis_reference
    canonical = futures_evidence_to_dict(evidence)
    if canonical is None:
        raise ValueError("T4 futures evidence is missing")
    db_cursor.execute(
        """
        INSERT INTO crypto_agent.t4_futures_snapshots (
            t4_batch_id, source_id, market_id, logical_symbol, contract_id,
            session_status, is_full_snapshot, observed_at, available_at,
            provider_ingested_at, basis_reference_symbol,
            basis_reference_type, basis_reference_source, basis_reference_price,
            basis_observed_at, basis_available_at, basis_ingested_at, content_hash,
            exchange_id, product_contract_id, active_market_id,
            basis_exchange_id, basis_product_contract_id, basis_market_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        RETURNING t4_snapshot_id
        """,
        (
            batch_id,
            source_id,
            market_id,
            logical_symbol,
            product_contract_id if schema_version == 5 else evidence.contract_id,
            evidence.session_status.value,
            evidence.is_full_snapshot,
            evidence.observed_at,
            evidence.available_at,
            evidence.ingested_at,
            basis.symbol,
            basis.reference_type,
            basis.source,
            basis.price,
            basis.observed_at,
            basis.available_at,
            basis.ingested_at,
            _object_hash(canonical),
            exchange_id,
            product_contract_id,
            active_market_id,
            basis_exchange_id,
            basis_contract_id,
            basis_market_id,
        ),
    )
    snapshot_row = db_cursor.fetchone()
    if snapshot_row is None:
        raise PostgresOperationError("T4 futures snapshot was not stored")
    snapshot_id = int(_row_value(snapshot_row, "t4_snapshot_id", 0))
    for side, levels in (("bid", evidence.bids), ("ask", evidence.asks)):
        for level in levels:
            db_cursor.execute(
                """
                INSERT INTO crypto_agent.t4_orderbook_levels (
                    t4_snapshot_id, side, level_no, price, quantity, content_hash
                ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    snapshot_id,
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
                ),
            )

    transition = evidence.contract_transition
    if transition is not None:
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.t4_contract_transition_evidence (
                t4_batch_id, source_id, market_id, logical_symbol,
                from_contract_id, to_contract_id, price_type, from_price,
                to_price, evidence_source, observed_at, available_at,
                provider_ingested_at, content_hash, exchange_id,
                product_contract_id, from_market_id, to_market_id
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                batch_id,
                source_id,
                market_id,
                logical_symbol,
                transition.from_contract_id,
                transition.to_contract_id,
                transition.price_type,
                transition.from_price,
                transition.to_price,
                transition.source,
                transition.observed_at,
                transition.available_at,
                transition.ingested_at,
                _object_hash(_transition_payload(transition)),
                exchange_id,
                product_contract_id,
                transition.from_contract_id if schema_version == 5 else None,
                transition.to_contract_id if schema_version == 5 else None,
            ),
        )


class T4ReplayProvider:
    source_id = T4_SOURCE_ID

    def __init__(self, repository: T4IngestRepository) -> None:
        self.repository = repository

    def fetch_batch(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> ProviderBatch:
        return self.repository.replay(
            symbol=symbol,
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
        ).as_provider_batch()

    def fetch_candles(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> list[Candle]:
        return list(
            self.fetch_batch(
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=as_of,
                limit=limit,
            ).candles
        )


class T4IngestionScheduler:
    """Small stoppable scheduler; every cycle uses a fresh UTC point-in-time cutoff."""

    def __init__(
        self,
        fetch_and_persist: Callable[[str, int, datetime], T4IngestionReceipt],
        *,
        poll_seconds: float = 300.0,
    ) -> None:
        if isinstance(poll_seconds, bool) or not math.isfinite(poll_seconds) or poll_seconds < 1:
            raise ValueError("poll_seconds must be finite and at least one second")
        self.fetch_and_persist = fetch_and_persist
        self.poll_seconds = float(poll_seconds)

    def run_once(
        self,
        *,
        symbols: Sequence[str] = ("BTC/USD", "ETH/USD"),
        intervals: Sequence[int] = (240, 1440, 10080),
        as_of: datetime | None = None,
    ) -> tuple[T4IngestionReceipt, ...]:
        cutoff = as_of or datetime.now(UTC)
        return tuple(
            self.fetch_and_persist(symbol, interval, cutoff)
            for symbol in symbols
            for interval in intervals
        )

    def run_forever(
        self,
        stop_event: threading.Event,
        *,
        symbols: Sequence[str] = ("BTC/USD", "ETH/USD"),
        intervals: Sequence[int] = (240, 1440, 10080),
    ) -> None:
        while not stop_event.is_set():
            self.run_once(symbols=symbols, intervals=intervals)
            stop_event.wait(self.poll_seconds)


def _validated_envelope(
    batch: ProviderBatch, *, requested_as_of: datetime
) -> dict[str, object]:
    if requested_as_of.tzinfo is None or requested_as_of.utcoffset() != timedelta(0):
        raise ValueError("requested_as_of must be UTC")
    if not batch.candles or batch.candles != batch.input_candles:
        raise ValueError("T4 ingest requires one exact non-empty input batch")
    raw = batch.raw_payload
    if not isinstance(raw, bytes) or not raw:
        raise ValueError("T4 ingest requires the exact raw bridge payload")
    if len(raw) > T4_MAX_RAW_PAYLOAD_BYTES:
        raise ValueError("T4 raw payload exceeds the approved size")
    if hashlib.sha256(raw).hexdigest() != _required_payload_hash(batch):
        raise ValueError("T4 raw payload hash mismatch")
    try:
        parsed: object = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ValueError("T4 raw payload is not valid JSON") from None
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) for key in parsed
    ):
        raise ValueError("T4 raw payload must be an object")
    envelope = cast(dict[str, object], parsed)
    metadata = batch.metadata
    schema_version = envelope.get("schema_version")
    if (
        type(schema_version) is not int
        or schema_version not in T4_SUPPORTED_SCHEMA_VERSIONS
        or metadata.get("t4_bridge_schema_version") != schema_version
    ):
        raise ValueError("T4 raw payload schema is unsupported")
    required: dict[str, object] = {
        "schema_version": schema_version,
        "source_id": T4_SOURCE_ID,
        "venue_id": T4_VENUE_ID,
        "read_only": True,
        "order_routes_exposed": False,
        "logical_symbol": batch.candles[0].symbol,
        "interval_minutes": batch.candles[0].interval_minutes,
        "contract_selection": metadata.get("t4_contract_selection"),
    }
    if schema_version == 5:
        required.update(
            {
                "environment": metadata.get("t4_environment"),
                "exchange_id": metadata.get("t4_exchange_id"),
                "contract_id": metadata.get("t4_contract_id"),
                "market_id": metadata.get("t4_market_id"),
                "rolled_from_market_id": metadata.get(
                    "t4_rolled_from_market_id"
                ),
            }
        )
    else:
        required.update(
            {
                "contract_id": metadata.get("t4_contract_id"),
                "rolled_from_contract_id": metadata.get(
                    "t4_rolled_from_contract_id"
                ),
            }
        )
    if any(envelope.get(key) != value for key, value in required.items()):
        raise ValueError("T4 raw payload attestation mismatch")
    if schema_version == 4 and envelope.get("environment") != "live_t4":
        raise ValueError("T4 live environment attestation mismatch")
    if schema_version == 4 and metadata.get("t4_environment") != "live_t4":
        raise ValueError("T4 live environment metadata mismatch")
    if schema_version == 5:
        if envelope.get("environment") not in {"t4_simulator", "live_t4"}:
            raise ValueError("T4 environment attestation mismatch")
        for key, max_length in (
            ("exchange_id", 64),
            ("contract_id", 128),
            ("market_id", 256),
        ):
            if not _valid_opaque_t4_id(envelope.get(key), max_length=max_length):
                raise ValueError(f"T4 {key} attestation is invalid")
        rolled_from_market_id = envelope.get("rolled_from_market_id")
        if (
            envelope.get("contract_selection") == "front_month"
            and rolled_from_market_id is not None
        ) or (
            envelope.get("contract_selection") == "rolled"
            and (
                not _valid_opaque_t4_id(rolled_from_market_id, max_length=256)
                or rolled_from_market_id == envelope.get("market_id")
            )
        ):
            raise ValueError("T4 roll MarketID attestation mismatch")
        raw_evidence = envelope.get("futures_evidence")
        raw_basis = (
            raw_evidence.get("basis_reference")
            if isinstance(raw_evidence, dict)
            else None
        )
        if not isinstance(raw_basis, dict):
            raise ValueError("T4 basis identity is missing")
        for raw_key, metadata_key, max_length in (
            ("exchange_id", "t4_basis_exchange_id", 64),
            ("contract_id", "t4_basis_contract_id", 128),
            ("market_id", "t4_basis_market_id", 256),
        ):
            if (
                raw_basis.get(raw_key) != metadata.get(metadata_key)
                or not _valid_opaque_t4_id(
                    raw_basis.get(raw_key), max_length=max_length
                )
            ):
                raise ValueError("T4 basis identity attestation mismatch")
        if (
            raw_basis.get("exchange_id") == envelope.get("exchange_id")
            and raw_basis.get("contract_id") == envelope.get("contract_id")
            and raw_basis.get("market_id") == envelope.get("market_id")
        ):
            raise ValueError("T4 basis identity is not independent")
    expires_at = _utc_metadata(metadata, "t4_contract_expires_at")
    roll_at = _utc_metadata(metadata, "t4_contract_roll_at")
    if (
        _parse_utc_text(envelope.get("contract_expires_at")) != expires_at
        or _parse_utc_text(envelope.get("contract_roll_at")) != roll_at
        or not requested_as_of < roll_at < expires_at
    ):
        raise ValueError("T4 raw payload contract lifecycle mismatch")
    raw_candles = envelope.get("candles")
    if not isinstance(raw_candles, list) or len(raw_candles) != len(batch.candles):
        raise ValueError("T4 raw payload candle count mismatch")
    if any(
        candle.source != T4_SOURCE_ID
        or candle.symbol != required["logical_symbol"]
        or candle.interval_minutes != required["interval_minutes"]
        for candle in batch.candles
    ):
        raise ValueError("T4 normalized candle scope mismatch")
    if any(
        not _raw_candle_matches(raw_item, candle)
        for raw_item, candle in zip(raw_candles, batch.candles, strict=True)
    ):
        raise ValueError("T4 raw payload differs from normalized candles")
    if schema_version == 5:
        candle_market_ids = metadata.get("t4_candle_market_ids")
        if (
            not isinstance(candle_market_ids, list)
            or len(candle_market_ids) != len(raw_candles)
            or any(
                not isinstance(raw_item, dict)
                or raw_item.get("market_id") != market_id
                or not _valid_opaque_t4_id(market_id, max_length=256)
                for raw_item, market_id in zip(
                    raw_candles, candle_market_ids, strict=True
                )
            )
            or candle_market_ids[-1] != required["market_id"]
        ):
            raise ValueError("T4 candle MarketID attestation mismatch")
    reference = _single_reference(batch)
    raw_reference = envelope.get("reference_price")
    if (
        not isinstance(raw_reference, dict)
        or raw_reference.get("symbol") != reference.symbol
        or raw_reference.get("source") != reference.source
        or not _same_number(raw_reference.get("price"), reference.price)
        or _parse_utc_text(raw_reference.get("event_time")) != reference.event_time
        or _parse_utc_text(raw_reference.get("available_at")) != reference.available_at
        or _parse_utc_text(raw_reference.get("ingested_at")) != reference.ingested_at
    ):
        raise ValueError("T4 raw reference price attestation mismatch")
    if schema_version == 2:
        if batch.futures_evidence is not None or "futures_evidence" in envelope:
            raise ValueError("T4 schema v2 cannot contain futures evidence")
    elif not _raw_futures_evidence_matches(
        envelope.get("futures_evidence"),
        batch.futures_evidence,
        schema_version=schema_version,
        exchange_id=required.get("exchange_id"),
        contract_id=required.get("contract_id"),
        market_id=required.get("market_id"),
        basis_exchange_id=metadata.get("t4_basis_exchange_id"),
        basis_contract_id=metadata.get("t4_basis_contract_id"),
        basis_market_id=metadata.get("t4_basis_market_id"),
    ):
        raise ValueError("T4 raw futures evidence attestation mismatch")
    elif not validate_futures_evidence_structure(
        batch.futures_evidence,
        as_of=requested_as_of,
        symbol=str(required["logical_symbol"]),
        contract_id=(
            required["market_id"] if schema_version == 5 else required["contract_id"]
        ),
        contract_selection=required["contract_selection"],
        rolled_from_contract_id=(
            required["rolled_from_market_id"]
            if schema_version == 5
            else required["rolled_from_contract_id"]
        ),
    ):
        raise ValueError("T4 futures evidence semantics are invalid")
    return envelope


def _single_reference(batch: ProviderBatch) -> ReferencePriceObservation:
    reference = batch.reference_price
    if (
        not isinstance(reference, ReferencePriceSnapshot)
        or len(reference.observations) != 1
        or reference.observations[0].source != T4_SOURCE_ID
    ):
        raise ValueError("T4 ingest requires one attested reference price")
    return reference.observations[0]


def _required_payload_hash(batch: ProviderBatch) -> str:
    value = batch.raw_payload_sha256
    if not isinstance(value, str) or len(value) != 64:
        raise ValueError("T4 raw payload SHA-256 is missing")
    try:
        int(value, 16)
    except ValueError:
        raise ValueError("T4 raw payload SHA-256 is invalid") from None
    return value.lower()


def _utc_metadata(metadata: dict[str, object], key: str) -> datetime:
    value = metadata.get(key)
    if not isinstance(value, str):
        raise ValueError(f"{key} is missing")
    return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _validate_replay_request(
    symbol: str, interval_minutes: int, as_of: datetime, limit: int
) -> None:
    if symbol not in {"BTC/USD", "ETH/USD"}:
        raise ProviderError("T4 replay instrument is not approved", code="T4_SYMBOL_NOT_ALLOWED")
    if interval_minutes not in {240, 1440, 10080}:
        raise ProviderError("T4 replay interval is not approved", code="T4_INTERVAL_NOT_ALLOWED")
    if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
        raise ProviderError("T4 replay cutoff must be UTC", code="T4_TIME_INVALID")
    if type(limit) is not int or not 60 <= limit <= 720:
        raise ProviderError("T4 replay limit is invalid", code="T4_LIMIT_INVALID")


def _candle_hash(candle: Candle, *, market_id: str | None = None) -> str:
    payload = {
        "available_at": candle.available_at.isoformat(),
        "close": candle.close,
        "close_time": candle.close_time.isoformat(),
        "high": candle.high,
        "ingested_at": candle.ingested_at.isoformat(),
        "interval_minutes": candle.interval_minutes,
        "low": candle.low,
        "open": candle.open,
        "open_time": candle.open_time.isoformat(),
        "source": candle.source,
        "symbol": candle.symbol,
        "volume": candle.volume,
    }
    if market_id is not None:
        payload["market_id"] = market_id
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def _raw_candle_matches(raw: object, candle: Candle) -> bool:
    if not isinstance(raw, dict):
        return False
    try:
        return bool(
            raw.get("symbol") == candle.symbol
            and raw.get("interval_minutes") == candle.interval_minutes
            and raw.get("source") == candle.source
            and _parse_utc_text(raw.get("open_time")) == candle.open_time
            and _parse_utc_text(raw.get("close_time")) == candle.close_time
            and _parse_utc_text(raw.get("available_at")) == candle.available_at
            and _parse_utc_text(raw.get("ingested_at")) == candle.ingested_at
            and _same_number(raw.get("open"), candle.open)
            and _same_number(raw.get("high"), candle.high)
            and _same_number(raw.get("low"), candle.low)
            and _same_number(raw.get("close"), candle.close)
            and _same_number(raw.get("volume"), candle.volume)
        )
    except (TypeError, ValueError):
        return False


def _raw_futures_evidence_matches(
    raw: object,
    evidence: FuturesEvidence | None,
    *,
    schema_version: int,
    exchange_id: object = None,
    contract_id: object = None,
    market_id: object = None,
    basis_exchange_id: object = None,
    basis_contract_id: object = None,
    basis_market_id: object = None,
) -> bool:
    if not isinstance(raw, dict) or not isinstance(evidence, FuturesEvidence):
        return False
    try:
        evidence_scope_matches = (
            raw.get("exchange_id") == exchange_id
            and raw.get("contract_id") == contract_id
            and raw.get("market_id") == market_id == evidence.contract_id
            if schema_version == 5
            else raw.get("contract_id") == evidence.contract_id
        )
        if not bool(
            evidence_scope_matches
            and raw.get("source_id") == evidence.source
            and raw.get("session_status") == evidence.session_status.value
            and raw.get("is_full_snapshot") is evidence.is_full_snapshot
            and _parse_utc_text(raw.get("observed_at")) == evidence.observed_at
            and _parse_utc_text(raw.get("available_at")) == evidence.available_at
            and _parse_utc_text(raw.get("ingested_at")) == evidence.ingested_at
            and _raw_levels_match(raw.get("bids"), evidence.bids)
            and _raw_levels_match(raw.get("asks"), evidence.asks)
        ):
            return False
        basis = raw.get("basis_reference")
        if not isinstance(basis, dict) or not bool(
            basis.get("symbol") == evidence.basis_reference.symbol
            and (
                schema_version != 5
                or (
                    basis.get("exchange_id") == basis_exchange_id
                    and basis.get("contract_id") == basis_contract_id
                    and basis.get("market_id") == basis_market_id
                )
            )
            and basis.get("reference_type") == evidence.basis_reference.reference_type
            and basis.get("source") == evidence.basis_reference.source
            and _same_number(basis.get("price"), evidence.basis_reference.price)
            and _parse_utc_text(basis.get("observed_at"))
            == evidence.basis_reference.observed_at
            and _parse_utc_text(basis.get("available_at"))
            == evidence.basis_reference.available_at
            and _parse_utc_text(basis.get("ingested_at"))
            == evidence.basis_reference.ingested_at
        ):
            return False
        return _raw_transition_matches(
            raw.get("contract_transition"),
            evidence.contract_transition,
            schema_version=schema_version,
        )
    except (TypeError, ValueError):
        return False


def _raw_levels_match(
    raw: object, levels: tuple[OrderBookLevel, ...]
) -> bool:
    return bool(
        isinstance(raw, list)
        and len(raw) == len(levels)
        and all(
            isinstance(item, dict)
            and item.get("level") == level.level
            and _same_number(item.get("price"), level.price)
            and _same_number(item.get("quantity"), level.quantity)
            for item, level in zip(raw, levels, strict=True)
        )
    )


def _raw_transition_matches(
    raw: object,
    transition: ContractTransitionEvidence | None,
    *,
    schema_version: int = 4,
) -> bool:
    if transition is None:
        return raw is None
    if not isinstance(raw, dict):
        return False
    from_key = "from_market_id" if schema_version == 5 else "from_contract_id"
    to_key = "to_market_id" if schema_version == 5 else "to_contract_id"
    return bool(
        raw.get(from_key) == transition.from_contract_id
        and raw.get(to_key) == transition.to_contract_id
        and raw.get("price_type") == transition.price_type
        and raw.get("source") == transition.source
        and _same_number(raw.get("from_price"), transition.from_price)
        and _same_number(raw.get("to_price"), transition.to_price)
        and _parse_utc_text(raw.get("observed_at")) == transition.observed_at
        and _parse_utc_text(raw.get("available_at")) == transition.available_at
        and _parse_utc_text(raw.get("ingested_at")) == transition.ingested_at
    )


def _valid_opaque_t4_id(value: object, *, max_length: int) -> bool:
    return bool(
        isinstance(value, str)
        and 1 <= len(value) <= max_length
        and value == value.strip()
        and not any(ord(character) < 32 or ord(character) == 127 for character in value)
    )


def _parse_utc_text(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is not text")
    return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _same_number(left: object, right: float) -> bool:
    if isinstance(left, bool) or not isinstance(left, (int, float)):
        return False
    return math.isfinite(float(left)) and float(left) == float(right)


def _replay_hash(
    candles: tuple[Candle, ...],
    source_hashes: tuple[str, ...],
    as_of: datetime,
    *,
    contract_id: str | None = None,
    contract_expires_at: datetime | None = None,
    contract_roll_at: datetime | None = None,
    contract_selection: str | None = None,
    rolled_from_contract_id: str | None = None,
    bridge_schema_version: int | None = None,
    futures_evidence: FuturesEvidence | None = None,
    environment: str | None = None,
    exchange_id: str | None = None,
    market_id: str | None = None,
    basis_exchange_id: str | None = None,
    basis_contract_id: str | None = None,
    basis_market_id: str | None = None,
    candle_market_ids: tuple[str, ...] = (),
    rolled_from_market_id: str | None = None,
) -> str:
    market_ids: tuple[str | None, ...] = (
        candle_market_ids
        if bridge_schema_version == 5
        else (None,) * len(candles)
    )
    if len(market_ids) != len(candles):
        raise ValueError("T4 replay candle MarketID count mismatch")
    payload = {
        "as_of": as_of.isoformat(),
        "candles": [
            _candle_hash(candle, market_id=candle_market_id)
            for candle, candle_market_id in zip(candles, market_ids, strict=True)
        ],
        "source_batch_hashes": source_hashes,
        "contract_lifecycle": {
            "contract_id": contract_id,
            "contract_expires_at": (
                contract_expires_at.isoformat()
                if contract_expires_at is not None
                else None
            ),
            "contract_roll_at": (
                contract_roll_at.isoformat() if contract_roll_at is not None else None
            ),
            "contract_selection": contract_selection,
            "rolled_from_contract_id": rolled_from_contract_id,
            "bridge_schema_version": bridge_schema_version,
        },
        "futures_evidence": futures_evidence_to_dict(futures_evidence),
    }
    if bridge_schema_version == 5:
        payload["t4_v5_identity"] = {
            "environment": environment,
            "exchange_id": exchange_id,
            "market_id": market_id,
            "basis_exchange_id": basis_exchange_id,
            "basis_contract_id": basis_contract_id,
            "basis_market_id": basis_market_id,
            "candle_market_ids": candle_market_ids,
            "rolled_from_market_id": rolled_from_market_id,
        }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


def _replay_provenance_hash(
    source_batch_ids: tuple[int, ...],
    source_batch_hashes: tuple[str, ...],
) -> str:
    if (
        not source_batch_ids
        or len(source_batch_ids) != len(source_batch_hashes)
        or tuple(sorted(source_batch_ids)) != source_batch_ids
        or len(set(source_batch_ids)) != len(source_batch_ids)
        or any(item <= 0 for item in source_batch_ids)
        or any(re.fullmatch(r"[0-9a-f]{64}", item) is None for item in source_batch_hashes)
    ):
        raise ValueError("T4 replay source-batch provenance is invalid")
    framed = "t4-replay-provenance-v1\n" + "".join(
        f"{batch_id}:{batch_hash}\n"
        for batch_id, batch_hash in zip(
            source_batch_ids,
            source_batch_hashes,
            strict=True,
        )
    )
    return hashlib.sha256(framed.encode("ascii")).hexdigest()


def _object_hash(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _transition_payload(
    transition: ContractTransitionEvidence,
) -> dict[str, object]:
    return {
        "from_contract_id": transition.from_contract_id,
        "to_contract_id": transition.to_contract_id,
        "price_type": transition.price_type,
        "from_price": transition.from_price,
        "to_price": transition.to_price,
        "source": transition.source,
        "observed_at": transition.observed_at.isoformat(),
        "available_at": transition.available_at.isoformat(),
        "ingested_at": transition.ingested_at.isoformat(),
    }


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _row_value(row: object, name: str, index: int) -> object:
    if isinstance(row, Mapping):
        typed_row = cast(Mapping[object, object], row)
        if name in typed_row:
            return typed_row[name]
        return tuple(typed_row.values())[index]
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        return cast(Sequence[object], row)[index]
    raise PostgresOperationError("PostgreSQL returned an unsupported T4 row shape")


def _as_utc(value: object) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("PostgreSQL replay returned an invalid timestamp")
    normalized = value.astimezone(UTC)
    if normalized.utcoffset() != timedelta(0):
        raise ValueError("PostgreSQL replay timestamp is not UTC")
    return normalized


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)

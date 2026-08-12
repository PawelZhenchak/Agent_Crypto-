from __future__ import annotations

import base64
import hashlib
import json
import math
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import cast

from .domain import Candle, ReferencePriceObservation, ReferencePriceSnapshot
from .postgres import (
    ConnectionFactory,
    PostgresError,
    PostgresOperationError,
    cursor,
    transaction,
)
from .providers.base import ProviderBatch, ProviderError


T4_SOURCE_ID = "plus500_t4_futures_v1"
T4_VENUE_ID = "plus500_t4"
T4_SCHEMA_VERSION = 2
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
    source_batch_hashes: tuple[str, ...]
    replay_fingerprint_sha256: str

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
                "t4_bridge_schema_version": T4_SCHEMA_VERSION,
                "t4_contract_id": self.contract_id,
                "t4_contract_expires_at": self.contract_expires_at.isoformat(),
                "t4_contract_roll_at": self.contract_roll_at.isoformat(),
                "t4_contract_selection": self.contract_selection,
                "t4_rolled_from_contract_id": self.rolled_from_contract_id,
                "t4_replay": True,
                "t4_replay_as_of": self.as_of.isoformat(),
                "t4_replay_fingerprint_sha256": self.replay_fingerprint_sha256,
                "t4_replay_source_batch_hashes": list(self.source_batch_hashes),
            },
            reference_price=self.reference_price,
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
                        observed_at, available_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, 'completed', %s, %s
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
                        metadata["t4_rolled_from_contract_id"],
                        candles[0].interval_minutes * 60,
                        requested_as_of,
                        T4_SCHEMA_VERSION,
                        reference.price,
                        reference.event_time,
                        reference.available_at,
                        reference.ingested_at,
                        base64.b64encode(batch.raw_payload or b"").decode("ascii"),
                        payload_hash,
                        len(candles),
                        observed_at,
                        available_at,
                    ),
                )
                inserted_row = db_cursor.fetchone()
                inserted = inserted_row is not None
                if inserted:
                    batch_id = int(_row_value(inserted_row, "t4_batch_id", 0))
                    for candle in candles:
                        db_cursor.execute(
                            """
                            INSERT INTO crypto_agent.t4_canonical_candles (
                                t4_batch_id, source_id, market_id, logical_symbol,
                                contract_id, interval_seconds, open_time, close_time,
                                open_price, high_price, low_price, close_price,
                                base_volume, available_at, provider_ingested_at,
                                content_hash
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s,
                                %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            """,
                            (
                                batch_id,
                                source_id,
                                market_id,
                                candle.symbol,
                                metadata["t4_contract_id"],
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
                                _candle_hash(candle),
                            ),
                        )
                else:
                    db_cursor.execute(
                        """
                        SELECT t4_batch_id, record_count
                        FROM crypto_agent.t4_ingestion_batches
                        WHERE raw_payload_hash = %s
                        """,
                        (payload_hash,),
                    )
                    existing = db_cursor.fetchone()
                    if existing is None or int(_row_value(existing, "record_count", 1)) != len(
                        candles
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
        source_hashes = tuple(
            sorted({str(_row_value(row, "raw_payload_hash", 9)) for row in ordered})
        )
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
            source_batch_hashes=source_hashes,
            replay_fingerprint_sha256=fingerprint,
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
        cutoff = as_of or datetime.now(timezone.utc)
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
    required = {
        "schema_version": T4_SCHEMA_VERSION,
        "source_id": T4_SOURCE_ID,
        "venue_id": T4_VENUE_ID,
        "read_only": True,
        "order_routes_exposed": False,
        "logical_symbol": batch.candles[0].symbol,
        "interval_minutes": batch.candles[0].interval_minutes,
        "contract_id": metadata.get("t4_contract_id"),
        "contract_selection": metadata.get("t4_contract_selection"),
        "rolled_from_contract_id": metadata.get("t4_rolled_from_contract_id"),
    }
    if any(envelope.get(key) != value for key, value in required.items()):
        raise ValueError("T4 raw payload attestation mismatch")
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


def _candle_hash(candle: Candle) -> str:
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


def _parse_utc_text(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp is not text")
    return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _same_number(left: object, right: float) -> bool:
    if isinstance(left, bool) or not isinstance(left, (int, float)):
        return False
    return math.isfinite(float(left)) and float(left) == float(right)


def _replay_hash(
    candles: tuple[Candle, ...], source_hashes: tuple[str, ...], as_of: datetime
) -> str:
    payload = {
        "as_of": as_of.isoformat(),
        "candles": [_candle_hash(candle) for candle in candles],
        "source_batch_hashes": source_hashes,
    }
    return hashlib.sha256(_canonical_json(payload)).hexdigest()


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
    normalized = value.astimezone(timezone.utc)
    if normalized.utcoffset() != timedelta(0):
        raise ValueError("PostgreSQL replay timestamp is not UTC")
    return normalized


def _optional_text(value: object) -> str | None:
    return None if value is None else str(value)

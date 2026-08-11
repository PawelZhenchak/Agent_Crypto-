from __future__ import annotations

import hashlib
import hmac
import json
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import cast

from .consensus_math import (
    CONSENSUS_ALGORITHM_VERSION,
    CONSENSUS_WINDOW_SIZE,
    ConsensusInput,
    ConsensusMathError,
    ConsensusPolicyParameters,
    ConsensusResult,
    build_cross_exchange_consensus,
)
from .domain import Candle
from .policy import PolicyConfigurationError, RiskPolicy
from .postgres import (
    ConnectionFactory,
    DBCursor,
    PostgresError,
    PostgresOperationError,
    cursor,
    transaction,
)
from .providers.coinbase import CoinbaseExchangePublicProvider
from .providers.kraken import KrakenPublicProvider
from .reference_price import (
    REFERENCE_PRICE_ALGORITHM_VERSION,
    ReferencePriceInput,
    ReferencePriceMathError,
    ReferencePriceResult,
    build_reference_price_snapshot,
)


SUPPORTED_CANONICAL_ALGORITHM = CONSENSUS_ALGORITHM_VERSION
NORMALIZED_VOLUME_UNIT = "dimensionless_ratio_to_source_median"
APPROVED_SOURCE_VENUES = {
    KrakenPublicProvider.source_id: "kraken",
    CoinbaseExchangePublicProvider.source_id: "coinbase",
}
LEGACY_R1_POLICY_ID = "v1-read-only-2026-08-10"
LEGACY_R1_POLICY_HASH = (
    "9d01dff8aa6832222e0cb2c11cfb2d45d48a6e2d7885f41f4cfa0adf83c2e9ee"
)
_LEGACY_R1_POLICY_FIELDS = frozenset(
    field
    for field in RiskPolicy.__dataclass_fields__
    if field
    not in {
        "policy_schema_version",
        "max_reference_price_age_seconds",
        "max_reference_price_deviation_from_median_bps",
    }
)


class IngestError(RuntimeError):
    pass


class IngestValidationError(IngestError):
    pass


class PersistenceInvariantError(IngestError):
    pass


@dataclass(frozen=True, slots=True)
class PersistedCandle:
    candle_id: int
    revision_no: int
    content_hash: str
    inserted: bool


@dataclass(frozen=True, slots=True)
class PersistedReferencePrice:
    reference_price_id: int
    content_hash: str
    inserted: bool


@dataclass(frozen=True, slots=True)
class ReferencePriceDraft:
    """Untrusted reference-price claim backed by exactly two raw one-minute rows."""

    canonical_market_id: int
    risk_policy_id: int
    symbol: str
    event_time: datetime
    median_price: Decimal
    pairwise_divergence_bps: Decimal
    max_deviation_from_median_bps: Decimal
    algorithm_version: str
    policy_hash: str
    source_candle_ids: tuple[int, int]

    def __post_init__(self) -> None:
        if self.canonical_market_id <= 0 or self.risk_policy_id <= 0:
            raise IngestValidationError(
                "Reference-price market and risk policy ids must be positive"
            )
        if not self.symbol.strip():
            raise IngestValidationError("Reference-price symbol must not be empty")
        _require_utc(self.event_time, "event_time")
        if self.algorithm_version != REFERENCE_PRICE_ALGORITHM_VERSION:
            raise IngestValidationError("Reference-price algorithm is unsupported")
        if not _is_sha256(self.policy_hash):
            raise IngestValidationError(
                "Reference-price policy_hash must be a lowercase SHA-256 digest"
            )
        if (
            len(self.source_candle_ids) != len(APPROVED_SOURCE_VENUES)
            or len(set(self.source_candle_ids)) != len(APPROVED_SOURCE_VENUES)
            or any(candle_id <= 0 for candle_id in self.source_candle_ids)
        ):
            raise IngestValidationError(
                "Reference price requires exactly two distinct positive source candle ids"
            )
        numeric_values = (
            self.median_price,
            self.pairwise_divergence_bps,
            self.max_deviation_from_median_bps,
        )
        if any(not isinstance(value, Decimal) for value in numeric_values):
            raise IngestValidationError(
                "Reference-price values must be Decimal instances"
            )
        if any(not value.is_finite() for value in numeric_values):
            raise IngestValidationError("Reference-price values must be finite")
        if self.median_price <= 0:
            raise IngestValidationError("Reference-price median must be positive")
        if self.pairwise_divergence_bps < 0 or self.max_deviation_from_median_bps < 0:
            raise IngestValidationError("Reference-price divergence must be non-negative")

    @classmethod
    def from_numbers(
        cls,
        *,
        canonical_market_id: int,
        risk_policy_id: int,
        symbol: str,
        event_time: datetime,
        median_price: int | float | Decimal,
        pairwise_divergence_bps: int | float | Decimal,
        max_deviation_from_median_bps: int | float | Decimal,
        algorithm_version: str,
        policy_hash: str,
        source_candle_ids: Sequence[int],
    ) -> ReferencePriceDraft:
        return cls(
            canonical_market_id=canonical_market_id,
            risk_policy_id=risk_policy_id,
            symbol=symbol,
            event_time=event_time,
            median_price=_decimal(median_price, "median_price"),
            pairwise_divergence_bps=_decimal(
                pairwise_divergence_bps,
                "pairwise_divergence_bps",
            ),
            max_deviation_from_median_bps=_decimal(
                max_deviation_from_median_bps,
                "max_deviation_from_median_bps",
            ),
            algorithm_version=algorithm_version,
            policy_hash=policy_hash,
            source_candle_ids=cast(tuple[int, int], tuple(source_candle_ids)),
        )


@dataclass(frozen=True, slots=True)
class ReferencePriceRecord:
    reference_price_id: int
    canonical_market_id: int
    risk_policy_id: int
    symbol: str
    event_time: datetime
    cutoff_as_of: datetime
    evaluated_at: datetime
    median_price: Decimal
    pairwise_divergence_bps: Decimal
    max_deviation_from_median_bps: Decimal
    algorithm_version: str
    policy_hash: str
    available_at: datetime
    ingested_at: datetime
    inputs_hash: str
    evidence_hash: str
    content_hash: str
    source_candle_ids: tuple[int, int]
    source_candle_receipt_ids: tuple[int, int]


@dataclass(frozen=True, slots=True)
class CanonicalCandleDraft:
    """Untrusted claim plus IDs that must equal the DB-derived raw universe."""

    canonical_series_id: int
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    base_volume: Decimal | None
    trade_count: int | None
    algorithm_version: str
    policy_hash: str
    source_candle_ids: tuple[int, ...]
    context_candle_ids: tuple[int, ...]

    def __post_init__(self) -> None:
        if self.canonical_series_id <= 0:
            raise IngestValidationError("canonical_series_id must be positive")
        if not self.algorithm_version.strip():
            raise IngestValidationError("algorithm_version must not be empty")
        if not _is_sha256(self.policy_hash):
            raise IngestValidationError("policy_hash must be a lowercase SHA-256 digest")
        if not self.source_candle_ids:
            raise IngestValidationError("Canonical candle requires source candle provenance")
        if len(set(self.source_candle_ids)) != len(self.source_candle_ids):
            raise IngestValidationError("Canonical candle provenance contains duplicate ids")
        if any(candle_id <= 0 for candle_id in self.source_candle_ids):
            raise IngestValidationError("Source candle ids must be positive")
        if not self.context_candle_ids:
            raise IngestValidationError("Canonical candle requires consensus context")
        if len(set(self.context_candle_ids)) != len(self.context_candle_ids):
            raise IngestValidationError("Canonical context contains duplicate ids")
        if any(candle_id <= 0 for candle_id in self.context_candle_ids):
            raise IngestValidationError("Context candle ids must be positive")
        if not set(self.source_candle_ids).issubset(self.context_candle_ids):
            raise IngestValidationError(
                "Canonical observations must be included in consensus context"
            )
        _require_utc(self.open_time, "open_time")
        _require_utc(self.close_time, "close_time")
        if self.open_time >= self.close_time:
            raise IngestValidationError("Candle open_time must precede close_time")
        _validate_prices(
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.base_volume,
        )
        if self.base_volume is not None:
            raise IngestValidationError(
                "Canonical base_volume must be NULL; normalized volume is manifest evidence"
            )
        if self.trade_count is not None and self.trade_count < 0:
            raise IngestValidationError("trade_count must not be negative")

    @classmethod
    def from_numbers(
        cls,
        *,
        canonical_series_id: int,
        open_time: datetime,
        close_time: datetime,
        open_price: int | float | Decimal,
        high_price: int | float | Decimal,
        low_price: int | float | Decimal,
        close_price: int | float | Decimal,
        base_volume: int | float | Decimal | None,
        trade_count: int | None,
        algorithm_version: str,
        policy_hash: str,
        source_candle_ids: Sequence[int],
        context_candle_ids: Sequence[int],
    ) -> CanonicalCandleDraft:
        return cls(
            canonical_series_id=canonical_series_id,
            open_time=open_time,
            close_time=close_time,
            open_price=_decimal(open_price, "open_price"),
            high_price=_decimal(high_price, "high_price"),
            low_price=_decimal(low_price, "low_price"),
            close_price=_decimal(close_price, "close_price"),
            base_volume=(
                None if base_volume is None else _decimal(base_volume, "base_volume")
            ),
            trade_count=trade_count,
            algorithm_version=algorithm_version,
            policy_hash=policy_hash,
            source_candle_ids=tuple(source_candle_ids),
            context_candle_ids=tuple(context_candle_ids),
        )


@dataclass(frozen=True, slots=True)
class CanonicalCandleRecord:
    candle_id: int
    canonical_series_id: int
    risk_policy_id: int
    market_id: int
    interval_seconds: int
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    base_volume: Decimal | None
    normalized_volume: Decimal
    normalized_volume_unit: str
    trade_count: int | None
    revision_no: int
    observed_at: datetime
    available_at: datetime
    ingested_at: datetime
    content_hash: str
    algorithm_version: str
    policy_hash: str
    evidence_hash: str
    consensus_window_size: int
    source_candle_ids: tuple[int, ...]
    context_candle_ids: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _MarketBinding:
    source_key: str
    canonical_symbol: str
    venue_symbol: str
    exchange_id: int
    exchange_key: str


@dataclass(frozen=True, slots=True)
class _CanonicalSeries:
    canonical_series_id: int
    canonical_market_id: int
    canonical_source_id: int
    risk_policy_id: int
    canonical_symbol: str
    interval_seconds: int
    algorithm_version: str
    policy_hash: str
    exchange_id: int
    base_asset_id: int
    quote_asset_id: int
    instrument_type: str
    policy_effective_from: datetime
    policy_effective_to: datetime | None
    registry_available_at: datetime
    registry_ingested_at: datetime
    risk_policy: RiskPolicy
    policy_parameters: ConsensusPolicyParameters


@dataclass(frozen=True, slots=True)
class _ReferencePriceScope:
    canonical_market_id: int
    risk_policy_id: int
    canonical_symbol: str
    policy_hash: str
    exchange_id: int
    base_asset_id: int
    quote_asset_id: int
    instrument_type: str
    policy_effective_from: datetime
    policy_effective_to: datetime | None
    registry_available_at: datetime
    registry_ingested_at: datetime
    risk_policy: RiskPolicy


@dataclass(frozen=True, slots=True)
class _RawCandleRow:
    candle_id: int
    market_id: int
    interval_seconds: int
    open_time: datetime
    close_time: datetime
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    base_volume: Decimal | None
    trade_count: int | None
    source_id: int
    source_record_key: str
    source_version: str
    revision_no: int
    content_hash: str
    observed_at: datetime
    available_at: datetime
    ingested_at: datetime
    is_final: bool
    exchange_id: int
    exchange_key: str
    base_asset_id: int
    quote_asset_id: int
    instrument_type: str
    source_key: str
    canonical_symbol: str
    venue_symbol: str


@dataclass(frozen=True, slots=True)
class _ReferencePriceRawRow:
    candle: _RawCandleRow
    source_candle_receipt_id: int
    provider_ingested_at: datetime


@dataclass(frozen=True, slots=True)
class _ComputedCandle:
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    normalized_volume: Decimal
    normalized_volume_unit: str
    trade_count: int | None
    available_at: datetime
    diagnostics_document: Mapping[str, object]


class PointInTimeCandleRepository:
    """Transactional append-only raw storage and canonical consensus persistence."""

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        clock: Callable[[], datetime] | None = None,
        min_independent_sources: int = 2,
    ) -> None:
        if min_independent_sources != len(APPROVED_SOURCE_VENUES):
            raise ValueError("V1.1 requires exactly two approved independent sources")
        self._connection_factory = connection_factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._min_independent_sources = min_independent_sources

    def append_source_candles(
        self,
        *,
        market_id: int,
        source_id: int,
        source_key: str,
        source_version: str,
        candles: Sequence[Candle],
        as_of: datetime,
        batch_id: int | None = None,
    ) -> tuple[PersistedCandle, ...]:
        """Append provider revisions only after a source/venue/symbol authorization."""

        if market_id <= 0 or source_id <= 0:
            raise IngestValidationError("market_id and source_id must be positive")
        if not source_key.strip() or not source_version.strip():
            raise IngestValidationError("source key and version must not be empty")
        _require_utc(as_of, "as_of")
        captured_at = self._clock()
        _require_utc(captured_at, "clock")
        if as_of > captured_at:
            raise IngestValidationError("Market-data cutoff cannot be later than receipt")
        if not candles:
            return ()

        results: list[PersistedCandle] = []
        try:
            with transaction(self._connection_factory) as connection, cursor(
                connection
            ) as db_cursor:
                binding = _load_market_binding(
                    db_cursor,
                    source_id=source_id,
                    market_id=market_id,
                    as_of=as_of,
                )
                if binding is None:
                    raise IngestValidationError(
                        "Source is not authorized for the requested venue market"
                    )
                if binding.source_key != source_key:
                    raise IngestValidationError("source_id does not match source_key")

                for candle in candles:
                    _validate_source_candle(
                        candle,
                        binding=binding,
                        as_of=as_of,
                        captured_at=captured_at,
                    )
                    record_key = source_candle_record_key(candle, market_id=market_id)
                    content_hash = source_candle_content_hash(
                        candle,
                        market_id=market_id,
                        source_version=source_version,
                        record_key=record_key,
                    )
                    _advisory_lock(db_cursor, f"source:{source_id}:{record_key}")
                    revisions = _load_revisions(
                        db_cursor,
                        source_id=source_id,
                        source_record_key=record_key,
                    )
                    existing = _find_content_hash(revisions, content_hash)
                    if existing is None:
                        revision_no = 1 + max((row[1] for row in revisions), default=0)
                        db_cursor.execute(
                            """
                            INSERT INTO crypto_agent.candles (
                                market_id, interval_seconds, open_time, close_time,
                                open_price, high_price, low_price, close_price,
                                base_volume, quote_volume, trade_count, is_final,
                                source_id, source_record_key, source_version, revision_no,
                                observed_at, available_at, ingested_at, batch_id, content_hash
                            ) VALUES (
                                %s, %s, %s, %s,
                                %s, %s, %s, %s,
                                %s, NULL, NULL, true,
                                %s, %s, %s, %s,
                                %s, %s, %s, %s, %s
                            )
                            RETURNING candle_id
                            """,
                            (
                                market_id,
                                candle.interval_minutes * 60,
                                candle.open_time,
                                candle.close_time,
                                _decimal(candle.open, "open"),
                                _decimal(candle.high, "high"),
                                _decimal(candle.low, "low"),
                                _decimal(candle.close, "close"),
                                _decimal(candle.volume, "volume"),
                                source_id,
                                record_key,
                                source_version,
                                revision_no,
                                candle.close_time,
                                candle.available_at,
                                captured_at,
                                batch_id,
                                content_hash,
                            ),
                        )
                        inserted_row = db_cursor.fetchone()
                        if inserted_row is None:
                            raise PersistenceInvariantError("Candle insert returned no identity")
                        candle_id = int(_row_value(inserted_row, "candle_id", 0))
                        persisted = PersistedCandle(
                            candle_id=candle_id,
                            revision_no=revision_no,
                            content_hash=content_hash,
                            inserted=True,
                        )
                    else:
                        persisted = PersistedCandle(
                            candle_id=existing[0],
                            revision_no=existing[1],
                            content_hash=content_hash,
                            inserted=False,
                        )
                    _append_source_receipt(
                        db_cursor,
                        candle_id=persisted.candle_id,
                        provider_ingested_at=candle.ingested_at,
                        received_at=captured_at,
                        cutoff_as_of=as_of,
                    )
                    results.append(persisted)
        except (IngestError, PostgresError):
            raise
        except Exception:
            raise PostgresOperationError("PostgreSQL source candle ingest failed") from None
        return tuple(results)

    def append_reference_price(
        self,
        draft: ReferencePriceDraft,
        *,
        cutoff_as_of: datetime,
        evaluated_at: datetime,
    ) -> PersistedReferencePrice:
        """Derive and append one immutable two-venue reference-price manifest."""

        _require_utc(cutoff_as_of, "cutoff_as_of")
        _require_utc(evaluated_at, "evaluated_at")
        captured_at = self._clock()
        _require_utc(captured_at, "clock")
        if not cutoff_as_of <= evaluated_at <= captured_at:
            raise IngestValidationError(
                "Reference-price lineage must satisfy cutoff<=evaluated<=captured"
            )

        try:
            with transaction(self._connection_factory) as connection, cursor(
                connection
            ) as db_cursor:
                scope = _load_reference_price_scope(
                    db_cursor,
                    canonical_market_id=draft.canonical_market_id,
                    risk_policy_id=draft.risk_policy_id,
                    policy_hash=draft.policy_hash,
                    symbol=draft.symbol,
                    evaluated_at=evaluated_at,
                )
                if scope is None:
                    raise IngestValidationError(
                        "Reference-price market or current policy is unavailable"
                    )
                reference_key = _reference_price_record_key(
                    draft,
                    cutoff_as_of=cutoff_as_of,
                    evaluated_at=evaluated_at,
                )
                _advisory_lock(db_cursor, f"reference-price:{reference_key}")
                db_cursor.execute(
                    """
                    SELECT reference_price_id, canonical_market_id, risk_policy_id,
                           symbol, algorithm_version, policy_hash, event_time,
                           cutoff_as_of, evaluated_at, median_price,
                           pairwise_divergence_bps,
                           max_deviation_from_median_bps, inputs_hash, evidence_hash,
                           available_at, ingested_at, content_hash
                    FROM crypto_agent.reference_price_manifests
                    WHERE reference_key = %s
                    """,
                    (reference_key,),
                )
                existing = db_cursor.fetchone()
                replay_captured_at = captured_at
                if existing is not None:
                    replay_captured_at = _datetime_value(
                        existing,
                        "ingested_at",
                        15,
                    )
                    if not evaluated_at <= replay_captured_at <= captured_at:
                        raise PersistenceInvariantError(
                            "Existing reference-price capture time is invalid"
                        )
                raw_rows = _derive_reference_price_universe(
                    db_cursor,
                    scope=scope,
                    event_time=draft.event_time,
                    cutoff_as_of=cutoff_as_of,
                    evaluated_at=evaluated_at,
                    captured_at=replay_captured_at,
                )
                computed = _recompute_reference_price(
                    raw_rows,
                    scope=scope,
                    cutoff_as_of=cutoff_as_of,
                    evaluated_at=evaluated_at,
                )
                _verify_reference_price_draft(draft, scope, raw_rows, computed)

                source_provenance = tuple(
                    sorted(
                        (
                            item.candle.candle_id,
                            item.source_candle_receipt_id,
                        )
                        for item in raw_rows
                    )
                )
                inputs_hash = _reference_price_inputs_hash(
                    raw_rows,
                    scope=scope,
                )
                evidence_hash = _reference_price_evidence_hash(
                    scope=scope,
                    cutoff_as_of=cutoff_as_of,
                    evaluated_at=evaluated_at,
                    computed=computed,
                    inputs_hash=inputs_hash,
                )
                content_hash = _reference_price_content_hash(
                    reference_key=reference_key,
                    scope=scope,
                    computed=computed,
                    inputs_hash=inputs_hash,
                    evidence_hash=evidence_hash,
                )
                expected = _reference_manifest_values(
                    reference_key=reference_key,
                    scope=scope,
                    computed=computed,
                    cutoff_as_of=cutoff_as_of,
                    evaluated_at=evaluated_at,
                    inputs_hash=inputs_hash,
                    evidence_hash=evidence_hash,
                    captured_at=replay_captured_at,
                    content_hash=content_hash,
                )

                if existing is None:
                    db_cursor.execute(
                        """
                        INSERT INTO crypto_agent.reference_price_manifests (
                            reference_key, canonical_market_id, risk_policy_id,
                            symbol, algorithm_version, policy_hash, event_time,
                            cutoff_as_of, evaluated_at, median_price,
                            pairwise_divergence_bps,
                            max_deviation_from_median_bps, inputs_hash, evidence_hash,
                            available_at, ingested_at, content_hash
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s
                        )
                        RETURNING reference_price_id
                        """,
                        expected,
                    )
                    inserted_row = db_cursor.fetchone()
                    if inserted_row is None:
                        raise PersistenceInvariantError(
                            "Reference-price insert returned no identity"
                        )
                    reference_price_id = int(
                        _row_value(inserted_row, "reference_price_id", 0)
                    )
                    inserted = True
                else:
                    reference_price_id = _verify_existing_reference_manifest(
                        existing,
                        expected=expected,
                    )
                    inserted = False

                _ensure_reference_price_provenance(
                    db_cursor,
                    reference_price_id=reference_price_id,
                    source_provenance=source_provenance,
                    linked_at=replay_captured_at,
                )
                return PersistedReferencePrice(
                    reference_price_id=reference_price_id,
                    content_hash=content_hash,
                    inserted=inserted,
                )
        except (IngestError, PostgresError):
            raise
        except Exception:
            raise PostgresOperationError(
                "PostgreSQL reference-price ingest failed"
            ) from None

    def load_reference_price(
        self,
        *,
        reference_price_id: int,
        as_of: datetime,
    ) -> ReferencePriceRecord:
        """Load and independently replay one persisted reference-price manifest."""

        if reference_price_id <= 0:
            raise IngestValidationError("reference_price_id must be positive")
        _require_utc(as_of, "as_of")
        try:
            with transaction(self._connection_factory) as connection, cursor(
                connection
            ) as db_cursor:
                db_cursor.execute(
                    """
                    SELECT reference_price_id, reference_key, canonical_market_id,
                           risk_policy_id, symbol, algorithm_version, policy_hash,
                           event_time, cutoff_as_of, evaluated_at, median_price,
                           pairwise_divergence_bps,
                           max_deviation_from_median_bps, inputs_hash, evidence_hash,
                           available_at, ingested_at, content_hash
                    FROM crypto_agent.reference_price_manifests
                    WHERE reference_price_id = %s
                      AND available_at <= %s
                      AND ingested_at <= %s
                    """,
                    (reference_price_id, as_of, as_of),
                )
                manifest = db_cursor.fetchone()
                if manifest is None:
                    raise IngestValidationError(
                        "Reference-price manifest is unavailable at as_of"
                    )
                evaluated_at = _datetime_value(manifest, "evaluated_at", 9)
                scope = _load_reference_price_scope(
                    db_cursor,
                    canonical_market_id=int(
                        _row_value(manifest, "canonical_market_id", 2)
                    ),
                    risk_policy_id=int(_row_value(manifest, "risk_policy_id", 3)),
                    policy_hash=str(_row_value(manifest, "policy_hash", 6)),
                    symbol=str(_row_value(manifest, "symbol", 4)),
                    evaluated_at=evaluated_at,
                    allow_legacy_replay=False,
                )
                if scope is None:
                    raise PersistenceInvariantError(
                        "Reference-price policy scope is unavailable at evaluation time"
                    )
                captured_at = _datetime_value(manifest, "ingested_at", 16)
                cutoff_as_of = _datetime_value(manifest, "cutoff_as_of", 8)
                eligible_rows = _derive_reference_price_universe(
                    db_cursor,
                    scope=scope,
                    event_time=_datetime_value(manifest, "event_time", 7),
                    cutoff_as_of=cutoff_as_of,
                    evaluated_at=evaluated_at,
                    captured_at=captured_at,
                )
                raw_rows = _load_reference_price_provenance(
                    db_cursor,
                    reference_price_id=reference_price_id,
                    evaluated_at=evaluated_at,
                    captured_at=captured_at,
                )
                eligible_identity = tuple(
                    sorted(
                        (
                            item.candle.candle_id,
                            item.candle.content_hash,
                            item.source_candle_receipt_id,
                            item.provider_ingested_at,
                        )
                        for item in eligible_rows
                    )
                )
                linked_identity = tuple(
                    sorted(
                        (
                            item.candle.candle_id,
                            item.candle.content_hash,
                            item.source_candle_receipt_id,
                            item.provider_ingested_at,
                        )
                        for item in raw_rows
                    )
                )
                if linked_identity != eligible_identity:
                    raise PersistenceInvariantError(
                        "Reference-price provenance differs from its DB-derived universe"
                    )
        except (IngestError, PostgresError):
            raise
        except Exception:
            raise PostgresOperationError(
                "PostgreSQL reference-price query failed"
            ) from None
        return _verify_loaded_reference_price_manifest(
            manifest,
            scope=scope,
            raw_rows=raw_rows,
        )

    def append_canonical_candle(
        self,
        draft: CanonicalCandleDraft,
        *,
        as_of: datetime,
        batch_id: int | None = None,
    ) -> PersistedCandle:
        """Replay the shared consensus algorithm over raw context and append it."""

        _require_utc(as_of, "as_of")
        captured_at = self._clock()
        _require_utc(captured_at, "clock")
        if as_of > captured_at:
            raise IngestValidationError("Market-data cutoff cannot be later than receipt")

        try:
            with transaction(self._connection_factory) as connection, cursor(
                connection
            ) as db_cursor:
                series = _load_canonical_series(
                    db_cursor,
                    canonical_series_id=draft.canonical_series_id,
                    as_of=as_of,
                    require_policy_active_at=as_of,
                )
                if series is None:
                    raise IngestValidationError("Canonical series is unavailable at as_of")
                if series.risk_policy.policy_schema_version != 2:
                    raise IngestValidationError(
                        "Archived policy revisions are replay-only and cannot create "
                        "canonical candles"
                    )
                raw_rows = _derive_raw_candle_universe(
                    db_cursor,
                    series=series,
                    draft=draft,
                    as_of=as_of,
                    captured_at=captured_at,
                )
                _validate_canonical_inputs(
                    draft,
                    series,
                    raw_rows,
                    as_of=as_of,
                    captured_at=captured_at,
                )
                computed = _recompute_canonical(draft, series, raw_rows)
                _verify_untrusted_draft(draft, computed)
                observation_candle_ids = tuple(
                    sorted(
                        item.candle_id
                        for item in raw_rows
                        if item.open_time == draft.open_time
                        and item.close_time == draft.close_time
                    )
                )
                context_candle_ids = tuple(sorted(item.candle_id for item in raw_rows))

                record_key = canonical_candle_record_key(draft)
                inputs_hash = _canonical_inputs_hash(
                    raw_rows,
                    series=series,
                    observation_candle_ids=observation_candle_ids,
                )
                payload_hash = _computed_payload_hash(draft, series, computed)
                evidence_hash = _canonical_evidence_hash(
                    series=series,
                    cutoff_as_of=as_of,
                    inputs_hash=inputs_hash,
                    computed_payload_hash=payload_hash,
                    diagnostics_document=computed.diagnostics_document,
                )
                content_hash = _canonical_content_hash(
                    draft,
                    series,
                    inputs_hash=inputs_hash,
                    computed_payload_hash=payload_hash,
                    evidence_hash=evidence_hash,
                )
                available_at = computed.available_at
                source_version = (
                    f"{series.algorithm_version}:{series.policy_hash[:16]}"
                )

                _advisory_lock(
                    db_cursor,
                    f"canonical:{draft.canonical_series_id}:{record_key}",
                )
                revisions = _load_revisions(
                    db_cursor,
                    source_id=series.canonical_source_id,
                    source_record_key=record_key,
                )
                existing = _find_content_hash(revisions, content_hash)
                if existing is None:
                    revision_no = 1 + max((row[1] for row in revisions), default=0)
                    db_cursor.execute(
                        """
                        INSERT INTO crypto_agent.candles (
                            market_id, interval_seconds, open_time, close_time,
                            open_price, high_price, low_price, close_price,
                            base_volume, quote_volume, trade_count, is_final,
                            source_id, source_record_key, source_version, revision_no,
                            observed_at, available_at, ingested_at, batch_id, content_hash
                        ) VALUES (
                            %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            NULL, NULL, %s, true,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s
                        )
                        RETURNING candle_id
                        """,
                        (
                            series.canonical_market_id,
                            series.interval_seconds,
                            draft.open_time,
                            draft.close_time,
                            computed.open_price,
                            computed.high_price,
                            computed.low_price,
                            computed.close_price,
                            computed.trade_count,
                            series.canonical_source_id,
                            record_key,
                            source_version,
                            revision_no,
                            draft.close_time,
                            available_at,
                            captured_at,
                            batch_id,
                            content_hash,
                        ),
                    )
                    inserted_row = db_cursor.fetchone()
                    if inserted_row is None:
                        raise PersistenceInvariantError(
                            "Canonical candle insert returned no identity"
                        )
                    candle_id = int(_row_value(inserted_row, "candle_id", 0))
                    inserted = True
                else:
                    candle_id, revision_no, _ = existing
                    inserted = False

                _ensure_manifest(
                    db_cursor,
                    candle_id=candle_id,
                    series=series,
                    inputs_hash=inputs_hash,
                    computed_payload_hash=payload_hash,
                    cutoff_as_of=as_of,
                    computed=computed,
                    evidence_hash=evidence_hash,
                )
                _ensure_provenance(
                    db_cursor,
                    canonical_candle_id=candle_id,
                    observation_candle_ids=observation_candle_ids,
                    context_candle_ids=context_candle_ids,
                )
                return PersistedCandle(
                    candle_id=candle_id,
                    revision_no=revision_no,
                    content_hash=content_hash,
                    inserted=inserted,
                )
        except (IngestError, PostgresError):
            raise
        except Exception:
            raise PostgresOperationError("PostgreSQL canonical candle ingest failed") from None

    def load_canonical_candles(
        self,
        *,
        canonical_series_id: int,
        as_of: datetime,
        limit: int,
    ) -> tuple[CanonicalCandleRecord, ...]:
        """Load latest eligible revisions with availability and ingest cutoffs."""

        if canonical_series_id <= 0:
            raise IngestValidationError("canonical_series_id must be positive")
        if not 1 <= limit <= 10_000:
            raise IngestValidationError("Candle query limit must be between 1 and 10000")
        _require_utc(as_of, "as_of")
        try:
            with transaction(self._connection_factory) as connection, cursor(
                connection
            ) as db_cursor:
                series = _load_canonical_series(
                    db_cursor,
                    canonical_series_id=canonical_series_id,
                    as_of=as_of,
                )
                if series is None:
                    raise IngestValidationError(
                        "Canonical series or its validated policy is unavailable at as_of"
                    )
                db_cursor.execute(
                    """
                    WITH eligible AS (
                        SELECT DISTINCT ON (c.source_record_key)
                            c.candle_id, c.market_id, c.interval_seconds,
                            c.open_time, c.close_time,
                            c.open_price, c.high_price, c.low_price, c.close_price,
                            c.base_volume, c.trade_count, c.revision_no,
                            c.observed_at, c.available_at, c.ingested_at, c.content_hash,
                            manifest.canonical_series_id, manifest.risk_policy_id,
                            manifest.algorithm_version, manifest.policy_hash,
                            manifest.normalized_volume,
                            manifest.normalized_volume_unit,
                            manifest.evidence_hash,
                            manifest.consensus_window_size,
                            manifest.cutoff_as_of,
                            manifest.inputs_hash,
                            manifest.computed_payload_hash,
                            manifest.diagnostics_document,
                            c.source_record_key
                        FROM crypto_agent.candles c
                        JOIN crypto_agent.canonical_candle_manifests manifest
                          ON manifest.canonical_candle_id = c.candle_id
                        WHERE manifest.canonical_series_id = %s
                          AND c.is_final
                          AND c.available_at <= %s
                          AND c.ingested_at <= %s
                          AND manifest.created_at <= %s
                          AND (
                              SELECT COUNT(DISTINCT source.source_id) >= 2
                                 AND COUNT(DISTINCT market.exchange_id) >= 2
                              FROM crypto_agent.canonical_candle_provenance p
                              JOIN crypto_agent.candles source
                                ON source.candle_id = p.source_candle_id
                              JOIN crypto_agent.markets market
                                ON market.market_id = source.market_id
                              WHERE p.canonical_candle_id = c.candle_id
                                AND p.input_role = 'observation'
                                AND p.linked_at <= %s
                          )
                        ORDER BY c.source_record_key, c.revision_no DESC,
                                 c.available_at DESC, c.ingested_at DESC
                    )
                    SELECT * FROM eligible
                    ORDER BY open_time DESC
                    LIMIT %s
                    """,
                    (canonical_series_id, as_of, as_of, as_of, as_of, limit),
                )
                rows = db_cursor.fetchall()
                candle_ids = tuple(
                    int(_row_value(row, "candle_id", 0)) for row in rows
                )
                observations: dict[int, tuple[int, ...]] = {}
                contexts: dict[int, tuple[int, ...]] = {}
                raw_contexts: dict[int, tuple[_RawCandleRow, ...]] = {}
                if candle_ids:
                    db_cursor.execute(
                        """
                        SELECT source.candle_id, source.market_id,
                               source.interval_seconds,
                               source.open_time, source.close_time,
                               source.open_price, source.high_price,
                               source.low_price, source.close_price,
                               source.base_volume, source.trade_count,
                               source.source_id, source.source_record_key,
                               source.source_version, source.revision_no,
                               source.content_hash, source.observed_at,
                               source.available_at, source.ingested_at,
                               source.is_final, market.exchange_id,
                               exchange.exchange_key, market.base_asset_id,
                               market.quote_asset_id, market.instrument_type,
                               data_source.source_key, binding.canonical_symbol,
                               binding.venue_symbol,
                               provenance.canonical_candle_id,
                               provenance.input_role
                        FROM crypto_agent.canonical_candle_provenance provenance
                        JOIN crypto_agent.candles source
                          ON source.candle_id = provenance.source_candle_id
                        JOIN crypto_agent.markets market
                          ON market.market_id = source.market_id
                        JOIN crypto_agent.exchanges exchange
                          ON exchange.exchange_id = market.exchange_id
                        JOIN crypto_agent.data_sources data_source
                          ON data_source.source_id = source.source_id
                        JOIN crypto_agent.market_data_source_bindings binding
                          ON binding.source_id = source.source_id
                         AND binding.market_id = source.market_id
                        WHERE provenance.canonical_candle_id = ANY(%s::bigint[])
                          AND provenance.linked_at <= %s
                        ORDER BY provenance.canonical_candle_id,
                                 data_source.source_key, source.open_time,
                                 source.candle_id
                        """,
                        (list(candle_ids), as_of),
                    )
                    observation_members: dict[int, list[int]] = {}
                    context_members: dict[int, list[int]] = {}
                    raw_context_members: dict[int, list[_RawCandleRow]] = {}
                    for row in db_cursor.fetchall():
                        canonical_id = int(
                            _row_value(row, "canonical_candle_id", 28)
                        )
                        raw_candle = _raw_row(row)
                        source_id = raw_candle.candle_id
                        role = str(_row_value(row, "input_role", 29))
                        if role == "observation":
                            target = observation_members
                        elif role == "context":
                            target = context_members
                        else:
                            raise PersistenceInvariantError(
                                "Canonical candle has an invalid provenance role"
                            )
                        target.setdefault(canonical_id, []).append(source_id)
                        raw_context_members.setdefault(canonical_id, []).append(
                            raw_candle
                        )
                    observations = {
                        key: tuple(value) for key, value in observation_members.items()
                    }
                    contexts = {
                        key: tuple(value) for key, value in context_members.items()
                    }
                    raw_contexts = {
                        key: tuple(value)
                        for key, value in raw_context_members.items()
                    }
        except (IngestError, PostgresError):
            raise
        except Exception:
            raise PostgresOperationError("PostgreSQL canonical candle query failed") from None

        records = tuple(
            _verify_loaded_canonical_manifest(
                row,
                series=series,
                raw_rows=raw_contexts.get(
                    int(_row_value(row, "candle_id", 0)),
                    (),
                ),
                observation_candle_ids=observations.get(
                    int(_row_value(row, "candle_id", 0)),
                    (),
                ),
                context_candle_ids=contexts.get(
                    int(_row_value(row, "candle_id", 0)),
                    (),
                ),
            )
            for row in reversed(rows)
        )
        if any(
            record.canonical_series_id != series.canonical_series_id
            or record.risk_policy_id != series.risk_policy_id
            or record.market_id != series.canonical_market_id
            or record.interval_seconds != series.interval_seconds
            or record.algorithm_version != series.algorithm_version
            or record.policy_hash != series.policy_hash
            or len(record.source_candle_ids) != self._min_independent_sources
            or len(record.context_candle_ids)
            != self._min_independent_sources * CONSENSUS_WINDOW_SIZE
            or not set(record.source_candle_ids).issubset(record.context_candle_ids)
            for record in records
        ):
            raise PersistenceInvariantError(
                "Canonical candle lacks the required persisted provenance"
            )
        return records


def source_candle_record_key(candle: Candle, *, market_id: int) -> str:
    if market_id <= 0:
        raise IngestValidationError("market_id must be positive")
    symbol = candle.symbol.strip().upper().replace("/", "-")
    return (
        f"ohlc:{market_id}:{symbol}:{candle.interval_minutes * 60}:"
        f"{_iso(candle.open_time)}"
    )


def canonical_candle_record_key(draft: CanonicalCandleDraft) -> str:
    return (
        f"canonical:{draft.canonical_series_id}:"
        f"{_iso(draft.open_time)}"
    )


def source_candle_content_hash(
    candle: Candle,
    *,
    market_id: int,
    source_version: str,
    record_key: str | None = None,
) -> str:
    return _source_candle_fields_content_hash(
        record_key=record_key or source_candle_record_key(candle, market_id=market_id),
        market_id=market_id,
        source_version=source_version,
        symbol=candle.symbol,
        interval_seconds=candle.interval_minutes * 60,
        open_time=candle.open_time,
        close_time=candle.close_time,
        open_price=candle.open,
        high_price=candle.high,
        low_price=candle.low,
        close_price=candle.close,
        volume=candle.volume,
    )


def _source_candle_fields_content_hash(
    *,
    record_key: str,
    market_id: int,
    source_version: str,
    symbol: str,
    interval_seconds: int,
    open_time: datetime,
    close_time: datetime,
    open_price: object,
    high_price: object,
    low_price: object,
    close_price: object,
    volume: object,
) -> str:
    if not source_version.strip():
        raise IngestValidationError("source_version must not be empty")
    payload = {
        "record_key": record_key,
        "market_id": market_id,
        "source_version": source_version,
        "symbol": symbol,
        "interval_seconds": interval_seconds,
        "open_time": _iso(open_time),
        "close_time": _iso(close_time),
        "open": _decimal_text(_decimal(open_price, "open")),
        "high": _decimal_text(_decimal(high_price, "high")),
        "low": _decimal_text(_decimal(low_price, "low")),
        "close": _decimal_text(_decimal(close_price, "close")),
        "volume": _decimal_text(_decimal(volume, "volume")),
        "is_final": True,
    }
    return _content_hash(payload)


def _raw_source_candle_content_hash(candle: _RawCandleRow) -> str:
    if candle.interval_seconds % 60 != 0 or candle.base_volume is None:
        raise PersistenceInvariantError(
            "Raw source candle cannot be reconstructed for content-hash verification"
        )
    return _source_candle_fields_content_hash(
        record_key=candle.source_record_key,
        market_id=candle.market_id,
        source_version=candle.source_version,
        symbol=candle.canonical_symbol,
        interval_seconds=candle.interval_seconds,
        open_time=candle.open_time,
        close_time=candle.close_time,
        open_price=candle.open_price,
        high_price=candle.high_price,
        low_price=candle.low_price,
        close_price=candle.close_price,
        volume=candle.base_volume,
    )


def _load_market_binding(
    db_cursor: DBCursor,
    *,
    source_id: int,
    market_id: int,
    as_of: datetime,
) -> _MarketBinding | None:
    db_cursor.execute(
        """
        SELECT source.source_key, binding.canonical_symbol, binding.venue_symbol,
               market.exchange_id, exchange.exchange_key
        FROM crypto_agent.market_data_source_bindings binding
        JOIN crypto_agent.data_sources source ON source.source_id = binding.source_id
        JOIN crypto_agent.markets market ON market.market_id = binding.market_id
        JOIN crypto_agent.exchanges exchange ON exchange.exchange_id = market.exchange_id
        WHERE binding.source_id = %s
          AND binding.market_id = %s
          AND binding.available_at <= %s AND binding.ingested_at <= %s
          AND source.available_at <= %s AND source.ingested_at <= %s
          AND market.available_at <= %s AND market.ingested_at <= %s
          AND exchange.available_at <= %s AND exchange.ingested_at <= %s
          AND EXISTS (
              SELECT 1 FROM crypto_agent.market_symbols symbol
              WHERE symbol.market_id = market.market_id
                AND symbol.symbol = binding.venue_symbol
                AND symbol.available_at <= %s AND symbol.ingested_at <= %s
          )
        """,
        (source_id, market_id, *(as_of for _ in range(10))),
    )
    row = db_cursor.fetchone()
    if row is None:
        return None
    return _MarketBinding(
        source_key=str(_row_value(row, "source_key", 0)),
        canonical_symbol=str(_row_value(row, "canonical_symbol", 1)),
        venue_symbol=str(_row_value(row, "venue_symbol", 2)),
        exchange_id=int(_row_value(row, "exchange_id", 3)),
        exchange_key=str(_row_value(row, "exchange_key", 4)),
    )


def _load_canonical_series(
    db_cursor: DBCursor,
    *,
    canonical_series_id: int,
    as_of: datetime,
    require_policy_active_at: datetime | None = None,
) -> _CanonicalSeries | None:
    effective_clause = ""
    parameters: tuple[object, ...] = (
        canonical_series_id,
        *(as_of for _ in range(10)),
    )
    if require_policy_active_at is not None:
        _require_utc(require_policy_active_at, "require_policy_active_at")
        effective_clause = """
          AND policy.effective_from <= %s
          AND (policy.effective_to IS NULL OR policy.effective_to > %s)
        """
        parameters = (
            *parameters,
            require_policy_active_at,
            require_policy_active_at,
        )
    db_cursor.execute(
        f"""
        SELECT series.canonical_series_id, series.canonical_market_id,
               series.canonical_source_id, series.risk_policy_id,
               series.canonical_symbol,
               series.interval_seconds, series.algorithm_version, series.policy_hash,
               market.exchange_id, market.base_asset_id, market.quote_asset_id,
               market.instrument_type,
               policy.policy_document, policy.effective_from, policy.effective_to,
               GREATEST(
                   series.available_at, market.available_at, exchange.available_at,
                   source.available_at, policy.available_at
               ) AS registry_available_at,
               GREATEST(
                   series.ingested_at, market.ingested_at, exchange.ingested_at,
                   source.ingested_at, policy.ingested_at
               ) AS registry_ingested_at
        FROM crypto_agent.canonical_candle_series series
        JOIN crypto_agent.markets market
          ON market.market_id = series.canonical_market_id
        JOIN crypto_agent.exchanges exchange ON exchange.exchange_id = market.exchange_id
        JOIN crypto_agent.data_sources source
          ON source.source_id = series.canonical_source_id
        JOIN crypto_agent.risk_policies policy
          ON policy.risk_policy_id = series.risk_policy_id
         AND policy.content_hash = series.policy_hash
        WHERE series.canonical_series_id = %s
          AND series.available_at <= %s AND series.ingested_at <= %s
          AND market.available_at <= %s AND market.ingested_at <= %s
          AND exchange.available_at <= %s AND exchange.ingested_at <= %s
          AND source.available_at <= %s AND source.ingested_at <= %s
          AND policy.available_at <= %s AND policy.ingested_at <= %s
          {effective_clause}
        """,
        parameters,
    )
    row = db_cursor.fetchone()
    if row is None:
        return None
    policy = _risk_policy_from_document(
        _row_value(row, "policy_document", 12),
    )
    policy_hash = str(_row_value(row, "policy_hash", 7))
    if policy.fingerprint() != policy_hash:
        raise IngestValidationError(
            "Canonical series policy hash does not match the validated policy document"
        )
    return _CanonicalSeries(
        canonical_series_id=int(_row_value(row, "canonical_series_id", 0)),
        canonical_market_id=int(_row_value(row, "canonical_market_id", 1)),
        canonical_source_id=int(_row_value(row, "canonical_source_id", 2)),
        risk_policy_id=int(_row_value(row, "risk_policy_id", 3)),
        canonical_symbol=str(_row_value(row, "canonical_symbol", 4)),
        interval_seconds=int(_row_value(row, "interval_seconds", 5)),
        algorithm_version=str(_row_value(row, "algorithm_version", 6)),
        policy_hash=policy_hash,
        exchange_id=int(_row_value(row, "exchange_id", 8)),
        base_asset_id=int(_row_value(row, "base_asset_id", 9)),
        quote_asset_id=int(_row_value(row, "quote_asset_id", 10)),
        instrument_type=str(_row_value(row, "instrument_type", 11)),
        policy_effective_from=_datetime_value(
            row, "effective_from", 13
        ),
        policy_effective_to=(
            None
            if _row_value(row, "effective_to", 14) is None
            else _datetime_value(row, "effective_to", 14)
        ),
        registry_available_at=_datetime_value(row, "registry_available_at", 15),
        registry_ingested_at=_datetime_value(row, "registry_ingested_at", 16),
        risk_policy=policy,
        policy_parameters=ConsensusPolicyParameters(
            min_overlap=CONSENSUS_WINDOW_SIZE,
            max_close_divergence_bps=(
                policy.max_pairwise_reference_price_divergence_bps
            ),
            max_ohlc_divergence_bps=policy.max_cross_source_ohlc_divergence_bps,
            max_volume_zscore_delta=policy.max_cross_source_volume_zscore_delta,
            max_divergent_fraction=policy.max_divergent_candle_fraction,
            max_market_price=policy.max_market_price,
            max_base_volume=policy.max_base_volume,
        ),
    )


def _load_reference_price_scope(
    db_cursor: DBCursor,
    *,
    canonical_market_id: int,
    risk_policy_id: int,
    policy_hash: str,
    symbol: str,
    evaluated_at: datetime,
    allow_legacy_replay: bool = False,
) -> _ReferencePriceScope | None:
    db_cursor.execute(
        """
        SELECT market.market_id, policy.risk_policy_id,
               base_version.canonical_symbol || '/' || quote_version.canonical_symbol
                   AS canonical_symbol,
               policy.content_hash, market.exchange_id,
               market.base_asset_id, market.quote_asset_id, market.instrument_type,
               policy.policy_document, policy.effective_from, policy.effective_to,
               GREATEST(
                   market.available_at, exchange.available_at, policy.available_at,
                   base_version.available_at, quote_version.available_at
               ) AS registry_available_at,
               GREATEST(
                   market.ingested_at, exchange.ingested_at, policy.ingested_at,
                   base_version.ingested_at, quote_version.ingested_at
               ) AS registry_ingested_at
        FROM crypto_agent.markets market
        JOIN crypto_agent.exchanges exchange
          ON exchange.exchange_id = market.exchange_id
        JOIN crypto_agent.risk_policies policy
          ON policy.risk_policy_id = %s
         AND policy.content_hash = %s
        JOIN LATERAL (
            SELECT canonical_symbol, available_at, ingested_at
            FROM crypto_agent.asset_versions
            WHERE asset_id = market.base_asset_id
              AND available_at <= %s AND ingested_at <= %s
            ORDER BY available_at DESC, ingested_at DESC, revision_no DESC
            LIMIT 1
        ) base_version ON true
        JOIN LATERAL (
            SELECT canonical_symbol, available_at, ingested_at
            FROM crypto_agent.asset_versions
            WHERE asset_id = market.quote_asset_id
              AND available_at <= %s AND ingested_at <= %s
            ORDER BY available_at DESC, ingested_at DESC, revision_no DESC
            LIMIT 1
        ) quote_version ON true
        WHERE market.market_id = %s
          AND market.available_at <= %s AND market.ingested_at <= %s
          AND exchange.available_at <= %s AND exchange.ingested_at <= %s
          AND policy.available_at <= %s AND policy.ingested_at <= %s
          AND policy.effective_from <= %s
          AND (policy.effective_to IS NULL OR policy.effective_to > %s)
        """,
        (
            risk_policy_id,
            policy_hash,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            canonical_market_id,
            *(evaluated_at for _ in range(8)),
        ),
    )
    row = db_cursor.fetchone()
    if row is None:
        return None
    policy = _risk_policy_from_document(_row_value(row, "policy_document", 8))
    persisted_hash = str(_row_value(row, "content_hash", 3))
    if policy.fingerprint() != persisted_hash or persisted_hash != policy_hash:
        raise IngestValidationError(
            "Reference-price policy hash does not match its immutable document"
        )
    if policy.policy_schema_version != 2 and not allow_legacy_replay:
        raise IngestValidationError(
            "Archived policy revisions are replay-only and cannot create reference prices"
        )
    canonical_symbol = str(_row_value(row, "canonical_symbol", 2))
    if canonical_symbol != symbol or canonical_symbol not in policy.allowed_assets:
        raise IngestValidationError(
            "Reference-price symbol is outside the validated market policy scope"
        )
    return _ReferencePriceScope(
        canonical_market_id=int(_row_value(row, "market_id", 0)),
        risk_policy_id=int(_row_value(row, "risk_policy_id", 1)),
        canonical_symbol=canonical_symbol,
        policy_hash=persisted_hash,
        exchange_id=int(_row_value(row, "exchange_id", 4)),
        base_asset_id=int(_row_value(row, "base_asset_id", 5)),
        quote_asset_id=int(_row_value(row, "quote_asset_id", 6)),
        instrument_type=str(_row_value(row, "instrument_type", 7)),
        policy_effective_from=_datetime_value(row, "effective_from", 9),
        policy_effective_to=(
            None
            if _row_value(row, "effective_to", 10) is None
            else _datetime_value(row, "effective_to", 10)
        ),
        registry_available_at=_datetime_value(row, "registry_available_at", 11),
        registry_ingested_at=_datetime_value(row, "registry_ingested_at", 12),
        risk_policy=policy,
    )


def _derive_reference_price_universe(
    db_cursor: DBCursor,
    *,
    scope: _ReferencePriceScope,
    event_time: datetime,
    cutoff_as_of: datetime,
    evaluated_at: datetime,
    captured_at: datetime,
) -> tuple[_ReferencePriceRawRow, ...]:
    open_time = event_time - timedelta(seconds=60)
    approved_pairs = tuple(APPROVED_SOURCE_VENUES.items())
    db_cursor.execute(
        """
        WITH eligible_revisions AS (
            SELECT DISTINCT ON (candle.source_id, candle.source_record_key)
                   candle.candle_id, candle.market_id, candle.interval_seconds,
                   candle.open_time, candle.close_time,
                   candle.open_price, candle.high_price, candle.low_price,
                   candle.close_price, candle.base_volume, candle.trade_count,
                   candle.source_id, candle.source_record_key, candle.source_version,
                   candle.revision_no, candle.content_hash, candle.observed_at,
                   candle.available_at, candle.ingested_at, candle.is_final,
                   market.exchange_id, exchange.exchange_key,
                   market.base_asset_id, market.quote_asset_id,
                   market.instrument_type, source.source_key,
                   binding.canonical_symbol, binding.venue_symbol,
                   receipt.source_candle_receipt_id,
                   receipt.provider_ingested_at
            FROM crypto_agent.candles candle
            JOIN crypto_agent.markets market ON market.market_id = candle.market_id
            JOIN crypto_agent.exchanges exchange
              ON exchange.exchange_id = market.exchange_id
            JOIN crypto_agent.data_sources source
              ON source.source_id = candle.source_id
            JOIN crypto_agent.market_data_source_bindings binding
              ON binding.source_id = candle.source_id
             AND binding.market_id = candle.market_id
            JOIN LATERAL (
                SELECT source_receipt.source_candle_receipt_id,
                       source_receipt.provider_ingested_at
                FROM crypto_agent.source_candle_receipts source_receipt
                WHERE source_receipt.candle_id = candle.candle_id
                  AND source_receipt.provider_ingested_at <= %s
                  AND source_receipt.cutoff_as_of <= %s
                  AND source_receipt.received_at <= %s
                ORDER BY source_receipt.provider_ingested_at DESC,
                         source_receipt.received_at DESC,
                         source_receipt.source_candle_receipt_id DESC
                LIMIT 1
            ) receipt ON true
            WHERE candle.interval_seconds = 60
              AND candle.open_time = %s
              AND candle.close_time = %s
              AND candle.is_final
              AND market.base_asset_id = %s
              AND market.quote_asset_id = %s
              AND market.instrument_type = %s
              AND binding.canonical_symbol = %s
              AND (
                  (source.source_key = %s AND exchange.exchange_key = %s)
                  OR (source.source_key = %s AND exchange.exchange_key = %s)
              )
              AND candle.available_at <= %s AND candle.ingested_at <= %s
              AND binding.available_at <= %s AND binding.ingested_at <= %s
              AND market.available_at <= %s AND market.ingested_at <= %s
              AND exchange.available_at <= %s AND exchange.ingested_at <= %s
              AND source.available_at <= %s AND source.ingested_at <= %s
              AND EXISTS (
                  SELECT 1 FROM crypto_agent.market_symbols symbol
                  WHERE symbol.market_id = market.market_id
                    AND symbol.symbol = binding.venue_symbol
                    AND symbol.available_at <= %s AND symbol.ingested_at <= %s
              )
            ORDER BY candle.source_id, candle.source_record_key,
                     candle.revision_no DESC, candle.available_at DESC,
                     candle.ingested_at DESC, candle.candle_id DESC
        )
        SELECT * FROM eligible_revisions
        ORDER BY source_key, candle_id
        """,
        (
            evaluated_at,
            evaluated_at,
            captured_at,
            open_time,
            event_time,
            scope.base_asset_id,
            scope.quote_asset_id,
            scope.instrument_type,
            scope.canonical_symbol,
            approved_pairs[0][0],
            approved_pairs[0][1],
            approved_pairs[1][0],
            approved_pairs[1][1],
            evaluated_at,
            captured_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
            evaluated_at,
        ),
    )
    rows = tuple(_reference_price_raw_row(row) for row in db_cursor.fetchall())
    if event_time > cutoff_as_of:
        raise IngestValidationError(
            "Reference-price event is later than the selection cutoff"
        )
    return rows


def _load_reference_price_provenance(
    db_cursor: DBCursor,
    *,
    reference_price_id: int,
    evaluated_at: datetime,
    captured_at: datetime,
) -> tuple[_ReferencePriceRawRow, ...]:
    db_cursor.execute(
        """
        SELECT candle.candle_id, candle.market_id, candle.interval_seconds,
               candle.open_time, candle.close_time,
               candle.open_price, candle.high_price, candle.low_price,
               candle.close_price, candle.base_volume, candle.trade_count,
               candle.source_id, candle.source_record_key, candle.source_version,
               candle.revision_no, candle.content_hash, candle.observed_at,
               candle.available_at, candle.ingested_at, candle.is_final,
               market.exchange_id, exchange.exchange_key,
               market.base_asset_id, market.quote_asset_id,
               market.instrument_type, source.source_key,
               binding.canonical_symbol, binding.venue_symbol,
               receipt.source_candle_receipt_id,
               receipt.provider_ingested_at
        FROM crypto_agent.reference_price_provenance provenance
        JOIN crypto_agent.candles candle
          ON candle.candle_id = provenance.source_candle_id
        JOIN crypto_agent.markets market ON market.market_id = candle.market_id
        JOIN crypto_agent.exchanges exchange
          ON exchange.exchange_id = market.exchange_id
        JOIN crypto_agent.data_sources source ON source.source_id = candle.source_id
        JOIN crypto_agent.market_data_source_bindings binding
          ON binding.source_id = candle.source_id
         AND binding.market_id = candle.market_id
        JOIN crypto_agent.source_candle_receipts receipt
          ON receipt.source_candle_receipt_id
             = provenance.source_candle_receipt_id
         AND receipt.candle_id = candle.candle_id
         AND receipt.provider_ingested_at <= %s
         AND receipt.cutoff_as_of <= %s
         AND receipt.received_at <= %s
        WHERE provenance.reference_price_id = %s
          AND provenance.linked_at <= %s
        ORDER BY source.source_key, candle.candle_id
        """,
        (
            evaluated_at,
            evaluated_at,
            captured_at,
            reference_price_id,
            captured_at,
        ),
    )
    return tuple(_reference_price_raw_row(row) for row in db_cursor.fetchall())


def _reference_price_raw_row(row: object) -> _ReferencePriceRawRow:
    receipt_id = int(_row_value(row, "source_candle_receipt_id", 28))
    if receipt_id <= 0:
        raise PersistenceInvariantError(
            "Reference-price source receipt id must be positive"
        )
    return _ReferencePriceRawRow(
        candle=_raw_row(row),
        source_candle_receipt_id=receipt_id,
        provider_ingested_at=_datetime_value(row, "provider_ingested_at", 29),
    )


def _recompute_reference_price(
    raw_rows: Sequence[_ReferencePriceRawRow],
    *,
    scope: _ReferencePriceScope,
    cutoff_as_of: datetime,
    evaluated_at: datetime,
) -> ReferencePriceResult:
    for item in raw_rows:
        recomputed_hash = _raw_source_candle_content_hash(item.candle)
        if not _is_sha256(item.candle.content_hash) or not hmac.compare_digest(
            item.candle.content_hash,
            recomputed_hash,
        ):
            raise PersistenceInvariantError(
                "Reference-price source content hash does not match raw OHLCV"
            )
        if (
            item.candle.base_asset_id != scope.base_asset_id
            or item.candle.quote_asset_id != scope.quote_asset_id
            or item.candle.instrument_type != scope.instrument_type
            or item.candle.canonical_symbol != scope.canonical_symbol
            or item.candle.exchange_id == scope.exchange_id
        ):
            raise IngestValidationError(
                "Reference-price source is outside the canonical market scope"
            )
    try:
        return build_reference_price_snapshot(
            tuple(
                ReferencePriceInput(
                    source_id=item.candle.source_key,
                    venue_id=item.candle.exchange_key,
                    symbol=item.candle.canonical_symbol,
                    open_time=item.candle.open_time,
                    close_time=item.candle.close_time,
                    price=item.candle.close_price,
                    available_at=item.candle.available_at,
                    ingested_at=item.provider_ingested_at,
                )
                for item in raw_rows
            ),
            cutoff_as_of=cutoff_as_of,
            evaluated_at=evaluated_at,
            max_age_seconds=scope.risk_policy.max_reference_price_age_seconds,
            max_deviation_from_median_bps=(
                scope.risk_policy.max_reference_price_deviation_from_median_bps
            ),
            max_market_price=scope.risk_policy.max_market_price,
            approved_source_venues=APPROVED_SOURCE_VENUES,
        )
    except ReferencePriceMathError as exc:
        raise IngestValidationError(f"{exc.code}: {exc}") from None


def _verify_reference_price_draft(
    draft: ReferencePriceDraft,
    scope: _ReferencePriceScope,
    raw_rows: Sequence[_ReferencePriceRawRow],
    computed: ReferencePriceResult,
) -> None:
    derived_ids = tuple(sorted(item.candle.candle_id for item in raw_rows))
    if (
        draft.canonical_market_id != scope.canonical_market_id
        or draft.risk_policy_id != scope.risk_policy_id
        or draft.policy_hash != scope.policy_hash
        or draft.symbol != scope.canonical_symbol
        or draft.algorithm_version != computed.algorithm_version
        or draft.event_time != computed.event_time
        or tuple(sorted(draft.source_candle_ids)) != derived_ids
    ):
        raise IngestValidationError(
            "Reference-price draft scope or DB-derived provenance does not match"
        )
    if (
        draft.median_price != computed.median_price
        or draft.pairwise_divergence_bps != computed.pairwise_divergence_bps
        or draft.max_deviation_from_median_bps
        != computed.max_deviation_from_median_bps
    ):
        raise IngestValidationError(
            "REFERENCE_PRICE_VALUE_MISMATCH: claimed values differ from replay"
        )


def _reference_price_record_key(
    draft: ReferencePriceDraft,
    *,
    cutoff_as_of: datetime,
    evaluated_at: datetime,
) -> str:
    return (
        f"reference:{draft.canonical_market_id}:{draft.symbol}:"
        f"{_iso(draft.event_time)}:{_iso(cutoff_as_of)}:{_iso(evaluated_at)}:"
        f"{draft.policy_hash}"
    )


def _reference_price_inputs_hash(
    raw_rows: Sequence[_ReferencePriceRawRow],
    *,
    scope: _ReferencePriceScope,
) -> str:
    return _content_hash(
        {
            "algorithm_version": REFERENCE_PRICE_ALGORITHM_VERSION,
            "canonical_market_id": scope.canonical_market_id,
            "policy_hash": scope.policy_hash,
            "inputs": [
                {
                    "candle_id": item.candle.candle_id,
                    "market_id": item.candle.market_id,
                    "exchange_id": item.candle.exchange_id,
                    "exchange_key": item.candle.exchange_key,
                    "source_id": item.candle.source_id,
                    "source_key": item.candle.source_key,
                    "source_record_key": item.candle.source_record_key,
                    "source_version": item.candle.source_version,
                    "revision_no": item.candle.revision_no,
                    "content_hash": item.candle.content_hash,
                    "source_candle_receipt_id": item.source_candle_receipt_id,
                    "open_time": _iso(item.candle.open_time),
                    "close_time": _iso(item.candle.close_time),
                    "close_price": _decimal_text(item.candle.close_price),
                    "available_at": _iso(item.candle.available_at),
                    "provider_ingested_at": _iso(item.provider_ingested_at),
                }
                for item in sorted(
                    raw_rows,
                    key=lambda row: (
                        row.candle.source_key,
                        row.candle.candle_id,
                    ),
                )
            ],
        }
    )


def _reference_price_evidence_hash(
    *,
    scope: _ReferencePriceScope,
    cutoff_as_of: datetime,
    evaluated_at: datetime,
    computed: ReferencePriceResult,
    inputs_hash: str,
) -> str:
    return _content_hash(
        {
            "algorithm_version": computed.algorithm_version,
            "canonical_market_id": scope.canonical_market_id,
            "risk_policy_id": scope.risk_policy_id,
            "policy_hash": scope.policy_hash,
            "symbol": computed.symbol,
            "event_time": _iso(computed.event_time),
            "cutoff_as_of": _iso(cutoff_as_of),
            "evaluated_at": _iso(evaluated_at),
            "median_price": _decimal_text(computed.median_price),
            "pairwise_divergence_bps": _decimal_text(
                computed.pairwise_divergence_bps
            ),
            "max_deviation_from_median_bps": _decimal_text(
                computed.max_deviation_from_median_bps
            ),
            "available_at": _iso(computed.available_at),
            "provider_ingested_at": _iso(computed.ingested_at),
            "source_ids": computed.source_ids,
            "venue_ids": computed.venue_ids,
            "inputs_hash": inputs_hash,
        }
    )


def _reference_price_content_hash(
    *,
    reference_key: str,
    scope: _ReferencePriceScope,
    computed: ReferencePriceResult,
    inputs_hash: str,
    evidence_hash: str,
) -> str:
    return _content_hash(
        {
            "reference_key": reference_key,
            "canonical_market_id": scope.canonical_market_id,
            "risk_policy_id": scope.risk_policy_id,
            "algorithm_version": computed.algorithm_version,
            "policy_hash": scope.policy_hash,
            "inputs_hash": inputs_hash,
            "evidence_hash": evidence_hash,
        }
    )


def _reference_manifest_values(
    *,
    reference_key: str,
    scope: _ReferencePriceScope,
    computed: ReferencePriceResult,
    cutoff_as_of: datetime,
    evaluated_at: datetime,
    inputs_hash: str,
    evidence_hash: str,
    captured_at: datetime,
    content_hash: str,
) -> tuple[object, ...]:
    return (
        reference_key,
        scope.canonical_market_id,
        scope.risk_policy_id,
        computed.symbol,
        computed.algorithm_version,
        scope.policy_hash,
        computed.event_time,
        cutoff_as_of,
        evaluated_at,
        computed.median_price,
        computed.pairwise_divergence_bps,
        computed.max_deviation_from_median_bps,
        inputs_hash,
        evidence_hash,
        computed.available_at,
        captured_at,
        content_hash,
    )


def _verify_existing_reference_manifest(
    row: object,
    *,
    expected: tuple[object, ...],
) -> int:
    reference_price_id = int(_row_value(row, "reference_price_id", 0))
    keys = (
        "canonical_market_id",
        "risk_policy_id",
        "symbol",
        "algorithm_version",
        "policy_hash",
        "event_time",
        "cutoff_as_of",
        "evaluated_at",
        "median_price",
        "pairwise_divergence_bps",
        "max_deviation_from_median_bps",
        "inputs_hash",
        "evidence_hash",
        "available_at",
        "ingested_at",
        "content_hash",
    )
    values = [_row_value(row, key, index + 1) for index, key in enumerate(keys)]
    for index in (8, 9, 10):
        values[index] = _decimal(values[index], keys[index])
    stored_ingested_at = _datetime_value(row, "ingested_at", 15)
    expected_evaluated_at = cast(datetime, expected[8])
    expected_available_at = cast(datetime, expected[14])
    current_captured_at = cast(datetime, expected[15])
    if not (
        expected_available_at <= stored_ingested_at <= current_captured_at
        and expected_evaluated_at <= stored_ingested_at
    ):
        raise PersistenceInvariantError(
            "Existing reference-price manifest has invalid receipt lineage"
        )
    # ``ingested_at`` is the first durable capture time, not part of the
    # semantic content hash. A later retry must retain the original timestamp.
    semantic_values = (*values[:14], *values[15:])
    semantic_expected = (*expected[1:15], *expected[16:])
    if semantic_values != semantic_expected:
        raise PersistenceInvariantError(
            "Existing reference-price manifest differs from deterministic replay"
        )
    return reference_price_id


def _ensure_reference_price_provenance(
    db_cursor: DBCursor,
    *,
    reference_price_id: int,
    source_provenance: Sequence[tuple[int, int]],
    linked_at: datetime,
) -> None:
    _require_utc(linked_at, "linked_at")
    expected = tuple(sorted(source_provenance))
    if (
        len(expected) != 2
        or len({item[0] for item in expected}) != 2
        or len({item[1] for item in expected}) != 2
        or any(candle_id <= 0 or receipt_id <= 0 for candle_id, receipt_id in expected)
    ):
        raise PersistenceInvariantError(
            "Reference-price provenance requires two distinct candles and receipts"
        )
    db_cursor.execute(
        """
        SELECT source_candle_id, source_candle_receipt_id
        FROM crypto_agent.reference_price_provenance
        WHERE reference_price_id = %s
        ORDER BY source_candle_id, source_candle_receipt_id
        """,
        (reference_price_id,),
    )
    existing = tuple(
        (
            int(_row_value(row, "source_candle_id", 0)),
            int(_row_value(row, "source_candle_receipt_id", 1)),
        )
        for row in db_cursor.fetchall()
    )
    if existing and existing != expected:
        raise PersistenceInvariantError(
            "Reference-price provenance does not match its manifest"
        )
    if existing == expected:
        return
    for source_candle_id, source_candle_receipt_id in expected:
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.reference_price_provenance (
                reference_price_id, source_candle_id,
                source_candle_receipt_id, linked_at
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (reference_price_id, source_candle_id) DO NOTHING
            """,
            (
                reference_price_id,
                source_candle_id,
                source_candle_receipt_id,
                linked_at,
            ),
        )
    db_cursor.execute(
        """
        SELECT source_candle_id, source_candle_receipt_id
        FROM crypto_agent.reference_price_provenance
        WHERE reference_price_id = %s
        ORDER BY source_candle_id, source_candle_receipt_id
        """,
        (reference_price_id,),
    )
    persisted = tuple(
        (
            int(_row_value(row, "source_candle_id", 0)),
            int(_row_value(row, "source_candle_receipt_id", 1)),
        )
        for row in db_cursor.fetchall()
    )
    if persisted != expected:
        raise PersistenceInvariantError(
            "Reference-price provenance insert did not persist the exact mapping"
        )


def _verify_loaded_reference_price_manifest(
    row: object,
    *,
    scope: _ReferencePriceScope,
    raw_rows: Sequence[_ReferencePriceRawRow],
) -> ReferencePriceRecord:
    reference_price_id = int(_row_value(row, "reference_price_id", 0))
    reference_key = str(_row_value(row, "reference_key", 1))
    cutoff_as_of = _datetime_value(row, "cutoff_as_of", 8)
    evaluated_at = _datetime_value(row, "evaluated_at", 9)
    captured_at = _datetime_value(row, "ingested_at", 16)
    computed = _recompute_reference_price(
        raw_rows,
        scope=scope,
        cutoff_as_of=cutoff_as_of,
        evaluated_at=evaluated_at,
    )
    inputs_hash = _reference_price_inputs_hash(raw_rows, scope=scope)
    evidence_hash = _reference_price_evidence_hash(
        scope=scope,
        cutoff_as_of=cutoff_as_of,
        evaluated_at=evaluated_at,
        computed=computed,
        inputs_hash=inputs_hash,
    )
    content_hash = _reference_price_content_hash(
        reference_key=reference_key,
        scope=scope,
        computed=computed,
        inputs_hash=inputs_hash,
        evidence_hash=evidence_hash,
    )
    expected = _reference_manifest_values(
        reference_key=reference_key,
        scope=scope,
        computed=computed,
        cutoff_as_of=cutoff_as_of,
        evaluated_at=evaluated_at,
        inputs_hash=inputs_hash,
        evidence_hash=evidence_hash,
        captured_at=captured_at,
        content_hash=content_hash,
    )
    # The load query places reference_key after the identity; every remaining
    # manifest field must equal the replayed canonical tuple byte-for-byte.
    actual = (
        reference_key,
        int(_row_value(row, "canonical_market_id", 2)),
        int(_row_value(row, "risk_policy_id", 3)),
        str(_row_value(row, "symbol", 4)),
        str(_row_value(row, "algorithm_version", 5)),
        str(_row_value(row, "policy_hash", 6)),
        _datetime_value(row, "event_time", 7),
        cutoff_as_of,
        evaluated_at,
        _decimal(_row_value(row, "median_price", 10), "median_price"),
        _decimal(
            _row_value(row, "pairwise_divergence_bps", 11),
            "pairwise_divergence_bps",
        ),
        _decimal(
            _row_value(row, "max_deviation_from_median_bps", 12),
            "max_deviation_from_median_bps",
        ),
        str(_row_value(row, "inputs_hash", 13)),
        str(_row_value(row, "evidence_hash", 14)),
        _datetime_value(row, "available_at", 15),
        captured_at,
        str(_row_value(row, "content_hash", 17)),
    )
    if actual != expected or reference_key != _reference_price_record_key(
        ReferencePriceDraft(
            canonical_market_id=scope.canonical_market_id,
            risk_policy_id=scope.risk_policy_id,
            symbol=computed.symbol,
            event_time=computed.event_time,
            median_price=computed.median_price,
            pairwise_divergence_bps=computed.pairwise_divergence_bps,
            max_deviation_from_median_bps=computed.max_deviation_from_median_bps,
            algorithm_version=computed.algorithm_version,
            policy_hash=scope.policy_hash,
            source_candle_ids=cast(
                tuple[int, int],
                tuple(sorted(item.candle.candle_id for item in raw_rows)),
            ),
        ),
        cutoff_as_of=cutoff_as_of,
        evaluated_at=evaluated_at,
    ):
        raise PersistenceInvariantError(
            "Reference-price manifest failed deterministic hash replay"
        )
    source_ids = tuple(sorted(item.candle.candle_id for item in raw_rows))
    receipt_ids = tuple(sorted(item.source_candle_receipt_id for item in raw_rows))
    return ReferencePriceRecord(
        reference_price_id=reference_price_id,
        canonical_market_id=scope.canonical_market_id,
        risk_policy_id=scope.risk_policy_id,
        symbol=computed.symbol,
        event_time=computed.event_time,
        cutoff_as_of=cutoff_as_of,
        evaluated_at=evaluated_at,
        median_price=computed.median_price,
        pairwise_divergence_bps=computed.pairwise_divergence_bps,
        max_deviation_from_median_bps=computed.max_deviation_from_median_bps,
        algorithm_version=computed.algorithm_version,
        policy_hash=scope.policy_hash,
        available_at=computed.available_at,
        ingested_at=captured_at,
        inputs_hash=inputs_hash,
        evidence_hash=evidence_hash,
        content_hash=content_hash,
        source_candle_ids=cast(tuple[int, int], source_ids),
        source_candle_receipt_ids=cast(tuple[int, int], receipt_ids),
    )


def _derive_raw_candle_universe(
    db_cursor: DBCursor,
    *,
    series: _CanonicalSeries,
    draft: CanonicalCandleDraft,
    as_of: datetime,
    captured_at: datetime,
) -> tuple[_RawCandleRow, ...]:
    first_open_time = draft.open_time - timedelta(
        seconds=series.interval_seconds * (CONSENSUS_WINDOW_SIZE - 1)
    )
    approved_pairs = tuple(APPROVED_SOURCE_VENUES.items())
    db_cursor.execute(
        """
        WITH eligible_revisions AS (
            SELECT DISTINCT ON (candle.source_id, candle.source_record_key)
                   candle.candle_id, candle.market_id, candle.interval_seconds,
                   candle.open_time, candle.close_time,
                   candle.open_price, candle.high_price, candle.low_price,
                   candle.close_price, candle.base_volume, candle.trade_count,
                   candle.source_id, candle.source_record_key, candle.source_version,
                   candle.revision_no, candle.content_hash, candle.observed_at,
                   candle.available_at, candle.ingested_at, candle.is_final,
                   market.exchange_id, exchange.exchange_key,
                   market.base_asset_id, market.quote_asset_id,
                   market.instrument_type, source.source_key,
                   binding.canonical_symbol, binding.venue_symbol
            FROM crypto_agent.candles candle
            JOIN crypto_agent.markets market ON market.market_id = candle.market_id
            JOIN crypto_agent.exchanges exchange
              ON exchange.exchange_id = market.exchange_id
            JOIN crypto_agent.data_sources source
              ON source.source_id = candle.source_id
            JOIN crypto_agent.market_data_source_bindings binding
              ON binding.source_id = candle.source_id
             AND binding.market_id = candle.market_id
            WHERE candle.interval_seconds = %s
              AND candle.open_time >= %s AND candle.close_time <= %s
              AND market.base_asset_id = %s AND market.quote_asset_id = %s
              AND market.instrument_type = %s
              AND binding.canonical_symbol = %s
              AND (
                  (source.source_key = %s AND exchange.exchange_key = %s)
                  OR (source.source_key = %s AND exchange.exchange_key = %s)
              )
              AND candle.is_final
              AND candle.available_at <= %s AND candle.ingested_at <= %s
              AND binding.available_at <= %s AND binding.ingested_at <= %s
              AND market.available_at <= %s AND market.ingested_at <= %s
              AND exchange.available_at <= %s AND exchange.ingested_at <= %s
              AND source.available_at <= %s AND source.ingested_at <= %s
              AND EXISTS (
                  SELECT 1 FROM crypto_agent.market_symbols symbol
                  WHERE symbol.market_id = market.market_id
                    AND symbol.symbol = binding.venue_symbol
                    AND symbol.available_at <= %s AND symbol.ingested_at <= %s
              )
              AND EXISTS (
                  SELECT 1 FROM crypto_agent.source_candle_receipts receipt
                  WHERE receipt.candle_id = candle.candle_id
                    AND receipt.provider_ingested_at <= %s
                    AND receipt.cutoff_as_of <= %s
                    AND receipt.received_at <= %s
              )
            ORDER BY candle.source_id, candle.source_record_key,
                     candle.revision_no DESC, candle.available_at DESC,
                     candle.ingested_at DESC, candle.candle_id DESC
        )
        SELECT * FROM eligible_revisions
        ORDER BY source_key, open_time, close_time, candle_id
        """,
        (
            series.interval_seconds,
            first_open_time,
            draft.close_time,
            series.base_asset_id,
            series.quote_asset_id,
            series.instrument_type,
            series.canonical_symbol,
            approved_pairs[0][0],
            approved_pairs[0][1],
            approved_pairs[1][0],
            approved_pairs[1][1],
            # Raw availability is bounded by the market cutoff; durable
            # receipt/metadata writes may occur just after that cutoff.
            as_of, captured_at,  # candle
            as_of, captured_at,  # binding
            as_of, captured_at,  # market
            as_of, captured_at,  # exchange
            as_of, captured_at,  # data source
            as_of, captured_at,  # venue symbol
            as_of, as_of, captured_at,  # provider receipt/cutoff/received
        ),
    )
    return tuple(_raw_row(row) for row in db_cursor.fetchall())


def _validate_source_candle(
    candle: Candle,
    *,
    binding: _MarketBinding,
    as_of: datetime,
    captured_at: datetime,
) -> None:
    if candle.source != binding.source_key:
        raise IngestValidationError("Candle source does not match market binding")
    if candle.symbol != binding.canonical_symbol:
        raise IngestValidationError("Candle symbol does not match market binding")
    if candle.interval_minutes <= 0:
        raise IngestValidationError("Candle interval must be positive")
    _validate_times(candle.open_time, candle.close_time, candle.interval_minutes * 60)
    _require_utc(candle.available_at, "available_at")
    _require_utc(candle.ingested_at, "ingested_at")
    _validate_prices(
        _decimal(candle.open, "open"),
        _decimal(candle.high, "high"),
        _decimal(candle.low, "low"),
        _decimal(candle.close, "close"),
        _decimal(candle.volume, "volume"),
    )
    if not (
        candle.close_time
        <= candle.available_at
        <= candle.ingested_at
        <= as_of
        <= captured_at
    ):
        raise IngestValidationError(
            "Candle lineage must satisfy close<=available<=provider_ingested"
            "<=cutoff<=received"
        )


def _validate_canonical_inputs(
    draft: CanonicalCandleDraft,
    series: _CanonicalSeries,
    raw_rows: Sequence[_RawCandleRow],
    *,
    as_of: datetime,
    captured_at: datetime,
) -> None:
    if draft.algorithm_version != series.algorithm_version:
        raise IngestValidationError("Canonical algorithm version does not match series")
    if draft.policy_hash != series.policy_hash:
        raise IngestValidationError("Canonical policy hash does not match series")
    if series.algorithm_version != SUPPORTED_CANONICAL_ALGORITHM:
        raise IngestValidationError("Canonical series algorithm is unsupported")
    if not _is_sha256(series.policy_hash):
        raise PersistenceInvariantError("Canonical series has an invalid policy hash")
    _validate_times(draft.open_time, draft.close_time, series.interval_seconds)
    if series.interval_seconds % 60 != 0 or (
        series.interval_seconds // 60
        not in series.risk_policy.allowed_intervals_minutes
    ):
        raise IngestValidationError("Canonical interval is outside the validated policy")
    if series.canonical_symbol not in series.risk_policy.allowed_assets:
        raise IngestValidationError("Canonical symbol is outside the validated policy")
    derived_context_ids = {item.candle_id for item in raw_rows}
    if derived_context_ids != set(draft.context_candle_ids):
        raise IngestValidationError(
            "Canonical context IDs do not equal the exact DB-derived raw universe"
        )
    observations = [
        item
        for item in raw_rows
        if item.open_time == draft.open_time and item.close_time == draft.close_time
    ]
    derived_observation_ids = {item.candle_id for item in observations}
    if derived_observation_ids != set(draft.source_candle_ids):
        raise IngestValidationError(
            "Canonical observation IDs do not equal the exact DB-derived final window"
        )

    expected_row_count = len(APPROVED_SOURCE_VENUES) * CONSENSUS_WINDOW_SIZE
    if len(raw_rows) != expected_row_count:
        raise IngestValidationError(
            f"Canonical universe must contain exactly {expected_row_count} raw candles"
        )
    source_ids = {item.source_id for item in observations}
    venue_ids = {item.exchange_id for item in observations}
    if series.canonical_source_id in source_ids or series.exchange_id in venue_ids:
        raise IngestValidationError("Canonical series cannot use itself as provenance")
    if len(source_ids) != len(APPROVED_SOURCE_VENUES):
        raise IngestValidationError("Canonical candle requires the exact approved sources")
    if len(venue_ids) != len(APPROVED_SOURCE_VENUES):
        raise IngestValidationError("Canonical candle requires exact distinct venues")

    expected_windows = tuple(
        (
            draft.open_time
            - timedelta(
                seconds=series.interval_seconds
                * (CONSENSUS_WINDOW_SIZE - 1 - index)
            ),
            draft.close_time
            - timedelta(
                seconds=series.interval_seconds
                * (CONSENSUS_WINDOW_SIZE - 1 - index)
            ),
        )
        for index in range(CONSENSUS_WINDOW_SIZE)
    )
    for source_key, expected_venue_key in APPROVED_SOURCE_VENUES.items():
        members = sorted(
            (item for item in raw_rows if item.source_key == source_key),
            key=lambda item: (item.open_time, item.close_time, item.candle_id),
        )
        if len(members) != CONSENSUS_WINDOW_SIZE:
            raise IngestValidationError(
                "Each approved source must provide the exact fixed consensus window"
            )
        if len({item.source_id for item in members}) != 1:
            raise IngestValidationError("Approved source identity is not stable")
        if len({item.market_id for item in members}) != 1:
            raise IngestValidationError("Approved source market identity is not stable")
        if len({item.exchange_id for item in members}) != 1 or any(
            item.exchange_key != expected_venue_key for item in members
        ):
            raise IngestValidationError("Approved source-to-venue identity is invalid")
        actual_windows = tuple(
            (item.open_time, item.close_time) for item in members
        )
        if actual_windows != expected_windows:
            raise IngestValidationError(
                "Approved source windows must be identical, aligned, and contiguous"
            )

    for item in raw_rows:
        recomputed_source_hash = _raw_source_candle_content_hash(item)
        if not _is_sha256(item.content_hash) or not hmac.compare_digest(
            item.content_hash,
            recomputed_source_hash,
        ):
            raise PersistenceInvariantError(
                "Raw source candle content hash failed deterministic replay"
            )
        if (
            item.base_asset_id != series.base_asset_id
            or item.quote_asset_id != series.quote_asset_id
            or item.instrument_type != series.instrument_type
        ):
            raise IngestValidationError("Source market is not asset-equivalent to series")
        if item.canonical_symbol != series.canonical_symbol:
            raise IngestValidationError("Source symbol binding does not match series")
        _validate_times(item.open_time, item.close_time, series.interval_seconds)
        if item.candle_id in derived_observation_ids:
            if item.open_time != draft.open_time or item.close_time != draft.close_time:
                raise IngestValidationError(
                    "Source observation time scope does not match series"
                )
        elif item.close_time > draft.open_time:
            raise IngestValidationError("Consensus context must precede observation")
        if not item.is_final:
            raise IngestValidationError("Canonical candle cannot use unfinalized data")
        if item.available_at > as_of:
            raise IngestValidationError("Source candle violates point-in-time cutoff")
        if item.available_at > captured_at or item.ingested_at > captured_at:
            raise IngestValidationError("Source candle is later than canonical ingest")
        if not (item.close_time <= item.observed_at <= item.available_at):
            raise IngestValidationError("Source observation lineage is inconsistent")
        _validate_prices(
            item.open_price,
            item.high_price,
            item.low_price,
            item.close_price,
            item.base_volume,
        )
        if item.base_volume is None:
            raise IngestValidationError("Consensus context requires base volume")


def _recompute_canonical(
    draft: CanonicalCandleDraft,
    series: _CanonicalSeries,
    raw_rows: Sequence[_RawCandleRow],
) -> _ComputedCandle:
    if series.algorithm_version != SUPPORTED_CANONICAL_ALGORITHM:
        raise IngestValidationError("Canonical series algorithm is unsupported")
    # Freeze provider order independently of SQL row order so live and durable
    # diagnostics hash the same source sequence on every database/fixture.
    grouped: dict[str, list[ConsensusInput]] = {
        source_key: [] for source_key in APPROVED_SOURCE_VENUES
    }
    for item in raw_rows:
        if item.base_volume is None:
            raise IngestValidationError("Consensus context requires base volume")
        source_key = item.source_key
        if source_key not in grouped:
            raise IngestValidationError("Canonical context contains an unapproved source")
        grouped[source_key].append(
            ConsensusInput(
                source_id=source_key,
                open_time=item.open_time,
                close_time=item.close_time,
                open=float(item.open_price),
                high=float(item.high_price),
                low=float(item.low_price),
                close=float(item.close_price),
                volume=float(item.base_volume),
                available_at=item.available_at,
                ingested_at=item.ingested_at,
            )
        )
    try:
        result = build_cross_exchange_consensus(
            grouped,
            series.policy_parameters,
            limit=CONSENSUS_WINDOW_SIZE,
        )
    except ConsensusMathError as exc:
        raise IngestValidationError(f"{exc.code}: {exc}") from None
    latest = result.candles[-1]
    if latest.open_time != draft.open_time or latest.close_time != draft.close_time:
        raise IngestValidationError("Consensus context does not end at observation window")
    computed = _ComputedCandle(
        open_price=_decimal(latest.open, "open_price"),
        high_price=_decimal(latest.high, "high_price"),
        low_price=_decimal(latest.low, "low_price"),
        close_price=_decimal(latest.close, "close_price"),
        normalized_volume=_decimal(latest.normalized_volume, "normalized_volume"),
        normalized_volume_unit=NORMALIZED_VOLUME_UNIT,
        trade_count=None,
        available_at=max(item.available_at for item in raw_rows),
        diagnostics_document=_consensus_diagnostics(result),
    )
    _validate_prices(
        computed.open_price,
        computed.high_price,
        computed.low_price,
        computed.close_price,
        None,
    )
    if not computed.normalized_volume.is_finite() or computed.normalized_volume < 0:
        raise IngestValidationError(
            "Canonical normalized volume must be finite and non-negative"
        )
    return computed


def _consensus_diagnostics(result: ConsensusResult) -> Mapping[str, object]:
    return {
        "consensus_source_ids": list(result.source_ids),
        "consensus_source_counts": {
            key: int(result.source_counts[key]) for key in sorted(result.source_counts)
        },
        "consensus_overlap_count": result.overlap_count,
        "consensus_close_divergences_bps": list(result.close_divergences_bps),
        "consensus_ohlc_divergences_bps": list(result.ohlc_divergences_bps),
        "consensus_volume_zscores": {
            key: result.volume_zscores[key] for key in sorted(result.volume_zscores)
        },
        "consensus_volume_zscore_delta": result.volume_zscore_delta,
        "consensus_volume_scales": {
            key: result.volume_scales[key] for key in sorted(result.volume_scales)
        },
    }


def _verify_untrusted_draft(
    draft: CanonicalCandleDraft,
    computed: _ComputedCandle,
) -> None:
    claimed = (
        draft.open_price,
        draft.high_price,
        draft.low_price,
        draft.close_price,
        draft.base_volume,
        draft.trade_count,
    )
    expected = (
        computed.open_price,
        computed.high_price,
        computed.low_price,
        computed.close_price,
        None,
        computed.trade_count,
    )
    if claimed != expected:
        raise IngestValidationError(
            "CANONICAL_VALUE_MISMATCH: draft OHLCV differs from recomputed raw inputs"
        )


def _append_source_receipt(
    db_cursor: DBCursor,
    *,
    candle_id: int,
    provider_ingested_at: datetime,
    received_at: datetime,
    cutoff_as_of: datetime,
) -> None:
    receipt_hash = _content_hash(
        {
            "candle_id": candle_id,
            "provider_ingested_at": _iso(provider_ingested_at),
            "received_at": _iso(received_at),
            "cutoff_as_of": _iso(cutoff_as_of),
        }
    )
    db_cursor.execute(
        """
        INSERT INTO crypto_agent.source_candle_receipts (
            candle_id, provider_ingested_at, received_at, cutoff_as_of, receipt_hash
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (receipt_hash) DO NOTHING
        """,
        (candle_id, provider_ingested_at, received_at, cutoff_as_of, receipt_hash),
    )


def _ensure_manifest(
    db_cursor: DBCursor,
    *,
    candle_id: int,
    series: _CanonicalSeries,
    inputs_hash: str,
    computed_payload_hash: str,
    cutoff_as_of: datetime,
    computed: _ComputedCandle,
    evidence_hash: str,
) -> None:
    expected = (
        series.canonical_series_id,
        series.risk_policy_id,
        series.algorithm_version,
        series.policy_hash,
        CONSENSUS_WINDOW_SIZE,
        cutoff_as_of,
        inputs_hash,
        computed_payload_hash,
        computed.normalized_volume,
        computed.normalized_volume_unit,
        computed.diagnostics_document,
        evidence_hash,
    )
    db_cursor.execute(
        """
        SELECT canonical_series_id, risk_policy_id, algorithm_version, policy_hash,
               consensus_window_size, cutoff_as_of, inputs_hash,
               computed_payload_hash, normalized_volume, normalized_volume_unit,
               diagnostics_document, evidence_hash
        FROM crypto_agent.canonical_candle_manifests
        WHERE canonical_candle_id = %s
        """,
        (candle_id,),
    )
    existing_row = db_cursor.fetchone()
    if existing_row is not None:
        keys = (
            "canonical_series_id",
            "risk_policy_id",
            "algorithm_version",
            "policy_hash",
            "consensus_window_size",
            "cutoff_as_of",
            "inputs_hash",
            "computed_payload_hash",
            "normalized_volume",
            "normalized_volume_unit",
            "diagnostics_document",
            "evidence_hash",
        )
        existing_values = [
            _row_value(existing_row, key, index) for index, key in enumerate(keys)
        ]
        existing_values[8] = _decimal(existing_values[8], "normalized_volume")
        existing_values[10] = _json_object(
            existing_values[10], "diagnostics_document"
        )
        existing = tuple(existing_values)
        if existing != expected:
            raise PersistenceInvariantError("Canonical computation manifest mismatch")
        return
    db_cursor.execute(
        """
        INSERT INTO crypto_agent.canonical_candle_manifests (
            canonical_candle_id, canonical_series_id, risk_policy_id,
            algorithm_version, policy_hash, consensus_window_size, cutoff_as_of,
            inputs_hash, computed_payload_hash, normalized_volume,
            normalized_volume_unit, diagnostics_document, evidence_hash
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s
        )
        """,
        (
            candle_id,
            *expected[:10],
            _canonical_json(computed.diagnostics_document),
            evidence_hash,
        ),
    )


def _ensure_provenance(
    db_cursor: DBCursor,
    *,
    canonical_candle_id: int,
    observation_candle_ids: Sequence[int],
    context_candle_ids: Sequence[int],
) -> None:
    observation_set = set(observation_candle_ids)
    expected = {
        candle_id: "observation" if candle_id in observation_set else "context"
        for candle_id in context_candle_ids
    }
    db_cursor.execute(
        """
        SELECT source_candle_id, input_role
        FROM crypto_agent.canonical_candle_provenance
        WHERE canonical_candle_id = %s
        ORDER BY source_candle_id
        """,
        (canonical_candle_id,),
    )
    existing = {
        int(_row_value(row, "source_candle_id", 0)): str(
            _row_value(row, "input_role", 1)
        )
        for row in db_cursor.fetchall()
    }
    if existing and existing != expected:
        raise PersistenceInvariantError("Canonical candle provenance does not match")
    if existing == expected:
        return
    for source_candle_id, input_role in sorted(expected.items()):
        db_cursor.execute(
            """
            INSERT INTO crypto_agent.canonical_candle_provenance (
                canonical_candle_id, source_candle_id, input_role
            ) VALUES (%s, %s, %s)
            ON CONFLICT (canonical_candle_id, source_candle_id) DO NOTHING
            """,
            (canonical_candle_id, source_candle_id, input_role),
        )
    db_cursor.execute(
        """
        SELECT source_candle_id, input_role
        FROM crypto_agent.canonical_candle_provenance
        WHERE canonical_candle_id = %s
        ORDER BY source_candle_id
        """,
        (canonical_candle_id,),
    )
    persisted = {
        int(_row_value(row, "source_candle_id", 0)): str(
            _row_value(row, "input_role", 1)
        )
        for row in db_cursor.fetchall()
    }
    if persisted != expected:
        raise PersistenceInvariantError(
            "Canonical candle provenance insert did not persist the exact mapping"
        )


def _canonical_inputs_hash(
    raw_rows: Sequence[_RawCandleRow],
    *,
    series: _CanonicalSeries,
    observation_candle_ids: Sequence[int],
) -> str:
    observation_set = set(observation_candle_ids)
    return _content_hash(
        {
            "algorithm_version": series.algorithm_version,
            "consensus_window_size": CONSENSUS_WINDOW_SIZE,
            "policy_hash": series.policy_hash,
            "approved_source_venues": APPROVED_SOURCE_VENUES,
            "inputs": [
                {
                    "candle_id": item.candle_id,
                    "market_id": item.market_id,
                    "exchange_id": item.exchange_id,
                    "exchange_key": item.exchange_key,
                    "source_id": item.source_id,
                    "source_key": item.source_key,
                    "source_record_key": item.source_record_key,
                    "source_version": item.source_version,
                    "revision_no": item.revision_no,
                    "content_hash": item.content_hash,
                    "input_role": (
                        "observation"
                        if item.candle_id in observation_set
                        else "context"
                    ),
                    "open_time": _iso(item.open_time),
                    "close_time": _iso(item.close_time),
                }
                for item in sorted(
                    raw_rows,
                    key=lambda row: (
                        row.source_key,
                        row.open_time,
                        row.close_time,
                        row.revision_no,
                        row.candle_id,
                    ),
                )
            ]
        }
    )


def _computed_payload_hash(
    draft: CanonicalCandleDraft,
    series: _CanonicalSeries,
    computed: _ComputedCandle,
) -> str:
    return _content_hash(
        {
            "canonical_series_id": series.canonical_series_id,
            "open_time": _iso(draft.open_time),
            "close_time": _iso(draft.close_time),
            "open": _decimal_text(computed.open_price),
            "high": _decimal_text(computed.high_price),
            "low": _decimal_text(computed.low_price),
            "close": _decimal_text(computed.close_price),
            "base_volume": None,
            "normalized_volume": _decimal_text(computed.normalized_volume),
            "normalized_volume_unit": computed.normalized_volume_unit,
            "trade_count": computed.trade_count,
        }
    )


def _canonical_evidence_hash(
    *,
    series: _CanonicalSeries,
    cutoff_as_of: datetime,
    inputs_hash: str,
    computed_payload_hash: str,
    diagnostics_document: Mapping[str, object],
) -> str:
    return _content_hash(
        {
            "algorithm_version": series.algorithm_version,
            "consensus_window_size": CONSENSUS_WINDOW_SIZE,
            "policy_hash": series.policy_hash,
            "cutoff_as_of": _iso(cutoff_as_of),
            "inputs_hash": inputs_hash,
            "computed_payload_hash": computed_payload_hash,
            "diagnostics": diagnostics_document,
        }
    )


def _canonical_content_hash(
    draft: CanonicalCandleDraft,
    series: _CanonicalSeries,
    *,
    inputs_hash: str,
    computed_payload_hash: str,
    evidence_hash: str,
) -> str:
    return _content_hash(
        {
            "record_key": canonical_candle_record_key(draft),
            "algorithm_version": series.algorithm_version,
            "risk_policy_id": series.risk_policy_id,
            "policy_hash": series.policy_hash,
            "inputs_hash": inputs_hash,
            "computed_payload_hash": computed_payload_hash,
            "evidence_hash": evidence_hash,
        }
    )


def _load_revisions(
    db_cursor: DBCursor,
    *,
    source_id: int,
    source_record_key: str,
) -> tuple[tuple[int, int, str], ...]:
    db_cursor.execute(
        """
        SELECT candle_id, revision_no, content_hash
        FROM crypto_agent.candles
        WHERE source_id = %s AND source_record_key = %s
        ORDER BY revision_no DESC
        """,
        (source_id, source_record_key),
    )
    return tuple(
        (
            int(_row_value(row, "candle_id", 0)),
            int(_row_value(row, "revision_no", 1)),
            str(_row_value(row, "content_hash", 2)),
        )
        for row in db_cursor.fetchall()
    )


def _find_content_hash(
    revisions: Sequence[tuple[int, int, str]],
    content_hash: str,
) -> tuple[int, int, str] | None:
    return next((row for row in revisions if row[2] == content_hash), None)


def _advisory_lock(db_cursor: DBCursor, lock_key: str) -> None:
    db_cursor.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (lock_key,))


def _raw_row(row: object) -> _RawCandleRow:
    volume = _row_value(row, "base_volume", 9)
    trade_count = _row_value(row, "trade_count", 10)
    return _RawCandleRow(
        candle_id=int(_row_value(row, "candle_id", 0)),
        market_id=int(_row_value(row, "market_id", 1)),
        interval_seconds=int(_row_value(row, "interval_seconds", 2)),
        open_time=_datetime_value(row, "open_time", 3),
        close_time=_datetime_value(row, "close_time", 4),
        open_price=_decimal(_row_value(row, "open_price", 5), "open_price"),
        high_price=_decimal(_row_value(row, "high_price", 6), "high_price"),
        low_price=_decimal(_row_value(row, "low_price", 7), "low_price"),
        close_price=_decimal(_row_value(row, "close_price", 8), "close_price"),
        base_volume=None if volume is None else _decimal(volume, "base_volume"),
        trade_count=None if trade_count is None else int(trade_count),
        source_id=int(_row_value(row, "source_id", 11)),
        source_record_key=str(_row_value(row, "source_record_key", 12)),
        source_version=str(_row_value(row, "source_version", 13)),
        revision_no=int(_row_value(row, "revision_no", 14)),
        content_hash=str(_row_value(row, "content_hash", 15)),
        observed_at=_datetime_value(row, "observed_at", 16),
        available_at=_datetime_value(row, "available_at", 17),
        ingested_at=_datetime_value(row, "ingested_at", 18),
        is_final=bool(_row_value(row, "is_final", 19)),
        exchange_id=int(_row_value(row, "exchange_id", 20)),
        exchange_key=str(_row_value(row, "exchange_key", 21)),
        base_asset_id=int(_row_value(row, "base_asset_id", 22)),
        quote_asset_id=int(_row_value(row, "quote_asset_id", 23)),
        instrument_type=str(_row_value(row, "instrument_type", 24)),
        source_key=str(_row_value(row, "source_key", 25)),
        canonical_symbol=str(_row_value(row, "canonical_symbol", 26)),
        venue_symbol=str(_row_value(row, "venue_symbol", 27)),
    )


def _verify_loaded_canonical_manifest(
    row: object,
    *,
    series: _CanonicalSeries,
    raw_rows: Sequence[_RawCandleRow],
    observation_candle_ids: Sequence[int],
    context_candle_ids: Sequence[int],
) -> CanonicalCandleRecord:
    """Recompute persisted consensus and its complete manifest hash chain."""

    candle_id = int(_row_value(row, "candle_id", 0))
    record = _canonical_record(
        row,
        {candle_id: tuple(observation_candle_ids)},
        {candle_id: tuple(context_candle_ids)},
    )
    cutoff_as_of = _datetime_value(row, "cutoff_as_of", 24)
    inputs_hash = str(_row_value(row, "inputs_hash", 25))
    computed_payload_hash = str(
        _row_value(row, "computed_payload_hash", 26)
    )
    diagnostics_document = _json_object(
        _row_value(row, "diagnostics_document", 27),
        "diagnostics_document",
    )
    source_record_key = str(_row_value(row, "source_record_key", 28))
    full_context_ids = tuple(
        sorted({*observation_candle_ids, *context_candle_ids})
    )
    try:
        draft = CanonicalCandleDraft(
            canonical_series_id=record.canonical_series_id,
            open_time=record.open_time,
            close_time=record.close_time,
            open_price=record.open_price,
            high_price=record.high_price,
            low_price=record.low_price,
            close_price=record.close_price,
            base_volume=record.base_volume,
            trade_count=record.trade_count,
            algorithm_version=record.algorithm_version,
            policy_hash=record.policy_hash,
            source_candle_ids=tuple(observation_candle_ids),
            context_candle_ids=full_context_ids,
        )
    except IngestError:
        raise PersistenceInvariantError(
            "Canonical manifest failed deterministic replay"
        ) from None

    try:
        if (
            series.registry_available_at > cutoff_as_of
            or series.registry_ingested_at > cutoff_as_of
        ):
            raise PersistenceInvariantError(
                "Canonical manifest cutoff predates required registry metadata"
            )
        if cutoff_as_of < series.policy_effective_from or (
            series.policy_effective_to is not None
            and cutoff_as_of >= series.policy_effective_to
        ):
            raise PersistenceInvariantError(
                "Canonical manifest cutoff is outside its policy validity interval"
            )
        if not (
            record.observed_at == record.close_time
            and cutoff_as_of <= record.ingested_at
        ):
            raise PersistenceInvariantError(
                "Canonical manifest has inconsistent temporal lineage"
            )
        _validate_canonical_inputs(
            draft,
            series,
            raw_rows,
            as_of=cutoff_as_of,
            captured_at=record.ingested_at,
        )
        computed = _recompute_canonical(draft, series, raw_rows)
        _verify_untrusted_draft(draft, computed)
        if (
            record.available_at != computed.available_at
            or record.normalized_volume != computed.normalized_volume
            or record.normalized_volume_unit != computed.normalized_volume_unit
            or _canonical_json(diagnostics_document)
            != _canonical_json(computed.diagnostics_document)
        ):
            raise PersistenceInvariantError(
                "Canonical manifest values differ from deterministic replay"
            )

        expected_inputs_hash = _canonical_inputs_hash(
            raw_rows,
            series=series,
            observation_candle_ids=observation_candle_ids,
        )
        expected_payload_hash = _computed_payload_hash(draft, series, computed)
        expected_evidence_hash = _canonical_evidence_hash(
            series=series,
            cutoff_as_of=cutoff_as_of,
            inputs_hash=expected_inputs_hash,
            computed_payload_hash=expected_payload_hash,
            diagnostics_document=computed.diagnostics_document,
        )
        expected_content_hash = _canonical_content_hash(
            draft,
            series,
            inputs_hash=expected_inputs_hash,
            computed_payload_hash=expected_payload_hash,
            evidence_hash=expected_evidence_hash,
        )
        claimed_hashes = (
            inputs_hash,
            computed_payload_hash,
            record.evidence_hash,
            record.content_hash,
        )
        expected_hashes = (
            expected_inputs_hash,
            expected_payload_hash,
            expected_evidence_hash,
            expected_content_hash,
        )
        if not all(_is_sha256(value) for value in claimed_hashes) or not all(
            hmac.compare_digest(claimed, expected)
            for claimed, expected in zip(claimed_hashes, expected_hashes, strict=True)
        ):
            raise PersistenceInvariantError(
                "Canonical manifest hash chain failed deterministic replay"
            )
        if source_record_key != canonical_candle_record_key(draft):
            raise PersistenceInvariantError(
                "Canonical source record key does not match its immutable scope"
            )
    except PersistenceInvariantError:
        raise
    except (IngestError, ArithmeticError, ValueError, TypeError):
        raise PersistenceInvariantError(
            "Canonical manifest failed deterministic replay"
        ) from None
    return record


def _canonical_record(
    row: object,
    observations: Mapping[int, tuple[int, ...]],
    contexts: Mapping[int, tuple[int, ...]],
) -> CanonicalCandleRecord:
    candle_id = int(_row_value(row, "candle_id", 0))
    volume = _row_value(row, "base_volume", 9)
    if volume is not None:
        raise PersistenceInvariantError("Canonical candle base_volume must be NULL")
    trade_count = _row_value(row, "trade_count", 10)
    normalized_volume = _decimal(
        _row_value(row, "normalized_volume", 20), "normalized_volume"
    )
    normalized_volume_unit = str(
        _row_value(row, "normalized_volume_unit", 21)
    )
    evidence_hash = str(_row_value(row, "evidence_hash", 22))
    consensus_window_size = int(
        _row_value(row, "consensus_window_size", 23)
    )
    if normalized_volume < 0:
        raise PersistenceInvariantError("Canonical normalized volume is invalid")
    if normalized_volume_unit != NORMALIZED_VOLUME_UNIT:
        raise PersistenceInvariantError("Canonical normalized volume unit is invalid")
    if not _is_sha256(evidence_hash):
        raise PersistenceInvariantError("Canonical evidence hash is invalid")
    if consensus_window_size != CONSENSUS_WINDOW_SIZE:
        raise PersistenceInvariantError("Canonical consensus window is invalid")
    return CanonicalCandleRecord(
        candle_id=candle_id,
        canonical_series_id=int(_row_value(row, "canonical_series_id", 16)),
        risk_policy_id=int(_row_value(row, "risk_policy_id", 17)),
        market_id=int(_row_value(row, "market_id", 1)),
        interval_seconds=int(_row_value(row, "interval_seconds", 2)),
        open_time=_datetime_value(row, "open_time", 3),
        close_time=_datetime_value(row, "close_time", 4),
        open_price=_decimal(_row_value(row, "open_price", 5), "open_price"),
        high_price=_decimal(_row_value(row, "high_price", 6), "high_price"),
        low_price=_decimal(_row_value(row, "low_price", 7), "low_price"),
        close_price=_decimal(_row_value(row, "close_price", 8), "close_price"),
        base_volume=None,
        normalized_volume=normalized_volume,
        normalized_volume_unit=normalized_volume_unit,
        trade_count=None if trade_count is None else int(trade_count),
        revision_no=int(_row_value(row, "revision_no", 11)),
        observed_at=_datetime_value(row, "observed_at", 12),
        available_at=_datetime_value(row, "available_at", 13),
        ingested_at=_datetime_value(row, "ingested_at", 14),
        content_hash=str(_row_value(row, "content_hash", 15)),
        algorithm_version=str(_row_value(row, "algorithm_version", 18)),
        policy_hash=str(_row_value(row, "policy_hash", 19)),
        evidence_hash=evidence_hash,
        consensus_window_size=consensus_window_size,
        source_candle_ids=observations.get(candle_id, ()),
        context_candle_ids=tuple(
            sorted(
                {
                    *observations.get(candle_id, ()),
                    *contexts.get(candle_id, ()),
                }
            )
        ),
    )


def _validate_times(open_time: datetime, close_time: datetime, interval_seconds: int) -> None:
    _require_utc(open_time, "open_time")
    _require_utc(close_time, "close_time")
    if open_time >= close_time:
        raise IngestValidationError("Candle open_time must precede close_time")
    if close_time - open_time != timedelta(seconds=interval_seconds):
        raise IngestValidationError("Candle duration does not match its interval")


def _validate_prices(
    open_price: Decimal,
    high_price: Decimal,
    low_price: Decimal,
    close_price: Decimal,
    base_volume: Decimal | None,
) -> None:
    values = (open_price, high_price, low_price, close_price)
    if any(not value.is_finite() or value <= 0 for value in values):
        raise IngestValidationError("Candle prices must be finite and positive")
    if low_price > min(open_price, close_price, high_price):
        raise IngestValidationError("Candle low price is inconsistent")
    if high_price < max(open_price, close_price, low_price):
        raise IngestValidationError("Candle high price is inconsistent")
    if base_volume is not None and (not base_volume.is_finite() or base_volume < 0):
        raise IngestValidationError("Candle volume must be finite and non-negative")


def _datetime_value(row: object, key: str, index: int) -> datetime:
    value = _row_value(row, key, index)
    if not isinstance(value, datetime):
        raise PersistenceInvariantError(f"PostgreSQL {key} is not a datetime")
    _require_utc(value, key)
    return value


def _row_value(row: object, key: str, index: int) -> object:
    if isinstance(row, Mapping):
        return cast(Mapping[object, object], row)[key]
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        return cast(Sequence[object], row)[index]
    raise PersistenceInvariantError("PostgreSQL returned an unsupported row shape")


def _decimal(value: object, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise IngestValidationError(f"{field_name} must be numeric")
    if isinstance(value, float) and not math.isfinite(value):
        raise IngestValidationError(f"{field_name} must be finite")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise IngestValidationError(f"{field_name} must be numeric") from None
    if not result.is_finite():
        raise IngestValidationError(f"{field_name} must be finite")
    return result


def _policy_int(value: object, field_name: str) -> int:
    try:
        if isinstance(value, bool):
            raise ValueError
        result = int(str(value))
    except ValueError:
        raise PersistenceInvariantError(f"PostgreSQL {field_name} is invalid") from None
    if result < 2:
        raise PersistenceInvariantError(f"PostgreSQL {field_name} must be at least two")
    return result


def _policy_float(
    value: object,
    field_name: str,
    *,
    allow_zero: bool = False,
) -> float:
    if isinstance(value, bool):
        raise PersistenceInvariantError(f"PostgreSQL {field_name} is invalid")
    try:
        result = float(str(value))
    except ValueError:
        raise PersistenceInvariantError(f"PostgreSQL {field_name} is invalid") from None
    if not math.isfinite(result) or (result < 0 if allow_zero else result <= 0):
        raise PersistenceInvariantError(f"PostgreSQL {field_name} is invalid")
    return result


def _risk_policy_from_document(value: object) -> RiskPolicy:
    document = dict(_json_object(value, "policy_document"))
    try:
        if (
            set(document) == _LEGACY_R1_POLICY_FIELDS
            and document.get("policy_id") == LEGACY_R1_POLICY_ID
        ):
            document["policy_schema_version"] = 1
            document["max_reference_price_age_seconds"] = 300
            document["max_reference_price_deviation_from_median_bps"] = 50.0
        for key in (
            "allowed_decisions",
            "allowed_assets",
            "allowed_intervals_minutes",
        ):
            candidate = document[key]
            if isinstance(candidate, (str, bytes, bytearray)) or not isinstance(
                candidate, Sequence
            ):
                raise TypeError
            document[key] = tuple(candidate)
        policy = RiskPolicy(**document)
        policy.validate_v1_safety()
        if policy.policy_schema_version == 1 and (
            policy.policy_id != LEGACY_R1_POLICY_ID
            or policy.fingerprint() != LEGACY_R1_POLICY_HASH
        ):
            raise PolicyConfigurationError("Archived policy fingerprint is not approved")
    except (KeyError, TypeError, PolicyConfigurationError):
        raise IngestValidationError(
            "Canonical series policy document fails V1 safety validation"
        ) from None
    return policy


def _json_object(value: object, field_name: str) -> Mapping[str, object]:
    candidate = value
    if isinstance(value, str):
        try:
            candidate = json.loads(value)
        except (json.JSONDecodeError, ValueError):
            raise PersistenceInvariantError(
                f"PostgreSQL {field_name} is invalid JSON"
            ) from None
    if not isinstance(candidate, Mapping) or not all(
        isinstance(key, str) for key in candidate
    ):
        raise PersistenceInvariantError(
            f"PostgreSQL {field_name} must be a JSON object"
        )
    return cast(Mapping[str, object], candidate)


def _canonical_json(payload: Mapping[str, object]) -> str:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _content_hash(payload: Mapping[str, object]) -> str:
    canonical = _canonical_json(payload)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _iso(value: datetime) -> str:
    _require_utc(value, "timestamp")
    return value.isoformat().replace("+00:00", "Z")


def _require_utc(value: datetime, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise IngestValidationError(f"{field_name} must be timezone-aware UTC")


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)

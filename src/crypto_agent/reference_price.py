from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP, localcontext


REFERENCE_PRICE_ALGORITHM_VERSION = "cross_exchange_reference_price_v1"
REFERENCE_PRICE_INTERVAL_SECONDS = 60
REFERENCE_PRICE_SOURCE_COUNT = 2
REFERENCE_PRICE_BPS_DECIMAL_PLACES = 12
REFERENCE_PRICE_MAX_PRICE_DECIMAL_PLACES = 18
_REFERENCE_PRICE_BPS_QUANTUM = Decimal("0.000000000001")
_REFERENCE_PRICE_MATH_PRECISION = 80


class ReferencePriceMathError(ValueError):
    """A deterministic, machine-classifiable reference-price rejection."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: Mapping[str, object] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class ReferencePriceInput:
    """One normalized, closed one-minute observation from an independent venue."""

    source_id: str
    venue_id: str
    symbol: str
    open_time: datetime
    close_time: datetime
    price: int | float | Decimal
    available_at: datetime
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class ReferencePriceResult:
    algorithm_version: str
    symbol: str
    event_time: datetime
    median_price: Decimal
    pairwise_divergence_bps: Decimal
    max_deviation_from_median_bps: Decimal
    available_at: datetime
    ingested_at: datetime
    source_ids: tuple[str, str]
    venue_ids: tuple[str, str]


def build_reference_price_snapshot(
    inputs: Sequence[ReferencePriceInput],
    *,
    cutoff_as_of: datetime,
    evaluated_at: datetime,
    max_age_seconds: int,
    max_deviation_from_median_bps: int | float | Decimal,
    max_market_price: int | float | Decimal,
    approved_source_venues: Mapping[str, str],
) -> ReferencePriceResult:
    """Recompute the exact two-venue V1 reference price from raw observations."""

    if not _is_utc(cutoff_as_of) or not _is_utc(evaluated_at):
        raise ReferencePriceMathError(
            "Reference-price cutoff and evaluation time must be timezone-aware UTC",
            code="REFERENCE_PRICE_TIME_INVALID",
        )
    if cutoff_as_of > evaluated_at:
        raise ReferencePriceMathError(
            "Reference-price cutoff cannot follow its evaluation time",
            code="REFERENCE_PRICE_TIME_INVALID",
        )
    if type(max_age_seconds) is not int or max_age_seconds != 300:
        raise ReferencePriceMathError(
            "Reference-price age policy must be exactly 300 seconds",
            code="REFERENCE_PRICE_POLICY_INVALID",
        )
    maximum_deviation = _finite_decimal(
        max_deviation_from_median_bps,
        field_name="max_deviation_from_median_bps",
    )
    maximum_price = _finite_decimal(
        max_market_price,
        field_name="max_market_price",
    )
    if not Decimal("1") <= maximum_deviation <= Decimal("50"):
        raise ReferencePriceMathError(
            "Reference-price median-deviation policy is outside [1, 50] bps",
            code="REFERENCE_PRICE_POLICY_INVALID",
        )
    if maximum_price <= 0:
        raise ReferencePriceMathError(
            "Reference-price market bound must be positive",
            code="REFERENCE_PRICE_POLICY_INVALID",
        )
    if len(approved_source_venues) != REFERENCE_PRICE_SOURCE_COUNT:
        raise ReferencePriceMathError(
            "Reference-price source policy must contain exactly two venues",
            code="REFERENCE_PRICE_POLICY_INVALID",
        )
    if len(inputs) != REFERENCE_PRICE_SOURCE_COUNT:
        raise ReferencePriceMathError(
            "Reference price requires exactly two source observations",
            code="REFERENCE_PRICE_PROVENANCE_INVALID",
            details={"source_count": len(inputs)},
        )

    ordered = tuple(sorted(inputs, key=lambda item: (item.source_id, item.venue_id)))
    source_ids = tuple(item.source_id for item in ordered)
    venue_ids = tuple(item.venue_id for item in ordered)
    expected_sources = tuple(sorted(approved_source_venues))
    expected_venues = tuple(sorted(approved_source_venues.values()))
    if (
        len(set(source_ids)) != REFERENCE_PRICE_SOURCE_COUNT
        or len(set(venue_ids)) != REFERENCE_PRICE_SOURCE_COUNT
        or tuple(sorted(source_ids)) != expected_sources
        or tuple(sorted(venue_ids)) != expected_venues
        or any(
            approved_source_venues.get(item.source_id) != item.venue_id
            for item in ordered
        )
    ):
        raise ReferencePriceMathError(
            "Reference price does not use the exact approved source-to-venue pair",
            code="REFERENCE_PRICE_PROVENANCE_INVALID",
            details={"source_ids": source_ids, "venue_ids": venue_ids},
        )

    symbols = {item.symbol for item in ordered}
    windows = {(item.open_time, item.close_time) for item in ordered}
    if len(symbols) != 1 or next(iter(symbols), "").strip() == "":
        raise ReferencePriceMathError(
            "Reference-price observations disagree on the symbol",
            code="REFERENCE_PRICE_SCOPE_INVALID",
        )
    if len(windows) != 1:
        raise ReferencePriceMathError(
            "Reference-price observations are not aligned to one window",
            code="REFERENCE_PRICE_WINDOW_INVALID",
        )

    prices: list[Decimal] = []
    for item in ordered:
        timestamps = (
            item.open_time,
            item.close_time,
            item.available_at,
            item.ingested_at,
        )
        if any(not _is_utc(timestamp) for timestamp in timestamps):
            raise ReferencePriceMathError(
                "Reference-price timestamps must be normalized to UTC",
                code="REFERENCE_PRICE_TIME_INVALID",
                details={"source_id": item.source_id},
            )
        if item.close_time - item.open_time != timedelta(
            seconds=REFERENCE_PRICE_INTERVAL_SECONDS
        ) or any(
            value != 0
            for value in (
                item.open_time.second,
                item.open_time.microsecond,
                item.close_time.second,
                item.close_time.microsecond,
            )
        ):
            raise ReferencePriceMathError(
                "Reference-price observation must be one UTC-aligned closed minute",
                code="REFERENCE_PRICE_WINDOW_INVALID",
                details={"source_id": item.source_id},
            )
        if not (
            item.open_time
            < item.close_time
            <= item.available_at
            <= item.ingested_at
            <= evaluated_at
        ):
            raise ReferencePriceMathError(
                "Reference-price observation violates point-in-time lineage",
                code="REFERENCE_PRICE_LOOKAHEAD",
                details={"source_id": item.source_id},
            )
        if item.close_time > cutoff_as_of:
            raise ReferencePriceMathError(
                "Reference-price observation is later than the selection cutoff",
                code="REFERENCE_PRICE_LOOKAHEAD",
                details={"source_id": item.source_id},
            )
        age = evaluated_at - item.close_time
        if not timedelta(0) <= age <= timedelta(seconds=max_age_seconds):
            raise ReferencePriceMathError(
                "Reference-price observation exceeds the five-minute limit",
                code="REFERENCE_PRICE_STALE",
                details={
                    "source_id": item.source_id,
                    "age_seconds": str(age.total_seconds()),
                },
            )
        price = _finite_decimal(item.price, field_name="price")
        if price <= 0 or price > maximum_price:
            raise ReferencePriceMathError(
                "Reference-price observation is outside the bounded price domain",
                code="REFERENCE_PRICE_VALUE_INVALID",
                details={"source_id": item.source_id},
            )
        price_exponent = price.as_tuple().exponent
        if (
            not isinstance(price_exponent, int)
            or price_exponent < -REFERENCE_PRICE_MAX_PRICE_DECIMAL_PLACES
        ):
            raise ReferencePriceMathError(
                "Reference-price observation has unsupported price precision",
                code="REFERENCE_PRICE_VALUE_INVALID",
                details={"source_id": item.source_id},
            )
        prices.append(price)

    with localcontext() as context:
        context.prec = _REFERENCE_PRICE_MATH_PRECISION
        context.rounding = ROUND_HALF_UP
        median_price = (prices[0] + prices[1]) / Decimal("2")
        raw_deviations = tuple(
            abs(price - median_price) / median_price * Decimal("10000")
            for price in prices
        )
        raw_maximum_observed_deviation = max(raw_deviations)
        raw_pairwise_divergence = (
            abs(prices[0] - prices[1]) / median_price * Decimal("10000")
        )
        maximum_observed_deviation = raw_maximum_observed_deviation.quantize(
            _REFERENCE_PRICE_BPS_QUANTUM,
            rounding=ROUND_HALF_UP,
        )
        pairwise_divergence = raw_pairwise_divergence.quantize(
            _REFERENCE_PRICE_BPS_QUANTUM,
            rounding=ROUND_HALF_UP,
        )
    if raw_maximum_observed_deviation > maximum_deviation:
        raise ReferencePriceMathError(
            "Reference-price venues exceed the median-deviation policy",
            code="REFERENCE_PRICE_DIVERGENCE",
            details={
                "pairwise_divergence_bps": str(raw_pairwise_divergence),
                "max_deviation_from_median_bps": str(
                    raw_maximum_observed_deviation
                ),
                "threshold_bps": str(maximum_deviation),
            },
        )

    event_time = ordered[0].close_time
    return ReferencePriceResult(
        algorithm_version=REFERENCE_PRICE_ALGORITHM_VERSION,
        symbol=ordered[0].symbol,
        event_time=event_time,
        median_price=median_price,
        pairwise_divergence_bps=pairwise_divergence,
        max_deviation_from_median_bps=maximum_observed_deviation,
        available_at=max(item.available_at for item in ordered),
        ingested_at=max(item.ingested_at for item in ordered),
        source_ids=(source_ids[0], source_ids[1]),
        venue_ids=(venue_ids[0], venue_ids[1]),
    )


def _finite_decimal(value: object, *, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise ReferencePriceMathError(
            f"Reference-price {field_name} must be a finite number",
            code="REFERENCE_PRICE_VALUE_INVALID",
        )
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        raise ReferencePriceMathError(
            f"Reference-price {field_name} must be a finite number",
            code="REFERENCE_PRICE_VALUE_INVALID",
        ) from None
    if not result.is_finite():
        raise ReferencePriceMathError(
            f"Reference-price {field_name} must be a finite number",
            code="REFERENCE_PRICE_VALUE_INVALID",
        )
    return result


def _is_utc(value: object) -> bool:
    return bool(
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
    )

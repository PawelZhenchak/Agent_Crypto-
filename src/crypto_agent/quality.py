from __future__ import annotations

import math
from datetime import datetime, timedelta

from .domain import Candle, DataQualityReport
from .policy import RiskPolicy


CRITICAL_FLAGS = {
    "DUPLICATE_CONFLICT",
    "EMPTY_DATA",
    "FUTURE_DATA",
    "INSUFFICIENT_HISTORY",
    "INVALID_CANDLE_DURATION",
    "INVALID_OHLC",
    "INVALID_PRICE",
    "INVALID_TIME_ORDER",
    "INVALID_TIMEZONE",
    "LOOKAHEAD_DATA",
    "MISSING_INTERVALS",
    "NON_FINITE_VALUE",
    "NON_UTC_TIMESTAMP",
    "POST_CUTOFF_INGESTION",
    "STALE_DATA",
    "UNEXPECTED_INTERVAL",
    "UNEXPECTED_SOURCE",
    "UNEXPECTED_SYMBOL",
}


def assess_data_quality(
    candles: list[Candle],
    *,
    as_of: datetime,
    interval_minutes: int,
    expected_symbol: str,
    expected_source: str,
    policy: RiskPolicy,
) -> DataQualityReport:
    if not _is_utc(as_of):
        raise ValueError("as_of must be timezone-aware and normalized to UTC")
    if not candles:
        return _empty_report("EMPTY_DATA")

    flags: list[str] = []
    expected_delta = timedelta(minutes=interval_minutes)
    time_valid: list[Candle] = []

    for candle in candles:
        if candle.symbol != expected_symbol:
            flags.append("UNEXPECTED_SYMBOL")
        if candle.interval_minutes != interval_minutes:
            flags.append("UNEXPECTED_INTERVAL")
        if candle.source != expected_source:
            flags.append("UNEXPECTED_SOURCE")

        numeric_values = (
            candle.open,
            candle.high,
            candle.low,
            candle.close,
            candle.volume,
        )
        finite = all(
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(value)
            for value in numeric_values
        )
        if not finite:
            flags.append("NON_FINITE_VALUE")
        else:
            if min(candle.open, candle.high, candle.low, candle.close) <= 0 or candle.volume < 0:
                flags.append("INVALID_PRICE")
            if not (candle.low <= candle.open <= candle.high):
                flags.append("INVALID_OHLC")
            if not (candle.low <= candle.close <= candle.high):
                flags.append("INVALID_OHLC")

        timestamps = (
            candle.open_time,
            candle.close_time,
            candle.available_at,
            candle.ingested_at,
        )
        if any(
            timestamp.tzinfo is None or timestamp.utcoffset() is None
            for timestamp in timestamps
        ):
            flags.append("INVALID_TIMEZONE")
            continue
        if any(timestamp.utcoffset() != timedelta(0) for timestamp in timestamps):
            flags.append("NON_UTC_TIMESTAMP")
            continue

        time_valid.append(candle)
        if candle.open_time >= candle.close_time:
            flags.append("INVALID_TIME_ORDER")
        if candle.close_time - candle.open_time != expected_delta:
            flags.append("INVALID_CANDLE_DURATION")
        if not (candle.close_time <= candle.available_at <= candle.ingested_at):
            flags.append("INVALID_TIME_ORDER")
        if candle.open_time >= as_of or candle.close_time > as_of:
            flags.append("FUTURE_DATA")
        if candle.available_at > as_of:
            flags.append("LOOKAHEAD_DATA")
        if candle.ingested_at > as_of:
            flags.append("POST_CUTOFF_INGESTION")

    ordered = sorted(time_valid, key=lambda candle: candle.open_time)
    seen: dict[datetime, Candle] = {}
    for candle in ordered:
        existing = seen.get(candle.open_time)
        if existing is not None:
            if _market_values(existing) != _market_values(candle):
                flags.append("DUPLICATE_CONFLICT")
            else:
                flags.append("DUPLICATE_EXACT")
        else:
            seen[candle.open_time] = candle

    unique = sorted(seen.values(), key=lambda candle: candle.open_time)
    for previous, current in zip(unique, unique[1:], strict=False):
        if current.open_time - previous.open_time > expected_delta * policy.max_gap_multiplier:
            flags.append("MISSING_INTERVALS")
            break

    newest_observed_at = max((candle.close_time for candle in unique), default=None)
    newest_available_at = max((candle.available_at for candle in unique), default=None)
    max_age = expected_delta * policy.max_staleness_multiplier
    if newest_observed_at is None or as_of - newest_observed_at > max_age:
        flags.append("STALE_DATA")
    if len(unique) < policy.min_samples:
        flags.append("INSUFFICIENT_HISTORY")

    deduplicated_flags = tuple(dict.fromkeys(flags))
    critical = tuple(flag for flag in deduplicated_flags if flag in CRITICAL_FLAGS)
    score = max(0.0, round(1.0 - 0.03 * ("DUPLICATE_EXACT" in flags) - len(critical), 4))
    return DataQualityReport(
        score=score,
        sample_count=len(unique),
        flags=deduplicated_flags,
        critical_flags=critical,
        newest_observed_at=newest_observed_at,
        newest_available_at=newest_available_at,
    )


def deduplicate_exact_candles(candles: list[Candle]) -> list[Candle]:
    """Return one row per open_time. Conflicts must already have been quarantined."""

    unique: dict[datetime, Candle] = {}
    for candle in sorted(candles, key=lambda item: item.open_time):
        unique.setdefault(candle.open_time, candle)
    return list(unique.values())


def _empty_report(flag: str) -> DataQualityReport:
    return DataQualityReport(
        score=0.0,
        sample_count=0,
        flags=(flag,),
        critical_flags=(flag,),
        newest_observed_at=None,
        newest_available_at=None,
    )


def _is_utc(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() == timedelta(0)


def _market_values(candle: Candle) -> tuple[float, float, float, float, float]:
    return candle.open, candle.high, candle.low, candle.close, candle.volume

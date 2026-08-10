from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal


CONSENSUS_ALGORITHM_VERSION = "cross_exchange_spot_consensus_v1"
CONSENSUS_WINDOW_SIZE = 120


class ConsensusMathError(RuntimeError):
    """Fail-closed consensus error with secret-free diagnostic measurements."""

    def __init__(
        self,
        message: str,
        *,
        code: str,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class ConsensusPolicyParameters:
    min_overlap: int
    max_close_divergence_bps: float
    max_ohlc_divergence_bps: float
    max_volume_zscore_delta: float
    max_divergent_fraction: float
    max_market_price: float
    max_base_volume: float


@dataclass(frozen=True, slots=True)
class ConsensusInput:
    source_id: str
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    available_at: datetime
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class ConsensusOutput:
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    normalized_volume: float
    available_at: datetime
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class ConsensusResult:
    candles: tuple[ConsensusOutput, ...]
    source_ids: tuple[str, ...]
    source_counts: Mapping[str, int]
    overlap_count: int
    close_divergences_bps: tuple[float, ...]
    ohlc_divergences_bps: tuple[float, ...]
    volume_zscores: Mapping[str, float]
    volume_zscore_delta: float
    volume_scales: Mapping[str, float]


def build_cross_exchange_consensus(
    inputs_by_source: Mapping[str, Sequence[ConsensusInput]],
    policy: ConsensusPolicyParameters,
    *,
    limit: int | None = None,
) -> ConsensusResult:
    """Run the sole versioned V1.1 numeric algorithm for live and durable replay."""

    _validate_policy(policy)
    if limit is not None and (
        isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0
    ):
        raise ValueError("limit must be a positive integer")

    source_ids = tuple(inputs_by_source)
    if len(source_ids) < 2:
        raise ConsensusMathError(
            "Consensus requires at least two distinct sources",
            code="INSUFFICIENT_SOURCE_OVERLAP",
            details={"consensus_source_ids": list(source_ids)},
        )

    indexed: dict[str, dict[tuple[datetime, datetime], ConsensusInput]] = {}
    for source_id, inputs in inputs_by_source.items():
        if not isinstance(source_id, str) or not source_id.strip() or not inputs:
            raise ConsensusMathError(
                "Consensus source returned no data",
                code="EMPTY_SOURCE_DATA",
                details={"source_id": source_id},
            )
        source_index: dict[tuple[datetime, datetime], ConsensusInput] = {}
        for item in inputs:
            _validate_input(item, source_id=source_id, policy=policy)
            key = (item.open_time, item.close_time)
            if key in source_index:
                raise ConsensusMathError(
                    "Consensus source returned duplicate intervals",
                    code="SOURCE_DUPLICATE_INTERVAL",
                    details={
                        "source_id": source_id,
                        "open_time": item.open_time.isoformat(),
                    },
                )
            source_index[key] = item
        indexed[source_id] = source_index

    common_keys = set.intersection(*(set(items) for items in indexed.values()))
    ordered_keys = sorted(common_keys)
    if limit is not None:
        ordered_keys = ordered_keys[-limit:]
    if len(ordered_keys) < policy.min_overlap:
        raise ConsensusMathError(
            "Independent feeds do not have enough aligned observations",
            code="INSUFFICIENT_SOURCE_OVERLAP",
            details={
                "consensus_source_ids": list(source_ids),
                "consensus_source_counts": {
                    source_id: len(indexed[source_id]) for source_id in source_ids
                },
                "consensus_overlap_count": len(ordered_keys),
                "consensus_min_overlap": policy.min_overlap,
            },
        )

    _validate_contiguous_window(ordered_keys)

    aligned = {
        source_id: [indexed[source_id][key] for key in ordered_keys]
        for source_id in source_ids
    }
    volume_zscores = {
        source_id: _latest_zscore([item.volume for item in aligned[source_id]])
        for source_id in source_ids
    }
    volume_zscore_delta = _max_pairwise_delta(tuple(volume_zscores.values()))
    if volume_zscore_delta > policy.max_volume_zscore_delta:
        raise ConsensusMathError(
            "Independent market feeds disagree on the latest volume anomaly",
            code="CROSS_SOURCE_VOLUME_DIVERGENCE",
            details={
                "consensus_source_ids": list(source_ids),
                "consensus_volume_zscores": dict(volume_zscores),
                "consensus_volume_zscore_delta": volume_zscore_delta,
                "consensus_volume_zscore_threshold": policy.max_volume_zscore_delta,
            },
        )
    volume_scales = {
        source_id: _robust_positive_scale(
            [item.volume for item in aligned[source_id][:-1]],
            source_id=source_id,
        )
        for source_id in source_ids
    }

    close_divergences: list[float] = []
    ohlc_divergences: list[float] = []
    divergent_indexes: list[int] = []
    for index, key in enumerate(ordered_keys):
        observations = [indexed[source_id][key] for source_id in source_ids]
        close_divergence = _field_divergence(observations, "close")
        ohlc_divergence = max(
            _field_divergence(observations, field)
            for field in ("open", "high", "low", "close")
        )
        close_divergences.append(close_divergence)
        ohlc_divergences.append(ohlc_divergence)
        if (
            close_divergence > policy.max_close_divergence_bps
            or ohlc_divergence > policy.max_ohlc_divergence_bps
        ):
            divergent_indexes.append(index)

    divergent_fraction = len(divergent_indexes) / len(ordered_keys)
    if (
        len(ordered_keys) - 1 in divergent_indexes
        or divergent_fraction > policy.max_divergent_fraction
    ):
        worst = max(
            range(len(ordered_keys)),
            key=lambda index: max(
                close_divergences[index], ohlc_divergences[index]
            ),
        )
        worst_key = ordered_keys[worst]
        raise ConsensusMathError(
            "At least one aligned candle exceeds the divergence policy",
            code="CROSS_SOURCE_DIVERGENCE",
            details={
                "consensus_source_ids": list(source_ids),
                "consensus_overlap_count": len(ordered_keys),
                "consensus_max_close_divergence_bps": max(close_divergences),
                "consensus_latest_close_divergence_bps": close_divergences[-1],
                "consensus_close_threshold_bps": policy.max_close_divergence_bps,
                "consensus_max_ohlc_divergence_bps": max(ohlc_divergences),
                "consensus_latest_ohlc_divergence_bps": ohlc_divergences[-1],
                "consensus_ohlc_threshold_bps": policy.max_ohlc_divergence_bps,
                "consensus_divergent_candle_fraction": divergent_fraction,
                "consensus_divergent_fraction_limit": policy.max_divergent_fraction,
                "consensus_worst_open_time": worst_key[0].isoformat(),
                "consensus_worst_close_time": worst_key[1].isoformat(),
            },
        )

    candles: list[ConsensusOutput] = []
    for key in ordered_keys:
        observations = [indexed[source_id][key] for source_id in source_ids]
        count = len(observations)
        candles.append(
            ConsensusOutput(
                open_time=key[0],
                close_time=key[1],
                open=sum(item.open for item in observations) / count,
                high=sum(item.high for item in observations) / count,
                low=sum(item.low for item in observations) / count,
                close=sum(item.close for item in observations) / count,
                normalized_volume=sum(
                    _normalized_volume(
                        item.volume,
                        volume_scales[item.source_id],
                        source_id=item.source_id,
                    )
                    for item in observations
                )
                / count,
                available_at=max(item.available_at for item in observations),
                ingested_at=max(item.ingested_at for item in observations),
            )
        )

    return ConsensusResult(
        candles=tuple(candles),
        source_ids=source_ids,
        source_counts={
            source_id: len(inputs_by_source[source_id]) for source_id in source_ids
        },
        overlap_count=len(ordered_keys),
        close_divergences_bps=tuple(close_divergences),
        ohlc_divergences_bps=tuple(ohlc_divergences),
        volume_zscores=volume_zscores,
        volume_zscore_delta=volume_zscore_delta,
        volume_scales=volume_scales,
    )


def _validate_policy(policy: ConsensusPolicyParameters) -> None:
    if (
        isinstance(policy.min_overlap, bool)
        or not isinstance(policy.min_overlap, int)
        or not 60 <= policy.min_overlap <= 720
    ):
        raise ConsensusMathError(
            "Consensus policy is outside the bounded V1.1 domain",
            code="INVALID_CONSENSUS_POLICY",
        )
    values = (
        policy.max_close_divergence_bps,
        policy.max_ohlc_divergence_bps,
        policy.max_volume_zscore_delta,
        policy.max_divergent_fraction,
        policy.max_market_price,
        policy.max_base_volume,
    )
    if not all(_is_finite_number(value) for value in values):
        raise ConsensusMathError(
            "Consensus policy is outside the bounded V1.1 domain",
            code="INVALID_CONSENSUS_POLICY",
        )
    if (
        not 1.0 <= policy.max_close_divergence_bps <= 100.0
        or not 1.0 <= policy.max_ohlc_divergence_bps <= 500.0
        or not 0.1 <= policy.max_volume_zscore_delta <= 3.0
        or policy.max_divergent_fraction != 0.0
        or not 1_000_000.0 <= policy.max_market_price <= 1_000_000_000.0
        or not 10_000_000.0 <= policy.max_base_volume <= 1_000_000_000_000.0
    ):
        raise ConsensusMathError(
            "Consensus policy is outside the bounded V1.1 domain",
            code="INVALID_CONSENSUS_POLICY",
        )


def _validate_contiguous_window(
    ordered_keys: Sequence[tuple[datetime, datetime]],
) -> None:
    if not ordered_keys:
        raise ConsensusMathError(
            "Consensus window contains no aligned intervals",
            code="INSUFFICIENT_SOURCE_OVERLAP",
        )
    expected_duration = ordered_keys[0][1] - ordered_keys[0][0]
    for index, (open_time, close_time) in enumerate(ordered_keys):
        if close_time - open_time != expected_duration:
            raise ConsensusMathError(
                "Aligned consensus intervals do not have one duration",
                code="CONSENSUS_DURATION_MISMATCH",
                details={
                    "consensus_window_size": len(ordered_keys),
                    "consensus_invalid_open_time": open_time.isoformat(),
                },
            )
        if index > 0 and open_time != ordered_keys[index - 1][1]:
            raise ConsensusMathError(
                "Aligned consensus window contains a gap or overlap",
                code="CONSENSUS_WINDOW_GAP",
                details={
                    "consensus_window_size": len(ordered_keys),
                    "consensus_previous_close_time": ordered_keys[index - 1][
                        1
                    ].isoformat(),
                    "consensus_next_open_time": open_time.isoformat(),
                },
            )


def _validate_input(
    item: ConsensusInput,
    *,
    source_id: str,
    policy: ConsensusPolicyParameters,
) -> None:
    if not isinstance(item, ConsensusInput):
        raise ConsensusMathError(
            "Consensus source returned invalid market data",
            code="SOURCE_VALUE_INVALID",
            details={"source_id": source_id},
        )
    timestamps = (
        item.open_time,
        item.close_time,
        item.available_at,
        item.ingested_at,
    )
    values = (item.open, item.high, item.low, item.close, item.volume)
    valid = (
        item.source_id == source_id
        and all(_is_utc_datetime(stamp) for stamp in timestamps)
        and item.open_time < item.close_time <= item.available_at <= item.ingested_at
        and all(_is_finite_number(value) for value in values)
        and min(item.open, item.high, item.low, item.close) > 0
        and item.volume >= 0
        and max(item.open, item.high, item.low, item.close)
        <= policy.max_market_price
        and item.volume <= policy.max_base_volume
        and item.low <= item.open <= item.high
        and item.low <= item.close <= item.high
    )
    if not valid:
        raise ConsensusMathError(
            "Consensus source returned invalid market data",
            code="SOURCE_VALUE_INVALID",
            details={"source_id": source_id},
        )


def _field_divergence(
    observations: Sequence[ConsensusInput],
    field: Literal["open", "high", "low", "close"],
) -> float:
    maximum = 0.0
    for index, left in enumerate(observations):
        for right in observations[index + 1 :]:
            left_value = getattr(left, field)
            right_value = getattr(right, field)
            midpoint = (left_value + right_value) / 2
            if midpoint <= 0 or not math.isfinite(midpoint):
                raise ConsensusMathError(
                    "Cannot calculate cross-source divergence safely",
                    code="SOURCE_VALUE_INVALID",
                )
            maximum = max(
                maximum,
                abs(left_value - right_value) / midpoint * 10_000,
            )
    return maximum


def _latest_zscore(values: Sequence[float]) -> float:
    baseline = values[-30:-1]
    if not baseline:
        return 0.0
    deviation = statistics.pstdev(baseline)
    center = statistics.fmean(baseline)
    if deviation == 0:
        return (values[-1] - center) / max(abs(center) * 0.01, 1e-12)
    result = (values[-1] - center) / deviation
    return result if math.isfinite(result) else math.copysign(math.inf, result)


def _robust_positive_scale(values: Sequence[float], *, source_id: str) -> float:
    positive = [value for value in values if value > 0 and math.isfinite(value)]
    if not positive:
        raise ConsensusMathError(
            "Consensus source has no positive volume baseline",
            code="SOURCE_VOLUME_INVALID",
            details={"source_id": source_id},
        )
    scale = statistics.median(positive)
    if scale < 1e-12 or not math.isfinite(scale):
        raise ConsensusMathError(
            "Consensus source has an invalid volume baseline",
            code="SOURCE_VOLUME_INVALID",
            details={"source_id": source_id},
        )
    return scale


def _normalized_volume(value: float, scale: float, *, source_id: str) -> float:
    result = value / scale
    if not math.isfinite(result):
        raise ConsensusMathError(
            "Consensus volume normalization exceeded the bounded domain",
            code="SOURCE_VOLUME_INVALID",
            details={"source_id": source_id},
        )
    return result


def _max_pairwise_delta(values: Sequence[float]) -> float:
    maximum = 0.0
    for index, left in enumerate(values):
        for right in values[index + 1 :]:
            delta = abs(left - right)
            if not math.isfinite(delta):
                return math.inf
            maximum = max(maximum, delta)
    return maximum


def _is_finite_number(value: object) -> bool:
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
    )


def _is_utc_datetime(value: object) -> bool:
    return bool(
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
    )

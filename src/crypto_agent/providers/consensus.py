from __future__ import annotations

import math
from datetime import datetime, timedelta

from ..consensus_math import (
    CONSENSUS_ALGORITHM_VERSION,
    CONSENSUS_WINDOW_SIZE,
    ConsensusInput,
    ConsensusMathError,
    ConsensusPolicyParameters,
    build_cross_exchange_consensus,
)
from ..deadline import AnalysisDeadlineExceeded, ensure_analysis_deadline
from ..domain import Candle
from ..policy import RiskPolicy
from .base import CandleProvider, ProviderBatch, ProviderError, ProviderFailureEvidence
from .coinbase import CoinbaseExchangePublicProvider
from .kraken import KrakenPublicProvider


def _composition_signature(
    providers: tuple[CandleProvider, ...],
) -> tuple[tuple[type[object], str, str | None, float | None, bool], ...]:
    return tuple(
        (
            type(provider),
            provider.source_id,
            _provider_origin(provider),
            _provider_timeout(provider),
            getattr(provider, "_configuration_sealed", False) is True,
        )
        for provider in providers
    )


def _provider_origin(provider: CandleProvider) -> str | None:
    value = getattr(provider, "base_url", None)
    return value if isinstance(value, str) else None


def _provider_timeout(provider: CandleProvider) -> float | None:
    value = getattr(provider, "timeout_seconds", None)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _provider_configuration_metadata(
    providers: tuple[CandleProvider, ...],
) -> dict[str, dict[str, object]]:
    return {
        provider.source_id: {
            "origin": _provider_origin(provider),
            "timeout_seconds": _provider_timeout(provider),
            "sealed": getattr(provider, "_configuration_sealed", False) is True,
        }
        for provider in providers
    }


def _is_approved_composition(providers: tuple[CandleProvider, ...]) -> bool:
    if not (
        len(providers) == 2
        and {type(provider) for provider in providers}
        == {KrakenPublicProvider, CoinbaseExchangePublicProvider}
        and {provider.source_id for provider in providers}
        == {
            KrakenPublicProvider.source_id,
            CoinbaseExchangePublicProvider.source_id,
        }
    ):
        return False
    expected_origins: dict[type[object], str] = {
        KrakenPublicProvider: "https://api.kraken.com",
        CoinbaseExchangePublicProvider: "https://api.exchange.coinbase.com",
    }
    return all(
        _provider_origin(provider) == expected_origins[type(provider)]
        and _provider_timeout(provider) is not None
        and getattr(provider, "_configuration_sealed", False) is True
        for provider in providers
    )


class CrossExchangeConsensusProvider:
    """Build a deterministic candle series only when independent venues agree."""

    __slots__ = (
        "_providers",
        "_policy",
        "_policy_fingerprint",
        "_composition_signature",
        "_initially_approved",
        "_allow_unapproved_for_testing",
        "_sealed",
    )

    source_id = CONSENSUS_ALGORITHM_VERSION

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("Consensus provider configuration is immutable")
        object.__setattr__(self, name, value)

    def __delattr__(self, name: str) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("Consensus provider configuration is immutable")
        object.__delattr__(self, name)

    def __init__(
        self,
        providers: tuple[CandleProvider, ...],
        policy: RiskPolicy,
        *,
        allow_unapproved_for_testing: bool = False,
    ) -> None:
        policy.validate_v1_safety()
        if len(providers) != policy.min_consensus_sources:
            raise ValueError(
                f"Consensus requires exactly {policy.min_consensus_sources} providers"
            )
        source_ids = tuple(provider.source_id for provider in providers)
        if len(set(source_ids)) != len(source_ids):
            raise ValueError("Consensus providers must have distinct source ids")
        if self.source_id in source_ids:
            raise ValueError("A consensus provider cannot contain itself")
        approved_pair = _is_approved_composition(providers)
        if not approved_pair and not allow_unapproved_for_testing:
            raise ValueError(
                "V1.1 consensus requires the approved independent Kraken and Coinbase venues"
            )
        object.__setattr__(self, "_providers", tuple(providers))
        object.__setattr__(self, "_policy", policy)
        object.__setattr__(self, "_policy_fingerprint", policy.fingerprint())
        object.__setattr__(
            self,
            "_composition_signature",
            _composition_signature(providers),
        )
        object.__setattr__(self, "_initially_approved", approved_pair)
        object.__setattr__(
            self,
            "_allow_unapproved_for_testing",
            bool(allow_unapproved_for_testing),
        )
        object.__setattr__(self, "_sealed", True)

    @property
    def providers(self) -> tuple[CandleProvider, ...]:
        return self._providers

    @property
    def policy(self) -> RiskPolicy:
        return self._policy

    @property
    def approved_venue_pair(self) -> bool:
        """Derive approval from the sealed state instead of trusting a latched bool."""

        try:
            return bool(
                self._initially_approved
                and self._policy.fingerprint() == self._policy_fingerprint
                and _composition_signature(self._providers)
                == self._composition_signature
                and _is_approved_composition(self._providers)
            )
        except Exception:
            return False

    def _validate_sealed_configuration(self) -> None:
        """Revalidate all attested state immediately before accessing either feed."""

        try:
            self._policy.validate_v1_safety()
            policy_matches = self._policy.fingerprint() == self._policy_fingerprint
            composition_matches = (
                _composition_signature(self._providers)
                == self._composition_signature
            )
            approval_matches = (
                _is_approved_composition(self._providers)
                == self._initially_approved
            )
            testing_mode_valid = bool(
                self._initially_approved or self._allow_unapproved_for_testing
            )
        except Exception:
            policy_matches = False
            composition_matches = False
            approval_matches = False
            testing_mode_valid = False
        if not all(
            (
                policy_matches,
                composition_matches,
                approval_matches,
                testing_mode_valid,
            )
        ):
            raise ProviderError(
                "Consensus provider configuration failed integrity validation",
                code="CONSENSUS_CONFIGURATION_TAMPERED",
            )

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

    def fetch_batch(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> ProviderBatch:
        self._validate_sealed_configuration()
        fetch_limit = max(limit, CONSENSUS_WINDOW_SIZE)
        by_source: dict[str, list[Candle]] = {}
        for provider in self.providers:
            ensure_analysis_deadline()
            try:
                candles = provider.fetch_candles(
                    symbol=symbol,
                    interval_minutes=interval_minutes,
                    as_of=as_of,
                    limit=fetch_limit,
                )
            except AnalysisDeadlineExceeded:
                raise
            except ProviderError as exc:
                raise ProviderError(
                    f"Required consensus source unavailable: {provider.source_id}",
                    code="CONSENSUS_SOURCE_UNAVAILABLE",
                    evidence=_failure_evidence(
                        self.providers,
                        by_source,
                        self.policy,
                        failed_source_id=provider.source_id,
                        upstream_error_code=exc.code,
                    ),
                ) from exc
            except Exception as exc:
                raise ProviderError(
                    f"Required consensus source failed safely: {provider.source_id}",
                    code="CONSENSUS_SOURCE_UNAVAILABLE",
                    evidence=_failure_evidence(
                        self.providers,
                        by_source,
                        self.policy,
                        failed_source_id=provider.source_id,
                        upstream_error_code="PROVIDER_INTERNAL_ERROR",
                    ),
                ) from exc
            by_source[provider.source_id] = candles
            try:
                _validate_source_series(
                    candles,
                    source_id=provider.source_id,
                    symbol=symbol,
                    interval_minutes=interval_minutes,
                    as_of=as_of,
                    max_market_price=self.policy.max_market_price,
                    max_base_volume=self.policy.max_base_volume,
                )
            except ProviderError as exc:
                raise ProviderError(
                    str(exc),
                    code=exc.code,
                    evidence=_failure_evidence(
                        self.providers,
                        by_source,
                        self.policy,
                        failed_source_id=provider.source_id,
                        upstream_error_code=exc.code,
                    ),
                ) from exc

        inputs = {
            source_id: tuple(
                ConsensusInput(
                    source_id=source_id,
                    open_time=item.open_time,
                    close_time=item.close_time,
                    open=item.open,
                    high=item.high,
                    low=item.low,
                    close=item.close,
                    volume=item.volume,
                    available_at=item.available_at,
                    ingested_at=item.ingested_at,
                )
                for item in candles
            )
            for source_id, candles in by_source.items()
        }
        parameters = ConsensusPolicyParameters(
            min_overlap=CONSENSUS_WINDOW_SIZE,
            max_close_divergence_bps=self.policy.max_cross_source_divergence_bps,
            max_ohlc_divergence_bps=(
                self.policy.max_cross_source_ohlc_divergence_bps
            ),
            max_volume_zscore_delta=(
                self.policy.max_cross_source_volume_zscore_delta
            ),
            max_divergent_fraction=self.policy.max_divergent_candle_fraction,
            max_market_price=self.policy.max_market_price,
            max_base_volume=self.policy.max_base_volume,
        )
        try:
            result = build_cross_exchange_consensus(
                inputs,
                parameters,
                limit=CONSENSUS_WINDOW_SIZE,
            )
        except ConsensusMathError as exc:
            raise ProviderError(
                str(exc),
                code=exc.code,
                evidence=_failure_evidence(
                    self.providers,
                    by_source,
                    self.policy,
                    **exc.details,
                ),
            ) from exc
        if result.overlap_count != CONSENSUS_WINDOW_SIZE:
            raise ProviderError(
                "Independent feeds do not contain the fixed consensus window",
                code="INSUFFICIENT_SOURCE_OVERLAP",
                evidence=_failure_evidence(
                    self.providers,
                    by_source,
                    self.policy,
                    consensus_overlap_count=result.overlap_count,
                    consensus_required_overlap=CONSENSUS_WINDOW_SIZE,
                ),
            )
        if result.candles[-1].close_time != _latest_closed_boundary(
            as_of, interval_minutes
        ):
            raise ProviderError(
                "Independent feeds do not contain the latest completed UTC window",
                code="CONSENSUS_LATEST_WINDOW_MISSING",
                evidence=_failure_evidence(
                    self.providers,
                    by_source,
                    self.policy,
                    consensus_overlap_count=result.overlap_count,
                    expected_latest_close=_latest_closed_boundary(
                        as_of, interval_minutes
                    ).isoformat(),
                    actual_latest_close=result.candles[-1].close_time.isoformat(),
                ),
            )
        consensus = tuple(
            Candle(
                symbol=symbol,
                interval_minutes=interval_minutes,
                open_time=item.open_time,
                close_time=item.close_time,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.normalized_volume,
                source=self.source_id,
                available_at=item.available_at,
                ingested_at=item.ingested_at,
            )
            for item in result.candles
        )
        used_intervals = {
            (item.open_time, item.close_time) for item in result.candles
        }
        raw_inputs = tuple(
            sorted(
                (
                    item
                    for candles in by_source.values()
                    for item in candles
                    if (item.open_time, item.close_time) in used_intervals
                ),
                key=lambda item: (item.source, item.open_time, item.close_time),
            )
        )
        close_divergences = result.close_divergences_bps
        ohlc_divergences = result.ohlc_divergences_bps
        return ProviderBatch(
            candles=consensus,
            input_candles=raw_inputs,
            sources=tuple(
                {
                    "id": source_id,
                    "kind": "market_data",
                    "trust": "independent_untrusted_external_data",
                }
                for source_id in result.source_ids
            ),
            metadata={
                "consensus_passed": True,
                "consensus_version": self.source_id,
                "consensus_policy_hash": self.policy.fingerprint(),
                "consensus_source_ids": list(result.source_ids),
                "consensus_source_configurations": _provider_configuration_metadata(
                    self.providers
                ),
                "consensus_source_counts": dict(result.source_counts),
                "consensus_overlap_count": result.overlap_count,
                "consensus_window_size": CONSENSUS_WINDOW_SIZE,
                "consensus_max_close_divergence_bps": max(close_divergences),
                "consensus_mean_close_divergence_bps": (
                    sum(close_divergences) / len(close_divergences)
                ),
                "consensus_latest_close_divergence_bps": close_divergences[-1],
                "consensus_divergent_candle_fraction": 0.0,
                "consensus_threshold_bps": parameters.max_close_divergence_bps,
                "consensus_max_ohlc_divergence_bps": max(ohlc_divergences),
                "consensus_latest_ohlc_divergence_bps": ohlc_divergences[-1],
                "consensus_ohlc_divergent_candle_fraction": 0.0,
                "consensus_ohlc_threshold_bps": parameters.max_ohlc_divergence_bps,
                "consensus_volume_zscores": dict(result.volume_zscores),
                "consensus_volume_zscore_delta": result.volume_zscore_delta,
                "consensus_volume_scales": dict(result.volume_scales),
            },
        )


def _validate_source_series(
    candles: list[Candle],
    *,
    source_id: str,
    symbol: str,
    interval_minutes: int,
    as_of: datetime,
    max_market_price: float,
    max_base_volume: float,
) -> None:
    if not candles:
        raise ProviderError("Consensus source returned no data", code="EMPTY_SOURCE_DATA")
    expected_delta = timedelta(minutes=interval_minutes)
    seen: set[tuple[datetime, datetime]] = set()
    for candle in candles:
        if not isinstance(candle, Candle):
            raise ProviderError(
                "Consensus source returned a malformed candle",
                code="SOURCE_VALUE_INVALID",
            )
        timestamps = (
            candle.open_time,
            candle.close_time,
            candle.available_at,
            candle.ingested_at,
        )
        values = (candle.open, candle.high, candle.low, candle.close, candle.volume)
        if candle.source != source_id or candle.symbol != symbol:
            raise ProviderError(
                "Consensus source returned a mismatched instrument",
                code="SOURCE_IDENTITY_MISMATCH",
            )
        if candle.interval_minutes != interval_minutes:
            raise ProviderError(
                "Consensus source returned a mismatched interval",
                code="SOURCE_INTERVAL_MISMATCH",
            )
        if any(
            timestamp.tzinfo is None or timestamp.utcoffset() != timedelta(0)
            for timestamp in timestamps
        ):
            raise ProviderError(
                "Consensus source returned a non-UTC timestamp",
                code="SOURCE_TIME_INVALID",
            )
        if (
            candle.close_time - candle.open_time != expected_delta
            or candle.close_time > as_of
            or not candle.close_time <= candle.available_at <= candle.ingested_at
        ):
            raise ProviderError(
                "Consensus source returned invalid time lineage",
                code="SOURCE_TIME_INVALID",
            )
        if not all(
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(value)
            for value in values
        ):
            raise ProviderError(
                "Consensus source returned non-finite market data",
                code="SOURCE_VALUE_INVALID",
            )
        if (
            min(candle.open, candle.high, candle.low, candle.close) <= 0
            or candle.volume < 0
            or max(candle.open, candle.high, candle.low, candle.close)
            > max_market_price
            or candle.volume > max_base_volume
            or not (
                candle.low <= candle.open <= candle.high
                and candle.low <= candle.close <= candle.high
            )
        ):
            raise ProviderError(
                "Consensus source returned invalid market values",
                code="SOURCE_VALUE_INVALID",
            )
        key = (candle.open_time, candle.close_time)
        if key in seen:
            raise ProviderError(
                "Consensus source returned duplicate intervals",
                code="SOURCE_DUPLICATE_INTERVAL",
            )
        seen.add(key)


def _failure_evidence(
    providers: tuple[CandleProvider, ...],
    by_source: dict[str, list[Candle]],
    policy: RiskPolicy,
    **details: object,
) -> ProviderFailureEvidence:
    source_ids = tuple(provider.source_id for provider in providers)
    raw_inputs = tuple(
        sorted(
            (
                item
                for candles in by_source.values()
                for item in candles
                if isinstance(item, Candle)
            ),
            key=lambda item: (
                str(item.source),
                str(item.open_time),
                str(item.close_time),
            ),
        )
    )
    metadata: dict[str, object] = {
        "consensus_passed": False,
        "consensus_version": CONSENSUS_ALGORITHM_VERSION,
        "consensus_window_size": CONSENSUS_WINDOW_SIZE,
        "consensus_policy_hash": policy.fingerprint(),
        "consensus_source_ids": list(source_ids),
        "consensus_source_configurations": _provider_configuration_metadata(providers),
        "consensus_source_counts": {
            source_id: len(by_source.get(source_id, ())) for source_id in source_ids
        },
        **details,
    }
    return ProviderFailureEvidence(
        input_candles=raw_inputs,
        sources=tuple(
            {
                "id": source_id,
                "kind": "market_data",
                "trust": "independent_untrusted_external_data",
            }
            for source_id in source_ids
        ),
        metadata=metadata,
    )


def _latest_closed_boundary(as_of: datetime, interval_minutes: int) -> datetime:
    if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
        raise ProviderError(
            "Consensus cutoff must be timezone-aware UTC",
            code="SOURCE_TIME_INVALID",
        )
    # Kraken's native 10080-minute interval and Coinbase's resampled weekly
    # buckets are both anchored to the Unix epoch (Thursday 00:00 UTC).
    anchor = datetime(1970, 1, 1, tzinfo=as_of.tzinfo)
    step_seconds = interval_minutes * 60
    elapsed_seconds = int((as_of - anchor).total_seconds())
    return anchor + timedelta(seconds=(elapsed_seconds // step_seconds) * step_seconds)

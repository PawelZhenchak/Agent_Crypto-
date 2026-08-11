from __future__ import annotations

import math
import urllib.request as _urllib_request
from dataclasses import dataclass
from datetime import datetime, timedelta
from types import FunctionType, ModuleType
from typing import Callable

from ..consensus_math import (
    CONSENSUS_ALGORITHM_VERSION,
    CONSENSUS_WINDOW_SIZE,
    ConsensusInput,
    ConsensusMathError,
    ConsensusPolicyParameters,
    build_cross_exchange_consensus,
)
from ..deadline import AnalysisDeadlineExceeded, ensure_analysis_deadline
from ..domain import Candle, ReferencePriceObservation, ReferencePriceSnapshot
from ..policy import RiskPolicy
from ..reference_price import (
    REFERENCE_PRICE_ALGORITHM_VERSION,
    ReferencePriceInput,
    ReferencePriceMathError,
    ReferencePriceResult,
    build_reference_price_snapshot,
)
from . import coinbase as _coinbase_module
from . import kraken as _kraken_module
from .base import CandleProvider, ProviderBatch, ProviderError, ProviderFailureEvidence
from .coinbase import CoinbaseExchangePublicProvider
from .kraken import KrakenPublicProvider


def _function_signature(function: FunctionType) -> tuple[object, ...]:
    closure: list[object] = []
    for cell in function.__closure__ or ():
        try:
            closure.append(_deep_freeze(cell.cell_contents))
        except ValueError:
            closure.append(("empty_cell",))
    return (
        function,
        function.__code__,
        _deep_freeze(function.__defaults__),
        _deep_freeze(function.__kwdefaults__),
        _deep_freeze(function.__annotations__),
        _deep_freeze(function.__dict__),
        tuple(closure),
        function.__module__,
        function.__qualname__,
    )


def _descriptor_signature(descriptor: object) -> object:
    if type(descriptor) is FunctionType:
        return ("function", _function_signature(descriptor))
    if type(descriptor) is staticmethod:
        return ("staticmethod", _function_signature(descriptor.__func__))
    if type(descriptor) is classmethod:
        return ("classmethod", _function_signature(descriptor.__func__))
    if type(descriptor) is property:
        return (
            "property",
            _descriptor_signature(descriptor.fget),
            _descriptor_signature(descriptor.fset),
            _descriptor_signature(descriptor.fdel),
            _deep_freeze(descriptor.__doc__),
        )
    if descriptor is None:
        return ("none",)
    return ("unsupported_descriptor", type(descriptor), id(descriptor))


def _class_implementation_signature(
    provider_type: type[object],
) -> tuple[tuple[str, object], ...]:
    """Snapshot code, defaults and closures instead of only function identity."""

    descriptor_types = (FunctionType, staticmethod, classmethod, property)
    return tuple(
        (name, _descriptor_signature(descriptor))
        for name, descriptor in vars(provider_type).items()
        if type(descriptor) in descriptor_types
    )


_PROVIDER_STRUCTURE_ATTRIBUTES = {
    KrakenPublicProvider: (
        "source_id",
        "_pairs",
        "_result_keys",
        "_max_response_bytes",
        "_interval_specs",
    ),
    CoinbaseExchangePublicProvider: (
        "source_id",
        "_products",
        "_interval_specs",
        "_source_intervals_per_request",
        "_max_source_candles_per_response",
        "_max_target_candles",
        "_max_response_bytes",
        "_utc_epoch",
    ),
}


def _deep_freeze(value: object) -> object:
    if type(value) is dict:
        items = tuple(
            sorted(
                (
                    (_deep_freeze(key), _deep_freeze(item))
                    for key, item in dict.items(value)
                ),
                key=repr,
            )
        )
        return ("mapping", items)
    if type(value) in {set, frozenset}:
        return (
            type(value).__name__,
            tuple(sorted((_deep_freeze(item) for item in value), key=repr)),
        )
    if type(value) is tuple:
        return ("tuple", tuple(_deep_freeze(item) for item in value))
    if type(value) is datetime:
        return ("datetime", value.isoformat())
    if value is None or type(value) in {bool, int, float, str}:
        return (type(value).__name__, value)
    return (
        "unsupported",
        type(value).__module__,
        type(value).__qualname__,
        id(value),
    )


def _dependency_signature(value: object) -> object:
    if type(value) is FunctionType:
        return ("function_dependency", _function_signature(value))
    if type(value) is type:
        return (
            "type_dependency",
            value,
            tuple(value.__bases__),
            _class_implementation_signature(value),
        )
    return ("object_dependency", _deep_freeze(value))


def _transport_signature(module: ModuleType) -> tuple[tuple[str, object], ...]:
    # Defense in depth for the local dispatch chain and the mutable stdlib opener
    # classes it constructs. This is not a sandbox for arbitrary in-process code;
    # the deployment boundary for untrusted code must remain a separate worker.
    local = tuple(
        (name, _dependency_signature(getattr(module, name, None)))
        for name in ("urlopen", "build_opener", "_NoRedirectHandler")
    )
    stdlib = tuple(
        (
            f"urllib.request.{name}",
            _dependency_signature(getattr(_urllib_request, name, None)),
        )
        for name in (
            "OpenerDirector",
            "Request",
            "ProxyHandler",
            "UnknownHandler",
            "HTTPHandler",
            "HTTPDefaultErrorHandler",
            "HTTPRedirectHandler",
            "FTPHandler",
            "FileHandler",
            "HTTPErrorProcessor",
            "HTTPSHandler",
        )
    )
    return (*local, *stdlib)


def _class_structural_signature(
    provider_type: type[object],
) -> tuple[tuple[str, object], ...]:
    names = _PROVIDER_STRUCTURE_ATTRIBUTES.get(provider_type, ())
    return tuple(
        (name, _deep_freeze(getattr(provider_type, name, None))) for name in names
    )


@dataclass(frozen=True, slots=True)
class _ApprovedProviderSpec:
    provider_type: type[object]
    source_id: str
    venue_id: str
    origin: str
    fetch_candles: Callable[..., list[Candle]]
    fetch_reference_price: Callable[..., ReferencePriceObservation]
    transport_module: ModuleType
    transport: object
    transport_signature: tuple[tuple[str, object], ...]
    implementation_signature: tuple[tuple[str, object], ...]
    structural_signature: tuple[tuple[str, object], ...]


_APPROVED_PROVIDER_SPECS = (
    _ApprovedProviderSpec(
        provider_type=KrakenPublicProvider,
        source_id=KrakenPublicProvider.source_id,
        venue_id="kraken",
        origin="https://api.kraken.com",
        fetch_candles=KrakenPublicProvider.fetch_candles,
        fetch_reference_price=KrakenPublicProvider.fetch_reference_price,
        transport_module=_kraken_module,
        transport=_kraken_module.urlopen,
        transport_signature=_transport_signature(_kraken_module),
        implementation_signature=_class_implementation_signature(KrakenPublicProvider),
        structural_signature=_class_structural_signature(KrakenPublicProvider),
    ),
    _ApprovedProviderSpec(
        provider_type=CoinbaseExchangePublicProvider,
        source_id=CoinbaseExchangePublicProvider.source_id,
        venue_id="coinbase",
        origin="https://api.exchange.coinbase.com",
        fetch_candles=CoinbaseExchangePublicProvider.fetch_candles,
        fetch_reference_price=CoinbaseExchangePublicProvider.fetch_reference_price,
        transport_module=_coinbase_module,
        transport=_coinbase_module.urlopen,
        transport_signature=_transport_signature(_coinbase_module),
        implementation_signature=_class_implementation_signature(
            CoinbaseExchangePublicProvider
        ),
        structural_signature=_class_structural_signature(
            CoinbaseExchangePublicProvider
        ),
    ),
)


def _approved_provider_spec(provider: CandleProvider) -> _ApprovedProviderSpec | None:
    return next(
        (
            spec
            for spec in _APPROVED_PROVIDER_SPECS
            if type(provider) is spec.provider_type
        ),
        None,
    )


def _instance_fetches_are_unshadowed(provider: CandleProvider) -> bool:
    instance_state = getattr(provider, "__dict__", None)
    return type(instance_state) is not dict or not {
        "fetch_candles",
        "fetch_reference_price",
    }.intersection(instance_state)


def _provider_integrity_signature(provider: CandleProvider) -> tuple[object, ...]:
    spec = _approved_provider_spec(provider)
    current_transport_signature = (
        _transport_signature(spec.transport_module) if spec is not None else None
    )
    return (
        _class_implementation_signature(type(provider)),
        _class_structural_signature(type(provider)),
        current_transport_signature,
        _instance_fetches_are_unshadowed(provider),
    )


def _provider_implementation_is_trusted(provider: CandleProvider) -> bool:
    spec = _approved_provider_spec(provider)
    return bool(
        spec is not None
        and provider.source_id == spec.source_id
        and _class_implementation_signature(type(provider))
        == spec.implementation_signature
        and _class_structural_signature(type(provider)) == spec.structural_signature
        and getattr(spec.transport_module, "urlopen", None) is spec.transport
        and _transport_signature(spec.transport_module) == spec.transport_signature
        and _instance_fetches_are_unshadowed(provider)
    )


def _composition_signature(
    providers: tuple[CandleProvider, ...],
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (
            type(provider),
            provider.source_id,
            _provider_origin(provider),
            _provider_timeout(provider),
            getattr(provider, "_configuration_sealed", False) is True,
            _provider_integrity_signature(provider),
        )
        for provider in providers
    )


def _provider_origin(provider: CandleProvider) -> str | None:
    value = getattr(provider, "base_url", None)
    return value if type(value) is str else None


def _provider_timeout(provider: CandleProvider) -> float | None:
    value = getattr(provider, "timeout_seconds", None)
    if type(value) is not float:
        return None
    return float(value) if math.isfinite(value) else None


def _provider_configuration_metadata(
    providers: tuple[CandleProvider, ...],
) -> dict[str, dict[str, object]]:
    return {
        provider.source_id: {
            "venue_id": (
                spec.venue_id
                if (spec := _approved_provider_spec(provider)) is not None
                else None
            ),
            "origin": _provider_origin(provider),
            "timeout_seconds": _provider_timeout(provider),
            "sealed": getattr(provider, "_configuration_sealed", False) is True,
            "implementation_attested": _provider_implementation_is_trusted(provider),
        }
        for provider in providers
    }


def _is_approved_composition(providers: tuple[CandleProvider, ...]) -> bool:
    if not (
        len(providers) == 2
        and {type(provider) for provider in providers}
        == {KrakenPublicProvider, CoinbaseExchangePublicProvider}
        and {provider.source_id for provider in providers}
        == {spec.source_id for spec in _APPROVED_PROVIDER_SPECS}
    ):
        return False
    return all(
        (spec := _approved_provider_spec(provider)) is not None
        and _provider_origin(provider) == spec.origin
        and _provider_timeout(provider) is not None
        and getattr(provider, "_configuration_sealed", False) is True
        and _provider_implementation_is_trusted(provider)
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
        if policy.policy_schema_version != 2:
            raise ValueError(
                "Live consensus requires policy_schema_version=2; legacy policies "
                "are replay-only"
            )
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
                and _consensus_implementation_is_trusted(self)
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
            implementation_matches = bool(
                not self._initially_approved
                or _consensus_implementation_is_trusted(self)
            )
            testing_mode_valid = bool(
                self._initially_approved or self._allow_unapproved_for_testing
            )
        except Exception:
            policy_matches = False
            composition_matches = False
            approval_matches = False
            implementation_matches = False
            testing_mode_valid = False
        if not all(
            (
                policy_matches,
                composition_matches,
                approval_matches,
                implementation_matches,
                testing_mode_valid,
            )
        ):
            raise ProviderError(
                "Consensus provider configuration failed integrity validation",
                code="CONSENSUS_CONFIGURATION_TAMPERED",
            )

    def _fetch_provider_candles(
        self,
        provider: CandleProvider,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> list[Candle]:
        if not self._initially_approved:
            return provider.fetch_candles(
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=as_of,
                limit=limit,
            )
        spec = _approved_provider_spec(provider)
        if spec is None or not _provider_implementation_is_trusted(provider):
            raise ProviderError(
                "Consensus provider implementation failed integrity validation",
                code="CONSENSUS_CONFIGURATION_TAMPERED",
            )
        # Dispatch through the function object captured before any runtime mutation.
        return spec.fetch_candles(
            provider,
            symbol=symbol,
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
        )

    def _fetch_provider_reference_price(
        self,
        provider: CandleProvider,
        *,
        symbol: str,
        as_of: datetime,
    ) -> ReferencePriceObservation | None:
        # The explicitly unapproved offline seam remains candle-only so legacy
        # shape tests cannot accidentally mint production reference evidence.
        if not self._initially_approved:
            return None
        spec = _approved_provider_spec(provider)
        if spec is None or not _provider_implementation_is_trusted(provider):
            raise ProviderError(
                "Consensus provider implementation failed integrity validation",
                code="CONSENSUS_CONFIGURATION_TAMPERED",
            )
        # Dispatch through the second function object captured at module import.
        return spec.fetch_reference_price(
            provider,
            symbol=symbol,
            as_of=as_of,
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
        reference_by_source: dict[str, ReferencePriceObservation] = {}
        for provider in self.providers:
            self._validate_sealed_configuration()
            ensure_analysis_deadline()
            try:
                candles = self._fetch_provider_candles(
                    provider,
                    symbol=symbol,
                    interval_minutes=interval_minutes,
                    as_of=as_of,
                    limit=fetch_limit,
                )
            except AnalysisDeadlineExceeded:
                self._validate_sealed_configuration()
                raise
            except ProviderError as exc:
                self._validate_sealed_configuration()
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
                self._validate_sealed_configuration()
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
            self._validate_sealed_configuration()
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
            if self._initially_approved:
                self._validate_sealed_configuration()
                ensure_analysis_deadline()
                try:
                    observation = self._fetch_provider_reference_price(
                        provider,
                        symbol=symbol,
                        as_of=as_of,
                    )
                except AnalysisDeadlineExceeded:
                    self._validate_sealed_configuration()
                    raise
                except ProviderError as exc:
                    self._validate_sealed_configuration()
                    raise ProviderError(
                        f"Required reference-price source unavailable: "
                        f"{provider.source_id}",
                        code="REFERENCE_PRICE_SOURCE_UNAVAILABLE",
                        evidence=_failure_evidence(
                            self.providers,
                            by_source,
                            self.policy,
                            reference_by_source=reference_by_source,
                            failed_source_id=provider.source_id,
                            upstream_error_code=exc.code,
                        ),
                    ) from exc
                except Exception as exc:
                    self._validate_sealed_configuration()
                    raise ProviderError(
                        f"Required reference-price source failed safely: "
                        f"{provider.source_id}",
                        code="REFERENCE_PRICE_SOURCE_UNAVAILABLE",
                        evidence=_failure_evidence(
                            self.providers,
                            by_source,
                            self.policy,
                            reference_by_source=reference_by_source,
                            failed_source_id=provider.source_id,
                            upstream_error_code="PROVIDER_INTERNAL_ERROR",
                        ),
                    ) from exc
                self._validate_sealed_configuration()
                if not isinstance(observation, ReferencePriceObservation):
                    raise ProviderError(
                        "Reference-price provider returned a malformed observation",
                        code="REFERENCE_PRICE_PROVENANCE_INVALID",
                        evidence=_failure_evidence(
                            self.providers,
                            by_source,
                            self.policy,
                            reference_by_source=reference_by_source,
                            failed_source_id=provider.source_id,
                        ),
                    )
                reference_by_source[provider.source_id] = observation

        reference_price: ReferencePriceSnapshot | None = None
        reference_result = None
        if self._initially_approved:
            try:
                reference_price, reference_result = _build_reference_price_envelope(
                    self.providers,
                    reference_by_source,
                    symbol=symbol,
                    as_of=as_of,
                    policy=self.policy,
                )
            except ReferencePriceMathError as exc:
                raise ProviderError(
                    str(exc),
                    code=exc.code,
                    evidence=_failure_evidence(
                        self.providers,
                        by_source,
                        self.policy,
                        reference_by_source=reference_by_source,
                        **exc.details,
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
            max_close_divergence_bps=(
                self.policy.max_pairwise_reference_price_divergence_bps
            ),
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
                    reference_by_source=reference_by_source,
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
                    reference_by_source=reference_by_source,
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
                    reference_by_source=reference_by_source,
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
                **(
                    _reference_price_metadata(reference_price, reference_result)
                    if reference_price is not None and reference_result is not None
                    else {"reference_price_present": False}
                ),
            },
            reference_price=reference_price,
        )


def _build_reference_price_envelope(
    providers: tuple[CandleProvider, ...],
    reference_by_source: dict[str, ReferencePriceObservation],
    *,
    symbol: str,
    as_of: datetime,
    policy: RiskPolicy,
) -> tuple[ReferencePriceSnapshot, ReferencePriceResult]:
    observations = tuple(
        reference_by_source[provider.source_id] for provider in providers
    )
    source_venues = {
        spec.source_id: spec.venue_id for spec in _APPROVED_PROVIDER_SPECS
    }
    inputs: list[ReferencePriceInput] = []
    for provider, observation in zip(providers, observations, strict=True):
        spec = _approved_provider_spec(provider)
        if spec is None:
            raise ReferencePriceMathError(
                "Reference-price observation has an unapproved source",
                code="REFERENCE_PRICE_PROVENANCE_INVALID",
                details={"source_id": observation.source},
            )
        if observation.source != provider.source_id:
            raise ReferencePriceMathError(
                "Reference-price observation does not match its sealed source",
                code="REFERENCE_PRICE_PROVENANCE_INVALID",
                details={
                    "expected_source_id": provider.source_id,
                    "actual_source_id": observation.source,
                },
            )
        if observation.symbol != symbol:
            raise ReferencePriceMathError(
                "Reference-price observation does not match the requested symbol",
                code="REFERENCE_PRICE_SCOPE_INVALID",
                details={
                    "expected_symbol": symbol,
                    "actual_symbol": observation.symbol,
                },
            )
        inputs.append(
            ReferencePriceInput(
                source_id=observation.source,
                venue_id=spec.venue_id,
                symbol=observation.symbol,
                open_time=observation.event_time - timedelta(minutes=1),
                close_time=observation.event_time,
                price=observation.price,
                available_at=observation.available_at,
                ingested_at=observation.ingested_at,
            )
        )
    evaluated_at = max(as_of, *(item.ingested_at for item in observations))
    result = build_reference_price_snapshot(
        inputs,
        cutoff_as_of=as_of,
        evaluated_at=evaluated_at,
        max_age_seconds=policy.max_reference_price_age_seconds,
        max_deviation_from_median_bps=(
            policy.max_reference_price_deviation_from_median_bps
        ),
        max_market_price=policy.max_market_price,
        approved_source_venues=source_venues,
    )
    if result.symbol != symbol:
        raise ReferencePriceMathError(
            "Reference-price result does not match the requested symbol",
            code="REFERENCE_PRICE_SCOPE_INVALID",
            details={"expected_symbol": symbol, "actual_symbol": result.symbol},
        )
    return (
        ReferencePriceSnapshot(symbol=symbol, observations=observations),
        result,
    )


def _reference_price_metadata(
    snapshot: ReferencePriceSnapshot,
    result: ReferencePriceResult,
) -> dict[str, object]:
    return {
        "reference_price_present": True,
        "reference_price_version": REFERENCE_PRICE_ALGORITHM_VERSION,
        "reference_price_symbol": result.symbol,
        "reference_price": str(result.median_price),
        "reference_price_event_time": result.event_time.isoformat(),
        "reference_price_available_at": result.available_at.isoformat(),
        "reference_price_ingested_at": result.ingested_at.isoformat(),
        "reference_price_source_ids": list(result.source_ids),
        "reference_price_venue_ids": list(result.venue_ids),
        "reference_price_pairwise_divergence_bps": str(
            result.pairwise_divergence_bps
        ),
        "reference_price_max_deviation_from_median_bps": str(
            result.max_deviation_from_median_bps
        ),
        "reference_price_observations": [
            {
                "symbol": item.symbol,
                "price": item.price,
                "event_time": item.event_time.isoformat(),
                "available_at": item.available_at.isoformat(),
                "ingested_at": item.ingested_at.isoformat(),
                "source": item.source,
            }
            for item in snapshot.observations
        ],
    }


_TRUSTED_CONSENSUS_IMPLEMENTATION_SIGNATURE = _class_implementation_signature(
    CrossExchangeConsensusProvider
)


def _consensus_implementation_is_trusted(
    provider: CrossExchangeConsensusProvider,
) -> bool:
    return bool(
        type(provider) is CrossExchangeConsensusProvider
        and _class_implementation_signature(CrossExchangeConsensusProvider)
        == _TRUSTED_CONSENSUS_IMPLEMENTATION_SIGNATURE
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
    reference_by_source: dict[str, ReferencePriceObservation] | None = None,
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
    reference_observations = tuple(
        reference_by_source[provider.source_id]
        for provider in providers
        if reference_by_source is not None
        and isinstance(
            reference_by_source.get(provider.source_id),
            ReferencePriceObservation,
        )
    )
    reference_price = (
        ReferencePriceSnapshot(
            symbol=reference_observations[0].symbol,
            observations=reference_observations,
        )
        if reference_observations
        else None
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
        "reference_price_present": False,
        "reference_price_partial_observations": [
            {
                "symbol": item.symbol,
                "price": item.price,
                "event_time": item.event_time.isoformat(),
                "available_at": item.available_at.isoformat(),
                "ingested_at": item.ingested_at.isoformat(),
                "source": item.source,
            }
            for item in (reference_by_source or {}).values()
            if isinstance(item, ReferencePriceObservation)
        ],
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
        reference_price=reference_price,
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

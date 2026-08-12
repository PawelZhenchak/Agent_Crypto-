from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol

from ..deadline import AnalysisDeadlineExceeded
from ..domain import Candle, ReferencePriceObservation, ReferencePriceSnapshot


@dataclass(frozen=True, slots=True)
class ProviderFailureEvidence:
    """Safe, immutable envelope retained when a provider fails closed."""

    input_candles: tuple[Candle, ...] = ()
    sources: tuple[dict[str, str], ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)
    reference_price: ReferencePriceSnapshot | None = None


class ProviderError(RuntimeError):
    """A fail-closed, machine-classifiable public market-data error."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "PROVIDER_UNAVAILABLE",
        evidence: ProviderFailureEvidence | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.evidence = evidence


@dataclass(frozen=True, slots=True)
class ProviderBatch:
    """One immutable provider result plus the exact inputs used to produce it."""

    candles: tuple[Candle, ...]
    input_candles: tuple[Candle, ...]
    sources: tuple[dict[str, str], ...]
    metadata: dict[str, Any] = field(default_factory=dict)
    reference_price: ReferencePriceSnapshot | None = None
    raw_payload: bytes | None = field(default=None, repr=False)
    raw_payload_sha256: str | None = None


class CandleProvider(Protocol):
    source_id: str

    def fetch_candles(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> list[Candle]: ...


class ReferencePriceProvider(CandleProvider, Protocol):
    def fetch_reference_price(
        self,
        *,
        symbol: str,
        as_of: datetime,
    ) -> ReferencePriceObservation: ...


class BatchCandleProvider(CandleProvider, Protocol):
    def fetch_batch(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> ProviderBatch: ...


def fetch_provider_batch(
    provider: CandleProvider,
    *,
    symbol: str,
    interval_minutes: int,
    as_of: datetime,
    limit: int,
) -> ProviderBatch:
    """Fetch a rich batch when supported, otherwise wrap the legacy candle contract."""

    try:
        fetch_batch = getattr(provider, "fetch_batch", None)
        if callable(fetch_batch):
            batch = fetch_batch(
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=as_of,
                limit=limit,
            )
            if not isinstance(batch, ProviderBatch):
                raise ProviderError(
                    "Provider returned an invalid batch", code="INVALID_PROVIDER_BATCH"
                )
            return batch

        candles = tuple(
            provider.fetch_candles(
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=as_of,
                limit=limit,
            )
        )
    except (ProviderError, AnalysisDeadlineExceeded):
        raise
    except Exception as exc:
        raise ProviderError(
            "Market-data provider failed safely", code="PROVIDER_INTERNAL_ERROR"
        ) from exc
    return ProviderBatch(
        candles=candles,
        input_candles=candles,
        sources=(
            {
                "id": provider.source_id,
                "kind": "market_data",
                "trust": "untrusted_external_data",
            },
        ),
    )

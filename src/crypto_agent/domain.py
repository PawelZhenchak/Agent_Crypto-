from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from statistics import median
from typing import Any


class Decision(StrEnum):
    """The only decisions V1 is permitted to emit."""

    NO_SIGNAL = "NO_SIGNAL"
    ALERT = "ALERT"


class Regime(StrEnum):
    UNKNOWN = "UNKNOWN"
    RANGE = "RANGE"
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    HIGH_VOLATILITY = "HIGH_VOLATILITY"


@dataclass(frozen=True, slots=True)
class Candle:
    symbol: str
    interval_minutes: int
    open_time: datetime
    close_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    available_at: datetime
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class ReferencePriceObservation:
    """One venue's price from its latest completed one-minute candle."""

    symbol: str
    price: float
    event_time: datetime
    available_at: datetime
    ingested_at: datetime
    source: str


@dataclass(frozen=True, slots=True)
class ReferencePriceSnapshot:
    """Immutable multi-venue reference-price evidence used by the risk gate."""

    symbol: str
    observations: tuple[ReferencePriceObservation, ...]

    @property
    def price(self) -> float:
        if not self.observations:
            raise ValueError("A reference-price snapshot requires observations")
        return float(median(item.price for item in self.observations))

    @property
    def event_time(self) -> datetime:
        if not self.observations:
            raise ValueError("A reference-price snapshot requires observations")
        return min(item.event_time for item in self.observations)

    @property
    def available_at(self) -> datetime:
        if not self.observations:
            raise ValueError("A reference-price snapshot requires observations")
        return max(item.available_at for item in self.observations)

    @property
    def ingested_at(self) -> datetime:
        if not self.observations:
            raise ValueError("A reference-price snapshot requires observations")
        return max(item.ingested_at for item in self.observations)


@dataclass(frozen=True, slots=True)
class DataQualityReport:
    score: float
    sample_count: int
    flags: tuple[str, ...]
    critical_flags: tuple[str, ...]
    newest_observed_at: datetime | None
    newest_available_at: datetime | None

    @property
    def passed(self) -> bool:
        return not self.critical_flags


@dataclass(frozen=True, slots=True)
class MarketMetrics:
    last_price: float
    period_return: float
    return_7_periods: float
    annualized_volatility: float
    max_drawdown: float
    sma_20: float
    sma_50: float
    volume_zscore: float
    regime: Regime


@dataclass(frozen=True, slots=True)
class RiskAssessment:
    assessment_id: str
    policy_id: str
    policy_hash: str
    as_of: datetime
    expires_at: datetime
    input_fingerprint_sha256: str
    decision: Decision
    vetoed: bool
    flags: tuple[str, ...]
    reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResearchReport:
    decision_id: str
    trace_id: str
    as_of: datetime
    expires_at: datetime
    asset_id: str
    instrument_id: str
    horizon: str
    decision: Decision
    reason_codes: tuple[str, ...]
    regime_probabilities: tuple[dict[str, Any], ...]
    model_version: str
    policy_version: str
    data_snapshot_id: str
    thesis: str
    counter_evidence: tuple[str, ...]
    scenarios: tuple[dict[str, Any], ...]
    invalidation_conditions: tuple[str, ...]
    data_quality: DataQualityReport
    risk: RiskAssessment
    sources: tuple[dict[str, str], ...]
    metrics: MarketMetrics | None
    narrative: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _serialize(asdict(self))


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, StrEnum):
        return value.value
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize(item) for item in value]
    return value

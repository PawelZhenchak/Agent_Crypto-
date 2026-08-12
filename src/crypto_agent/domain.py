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


class MetricStatus(StrEnum):
    """Availability of one futures-specific metric.

    Missing evidence is never represented by a numeric zero.  ``INVALID`` means
    evidence was present but failed validation, while ``NOT_APPLICABLE`` is used
    only when a metric has no meaning for the current contract state (for example
    roll impact before the first controlled roll).
    """

    AVAILABLE = "AVAILABLE"
    UNAVAILABLE = "UNAVAILABLE"
    INVALID = "INVALID"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class SessionStatus(StrEnum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    HALTED = "HALTED"


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
class OrderBookLevel:
    level: int
    price: float
    quantity: float


@dataclass(frozen=True, slots=True)
class BasisReference:
    """Explicit spot/index observation used for futures basis.

    The existing T4 ``reference_price`` is intentionally not reused here: it is
    an execution-safety observation for the futures market and is not attested as
    a spot or index price.
    """

    symbol: str
    reference_type: str
    source: str
    price: float
    observed_at: datetime
    available_at: datetime
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class ContractTransitionEvidence:
    from_contract_id: str
    to_contract_id: str
    price_type: str
    from_price: float
    to_price: float
    source: str
    observed_at: datetime
    available_at: datetime
    ingested_at: datetime


@dataclass(frozen=True, slots=True)
class FuturesEvidence:
    """Immutable microstructure evidence attached to one exact T4 batch."""

    contract_id: str
    source: str
    session_status: SessionStatus
    is_full_snapshot: bool
    observed_at: datetime
    available_at: datetime
    ingested_at: datetime
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    basis_reference: BasisReference
    contract_transition: ContractTransitionEvidence | None = None


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
class FuturesMetrics:
    volume_status: MetricStatus
    volume_zscore: float | None
    relative_volume: float | None
    spread_status: MetricStatus
    spread_bps: float | None
    depth_status: MetricStatus
    bid_depth: float | None
    ask_depth: float | None
    book_imbalance: float | None
    basis_status: MetricStatus
    basis_bps: float | None
    annualized_basis: float | None
    lifecycle_status: MetricStatus
    seconds_to_roll: float | None
    seconds_to_expiry: float | None
    expiry_risk: str | None
    roll_impact_status: MetricStatus
    roll_impact_bps: float | None
    session_status: SessionStatus | None
    flags: tuple[str, ...]


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
    futures_metrics: FuturesMetrics | None = None
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

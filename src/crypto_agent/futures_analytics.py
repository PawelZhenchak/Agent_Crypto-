from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .domain import (
    BasisReference,
    Candle,
    ContractTransitionEvidence,
    FuturesEvidence,
    FuturesMetrics,
    MetricStatus,
    OrderBookLevel,
    SessionStatus,
)
from .futures_policy import (
    APPROVED_BASIS_REFERENCE_TYPE,
    APPROVED_BASIS_SOURCE_ID,
    FuturesAnalysisPolicy,
)

T4_SOURCE_ID = "plus500_t4_futures_v1"
SECONDS_PER_YEAR = 365.0 * 24 * 60 * 60
MAX_EVIDENCE_TIMESTAMP_SKEW_SECONDS = 5


@dataclass(frozen=True, slots=True)
class FuturesAnalysis:
    metrics: FuturesMetrics
    gate_passed: bool
    reason_codes: tuple[str, ...]
    veto_flags: tuple[str, ...]
    counter_evidence: tuple[str, ...]
    invalidation_conditions: tuple[str, ...]


def analyze_futures(
    candles: list[Candle],
    evidence: FuturesEvidence | None,
    *,
    as_of: datetime,
    symbol: str,
    contract_id: object,
    contract_expires_at: object,
    contract_roll_at: object,
    contract_selection: object,
    rolled_from_contract_id: object,
    policy: FuturesAnalysisPolicy,
) -> FuturesAnalysis:
    """Calculate deterministic futures metrics without inventing missing evidence."""

    policy.validate()
    reason_codes: list[str] = []
    veto_flags: list[str] = []
    counter_evidence: list[str] = []

    volume_status, volume_zscore, relative_volume, volume_flags = _volume_metrics(
        candles, policy
    )
    reason_codes.extend(volume_flags)
    if volume_status is not MetricStatus.AVAILABLE:
        veto_flags.extend(volume_flags)
        veto_flags.append("VOLUME_METRICS_UNAVAILABLE")

    lifecycle = _lifecycle_metrics(
        as_of=as_of,
        contract_id=contract_id,
        contract_expires_at=contract_expires_at,
        contract_roll_at=contract_roll_at,
        contract_selection=contract_selection,
        rolled_from_contract_id=rolled_from_contract_id,
        policy=policy,
    )
    reason_codes.extend(lifecycle.flags)
    veto_flags.extend(lifecycle.veto_flags)

    spread_status = MetricStatus.UNAVAILABLE
    spread_bps: float | None = None
    depth_status = MetricStatus.UNAVAILABLE
    bid_depth: float | None = None
    ask_depth: float | None = None
    book_imbalance: float | None = None
    basis_status = MetricStatus.UNAVAILABLE
    basis_bps: float | None = None
    annualized_basis: float | None = None
    roll_impact_status = (
        MetricStatus.NOT_APPLICABLE
        if contract_selection == "front_month"
        else MetricStatus.UNAVAILABLE
    )
    roll_impact_bps: float | None = None
    session_status: SessionStatus | None = None

    if evidence is None:
        missing = [
            "FUTURES_EVIDENCE_INCOMPLETE",
            "ORDER_BOOK_UNAVAILABLE",
            "BASIS_REFERENCE_UNAVAILABLE",
            "SESSION_STATUS_UNAVAILABLE",
        ]
        if contract_selection == "rolled":
            missing.append("ROLL_EVIDENCE_UNAVAILABLE")
        reason_codes.extend(missing)
        veto_flags.extend(missing)
    else:
        snapshot = _snapshot_metrics(
            evidence,
            as_of=as_of,
            symbol=symbol,
            contract_id=contract_id,
            seconds_to_expiry=lifecycle.seconds_to_expiry,
            policy=policy,
        )
        spread_status = snapshot.spread_status
        spread_bps = snapshot.spread_bps
        depth_status = snapshot.depth_status
        bid_depth = snapshot.bid_depth
        ask_depth = snapshot.ask_depth
        book_imbalance = snapshot.book_imbalance
        basis_status = snapshot.basis_status
        basis_bps = snapshot.basis_bps
        annualized_basis = snapshot.annualized_basis
        session_status = snapshot.session_status
        reason_codes.extend(snapshot.flags)
        veto_flags.extend(snapshot.veto_flags)

        transition = _transition_metrics(
            evidence.contract_transition,
            evidence=evidence,
            current_mid=_current_mid(evidence),
            as_of=as_of,
            contract_id=contract_id,
            contract_selection=contract_selection,
            rolled_from_contract_id=rolled_from_contract_id,
            policy=policy,
        )
        roll_impact_status = transition.status
        roll_impact_bps = transition.roll_impact_bps
        reason_codes.extend(transition.flags)
        veto_flags.extend(transition.veto_flags)

    required_statuses = (
        volume_status,
        lifecycle.status,
        spread_status,
        depth_status,
        basis_status,
    )
    if (
        any(status is not MetricStatus.AVAILABLE for status in required_statuses)
        or roll_impact_status not in {
            MetricStatus.AVAILABLE,
            MetricStatus.NOT_APPLICABLE,
        }
        or session_status is not SessionStatus.OPEN
    ):
        reason_codes.append("FUTURES_EVIDENCE_INCOMPLETE")
        veto_flags.append("FUTURES_EVIDENCE_INCOMPLETE")

    if any(
        status is MetricStatus.INVALID
        for status in (
            lifecycle.status,
            spread_status,
            depth_status,
            basis_status,
            roll_impact_status,
        )
    ):
        reason_codes.append("FUTURES_EVIDENCE_INVALID")
        veto_flags.append("FUTURES_EVIDENCE_INVALID")

    reason_codes = list(dict.fromkeys(reason_codes))
    veto_flags = list(dict.fromkeys(veto_flags))
    counter_evidence.extend(
        _futures_counter_evidence(
            spread_status=spread_status,
            spread_bps=spread_bps,
            depth_status=depth_status,
            basis_status=basis_status,
            basis_bps=basis_bps,
            lifecycle=lifecycle,
            session_status=session_status,
            reason_codes=tuple(reason_codes),
        )
    )
    metrics = FuturesMetrics(
        volume_status=volume_status,
        volume_zscore=_rounded(volume_zscore),
        relative_volume=_rounded(relative_volume),
        spread_status=spread_status,
        spread_bps=_rounded(spread_bps),
        depth_status=depth_status,
        bid_depth=_rounded(bid_depth),
        ask_depth=_rounded(ask_depth),
        book_imbalance=_rounded(book_imbalance),
        basis_status=basis_status,
        basis_bps=_rounded(basis_bps),
        annualized_basis=_rounded(annualized_basis),
        lifecycle_status=lifecycle.status,
        seconds_to_roll=_rounded(lifecycle.seconds_to_roll),
        seconds_to_expiry=_rounded(lifecycle.seconds_to_expiry),
        expiry_risk=lifecycle.expiry_risk,
        roll_impact_status=roll_impact_status,
        roll_impact_bps=_rounded(roll_impact_bps),
        session_status=session_status,
        flags=tuple(reason_codes),
    )
    return FuturesAnalysis(
        metrics=metrics,
        gate_passed=not veto_flags,
        reason_codes=tuple(reason_codes),
        veto_flags=tuple(veto_flags),
        counter_evidence=tuple(counter_evidence),
        invalidation_conditions=(
            "NEWER_EVIDENCE_AVAILABLE: pojawiły się dane po analizowanym as_of.",
            "INPUT_FINGERPRINT_CHANGED: zmienił się którykolwiek dowód wejściowy.",
            "CONTRACT_CHANGED_OR_ROLLED: aktywny contract_id uległ zmianie.",
            "AS_OF_REACHES_ROLL_AT: punkt czasu osiągnął granicę rollu.",
            "DATA_QUALITY_VETO: kontrola kompletności lub czasu danych nie przeszła.",
            "REGIME_CHANGED: klasyfikacja reżimu uległa zmianie.",
            "BOOK_STALE_OR_INVALID: order book stał się stary lub niespójny.",
            "BASIS_REFERENCE_STALE_OR_INVALID: referencja spot/index utraciła ważność.",
        ),
    )


@dataclass(frozen=True, slots=True)
class _LifecycleResult:
    status: MetricStatus
    seconds_to_roll: float | None
    seconds_to_expiry: float | None
    expiry_risk: str | None
    flags: tuple[str, ...]
    veto_flags: tuple[str, ...]


def _lifecycle_metrics(
    *,
    as_of: datetime,
    contract_id: object,
    contract_expires_at: object,
    contract_roll_at: object,
    contract_selection: object,
    rolled_from_contract_id: object,
    policy: FuturesAnalysisPolicy,
) -> _LifecycleResult:
    try:
        expires_at = _utc_datetime(contract_expires_at)
        roll_at = _utc_datetime(contract_roll_at)
        valid = bool(
            _is_utc(as_of)
            and isinstance(contract_id, str)
            and bool(contract_id)
            and contract_selection in {"front_month", "rolled"}
            and (
                (contract_selection == "front_month" and rolled_from_contract_id is None)
                or (
                    contract_selection == "rolled"
                    and isinstance(rolled_from_contract_id, str)
                    and bool(rolled_from_contract_id)
                    and rolled_from_contract_id != contract_id
                )
            )
            and as_of < roll_at < expires_at
        )
    except (TypeError, ValueError):
        valid = False
        expires_at = None
        roll_at = None
    if not valid or expires_at is None or roll_at is None:
        return _LifecycleResult(
            MetricStatus.INVALID,
            None,
            None,
            None,
            ("CONTRACT_LIFECYCLE_INVALID",),
            ("CONTRACT_LIFECYCLE_INVALID",),
        )

    seconds_to_roll = (roll_at - as_of).total_seconds()
    seconds_to_expiry = (expires_at - as_of).total_seconds()
    flags: list[str] = []
    veto_flags: list[str] = []
    if seconds_to_expiry <= policy.expiry_high_risk_seconds:
        expiry_risk = "HIGH"
        flags.append("EXPIRY_RISK_HIGH")
        veto_flags.append("EXPIRY_RISK_HIGH")
    elif seconds_to_expiry <= policy.expiry_warning_seconds:
        expiry_risk = "ELEVATED"
        flags.append("EXPIRY_RISK_ELEVATED")
    else:
        expiry_risk = "NORMAL"

    if seconds_to_roll <= policy.roll_high_risk_seconds:
        flags.append("ROLL_WINDOW_TOO_CLOSE")
        veto_flags.append("ROLL_WINDOW_TOO_CLOSE")
    elif seconds_to_roll <= policy.roll_warning_seconds:
        flags.append("ROLL_WINDOW_APPROACHING")

    return _LifecycleResult(
        MetricStatus.AVAILABLE,
        seconds_to_roll,
        seconds_to_expiry,
        expiry_risk,
        tuple(flags),
        tuple(veto_flags),
    )


def _volume_metrics(
    candles: list[Candle], policy: FuturesAnalysisPolicy
) -> tuple[MetricStatus, float | None, float | None, tuple[str, ...]]:
    ordered = sorted(candles, key=lambda item: item.open_time)
    minimum = max(policy.volume_window, policy.relative_volume_window + 1)
    if len(ordered) < minimum:
        return MetricStatus.UNAVAILABLE, None, None, ("VOLUME_HISTORY_INCOMPLETE",)
    volumes = [item.volume for item in ordered]
    if any(not _finite_nonnegative(value) for value in volumes):
        return MetricStatus.INVALID, None, None, ("VOLUME_EVIDENCE_INVALID",)

    zscore_window = volumes[-policy.volume_window :]
    history = zscore_window[:-1]
    try:
        deviation = statistics.pstdev(history)
        history_mean = statistics.fmean(history)
        relative_history = volumes[-(policy.relative_volume_window + 1) : -1]
        relative_mean = statistics.fmean(relative_history)
    except (OverflowError, statistics.StatisticsError):
        return MetricStatus.INVALID, None, None, ("VOLUME_EVIDENCE_INVALID",)
    if not all(math.isfinite(item) for item in (deviation, history_mean, relative_mean)):
        return MetricStatus.INVALID, None, None, ("VOLUME_EVIDENCE_INVALID",)
    if deviation == 0:
        return MetricStatus.UNAVAILABLE, None, None, ("VOLUME_VARIANCE_ZERO",)
    zscore = (zscore_window[-1] - history_mean) / deviation

    if relative_mean <= 0:
        return MetricStatus.UNAVAILABLE, zscore, None, ("VOLUME_BASELINE_ZERO",)
    relative_volume = volumes[-1] / relative_mean
    if not all(math.isfinite(item) for item in (zscore, relative_volume)):
        return MetricStatus.INVALID, None, None, ("VOLUME_EVIDENCE_INVALID",)
    return MetricStatus.AVAILABLE, zscore, relative_volume, ()


@dataclass(frozen=True, slots=True)
class _SnapshotResult:
    spread_status: MetricStatus
    spread_bps: float | None
    depth_status: MetricStatus
    bid_depth: float | None
    ask_depth: float | None
    book_imbalance: float | None
    basis_status: MetricStatus
    basis_bps: float | None
    annualized_basis: float | None
    session_status: SessionStatus | None
    flags: tuple[str, ...]
    veto_flags: tuple[str, ...]


def _snapshot_metrics(
    evidence: FuturesEvidence,
    *,
    as_of: datetime,
    symbol: str,
    contract_id: object,
    seconds_to_expiry: float | None,
    policy: FuturesAnalysisPolicy,
) -> _SnapshotResult:
    flags: list[str] = []
    veto_flags: list[str] = []
    session_status = (
        evidence.session_status
        if isinstance(evidence.session_status, SessionStatus)
        else None
    )
    timestamps_valid = _timestamp_chain(
        evidence.observed_at,
        evidence.available_at,
        evidence.ingested_at,
        as_of,
    )
    snapshot_valid = bool(
        isinstance(evidence, FuturesEvidence)
        and evidence.contract_id == contract_id
        and evidence.source == T4_SOURCE_ID
        and evidence.is_full_snapshot is True
        and timestamps_valid
        and as_of - evidence.observed_at
        <= timedelta(seconds=policy.max_snapshot_age_seconds)
        and session_status is not None
    )
    if not snapshot_valid:
        return _SnapshotResult(
            MetricStatus.INVALID,
            None,
            MetricStatus.INVALID,
            None,
            None,
            None,
            MetricStatus.INVALID,
            None,
            None,
            session_status,
            ("FUTURES_SNAPSHOT_INVALID",),
            ("FUTURES_SNAPSHOT_INVALID",),
        )

    if session_status is SessionStatus.CLOSED:
        flags.append("SESSION_CLOSED")
        veto_flags.append("SESSION_CLOSED")
    elif session_status is SessionStatus.HALTED:
        flags.append("SESSION_HALT_CONFIRMED")
        veto_flags.append("SESSION_HALT_CONFIRMED")

    book_valid = _valid_book(evidence.bids, evidence.asks)
    if not book_valid:
        return _SnapshotResult(
            MetricStatus.INVALID,
            None,
            MetricStatus.INVALID,
            None,
            None,
            None,
            MetricStatus.INVALID,
            None,
            None,
            session_status,
            tuple((*flags, "ORDER_BOOK_INVALID")),
            tuple((*veto_flags, "ORDER_BOOK_INVALID")),
        )

    best_bid = evidence.bids[0].price
    best_ask = evidence.asks[0].price
    mid = best_bid + (best_ask - best_bid) / 2.0
    spread_bps = 10_000.0 * (best_ask - best_bid) / mid
    if not math.isfinite(mid) or not math.isfinite(spread_bps):
        return _SnapshotResult(
            MetricStatus.INVALID,
            None,
            MetricStatus.INVALID,
            None,
            None,
            None,
            MetricStatus.INVALID,
            None,
            None,
            session_status,
            tuple((*flags, "ORDER_BOOK_INVALID")),
            tuple((*veto_flags, "ORDER_BOOK_INVALID")),
        )
    spread_status = MetricStatus.AVAILABLE
    if spread_bps > policy.max_spread_bps:
        flags.append("SPREAD_TOO_WIDE")
        veto_flags.append("SPREAD_TOO_WIDE")

    if (
        len(evidence.bids) < policy.required_book_depth
        or len(evidence.asks) < policy.required_book_depth
    ):
        depth_status = MetricStatus.UNAVAILABLE
        bid_depth = None
        ask_depth = None
        imbalance = None
        flags.append("DEPTH_INSUFFICIENT")
        veto_flags.append("DEPTH_INSUFFICIENT")
    else:
        selected_bids = evidence.bids[: policy.required_book_depth]
        selected_asks = evidence.asks[: policy.required_book_depth]
        bid_depth = sum(item.quantity for item in selected_bids)
        ask_depth = sum(item.quantity for item in selected_asks)
        total_depth = bid_depth + ask_depth
        if (
            not all(math.isfinite(item) for item in (bid_depth, ask_depth, total_depth))
            or bid_depth <= 0
            or ask_depth <= 0
            or total_depth <= 0
        ):
            depth_status = MetricStatus.INVALID
            imbalance = None
            flags.append("ORDER_BOOK_DEPTH_INVALID")
            veto_flags.append("ORDER_BOOK_DEPTH_INVALID")
        else:
            depth_status = MetricStatus.AVAILABLE
            imbalance = (bid_depth - ask_depth) / total_depth
            if (
                bid_depth < policy.min_depth_per_side
                or ask_depth < policy.min_depth_per_side
            ):
                flags.append("DEPTH_INSUFFICIENT")
                veto_flags.append("DEPTH_INSUFFICIENT")

    basis_status, basis_bps, annualized_basis, basis_flags = _basis_metrics(
        evidence.basis_reference,
        expected_symbol=symbol,
        futures_mid=mid,
        snapshot_observed_at=evidence.observed_at,
        as_of=as_of,
        seconds_to_expiry=seconds_to_expiry,
        policy=policy,
    )
    flags.extend(basis_flags)
    if basis_status is not MetricStatus.AVAILABLE:
        veto_flags.extend(basis_flags or ("BASIS_REFERENCE_UNAVAILABLE",))
    elif basis_bps is not None and abs(basis_bps) > policy.max_absolute_basis_bps:
        flags.append("BASIS_EXTREME")
        veto_flags.append("BASIS_EXTREME")

    return _SnapshotResult(
        spread_status,
        spread_bps,
        depth_status,
        bid_depth,
        ask_depth,
        imbalance,
        basis_status,
        basis_bps,
        annualized_basis,
        session_status,
        tuple(dict.fromkeys(flags)),
        tuple(dict.fromkeys(veto_flags)),
    )


def _basis_metrics(
    reference: BasisReference,
    *,
    expected_symbol: str,
    futures_mid: float,
    snapshot_observed_at: datetime,
    as_of: datetime,
    seconds_to_expiry: float | None,
    policy: FuturesAnalysisPolicy,
) -> tuple[MetricStatus, float | None, float | None, tuple[str, ...]]:
    valid = bool(
        isinstance(reference, BasisReference)
        and reference.reference_type == policy.basis_reference_type
        and reference.source == policy.basis_source_id
        and reference.symbol == expected_symbol
        and _finite_positive(reference.price)
        and _timestamp_chain(
            reference.observed_at,
            reference.available_at,
            reference.ingested_at,
            as_of,
        )
        and as_of - reference.observed_at
        <= timedelta(seconds=policy.max_basis_age_seconds)
        and abs((reference.observed_at - snapshot_observed_at).total_seconds())
        <= policy.max_timestamp_skew_seconds
    )
    if not valid:
        return MetricStatus.INVALID, None, None, ("BASIS_REFERENCE_INVALID",)
    basis_ratio = futures_mid / reference.price - 1.0
    basis_bps = 10_000.0 * basis_ratio
    annualized = (
        None
        if seconds_to_expiry is None or seconds_to_expiry <= 0
        else basis_ratio * SECONDS_PER_YEAR / seconds_to_expiry
    )
    if not math.isfinite(basis_bps) or annualized is None or not math.isfinite(annualized):
        return MetricStatus.INVALID, None, None, ("BASIS_REFERENCE_INVALID",)
    return MetricStatus.AVAILABLE, basis_bps, annualized, ()


@dataclass(frozen=True, slots=True)
class _TransitionResult:
    status: MetricStatus
    roll_impact_bps: float | None
    flags: tuple[str, ...]
    veto_flags: tuple[str, ...]


def _transition_metrics(
    transition: ContractTransitionEvidence | None,
    *,
    evidence: FuturesEvidence,
    current_mid: float | None,
    as_of: datetime,
    contract_id: object,
    contract_selection: object,
    rolled_from_contract_id: object,
    policy: FuturesAnalysisPolicy,
) -> _TransitionResult:
    if contract_selection == "front_month":
        if transition is None:
            return _TransitionResult(MetricStatus.NOT_APPLICABLE, None, (), ())
        return _TransitionResult(
            MetricStatus.INVALID,
            None,
            ("ROLL_EVIDENCE_UNEXPECTED",),
            ("ROLL_EVIDENCE_UNEXPECTED",),
        )
    if contract_selection != "rolled" or transition is None:
        return _TransitionResult(
            MetricStatus.UNAVAILABLE,
            None,
            ("ROLL_EVIDENCE_UNAVAILABLE",),
            ("ROLL_EVIDENCE_UNAVAILABLE",),
        )
    valid = bool(
        isinstance(transition, ContractTransitionEvidence)
        and transition.from_contract_id == rolled_from_contract_id
        and transition.to_contract_id == contract_id
        and transition.price_type == "mid"
        and transition.source == T4_SOURCE_ID
        and _finite_positive(transition.from_price)
        and _finite_positive(transition.to_price)
        and current_mid is not None
        and math.isclose(
            transition.to_price,
            current_mid,
            rel_tol=1e-12,
            abs_tol=1e-12,
        )
        and _timestamp_chain(
            transition.observed_at,
            transition.available_at,
            transition.ingested_at,
            as_of,
        )
        and abs((transition.observed_at - evidence.observed_at).total_seconds())
        <= policy.max_timestamp_skew_seconds
    )
    if not valid:
        return _TransitionResult(
            MetricStatus.INVALID,
            None,
            ("ROLL_EVIDENCE_INVALID",),
            ("ROLL_EVIDENCE_INVALID",),
        )
    impact = 10_000.0 * (transition.to_price / transition.from_price - 1.0)
    if not math.isfinite(impact):
        return _TransitionResult(
            MetricStatus.INVALID,
            None,
            ("ROLL_EVIDENCE_INVALID",),
            ("ROLL_EVIDENCE_INVALID",),
        )
    if abs(impact) > policy.max_absolute_roll_impact_bps:
        return _TransitionResult(
            MetricStatus.AVAILABLE,
            impact,
            ("ROLL_IMPACT_EXTREME",),
            ("ROLL_IMPACT_EXTREME",),
        )
    return _TransitionResult(MetricStatus.AVAILABLE, impact, (), ())


def validate_futures_evidence_structure(
    evidence: FuturesEvidence | None,
    *,
    as_of: datetime,
    symbol: str,
    contract_id: object,
    contract_selection: object,
    rolled_from_contract_id: object,
) -> bool:
    """Validate the immutable schema-v3 attestation before durable storage."""

    if not bool(
        type(evidence) is FuturesEvidence
        and evidence.contract_id == contract_id
        and evidence.source == T4_SOURCE_ID
        and evidence.is_full_snapshot is True
        and type(evidence.session_status) is SessionStatus
        and _timestamp_chain(
            evidence.observed_at,
            evidence.available_at,
            evidence.ingested_at,
            as_of,
        )
        and _valid_book(evidence.bids, evidence.asks)
    ):
        return False
    reference = evidence.basis_reference
    if not bool(
        type(reference) is BasisReference
        and reference.symbol == symbol
        and reference.reference_type == APPROVED_BASIS_REFERENCE_TYPE
        and reference.source == APPROVED_BASIS_SOURCE_ID
        and _finite_positive(reference.price)
        and _timestamp_chain(
            reference.observed_at,
            reference.available_at,
            reference.ingested_at,
            as_of,
        )
        and abs((reference.observed_at - evidence.observed_at).total_seconds())
        <= MAX_EVIDENCE_TIMESTAMP_SKEW_SECONDS
    ):
        return False
    transition = evidence.contract_transition
    if contract_selection == "front_month":
        return rolled_from_contract_id is None and transition is None
    mid = _current_mid(evidence)
    return bool(
        contract_selection == "rolled"
        and isinstance(rolled_from_contract_id, str)
        and bool(rolled_from_contract_id)
        and type(transition) is ContractTransitionEvidence
        and transition.from_contract_id == rolled_from_contract_id
        and transition.to_contract_id == contract_id
        and transition.price_type == "mid"
        and transition.source == T4_SOURCE_ID
        and _finite_positive(transition.from_price)
        and _finite_positive(transition.to_price)
        and mid is not None
        and math.isclose(transition.to_price, mid, rel_tol=1e-12, abs_tol=1e-12)
        and _timestamp_chain(
            transition.observed_at,
            transition.available_at,
            transition.ingested_at,
            as_of,
        )
        and abs((transition.observed_at - evidence.observed_at).total_seconds())
        <= MAX_EVIDENCE_TIMESTAMP_SKEW_SECONDS
    )


def _current_mid(evidence: FuturesEvidence) -> float | None:
    if not _valid_book(evidence.bids, evidence.asks):
        return None
    best_bid = evidence.bids[0].price
    best_ask = evidence.asks[0].price
    mid = best_bid + (best_ask - best_bid) / 2.0
    return mid if math.isfinite(mid) and mid > 0 else None


def _valid_book(
    bids: tuple[OrderBookLevel, ...], asks: tuple[OrderBookLevel, ...]
) -> bool:
    if not isinstance(bids, tuple) or not isinstance(asks, tuple) or not bids or not asks:
        return False
    for levels in (bids, asks):
        if any(type(item) is not OrderBookLevel for item in levels):
            return False
        if tuple(item.level for item in levels) != tuple(range(1, len(levels) + 1)):
            return False
        if any(
            type(item.level) is not int
            or not _finite_positive(item.price)
            or not _finite_nonnegative(item.quantity)
            for item in levels
        ):
            return False
    if any(
        current.price >= previous.price
        for previous, current in zip(bids, bids[1:], strict=False)
    ):
        return False
    if any(
        current.price <= previous.price
        for previous, current in zip(asks, asks[1:], strict=False)
    ):
        return False
    return asks[0].price > bids[0].price


def futures_evidence_to_dict(evidence: FuturesEvidence | None) -> dict[str, Any] | None:
    if evidence is None:
        return None
    return {
        "contract_id": evidence.contract_id,
        "source_id": evidence.source,
        "session_status": evidence.session_status.value,
        "is_full_snapshot": evidence.is_full_snapshot,
        "observed_at": evidence.observed_at.isoformat(),
        "available_at": evidence.available_at.isoformat(),
        "ingested_at": evidence.ingested_at.isoformat(),
        "bids": [_level_to_dict(item) for item in evidence.bids],
        "asks": [_level_to_dict(item) for item in evidence.asks],
        "basis_reference": {
            "symbol": evidence.basis_reference.symbol,
            "reference_type": evidence.basis_reference.reference_type,
            "source": evidence.basis_reference.source,
            "price": evidence.basis_reference.price,
            "observed_at": evidence.basis_reference.observed_at.isoformat(),
            "available_at": evidence.basis_reference.available_at.isoformat(),
            "ingested_at": evidence.basis_reference.ingested_at.isoformat(),
        },
        "contract_transition": _transition_to_dict(evidence.contract_transition),
    }


def _level_to_dict(level: OrderBookLevel) -> dict[str, object]:
    return {"level": level.level, "price": level.price, "quantity": level.quantity}


def _transition_to_dict(
    transition: ContractTransitionEvidence | None,
) -> dict[str, object] | None:
    if transition is None:
        return None
    return {
        "from_contract_id": transition.from_contract_id,
        "to_contract_id": transition.to_contract_id,
        "price_type": transition.price_type,
        "from_price": transition.from_price,
        "to_price": transition.to_price,
        "source": transition.source,
        "observed_at": transition.observed_at.isoformat(),
        "available_at": transition.available_at.isoformat(),
        "ingested_at": transition.ingested_at.isoformat(),
    }


def _futures_counter_evidence(
    *,
    spread_status: MetricStatus,
    spread_bps: float | None,
    depth_status: MetricStatus,
    basis_status: MetricStatus,
    basis_bps: float | None,
    lifecycle: _LifecycleResult,
    session_status: SessionStatus | None,
    reason_codes: tuple[str, ...],
) -> tuple[str, ...]:
    evidence = [
        f"Status sesji futures: {session_status.value if session_status else 'UNAVAILABLE'}.",
        f"Ryzyko wygaśnięcia: {lifecycle.expiry_risk or 'UNAVAILABLE'}.",
    ]
    if spread_status is MetricStatus.AVAILABLE and spread_bps is not None:
        evidence.append(f"Spread bid/ask: {spread_bps:.2f} bps.")
    else:
        evidence.append(f"Spread bid/ask: {spread_status.value}.")
    evidence.append(f"Głębokość order booka: {depth_status.value}.")
    if basis_status is MetricStatus.AVAILABLE and basis_bps is not None:
        evidence.append(f"Basis futures względem jawnej referencji: {basis_bps:.2f} bps.")
    else:
        evidence.append(f"Basis futures: {basis_status.value}.")
    limitations = [
        code
        for code in reason_codes
        if code.endswith("UNAVAILABLE") or code.endswith("INCOMPLETE")
    ]
    if limitations:
        evidence.append("Ograniczenia dowodów: " + ", ".join(limitations) + ".")
    return tuple(evidence)


def _timestamp_chain(
    observed_at: object,
    available_at: object,
    ingested_at: object,
    as_of: datetime,
) -> bool:
    return bool(
        _is_utc(as_of)
        and isinstance(observed_at, datetime)
        and isinstance(available_at, datetime)
        and isinstance(ingested_at, datetime)
        and _is_utc(observed_at)
        and _is_utc(available_at)
        and _is_utc(ingested_at)
        and observed_at <= available_at <= ingested_at <= as_of
    )


def _utc_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    else:
        raise TypeError("timestamp is unavailable")
    if not _is_utc(parsed):
        raise ValueError("timestamp must be UTC")
    return parsed


def _is_utc(value: datetime) -> bool:
    return value.tzinfo is not None and value.utcoffset() == timedelta(0)


def _finite_positive(value: object) -> bool:
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) > 0
    )


def _finite_nonnegative(value: object) -> bool:
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and float(value) >= 0
    )


def _rounded(value: float | None) -> float | None:
    return None if value is None else round(value, 8)

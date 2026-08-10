from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from .consensus_math import CONSENSUS_WINDOW_SIZE
from .domain import DataQualityReport, Decision, MarketMetrics, Regime, RiskAssessment


class PolicyConfigurationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class RiskPolicy:
    policy_id: str
    mode: str
    execution_enabled: bool
    allowed_decisions: tuple[str, ...]
    allowed_assets: tuple[str, ...]
    allowed_intervals_minutes: tuple[int, ...]
    min_samples: int
    min_data_quality: float
    max_staleness_multiplier: float
    max_gap_multiplier: float
    max_annualized_volatility: float
    max_absolute_period_return: float
    report_ttl_seconds: int
    max_clock_skew_seconds: int
    min_consensus_sources: int
    min_consensus_overlap: int
    max_cross_source_divergence_bps: float
    max_cross_source_ohlc_divergence_bps: float
    max_cross_source_volume_zscore_delta: float
    max_divergent_candle_fraction: float
    max_market_price: float
    max_base_volume: float
    leverage_allowed: bool
    martingale_allowed: bool
    exchange_credentials_allowed: bool
    human_approval_required_for_execution: bool

    @classmethod
    def load(cls, path: str | Path) -> "RiskPolicy":
        payload = json.loads(
            Path(path).read_text(encoding="utf-8"),
            parse_constant=_reject_nonstandard_number,
        )
        policy = cls(**_normalize(payload))
        policy.validate_v1_safety()
        return policy

    def validate_v1_safety(self) -> None:
        errors: list[str] = []
        min_samples_valid = _strict_integer_between(self.min_samples, 60, 720)
        min_consensus_overlap_valid = bool(
            type(self.min_consensus_overlap) is int
            and self.min_consensus_overlap == CONSENSUS_WINDOW_SIZE
        )
        if self.mode != "V1_READ_ONLY":
            errors.append("V1 requires mode=V1_READ_ONLY")
        if self.execution_enabled is not False:
            errors.append("execution must be disabled in V1")
        if self.leverage_allowed is not False:
            errors.append("leverage must be disabled in V1")
        if self.martingale_allowed is not False:
            errors.append("martingale must be disabled")
        if self.exchange_credentials_allowed is not False:
            errors.append("exchange credentials are forbidden in V1")
        if set(self.allowed_decisions) != {Decision.NO_SIGNAL.value, Decision.ALERT.value}:
            errors.append("V1 decisions must be exactly NO_SIGNAL and ALERT")
        if (
            self.allowed_intervals_minutes != (240, 1440, 10080)
            or any(type(value) is not int for value in self.allowed_intervals_minutes)
        ):
            errors.append("V1 intervals must be exactly 4h, 1d and 1w")
        if not self.allowed_assets or len(self.allowed_assets) > 20:
            errors.append("V1 asset allowlist must contain 1-20 assets")
        if not _finite_between(self.min_data_quality, 0.85, 1.0):
            errors.append("min_data_quality must be finite and in [0.85, 1.0]")
        if not min_samples_valid:
            errors.append("min_samples must be an integer in [60, 720]")
        if not _finite_between(self.max_staleness_multiplier, 0.5, 1.25):
            errors.append("max_staleness_multiplier must be finite and in [0.5, 1.25]")
        if not _finite_between(self.max_gap_multiplier, 1.0, 1.5):
            errors.append("max_gap_multiplier must be finite and in [1.0, 1.5]")
        if not _finite_between(self.max_annualized_volatility, 0.1, 2.0):
            errors.append("max_annualized_volatility must be finite and in [0.1, 2.0]")
        if not _finite_between(self.max_absolute_period_return, 0.01, 0.5):
            errors.append("max_absolute_period_return must be finite and in [0.01, 0.5]")
        if not _strict_integer_between(self.report_ttl_seconds, 60, 86400):
            errors.append("report_ttl_seconds must be an integer in [60, 86400]")
        if not _strict_integer_between(self.max_clock_skew_seconds, 0, 300):
            errors.append("max_clock_skew_seconds must be an integer in [0, 300]")
        if (
            type(self.min_consensus_sources) is not int
            or self.min_consensus_sources != 2
        ):
            errors.append("V1.1 requires exactly two independent consensus sources")
        if not min_consensus_overlap_valid:
            errors.append(
                f"V1.1 min_consensus_overlap must equal the versioned "
                f"{CONSENSUS_WINDOW_SIZE}-candle window"
            )
        if not _finite_between(self.max_cross_source_divergence_bps, 1.0, 100.0):
            errors.append(
                "max_cross_source_divergence_bps must be finite and in [1.0, 100.0]"
            )
        if not _finite_between(self.max_cross_source_ohlc_divergence_bps, 1.0, 500.0):
            errors.append(
                "max_cross_source_ohlc_divergence_bps must be finite and in [1.0, 500.0]"
            )
        if not _finite_between(self.max_cross_source_volume_zscore_delta, 0.1, 3.0):
            errors.append(
                "max_cross_source_volume_zscore_delta must be finite and in [0.1, 3.0]"
            )
        if not _finite_between(self.max_divergent_candle_fraction, 0.0, 0.0):
            errors.append("V1.1 requires max_divergent_candle_fraction=0.0")
        if not _finite_between(self.max_market_price, 1_000_000.0, 1_000_000_000.0):
            errors.append("max_market_price must be finite and in [1e6, 1e9]")
        if not _finite_between(self.max_base_volume, 10_000_000.0, 1_000_000_000_000.0):
            errors.append("max_base_volume must be finite and in [1e7, 1e12]")
        if self.human_approval_required_for_execution is not True:
            errors.append("human approval must remain required for execution")
        if errors:
            raise PolicyConfigurationError("; ".join(errors))

    def fingerprint(self) -> str:
        encoded = json.dumps(
            asdict(self), sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


class RiskGate:
    """Deterministic veto boundary. No model can bypass it."""

    def __init__(self, policy: RiskPolicy) -> None:
        policy.validate_v1_safety()
        self.policy = policy

    def evaluate(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        quality: DataQualityReport,
        metrics: MarketMetrics | None,
        as_of: datetime,
        expires_at: datetime,
        input_fingerprint_sha256: str,
        consensus_passed: bool,
        proposed_decision: Decision = Decision.NO_SIGNAL,
        proposal_reasons: tuple[str, ...] = (),
    ) -> RiskAssessment:
        quality_is_structured = isinstance(quality, DataQualityReport)
        flags = list(quality.flags) if quality_is_structured else []
        reasons: list[str] = []
        invalid_input_reasons: list[str] = []

        quality_valid = quality_is_structured and _valid_quality_input(quality, as_of)
        metrics_valid = metrics is not None and _valid_metrics_input(metrics)
        time_valid = (
            _is_utc_datetime(as_of)
            and _is_utc_datetime(expires_at)
            and expires_at > as_of
            and expires_at
            <= as_of + timedelta(seconds=self.policy.report_ttl_seconds)
        )
        fingerprint_valid = bool(
            isinstance(input_fingerprint_sha256, str)
            and re.fullmatch(r"[0-9a-f]{64}", input_fingerprint_sha256)
        )
        decision_valid = bool(
            isinstance(proposed_decision, Decision)
            and proposed_decision.value in self.policy.allowed_decisions
        )
        consensus_input_valid = isinstance(consensus_passed, bool)

        if not quality_valid:
            invalid_input_reasons.append("DataQualityReport failed independent validation.")
        if metrics is not None and not metrics_valid:
            invalid_input_reasons.append("MarketMetrics contains invalid or non-finite values.")
        if not time_valid:
            invalid_input_reasons.append("Risk assessment timestamps are invalid or expired.")
        if not fingerprint_valid:
            invalid_input_reasons.append("Input fingerprint is not a lowercase SHA-256 value.")
        if not decision_valid:
            invalid_input_reasons.append("Proposed decision is outside the V1 allowlist.")
        if not consensus_input_valid:
            invalid_input_reasons.append("Consensus proof must be a strict boolean.")
        if invalid_input_reasons:
            flags.append("INVALID_RISK_INPUT")
            reasons.extend(invalid_input_reasons)

        if consensus_passed is not True:
            flags.append("CONSENSUS_REQUIRED")
            reasons.append(
                "Two independent market-data sources did not pass deterministic consensus."
            )

        if symbol not in self.policy.allowed_assets:
            flags.append("ASSET_NOT_ALLOWED")
            reasons.append(f"Asset {symbol} is outside the V1 allowlist.")
        if interval_minutes not in self.policy.allowed_intervals_minutes:
            flags.append("INTERVAL_NOT_ALLOWED")
            reasons.append(f"Interval {interval_minutes} is outside the V1 allowlist.")
        if not quality_valid or quality.sample_count < self.policy.min_samples:
            flags.append("INSUFFICIENT_HISTORY")
            reasons.append("Not enough point-in-time observations.")
        if not quality_valid or quality.score < self.policy.min_data_quality:
            flags.append("LOW_DATA_QUALITY")
            reasons.append("Data quality score is below policy threshold.")
        if quality_is_structured and quality.critical_flags:
            reasons.append("Critical data-quality veto is active.")

        if not metrics_valid:
            flags.append("METRICS_UNAVAILABLE")
            reasons.append("Metrics could not be calculated safely.")
        else:
            assert metrics is not None
            if metrics.annualized_volatility > self.policy.max_annualized_volatility:
                flags.append("EXTREME_VOLATILITY")
                reasons.append("Annualized volatility exceeds the V1 safety threshold.")
            if abs(metrics.period_return) > self.policy.max_absolute_period_return:
                flags.append("EXTREME_PERIOD_MOVE")
                reasons.append("Latest period move exceeds the V1 safety threshold.")

        veto_flags = {
            "ASSET_NOT_ALLOWED",
            "INTERVAL_NOT_ALLOWED",
            "INSUFFICIENT_HISTORY",
            "LOW_DATA_QUALITY",
            "METRICS_UNAVAILABLE",
            "EXTREME_VOLATILITY",
            "EXTREME_PERIOD_MOVE",
            "CONSENSUS_REQUIRED",
            "INVALID_RISK_INPUT",
            *(quality.critical_flags if quality_is_structured else ()),
        }
        vetoed = any(flag in veto_flags for flag in flags)
        decision = Decision.NO_SIGNAL if vetoed or not decision_valid else proposed_decision
        if not reasons:
            reasons.extend(
                proposal_reasons
                or (
                    "Brak kwalifikowanego alertu badawczego; NO_SIGNAL jest stanem domyślnym.",
                )
            )

        safe_as_of = (
            as_of
            if _is_utc_datetime(as_of)
            else datetime(1970, 1, 1, tzinfo=timezone.utc)
        )
        safe_expires_at = (
            expires_at
            if time_valid
            else safe_as_of + timedelta(seconds=self.policy.report_ttl_seconds)
        )
        safe_fingerprint = input_fingerprint_sha256 if fingerprint_valid else "0" * 64
        return RiskAssessment(
            assessment_id=str(uuid4()),
            policy_id=self.policy.policy_id,
            policy_hash=self.policy.fingerprint(),
            as_of=safe_as_of,
            expires_at=safe_expires_at,
            input_fingerprint_sha256=safe_fingerprint,
            decision=decision,
            vetoed=vetoed,
            flags=tuple(dict.fromkeys(flags)),
            reasons=tuple(reasons),
        )


def _normalize(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)
    for key in ("allowed_decisions", "allowed_assets", "allowed_intervals_minutes"):
        normalized[key] = tuple(normalized[key])
    return normalized


def _finite_between(value: Any, minimum: float, maximum: float) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value)) and minimum <= float(value) <= maximum


def _strict_integer_between(value: Any, minimum: int, maximum: int) -> bool:
    return type(value) is int and minimum <= value <= maximum


def _is_utc_datetime(value: Any) -> bool:
    return bool(
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() == timedelta(0)
    )


def _valid_quality_input(quality: DataQualityReport, as_of: datetime) -> bool:
    if (
        isinstance(quality.score, bool)
        or not isinstance(quality.score, (int, float))
        or not math.isfinite(float(quality.score))
        or not 0.0 <= float(quality.score) <= 1.0
        or isinstance(quality.sample_count, bool)
        or not isinstance(quality.sample_count, int)
        or quality.sample_count < 0
        or not all(isinstance(flag, str) and flag for flag in quality.flags)
        or not all(isinstance(flag, str) and flag for flag in quality.critical_flags)
        or not set(quality.critical_flags).issubset(quality.flags)
        or not _is_utc_datetime(as_of)
    ):
        return False
    if quality.sample_count > 0 and (
        quality.newest_observed_at is None
        or quality.newest_available_at is None
    ):
        return False
    for timestamp in (quality.newest_observed_at, quality.newest_available_at):
        if timestamp is not None and (
            not _is_utc_datetime(timestamp) or timestamp > as_of
        ):
            return False
    if (
        quality.newest_observed_at is not None
        and quality.newest_available_at is not None
        and quality.newest_observed_at > quality.newest_available_at
    ):
        return False
    return True


def _valid_metrics_input(metrics: MarketMetrics) -> bool:
    values = (
        metrics.last_price,
        metrics.period_return,
        metrics.return_7_periods,
        metrics.annualized_volatility,
        metrics.max_drawdown,
        metrics.sma_20,
        metrics.sma_50,
        metrics.volume_zscore,
    )
    return bool(
        all(
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(float(value))
            for value in values
        )
        and metrics.last_price > 0
        and metrics.sma_20 > 0
        and metrics.sma_50 > 0
        and metrics.annualized_volatility >= 0
        and metrics.max_drawdown <= 0
        and isinstance(metrics.regime, Regime)
    )


def _reject_nonstandard_number(value: str) -> None:
    raise PolicyConfigurationError(f"Non-standard numeric constant is forbidden: {value}")

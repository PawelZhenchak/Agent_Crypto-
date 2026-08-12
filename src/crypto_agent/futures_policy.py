from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path

APPROVED_BASIS_REFERENCE_TYPE = "index"
APPROVED_BASIS_SOURCE_ID = "plus500_t4_index_v1"


class FuturesPolicyConfigurationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class FuturesAnalysisPolicy:
    """Versioned thresholds for deterministic futures evidence analysis.

    This policy is deliberately separate from the immutable V1 risk-policy row
    already seeded in deployed PostgreSQL databases.  Updating it therefore does
    not silently change the meaning of an existing risk-policy hash.
    """

    policy_id: str
    schema_version: int
    required_book_depth: int
    max_snapshot_age_seconds: int
    max_basis_age_seconds: int
    max_timestamp_skew_seconds: int
    max_spread_bps: float
    min_depth_per_side: float
    max_absolute_basis_bps: float
    max_absolute_roll_impact_bps: float
    basis_reference_type: str
    basis_source_id: str
    roll_warning_seconds: int
    roll_high_risk_seconds: int
    expiry_warning_seconds: int
    expiry_high_risk_seconds: int
    volume_window: int
    relative_volume_window: int

    @classmethod
    def load(cls, path: str | Path) -> FuturesAnalysisPolicy:
        try:
            payload = json.loads(
                Path(path).read_text(encoding="utf-8"),
                parse_constant=_reject_nonstandard_number,
            )
            if not isinstance(payload, dict):
                raise TypeError
            policy = cls(**payload)
        except (OSError, TypeError, json.JSONDecodeError) as exc:
            raise FuturesPolicyConfigurationError(
                "Futures analysis policy is unavailable or malformed"
            ) from exc
        policy.validate()
        return policy

    def validate(self) -> None:
        errors: list[str] = []
        if not isinstance(self.policy_id, str) or not self.policy_id.strip():
            errors.append("policy_id must be non-empty text")
        if type(self.schema_version) is not int or self.schema_version != 1:
            errors.append("schema_version must be 1")
        if not _strict_int_between(self.required_book_depth, 1, 50):
            errors.append("required_book_depth must be in [1, 50]")
        for name in (
            "max_snapshot_age_seconds",
            "max_basis_age_seconds",
            "max_timestamp_skew_seconds",
            "roll_warning_seconds",
            "roll_high_risk_seconds",
            "expiry_warning_seconds",
            "expiry_high_risk_seconds",
            "volume_window",
            "relative_volume_window",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) <= 0:
                errors.append(f"{name} must be a positive integer")
        for name in (
            "max_spread_bps",
            "min_depth_per_side",
            "max_absolute_basis_bps",
            "max_absolute_roll_impact_bps",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
            ):
                errors.append(f"{name} must be a positive finite number")
        if self.roll_high_risk_seconds >= self.roll_warning_seconds:
            errors.append("roll high-risk threshold must be below warning threshold")
        if self.expiry_high_risk_seconds >= self.expiry_warning_seconds:
            errors.append("expiry high-risk threshold must be below warning threshold")
        if self.volume_window < 3 or self.relative_volume_window < 2:
            errors.append("volume windows are too short")
        if self.basis_reference_type != APPROVED_BASIS_REFERENCE_TYPE:
            errors.append("basis_reference_type must be the approved T4 index type")
        if self.basis_source_id != APPROVED_BASIS_SOURCE_ID:
            errors.append("basis_source_id must be the approved T4 index provenance")
        if errors:
            raise FuturesPolicyConfigurationError("; ".join(errors))

    @property
    def policy_hash_sha256(self) -> str:
        payload = {
            field_name: getattr(self, field_name)
            for field_name in self.__dataclass_fields__
        }
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()


def _strict_int_between(value: object, lower: int, upper: int) -> bool:
    return type(value) is int and lower <= value <= upper


def _reject_nonstandard_number(value: str) -> None:
    raise FuturesPolicyConfigurationError(
        f"Non-standard numeric constant is forbidden: {value}"
    )

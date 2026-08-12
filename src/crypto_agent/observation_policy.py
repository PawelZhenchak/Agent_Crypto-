from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import TypeGuard

_SCENARIO_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


class ObservationPolicyConfigurationError(ValueError):
    """Raised when the frozen observation policy cannot be trusted."""


@dataclass(frozen=True, slots=True)
class ObservationPolicy:
    """Versioned acceptance thresholds for the 28-day T4 observation."""

    policy_id: str
    schema_version: int
    required_bridge_schema_version: int
    minimum_elapsed_hours: int
    minimum_cycle_attempt_rate: float
    minimum_cycle_success_rate: float
    maximum_unexplained_gap_multiplier: float
    maximum_bridge_rtt_p95_seconds: float
    maximum_bridge_rtt_p99_seconds: float
    mandatory_scenarios: tuple[str, ...]

    @classmethod
    def load(cls, path: str | Path) -> ObservationPolicy:
        try:
            payload = json.loads(
                Path(path).read_text(encoding="utf-8"),
                parse_constant=_reject_nonstandard_number,
            )
            if not isinstance(payload, dict):
                raise TypeError
            if frozenset(payload) != frozenset(cls.__dataclass_fields__):
                raise TypeError
            scenarios = payload.get("mandatory_scenarios")
            if not isinstance(scenarios, list):
                raise TypeError
            policy = cls(
                **{
                    **payload,
                    "mandatory_scenarios": tuple(scenarios),
                }
            )
        except (OSError, TypeError, json.JSONDecodeError) as exc:
            raise ObservationPolicyConfigurationError(
                "Observation policy is unavailable or malformed"
            ) from exc
        policy.validate()
        return policy

    def validate(self) -> None:
        errors: list[str] = []
        if not isinstance(self.policy_id, str) or not self.policy_id.strip():
            errors.append("policy_id must be non-empty text")
        if type(self.schema_version) is not int or self.schema_version != 1:
            errors.append("schema_version must be 1")
        if (
            type(self.required_bridge_schema_version) is not int
            or self.required_bridge_schema_version < 5
        ):
            errors.append("required_bridge_schema_version must be at least 5")
        if type(self.minimum_elapsed_hours) is not int or self.minimum_elapsed_hours < 672:
            errors.append("minimum_elapsed_hours must be at least 672")
        if not _finite_between(self.minimum_cycle_attempt_rate, 0.0, 1.0):
            errors.append("minimum_cycle_attempt_rate must be finite and in [0, 1]")
        if not _finite_between(self.minimum_cycle_success_rate, 0.0, 1.0):
            errors.append("minimum_cycle_success_rate must be finite and in [0, 1]")
        if not _finite_between(self.maximum_unexplained_gap_multiplier, 1.0, 24.0):
            errors.append(
                "maximum_unexplained_gap_multiplier must be finite and in [1, 24]"
            )
        if not _finite_between(self.maximum_bridge_rtt_p95_seconds, 0.001, 60.0):
            errors.append(
                "maximum_bridge_rtt_p95_seconds must be finite and in [0.001, 60]"
            )
        if not _finite_between(self.maximum_bridge_rtt_p99_seconds, 0.001, 60.0):
            errors.append(
                "maximum_bridge_rtt_p99_seconds must be finite and in [0.001, 60]"
            )
        elif _is_finite_number(self.maximum_bridge_rtt_p95_seconds) and (
            float(self.maximum_bridge_rtt_p99_seconds)
            < float(self.maximum_bridge_rtt_p95_seconds)
        ):
            errors.append("maximum_bridge_rtt_p99_seconds must be at least p95")
        if (
            not isinstance(self.mandatory_scenarios, tuple)
            or not self.mandatory_scenarios
            or any(
                not isinstance(value, str) or _SCENARIO_CODE.fullmatch(value) is None
                for value in self.mandatory_scenarios
            )
            or len(set(self.mandatory_scenarios)) != len(self.mandatory_scenarios)
            or tuple(sorted(self.mandatory_scenarios)) != self.mandatory_scenarios
        ):
            errors.append(
                "mandatory_scenarios must be a non-empty sorted tuple of unique safe codes"
            )
        if errors:
            raise ObservationPolicyConfigurationError("; ".join(errors))

    @property
    def minimum_elapsed_seconds(self) -> int:
        return self.minimum_elapsed_hours * 3600

    @property
    def policy_hash_sha256(self) -> str:
        canonical = json.dumps(
            self.as_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        return hashlib.sha256(canonical).hexdigest()

    def as_payload(self) -> dict[str, object]:
        return {
            "policy_id": self.policy_id,
            "schema_version": self.schema_version,
            "required_bridge_schema_version": self.required_bridge_schema_version,
            "minimum_elapsed_hours": self.minimum_elapsed_hours,
            "minimum_cycle_attempt_rate": self.minimum_cycle_attempt_rate,
            "minimum_cycle_success_rate": self.minimum_cycle_success_rate,
            "maximum_unexplained_gap_multiplier": (
                self.maximum_unexplained_gap_multiplier
            ),
            "maximum_bridge_rtt_p95_seconds": self.maximum_bridge_rtt_p95_seconds,
            "maximum_bridge_rtt_p99_seconds": self.maximum_bridge_rtt_p99_seconds,
            "mandatory_scenarios": list(self.mandatory_scenarios),
        }


def _is_finite_number(value: object) -> TypeGuard[int | float]:
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _finite_between(value: object, lower: float, upper: float) -> bool:
    return _is_finite_number(value) and lower <= float(value) <= upper


def _reject_nonstandard_number(value: str) -> None:
    raise ObservationPolicyConfigurationError(
        f"Non-standard numeric constant is forbidden: {value}"
    )

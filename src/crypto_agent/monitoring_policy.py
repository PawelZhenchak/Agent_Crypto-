from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import TypeGuard

APPROVED_MONITORING_CHANNEL = "stdout_json"
APPROVED_MONITORING_DESTINATION = "process_stdout"


class MonitoringPolicyConfigurationError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class MonitoringPolicy:
    """Strict, versioned limits for local read-only monitoring delivery."""

    policy_id: str
    schema_version: int
    retention_days: int
    channel: str
    destination: str
    max_attempts: int
    retry_base_seconds: float
    retry_max_seconds: float
    attempt_timeout_seconds: float
    max_page_size: int

    @classmethod
    def load(cls, path: str | Path) -> MonitoringPolicy:
        try:
            payload = json.loads(
                Path(path).read_text(encoding="utf-8"),
                parse_constant=_reject_nonstandard_number,
            )
            if not isinstance(payload, dict):
                raise TypeError
            expected_fields = frozenset(cls.__dataclass_fields__)
            if frozenset(payload) != expected_fields:
                raise TypeError
            policy = cls(**payload)
        except (OSError, TypeError, json.JSONDecodeError) as exc:
            raise MonitoringPolicyConfigurationError(
                "Monitoring policy is unavailable or malformed"
            ) from exc
        policy.validate()
        return policy

    def validate(self) -> None:
        errors: list[str] = []
        if not isinstance(self.policy_id, str) or not self.policy_id.strip():
            errors.append("policy_id must be non-empty text")
        if type(self.schema_version) is not int or self.schema_version != 1:
            errors.append("schema_version must be 1")
        if not _strict_int_between(self.retention_days, 35, 365):
            errors.append("retention_days must be in [35, 365]")
        if self.channel != APPROVED_MONITORING_CHANNEL:
            errors.append("channel must be stdout_json")
        if self.destination != APPROVED_MONITORING_DESTINATION:
            errors.append("destination must be process_stdout")
        if not _strict_int_between(self.max_attempts, 1, 5):
            errors.append("max_attempts must be in [1, 5]")
        if not _finite_between(self.retry_base_seconds, 1.0, 300.0):
            errors.append("retry_base_seconds must be finite and in [1, 300]")
        if not _finite_between(self.retry_max_seconds, 1.0, 3600.0):
            errors.append("retry_max_seconds must be finite and in [1, 3600]")
        elif _is_finite_number(self.retry_base_seconds) and (
            float(self.retry_max_seconds) < float(self.retry_base_seconds)
        ):
            errors.append("retry_max_seconds must be at least retry_base_seconds")
        if not _finite_between(self.attempt_timeout_seconds, 0.1, 10.0):
            errors.append("attempt_timeout_seconds must be finite and in [0.1, 10]")
        if not _strict_int_between(self.max_page_size, 1, 100):
            errors.append("max_page_size must be in [1, 100]")
        if errors:
            raise MonitoringPolicyConfigurationError("; ".join(errors))

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


def _is_finite_number(value: object) -> TypeGuard[int | float]:
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    )


def _finite_between(value: object, lower: float, upper: float) -> bool:
    return _is_finite_number(value) and lower <= float(value) <= upper


def _reject_nonstandard_number(value: str) -> None:
    raise MonitoringPolicyConfigurationError(
        f"Non-standard numeric constant is forbidden: {value}"
    )

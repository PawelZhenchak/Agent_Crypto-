from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from crypto_agent.monitoring_policy import (
    MonitoringPolicy,
    MonitoringPolicyConfigurationError,
)

POLICY_PATH = Path("configs/monitoring_policy.v1.json")


def _policy() -> MonitoringPolicy:
    return MonitoringPolicy.load(POLICY_PATH)


class MonitoringPolicyTests(unittest.TestCase):
    def test_packaged_policy_is_strict_and_has_canonical_hash(self) -> None:
        policy = _policy()
        self.assertEqual(policy.schema_version, 1)
        self.assertEqual(policy.retention_days, 90)
        self.assertEqual(policy.channel, "stdout_json")
        self.assertEqual(policy.destination, "process_stdout")

        canonical = json.dumps(
            {
                field_name: getattr(policy, field_name)
                for field_name in policy.__dataclass_fields__
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        expected_hash = hashlib.sha256(canonical).hexdigest()
        self.assertEqual(policy.policy_hash_sha256, expected_hash)
        self.assertEqual(len(policy.policy_hash_sha256), 64)

    def test_unknown_and_missing_fields_are_rejected(self) -> None:
        payload = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        for mutated in (
            {**payload, "unexpected": True},
            {key: value for key, value in payload.items() if key != "channel"},
        ):
            with self.subTest(fields=sorted(mutated)):
                self._assert_payload_rejected(mutated)

    def test_boolean_values_are_not_accepted_as_integers_or_numbers(self) -> None:
        for field_name in (
            "schema_version",
            "retention_days",
            "max_attempts",
            "retry_base_seconds",
            "retry_max_seconds",
            "attempt_timeout_seconds",
            "max_page_size",
        ):
            with self.subTest(field_name=field_name):
                policy = replace(_policy(), **{field_name: True})
                with self.assertRaises(MonitoringPolicyConfigurationError):
                    policy.validate()

    def test_nonfinite_json_numbers_are_rejected(self) -> None:
        payload = POLICY_PATH.read_text(encoding="utf-8")
        for constant in ("NaN", "Infinity", "-Infinity"):
            mutated = payload.replace('"attempt_timeout_seconds": 2.0', (
                f'"attempt_timeout_seconds": {constant}'
            ))
            with self.subTest(constant=constant):
                self._assert_text_rejected(mutated)

    def test_numeric_boundaries_and_retry_order_are_enforced(self) -> None:
        invalid_changes = (
            {"retention_days": 34},
            {"retention_days": 366},
            {"max_attempts": 0},
            {"max_attempts": 6},
            {"retry_base_seconds": 0.99},
            {"retry_base_seconds": 301},
            {"retry_max_seconds": 3601},
            {"retry_base_seconds": 30, "retry_max_seconds": 29.99},
            {"attempt_timeout_seconds": 0.09},
            {"attempt_timeout_seconds": 10.01},
            {"max_page_size": 0},
            {"max_page_size": 101},
        )
        for changes in invalid_changes:
            with self.subTest(changes=changes):
                policy = replace(_policy(), **changes)
                with self.assertRaises(MonitoringPolicyConfigurationError):
                    policy.validate()

        for changes in (
            {"retention_days": 35},
            {"retention_days": 365},
            {"max_attempts": 1},
            {"max_attempts": 5},
            {"retry_base_seconds": 1, "retry_max_seconds": 1},
            {"retry_base_seconds": 300, "retry_max_seconds": 3600},
            {"attempt_timeout_seconds": 0.1},
            {"attempt_timeout_seconds": 10},
            {"max_page_size": 1},
            {"max_page_size": 100},
        ):
            with self.subTest(valid_changes=changes):
                replace(_policy(), **changes).validate()

    def test_channel_destination_and_policy_identity_are_enforced(self) -> None:
        for changes in (
            {"policy_id": ""},
            {"policy_id": True},
            {"schema_version": 2},
            {"channel": "webhook"},
            {"destination": "https://example.test/hook"},
        ):
            with self.subTest(changes=changes):
                policy = replace(_policy(), **changes)
                with self.assertRaises(MonitoringPolicyConfigurationError):
                    policy.validate()

    def test_hash_changes_when_policy_content_changes(self) -> None:
        policy = _policy()
        changed = replace(policy, max_page_size=policy.max_page_size + 1)
        self.assertNotEqual(policy.policy_hash_sha256, changed.policy_hash_sha256)

    def _assert_payload_rejected(self, payload: object) -> None:
        self._assert_text_rejected(
            json.dumps(payload, ensure_ascii=False, allow_nan=False)
        )

    def _assert_text_rejected(self, text: str) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "monitoring-policy.json"
            path.write_text(text, encoding="utf-8")
            with self.assertRaises(MonitoringPolicyConfigurationError):
                MonitoringPolicy.load(path)


if __name__ == "__main__":
    unittest.main()

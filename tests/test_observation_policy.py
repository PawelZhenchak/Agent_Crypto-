from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path

from crypto_agent.observation_policy import (
    ObservationPolicy,
    ObservationPolicyConfigurationError,
)

POLICY_PATH = Path("configs/observation_policy.v1.json")


class ObservationPolicyTests(unittest.TestCase):
    def test_packaged_policy_has_a_canonical_hash_and_real_28_day_minimum(self) -> None:
        policy = ObservationPolicy.load(POLICY_PATH)

        self.assertEqual(policy.schema_version, 1)
        self.assertEqual(policy.required_bridge_schema_version, 5)
        self.assertEqual(policy.minimum_elapsed_hours, 672)
        self.assertEqual(policy.minimum_elapsed_seconds, 2_419_200)
        self.assertEqual(tuple(sorted(policy.mandatory_scenarios)), policy.mandatory_scenarios)
        canonical = json.dumps(
            policy.as_payload(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        self.assertEqual(policy.policy_hash_sha256, hashlib.sha256(canonical).hexdigest())

    def test_unknown_missing_and_nonfinite_fields_are_rejected(self) -> None:
        payload = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
        mutations = (
            {**payload, "unknown": True},
            {key: value for key, value in payload.items() if key != "policy_id"},
        )
        for mutation in mutations:
            with self.subTest(fields=sorted(mutation)):
                self._assert_rejected(json.dumps(mutation))
        for constant in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(constant=constant):
                self._assert_rejected(
                    POLICY_PATH.read_text(encoding="utf-8").replace(
                        '"maximum_bridge_rtt_p95_seconds": 5.0',
                        f'"maximum_bridge_rtt_p95_seconds": {constant}',
                    )
                )

    def test_policy_is_fail_closed_on_duration_rates_latency_and_scenarios(self) -> None:
        policy = ObservationPolicy.load(POLICY_PATH)
        invalid_changes = (
            {"required_bridge_schema_version": 4},
            {"minimum_elapsed_hours": 671},
            {"minimum_elapsed_hours": True},
            {"minimum_cycle_attempt_rate": 1.01},
            {"minimum_cycle_success_rate": -0.01},
            {"maximum_unexplained_gap_multiplier": 0.99},
            {"maximum_bridge_rtt_p95_seconds": 0.0},
            {
                "maximum_bridge_rtt_p95_seconds": 11.0,
                "maximum_bridge_rtt_p99_seconds": 10.0,
            },
            {"mandatory_scenarios": ()},
            {"mandatory_scenarios": ("reconnect", "reconnect")},
            {"mandatory_scenarios": ("stale_data", "reconnect")},
            {"mandatory_scenarios": ("UNSAFE",)},
        )
        for changes in invalid_changes:
            with (
                self.subTest(changes=changes),
                self.assertRaises(ObservationPolicyConfigurationError),
            ):
                replace(policy, **changes).validate()

    def test_policy_hash_changes_with_any_acceptance_threshold(self) -> None:
        policy = ObservationPolicy.load(POLICY_PATH)
        changed = replace(policy, minimum_cycle_success_rate=0.995)
        self.assertNotEqual(policy.policy_hash_sha256, changed.policy_hash_sha256)

    def _assert_rejected(self, text: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "policy.json"
            path.write_text(text, encoding="utf-8")
            with self.assertRaises(ObservationPolicyConfigurationError):
                ObservationPolicy.load(path)


if __name__ == "__main__":
    unittest.main()

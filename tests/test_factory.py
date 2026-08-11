from __future__ import annotations

import os
import unittest
from contextlib import chdir
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from crypto_agent.factory import build_orchestrator


class FactorySafetyTests(unittest.TestCase):
    def test_t4_credentials_are_rejected_in_analysis_process(self) -> None:
        with patch.dict(
            "os.environ",
            {"T4_PASSWORD": "forbidden", "CRYPTO_AGENT_ENV": "development"},
            clear=False,
        ):
            with self.assertRaises(RuntimeError):
                build_orchestrator("synthetic")

    def test_production_is_blocked_until_full_v1_gate_passes(self) -> None:
        with patch.dict("os.environ", {"CRYPTO_AGENT_ENV": "production"}, clear=False):
            with self.assertRaises(RuntimeError):
                build_orchestrator("t4")

    def test_default_policy_is_not_relative_to_the_process_cwd(self) -> None:
        with TemporaryDirectory() as directory:
            database_path = Path(directory) / "reports.db"
            with (
                patch.dict(
                    os.environ,
                    {
                        "CRYPTO_AGENT_ENV": "development",
                        "CRYPTO_AGENT_DATABASE_PATH": str(database_path),
                    },
                    clear=True,
                ),
                chdir(directory),
            ):
                orchestrator = build_orchestrator("synthetic")

        self.assertEqual(
            orchestrator.policy.policy_id, "v1-read-only-plus500-t4-2026-08-11"
        )

    def test_wheel_includes_runtime_policy_and_migrations(self) -> None:
        pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
        contents = pyproject.read_text(encoding="utf-8")
        self.assertIn(
            '"configs/risk_policy.v1.json" = '
            '"crypto_agent/resources/configs/risk_policy.v1.json"',
            contents,
        )
        self.assertIn(
            '"db/migrations" = "crypto_agent/resources/db/migrations"',
            contents,
        )
        self.assertIn(
            '"db/seeds" = "crypto_agent/resources/db/seeds"',
            contents,
        )


if __name__ == "__main__":
    unittest.main()

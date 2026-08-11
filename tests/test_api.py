from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from crypto_agent.api_config import analysis_timeout_seconds


class ApiV11Tests(unittest.TestCase):
    def test_analysis_deadline_is_bounded(self) -> None:
        for value in ("nan", "inf", "0", "121", "invalid"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"CRYPTO_AGENT_ANALYSIS_TIMEOUT_SECONDS": value},
                clear=False,
            ):
                with self.assertRaises(ValueError):
                    analysis_timeout_seconds()

        with patch.dict(
            os.environ,
            {"CRYPTO_AGENT_ANALYSIS_TIMEOUT_SECONDS": "30"},
            clear=False,
        ):
            self.assertEqual(analysis_timeout_seconds(), 30.0)

    def test_api_uses_threaded_deadline_and_postgres_readiness(self) -> None:
        source = Path("src/crypto_agent/api.py").read_text(encoding="utf-8")
        self.assertIn("asyncio.to_thread", source)
        self.assertIn("asyncio.wait_for", source)
        self.assertIn("analysis_deadline_scope", source)
        self.assertIn("deadline_monotonic", source)
        self.assertIn("_build_and_analyze_with_deadline", source)
        self.assertNotIn("orchestrator = build_orchestrator(provider)\n", source)
        self.assertIn("check_postgres_health", source)
        self.assertIn('"missing_seeds": list(database.missing_seeds)', source)
        self.assertIn('"missing_triggers": list(database.missing_triggers)', source)
        self.assertIn("MIGRATIONS_PENDING", Path("src/crypto_agent/postgres.py").read_text())

if __name__ == "__main__":
    unittest.main()

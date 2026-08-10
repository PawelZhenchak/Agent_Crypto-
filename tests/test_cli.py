from __future__ import annotations

import io
import os
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from crypto_agent.cli import build_parser, main


class _BrokenDriver:
    @staticmethod
    def connect(*args: object, **kwargs: object) -> object:
        raise RuntimeError("postgresql://admin:driver-secret@db/internal")


class CliTests(unittest.TestCase):
    def test_parser_exposes_all_read_only_v11_providers(self) -> None:
        parser = build_parser()
        for provider in ("synthetic", "kraken", "coinbase", "consensus"):
            args = parser.parse_args(["analyze", "--provider", provider])
            self.assertEqual(args.provider, provider)

    def test_database_failure_never_prints_dsn(self) -> None:
        secret = "cli-secret-value"
        output = io.StringIO()
        environment = {
            "CRYPTO_AGENT_POSTGRES_DSN": (
                f"postgresql://crypto_agent:{secret}@127.0.0.1/crypto_agent"
            )
        }
        with (
            patch.dict(os.environ, environment, clear=False),
            patch("crypto_agent.postgres.importlib.import_module", return_value=_BrokenDriver),
            redirect_stdout(output),
        ):
            exit_code = main(["db", "plan"])

        payload = output.getvalue()
        self.assertEqual(exit_code, 1)
        self.assertIn('"status": "error"', payload)
        self.assertIn("PostgreSQL is unavailable", payload)
        self.assertNotIn(secret, payload)
        self.assertNotIn("driver-secret", payload)

    def test_compose_bootstrap_does_not_bypass_migration_ledger(self) -> None:
        compose = Path("compose.yaml").read_text(encoding="utf-8")
        self.assertIn("127.0.0.1:5432:5432", compose)
        self.assertIn("db/schema.sql:/docker-entrypoint-initdb.d", compose)
        self.assertNotIn("0011_canonical_candle_provenance.sql:/docker-entrypoint", compose)


if __name__ == "__main__":
    unittest.main()

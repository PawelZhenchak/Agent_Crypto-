from __future__ import annotations

import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from crypto_agent.cli import build_parser, main
from crypto_agent.monitoring import DeliveryResult, MonitoringError
from crypto_agent.postgres import PostgresUnavailableError


class _BrokenDriver:
    @staticmethod
    def connect(*args: object, **kwargs: object) -> object:
        raise RuntimeError("opaque-driver-sensitive-marker")


class CliTests(unittest.TestCase):
    def test_parser_exposes_only_synthetic_and_t4(self) -> None:
        parser = build_parser()
        for provider in ("synthetic", "t4"):
            args = parser.parse_args(["analyze", "--provider", provider])
            self.assertEqual(args.provider, provider)
        for removed in ("kraken", "coinbase", "consensus"):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parser.parse_args(["analyze", "--provider", removed])

    def test_parser_exposes_read_only_ingest_and_exact_cutoff_replay(self) -> None:
        parser = build_parser()
        ingest = parser.parse_args(
            ["ingest", "--symbol", "ETH/USD", "--interval", "240", "--watch"]
        )
        self.assertEqual(ingest.command, "ingest")
        self.assertTrue(ingest.watch)
        replay = parser.parse_args(
            ["replay", "--as-of", "2026-08-11T00:00:00+00:00"]
        )
        self.assertEqual(replay.command, "replay")
        self.assertEqual(replay.limit, 120)

    def test_parser_exposes_monitoring_and_delivery_commands(self) -> None:
        parser = build_parser()

        monitor = parser.parse_args(
            [
                "monitor",
                "--symbol",
                "ETH/USD",
                "--interval",
                "240",
                "--limit",
                "180",
                "--watch",
                "--poll-seconds",
                "60",
            ]
        )
        self.assertEqual(monitor.command, "monitor")
        self.assertEqual(monitor.symbol, "ETH/USD")
        self.assertEqual(monitor.interval, 240)
        self.assertEqual(monitor.limit, 180)
        self.assertTrue(monitor.watch)
        self.assertEqual(monitor.poll_seconds, 60.0)

        delivery = parser.parse_args(
            ["deliver-alerts", "--watch", "--poll-seconds", "2.5"]
        )
        self.assertEqual(delivery.command, "deliver-alerts")
        self.assertTrue(delivery.watch)
        self.assertEqual(delivery.poll_seconds, 2.5)

        status = parser.parse_args(["monitoring-status"])
        self.assertEqual(status.command, "monitoring-status")

    def test_invalid_monitor_poll_interval_fails_before_runtime_access(self) -> None:
        for value in ("nan", "inf", "0", "86401"):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                self.subTest(value=value),
                patch("crypto_agent.cli.build_orchestrator") as build_orchestrator,
                patch(
                    "crypto_agent.cli.build_monitoring_repository"
                ) as build_monitoring_repository,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                exit_code = main(["monitor", "--poll-seconds", value])

            self.assertEqual(exit_code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("POLL_INTERVAL_INVALID", stderr.getvalue())
            build_orchestrator.assert_not_called()
            build_monitoring_repository.assert_not_called()

    def test_invalid_delivery_poll_interval_fails_before_repository_access(self) -> None:
        for value in ("nan", "inf", "0", "0.49", "86401"):
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                self.subTest(value=value),
                patch(
                    "crypto_agent.cli.build_monitoring_repository"
                ) as build_monitoring_repository,
                redirect_stdout(stdout),
                redirect_stderr(stderr),
            ):
                exit_code = main(["deliver-alerts", "--poll-seconds", value])

            self.assertEqual(exit_code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("POLL_INTERVAL_INVALID", stderr.getvalue())
            build_monitoring_repository.assert_not_called()

    def test_delivery_status_uses_stderr_without_contaminating_alert_stdout(self) -> None:
        class DeliveryRepository:
            @staticmethod
            def deliver_one(output: io.StringIO) -> SimpleNamespace:
                print('{"schema_version":1,"alert_key":"alert-1"}', file=output)
                return SimpleNamespace(
                    status="delivered",
                    alert_key="alert-1",
                    attempt_no=1,
                    error_code=None,
                )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with (
            patch(
                "crypto_agent.cli.build_monitoring_repository",
                return_value=DeliveryRepository(),
            ),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            exit_code = main(["deliver-alerts"])

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            stdout.getvalue(), '{"schema_version":1,"alert_key":"alert-1"}\n'
        )
        self.assertNotIn("delivery_status", stdout.getvalue())
        self.assertIn('"delivery_status": "delivered"', stderr.getvalue())
        self.assertIn('"attempt_no": 1', stderr.getvalue())

    def test_delivery_and_status_factory_failures_are_stable_json(self) -> None:
        cases = (
            (
                "deliver-alerts",
                PostgresUnavailableError("opaque-postgres-secret-marker"),
                "stderr",
                "ALERT_DELIVERY_FAILED",
            ),
            (
                "deliver-alerts",
                MonitoringError("opaque-monitoring-secret-marker"),
                "stderr",
                "ALERT_DELIVERY_FAILED",
            ),
            (
                "monitoring-status",
                PostgresUnavailableError("opaque-postgres-secret-marker"),
                "stdout",
                "MONITORING_UNAVAILABLE",
            ),
            (
                "monitoring-status",
                MonitoringError("opaque-monitoring-secret-marker"),
                "stdout",
                "MONITORING_UNAVAILABLE",
            ),
        )
        for command, exception, output_channel, expected_code in cases:
            with self.subTest(command=command, exception=type(exception).__name__):
                stdout = io.StringIO()
                stderr = io.StringIO()
                with (
                    patch(
                        "crypto_agent.cli.build_monitoring_repository",
                        side_effect=exception,
                    ),
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                ):
                    exit_code = main([command])

                self.assertNotEqual(exit_code, 0)
                selected_output = stdout if output_channel == "stdout" else stderr
                payload = json.loads(selected_output.getvalue())
                self.assertEqual(payload["status"], "error")
                self.assertEqual(payload["error_code"], expected_code)
                combined = stdout.getvalue() + stderr.getvalue()
                self.assertNotIn("opaque", combined)
                self.assertNotIn("secret", combined)

    def test_non_delivered_outcomes_never_return_cli_success(self) -> None:
        class DeliveryRepository:
            def __init__(self, result: DeliveryResult) -> None:
                self.result = result

            def deliver_one(self, output: io.StringIO) -> DeliveryResult:
                del output
                return self.result

        for outcome in ("retryable_failure", "permanent_failure", "expired"):
            with self.subTest(outcome=outcome):
                stdout = io.StringIO()
                stderr = io.StringIO()
                result = DeliveryResult(
                    status=outcome,
                    alert_key="alert-1",
                    attempt_no=1,
                    error_code="STABLE_DELIVERY_FAILURE",
                )
                with (
                    patch(
                        "crypto_agent.cli.build_monitoring_repository",
                        return_value=DeliveryRepository(result),
                    ),
                    redirect_stdout(stdout),
                    redirect_stderr(stderr),
                ):
                    exit_code = main(["deliver-alerts"])

                self.assertEqual(exit_code, 1)
                self.assertEqual(stdout.getvalue(), "")
                status_payload = json.loads(stderr.getvalue())
                self.assertEqual(status_payload["delivery_status"], outcome)
                self.assertEqual(
                    status_payload["error_code"], "STABLE_DELIVERY_FAILURE"
                )

    def test_database_failure_never_prints_dsn(self) -> None:
        secret = "cli-secret-value"
        output = io.StringIO()
        environment = {"CRYPTO_AGENT_POSTGRES_DSN": f"opaque-{secret}-dsn"}
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
        self.assertNotIn("opaque-driver-sensitive-marker", payload)

    def test_compose_bootstrap_does_not_bypass_migration_ledger(self) -> None:
        compose = Path("compose.yaml").read_text(encoding="utf-8")
        self.assertIn("127.0.0.1:5432:5432", compose)
        self.assertIn("db/schema.sql:/docker-entrypoint-initdb.d", compose)
        self.assertNotIn("0011_canonical_candle_provenance.sql:/docker-entrypoint", compose)

        cli_source = Path("src/crypto_agent/cli.py").read_text(encoding="utf-8")
        self.assertIn('"missing_seeds": list(health.missing_seeds)', cli_source)
        self.assertIn('"missing_triggers": list(health.missing_triggers)', cli_source)


if __name__ == "__main__":
    unittest.main()

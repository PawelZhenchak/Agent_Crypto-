from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import patch

from crypto_agent.postgres import (
    PostgresSettings,
    PostgresUnavailableError,
    PsycopgConnectionFactory,
    apply_migrations,
    check_postgres_health,
    discover_migrations,
)

from tests.db_fakes import FakeConnection, SQLStep


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class _BrokenDriver:
    @staticmethod
    def connect(*args: object, **kwargs: object) -> object:
        raise RuntimeError("could not use postgresql://admin:super-secret@db/internal")


class PostgresV11Tests(unittest.TestCase):
    def test_settings_repr_never_contains_dsn_secret(self) -> None:
        settings = PostgresSettings(
            dsn="postgresql://crypto_agent:super-secret@localhost/crypto_agent"
        )
        self.assertNotIn("super-secret", repr(settings))
        self.assertNotIn("postgresql://", repr(settings))

    def test_connector_failure_is_sanitized_and_drops_cause(self) -> None:
        settings = PostgresSettings(
            dsn="postgresql://crypto_agent:super-secret@localhost/crypto_agent"
        )
        with patch("crypto_agent.postgres.importlib.import_module", return_value=_BrokenDriver):
            with self.assertRaises(PostgresUnavailableError) as raised:
                PsycopgConnectionFactory(settings)()
        self.assertEqual(str(raised.exception), "PostgreSQL is unavailable")
        self.assertNotIn("super-secret", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)

    def test_migration_is_discovered_with_sha256(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
        self.assertEqual([item.version for item in migrations], ["0011"])
        self.assertRegex(migrations[0].checksum_sha256, r"^[0-9a-f]{64}$")

    def test_migration_defers_exact_approved_provenance_check(self) -> None:
        sql = (
            PROJECT_ROOT / "db/migrations/0011_canonical_candle_provenance.sql"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "CREATE CONSTRAINT TRIGGER canonical_candle_exact_provenance_guard",
            sql,
        )
        self.assertIn(
            "CREATE CONSTRAINT TRIGGER canonical_candle_exact_provenance_link_guard",
            sql,
        )
        self.assertIn(
            "AFTER INSERT ON crypto_agent.canonical_candle_provenance",
            sql,
        )
        self.assertIn("DEFERRABLE INITIALLY DEFERRED", sql)
        self.assertIn("COUNT(DISTINCT source.source_id)", sql)
        self.assertIn("COUNT(DISTINCT market.exchange_id)", sql)
        self.assertIn("provenance.input_role = 'observation'", sql)
        self.assertIn("total_count <> 240", sql)
        self.assertIn("observation_count <> 2", sql)
        self.assertIn("context_count <> 238", sql)
        self.assertIn("COUNT(DISTINCT source.open_time) <> 120", sql)
        self.assertIn("candidate.revision_no DESC", sql)
        self.assertIn("newer.revision_no > selected.revision_no", sql)
        self.assertIn("eligible_count <> 240", sql)
        self.assertIn("canonical provenance omits eligible raw input revisions", sql)
        self.assertIn("CHECK (consensus_window_size = 120)", sql)
        self.assertIn("coinbase_exchange_spot_rest_v1", sql)
        self.assertIn("kraken_spot_rest_v1", sql)
        self.assertIn(
            "provider_ingested_at <= cutoff_as_of AND cutoff_as_of <= received_at",
            sql,
        )
        self.assertIn("receipt.received_at <= canonical_row.ingested_at", sql)
        self.assertNotIn("receipt.received_at <= manifest_row.cutoff_as_of", sql)
        self.assertIn("manifest_created_at <> CURRENT_TIMESTAMP", sql)
        self.assertIn("provenance set is already finalized", sql)
        self.assertIn("policy.policy_document->>'min_consensus_overlap' = '120'", sql)
        self.assertIn(
            "jsonb_typeof(policy.policy_document->'execution_enabled') = 'boolean'",
            sql,
        )
        self.assertIn("leverage_allowed')::boolean IS FALSE", sql)
        self.assertIn("martingale_allowed')::boolean IS FALSE", sql)
        self.assertIn("exchange_credentials_allowed'", sql)
        self.assertIn("human_approval_required_for_execution'", sql)
        self.assertIn("allowed_intervals_minutes'", sql)
        self.assertIn("NEW.interval_seconds IN (14400, 86400, 604800)", sql)
        self.assertIn(
            "(policy.policy_document->'allowed_assets') ? NEW.canonical_symbol",
            sql,
        )
        self.assertIn("report_ttl_seconds'", sql)
        self.assertIn("policy.effective_from <= NEW.cutoff_as_of", sql)
        self.assertIn("policy.effective_to > NEW.cutoff_as_of", sql)
        self.assertIn("series_row.available_at > NEW.cutoff_as_of", sql)
        self.assertIn("series_row.ingested_at > NEW.cutoff_as_of", sql)
        self.assertIn("policy.available_at <= NEW.cutoff_as_of", sql)
        self.assertIn("market.available_at <= NEW.cutoff_as_of", sql)
        self.assertIn("exchange.available_at <= NEW.cutoff_as_of", sql)
        self.assertIn("source.available_at <= NEW.cutoff_as_of", sql)
        self.assertIn("max_divergent_candle_fraction')::numeric = 0", sql)
        self.assertIn("normalized_volume_unit text NOT NULL", sql)
        self.assertIn("dimensionless_ratio_to_source_median", sql)
        self.assertIn("diagnostics_document jsonb NOT NULL", sql)
        self.assertIn("evidence_hash       crypto_agent.sha256_hex", sql)
        self.assertIn("OR candle_row.base_volume IS NOT NULL", sql)

    def test_health_check_reports_pending_migration_without_mutation(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
        connection = FakeConnection(
            [
                SQLStep("SHOW server_version", [("16.4",)]),
                SQLStep("to_regclass('crypto_agent.candles')", [("crypto_agent.candles",)]),
                SQLStep("to_regclass('crypto_agent.schema_migrations')", [(None,)]),
            ]
        )
        health = check_postgres_health(lambda: connection, expected_migrations=migrations)
        self.assertFalse(health.healthy)
        self.assertTrue(health.database_reachable)
        self.assertTrue(health.base_schema_ready)
        self.assertEqual(health.status_code, "MIGRATIONS_PENDING")
        self.assertEqual(health.missing_migrations, ("0011",))
        self.assertTrue(connection.committed)
        self.assertTrue(connection.closed)

    def test_health_check_does_not_return_connection_exception(self) -> None:
        def unavailable() -> FakeConnection:
            raise RuntimeError("postgresql://root:secret-value@host/db")

        health = check_postgres_health(unavailable)
        self.assertEqual(health.status_code, "POSTGRES_UNAVAILABLE")
        self.assertNotIn("secret-value", repr(health))

    def test_migration_runner_records_checksum_after_sql(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
        planning = FakeConnection(
            [
                SQLStep("to_regclass('crypto_agent.candles')", [("crypto_agent.candles",)]),
                SQLStep("to_regclass('crypto_agent.schema_migrations')", [(None,)]),
            ]
        )
        applying = FakeConnection(
            [
                SQLStep("to_regclass('crypto_agent.candles')", [("crypto_agent.candles",)]),
                SQLStep("CREATE TABLE IF NOT EXISTS crypto_agent.schema_migrations"),
                SQLStep("FROM crypto_agent.schema_migrations", []),
                SQLStep("CREATE TABLE IF NOT EXISTS crypto_agent.schema_migrations"),
                SQLStep("INSERT INTO crypto_agent.schema_migrations"),
            ]
        )
        connections = iter((planning, applying))
        applied = apply_migrations(lambda: next(connections), migrations)
        self.assertEqual(applied, ("0011",))
        self.assertTrue(planning.committed)
        self.assertTrue(applying.committed)
        migration_sql_index = next(
            index
            for index, (query, _) in enumerate(applying.scripted_cursor.executions)
            if "canonical_candle_provenance" in query
        )
        ledger_insert_index = next(
            index
            for index, (query, _) in enumerate(applying.scripted_cursor.executions)
            if "INSERT INTO crypto_agent.schema_migrations" in query
        )
        self.assertLess(migration_sql_index, ledger_insert_index)


if __name__ == "__main__":
    unittest.main()

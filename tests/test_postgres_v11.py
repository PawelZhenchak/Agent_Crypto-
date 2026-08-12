from __future__ import annotations

import json
import re
import unittest
from pathlib import Path
from unittest.mock import patch

import crypto_agent.postgres as postgres_module
from crypto_agent.postgres import (
    PostgresSettings,
    PostgresUnavailableError,
    PsycopgConnectionFactory,
    apply_migrations,
    apply_v1_seeds,
    check_postgres_health,
    discover_migrations,
)

from tests.db_fakes import FakeConnection, SQLStep


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _trigger_rows(
    requirements: tuple[postgres_module._TriggerRequirement, ...],
) -> list[tuple[object, ...]]:
    return [
        (
            item.name,
            item.table,
            item.function,
            "O",
            item.type_mask,
            item.deferrable,
            item.initially_deferred,
        )
        for item in requirements
    ]


def _column_rows(
    requirements: tuple[postgres_module._ColumnRequirement, ...],
) -> list[tuple[object, ...]]:
    return [
        (item.table, item.name, item.type_name, item.not_null)
        for item in requirements
    ]


def _health_steps(
    *,
    server_version_num: str = "160004",
    server_version: str = "16.4",
    session_replication_role: str = "origin",
    namespace_exists: bool = True,
    base_relations: tuple[str, ...] | None = None,
    base_triggers: list[tuple[object, ...]] | None = None,
    applied_migrations: list[tuple[object, ...]] | None = None,
    migrated_relations: tuple[str, ...] | None = None,
    migrated_columns: list[tuple[object, ...]] | None = None,
    migrated_triggers: list[tuple[object, ...]] | None = None,
    bindings: tuple[tuple[str, str, str, str], ...] | None = None,
    series: tuple[tuple[str, int], ...] | None = None,
) -> list[SQLStep]:
    migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
    base_relation_names = (
        postgres_module._BASE_TABLES if base_relations is None else base_relations
    )
    migrated_relation_names = (
        postgres_module._MIGRATED_TABLES
        if migrated_relations is None
        else migrated_relations
    )
    migration_rows = applied_migrations
    if migration_rows is None:
        migration_rows = [
            (item.version, item.name, item.checksum_sha256) for item in migrations
        ]
    binding_rows = postgres_module._EXPECTED_BINDINGS if bindings is None else bindings
    series_rows = (
        postgres_module._EXPECTED_CANONICAL_SERIES if series is None else series
    )
    return [
        SQLStep("SET TRANSACTION READ ONLY"),
        SQLStep(
            "server_version_num",
            [(server_version_num, server_version, session_replication_role)],
        ),
        SQLStep("FROM pg_catalog.pg_namespace", [(namespace_exists,)]),
        SQLStep(
            "FROM pg_catalog.pg_class AS rel",
            [(name, "r") for name in base_relation_names],
        ),
        SQLStep("FROM pg_catalog.pg_type AS typ", [("sha256_hex", "d")]),
        SQLStep(
            "FROM pg_catalog.pg_proc AS proc",
            [(name,) for name in postgres_module._BASE_TRIGGER_FUNCTIONS],
        ),
        SQLStep(
            "FROM pg_catalog.pg_trigger AS trg",
            _trigger_rows(postgres_module._BASE_TRIGGER_REQUIREMENTS)
            if base_triggers is None
            else base_triggers,
        ),
        SQLStep(
            "to_regclass('crypto_agent.schema_migrations')",
            [("crypto_agent.schema_migrations",)] if migration_rows else [(None,)],
        ),
        *(
            [SQLStep("FROM crypto_agent.schema_migrations", migration_rows)]
            if migration_rows
            else []
        ),
        SQLStep(
            "FROM pg_catalog.pg_class AS rel",
            [(name, "r") for name in migrated_relation_names],
        ),
        SQLStep(
            "FROM pg_catalog.pg_attribute AS attr",
            _column_rows(postgres_module._MIGRATED_COLUMN_REQUIREMENTS)
            if migrated_columns is None
            else migrated_columns,
        ),
        SQLStep(
            "FROM pg_catalog.pg_proc AS proc",
            [(name,) for name in postgres_module._MIGRATED_TRIGGER_FUNCTIONS],
        ),
        SQLStep(
            "FROM pg_catalog.pg_trigger AS trg",
            _trigger_rows(postgres_module._MIGRATED_TRIGGER_REQUIREMENTS)
            if migrated_triggers is None
            else migrated_triggers,
        ),
        SQLStep("FROM crypto_agent.market_data_source_bindings binding", list(binding_rows)),
        SQLStep("FROM crypto_agent.canonical_candle_series series", list(series_rows)),
    ]


class _BrokenDriver:
    @staticmethod
    def connect(*args: object, **kwargs: object) -> object:
        raise RuntimeError("could not use opaque-sensitive-connection-marker")


class PostgresV11Tests(unittest.TestCase):
    def test_health_manifest_covers_all_versioned_tables_and_triggers(self) -> None:
        base_sql = (PROJECT_ROOT / "db/schema.sql").read_text(encoding="utf-8")
        migration_sql = "\n".join(
            (PROJECT_ROOT / "db/migrations" / name).read_text(encoding="utf-8")
            for name in (
                "0011_canonical_candle_provenance.sql",
                "0012_reference_price_policy_contract.sql",
                "0013_plus500_t4_runtime.sql",
                "0014_t4_operational_ingest.sql",
                "0015_t4_futures_evidence.sql",
            )
        )
        base_tables = set(re.findall(r"^CREATE TABLE ([a-z0-9_]+)", base_sql, re.MULTILINE))
        migrated_tables = set(
            re.findall(
                r"^CREATE TABLE (?:IF NOT EXISTS )?crypto_agent\.([a-z0-9_]+)",
                migration_sql,
                re.MULTILINE,
            )
        )
        self.assertEqual(set(postgres_module._BASE_TABLES), base_tables)
        self.assertEqual(set(postgres_module._MIGRATED_TABLES), migrated_tables)
        self.assertEqual(len(postgres_module._BASE_TABLES), 42)
        self.assertEqual(len(postgres_module._MIGRATED_TABLES), 14)
        self.assertEqual(
            len(postgres_module._BASE_TRIGGER_REQUIREMENTS)
            + len(postgres_module._MIGRATED_TRIGGER_REQUIREMENTS),
            127,
        )
        for requirement in (
            *postgres_module._BASE_TRIGGER_REQUIREMENTS,
            *postgres_module._MIGRATED_TRIGGER_REQUIREMENTS,
        ):
            self.assertIs(type(requirement.type_mask), int)
            self.assertIs(type(requirement.deferrable), bool)
            self.assertIs(type(requirement.initially_deferred), bool)

    def test_settings_repr_never_contains_dsn_secret(self) -> None:
        settings = PostgresSettings(
            dsn="opaque-sensitive-dsn"
        )
        self.assertNotIn("opaque-sensitive-dsn", repr(settings))

    def test_connector_failure_is_sanitized_and_drops_cause(self) -> None:
        settings = PostgresSettings(
            dsn="opaque-sensitive-dsn"
        )
        with patch("crypto_agent.postgres.importlib.import_module", return_value=_BrokenDriver):
            with self.assertRaises(PostgresUnavailableError) as raised:
                PsycopgConnectionFactory(settings)()
        self.assertEqual(str(raised.exception), "PostgreSQL is unavailable")
        self.assertNotIn("opaque-sensitive", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)

    def test_migration_is_discovered_with_sha256(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
        self.assertEqual(
            [item.version for item in migrations],
            ["0011", "0012", "0013", "0014", "0015"],
        )
        for migration in migrations:
            self.assertRegex(migration.checksum_sha256, r"^[0-9a-f]{64}$")

    def test_t4_operational_ingest_migration_is_append_only_and_idempotent(self) -> None:
        sql = (
            PROJECT_ROOT / "db/migrations/0014_t4_operational_ingest.sql"
        ).read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE crypto_agent.t4_ingestion_batches", sql)
        self.assertIn("CREATE TABLE crypto_agent.t4_canonical_candles", sql)
        self.assertIn("raw_payload_hash        crypto_agent.sha256_hex NOT NULL UNIQUE", sql)
        self.assertIn("t4_ingestion_batches_append_only_row_guard", sql)
        self.assertIn("t4_canonical_candles_append_only_row_guard", sql)
        self.assertNotIn("order_routes", sql.lower())

    def test_t4_futures_evidence_migration_is_append_only(self) -> None:
        sql = (
            PROJECT_ROOT / "db/migrations/0015_t4_futures_evidence.sql"
        ).read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE crypto_agent.t4_futures_snapshots", sql)
        self.assertIn("CREATE TABLE crypto_agent.t4_orderbook_levels", sql)
        self.assertIn(
            "CREATE TABLE crypto_agent.t4_contract_transition_evidence", sql
        )
        self.assertIn("bridge_schema_version IN (2, 3)", sql)
        self.assertIn("basis_reference_source = 'plus500_t4_index_v1'", sql)
        self.assertIn("evidence_source = 'plus500_t4_futures_v1'", sql)
        self.assertIn("t4_futures_snapshots_append_only_row_guard", sql)
        self.assertIn("t4_orderbook_levels_append_only_truncate_guard", sql)

    def test_reference_price_policy_migration_is_exact_and_guarded(self) -> None:
        sql = (
            PROJECT_ROOT / "db/migrations/0012_reference_price_policy_contract.sql"
        ).read_text(encoding="utf-8")
        self.assertIn(
            "CREATE TABLE crypto_agent.reference_price_manifests",
            sql,
        )
        self.assertIn("CREATE TABLE crypto_agent.reference_price_provenance", sql)
        self.assertIn(
            "CREATE OR REPLACE FUNCTION "
            "crypto_agent.enforce_v1_canonical_manifest_policy()",
            sql,
        )
        self.assertIn(
            "CREATE TRIGGER canonical_candle_manifest_reference_price_policy_guard",
            sql,
        )
        self.assertNotIn(
            "canonical_candle_series_reference_price_policy_guard",
            sql,
        )
        self.assertIn(
            "CREATE OR REPLACE FUNCTION "
            "crypto_agent.enforce_v1_reference_price_manifest()",
            sql,
        )
        self.assertIn("max_age_seconds <> 300", sql)
        self.assertIn("median_deviation > 50", sql)
        self.assertIn(
            "pairwise_divergence_bps - 2 * max_deviation_from_median_bps",
            sql,
        )
        self.assertIn("div(scaled_numerator, derived_median)", sql)
        self.assertIn("scale(source_row.close_price) > 18", sql)
        self.assertIn("source_row.close_time > source_row.available_at", sql)
        self.assertNotIn("scale(candidate.close_price) <= 18", sql)
        self.assertIn(
            "CREATE OR REPLACE FUNCTION "
            "crypto_agent.enforce_v1_reference_price_exact_provenance()",
            sql,
        )
        self.assertIn("candidate.revision_no DESC", sql)
        self.assertIn("receipt.cutoff_as_of <= manifest_row.evaluated_at", sql)
        self.assertIn("manifest_row.event_time > manifest_row.cutoff_as_of", sql)
        self.assertIn("INTERVAL '300 seconds'", sql)
        self.assertIn(
            "CREATE CONSTRAINT TRIGGER reference_price_exact_provenance_guard",
            sql,
        )
        self.assertIn(
            "CREATE CONSTRAINT TRIGGER reference_price_exact_provenance_link_guard",
            sql,
        )
        self.assertGreaterEqual(sql.count("DEFERRABLE INITIALLY DEFERRED"), 2)

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
        connection = FakeConnection(_health_steps(applied_migrations=[]))
        health = check_postgres_health(lambda: connection, expected_migrations=migrations)
        self.assertFalse(health.healthy)
        self.assertTrue(health.database_reachable)
        self.assertTrue(health.base_schema_ready)
        self.assertEqual(health.status_code, "MIGRATIONS_PENDING")
        self.assertEqual(
            health.missing_migrations,
            ("0011", "0012", "0013", "0014", "0015"),
        )
        self.assertTrue(connection.committed)
        self.assertTrue(connection.closed)
        self.assertTrue(
            all(
                not query.startswith(("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER"))
                for query, _ in connection.scripted_cursor.executions
            )
        )

    def test_health_check_rejects_postgres_12_even_with_complete_catalog(self) -> None:
        connection = FakeConnection(
            _health_steps(server_version_num="120019", server_version="12.19")
        )
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertTrue(health.database_reachable)
        self.assertEqual(health.server_version, "12.19")
        self.assertEqual(health.status_code, "POSTGRES_VERSION_UNSUPPORTED")

    def test_health_check_rejects_session_that_bypasses_origin_triggers(self) -> None:
        connection = FakeConnection(
            _health_steps(session_replication_role="replica")
        )
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertTrue(health.database_reachable)
        self.assertEqual(health.status_code, "POSTGRES_SESSION_UNSAFE")

    def test_empty_supplied_migration_manifest_cannot_bypass_packaged_manifest(self) -> None:
        connection = FakeConnection(_health_steps(applied_migrations=[]))
        health = check_postgres_health(lambda: connection, expected_migrations=())
        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "MIGRATIONS_PENDING")
        self.assertEqual(
            health.missing_migrations,
            ("0011", "0012", "0013", "0014", "0015"),
        )

    def test_caller_cannot_replace_packaged_migration_manifest_with_subset(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
        connection_attempted = False

        def connection_factory() -> FakeConnection:
            nonlocal connection_attempted
            connection_attempted = True
            return FakeConnection([])

        health = check_postgres_health(
            connection_factory,
            expected_migrations=migrations[:1],
        )
        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "MIGRATION_MANIFEST_INVALID")
        self.assertFalse(connection_attempted)

    def test_health_check_requires_complete_base_schema(self) -> None:
        without_audit_log = tuple(
            name for name in postgres_module._BASE_TABLES if name != "audit_log"
        )
        connection = FakeConnection(_health_steps(base_relations=without_audit_log))
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "BASE_SCHEMA_MISSING")
        self.assertIn("table:audit_log", health.missing_schema_objects)

    def test_health_check_rejects_view_substituted_for_required_table(self) -> None:
        steps = _health_steps()
        steps[3].rows = [
            (name, "v" if name == "candles" else "r")
            for name in postgres_module._BASE_TABLES
        ]
        connection = FakeConnection(steps)
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "BASE_SCHEMA_MISSING")
        self.assertIn("table:candles", health.missing_schema_objects)

    def test_health_check_rejects_missing_migrated_column(self) -> None:
        columns = _column_rows(postgres_module._MIGRATED_COLUMN_REQUIREMENTS)
        columns = [
            row
            for row in columns
            if row[:2] != ("reference_price_manifests", "inputs_hash")
        ]
        connection = FakeConnection(_health_steps(migrated_columns=columns))

        health = check_postgres_health(lambda: connection)

        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "column:reference_price_manifests.inputs_hash",
            health.missing_schema_objects,
        )

    def test_health_check_rejects_missing_or_disabled_trigger(self) -> None:
        trigger_rows = _trigger_rows(postgres_module._BASE_TRIGGER_REQUIREMENTS)
        trigger_rows[0] = (*trigger_rows[0][:3], "D", *trigger_rows[0][4:])
        connection = FakeConnection(_health_steps(base_triggers=trigger_rows))
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "BASE_TRIGGERS_MISSING")
        self.assertIn(
            postgres_module._BASE_TRIGGER_REQUIREMENTS[0].name,
            health.missing_triggers,
        )

    def test_health_check_rejects_missing_semantic_seeds(self) -> None:
        connection = FakeConnection(_health_steps(bindings=(), series=()))
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertTrue(health.schema_ready)
        self.assertTrue(health.triggers_ready)
        self.assertEqual(health.status_code, "SEEDS_MISSING")
        self.assertIn(
            "binding:plus500_t4_futures_v1:plus500_t4:BTC/USD:BTC-FUTURES-FRONT",
            health.missing_seeds,
        )

    def test_health_check_rejects_unexpected_semantic_seeds(self) -> None:
        bindings = (
            *postgres_module._EXPECTED_BINDINGS,
            ("plus500_t4_futures_v1", "plus500_t4", "LTC/USD", "LTC-FUTURES-FRONT"),
        )
        series = (*postgres_module._EXPECTED_CANONICAL_SERIES, ("BTC/USD", 3_600))
        connection = FakeConnection(_health_steps(bindings=bindings, series=series))

        health = check_postgres_health(lambda: connection)

        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "SEEDS_MISSING")
        self.assertIn(
            "unexpected_binding:plus500_t4_futures_v1:plus500_t4:"
            "LTC/USD:LTC-FUTURES-FRONT",
            health.missing_seeds,
        )
        self.assertIn("unexpected_series:BTC/USD:3600", health.missing_seeds)

    def test_health_check_rejects_migration_drift_and_ahead_database(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
        valid_rows = [
            (item.version, item.name, item.checksum_sha256) for item in migrations
        ]
        drifted_rows = list(valid_rows)
        drifted_rows[0] = (
            migrations[0].version,
            migrations[0].name,
            "f" * 64,
        )
        drifted = check_postgres_health(
            lambda: FakeConnection(_health_steps(applied_migrations=drifted_rows))
        )
        self.assertEqual(drifted.status_code, "MIGRATION_DRIFT")
        self.assertEqual(drifted.missing_migrations, ("0011",))

        ahead_rows = valid_rows + [("9999", "unknown_future", "e" * 64)]
        ahead = check_postgres_health(
            lambda: FakeConnection(_health_steps(applied_migrations=ahead_rows))
        )
        self.assertEqual(ahead.status_code, "MIGRATIONS_AHEAD")
        self.assertEqual(ahead.unexpected_migrations, ("9999",))

    def test_health_check_requires_deferred_reference_price_trigger(self) -> None:
        rows = _trigger_rows(postgres_module._MIGRATED_TRIGGER_REQUIREMENTS)
        rows = [
            row
            for row in rows
            if row[0] != "reference_price_exact_provenance_link_guard"
        ]
        connection = FakeConnection(_health_steps(migrated_triggers=rows))
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "MIGRATED_TRIGGERS_MISSING")
        self.assertIn(
            "reference_price_exact_provenance_link_guard",
            health.missing_triggers,
        )

    def test_health_check_returns_ready_only_for_complete_v1_contract(self) -> None:
        connection = FakeConnection(_health_steps())
        health = check_postgres_health(lambda: connection)
        self.assertTrue(health.healthy)
        self.assertTrue(health.database_reachable)
        self.assertTrue(health.base_schema_ready)
        self.assertTrue(health.migrations_current)
        self.assertTrue(health.schema_ready)
        self.assertTrue(health.triggers_ready)
        self.assertTrue(health.seeds_ready)
        self.assertEqual(health.status_code, "READY")

    def test_health_check_does_not_return_connection_exception(self) -> None:
        def unavailable() -> FakeConnection:
            raise RuntimeError("opaque-sensitive-connection-marker")

        health = check_postgres_health(unavailable)
        self.assertEqual(health.status_code, "POSTGRES_UNAVAILABLE")
        self.assertNotIn("opaque-sensitive", repr(health))

    def test_migration_runner_records_checksum_after_sql(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")[:1]
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

    def test_v1_seed_runner_is_atomic_and_executes_packaged_bundle(self) -> None:
        seed_path = PROJECT_ROOT / "db/seeds/v1_registry.sql"
        connection = FakeConnection(
            [
                SQLStep("to_regclass('crypto_agent.candles')", [("crypto_agent.candles",)]),
                SQLStep("INSERT INTO data_sources"),
            ]
        )

        apply_v1_seeds(lambda: connection, seed_path)

        self.assertTrue(connection.committed)
        self.assertFalse(connection.rolled_back)
        self.assertTrue(connection.closed)

    def test_v1_seed_bundle_contains_exact_operational_scope(self) -> None:
        sql = (PROJECT_ROOT / "db/seeds/v1_registry.sql").read_text(encoding="utf-8")
        self.assertIn("plus500_t4_futures_v1", sql)
        self.assertIn("plus500_t4", sql)
        self.assertIn("BTC-FUTURES-FRONT", sql)
        self.assertIn("ETH-FUTURES-FRONT", sql)
        self.assertNotIn("kraken_spot_rest_v1", sql)
        self.assertNotIn("coinbase_exchange_spot_rest_v1", sql)
        self.assertIn("order_routes_enabled", sql)
        self.assertIn("FALSE", sql)
        self.assertIn("589a39880a7bff780e510864525817dd890764e85b43ffdd6419cabc80a62d7d", sql)

        seed_policy = re.search(
            r"\$policy\$(?P<document>.*?)\$policy\$::jsonb",
            sql,
            re.DOTALL,
        )
        self.assertIsNotNone(seed_policy)
        assert seed_policy is not None
        configured_policy = json.loads(
            (PROJECT_ROOT / "configs/risk_policy.v1.json").read_text(encoding="utf-8")
        )
        self.assertEqual(json.loads(seed_policy.group("document")), configured_policy)


if __name__ == "__main__":
    unittest.main()

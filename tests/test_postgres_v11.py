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
    return [(item.table, item.name, item.type_name, item.not_null) for item in requirements]


def _constraint_rows(
    requirements: tuple[postgres_module._CatalogDefinitionRequirement, ...],
) -> list[tuple[object, ...]]:
    return [
        (item.table, item.name, item.object_type, True, item.definition) for item in requirements
    ]


def _index_rows(
    requirements: tuple[postgres_module._IndexRequirement, ...],
) -> list[tuple[object, ...]]:
    return [
        (item.table, item.name, item.unique, True, True, item.definition) for item in requirements
    ]


def _migration_function_source(name: str) -> str:
    migration_name = (
        "0016_operational_alert_outbox.sql"
        if name == "enforce_alert_delivery_attempt"
        else "0018_t4_scenario_evidence.sql"
        if name
        in {
            "enforce_t4_observation_campaign_start",
            "enforce_t4_observation_final_report",
            "enforce_t4_bridge_observation_event",
            "enforce_t4_observation_scenario_trial",
            "enforce_t4_observation_research_input",
            "enforce_t4_observation_cycle_chain",
            "enforce_t4_observation_event_chain",
            "enforce_t4_scenario_alert_finalization",
            "serialize_t4_observation_campaign_write",
        }
        else "0017_t4_observation_campaign.sql"
    )
    migration_sql = (PROJECT_ROOT / "db/migrations" / migration_name).read_text(
        encoding="utf-8"
    )
    matched = re.search(
        r"CREATE OR REPLACE FUNCTION "
        rf"crypto_agent\.{re.escape(name)}\(\).*?"
        r"AS \$\$(.*?)\$\$;",
        migration_sql,
        re.DOTALL,
    )
    if matched is None:
        raise AssertionError(f"migration function is missing: {name}")
    return matched.group(1)


def _migration_routine_source(name: str, argument_count: int) -> str:
    if name == "forbid_append_only_change" and argument_count == 0:
        schema_sql = (PROJECT_ROOT / "db/schema.sql").read_text(encoding="utf-8")
        matched = re.search(
            r"CREATE OR REPLACE FUNCTION forbid_append_only_change\(\).*?"
            r"AS \$\$(.*?)\$\$;",
            schema_sql,
            re.DOTALL,
        )
        if matched is None:
            raise AssertionError("base append-only function is missing")
        return matched.group(1)
    migration_sql = (
        PROJECT_ROOT / "db/migrations/0018_t4_scenario_evidence.sql"
    ).read_text(encoding="utf-8")
    matches = re.findall(
        r"CREATE OR REPLACE FUNCTION "
        rf"crypto_agent\.{re.escape(name)}\((.*?)\)\s*"
        r"RETURNS.*?AS \$\$(.*?)\$\$;",
        migration_sql,
        re.DOTALL,
    )
    for arguments, source in matches:
        count = 0 if not arguments.strip() else arguments.count(",") + 1
        if count == argument_count:
            return source
    raise AssertionError(f"migration routine is missing: {name}/{argument_count}")


def _function_definition_rows(
    requirements: tuple[postgres_module._FunctionDefinitionRequirement, ...],
) -> list[tuple[object, ...]]:
    return [
        (
            item.name,
            item.language,
            item.volatility,
            item.security_definer,
            _migration_function_source(item.name),
        )
        for item in requirements
    ]


def _routine_rows(
    requirements: tuple[postgres_module._RoutineRequirement, ...],
) -> list[tuple[object, ...]]:
    return [
        (
            item.name,
            item.argument_count,
            item.identity_arguments,
            item.language,
            item.volatility,
            item.security_definer,
            item.owner,
            item.fixed_search_path,
            _migration_routine_source(item.name, item.argument_count),
        )
        for item in requirements
    ]


def _health_steps(
    *,
    server_version_num: str = "160004",
    server_version: str = "16.4",
    session_replication_role: str = "origin",
    current_user: str = "crypto_agent_runtime",
    current_user_super: bool = False,
    current_user_owner_member: bool = False,
    current_user_verifier_member: bool = False,
    current_user_reader_member: bool = True,
    current_user_database_owner: bool = False,
    current_user_schema_owner: bool = False,
    current_user_table_owner: bool = False,
    current_user_function_owner: bool = False,
    current_user_role_safe: bool = True,
    namespace_exists: bool = True,
    base_relations: tuple[str, ...] | None = None,
    base_triggers: list[tuple[object, ...]] | None = None,
    applied_migrations: list[tuple[object, ...]] | None = None,
    migrated_relations: tuple[str, ...] | None = None,
    migrated_columns: list[tuple[object, ...]] | None = None,
    migrated_constraints: list[tuple[object, ...]] | None = None,
    migrated_indexes: list[tuple[object, ...]] | None = None,
    migrated_function_definitions: list[tuple[object, ...]] | None = None,
    migrated_routines: list[tuple[object, ...]] | None = None,
    evidence_boundary: tuple[object, ...] | None = None,
    migrated_triggers: list[tuple[object, ...]] | None = None,
    bindings: tuple[tuple[str, str, str, str], ...] | None = None,
    series: tuple[tuple[str, int], ...] | None = None,
) -> list[SQLStep]:
    migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
    base_relation_names = postgres_module._BASE_TABLES if base_relations is None else base_relations
    migrated_relation_names = (
        postgres_module._MIGRATED_TABLES if migrated_relations is None else migrated_relations
    )
    migration_rows = applied_migrations
    if migration_rows is None:
        migration_rows = [(item.version, item.name, item.checksum_sha256) for item in migrations]
    binding_rows = postgres_module._EXPECTED_BINDINGS if bindings is None else bindings
    series_rows = postgres_module._EXPECTED_CANONICAL_SERIES if series is None else series
    return [
        SQLStep("SET TRANSACTION READ ONLY"),
        SQLStep("SET LOCAL search_path TO pg_catalog"),
        SQLStep(
            "server_version_num",
            [(
                server_version_num, server_version, session_replication_role,
                current_user, current_user_super, current_user_owner_member,
                current_user_verifier_member, current_user_reader_member,
                current_user_database_owner, current_user_schema_owner,
                current_user_table_owner, current_user_function_owner,
                current_user_role_safe,
            )],
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
            _trigger_rows(
                (
                    *postgres_module._BASE_TRIGGER_REQUIREMENTS,
                    *(
                        requirement
                        for requirement in postgres_module._MIGRATED_TRIGGER_REQUIREMENTS
                        if requirement.table in postgres_module._BASE_TABLES
                    ),
                )
            )
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
            "AS fixed_search_path",
            _routine_rows(postgres_module._MIGRATED_ROUTINE_REQUIREMENTS)
            if migrated_routines is None
            else migrated_routines,
        ),
        SQLStep(
            "AS owner_inputs_readable",
            [evidence_boundary or (True,) * 11],
        ),
        SQLStep(
            "FROM pg_catalog.pg_constraint AS con",
            _constraint_rows(postgres_module._OPERATIONAL_CONSTRAINT_REQUIREMENTS)
            if migrated_constraints is None
            else migrated_constraints,
        ),
        SQLStep(
            "FROM pg_catalog.pg_index AS ind",
            _index_rows(postgres_module._OPERATIONAL_INDEX_REQUIREMENTS)
            if migrated_indexes is None
            else migrated_indexes,
        ),
        SQLStep(
            "pg_catalog.pg_get_function_identity_arguments",
            _function_definition_rows(postgres_module._OPERATIONAL_FUNCTION_REQUIREMENTS)
            if migrated_function_definitions is None
            else migrated_function_definitions,
        ),
        SQLStep(
            "FROM pg_catalog.pg_trigger AS trg",
            _trigger_rows(
                (
                    *postgres_module._MIGRATED_TRIGGER_REQUIREMENTS,
                    *(
                        requirement
                        for requirement in postgres_module._BASE_TRIGGER_REQUIREMENTS
                        if requirement.table
                        in postgres_module._MIGRATED_TRIGGER_CATALOG_TABLES
                    ),
                )
            )
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
                "0016_operational_alert_outbox.sql",
                "0017_t4_observation_campaign.sql",
                "0018_t4_scenario_evidence.sql",
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
        self.assertEqual(len(postgres_module._MIGRATED_TABLES), 26)
        self.assertEqual(
            len(postgres_module._BASE_TRIGGER_REQUIREMENTS)
            + len(postgres_module._MIGRATED_TRIGGER_REQUIREMENTS),
            165,
        )
        trigger_requirements = (
            *postgres_module._BASE_TRIGGER_REQUIREMENTS,
            *postgres_module._MIGRATED_TRIGGER_REQUIREMENTS,
        )
        self.assertEqual(
            len({requirement.name for requirement in trigger_requirements}),
            len(trigger_requirements),
        )
        self.assertIn(
            "t4_observation_scenario_trial_requests_append_only_truncate_gua",
            {requirement.name for requirement in trigger_requirements},
        )
        for requirement in trigger_requirements:
            self.assertLessEqual(len(requirement.name.encode("ascii")), 63)
            self.assertIs(type(requirement.type_mask), int)
            self.assertIs(type(requirement.deferrable), bool)
            self.assertIs(type(requirement.initially_deferred), bool)
        for function_requirement in postgres_module._OPERATIONAL_FUNCTION_REQUIREMENTS:
            self.assertEqual(
                function_requirement.source_sha256,
                postgres_module._definition_sha256(
                    _migration_function_source(function_requirement.name)
                ),
            )
            self.assertRegex(function_requirement.source_sha256, r"^[0-9a-f]{64}$")
        evidence_sql = (
            PROJECT_ROOT / "db/migrations/0018_t4_scenario_evidence.sql"
        ).read_text(encoding="utf-8")
        named_constraints = set(re.findall(
            r"\bCONSTRAINT\s+([a-z0-9_]+)", evidence_sql, re.IGNORECASE
        ))
        manifested_constraints = {
            item.name for item in postgres_module._OPERATIONAL_CONSTRAINT_REQUIREMENTS
        }
        self.assertEqual(len(named_constraints), 53)
        self.assertTrue(named_constraints <= manifested_constraints)

    def test_settings_repr_never_contains_dsn_secret(self) -> None:
        settings = PostgresSettings(dsn="opaque-sensitive-dsn")
        self.assertNotIn("opaque-sensitive-dsn", repr(settings))

    def test_connector_failure_is_sanitized_and_drops_cause(self) -> None:
        settings = PostgresSettings(dsn="opaque-sensitive-dsn")
        with (
            patch(
                "crypto_agent.postgres.importlib.import_module",
                return_value=_BrokenDriver,
            ),
            self.assertRaises(PostgresUnavailableError) as raised,
        ):
            PsycopgConnectionFactory(settings)()
        self.assertEqual(str(raised.exception), "PostgreSQL is unavailable")
        self.assertNotIn("opaque-sensitive", str(raised.exception))
        self.assertIsNone(raised.exception.__cause__)

    def test_migration_is_discovered_with_sha256(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
        self.assertEqual(
            [item.version for item in migrations],
            ["0011", "0012", "0013", "0014", "0015", "0016", "0017", "0018"],
        )
        for migration in migrations:
            self.assertRegex(migration.checksum_sha256, r"^[0-9a-f]{64}$")

    def test_t4_operational_ingest_migration_is_append_only_and_idempotent(self) -> None:
        sql = (PROJECT_ROOT / "db/migrations/0014_t4_operational_ingest.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("CREATE TABLE crypto_agent.t4_ingestion_batches", sql)
        self.assertIn("CREATE TABLE crypto_agent.t4_canonical_candles", sql)
        self.assertIn("raw_payload_hash        crypto_agent.sha256_hex NOT NULL UNIQUE", sql)
        self.assertIn("t4_ingestion_batches_append_only_row_guard", sql)
        self.assertIn("t4_canonical_candles_append_only_row_guard", sql)
        self.assertNotIn("order_routes", sql.lower())

    def test_t4_futures_evidence_migration_is_append_only(self) -> None:
        sql = (PROJECT_ROOT / "db/migrations/0015_t4_futures_evidence.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("CREATE TABLE crypto_agent.t4_futures_snapshots", sql)
        self.assertIn("CREATE TABLE crypto_agent.t4_orderbook_levels", sql)
        self.assertIn("CREATE TABLE crypto_agent.t4_contract_transition_evidence", sql)
        self.assertIn("bridge_schema_version IN (2, 3)", sql)
        self.assertIn("basis_reference_source = 'plus500_t4_index_v1'", sql)
        self.assertIn("evidence_source = 'plus500_t4_futures_v1'", sql)
        self.assertIn("t4_futures_snapshots_append_only_row_guard", sql)
        self.assertIn("t4_orderbook_levels_append_only_truncate_guard", sql)

    def test_operational_alert_outbox_is_strict_and_append_only(self) -> None:
        sql = (PROJECT_ROOT / "db/migrations/0016_operational_alert_outbox.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("CREATE TABLE crypto_agent.alert_delivery_outbox", sql)
        self.assertIn("CREATE TABLE crypto_agent.alert_delivery_attempts", sql)
        self.assertIn("channel = 'stdout_json'", sql)
        self.assertIn("destination = 'process_stdout'", sql)
        self.assertIn("payload->>'decision' = 'ALERT'", sql)
        self.assertIn("payload->'read_only'", sql)
        self.assertIn("UNIQUE (alert_id, channel, destination)", sql)
        self.assertIn("idempotency_key          crypto_agent.sha256_hex NOT NULL UNIQUE", sql)
        self.assertIn("monitoring_policy_id     text NOT NULL", sql)
        self.assertIn("monitoring_policy_hash   crypto_agent.sha256_hex NOT NULL", sql)
        self.assertIn("retention_days           smallint NOT NULL", sql)
        self.assertIn("enforce_alert_delivery_attempt", sql)
        self.assertIn("alert delivery already reached a terminal outcome", sql)
        self.assertIn("NEW.request_payload_hash <> outbox_row.payload_hash", sql)
        self.assertIn("NEW.attempt_no > outbox_row.max_attempts", sql)
        self.assertIn("NEW.finished_at > outbox_row.expires_at", sql)
        self.assertIn("alert_delivery_outbox_append_only_row_guard", sql)
        self.assertIn("alert_delivery_attempts_append_only_truncate_guard", sql)

    def test_t4_observation_campaign_is_frozen_chained_and_append_only(self) -> None:
        sql = (PROJECT_ROOT / "db/migrations/0017_t4_observation_campaign.sql").read_text(
            encoding="utf-8"
        )
        for table in (
            "t4_observation_campaigns",
            "t4_observation_research_inputs",
            "t4_observation_cycles",
            "t4_observation_session_events",
            "t4_observation_quality_reports",
        ):
            self.assertIn(f"CREATE TABLE crypto_agent.{table}", sql)
            self.assertIn(f"'{table}'", sql)
        self.assertIn("table_name || '_append_only_row_guard'", sql)
        self.assertIn("table_name || '_append_only_truncate_guard'", sql)
        self.assertIn("bridge_schema_version IN (2, 3, 4, 5)", sql)
        self.assertIn("environment IN ('t4_simulator', 'live_t4')", sql)
        self.assertIn("planned_ends_at >= started_at + interval '672 hours'", sql)
        self.assertIn("enforce_t4_observation_cycle_chain", sql)
        self.assertIn("enforce_t4_observation_event_chain", sql)
        self.assertIn("enforce_t4_observation_campaign_start", sql)
        self.assertIn("enforce_t4_observation_final_report", sql)
        self.assertIn("enforce_t4_observation_research_input", sql)
        self.assertIn("analysis_input_hash", sql)
        self.assertIn("observation exact batch-to-run linkage is invalid", sql)
        self.assertIn("observation cycle cannot be predeclared or backfilled", sql)
        self.assertIn("previous_cycle_hash <> previous_row.content_hash", sql)
        self.assertIn("previous_event_hash <> previous_row.content_hash", sql)
        self.assertIn("NOT v1_gate_passed", sql)
        self.assertNotIn("CREATE TABLE crypto_agent.orders", sql)

    def test_t4_scenario_evidence_is_restricted_and_database_derived(self) -> None:
        sql = (
            PROJECT_ROOT / "db/migrations/0018_t4_scenario_evidence.sql"
        ).read_text(encoding="utf-8")
        self.assertIn("CREATE ROLE crypto_agent_evidence_owner NOLOGIN", sql)
        self.assertIn("CREATE ROLE crypto_agent_evidence_verifier NOLOGIN", sql)
        self.assertIn("UNIQUE (evidence_key_fingerprint_sha256, sequence_no)", sql)
        self.assertIn("scenario_verified IS DISTINCT FROM TRUE", sql)
        self.assertIn("passive scenario evidence is not complete yet", sql)
        self.assertIn(
            "CREATE OR REPLACE FUNCTION crypto_agent.t4_observation_gate_is_verified(",
            sql,
        )
        self.assertRegex(
            sql,
            r"NEW\.v1_gate_passed IS DISTINCT FROM\s+"
            r"crypto_agent\.t4_observation_gate_is_verified\(\s*"
            r"NEW\.campaign_id, NEW\.bridge_checkpoint_event_id\s*\)",
        )
        self.assertIn("campaign_checkpoint", sql)
        self.assertIn("bridge_checkpoint_sequence_no", sql)
        self.assertIn("SELECT max(head.sequence_no)", sql)
        self.assertIn("t4_scenario_alert_finalization_guard", sql)
        self.assertIn(
            "cannot append an alert to finalized T4 scenario evidence", sql
        )
        self.assertIn("SECURITY DEFINER\nSET search_path = pg_catalog, crypto_agent", sql)
        self.assertIn("event.campaign_id IS DISTINCT FROM p_campaign_id", sql)
        self.assertIn("cycle_interval_seconds", sql)
        self.assertIn("pre_cycle.campaign_id = p_campaign_id", sql)
        self.assertIn("pre_cycle.outcome = 'success'", sql)
        self.assertIn(
            "secs => campaign_row.cycle_interval_seconds",
            sql,
        )
        self.assertIn("scenario evidence is not in exact signed chain order", sql)
        self.assertIn("alert_delivery_attempts attempt", sql)
        for table in (
            "t4_bridge_evidence_keys",
            "t4_bridge_observation_events",
            "t4_replay_verifier_attestations",
            "t4_observation_scenario_trial_requests",
            "t4_observation_scenario_trials",
        ):
            self.assertIn(f"{table}_append_only_row_guard", sql)
            self.assertIn(f"{table}_append_only_truncate_guard", sql)
        self.assertIn("record_verified_t4_replay_attestation", sql)
        self.assertIn("t4_replay_attestations_are_verified", sql)
        self.assertIn("replay_attestation_ids", sql)
        self.assertIn("replay_limit = 120", sql)
        self.assertIn("external_delivery_eligible", sql)
        self.assertIn("attested < campaign_row.planned_ends_at", sql)
        self.assertNotIn(
            "attested NOT BETWEEN campaign_row.planned_ends_at", sql
        )
        self.assertRegex(
            sql,
            r"attestation\.attested_at > checkpoint_row\.event_at",
        )
        self.assertIn(
            "replay attestation does not match an open frozen campaign", sql
        )

    def test_health_check_requires_restricted_evidence_routines(self) -> None:
        routines = _routine_rows(postgres_module._MIGRATED_ROUTINE_REQUIREMENTS)
        routines = [
            row for row in routines if row[0] != "t4_observation_gate_is_verified"
        ]
        health = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_routines=routines))
        )
        self.assertEqual(health.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "routine:t4_observation_gate_is_verified",
            health.missing_schema_objects,
        )

        routines = _routine_rows(postgres_module._MIGRATED_ROUTINE_REQUIREMENTS)
        gate_two = next(
            index for index, row in enumerate(routines)
            if row[0] == "t4_observation_gate_is_verified" and row[1] == 2
        )
        row = list(routines[gate_two])
        row[8] = "BEGIN RETURN TRUE; END;"
        routines[gate_two] = tuple(row)
        health = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_routines=routines))
        )
        self.assertEqual(health.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "routine:t4_observation_gate_is_verified",
            health.missing_schema_objects,
        )

    def test_routine_catalog_includes_attested_trigger_functions(self) -> None:
        requirements = tuple(
            requirement
            for requirement in postgres_module._MIGRATED_ROUTINE_REQUIREMENTS
            if requirement.name == "forbid_append_only_change"
        )
        connection = FakeConnection([
            SQLStep("AS fixed_search_path", _routine_rows(requirements))
        ])

        actual = postgres_module._catalog_routines(
            connection.scripted_cursor,
            requirements,
        )

        self.assertIn(("forbid_append_only_change", 0), actual)
        query = connection.scripted_cursor.executions[0][0]
        self.assertNotIn("proc.prorettype <>", query)

    def test_health_check_requires_evidence_roles_acl_and_pgcrypto_schema(self) -> None:
        boundary = (
            False, True, True, True, True, True, True, True, True, True, True
        )
        health = check_postgres_health(
            lambda: FakeConnection(_health_steps(evidence_boundary=boundary))
        )
        self.assertEqual(health.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn("boundary:pgcrypto_schema", health.missing_schema_objects)

        boundary = (
            True, True, True, True, False, True, True, True, True, True, True
        )
        health = check_postgres_health(
            lambda: FakeConnection(_health_steps(evidence_boundary=boundary))
        )
        self.assertIn(
            "boundary:evidence_roles_separated", health.missing_schema_objects
        )

        boundary = (
            True, True, True, True, True, True, False, True, True, True, True
        )
        health = check_postgres_health(
            lambda: FakeConnection(_health_steps(evidence_boundary=boundary))
        )
        self.assertIn("boundary:evidence_table_acl", health.missing_schema_objects)

    def test_reference_price_policy_migration_is_exact_and_guarded(self) -> None:
        sql = (PROJECT_ROOT / "db/migrations/0012_reference_price_policy_contract.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "CREATE TABLE crypto_agent.reference_price_manifests",
            sql,
        )
        self.assertIn("CREATE TABLE crypto_agent.reference_price_provenance", sql)
        self.assertIn(
            "CREATE OR REPLACE FUNCTION crypto_agent.enforce_v1_canonical_manifest_policy()",
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
            "CREATE OR REPLACE FUNCTION crypto_agent.enforce_v1_reference_price_manifest()",
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
            "CREATE OR REPLACE FUNCTION crypto_agent.enforce_v1_reference_price_exact_provenance()",
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
        sql = (PROJECT_ROOT / "db/migrations/0011_canonical_candle_provenance.sql").read_text(
            encoding="utf-8"
        )
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
            ("0011", "0012", "0013", "0014", "0015", "0016", "0017", "0018"),
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
        connection = FakeConnection(_health_steps(session_replication_role="replica"))
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertTrue(health.database_reachable)
        self.assertEqual(health.status_code, "POSTGRES_SESSION_UNSAFE")

    def test_health_check_rejects_privileged_or_unseparated_runtime(self) -> None:
        for override in (
            {"current_user_super": True},
            {"current_user_owner_member": True},
            {"current_user_verifier_member": True},
            {"current_user_reader_member": False},
            {"current_user_database_owner": True},
            {"current_user_schema_owner": True},
            {"current_user_table_owner": True},
            {"current_user_function_owner": True},
            {"current_user_role_safe": False},
        ):
            with self.subTest(override=override):
                health = check_postgres_health(
                    lambda override=override: FakeConnection(
                        _health_steps(**override)
                    )
                )
                self.assertEqual(health.status_code, "POSTGRES_SESSION_UNSAFE")

    def test_empty_supplied_migration_manifest_cannot_bypass_packaged_manifest(self) -> None:
        connection = FakeConnection(_health_steps(applied_migrations=[]))
        health = check_postgres_health(lambda: connection, expected_migrations=())
        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "MIGRATIONS_PENDING")
        self.assertEqual(
            health.missing_migrations,
            ("0011", "0012", "0013", "0014", "0015", "0016", "0017", "0018"),
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
        base_relations_step = next(
            step for step in steps if step.contains == "FROM pg_catalog.pg_class AS rel"
        )
        base_relations_step.rows = [
            (name, "v" if name == "candles" else "r") for name in postgres_module._BASE_TABLES
        ]
        connection = FakeConnection(steps)
        health = check_postgres_health(lambda: connection)
        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "BASE_SCHEMA_MISSING")
        self.assertIn("table:candles", health.missing_schema_objects)

    def test_health_check_rejects_missing_migrated_column(self) -> None:
        columns = _column_rows(postgres_module._MIGRATED_COLUMN_REQUIREMENTS)
        columns = [
            row for row in columns if row[:2] != ("reference_price_manifests", "inputs_hash")
        ]
        connection = FakeConnection(_health_steps(migrated_columns=columns))

        health = check_postgres_health(lambda: connection)

        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "column:reference_price_manifests.inputs_hash",
            health.missing_schema_objects,
        )

    def test_health_check_requires_observation_ledger_contract(self) -> None:
        columns = _column_rows(postgres_module._MIGRATED_COLUMN_REQUIREMENTS)
        columns = [
            row
            for row in columns
            if row[:2] != ("t4_observation_campaigns", "frozen_baseline_hash")
        ]
        missing_column = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_columns=columns))
        )
        self.assertEqual(missing_column.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "column:t4_observation_campaigns.frozen_baseline_hash",
            missing_column.missing_schema_objects,
        )

        columns = _column_rows(postgres_module._MIGRATED_COLUMN_REQUIREMENTS)
        columns = [
            row
            for row in columns
            if row[:2] != ("t4_observation_cycles", "analysis_input_hash")
        ]
        missing_analysis_hash = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_columns=columns))
        )
        self.assertEqual(
            missing_analysis_hash.status_code,
            "MIGRATED_SCHEMA_MISSING",
        )
        self.assertIn(
            "column:t4_observation_cycles.analysis_input_hash",
            missing_analysis_hash.missing_schema_objects,
        )

        constraints = _constraint_rows(
            postgres_module._OPERATIONAL_CONSTRAINT_REQUIREMENTS
        )
        constraints = [
            row
            for row in constraints
            if row[1] != "t4_observation_cycles_campaign_id_scope_key_sequence_no_key"
        ]
        missing_constraint = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_constraints=constraints))
        )
        self.assertEqual(missing_constraint.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "constraint:t4_observation_cycles."
            "t4_observation_cycles_campaign_id_scope_key_sequence_no_key",
            missing_constraint.missing_schema_objects,
        )

        constraints = _constraint_rows(
            postgres_module._OPERATIONAL_CONSTRAINT_REQUIREMENTS
        )
        constraints = [
            row
            for row in constraints
            if row[1]
            != "t4_observation_cycles_campaign_id_scope_key_sequence_no_ex_fkey"
        ]
        missing_exact_link = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_constraints=constraints))
        )
        self.assertEqual(missing_exact_link.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "constraint:t4_observation_cycles."
            "t4_observation_cycles_campaign_id_scope_key_sequence_no_ex_fkey",
            missing_exact_link.missing_schema_objects,
        )

        indexes = _index_rows(postgres_module._OPERATIONAL_INDEX_REQUIREMENTS)
        indexes = [
            row
            for row in indexes
            if row[1] != "t4_observation_session_events_scenario_idx"
        ]
        missing_index = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_indexes=indexes))
        )
        self.assertEqual(missing_index.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "index:t4_observation_session_events."
            "t4_observation_session_events_scenario_idx",
            missing_index.missing_schema_objects,
        )

        indexes = _index_rows(postgres_module._OPERATIONAL_INDEX_REQUIREMENTS)
        indexes = [
            row
            for row in indexes
            if row[1] != "t4_observation_research_inputs_campaign_time_idx"
        ]
        missing_input_index = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_indexes=indexes))
        )
        self.assertEqual(missing_input_index.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "index:t4_observation_research_inputs."
            "t4_observation_research_inputs_campaign_time_idx",
            missing_input_index.missing_schema_objects,
        )

        triggers = _trigger_rows(postgres_module._MIGRATED_TRIGGER_REQUIREMENTS)
        triggers = [
            row
            for row in triggers
            if row[0] != "t4_observation_event_chain_guard"
        ]
        missing_trigger = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_triggers=triggers))
        )
        self.assertEqual(missing_trigger.status_code, "MIGRATED_TRIGGERS_MISSING")
        self.assertIn(
            "t4_observation_event_chain_guard",
            missing_trigger.missing_triggers,
        )

        triggers = _trigger_rows(postgres_module._MIGRATED_TRIGGER_REQUIREMENTS)
        triggers = [
            row
            for row in triggers
            if row[0] != "t4_observation_research_input_guard"
        ]
        missing_input_guard = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_triggers=triggers))
        )
        self.assertEqual(
            missing_input_guard.status_code,
            "MIGRATED_TRIGGERS_MISSING",
        )
        self.assertIn(
            "t4_observation_research_input_guard",
            missing_input_guard.missing_triggers,
        )

    def test_health_check_rejects_missing_operational_constraint(self) -> None:
        cases = (
            "alert_delivery_outbox_alert_fk",
            "alert_delivery_outbox_idempotency_key_key",
            "alert_delivery_outbox_channel_check",
            "alert_delivery_outbox_destination_check",
            "alert_delivery_outbox_payload_check4",
            "alert_delivery_outbox_payload_check7",
        )
        for name in cases:
            with self.subTest(name=name):
                rows = _constraint_rows(postgres_module._OPERATIONAL_CONSTRAINT_REQUIREMENTS)
                rows = [row for row in rows if row[1] != name]
                connection = FakeConnection(_health_steps(migrated_constraints=rows))

                health = check_postgres_health(lambda current=connection: current)

                self.assertFalse(health.healthy)
                self.assertEqual(health.status_code, "MIGRATED_SCHEMA_MISSING")
                self.assertIn(
                    f"constraint:alert_delivery_outbox.{name}",
                    health.missing_schema_objects,
                )

    def test_health_check_uses_deterministic_catalog_search_path(self) -> None:
        connection = FakeConnection(_health_steps())

        health = check_postgres_health(lambda: connection)

        self.assertTrue(health.healthy)
        queries = [query for query, _ in connection.scripted_cursor.executions]
        self.assertEqual(queries[0], "SET TRANSACTION READ ONLY")
        self.assertEqual(queries[1], "SET LOCAL search_path TO pg_catalog")
        payload_requirement = next(
            item
            for item in postgres_module._OPERATIONAL_CONSTRAINT_REQUIREMENTS
            if item.name == "alert_delivery_outbox_payload_check1"
        )
        normalized = postgres_module._normalize_catalog_definition(
            payload_requirement.definition
        )
        self.assertIn("ARRAY['schema_version'::text", normalized)
        self.assertIn("'retention_days'::text]", normalized)
        self.assertNotIn("ARRAY[ 'schema_version'::text", normalized)

    def test_health_check_rejects_changed_or_unvalidated_constraint(self) -> None:
        rows = _constraint_rows(postgres_module._OPERATIONAL_CONSTRAINT_REQUIREMENTS)
        index = next(
            index
            for index, row in enumerate(rows)
            if row[1] == "alert_delivery_outbox_payload_check4"
        )
        rows[index] = (*rows[index][:3], True, "CHECK (true)")
        changed = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_constraints=rows))
        )
        self.assertEqual(changed.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "constraint:alert_delivery_outbox.alert_delivery_outbox_payload_check4",
            changed.missing_schema_objects,
        )

        rows = _constraint_rows(postgres_module._OPERATIONAL_CONSTRAINT_REQUIREMENTS)
        index = next(
            index for index, row in enumerate(rows) if row[1] == "alert_delivery_outbox_alert_fk"
        )
        rows[index] = (*rows[index][:3], False, rows[index][4])
        unvalidated = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_constraints=rows))
        )
        self.assertEqual(unvalidated.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "constraint:alert_delivery_outbox.alert_delivery_outbox_alert_fk",
            unvalidated.missing_schema_objects,
        )

    def test_health_check_rejects_missing_or_invalid_operational_index(self) -> None:
        rows = _index_rows(postgres_module._OPERATIONAL_INDEX_REQUIREMENTS)
        rows = [row for row in rows if row[1] != "alert_delivery_outbox_due_idx"]
        missing = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_indexes=rows))
        )
        self.assertEqual(missing.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "index:alert_delivery_outbox.alert_delivery_outbox_due_idx",
            missing.missing_schema_objects,
        )

        rows = _index_rows(postgres_module._OPERATIONAL_INDEX_REQUIREMENTS)
        rows[0] = (*rows[0][:3], False, *rows[0][4:])
        invalid = check_postgres_health(
            lambda: FakeConnection(_health_steps(migrated_indexes=rows))
        )
        self.assertEqual(invalid.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            f"index:{rows[0][0]}.{rows[0][1]}",
            invalid.missing_schema_objects,
        )

    def test_health_check_rejects_noop_alert_delivery_trigger_function(self) -> None:
        requirement = postgres_module._OPERATIONAL_FUNCTION_REQUIREMENTS[0]
        no_op_rows = [
            (
                requirement.name,
                requirement.language,
                requirement.volatility,
                requirement.security_definer,
                "BEGIN RETURN NEW; END;",
            )
        ]
        connection = FakeConnection(_health_steps(migrated_function_definitions=no_op_rows))

        health = check_postgres_health(lambda: connection)

        self.assertFalse(health.healthy)
        self.assertEqual(health.status_code, "MIGRATED_SCHEMA_MISSING")
        self.assertIn(
            "function_definition:enforce_alert_delivery_attempt",
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
            "unexpected_binding:plus500_t4_futures_v1:plus500_t4:LTC/USD:LTC-FUTURES-FRONT",
            health.missing_seeds,
        )
        self.assertIn("unexpected_series:BTC/USD:3600", health.missing_seeds)

    def test_health_check_rejects_migration_drift_and_ahead_database(self) -> None:
        migrations = discover_migrations(PROJECT_ROOT / "db/migrations")
        valid_rows = [(item.version, item.name, item.checksum_sha256) for item in migrations]
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
        rows = [row for row in rows if row[0] != "reference_price_exact_provenance_link_guard"]
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

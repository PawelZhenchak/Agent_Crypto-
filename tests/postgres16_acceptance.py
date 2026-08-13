from __future__ import annotations

import argparse
import base64
import hashlib
import io
import json
import os
import re
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from crypto_agent.domain import (  # noqa: E402
    BasisReference,
    Candle,
    DataQualityReport,
    Decision,
    FuturesEvidence,
    OrderBookLevel,
    ReferencePriceObservation,
    ReferencePriceSnapshot,
    ResearchReport,
    RiskAssessment,
    SessionStatus,
)
from crypto_agent.monitoring import MonitoringRepository  # noqa: E402
from crypto_agent.monitoring_policy import MonitoringPolicy  # noqa: E402
from crypto_agent.observation import (  # noqa: E402
    ObservationCampaign,
    ObservationRepository,
)
from crypto_agent.observation_policy import ObservationPolicy  # noqa: E402
from crypto_agent.observation_scenarios import (  # noqa: E402
    BridgeEventPage,
    BridgeEventPayload,
    PostgresScenarioEvidenceRepository,
    ScenarioEvidenceError,
    SignedBridgeEvent,
)
from crypto_agent.postgres import (  # noqa: E402
    PostgresOperationError,
    PostgresSettings,
    PsycopgConnectionFactory,
    apply_migrations,
    apply_v1_seeds,
    check_postgres_health,
    discover_migrations,
    transaction,
)
from crypto_agent.providers.base import ProviderBatch  # noqa: E402
from crypto_agent.resource_paths import (  # noqa: E402
    default_monitoring_policy_path,
    default_observation_policy_path,
    default_v1_seed_path,
)
from crypto_agent.t4_ingest import T4IngestRepository  # noqa: E402

_EVIDENCE_ROLE = "crypto_agent_evidence_acceptance"
_RUNTIME_ROLE = "crypto_agent_runtime_acceptance"
_REPLAY_READER_ROLE = "crypto_agent_replay_reader_acceptance"
_UNSAFE_EVIDENCE_ROLE = "crypto_agent_evidence_unsafe_acceptance"
_INDIRECT_EVIDENCE_ROLE = "crypto_agent_evidence_indirect_acceptance"
_INDIRECT_EVIDENCE_LOGIN = "crypto_agent_evidence_indirect_login_acceptance"
_CAMPAIGN_ID = "581ef534-1f26-4f06-95ce-5873911044d6"


def _factory() -> PsycopgConnectionFactory:
    return PsycopgConnectionFactory(PostgresSettings.from_env())


def _role_factory(role: str):  # type: ignore[no-untyped-def]
    if role not in {
        _EVIDENCE_ROLE,
        _RUNTIME_ROLE,
        _REPLAY_READER_ROLE,
        _UNSAFE_EVIDENCE_ROLE,
        _INDIRECT_EVIDENCE_LOGIN,
    }:
        raise AssertionError("invalid acceptance role")

    def connect():  # type: ignore[no-untyped-def]
        from psycopg.conninfo import conninfo_to_dict, make_conninfo

        admin_settings = PostgresSettings.from_env()
        role_parameters = conninfo_to_dict(admin_settings.dsn)
        role_parameters["user"] = role
        role_parameters.pop("password", None)
        role_dsn = make_conninfo(**role_parameters)
        return PsycopgConnectionFactory(PostgresSettings(
            dsn=role_dsn,
            connect_timeout_seconds=admin_settings.connect_timeout_seconds,
        ))()

    return connect


def _provision_acceptance_roles() -> None:
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_roles WHERE rolname = '{_EVIDENCE_ROLE}'
                    ) THEN
                        CREATE ROLE {_EVIDENCE_ROLE} LOGIN NOSUPERUSER NOCREATEDB
                            NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS;
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_roles WHERE rolname = '{_RUNTIME_ROLE}'
                    ) THEN
                        CREATE ROLE {_RUNTIME_ROLE} LOGIN NOSUPERUSER NOCREATEDB
                            NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS;
                    END IF;
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_roles
                        WHERE rolname = '{_REPLAY_READER_ROLE}'
                    ) THEN
                        CREATE ROLE {_REPLAY_READER_ROLE} LOGIN NOSUPERUSER
                            NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION
                            NOBYPASSRLS;
                    END IF;
                END;
                $$;
                ALTER ROLE {_EVIDENCE_ROLE} LOGIN NOSUPERUSER NOCREATEDB
                    NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS;
                ALTER ROLE {_RUNTIME_ROLE} LOGIN NOSUPERUSER NOCREATEDB
                    NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS;
                ALTER ROLE {_REPLAY_READER_ROLE} LOGIN NOSUPERUSER NOCREATEDB
                    NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS;
                REVOKE crypto_agent_evidence_owner FROM {_EVIDENCE_ROLE},
                    {_RUNTIME_ROLE}, {_REPLAY_READER_ROLE};
                REVOKE crypto_agent_evidence_reader FROM {_EVIDENCE_ROLE};
                REVOKE crypto_agent_evidence_verifier FROM {_RUNTIME_ROLE},
                    {_REPLAY_READER_ROLE};
                GRANT crypto_agent_evidence_verifier TO {_EVIDENCE_ROLE};
                GRANT crypto_agent_evidence_reader TO {_RUNTIME_ROLE};
                GRANT crypto_agent_evidence_reader TO {_REPLAY_READER_ROLE};
                GRANT SELECT, INSERT ON
                    crypto_agent.t4_ingestion_batches,
                    crypto_agent.t4_canonical_candles,
                    crypto_agent.t4_futures_snapshots,
                    crypto_agent.t4_orderbook_levels,
                    crypto_agent.t4_contract_transition_evidence,
                    crypto_agent.research_runs,
                    crypto_agent.research_run_events,
                    crypto_agent.research_run_inputs,
                    crypto_agent.research_artifacts,
                    crypto_agent.data_quality_incidents,
                    crypto_agent.data_quality_incident_events,
                    crypto_agent.alerts,
                    crypto_agent.alert_events,
                    crypto_agent.alert_delivery_outbox,
                    crypto_agent.alert_delivery_attempts,
                    crypto_agent.t4_observation_campaigns,
                    crypto_agent.t4_observation_research_inputs,
                    crypto_agent.t4_observation_cycles,
                    crypto_agent.t4_observation_session_events,
                    crypto_agent.t4_observation_quality_reports
                    TO {_RUNTIME_ROLE};
                GRANT SELECT ON
                    crypto_agent.schema_migrations,
                    crypto_agent.data_sources,
                    crypto_agent.exchanges,
                    crypto_agent.markets,
                    crypto_agent.risk_policies,
                    crypto_agent.market_data_source_bindings,
                    crypto_agent.canonical_candle_series,
                    crypto_agent.t4_runtime_config,
                    crypto_agent.t4_ingestion_batches,
                    crypto_agent.research_runs,
                    crypto_agent.research_run_events,
                    crypto_agent.research_run_inputs,
                    crypto_agent.research_artifacts,
                    crypto_agent.alerts,
                    crypto_agent.alert_delivery_outbox,
                    crypto_agent.alert_delivery_attempts
                    TO {_RUNTIME_ROLE};
                GRANT UPDATE (source_id) ON crypto_agent.data_sources
                    TO {_RUNTIME_ROLE};
                GRANT UPDATE (alert_delivery_outbox_id)
                    ON crypto_agent.alert_delivery_outbox
                    TO {_RUNTIME_ROLE};
                GRANT USAGE, SELECT ON SEQUENCE
                    crypto_agent.t4_ingestion_batches_t4_batch_id_seq,
                    crypto_agent.t4_canonical_candles_t4_candle_id_seq,
                    crypto_agent.t4_futures_snapshots_t4_snapshot_id_seq,
                    crypto_agent.t4_contract_transition_evidence_t4_transition_id_seq,
                    crypto_agent.research_runs_research_run_id_seq,
                    crypto_agent.research_run_events_research_run_event_id_seq,
                    crypto_agent.research_run_inputs_research_run_input_id_seq,
                    crypto_agent.research_artifacts_research_artifact_id_seq,
                    crypto_agent.data_quality_incidents_data_quality_incident_id_seq,
                    crypto_agent.data_quality_incident_events_data_quality_incident_event_id_seq,
                    crypto_agent.alerts_alert_id_seq,
                    crypto_agent.alert_events_alert_event_id_seq,
                    crypto_agent.alert_delivery_outbox_alert_delivery_outbox_id_seq,
                    crypto_agent.alert_delivery_attempts_alert_delivery_attempt_id_seq,
                    crypto_agent.t4_observation_research_input_observation_research_input_id_seq,
                    crypto_agent.t4_observation_cycles_observation_cycle_id_seq,
                    crypto_agent.t4_observation_session_events_observation_session_event_id_seq,
                    crypto_agent.t4_observation_quality_report_observation_quality_report_id_seq
                    TO {_RUNTIME_ROLE};
                """
            )
        connection.commit()
    finally:
        connection.close()


def _assert_runtime_identity_sequence_grants() -> None:
    identity_columns = (
        ("t4_ingestion_batches", "t4_batch_id"),
        ("t4_canonical_candles", "t4_candle_id"),
        ("t4_futures_snapshots", "t4_snapshot_id"),
        ("t4_contract_transition_evidence", "t4_transition_id"),
        ("research_runs", "research_run_id"),
        ("research_run_events", "research_run_event_id"),
        ("research_run_inputs", "research_run_input_id"),
        ("research_artifacts", "research_artifact_id"),
        ("data_quality_incidents", "data_quality_incident_id"),
        ("data_quality_incident_events", "data_quality_incident_event_id"),
        ("alerts", "alert_id"),
        ("alert_events", "alert_event_id"),
        ("alert_delivery_outbox", "alert_delivery_outbox_id"),
        ("alert_delivery_attempts", "alert_delivery_attempt_id"),
        ("t4_observation_research_inputs", "observation_research_input_id"),
        ("t4_observation_cycles", "observation_cycle_id"),
        ("t4_observation_session_events", "observation_session_event_id"),
        ("t4_observation_quality_reports", "observation_quality_report_id"),
    )
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            for table_name, column_name in identity_columns:
                db_cursor.execute(
                    "SELECT pg_get_serial_sequence(%s, %s)",
                    (f"crypto_agent.{table_name}", column_name),
                )
                row = db_cursor.fetchone()
                sequence_name = None if row is None else row[0]
                if sequence_name is None:
                    raise AssertionError(
                        "runtime identity sequence is missing: "
                        f"crypto_agent.{table_name}.{column_name}"
                    )
                db_cursor.execute(
                    "SELECT has_sequence_privilege(%s, %s, 'USAGE') "
                    "AND has_sequence_privilege(%s, %s, 'SELECT')",
                    (
                        _RUNTIME_ROLE,
                        sequence_name,
                        _RUNTIME_ROLE,
                        sequence_name,
                    ),
                )
                privilege_row = db_cursor.fetchone()
                if privilege_row is None or privilege_row[0] is not True:
                    raise AssertionError(
                        "runtime identity sequence grants are incomplete: "
                        f"{sequence_name}"
                    )
        connection.commit()
    finally:
        connection.close()


def _assert_runtime_row_lock_grants() -> None:
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "SELECT has_column_privilege(%s, "
                "'crypto_agent.data_sources', 'source_id', 'UPDATE'), "
                "has_column_privilege(%s, 'crypto_agent.alert_delivery_outbox', "
                "'alert_delivery_outbox_id', 'UPDATE'), "
                "has_column_privilege(%s, "
                "'crypto_agent.data_sources', 'source_key', 'UPDATE'), "
                "has_column_privilege(%s, 'crypto_agent.alert_delivery_outbox', "
                "'payload', 'UPDATE')",
                (_RUNTIME_ROLE,) * 4,
            )
            row = db_cursor.fetchone()
        connection.commit()
    finally:
        connection.close()
    if row != (True, True, False, False):
        raise AssertionError(f"runtime row-lock grants are unsafe: {row!r}")


def _execute_file(path: Path) -> None:
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(path.read_text(encoding="utf-8"))
        connection.commit()
    finally:
        connection.close()


def _scalar(query: str, params: tuple[object, ...] = ()) -> object:
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(query, params)
            row = db_cursor.fetchone()
        connection.commit()
    finally:
        connection.close()
    if row is None:
        raise AssertionError("PostgreSQL acceptance query returned no row")
    return row[0]


def _expect_sqlstate(query: str, sqlstate: str) -> None:
    _expect_sqlstate_after((), query, sqlstate)


def _expect_sqlstate_after(
    setup_queries: tuple[str, ...],
    query: str,
    sqlstate: str,
    connection_factory=None,  # type: ignore[no-untyped-def]
) -> None:
    connection = (connection_factory or _factory())()
    try:
        with connection.cursor() as db_cursor:
            for setup_query in setup_queries:
                db_cursor.execute(setup_query)
            try:
                db_cursor.execute(query)
            except Exception as exc:
                connection.rollback()
                if getattr(exc, "sqlstate", None) != sqlstate:
                    raise AssertionError(
                        f"expected SQLSTATE {sqlstate}, got {getattr(exc, 'sqlstate', None)}"
                    ) from exc
            else:
                connection.rollback()
                raise AssertionError(f"query unexpectedly succeeded: {query}")
    finally:
        connection.close()


def _assert_ready() -> None:
    migrations = discover_migrations(PROJECT_ROOT / "db" / "migrations")
    health = check_postgres_health(
        _role_factory(_RUNTIME_ROLE), expected_migrations=migrations
    )
    if not health.healthy or health.status_code != "READY":
        raise AssertionError(f"PostgreSQL health is not READY: {health!r}")
    if health.server_version is None or not health.server_version.startswith("16."):
        raise AssertionError(f"unexpected PostgreSQL server version: {health.server_version}")
    admin_health = check_postgres_health(_factory(), expected_migrations=migrations)
    if admin_health.status_code != "POSTGRES_SESSION_UNSAFE":
        raise AssertionError("migration administrator unexpectedly passed runtime health")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.market_data_source_bindings") != 2:
        raise AssertionError("expected exactly two Plus500 T4 instrument bindings")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_runtime_config") != 1:
        raise AssertionError("expected exactly one read-only T4 runtime config")


def _assert_health_mutation_detection() -> None:
    migrations = discover_migrations(PROJECT_ROOT / "db" / "migrations")

    def assert_unhealthy(label: str) -> None:
        health = check_postgres_health(
            _role_factory(_RUNTIME_ROLE), expected_migrations=migrations
        )
        if health.status_code != "MIGRATED_SCHEMA_MISSING":
            raise AssertionError(f"health accepted {label}: {health!r}")

    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                f"CREATE ROLE {_UNSAFE_EVIDENCE_ROLE} LOGIN NOSUPERUSER "
                "NOCREATEDB CREATEROLE INHERIT NOREPLICATION NOBYPASSRLS"
            )
            db_cursor.execute(
                f"GRANT crypto_agent_evidence_verifier TO {_UNSAFE_EVIDENCE_ROLE}"
            )
        connection.commit()
        assert_unhealthy("unsafe CREATEROLE verifier member")
        _expect_sqlstate_after(
            (),
            "SELECT crypto_agent.register_t4_bridge_evidence_key('')",
            "42501",
            _role_factory(_UNSAFE_EVIDENCE_ROLE),
        )
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                f"REVOKE crypto_agent_evidence_verifier "
                f"FROM {_UNSAFE_EVIDENCE_ROLE}"
            )
            db_cursor.execute(f"DROP ROLE {_UNSAFE_EVIDENCE_ROLE}")
        connection.commit()

        with connection.cursor() as db_cursor:
            db_cursor.execute(
                f"CREATE ROLE {_INDIRECT_EVIDENCE_ROLE} NOLOGIN NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
            db_cursor.execute(
                f"CREATE ROLE {_INDIRECT_EVIDENCE_LOGIN} LOGIN NOSUPERUSER "
                "NOCREATEDB NOCREATEROLE INHERIT NOREPLICATION NOBYPASSRLS"
            )
            db_cursor.execute(
                f"GRANT crypto_agent_evidence_verifier "
                f"TO {_INDIRECT_EVIDENCE_ROLE}"
            )
            db_cursor.execute(
                f"GRANT {_INDIRECT_EVIDENCE_ROLE} "
                f"TO {_INDIRECT_EVIDENCE_LOGIN}"
            )
        connection.commit()
        assert_unhealthy("indirect verifier SET ROLE path")
        _expect_sqlstate_after(
            (),
            "SELECT crypto_agent.register_t4_bridge_evidence_key('')",
            "42501",
            _role_factory(_INDIRECT_EVIDENCE_LOGIN),
        )
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                f"REVOKE {_INDIRECT_EVIDENCE_ROLE} "
                f"FROM {_INDIRECT_EVIDENCE_LOGIN}"
            )
            db_cursor.execute(
                f"REVOKE crypto_agent_evidence_verifier "
                f"FROM {_INDIRECT_EVIDENCE_ROLE}"
            )
            db_cursor.execute(f"DROP ROLE {_INDIRECT_EVIDENCE_LOGIN}")
            db_cursor.execute(f"DROP ROLE {_INDIRECT_EVIDENCE_ROLE}")
        connection.commit()

        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "CREATE TABLE crypto_agent.evidence_owner_probe(id integer)"
            )
            db_cursor.execute(
                f"ALTER TABLE crypto_agent.evidence_owner_probe "
                f"OWNER TO {_EVIDENCE_ROLE}"
            )
        connection.commit()
        assert_unhealthy("verifier member owns a crypto_agent relation")
        _expect_sqlstate_after(
            (),
            "SELECT crypto_agent.register_t4_bridge_evidence_key('')",
            "42501",
            _role_factory(_EVIDENCE_ROLE),
        )
        with connection.cursor() as db_cursor:
            db_cursor.execute("DROP TABLE crypto_agent.evidence_owner_probe")
        connection.commit()

        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "CREATE FUNCTION crypto_agent.reader_owner_probe() "
                "RETURNS integer LANGUAGE sql AS 'SELECT 1'"
            )
            db_cursor.execute(
                "ALTER FUNCTION crypto_agent.reader_owner_probe() OWNER TO "
                f"{_REPLAY_READER_ROLE}"
            )
        connection.commit()
        assert_unhealthy("reader member owns a crypto_agent routine")
        with connection.cursor() as db_cursor:
            db_cursor.execute("DROP FUNCTION crypto_agent.reader_owner_probe()")
        connection.commit()

        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "GRANT INSERT ON crypto_agent.t4_bridge_observation_events "
                "TO PUBLIC"
            )
        connection.commit()
        assert_unhealthy("PUBLIC protected-table INSERT")
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "REVOKE INSERT ON crypto_agent.t4_bridge_observation_events "
                "FROM PUBLIC"
            )
        connection.commit()

        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "GRANT EXECUTE ON FUNCTION "
                "crypto_agent.t4_observation_gate_is_verified(uuid, uuid) "
                "TO PUBLIC"
            )
        connection.commit()
        assert_unhealthy("PUBLIC internal-gate EXECUTE")
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "REVOKE EXECUTE ON FUNCTION "
                "crypto_agent.t4_observation_gate_is_verified(uuid, uuid) "
                "FROM PUBLIC"
            )
        connection.commit()

        with connection.cursor() as db_cursor:
            db_cursor.execute(
                """
                CREATE OR REPLACE FUNCTION
                    crypto_agent.t4_observation_gate_is_verified(
                        p_campaign_id uuid, p_checkpoint_event_id uuid
                    ) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER
                    SET search_path = pg_catalog, crypto_agent
                    AS $$ BEGIN RETURN TRUE; END; $$
                """
            )
        connection.commit()
        assert_unhealthy("forged internal gate body")
        migration_sql = (
            PROJECT_ROOT / "db/migrations/0018_t4_scenario_evidence.sql"
        ).read_text(encoding="utf-8")
        matched = re.search(
            r"CREATE OR REPLACE FUNCTION "
            r"crypto_agent\.t4_observation_gate_is_verified\(\s*"
            r"p_campaign_id uuid,\s*p_checkpoint_event_id uuid\s*\).*?\$\$;",
            migration_sql,
            re.DOTALL,
        )
        if matched is None:
            raise AssertionError("could not restore internal gate definition")
        with connection.cursor() as db_cursor:
            db_cursor.execute(matched.group(0))
        connection.commit()
    finally:
        connection.close()
    _assert_ready()


def _operational_batch() -> tuple[ProviderBatch, datetime]:
    as_of = datetime(2026, 8, 11, tzinfo=UTC)
    candles: list[Candle] = []
    raw_candles: list[dict[str, object]] = []
    for index in range(120):
        close_time = as_of - timedelta(hours=4 * (119 - index))
        raw = {
            "symbol": "BTC/USD",
            "interval_minutes": 240,
            "open_time": (close_time - timedelta(hours=4)).isoformat(),
            "close_time": close_time.isoformat(),
            "open": 100.0 + index,
            "high": 102.0 + index,
            "low": 99.0 + index,
            "close": 101.0 + index,
            "volume": 1000.0 + index,
            "source": "plus500_t4_futures_v1",
            "market_id": "MBT Sep26 (XCME)",
            "available_at": close_time.isoformat(),
            "ingested_at": as_of.isoformat(),
        }
        raw_candles.append(raw)
        candles.append(
            Candle(
                symbol="BTC/USD",
                interval_minutes=240,
                open_time=close_time - timedelta(hours=4),
                close_time=close_time,
                open=float(raw["open"]),
                high=float(raw["high"]),
                low=float(raw["low"]),
                close=float(raw["close"]),
                volume=float(raw["volume"]),
                source="plus500_t4_futures_v1",
                available_at=close_time,
                ingested_at=as_of,
            )
        )
    evidence = FuturesEvidence(
        contract_id="MBT Sep26 (XCME)",
        source="plus500_t4_futures_v1",
        session_status=SessionStatus.OPEN,
        is_full_snapshot=True,
        observed_at=as_of - timedelta(seconds=2),
        available_at=as_of - timedelta(seconds=1),
        ingested_at=as_of,
        bids=tuple(
            OrderBookLevel(level, 220.0 - level / 10, float(level))
            for level in range(1, 6)
        ),
        asks=tuple(
            OrderBookLevel(level, 220.0 + level / 10, float(level + 1))
            for level in range(1, 6)
        ),
        basis_reference=BasisReference(
            symbol="BTC/USD",
            reference_type="index",
            source="plus500_t4_index_v1",
            price=219.5,
            observed_at=as_of - timedelta(seconds=2),
            available_at=as_of - timedelta(seconds=1),
            ingested_at=as_of,
        ),
    )
    futures_evidence = {
        "exchange_id": "CME",
        "contract_id": "MBT",
        "market_id": evidence.contract_id,
        "source_id": evidence.source,
        "session_status": evidence.session_status.value,
        "is_full_snapshot": evidence.is_full_snapshot,
        "observed_at": evidence.observed_at.isoformat(),
        "available_at": evidence.available_at.isoformat(),
        "ingested_at": evidence.ingested_at.isoformat(),
        "bids": [
            {"level": item.level, "price": item.price, "quantity": item.quantity}
            for item in evidence.bids
        ],
        "asks": [
            {"level": item.level, "price": item.price, "quantity": item.quantity}
            for item in evidence.asks
        ],
        "basis_reference": {
            "symbol": evidence.basis_reference.symbol,
            "exchange_id": "CME",
            "contract_id": "BTC-INDEX",
            "market_id": "BTC Index (CME)",
            "reference_type": evidence.basis_reference.reference_type,
            "source": evidence.basis_reference.source,
            "price": evidence.basis_reference.price,
            "observed_at": evidence.basis_reference.observed_at.isoformat(),
            "available_at": evidence.basis_reference.available_at.isoformat(),
            "ingested_at": evidence.basis_reference.ingested_at.isoformat(),
        },
        "contract_transition": None,
    }
    envelope = {
        "schema_version": 5,
        "source_id": "plus500_t4_futures_v1",
        "venue_id": "plus500_t4",
        "read_only": True,
        "order_routes_exposed": False,
        "environment": "live_t4",
        "logical_symbol": "BTC/USD",
        "interval_minutes": 240,
        "exchange_id": "CME",
        "contract_id": "MBT",
        "market_id": "MBT Sep26 (XCME)",
        "contract_expires_at": (as_of + timedelta(days=30)).isoformat(),
        "contract_roll_at": (as_of + timedelta(days=25)).isoformat(),
        "contract_selection": "front_month",
        "rolled_from_market_id": None,
        "candles": raw_candles,
        "reference_price": {
            "symbol": "BTC/USD",
            "source": "plus500_t4_futures_v1",
            "price": 220.0,
            "event_time": (as_of - timedelta(minutes=1)).isoformat(),
            "available_at": as_of.isoformat(),
            "ingested_at": as_of.isoformat(),
        },
        "futures_evidence": futures_evidence,
    }
    raw_payload = json.dumps(envelope, separators=(",", ":")).encode()
    batch = ProviderBatch(
        candles=tuple(candles),
        input_candles=tuple(candles),
        sources=({"id": "plus500_t4_futures_v1"},),
        metadata={
            "t4_read_only_attested": True,
            "t4_source_id": "plus500_t4_futures_v1",
            "t4_venue_id": "plus500_t4",
            "t4_order_routes_exposed": False,
            "t4_bridge_schema_version": 5,
            "t4_environment": "live_t4",
            "t4_exchange_id": "CME",
            "t4_contract_id": "MBT",
            "t4_market_id": "MBT Sep26 (XCME)",
            "t4_basis_exchange_id": "CME",
            "t4_basis_contract_id": "BTC-INDEX",
            "t4_basis_market_id": "BTC Index (CME)",
            "t4_candle_market_ids": ["MBT Sep26 (XCME)"] * 120,
            "t4_contract_expires_at": (as_of + timedelta(days=30)).isoformat(),
            "t4_contract_roll_at": (as_of + timedelta(days=25)).isoformat(),
            "t4_contract_selection": "front_month",
            "t4_rolled_from_market_id": None,
            "t4_futures_evidence_attested": True,
        },
        reference_price=ReferencePriceSnapshot(
            symbol="BTC/USD",
            observations=(
                ReferencePriceObservation(
                    symbol="BTC/USD",
                    price=220.0,
                    event_time=as_of - timedelta(minutes=1),
                    available_at=as_of,
                    ingested_at=as_of,
                    source="plus500_t4_futures_v1",
                ),
            ),
        ),
        raw_payload=raw_payload,
        raw_payload_sha256=hashlib.sha256(raw_payload).hexdigest(),
        futures_evidence=evidence,
        external_delivery_eligible=True,
    )
    return batch, as_of


def _assert_operational_ingest_and_replay() -> None:
    batch, as_of = _operational_batch()
    repository = T4IngestRepository(_role_factory(_RUNTIME_ROLE))
    first = repository.ingest(batch, requested_as_of=as_of)
    duplicate = repository.ingest(batch, requested_as_of=as_of)
    if not first.inserted or duplicate.inserted or first.batch_id != duplicate.batch_id:
        raise AssertionError("T4 exact-payload ingest is not idempotent")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_ingestion_batches") != 1:
        raise AssertionError("expected one immutable T4 ingest batch")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_canonical_candles") != 120:
        raise AssertionError("expected 120 normalized T4 candles")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_futures_snapshots") != 1:
        raise AssertionError("expected one immutable T4 futures snapshot")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_orderbook_levels") != 10:
        raise AssertionError("expected ten immutable T4 order-book levels")
    _expect_sqlstate_after(
        (),
        "UPDATE crypto_agent.data_sources SET source_id = source_id "
        "WHERE source_key = 'plus500_t4_futures_v1'",
        "55000",
        _role_factory(_RUNTIME_ROLE),
    )
    replay_as_of = datetime.now(UTC)
    replay_a = repository.replay(
        symbol="BTC/USD", interval_minutes=240, as_of=replay_as_of, limit=120
    )
    isolated_reader_repository = T4IngestRepository(
        _role_factory(_REPLAY_READER_ROLE)
    )
    replay_b = isolated_reader_repository.replay(
        symbol="BTC/USD", interval_minutes=240, as_of=replay_as_of, limit=120
    )
    if replay_a.replay_fingerprint_sha256 != replay_b.replay_fingerprint_sha256:
        raise AssertionError("T4 point-in-time replay is not deterministic")
    if replay_a.futures_evidence != replay_b.futures_evidence:
        raise AssertionError("T4 futures evidence replay is not deterministic")
    _expect_sqlstate_after(
        (),
        "INSERT INTO crypto_agent.t4_replay_verifier_attestations ("
        "attestation_id,campaign_id,scope_key,replay_as_of,replay_limit,"
        "replay_fingerprint_sha256,source_batch_ids,source_batch_hashes,"
        "provenance_sha256,attested_by,content_hash) VALUES ("
        "gen_random_uuid(),gen_random_uuid(),'BTC/USD:240m',clock_timestamp(),"
        "120,repeat('a',64),ARRAY[1]::bigint[],"
        "ARRAY[repeat('b',64)]::crypto_agent.sha256_hex[],repeat('c',64),"
        "current_user,repeat('d',64))",
        "42501",
        _role_factory(_REPLAY_READER_ROLE),
    )


def _assert_operational_alert_outbox() -> None:
    observed_at = datetime.now(UTC)
    expires_at = observed_at + timedelta(hours=1)
    report = _operational_alert_report(observed_at, expires_at)
    policy = MonitoringPolicy.load(default_monitoring_policy_path())

    def warsaw_factory():  # type: ignore[no-untyped-def]
        connection = _role_factory(_RUNTIME_ROLE)()
        with connection.cursor() as db_cursor:
            db_cursor.execute("SET TIME ZONE 'Europe/Warsaw'")
        return connection

    repository = MonitoringRepository(warsaw_factory, policy)
    receipt = repository.record_report(
        report,
        operation="live_t4_analysis",
        recorded_at=observed_at,
    )
    if not receipt.alert_enqueued or receipt.alert_key is None:
        raise AssertionError("eligible live T4 report did not create an alert outbox")

    output = io.StringIO()
    delivery = repository.deliver_one(output, now=datetime.now(UTC))
    if delivery.status != "delivered" or delivery.attempt_no != 1:
        raise AssertionError("operational stdout delivery did not reach delivered")
    _expect_sqlstate_after(
        (),
        "UPDATE crypto_agent.alert_delivery_outbox "
        "SET alert_delivery_outbox_id = alert_delivery_outbox_id",
        "55000",
        _role_factory(_RUNTIME_ROLE),
    )
    emitted = json.loads(output.getvalue())
    if (
        emitted.get("channel") != "stdout_json"
        or emitted.get("destination") != "process_stdout"
        or emitted.get("payload", {}).get("decision") != "ALERT"
        or emitted.get("payload", {}).get("read_only") is not True
    ):
        raise AssertionError("operational stdout envelope is not fail-closed")

    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.alerts "
        "WHERE alert_type = 'research_alert_v1'"
    ) != 1:
        raise AssertionError("expected one immutable operational research alert")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.alert_delivery_outbox") != 1:
        raise AssertionError("expected one immutable operational alert outbox row")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.alert_delivery_attempts") != 1:
        raise AssertionError("expected one delivered alert attempt")
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.alert_delivery_attempts "
        "WHERE attempt_no = 1 AND outcome = 'delivered' "
        "AND previous_attempt_hash IS NULL"
    ) != 1:
        raise AssertionError("delivered alert attempt did not preserve its hash chain")
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.alert_events "
        "WHERE event_type IN ('created', 'delivered')"
    ) != 2:
        raise AssertionError("expected created and delivered alert events")


def _operational_alert_report(
    observed_at: datetime,
    expires_at: datetime,
) -> ResearchReport:
    risk = RiskAssessment(
        assessment_id="postgres16-risk-assessment",
        policy_id="v1-read-only-plus500-t4-2026-08-11",
        policy_hash="a" * 64,
        as_of=observed_at - timedelta(seconds=1),
        expires_at=expires_at,
        input_fingerprint_sha256="b" * 64,
        decision=Decision.ALERT,
        vetoed=False,
        flags=(),
        reasons=(),
    )
    return ResearchReport(
        decision_id="7e901aa1-0c48-4f9d-aa41-08374de37e28",
        trace_id="87f798af-2678-4464-b1b5-5d50df5c38e6",
        as_of=observed_at - timedelta(seconds=1),
        expires_at=expires_at,
        asset_id="bip122:000000000019d6689c085ae165831e93:native",
        instrument_id="plus500_t4_futures_v1:BTC/USD:240m",
        horizon="4h",
        decision=Decision.ALERT,
        reason_codes=("VOLUME_ANOMALY",),
        regime_probabilities=(),
        model_version="deterministic-futures-research-v2",
        policy_version="v1-read-only-plus500-t4-2026-08-11",
        data_snapshot_id=f"sha256:{'b' * 64}",
        thesis="Read-only PostgreSQL 16 acceptance alert.",
        counter_evidence=(),
        scenarios=(),
        invalidation_conditions=(),
        data_quality=DataQualityReport(
            score=1.0,
            sample_count=120,
            flags=(),
            critical_flags=(),
            newest_observed_at=observed_at - timedelta(seconds=2),
            newest_available_at=observed_at - timedelta(seconds=1),
        ),
        risk=risk,
        sources=(),
        metrics=None,
        futures_metrics=None,
        metadata={
            "system_version": "0.9.0",
            "mode": "V1_READ_ONLY",
            "execution_enabled": False,
            "not_financial_advice": True,
            "v1_gate_passed": False,
            "plus500_t4_source_attested": True,
            "external_delivery_eligible": True,
            "t4_bridge_schema_version": 5,
            "t4_environment": "live_t4",
            "futures_gate_passed": True,
            "futures_policy_id": "futures-analysis-v1-2026-08-12",
            "futures_policy_hash_sha256": "c" * 64,
            "input_fingerprint_sha256": "b" * 64,
            "input_candle_count": 120,
            "analysis_candle_count": 120,
            "provider_error_code": None,
        },
    )


def _assert_duplicate_attempt_is_noop() -> None:
    before = _scalar("SELECT COUNT(*) FROM crypto_agent.alert_delivery_attempts")
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                """
                INSERT INTO crypto_agent.alert_delivery_attempts (
                    alert_delivery_attempt_id, alert_delivery_outbox_id,
                    attempt_no, outcome, started_at, finished_at,
                    next_attempt_at, error_code, request_payload_hash,
                    previous_attempt_hash, content_hash, created_at
                ) OVERRIDING SYSTEM VALUE
                SELECT alert_delivery_attempt_id, alert_delivery_outbox_id,
                       attempt_no, outcome, started_at, finished_at,
                       next_attempt_at, error_code, request_payload_hash,
                       previous_attempt_hash, content_hash, created_at
                FROM crypto_agent.alert_delivery_attempts
                WHERE attempt_no = 1 AND outcome = 'delivered'
                ORDER BY alert_delivery_attempt_id
                LIMIT 1
                RETURNING alert_delivery_attempt_id
                """
            )
            if db_cursor.fetchone() is not None:
                raise AssertionError("exact duplicate alert attempt inserted a new row")
    finally:
        connection.rollback()
        connection.close()
    after = _scalar("SELECT COUNT(*) FROM crypto_agent.alert_delivery_attempts")
    if before != 1 or after != before:
        raise AssertionError("exact duplicate alert attempt was not a storage no-op")


def _assert_operational_alert_guards() -> None:
    _assert_duplicate_attempt_is_noop()

    _expect_sqlstate(
        """
        INSERT INTO crypto_agent.alert_delivery_attempts (
            alert_delivery_attempt_id, alert_delivery_outbox_id, attempt_no,
            outcome, started_at, finished_at, next_attempt_at, error_code,
            request_payload_hash, previous_attempt_hash, content_hash
        ) OVERRIDING SYSTEM VALUE
        SELECT alert_delivery_attempt_id, alert_delivery_outbox_id, attempt_no,
               outcome, started_at, finished_at, next_attempt_at, error_code,
               request_payload_hash, previous_attempt_hash, repeat('6', 64)
        FROM crypto_agent.alert_delivery_attempts
        WHERE attempt_no = 1 AND outcome = 'delivered'
        ORDER BY alert_delivery_attempt_id
        LIMIT 1
        """,
        "22023",
    )
    _expect_sqlstate(
        """
        INSERT INTO crypto_agent.alert_delivery_attempts (
            alert_delivery_outbox_id, attempt_no, outcome, started_at,
            finished_at, next_attempt_at, error_code, request_payload_hash,
            previous_attempt_hash, content_hash
        )
        SELECT attempt.alert_delivery_outbox_id, attempt.attempt_no + 1,
               'delivered', attempt.finished_at, attempt.finished_at,
               NULL, NULL, repeat('7', 64), attempt.content_hash, repeat('8', 64)
        FROM crypto_agent.alert_delivery_attempts AS attempt
        WHERE attempt.attempt_no = 1 AND attempt.outcome = 'delivered'
        ORDER BY attempt.alert_delivery_attempt_id
        LIMIT 1
        """,
        "22023",
    )
    _expect_sqlstate(
        """
        INSERT INTO crypto_agent.alert_delivery_attempts (
            alert_delivery_outbox_id, attempt_no, outcome, started_at,
            finished_at, next_attempt_at, error_code, request_payload_hash,
            previous_attempt_hash, content_hash
        )
        SELECT attempt.alert_delivery_outbox_id, attempt.attempt_no + 1,
               'delivered', attempt.finished_at, attempt.finished_at,
               NULL, NULL, outbox.payload_hash, attempt.content_hash, repeat('9', 64)
        FROM crypto_agent.alert_delivery_attempts AS attempt
        JOIN crypto_agent.alert_delivery_outbox AS outbox
          USING (alert_delivery_outbox_id)
        WHERE attempt.attempt_no = 1 AND attempt.outcome = 'delivered'
        ORDER BY attempt.alert_delivery_attempt_id
        LIMIT 1
        """,
        "22023",
    )
    _expect_sqlstate(
        """
        INSERT INTO crypto_agent.alert_delivery_outbox (
            alert_id, channel, destination, idempotency_key,
            monitoring_policy_id, monitoring_policy_hash, retention_days,
            payload, payload_hash, available_at, expires_at, max_attempts,
            content_hash
        )
        SELECT alert_id, 'invalid_route', destination, repeat('a', 64),
               monitoring_policy_id, monitoring_policy_hash, retention_days,
               payload, payload_hash, available_at, expires_at, max_attempts,
               repeat('b', 64)
        FROM crypto_agent.alert_delivery_outbox
        ORDER BY alert_delivery_outbox_id
        LIMIT 1
        """,
        "23514",
    )

    probe_alert = """
        INSERT INTO crypto_agent.alerts (
            alert_key, research_run_id, risk_assessment_id, asset_id, market_id,
            alert_type, severity, title, message, dedupe_key, expires_at,
            payload, source_id, source_record_key, source_version, revision_no,
            observed_at, available_at, ingested_at, content_hash
        )
        SELECT 'research-alert:' || repeat('1', 64), research_run_id,
               risk_assessment_id, asset_id, market_id, alert_type, severity,
               title, message, 'postgres16-trigger-probe', expires_at, payload,
               source_id, 'postgres16-trigger-probe-alert', source_version,
               revision_no, observed_at, available_at, ingested_at, repeat('1', 64)
        FROM crypto_agent.alerts
        WHERE alert_type = 'research_alert_v1'
        ORDER BY alert_id
        LIMIT 1
    """
    probe_outbox = """
        INSERT INTO crypto_agent.alert_delivery_outbox (
            alert_id, channel, destination, idempotency_key,
            monitoring_policy_id, monitoring_policy_hash, retention_days,
            payload, payload_hash, available_at, expires_at, max_attempts,
            content_hash
        )
        SELECT probe.alert_id, source.channel, source.destination, repeat('2', 64),
               source.monitoring_policy_id, source.monitoring_policy_hash,
               source.retention_days, source.payload, source.payload_hash,
               source.available_at, source.expires_at, source.max_attempts,
               repeat('3', 64)
        FROM crypto_agent.alerts AS probe
        CROSS JOIN LATERAL (
            SELECT * FROM crypto_agent.alert_delivery_outbox
            ORDER BY alert_delivery_outbox_id
            LIMIT 1
        ) AS source
        WHERE probe.alert_key = 'research-alert:' || repeat('1', 64)
    """
    _expect_sqlstate_after(
        (probe_alert, probe_outbox),
        """
        INSERT INTO crypto_agent.alert_delivery_attempts (
            alert_delivery_outbox_id, attempt_no, outcome, started_at,
            finished_at, next_attempt_at, error_code, request_payload_hash,
            previous_attempt_hash, content_hash
        )
        SELECT outbox.alert_delivery_outbox_id, 2, 'delivered',
               outbox.available_at, outbox.available_at, NULL, NULL,
               outbox.payload_hash, repeat('4', 64), repeat('5', 64)
        FROM crypto_agent.alert_delivery_outbox AS outbox
        JOIN crypto_agent.alerts AS alert USING (alert_id)
        WHERE alert.alert_key = 'research-alert:' || repeat('1', 64)
        """,
        "22023",
    )


def _assert_observation_ledger() -> tuple[bytes, ec.EllipticCurvePrivateKey, datetime]:
    private_key = ec.generate_private_key(ec.SECP256R1())
    public_key_der = private_key.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    evidence_repository = PostgresScenarioEvidenceRepository(
        _role_factory(_EVIDENCE_ROLE)
    )
    signer_fingerprint = evidence_repository.register_bridge_evidence_key(
        public_key_der
    )
    policy = ObservationPolicy.load(default_observation_policy_path())
    started_at = datetime.now(UTC).replace(microsecond=0)
    campaign = ObservationCampaign(
        campaign_id=_CAMPAIGN_ID,
        started_at=started_at,
        planned_ends_at=started_at + timedelta(hours=672),
        cycle_interval_seconds=3600,
        code_commit_hash="2" * 64,
        t4_protocol_commit_hash="3" * 64,
        runtime_config_hash="4" * 64,
        bridge_evidence_key_fingerprint=signer_fingerprint,
        scope_manifest=("BTC/USD:240m", "ETH/USD:240m"),
    )
    observation_repository = ObservationRepository(
        _role_factory(_RUNTIME_ROLE), policy
    )
    observation_repository.create_campaign(campaign)
    fractional_started_at = (datetime.now(UTC) - timedelta(seconds=1)).replace(
        microsecond=123456
    )
    fractional_campaign = ObservationCampaign(
        campaign_id="b4a69cf8-2112-4a98-a68a-92bbcfb56e8b",
        started_at=fractional_started_at,
        planned_ends_at=fractional_started_at + timedelta(hours=672),
        cycle_interval_seconds=3600,
        code_commit_hash="2" * 64,
        t4_protocol_commit_hash="3" * 64,
        runtime_config_hash="4" * 64,
        bridge_evidence_key_fingerprint=signer_fingerprint,
        scope_manifest=("BTC/USD:240m", "ETH/USD:240m"),
    )
    observation_repository.create_campaign(fractional_campaign)

    connection = _role_factory(_RUNTIME_ROLE)()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                """
                INSERT INTO crypto_agent.t4_observation_session_events (
                    campaign_id, sequence_no, event_at, event_type, outcome,
                    scenario_code, detail_code, read_only, execution_enabled,
                    previous_event_hash, content_hash
                ) VALUES (
                    %s, 1,
                    CURRENT_TIMESTAMP, 'campaign_started', 'pass', NULL, NULL,
                    TRUE, FALSE, NULL, repeat('8', 64)
                )
                """,
                (_CAMPAIGN_ID,),
            )
        connection.commit()
    finally:
        connection.close()

    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_observation_campaigns") != 2:
        raise AssertionError("expected zero- and fractional-microsecond campaigns")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_observation_session_events") != 1:
        raise AssertionError("expected one hash-chained T4 observation session event")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_observation_research_inputs") != 0:
        raise AssertionError("unexpected unverified T4 observation research input")

    _expect_sqlstate(
        """
        INSERT INTO crypto_agent.t4_observation_research_inputs (
            campaign_id, scope_key, sequence_no, expected_at, t4_batch_id,
            research_run_id, raw_payload_hash, analysis_input_hash, trace_id,
            content_hash
        )
        SELECT campaign.campaign_id, 'BTC/USD:240m', 1,
               campaign.started_at + interval '1 hour', batch.t4_batch_id,
               run.research_run_id, batch.raw_payload_hash, repeat('e', 64),
               'ba66e0e1-147e-4b99-885e-ddbb26ce05c6'::uuid, repeat('f', 64)
        FROM crypto_agent.t4_observation_campaigns AS campaign
        CROSS JOIN LATERAL (
            SELECT * FROM crypto_agent.t4_ingestion_batches
            ORDER BY t4_batch_id LIMIT 1
        ) AS batch
        CROSS JOIN LATERAL (
            SELECT * FROM crypto_agent.research_runs
            ORDER BY research_run_id LIMIT 1
        ) AS run
        WHERE campaign.campaign_id = '581ef534-1f26-4f06-95ce-5873911044d6'
        """,
        "22023",
    )

    _expect_sqlstate(
        "UPDATE crypto_agent.t4_observation_campaigns "
        "SET planned_ends_at = planned_ends_at + interval '1 hour'",
        "55000",
    )
    _expect_sqlstate(
        "UPDATE crypto_agent.t4_observation_session_events SET outcome = 'fail'",
        "55000",
    )
    _expect_sqlstate("TRUNCATE crypto_agent.t4_observation_cycles", "55000")
    _expect_sqlstate(
        "TRUNCATE crypto_agent.t4_observation_research_inputs, "
        "crypto_agent.t4_observation_cycles",
        "55000",
    )
    return public_key_der, private_key, started_at


def _assert_scenario_evidence_boundary(
    public_key_der: bytes,
    private_key: ec.EllipticCurvePrivateKey,
    event_at: datetime,
) -> None:
    fingerprint = hashlib.sha256(public_key_der).hexdigest()
    payload_bytes = b"{}"
    payload_hash = hashlib.sha256(payload_bytes).hexdigest()
    event_id = str(uuid.uuid4())
    boot_id = str(uuid.uuid4())
    claims = {
        "schema_version": 1,
        "event_id": event_id,
        "sequence_no": 1,
        "event_at": event_at.isoformat().replace("+00:00", "Z"),
        "event_type": "bridge_started",
        "reason_code": "BRIDGE_STARTED",
        "campaign_id": _CAMPAIGN_ID,
        "boot_id": boot_id,
        "session_generation": 0,
        "reconnect_count": 0,
        "environment": "live_t4",
        "bridge_schema_version": 5,
        "read_only": True,
        "order_routes_exposed": False,
        "scope_key": None,
        "scenario_code": None,
        "action_request_id": None,
        "previous_event_hash": None,
        "payload": {},
        "payload_hash_sha256": payload_hash,
    }
    canonical = json.dumps(claims, separators=(",", ":")).encode("utf-8")
    event_hash = hashlib.sha256(canonical).hexdigest()
    signature = private_key.sign(canonical, ec.ECDSA(hashes.SHA256()))
    event = SignedBridgeEvent(
        schema_version=1,
        event_id=event_id,
        sequence_no=1,
        event_at=event_at,
        event_type="bridge_started",
        reason_code="BRIDGE_STARTED",
        campaign_id=_CAMPAIGN_ID,
        boot_id=boot_id,
        session_generation=0,
        reconnect_count=0,
        environment="live_t4",
        bridge_schema_version=5,
        read_only=True,
        order_routes_exposed=False,
        scope_key=None,
        scenario_code=None,
        action_request_id=None,
        previous_event_hash=None,
        payload=BridgeEventPayload(),
        payload_hash_sha256=payload_hash,
        event_hash_sha256=event_hash,
        canonical_payload=canonical,
        signature_der=signature,
        evidence_key_fingerprint_sha256=fingerprint,
    )
    page = BridgeEventPage(
        boot_id=boot_id,
        environment="live_t4",
        evidence_key_fingerprint_sha256=fingerprint,
        public_key_spki_der=public_key_der,
        events=(event,),
    )
    evidence_repository = PostgresScenarioEvidenceRepository(
        _role_factory(_EVIDENCE_ROLE)
    )
    evidence_repository.store_bridge_event_page(campaign_id=_CAMPAIGN_ID, page=page)
    evidence_repository.store_bridge_event_page(campaign_id=_CAMPAIGN_ID, page=page)
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_bridge_observation_events") != 1:
        raise AssertionError("exact signed event replay was not idempotent")

    _expect_sqlstate_after(
        (),
        "SELECT crypto_agent.register_t4_bridge_evidence_key("
        f"'{base64.b64encode(public_key_der).decode('ascii')}')",
        "42501",
        _role_factory(_RUNTIME_ROLE),
    )
    replay_attestation_call = (
        "SELECT * FROM crypto_agent.record_verified_t4_replay_attestation("
        f"'{uuid.uuid4()}'::uuid, '{_CAMPAIGN_ID}'::uuid, 'BTC/USD:240m', "
        "(SELECT planned_ends_at FROM crypto_agent.t4_observation_campaigns "
        f"WHERE campaign_id = '{_CAMPAIGN_ID}'::uuid), repeat('a',64), "
        "ARRAY[1]::bigint[], "
        "ARRAY[repeat('b',64)]::crypto_agent.sha256_hex[], repeat('c',64))"
    )
    _expect_sqlstate_after(
        (), replay_attestation_call, "42501", _role_factory(_RUNTIME_ROLE)
    )
    _expect_sqlstate_after(
        (), replay_attestation_call, "22023", _role_factory(_EVIDENCE_ROLE)
    )
    _expect_sqlstate_after(
        (),
        "INSERT INTO crypto_agent.t4_observation_scenario_trials DEFAULT VALUES",
        "42501",
        _role_factory(_EVIDENCE_ROLE),
    )
    collision_sql = """
        SELECT crypto_agent.record_verified_t4_bridge_observation_event(
            %s::uuid, %s::uuid, 1, %s::timestamptz, 'bridge_started',
            'T4_SESSION_CONNECTING', %s::uuid, 0, 0, NULL, NULL, NULL,
            %s::crypto_agent.sha256_hex, %s::crypto_agent.sha256_hex,
            %s, %s::crypto_agent.sha256_hex, %s
        )
    """
    connection = _role_factory(_EVIDENCE_ROLE)()
    try:
        with connection.cursor() as db_cursor:
            try:
                db_cursor.execute(
                    collision_sql,
                    (
                        event_id,
                        _CAMPAIGN_ID,
                        event_at,
                        boot_id,
                        payload_hash,
                        event_hash,
                        base64.b64encode(canonical).decode("ascii"),
                        fingerprint,
                        base64.b64encode(signature).decode("ascii"),
                    ),
                )
            except Exception as exc:
                if getattr(exc, "sqlstate", None) != "23505":
                    raise AssertionError("signed event collision was not rejected") from exc
            else:
                raise AssertionError("signed event collision unexpectedly succeeded")
    finally:
        connection.rollback()
        connection.close()

    try:
        evidence_repository.verify_passive_trial(
            campaign_id=_CAMPAIGN_ID,
            scenario_code="replay_blocked",
        )
    except ScenarioEvidenceError:
        pass
    else:
        raise AssertionError("incomplete passive scenario unexpectedly passed")
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.t4_observation_scenario_trials "
        "WHERE scenario_code = 'replay_blocked'"
    ) != 0:
        raise AssertionError("incomplete passive verification poisoned the ledger")

    try:
        evidence_repository.begin_trial(
            campaign_id=_CAMPAIGN_ID,
            scenario_code="missing_data",
            action_request_id=str(uuid.uuid4()),
            scope_key="BTC/USD:240m",
        )
    except ScenarioEvidenceError:
        pass
    else:
        raise AssertionError("controlled fault armed without a fresh campaign anchor")
    if _scalar(
        "SELECT COUNT(*) FROM "
        "crypto_agent.t4_observation_scenario_trial_requests"
    ) != 0:
        raise AssertionError("rejected unanchored trial left a dangling request")

    incomplete_trial_id = str(uuid.uuid4())
    action_request_id = str(uuid.uuid4())
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                """
                INSERT INTO crypto_agent.t4_observation_scenario_trial_requests (
                    trial_id, campaign_id, scenario_code, scope_key,
                    action_request_id, started_at, content_hash
                ) VALUES (%s, %s, 'missing_data', 'BTC/USD:240m', %s,
                          %s, repeat('a', 64))
                """,
                (
                    incomplete_trial_id,
                    _CAMPAIGN_ID,
                    action_request_id,
                    event_at,
                ),
            )
        connection.commit()
    finally:
        connection.close()
    _expect_sqlstate_after(
        (),
        "SELECT * FROM crypto_agent.complete_t4_observation_scenario_trial("
        f"'{incomplete_trial_id}'::uuid, "
        "'{}'::crypto_agent.sha256_hex[], repeat('0', 64))",
        "55000",
        _role_factory(_EVIDENCE_ROLE),
    )
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.t4_observation_scenario_trials "
        "WHERE trial_id = %s",
        (incomplete_trial_id,),
    ) != 0:
        raise AssertionError("incomplete controlled evidence poisoned the trial ledger")
    connection = _role_factory(_EVIDENCE_ROLE)()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "SELECT trial_id, started_at FROM "
                "crypto_agent.begin_t4_observation_scenario_trial("
                "%s, 'missing_data', %s, 'BTC/USD:240m')",
                (_CAMPAIGN_ID, action_request_id),
            )
            resumed = db_cursor.fetchone()
        connection.commit()
    finally:
        connection.close()
    if resumed is None or str(resumed[0]) != incomplete_trial_id:
        raise AssertionError("controlled trial retry did not resume its exact request")
    _expect_sqlstate_after(
        (),
        "SELECT * FROM crypto_agent.begin_t4_observation_scenario_trial("
        f"'{_CAMPAIGN_ID}'::uuid, 'missing_data', "
        f"'{action_request_id}'::uuid, 'ETH/USD:240m')",
        "23505",
        _role_factory(_EVIDENCE_ROLE),
    )

    checkpoint_id = str(uuid.uuid4())
    checkpoint_action_id = str(uuid.uuid4())
    checkpoint_at_value = _scalar("SELECT clock_timestamp()")
    if not isinstance(checkpoint_at_value, datetime):
        raise AssertionError("checkpoint database clock is invalid")
    checkpoint_at = checkpoint_at_value
    checkpoint_payload = {
        "action": "checkpoint",
        "control_step": "consumed",
    }
    checkpoint_payload_bytes = json.dumps(
        checkpoint_payload, separators=(",", ":")
    ).encode("utf-8")
    checkpoint_payload_hash = hashlib.sha256(checkpoint_payload_bytes).hexdigest()
    checkpoint_claims = {
        "schema_version": 1,
        "event_id": checkpoint_id,
        "sequence_no": 2,
        "event_at": checkpoint_at.isoformat().replace("+00:00", "Z"),
        "event_type": "campaign_checkpoint",
        "reason_code": "OBSERVATION_CAMPAIGN_CHECKPOINT",
        "campaign_id": _CAMPAIGN_ID,
        "boot_id": boot_id,
        "session_generation": 0,
        "reconnect_count": 0,
        "environment": "live_t4",
        "bridge_schema_version": 5,
        "read_only": True,
        "order_routes_exposed": False,
        "scope_key": None,
        "scenario_code": None,
        "action_request_id": checkpoint_action_id,
        "previous_event_hash": event_hash,
        "payload": checkpoint_payload,
        "payload_hash_sha256": checkpoint_payload_hash,
    }
    checkpoint_canonical = json.dumps(
        checkpoint_claims, separators=(",", ":")
    ).encode("utf-8")
    checkpoint_hash = hashlib.sha256(checkpoint_canonical).hexdigest()
    checkpoint_signature = private_key.sign(
        checkpoint_canonical, ec.ECDSA(hashes.SHA256())
    )
    checkpoint_event = SignedBridgeEvent(
        schema_version=1,
        event_id=checkpoint_id,
        sequence_no=2,
        event_at=checkpoint_at,
        event_type="campaign_checkpoint",
        reason_code="OBSERVATION_CAMPAIGN_CHECKPOINT",
        campaign_id=_CAMPAIGN_ID,
        boot_id=boot_id,
        session_generation=0,
        reconnect_count=0,
        environment="live_t4",
        bridge_schema_version=5,
        read_only=True,
        order_routes_exposed=False,
        scope_key=None,
        scenario_code=None,
        action_request_id=checkpoint_action_id,
        previous_event_hash=event_hash,
        payload=BridgeEventPayload(action="checkpoint", control_step="consumed"),
        payload_hash_sha256=checkpoint_payload_hash,
        event_hash_sha256=checkpoint_hash,
        canonical_payload=checkpoint_canonical,
        signature_der=checkpoint_signature,
        evidence_key_fingerprint_sha256=fingerprint,
    )
    evidence_repository.store_bridge_event_page(
        campaign_id=_CAMPAIGN_ID,
        page=BridgeEventPage(
            boot_id=boot_id,
            environment="live_t4",
            evidence_key_fingerprint_sha256=fingerprint,
            public_key_spki_der=public_key_der,
            events=(checkpoint_event,),
        ),
    )
    if _scalar(
        "SELECT crypto_agent.t4_observation_gate_is_verified(%s, %s)",
        (_CAMPAIGN_ID, checkpoint_id),
    ) is not False:
        raise AssertionError("premature signed campaign checkpoint opened the gate")

    gate_connection = _role_factory(_RUNTIME_ROLE)()
    try:
        with gate_connection.cursor() as db_cursor:
            db_cursor.execute(
                "SELECT crypto_agent.t4_observation_gate_is_verified(%s)",
                (_CAMPAIGN_ID,),
            )
            gate_row = db_cursor.fetchone()
        gate_connection.commit()
    finally:
        gate_connection.close()
    if gate_row is None or gate_row[0] is not False:
        raise AssertionError("incomplete campaign gate did not fail closed for runtime")
    _expect_sqlstate_after(
        (),
        """
        INSERT INTO crypto_agent.t4_observation_quality_reports (
            campaign_id, generated_at, observed_until, overall_status,
            v1_gate_passed, observation_policy_id, observation_policy_hash,
            frozen_baseline_hash, report, report_hash, content_hash
        )
        SELECT campaign_id, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 'PASS', TRUE,
               observation_policy_id, observation_policy_hash,
               frozen_baseline_hash, '{}'::jsonb, repeat('b', 64), repeat('c', 64)
        FROM crypto_agent.t4_observation_campaigns
        WHERE campaign_id = '581ef534-1f26-4f06-95ce-5873911044d6'
        """,
        "22023",
        _role_factory(_RUNTIME_ROLE),
    )


def _assert_operational_persistence() -> None:
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_ingestion_batches") != 1:
        raise AssertionError("T4 ingest batch did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_canonical_candles") != 120:
        raise AssertionError("T4 canonical candles did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_futures_snapshots") != 1:
        raise AssertionError("T4 futures snapshot did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_orderbook_levels") != 10:
        raise AssertionError("T4 order book did not survive PostgreSQL restart")
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.alerts "
        "WHERE alert_type = 'research_alert_v1'"
    ) != 1:
        raise AssertionError("operational alert did not survive PostgreSQL restart")
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.alert_delivery_outbox AS outbox "
        "JOIN crypto_agent.alerts AS alert USING (alert_id) "
        "WHERE alert.alert_type = 'research_alert_v1'"
    ) != 1:
        raise AssertionError("alert outbox did not survive PostgreSQL restart")
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.alert_delivery_attempts AS attempt "
        "JOIN crypto_agent.alert_delivery_outbox AS outbox "
        "USING (alert_delivery_outbox_id) "
        "JOIN crypto_agent.alerts AS alert USING (alert_id) "
        "WHERE alert.alert_type = 'research_alert_v1' "
        "AND attempt.attempt_no = 1 AND attempt.outcome = 'delivered' "
        "AND attempt.previous_attempt_hash IS NULL"
    ) != 1:
        raise AssertionError("delivered alert attempt did not survive PostgreSQL restart")
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.alert_events AS event "
        "JOIN crypto_agent.alerts AS alert USING (alert_id) "
        "WHERE alert.alert_type = 'research_alert_v1' "
        "AND event.event_type IN ('created', 'delivered')"
    ) != 2:
        raise AssertionError("alert events did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_observation_campaigns") != 2:
        raise AssertionError("T4 observation campaigns did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_observation_session_events") != 1:
        raise AssertionError("T4 observation event did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_observation_research_inputs") != 0:
        raise AssertionError("unverified observation research input survived restart")


def bootstrap_and_test() -> None:
    _execute_file(PROJECT_ROOT / "db" / "schema.sql")
    migrations = discover_migrations(PROJECT_ROOT / "db" / "migrations")
    if apply_migrations(_factory(), migrations[:-1]) != (
        "0011",
        "0012",
        "0013",
        "0014",
        "0015",
        "0016",
        "0017",
    ):
        raise AssertionError("clean PostgreSQL 16 did not apply migrations 0011-0017")
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(
                "CREATE ROLE crypto_agent_evidence_owner LOGIN "
                "NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "
                "NOREPLICATION NOBYPASSRLS"
            )
        connection.commit()
    finally:
        connection.close()
    try:
        apply_migrations(_factory(), migrations)
    except PostgresOperationError:
        pass
    else:
        raise AssertionError("unsafe pre-existing evidence owner was accepted")
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute("DROP ROLE crypto_agent_evidence_owner")
        connection.commit()
    finally:
        connection.close()
    if apply_migrations(_factory(), migrations) != ("0018",):
        raise AssertionError("clean PostgreSQL 16 did not apply migration 0018")
    _provision_acceptance_roles()
    _assert_runtime_identity_sequence_grants()
    _assert_runtime_row_lock_grants()
    apply_v1_seeds(_factory(), default_v1_seed_path())
    apply_v1_seeds(_factory(), default_v1_seed_path())
    _assert_ready()
    _assert_health_mutation_detection()
    _assert_operational_ingest_and_replay()
    _assert_operational_alert_outbox()
    _assert_operational_alert_guards()
    public_key_der, private_key, campaign_started_at = _assert_observation_ledger()
    _assert_scenario_evidence_boundary(
        public_key_der,
        private_key,
        campaign_started_at,
    )

    _expect_sqlstate(
        "UPDATE crypto_agent.data_sources SET display_name = 'tampered' "
        "WHERE source_key = 'plus500_t4_futures_v1'",
        "55000",
    )
    _expect_sqlstate(
        "UPDATE crypto_agent.t4_ingestion_batches SET status = 'completed'",
        "55000",
    )
    _expect_sqlstate(
        "UPDATE crypto_agent.t4_futures_snapshots SET session_status = 'CLOSED'",
        "55000",
    )
    _expect_sqlstate("TRUNCATE crypto_agent.t4_orderbook_levels", "55000")
    _expect_sqlstate(
        "UPDATE crypto_agent.alert_delivery_outbox SET max_attempts = 2",
        "55000",
    )
    _expect_sqlstate(
        "UPDATE crypto_agent.alert_delivery_attempts SET outcome = 'expired'",
        "55000",
    )
    _expect_sqlstate("TRUNCATE crypto_agent.alert_delivery_attempts", "55000")
    _expect_sqlstate(
        "INSERT INTO crypto_agent.data_sources ("
        "source_key, display_name, source_kind, trust_tier, registry_version, "
        "config_hash, observed_at, available_at, ingested_at) VALUES ("
        "'invalid-trust-tier', 'Invalid', 'test', 6, 'v1', repeat('f', 64), "
        "CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
        "23514",
    )

    try:
        with transaction(_factory()) as connection:
            with connection.cursor() as db_cursor:
                db_cursor.execute(
                    "INSERT INTO crypto_agent.data_sources ("
                    "source_key, display_name, source_kind, trust_tier, registry_version, "
                    "config_hash, observed_at, available_at, ingested_at) VALUES ("
                    "'rollback-probe', 'Rollback probe', 'test', 3, 'v1', "
                    "repeat('e', 64), CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, "
                    "CURRENT_TIMESTAMP)"
                )
            raise RuntimeError("force rollback")
    except RuntimeError as exc:
        if str(exc) != "force rollback":
            raise
    if _scalar(
        "SELECT COUNT(*) FROM crypto_agent.data_sources "
        "WHERE source_key = 'rollback-probe'"
    ) != 0:
        raise AssertionError("transaction rollback did not remove the probe row")
    _assert_ready()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("bootstrap", "verify"))
    args = parser.parse_args()
    if not os.environ.get("CRYPTO_AGENT_POSTGRES_DSN"):
        raise RuntimeError("CRYPTO_AGENT_POSTGRES_DSN is required")
    if args.mode == "bootstrap":
        bootstrap_and_test()
    else:
        _assert_ready()
        _assert_operational_persistence()
    print(f"postgres16 acceptance {args.mode}: READY")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

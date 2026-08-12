from __future__ import annotations

import hashlib
import importlib
import os
import re
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast


SqlParams = Sequence[object] | Mapping[str, object]


class DBCursor(Protocol):
    def execute(self, query: str, params: SqlParams | None = None) -> object: ...

    def fetchone(self) -> object | None: ...

    def fetchall(self) -> Sequence[object]: ...

    def close(self) -> None: ...


class DBConnection(Protocol):
    def cursor(self) -> DBCursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


ConnectionFactory = Callable[[], DBConnection]


class PostgresError(RuntimeError):
    """Base error whose message is safe to expose in logs or an API response."""


class PostgresUnavailableError(PostgresError):
    """The driver or database cannot currently be reached."""


class PostgresOperationError(PostgresError):
    """A database operation failed without exposing driver or DSN details."""


class MigrationError(PostgresError):
    pass


class MigrationDriftError(MigrationError):
    pass


@dataclass(frozen=True, slots=True)
class PostgresSettings:
    """Connection settings with a secret-safe representation.

    The DSN is deliberately excluded from ``repr``. Callers must also avoid logging
    environment dictionaries or the return value of ``asdict`` on this object.
    """

    dsn: str = field(repr=False)
    connect_timeout_seconds: int = 5

    def __post_init__(self) -> None:
        if not self.dsn.strip():
            raise ValueError("PostgreSQL DSN must not be empty")
        if self.connect_timeout_seconds <= 0:
            raise ValueError("PostgreSQL connect timeout must be positive")

    @classmethod
    def from_env(cls) -> PostgresSettings:
        dsn = os.environ.get("CRYPTO_AGENT_POSTGRES_DSN", "")
        if not dsn:
            raise PostgresUnavailableError(
                "PostgreSQL is not configured; set CRYPTO_AGENT_POSTGRES_DSN"
            )
        timeout_raw = os.environ.get("CRYPTO_AGENT_POSTGRES_CONNECT_TIMEOUT", "5")
        try:
            timeout = int(timeout_raw)
        except ValueError:
            raise PostgresUnavailableError(
                "PostgreSQL connect timeout configuration is invalid"
            ) from None
        return cls(dsn=dsn, connect_timeout_seconds=timeout)


@dataclass(frozen=True, slots=True)
class PsycopgConnectionFactory:
    """Lazy psycopg 3 connector.

    Importing the project remains possible without the optional database driver.
    Connection failures intentionally discard the original exception because it may
    contain a DSN, user name, host details, or provider-specific diagnostics.
    """

    settings: PostgresSettings

    def __call__(self) -> DBConnection:
        try:
            driver = importlib.import_module("psycopg")
            connection = driver.connect(
                self.settings.dsn,
                connect_timeout=self.settings.connect_timeout_seconds,
                autocommit=False,
            )
        except Exception:
            raise PostgresUnavailableError("PostgreSQL is unavailable") from None
        return cast(DBConnection, connection)


@contextmanager
def transaction(connection_factory: ConnectionFactory) -> Iterator[DBConnection]:
    """Open one transaction and always close its connection.

    This helper guarantees rollback on errors, but deliberately does not translate
    errors raised inside the block. Repository boundaries translate unexpected driver
    errors after preserving domain validation failures.
    """

    try:
        connection = connection_factory()
    except PostgresError:
        raise
    except Exception:
        raise PostgresUnavailableError("PostgreSQL is unavailable") from None

    try:
        yield connection
        connection.commit()
    except Exception:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            connection.close()
        except Exception:
            pass


@contextmanager
def cursor(connection: DBConnection) -> Iterator[DBCursor]:
    database_cursor = connection.cursor()
    try:
        yield database_cursor
    finally:
        try:
            database_cursor.close()
        except Exception:
            pass


@dataclass(frozen=True, slots=True)
class Migration:
    version: str
    name: str
    path: Path
    checksum_sha256: str


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    applied: tuple[str, ...]
    pending: tuple[Migration, ...]


@dataclass(frozen=True, slots=True)
class PostgresHealth:
    healthy: bool
    database_reachable: bool
    base_schema_ready: bool
    migrations_current: bool
    server_version: str | None
    missing_migrations: tuple[str, ...]
    status_code: str
    schema_ready: bool = False
    triggers_ready: bool = False
    seeds_ready: bool = False
    missing_schema_objects: tuple[str, ...] = ()
    missing_triggers: tuple[str, ...] = ()
    missing_seeds: tuple[str, ...] = ()
    unexpected_migrations: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class _TriggerRequirement:
    name: str
    table: str
    function: str
    type_mask: int
    deferrable: bool = False
    initially_deferred: bool = False


@dataclass(frozen=True, slots=True)
class _ColumnRequirement:
    table: str
    name: str
    type_name: str
    not_null: bool = True


_MIGRATION_FILE = re.compile(r"^(?P<version>[0-9]{4})_(?P<name>[a-z0-9_]+)\.sql$")

_REQUIRED_POSTGRES_MAJOR = 16
_BASE_TABLES = (
    "data_sources",
    "data_source_versions",
    "ingestion_batches",
    "exchanges",
    "exchange_versions",
    "assets",
    "asset_versions",
    "asset_symbols",
    "markets",
    "market_versions",
    "market_symbols",
    "candles",
    "trades",
    "orderbook_snapshots",
    "orderbook_levels",
    "derivatives_metrics",
    "onchain_metrics",
    "macro_series",
    "macro_observations",
    "documents",
    "document_entities",
    "token_unlocks",
    "data_quality_incidents",
    "data_quality_incident_events",
    "research_runs",
    "research_run_events",
    "research_run_universe",
    "research_run_inputs",
    "research_artifacts",
    "risk_policies",
    "risk_assessments",
    "risk_assessment_flags",
    "alerts",
    "alert_events",
    "approval_requests",
    "approval_decisions",
    "paper_accounts",
    "paper_orders",
    "paper_order_events",
    "paper_fills",
    "paper_position_snapshots",
    "audit_log",
)
_MIGRATED_TABLES = (
    "schema_migrations",
    "market_data_source_bindings",
    "canonical_candle_series",
    "source_candle_receipts",
    "canonical_candle_manifests",
    "canonical_candle_provenance",
    "reference_price_manifests",
    "reference_price_provenance",
    "t4_runtime_config",
    "t4_ingestion_batches",
    "t4_canonical_candles",
)
_BASE_TRIGGER_FUNCTIONS = (
    "forbid_append_only_change",
    "enforce_research_input_cutoff",
    "enforce_paper_order_approval",
    "enforce_audit_hash_chain",
)
_MIGRATED_TRIGGER_FUNCTIONS = (
    "enforce_market_data_source_binding",
    "enforce_canonical_candle_series",
    "enforce_source_candle_receipt",
    "enforce_canonical_candle_manifest",
    "enforce_canonical_candle_provenance",
    "enforce_canonical_exact_provenance",
    "enforce_v1_canonical_manifest_policy",
    "enforce_v1_reference_price_manifest",
    "enforce_v1_reference_price_provenance",
    "enforce_v1_reference_price_exact_provenance",
)


def _column_requirements(
    table: str,
    definitions: Sequence[tuple[str, str]],
) -> tuple[_ColumnRequirement, ...]:
    return tuple(
        _ColumnRequirement(table=table, name=name, type_name=type_name)
        for name, type_name in definitions
    )


_MIGRATED_COLUMN_REQUIREMENTS = (
    _column_requirements(
        "schema_migrations",
        (
            ("version", "text"),
            ("name", "text"),
            ("checksum_sha256", "sha256_hex"),
            ("applied_at", "timestamptz"),
        ),
    )
    + _column_requirements(
        "market_data_source_bindings",
        (
            ("binding_id", "int8"),
            ("source_id", "int8"),
            ("market_id", "int8"),
            ("canonical_symbol", "text"),
            ("venue_symbol", "text"),
            ("observed_at", "timestamptz"),
            ("available_at", "timestamptz"),
            ("ingested_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "canonical_candle_series",
        (
            ("canonical_series_id", "int8"),
            ("series_key", "text"),
            ("canonical_market_id", "int8"),
            ("canonical_source_id", "int8"),
            ("risk_policy_id", "int8"),
            ("canonical_symbol", "text"),
            ("interval_seconds", "int4"),
            ("algorithm_version", "text"),
            ("policy_hash", "sha256_hex"),
            ("observed_at", "timestamptz"),
            ("available_at", "timestamptz"),
            ("ingested_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "source_candle_receipts",
        (
            ("source_candle_receipt_id", "int8"),
            ("candle_id", "int8"),
            ("provider_ingested_at", "timestamptz"),
            ("received_at", "timestamptz"),
            ("cutoff_as_of", "timestamptz"),
            ("receipt_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "canonical_candle_manifests",
        (
            ("canonical_candle_id", "int8"),
            ("canonical_series_id", "int8"),
            ("risk_policy_id", "int8"),
            ("algorithm_version", "text"),
            ("policy_hash", "sha256_hex"),
            ("consensus_window_size", "int4"),
            ("cutoff_as_of", "timestamptz"),
            ("inputs_hash", "sha256_hex"),
            ("computed_payload_hash", "sha256_hex"),
            ("normalized_volume", "numeric"),
            ("normalized_volume_unit", "text"),
            ("diagnostics_document", "jsonb"),
            ("evidence_hash", "sha256_hex"),
            ("created_at", "timestamptz"),
        ),
    )
    + _column_requirements(
        "canonical_candle_provenance",
        (
            ("canonical_candle_id", "int8"),
            ("source_candle_id", "int8"),
            ("input_role", "text"),
            ("linked_at", "timestamptz"),
        ),
    )
    + _column_requirements(
        "reference_price_manifests",
        (
            ("reference_price_id", "int8"),
            ("reference_key", "text"),
            ("canonical_market_id", "int8"),
            ("risk_policy_id", "int8"),
            ("symbol", "text"),
            ("algorithm_version", "text"),
            ("policy_hash", "sha256_hex"),
            ("event_time", "timestamptz"),
            ("cutoff_as_of", "timestamptz"),
            ("evaluated_at", "timestamptz"),
            ("median_price", "numeric"),
            ("pairwise_divergence_bps", "numeric"),
            ("max_deviation_from_median_bps", "numeric"),
            ("inputs_hash", "sha256_hex"),
            ("evidence_hash", "sha256_hex"),
            ("available_at", "timestamptz"),
            ("ingested_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "reference_price_provenance",
        (
            ("reference_price_id", "int8"),
            ("source_candle_id", "int8"),
            ("source_candle_receipt_id", "int8"),
            ("linked_at", "timestamptz"),
        ),
    )
    + _column_requirements(
        "t4_runtime_config",
        (
            ("runtime_config_id", "int8"),
            ("source_id", "int8"),
            ("provider_key", "text"),
            ("venue_key", "text"),
            ("bridge_protocol", "text"),
            ("read_only", "bool"),
            ("order_routes_enabled", "bool"),
            ("observed_at", "timestamptz"),
            ("available_at", "timestamptz"),
            ("ingested_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "t4_ingestion_batches",
        (
            ("t4_batch_id", "int8"),
            ("source_id", "int8"),
            ("market_id", "int8"),
            ("logical_symbol", "text"),
            ("contract_id", "text"),
            ("contract_expires_at", "timestamptz"),
            ("contract_roll_at", "timestamptz"),
            ("contract_selection", "text"),
            ("interval_seconds", "int4"),
            ("requested_as_of", "timestamptz"),
            ("bridge_schema_version", "int4"),
            ("reference_price", "numeric"),
            ("reference_event_time", "timestamptz"),
            ("reference_available_at", "timestamptz"),
            ("reference_ingested_at", "timestamptz"),
            ("raw_payload_base64", "text"),
            ("raw_payload_hash", "sha256_hex"),
            ("record_count", "int4"),
            ("status", "text"),
            ("observed_at", "timestamptz"),
            ("available_at", "timestamptz"),
            ("ingested_at", "timestamptz"),
        ),
    )
    + _column_requirements(
        "t4_canonical_candles",
        (
            ("t4_candle_id", "int8"),
            ("t4_batch_id", "int8"),
            ("source_id", "int8"),
            ("market_id", "int8"),
            ("logical_symbol", "text"),
            ("contract_id", "text"),
            ("interval_seconds", "int4"),
            ("open_time", "timestamptz"),
            ("close_time", "timestamptz"),
            ("open_price", "numeric"),
            ("high_price", "numeric"),
            ("low_price", "numeric"),
            ("close_price", "numeric"),
            ("base_volume", "numeric"),
            ("available_at", "timestamptz"),
            ("provider_ingested_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
)


def _append_only_trigger_requirements(
    tables: Sequence[str],
) -> tuple[_TriggerRequirement, ...]:
    requirements: list[_TriggerRequirement] = []
    for table in tables:
        requirements.extend(
            (
                _TriggerRequirement(
                    name=f"{table}_append_only_row_guard",
                    table=table,
                    function="forbid_append_only_change",
                    # ROW | BEFORE | DELETE | UPDATE
                    type_mask=1 | 2 | 8 | 16,
                ),
                _TriggerRequirement(
                    name=f"{table}_append_only_truncate_guard",
                    table=table,
                    function="forbid_append_only_change",
                    # STATEMENT | BEFORE | TRUNCATE
                    type_mask=2 | 32,
                ),
            )
        )
    return tuple(requirements)


_BASE_TRIGGER_REQUIREMENTS = _append_only_trigger_requirements(_BASE_TABLES) + (
    _TriggerRequirement(
        "research_inputs_cutoff_guard",
        "research_run_inputs",
        "enforce_research_input_cutoff",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "paper_order_approval_guard",
        "paper_orders",
        "enforce_paper_order_approval",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "audit_hash_chain_guard",
        "audit_log",
        "enforce_audit_hash_chain",
        1 | 2 | 4,
    ),
)
_MIGRATED_TRIGGER_REQUIREMENTS = _append_only_trigger_requirements(_MIGRATED_TABLES) + (
    _TriggerRequirement(
        "source_candle_receipt_integrity_guard",
        "source_candle_receipts",
        "enforce_source_candle_receipt",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "market_data_source_binding_integrity_guard",
        "market_data_source_bindings",
        "enforce_market_data_source_binding",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "canonical_candle_series_integrity_guard",
        "canonical_candle_series",
        "enforce_canonical_candle_series",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "canonical_candle_manifest_integrity_guard",
        "canonical_candle_manifests",
        "enforce_canonical_candle_manifest",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "canonical_candle_provenance_integrity_guard",
        "canonical_candle_provenance",
        "enforce_canonical_candle_provenance",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "canonical_candle_exact_provenance_guard",
        "canonical_candle_manifests",
        "enforce_canonical_exact_provenance",
        1 | 4,
        deferrable=True,
        initially_deferred=True,
    ),
    _TriggerRequirement(
        "canonical_candle_exact_provenance_link_guard",
        "canonical_candle_provenance",
        "enforce_canonical_exact_provenance",
        1 | 4,
        deferrable=True,
        initially_deferred=True,
    ),
    _TriggerRequirement(
        "canonical_candle_manifest_reference_price_policy_guard",
        "canonical_candle_manifests",
        "enforce_v1_canonical_manifest_policy",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "reference_price_manifest_integrity_guard",
        "reference_price_manifests",
        "enforce_v1_reference_price_manifest",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "reference_price_provenance_integrity_guard",
        "reference_price_provenance",
        "enforce_v1_reference_price_provenance",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "reference_price_exact_provenance_guard",
        "reference_price_manifests",
        "enforce_v1_reference_price_exact_provenance",
        1 | 4,
        deferrable=True,
        initially_deferred=True,
    ),
    _TriggerRequirement(
        "reference_price_exact_provenance_link_guard",
        "reference_price_provenance",
        "enforce_v1_reference_price_exact_provenance",
        1 | 4,
        deferrable=True,
        initially_deferred=True,
    ),
)
_EXPECTED_BINDINGS = (
    ("plus500_t4_futures_v1", "plus500_t4", "BTC/USD", "BTC-FUTURES-FRONT"),
    ("plus500_t4_futures_v1", "plus500_t4", "ETH/USD", "ETH-FUTURES-FRONT"),
)
_EXPECTED_CANONICAL_SERIES: tuple[tuple[str, int], ...] = ()


def discover_migrations(directory: str | Path) -> tuple[Migration, ...]:
    root = Path(directory)
    if not root.is_dir():
        raise MigrationError("Migration directory does not exist")

    migrations: list[Migration] = []
    versions: set[str] = set()
    for path in sorted(root.glob("*.sql")):
        match = _MIGRATION_FILE.fullmatch(path.name)
        if match is None:
            continue
        version = match.group("version")
        if version in versions:
            raise MigrationError(f"Duplicate migration version: {version}")
        versions.add(version)
        payload = path.read_bytes()
        migrations.append(
            Migration(
                version=version,
                name=match.group("name"),
                path=path,
                checksum_sha256=hashlib.sha256(payload).hexdigest(),
            )
        )
    return tuple(migrations)


def plan_migrations(
    connection_factory: ConnectionFactory,
    migrations: Sequence[Migration],
) -> MigrationPlan:
    """Return a read-only plan and fail on checksum drift."""

    try:
        with transaction(connection_factory) as connection, cursor(connection) as db_cursor:
            _require_base_schema(db_cursor)
            db_cursor.execute("SELECT to_regclass('crypto_agent.schema_migrations')")
            ledger_exists = _first_value(db_cursor.fetchone()) is not None
            applied_rows: Sequence[object] = ()
            if ledger_exists:
                db_cursor.execute(
                    """
                    SELECT version, checksum_sha256
                    FROM crypto_agent.schema_migrations
                    ORDER BY version
                    """
                )
                applied_rows = db_cursor.fetchall()
    except (MigrationError, PostgresError):
        raise
    except Exception:
        raise PostgresOperationError("PostgreSQL migration plan failed") from None

    applied = {_row_value(row, "version", 0): _row_value(row, "checksum_sha256", 1)
               for row in applied_rows}
    for migration in migrations:
        recorded_checksum = applied.get(migration.version)
        if recorded_checksum is not None and recorded_checksum != migration.checksum_sha256:
            raise MigrationDriftError(
                f"Migration {migration.version} differs from its applied checksum"
            )
    pending = tuple(migration for migration in migrations if migration.version not in applied)
    return MigrationPlan(applied=tuple(sorted(str(item) for item in applied)), pending=pending)


def apply_migrations(
    connection_factory: ConnectionFactory,
    migrations: Sequence[Migration],
) -> tuple[str, ...]:
    """Apply pending migrations, one atomic transaction per migration."""

    plan = plan_migrations(connection_factory, migrations)
    applied_now: list[str] = []
    for migration in plan.pending:
        try:
            sql = migration.path.read_text(encoding="utf-8")
            with transaction(connection_factory) as connection, cursor(connection) as db_cursor:
                _require_base_schema(db_cursor)
                _create_migration_ledger(db_cursor)
                db_cursor.execute(
                    """
                    SELECT checksum_sha256
                    FROM crypto_agent.schema_migrations
                    WHERE version = %s
                    """,
                    (migration.version,),
                )
                existing = db_cursor.fetchone()
                if existing is not None:
                    if _first_value(existing) != migration.checksum_sha256:
                        raise MigrationDriftError(
                            f"Migration {migration.version} differs from its applied checksum"
                        )
                    continue
                db_cursor.execute(sql)
                db_cursor.execute(
                    """
                    INSERT INTO crypto_agent.schema_migrations (
                        version, name, checksum_sha256
                    ) VALUES (%s, %s, %s)
                    """,
                    (migration.version, migration.name, migration.checksum_sha256),
                )
            applied_now.append(migration.version)
        except (MigrationError, PostgresError):
            raise
        except (OSError, UnicodeError):
            raise MigrationError(f"Migration {migration.version} could not be read") from None
        except Exception:
            raise PostgresOperationError(
                f"PostgreSQL migration {migration.version} failed"
            ) from None
    return tuple(applied_now)


def apply_v1_seeds(
    connection_factory: ConnectionFactory,
    seed_path: str | Path,
) -> None:
    """Apply the idempotent operational V1 registry bundle atomically."""

    try:
        sql = Path(seed_path).read_text(encoding="utf-8")
        with transaction(connection_factory) as connection, cursor(connection) as db_cursor:
            _require_base_schema(db_cursor)
            db_cursor.execute(sql)
    except PostgresError:
        raise
    except (OSError, UnicodeError):
        raise MigrationError("PostgreSQL V1 seed bundle could not be read") from None
    except Exception:
        raise PostgresOperationError("PostgreSQL V1 seed bundle failed") from None


def check_postgres_health(
    connection_factory: ConnectionFactory,
    *,
    expected_migrations: Sequence[Migration] = (),
) -> PostgresHealth:
    """Perform a fail-closed, non-mutating PostgreSQL 16 readiness check.

    Readiness is deliberately tied to the migrations packaged with this binary.
    A caller-provided empty directory or a hand-picked subset therefore cannot turn
    an incomplete database into ``READY``.
    """

    try:
        required_migrations = _packaged_health_migrations(expected_migrations)
        policy_id, policy_hash = _health_policy_contract()
    except Exception:
        return _health_result(
            status_code="MIGRATION_MANIFEST_INVALID",
            missing_migrations=(),
        )

    required_versions = tuple(item.version for item in required_migrations)
    server_version: str | None = None
    try:
        with transaction(connection_factory) as connection, cursor(connection) as db_cursor:
            db_cursor.execute("SET TRANSACTION READ ONLY")
            db_cursor.execute(
                """
                SELECT current_setting('server_version_num'),
                       current_setting('server_version'),
                       current_setting('session_replication_role')
                """
            )
            version_row = db_cursor.fetchone()
            if version_row is None:
                return _health_result(
                    status_code="POSTGRES_VERSION_UNSUPPORTED",
                    database_reachable=True,
                    missing_migrations=required_versions,
                )
            version_number_raw = _row_value(version_row, "server_version_num", 0)
            server_version = str(_row_value(version_row, "server_version", 1))
            replication_role = str(
                _row_value(version_row, "session_replication_role", 2)
            )
            try:
                version_number = int(str(version_number_raw))
            except (TypeError, ValueError):
                version_number = 0
            if version_number // 10_000 != _REQUIRED_POSTGRES_MAJOR:
                return _health_result(
                    status_code="POSTGRES_VERSION_UNSUPPORTED",
                    database_reachable=True,
                    server_version=server_version,
                    missing_migrations=required_versions,
                )
            if replication_role != "origin":
                return _health_result(
                    status_code="POSTGRES_SESSION_UNSAFE",
                    database_reachable=True,
                    server_version=server_version,
                    missing_migrations=required_versions,
                )

            db_cursor.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s)",
                ("crypto_agent",),
            )
            schema_exists = _first_value(db_cursor.fetchone()) is True
            base_missing: list[str] = []
            if not schema_exists:
                base_missing.append("schema:crypto_agent")

            base_relations = _catalog_relations(db_cursor, _BASE_TABLES)
            base_missing.extend(
                f"table:{name}"
                for name in _BASE_TABLES
                if base_relations.get(name) != "r"
            )
            db_cursor.execute(
                """
                SELECT typ.typname, typ.typtype
                FROM pg_catalog.pg_type AS typ
                JOIN pg_catalog.pg_namespace AS ns
                  ON ns.oid = typ.typnamespace
                WHERE ns.nspname = 'crypto_agent'
                  AND typ.typname = ANY(%s)
                """,
                (["sha256_hex"],),
            )
            domains = {
                (str(_row_value(row, "typname", 0)), str(_row_value(row, "typtype", 1)))
                for row in db_cursor.fetchall()
            }
            if ("sha256_hex", "d") not in domains:
                base_missing.append("domain:sha256_hex")

            base_functions = _catalog_trigger_functions(
                db_cursor, _BASE_TRIGGER_FUNCTIONS
            )
            base_missing.extend(
                f"function:{name}"
                for name in _BASE_TRIGGER_FUNCTIONS
                if name not in base_functions
            )
            if base_missing:
                return _health_result(
                    status_code="BASE_SCHEMA_MISSING",
                    database_reachable=True,
                    server_version=server_version,
                    missing_migrations=required_versions,
                    missing_schema_objects=tuple(sorted(base_missing)),
                )

            base_trigger_differences = _trigger_differences(
                _BASE_TRIGGER_REQUIREMENTS,
                _catalog_triggers(db_cursor, _BASE_TABLES),
            )
            if base_trigger_differences:
                return _health_result(
                    status_code="BASE_TRIGGERS_MISSING",
                    database_reachable=True,
                    server_version=server_version,
                    missing_migrations=required_versions,
                    missing_triggers=base_trigger_differences,
                )

            db_cursor.execute("SELECT to_regclass('crypto_agent.schema_migrations')")
            ledger_exists = _first_value(db_cursor.fetchone()) is not None
            applied: dict[str, tuple[str, str]] = {}
            if ledger_exists:
                db_cursor.execute(
                    """
                    SELECT version, name, checksum_sha256
                    FROM crypto_agent.schema_migrations
                    ORDER BY version
                    """
                )
                applied = {
                    str(_row_value(row, "version", 0)): (
                        str(_row_value(row, "name", 1)),
                        str(_row_value(row, "checksum_sha256", 2)),
                    )
                    for row in db_cursor.fetchall()
                }

            required_by_version = {
                item.version: (item.name, item.checksum_sha256)
                for item in required_migrations
            }
            drifted = tuple(
                version
                for version, expected in required_by_version.items()
                if version in applied and applied[version] != expected
            )
            missing = tuple(
                version for version in required_by_version if version not in applied
            )
            ahead = tuple(
                sorted(version for version in applied if version not in required_by_version)
            )
            if drifted:
                return _health_result(
                    status_code="MIGRATION_DRIFT",
                    database_reachable=True,
                    base_schema_ready=True,
                    server_version=server_version,
                    missing_migrations=drifted,
                )
            if missing:
                return _health_result(
                    status_code="MIGRATIONS_PENDING",
                    database_reachable=True,
                    base_schema_ready=True,
                    server_version=server_version,
                    missing_migrations=missing,
                )
            if ahead:
                return _health_result(
                    status_code="MIGRATIONS_AHEAD",
                    database_reachable=True,
                    base_schema_ready=True,
                    server_version=server_version,
                    unexpected_migrations=ahead,
                )

            migrated_missing: list[str] = []
            migrated_relations = _catalog_relations(db_cursor, _MIGRATED_TABLES)
            migrated_missing.extend(
                f"table:{name}"
                for name in _MIGRATED_TABLES
                if migrated_relations.get(name) != "r"
            )
            migrated_missing.extend(
                _column_differences(
                    _MIGRATED_COLUMN_REQUIREMENTS,
                    _catalog_columns(db_cursor, _MIGRATED_TABLES),
                )
            )
            migrated_functions = _catalog_trigger_functions(
                db_cursor, _MIGRATED_TRIGGER_FUNCTIONS
            )
            migrated_missing.extend(
                f"function:{name}"
                for name in _MIGRATED_TRIGGER_FUNCTIONS
                if name not in migrated_functions
            )
            if migrated_missing:
                return _health_result(
                    status_code="MIGRATED_SCHEMA_MISSING",
                    database_reachable=True,
                    base_schema_ready=True,
                    migrations_current=True,
                    server_version=server_version,
                    missing_schema_objects=tuple(sorted(migrated_missing)),
                )

            migrated_trigger_differences = _trigger_differences(
                _MIGRATED_TRIGGER_REQUIREMENTS,
                _catalog_triggers(db_cursor, _MIGRATED_TABLES),
            )
            if migrated_trigger_differences:
                return _health_result(
                    status_code="MIGRATED_TRIGGERS_MISSING",
                    database_reachable=True,
                    base_schema_ready=True,
                    migrations_current=True,
                    server_version=server_version,
                    schema_ready=True,
                    missing_triggers=migrated_trigger_differences,
                )

            missing_seeds = _missing_seed_requirements(
                db_cursor,
                policy_id=policy_id,
                policy_hash=policy_hash,
            )
            if missing_seeds:
                return _health_result(
                    status_code="SEEDS_MISSING",
                    database_reachable=True,
                    base_schema_ready=True,
                    migrations_current=True,
                    server_version=server_version,
                    schema_ready=True,
                    triggers_ready=True,
                    missing_seeds=missing_seeds,
                )
    except Exception:
        return _health_result(
            status_code="POSTGRES_UNAVAILABLE",
            server_version=server_version,
            missing_migrations=required_versions,
        )

    return _health_result(
        status_code="READY",
        healthy=True,
        database_reachable=True,
        base_schema_ready=True,
        migrations_current=True,
        server_version=server_version,
        schema_ready=True,
        triggers_ready=True,
        seeds_ready=True,
    )


def _packaged_health_migrations(
    supplied: Sequence[Migration],
) -> tuple[Migration, ...]:
    from .resource_paths import default_migration_directory

    packaged = discover_migrations(default_migration_directory())
    if not packaged:
        raise MigrationError("Packaged PostgreSQL migration manifest is empty")
    if supplied:
        packaged_identity = tuple(
            (item.version, item.name, item.checksum_sha256) for item in packaged
        )
        supplied_identity = tuple(
            (item.version, item.name, item.checksum_sha256) for item in supplied
        )
        if supplied_identity != packaged_identity:
            raise MigrationError("PostgreSQL migration manifest is not packaged")
    return packaged


def _health_policy_contract() -> tuple[str, str]:
    from .policy import RiskPolicy
    from .resource_paths import default_risk_policy_path

    policy = RiskPolicy.load(default_risk_policy_path())
    return policy.policy_id, policy.fingerprint()


def _catalog_relations(db_cursor: DBCursor, names: Sequence[str]) -> dict[str, str]:
    db_cursor.execute(
        """
        SELECT rel.relname, rel.relkind
        FROM pg_catalog.pg_class AS rel
        JOIN pg_catalog.pg_namespace AS ns
          ON ns.oid = rel.relnamespace
        WHERE ns.nspname = 'crypto_agent'
          AND rel.relname = ANY(%s)
        """,
        (list(names),),
    )
    return {
        str(_row_value(row, "relname", 0)): str(_row_value(row, "relkind", 1))
        for row in db_cursor.fetchall()
    }


def _catalog_trigger_functions(
    db_cursor: DBCursor, names: Sequence[str]
) -> set[str]:
    db_cursor.execute(
        """
        SELECT proc.proname
        FROM pg_catalog.pg_proc AS proc
        JOIN pg_catalog.pg_namespace AS ns
          ON ns.oid = proc.pronamespace
        WHERE ns.nspname = 'crypto_agent'
          AND proc.proname = ANY(%s)
          AND proc.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype
        """,
        (list(names),),
    )
    return {str(_row_value(row, "proname", 0)) for row in db_cursor.fetchall()}


def _catalog_columns(
    db_cursor: DBCursor,
    tables: Sequence[str],
) -> tuple[_ColumnRequirement, ...]:
    db_cursor.execute(
        """
        SELECT rel.relname, attr.attname, typ.typname, attr.attnotnull
        FROM pg_catalog.pg_attribute AS attr
        JOIN pg_catalog.pg_class AS rel ON rel.oid = attr.attrelid
        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = rel.relnamespace
        JOIN pg_catalog.pg_type AS typ ON typ.oid = attr.atttypid
        WHERE ns.nspname = 'crypto_agent'
          AND rel.relname = ANY(%s)
          AND attr.attnum > 0
          AND NOT attr.attisdropped
        """,
        (list(tables),),
    )
    return tuple(
        _ColumnRequirement(
            table=str(_row_value(row, "relname", 0)),
            name=str(_row_value(row, "attname", 1)),
            type_name=str(_row_value(row, "typname", 2)),
            not_null=_row_value(row, "attnotnull", 3) is True,
        )
        for row in db_cursor.fetchall()
    )


def _column_differences(
    required: Sequence[_ColumnRequirement],
    actual: Sequence[_ColumnRequirement],
) -> tuple[str, ...]:
    required_by_key = {(item.table, item.name): item for item in required}
    actual_by_key = {(item.table, item.name): item for item in actual}
    differences = {
        f"column:{table}.{name}"
        for (table, name), requirement in required_by_key.items()
        if actual_by_key.get((table, name)) != requirement
    }
    differences.update(
        f"unexpected:column:{table}.{name}"
        for table, name in actual_by_key.keys() - required_by_key.keys()
    )
    return tuple(sorted(differences))


def _catalog_triggers(
    db_cursor: DBCursor, tables: Sequence[str]
) -> tuple[_TriggerRequirement, ...]:
    db_cursor.execute(
        """
        SELECT trg.tgname, rel.relname, proc.proname,
               trg.tgenabled, trg.tgtype,
               trg.tgdeferrable, trg.tginitdeferred
        FROM pg_catalog.pg_trigger AS trg
        JOIN pg_catalog.pg_class AS rel ON rel.oid = trg.tgrelid
        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = rel.relnamespace
        JOIN pg_catalog.pg_proc AS proc ON proc.oid = trg.tgfoid
        WHERE ns.nspname = 'crypto_agent'
          AND rel.relname = ANY(%s)
          AND NOT trg.tgisinternal
        """,
        (list(tables),),
    )
    result: list[_TriggerRequirement] = []
    for row in db_cursor.fetchall():
        enabled = str(_row_value(row, "tgenabled", 3))
        if enabled not in {"O", "A"}:
            continue
        result.append(
            _TriggerRequirement(
                name=str(_row_value(row, "tgname", 0)),
                table=str(_row_value(row, "relname", 1)),
                function=str(_row_value(row, "proname", 2)),
                type_mask=int(_row_value(row, "tgtype", 4)),
                deferrable=_row_value(row, "tgdeferrable", 5) is True,
                initially_deferred=_row_value(row, "tginitdeferred", 6) is True,
            )
        )
    return tuple(result)


def _trigger_differences(
    required: Sequence[_TriggerRequirement],
    actual: Sequence[_TriggerRequirement],
) -> tuple[str, ...]:
    required_set = set(required)
    actual_set = set(actual)
    missing = {item.name for item in required_set - actual_set}
    unexpected = {f"unexpected:{item.name}" for item in actual_set - required_set}
    return tuple(sorted(missing | unexpected))


def _missing_seed_requirements(
    db_cursor: DBCursor,
    *,
    policy_id: str,
    policy_hash: str,
) -> tuple[str, ...]:
    db_cursor.execute(
        """
        SELECT source.source_key, exchange.exchange_key,
               binding.canonical_symbol, binding.venue_symbol
        FROM crypto_agent.market_data_source_bindings binding
        JOIN crypto_agent.data_sources source ON source.source_id = binding.source_id
        JOIN crypto_agent.markets market ON market.market_id = binding.market_id
        JOIN crypto_agent.exchanges exchange ON exchange.exchange_id = market.exchange_id
        WHERE source.available_at <= CURRENT_TIMESTAMP
          AND source.ingested_at <= CURRENT_TIMESTAMP
          AND exchange.available_at <= CURRENT_TIMESTAMP
          AND exchange.ingested_at <= CURRENT_TIMESTAMP
          AND market.available_at <= CURRENT_TIMESTAMP
          AND market.ingested_at <= CURRENT_TIMESTAMP
          AND binding.available_at <= CURRENT_TIMESTAMP
          AND binding.ingested_at <= CURRENT_TIMESTAMP
          AND source.source_key = 'plus500_t4_futures_v1'
        """,
    )
    binding_counts = Counter(
        tuple(
            str(_row_value(row, key, index))
            for index, key in enumerate(
                ("source_key", "exchange_key", "canonical_symbol", "venue_symbol")
            )
        )
        for row in db_cursor.fetchall()
    )
    missing = [
        f"binding:{source}:{venue}:{symbol}:{venue_symbol}"
        for source, venue, symbol, venue_symbol in _EXPECTED_BINDINGS
        if binding_counts[(source, venue, symbol, venue_symbol)] != 1
    ]
    expected_bindings = set(_EXPECTED_BINDINGS)
    missing.extend(
        f"unexpected_binding:{source}:{venue}:{symbol}:{venue_symbol}"
        for source, venue, symbol, venue_symbol in sorted(binding_counts)
        if (source, venue, symbol, venue_symbol) not in expected_bindings
    )

    db_cursor.execute(
        """
        SELECT series.canonical_symbol, series.interval_seconds
        FROM crypto_agent.canonical_candle_series series
        JOIN crypto_agent.markets market
          ON market.market_id = series.canonical_market_id
        JOIN crypto_agent.exchanges exchange ON exchange.exchange_id = market.exchange_id
        JOIN crypto_agent.data_sources source
          ON source.source_id = series.canonical_source_id
        JOIN crypto_agent.risk_policies policy
          ON policy.risk_policy_id = series.risk_policy_id
         AND policy.content_hash = series.policy_hash
        WHERE series.algorithm_version = 'plus500_t4_direct_v1'
          AND series.policy_hash = %s
          AND policy.content_hash = %s
          AND policy.policy_document->>'policy_id' = %s
          AND policy.effective_from <= CURRENT_TIMESTAMP
          AND (policy.effective_to IS NULL OR policy.effective_to > CURRENT_TIMESTAMP)
          AND series.available_at <= CURRENT_TIMESTAMP
          AND series.ingested_at <= CURRENT_TIMESTAMP
          AND policy.available_at <= CURRENT_TIMESTAMP
          AND policy.ingested_at <= CURRENT_TIMESTAMP
          AND market.instrument_type = 'future'
          AND source.source_key = 'plus500_t4_futures_v1'
          AND exchange.exchange_key = 'plus500_t4'
        """,
        (
            policy_hash,
            policy_hash,
            policy_id,
        ),
    )
    series_counts = Counter(
        (
            str(_row_value(row, "canonical_symbol", 0)),
            int(_row_value(row, "interval_seconds", 1)),
        )
        for row in db_cursor.fetchall()
    )
    missing.extend(
        f"series:{symbol}:{interval_seconds}"
        for symbol, interval_seconds in _EXPECTED_CANONICAL_SERIES
        if series_counts[(symbol, interval_seconds)] != 1
    )
    expected_series = set(_EXPECTED_CANONICAL_SERIES)
    missing.extend(
        f"unexpected_series:{symbol}:{interval_seconds}"
        for symbol, interval_seconds in sorted(series_counts)
        if (symbol, interval_seconds) not in expected_series
    )
    return tuple(sorted(missing))


def _health_result(
    *,
    status_code: str,
    healthy: bool = False,
    database_reachable: bool = False,
    base_schema_ready: bool = False,
    migrations_current: bool = False,
    server_version: str | None = None,
    missing_migrations: tuple[str, ...] = (),
    schema_ready: bool = False,
    triggers_ready: bool = False,
    seeds_ready: bool = False,
    missing_schema_objects: tuple[str, ...] = (),
    missing_triggers: tuple[str, ...] = (),
    missing_seeds: tuple[str, ...] = (),
    unexpected_migrations: tuple[str, ...] = (),
) -> PostgresHealth:
    return PostgresHealth(
        healthy=healthy,
        database_reachable=database_reachable,
        base_schema_ready=base_schema_ready,
        migrations_current=migrations_current,
        server_version=server_version,
        missing_migrations=missing_migrations,
        status_code=status_code,
        schema_ready=schema_ready,
        triggers_ready=triggers_ready,
        seeds_ready=seeds_ready,
        missing_schema_objects=missing_schema_objects,
        missing_triggers=missing_triggers,
        missing_seeds=missing_seeds,
        unexpected_migrations=unexpected_migrations,
    )


def _require_base_schema(db_cursor: DBCursor) -> None:
    db_cursor.execute("SELECT to_regclass('crypto_agent.candles')")
    if _first_value(db_cursor.fetchone()) is None:
        raise MigrationError("Base PostgreSQL schema is not installed")


def _create_migration_ledger(db_cursor: DBCursor) -> None:
    db_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS crypto_agent.schema_migrations (
            version text PRIMARY KEY,
            name text NOT NULL,
            checksum_sha256 crypto_agent.sha256_hex NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CHECK (btrim(version) <> ''),
            CHECK (btrim(name) <> '')
        )
        """
    )


def _first_value(row: object | None) -> object | None:
    if row is None:
        return None
    return _row_value(row, "value", 0)


def _row_value(row: object, key: str, index: int) -> object:
    if isinstance(row, Mapping):
        typed_row = cast(Mapping[object, object], row)
        if key in typed_row:
            return typed_row[key]
        return tuple(typed_row.values())[index]
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        return cast(Sequence[object], row)[index]
    raise PostgresOperationError("PostgreSQL returned an unsupported row shape")

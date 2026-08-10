from __future__ import annotations

import hashlib
import importlib
import os
import re
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


_MIGRATION_FILE = re.compile(r"^(?P<version>[0-9]{4})_(?P<name>[a-z0-9_]+)\.sql$")


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


def check_postgres_health(
    connection_factory: ConnectionFactory,
    *,
    expected_migrations: Sequence[Migration] = (),
) -> PostgresHealth:
    """Perform a non-mutating readiness check without returning driver diagnostics."""

    try:
        with transaction(connection_factory) as connection, cursor(connection) as db_cursor:
            db_cursor.execute("SHOW server_version")
            server_version_value = _first_value(db_cursor.fetchone())
            server_version = None if server_version_value is None else str(server_version_value)
            db_cursor.execute("SELECT to_regclass('crypto_agent.candles')")
            base_ready = _first_value(db_cursor.fetchone()) is not None
            if not base_ready:
                return PostgresHealth(
                    healthy=False,
                    database_reachable=True,
                    base_schema_ready=False,
                    migrations_current=False,
                    server_version=server_version,
                    missing_migrations=tuple(item.version for item in expected_migrations),
                    status_code="BASE_SCHEMA_MISSING",
                )

            db_cursor.execute("SELECT to_regclass('crypto_agent.schema_migrations')")
            ledger_exists = _first_value(db_cursor.fetchone()) is not None
            applied: dict[str, str] = {}
            if ledger_exists:
                db_cursor.execute(
                    """
                    SELECT version, checksum_sha256
                    FROM crypto_agent.schema_migrations
                    ORDER BY version
                    """
                )
                applied = {
                    str(_row_value(row, "version", 0)): str(
                        _row_value(row, "checksum_sha256", 1)
                    )
                    for row in db_cursor.fetchall()
                }
    except Exception:
        return PostgresHealth(
            healthy=False,
            database_reachable=False,
            base_schema_ready=False,
            migrations_current=False,
            server_version=None,
            missing_migrations=tuple(item.version for item in expected_migrations),
            status_code="POSTGRES_UNAVAILABLE",
        )

    missing = tuple(item.version for item in expected_migrations if item.version not in applied)
    drifted = tuple(
        item.version
        for item in expected_migrations
        if item.version in applied and applied[item.version] != item.checksum_sha256
    )
    if drifted:
        return PostgresHealth(
            healthy=False,
            database_reachable=True,
            base_schema_ready=True,
            migrations_current=False,
            server_version=server_version,
            missing_migrations=drifted,
            status_code="MIGRATION_DRIFT",
        )
    if missing:
        return PostgresHealth(
            healthy=False,
            database_reachable=True,
            base_schema_ready=True,
            migrations_current=False,
            server_version=server_version,
            missing_migrations=missing,
            status_code="MIGRATIONS_PENDING",
        )
    return PostgresHealth(
        healthy=True,
        database_reachable=True,
        base_schema_ready=True,
        migrations_current=True,
        server_version=server_version,
        missing_migrations=(),
        status_code="READY",
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

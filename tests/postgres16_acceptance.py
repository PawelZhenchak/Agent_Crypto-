from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from crypto_agent.postgres import (  # noqa: E402
    PostgresSettings,
    PsycopgConnectionFactory,
    apply_migrations,
    apply_v1_seeds,
    check_postgres_health,
    discover_migrations,
    transaction,
)
from crypto_agent.resource_paths import default_v1_seed_path  # noqa: E402


def _factory() -> PsycopgConnectionFactory:
    return PsycopgConnectionFactory(PostgresSettings.from_env())


def _execute_file(path: Path) -> None:
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(path.read_text(encoding="utf-8"))
        connection.commit()
    finally:
        connection.close()


def _scalar(query: str) -> object:
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            db_cursor.execute(query)
            row = db_cursor.fetchone()
        connection.commit()
    finally:
        connection.close()
    if row is None:
        raise AssertionError("PostgreSQL acceptance query returned no row")
    return row[0]


def _expect_sqlstate(query: str, sqlstate: str) -> None:
    connection = _factory()()
    try:
        with connection.cursor() as db_cursor:
            try:
                db_cursor.execute(query)
                connection.commit()
            except Exception as exc:
                connection.rollback()
                if getattr(exc, "sqlstate", None) != sqlstate:
                    raise AssertionError(
                        f"expected SQLSTATE {sqlstate}, got {getattr(exc, 'sqlstate', None)}"
                    ) from exc
            else:
                raise AssertionError(f"query unexpectedly succeeded: {query}")
    finally:
        connection.close()


def _assert_ready() -> None:
    migrations = discover_migrations(PROJECT_ROOT / "db" / "migrations")
    health = check_postgres_health(_factory(), expected_migrations=migrations)
    if not health.healthy or health.status_code != "READY":
        raise AssertionError(f"PostgreSQL health is not READY: {health!r}")
    if health.server_version is None or not health.server_version.startswith("16."):
        raise AssertionError(f"unexpected PostgreSQL server version: {health.server_version}")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.market_data_source_bindings") != 2:
        raise AssertionError("expected exactly two Plus500 T4 instrument bindings")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_runtime_config") != 1:
        raise AssertionError("expected exactly one read-only T4 runtime config")


def bootstrap_and_test() -> None:
    _execute_file(PROJECT_ROOT / "db" / "schema.sql")
    migrations = discover_migrations(PROJECT_ROOT / "db" / "migrations")
    if apply_migrations(_factory(), migrations) != ("0011", "0012", "0013"):
        raise AssertionError("clean PostgreSQL 16 did not apply migrations 0011-0013")
    apply_v1_seeds(_factory(), default_v1_seed_path())
    apply_v1_seeds(_factory(), default_v1_seed_path())
    _assert_ready()

    _expect_sqlstate(
        "UPDATE crypto_agent.data_sources SET display_name = 'tampered' "
        "WHERE source_key = 'plus500_t4_futures_v1'",
        "55000",
    )
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
    print(f"postgres16 acceptance {args.mode}: READY")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

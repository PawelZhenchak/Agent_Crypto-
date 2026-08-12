from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from crypto_agent.domain import (  # noqa: E402
    Candle,
    ReferencePriceObservation,
    ReferencePriceSnapshot,
)
from crypto_agent.postgres import (  # noqa: E402
    PostgresSettings,
    PsycopgConnectionFactory,
    apply_migrations,
    apply_v1_seeds,
    check_postgres_health,
    discover_migrations,
    transaction,
)
from crypto_agent.providers.base import ProviderBatch  # noqa: E402
from crypto_agent.resource_paths import default_v1_seed_path  # noqa: E402
from crypto_agent.t4_ingest import T4IngestRepository  # noqa: E402


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


def _operational_batch() -> tuple[ProviderBatch, datetime]:
    as_of = datetime(2026, 8, 11, tzinfo=timezone.utc)
    candles: list[Candle] = []
    raw_candles: list[dict[str, object]] = []
    for index in range(60):
        close_time = as_of - timedelta(days=59 - index)
        raw = {
            "symbol": "BTC/USD",
            "interval_minutes": 1440,
            "open_time": (close_time - timedelta(days=1)).isoformat(),
            "close_time": close_time.isoformat(),
            "open": 100.0 + index,
            "high": 102.0 + index,
            "low": 99.0 + index,
            "close": 101.0 + index,
            "volume": 1000.0 + index,
            "source": "plus500_t4_futures_v1",
            "available_at": close_time.isoformat(),
            "ingested_at": as_of.isoformat(),
        }
        raw_candles.append(raw)
        candles.append(
            Candle(
                symbol="BTC/USD",
                interval_minutes=1440,
                open_time=close_time - timedelta(days=1),
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
    envelope = {
        "schema_version": 2,
        "source_id": "plus500_t4_futures_v1",
        "venue_id": "plus500_t4",
        "read_only": True,
        "order_routes_exposed": False,
        "logical_symbol": "BTC/USD",
        "interval_minutes": 1440,
        "contract_id": "CME:MBT:202609",
        "contract_expires_at": (as_of + timedelta(days=30)).isoformat(),
        "contract_roll_at": (as_of + timedelta(days=25)).isoformat(),
        "contract_selection": "front_month",
        "rolled_from_contract_id": None,
        "candles": raw_candles,
        "reference_price": {
            "symbol": "BTC/USD",
            "source": "plus500_t4_futures_v1",
            "price": 161.0,
            "event_time": (as_of - timedelta(minutes=1)).isoformat(),
            "available_at": as_of.isoformat(),
            "ingested_at": as_of.isoformat(),
        },
    }
    raw_payload = json.dumps(envelope, separators=(",", ":")).encode()
    batch = ProviderBatch(
        candles=tuple(candles),
        input_candles=tuple(candles),
        sources=({"id": "plus500_t4_futures_v1"},),
        metadata={
            "t4_contract_id": "CME:MBT:202609",
            "t4_contract_expires_at": (as_of + timedelta(days=30)).isoformat(),
            "t4_contract_roll_at": (as_of + timedelta(days=25)).isoformat(),
            "t4_contract_selection": "front_month",
            "t4_rolled_from_contract_id": None,
        },
        reference_price=ReferencePriceSnapshot(
            symbol="BTC/USD",
            observations=(
                ReferencePriceObservation(
                    symbol="BTC/USD",
                    price=161.0,
                    event_time=as_of - timedelta(minutes=1),
                    available_at=as_of,
                    ingested_at=as_of,
                    source="plus500_t4_futures_v1",
                ),
            ),
        ),
        raw_payload=raw_payload,
        raw_payload_sha256=hashlib.sha256(raw_payload).hexdigest(),
    )
    return batch, as_of


def _assert_operational_ingest_and_replay() -> None:
    batch, as_of = _operational_batch()
    repository = T4IngestRepository(_factory())
    first = repository.ingest(batch, requested_as_of=as_of)
    duplicate = repository.ingest(batch, requested_as_of=as_of)
    if not first.inserted or duplicate.inserted or first.batch_id != duplicate.batch_id:
        raise AssertionError("T4 exact-payload ingest is not idempotent")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_ingestion_batches") != 1:
        raise AssertionError("expected one immutable T4 ingest batch")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_canonical_candles") != 60:
        raise AssertionError("expected sixty normalized T4 candles")
    replay_as_of = datetime.now(timezone.utc)
    replay_a = repository.replay(
        symbol="BTC/USD", interval_minutes=1440, as_of=replay_as_of, limit=60
    )
    replay_b = repository.replay(
        symbol="BTC/USD", interval_minutes=1440, as_of=replay_as_of, limit=60
    )
    if replay_a.replay_fingerprint_sha256 != replay_b.replay_fingerprint_sha256:
        raise AssertionError("T4 point-in-time replay is not deterministic")


def _assert_operational_persistence() -> None:
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_ingestion_batches") != 1:
        raise AssertionError("T4 ingest batch did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_canonical_candles") != 60:
        raise AssertionError("T4 canonical candles did not survive PostgreSQL restart")


def bootstrap_and_test() -> None:
    _execute_file(PROJECT_ROOT / "db" / "schema.sql")
    migrations = discover_migrations(PROJECT_ROOT / "db" / "migrations")
    if apply_migrations(_factory(), migrations) != ("0011", "0012", "0013", "0014"):
        raise AssertionError("clean PostgreSQL 16 did not apply migrations 0011-0014")
    apply_v1_seeds(_factory(), default_v1_seed_path())
    apply_v1_seeds(_factory(), default_v1_seed_path())
    _assert_ready()
    _assert_operational_ingest_and_replay()

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

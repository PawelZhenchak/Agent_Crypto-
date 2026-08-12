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
    BasisReference,
    Candle,
    FuturesEvidence,
    OrderBookLevel,
    ReferencePriceObservation,
    ReferencePriceSnapshot,
    SessionStatus,
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
    for index in range(120):
        close_time = as_of - timedelta(days=119 - index)
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
    evidence = FuturesEvidence(
        contract_id="CME:MBT:202609",
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
        "contract_id": evidence.contract_id,
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
        "schema_version": 3,
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
            "t4_bridge_schema_version": 3,
            "t4_contract_id": "CME:MBT:202609",
            "t4_contract_expires_at": (as_of + timedelta(days=30)).isoformat(),
            "t4_contract_roll_at": (as_of + timedelta(days=25)).isoformat(),
            "t4_contract_selection": "front_month",
            "t4_rolled_from_contract_id": None,
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
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_canonical_candles") != 120:
        raise AssertionError("expected 120 normalized T4 candles")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_futures_snapshots") != 1:
        raise AssertionError("expected one immutable T4 futures snapshot")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_orderbook_levels") != 10:
        raise AssertionError("expected ten immutable T4 order-book levels")
    replay_as_of = datetime.now(timezone.utc)
    replay_a = repository.replay(
        symbol="BTC/USD", interval_minutes=1440, as_of=replay_as_of, limit=120
    )
    replay_b = repository.replay(
        symbol="BTC/USD", interval_minutes=1440, as_of=replay_as_of, limit=120
    )
    if replay_a.replay_fingerprint_sha256 != replay_b.replay_fingerprint_sha256:
        raise AssertionError("T4 point-in-time replay is not deterministic")
    if replay_a.futures_evidence != replay_b.futures_evidence:
        raise AssertionError("T4 futures evidence replay is not deterministic")


def _assert_operational_persistence() -> None:
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_ingestion_batches") != 1:
        raise AssertionError("T4 ingest batch did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_canonical_candles") != 120:
        raise AssertionError("T4 canonical candles did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_futures_snapshots") != 1:
        raise AssertionError("T4 futures snapshot did not survive PostgreSQL restart")
    if _scalar("SELECT COUNT(*) FROM crypto_agent.t4_orderbook_levels") != 10:
        raise AssertionError("T4 order book did not survive PostgreSQL restart")


def bootstrap_and_test() -> None:
    _execute_file(PROJECT_ROOT / "db" / "schema.sql")
    migrations = discover_migrations(PROJECT_ROOT / "db" / "migrations")
    if apply_migrations(_factory(), migrations) != (
        "0011",
        "0012",
        "0013",
        "0014",
        "0015",
    ):
        raise AssertionError("clean PostgreSQL 16 did not apply migrations 0011-0015")
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
        "UPDATE crypto_agent.t4_futures_snapshots SET session_status = 'CLOSED'",
        "55000",
    )
    _expect_sqlstate("TRUNCATE crypto_agent.t4_orderbook_levels", "55000")
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

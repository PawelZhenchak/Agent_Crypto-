from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

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
from crypto_agent.resource_paths import (  # noqa: E402
    default_monitoring_policy_path,
    default_v1_seed_path,
)
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
    _expect_sqlstate_after((), query, sqlstate)


def _expect_sqlstate_after(
    setup_queries: tuple[str, ...],
    query: str,
    sqlstate: str,
) -> None:
    connection = _factory()()
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
    as_of = datetime(2026, 8, 11, tzinfo=UTC)
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
        "schema_version": 4,
        "source_id": "plus500_t4_futures_v1",
        "venue_id": "plus500_t4",
        "read_only": True,
        "order_routes_exposed": False,
        "environment": "live_t4",
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
            "t4_bridge_schema_version": 4,
            "t4_environment": "live_t4",
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
        external_delivery_eligible=True,
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
    replay_as_of = datetime.now(UTC)
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


def _assert_operational_alert_outbox() -> None:
    observed_at = datetime.now(UTC)
    expires_at = observed_at + timedelta(hours=1)
    report = _operational_alert_report(observed_at, expires_at)
    policy = MonitoringPolicy.load(default_monitoring_policy_path())

    def warsaw_factory():  # type: ignore[no-untyped-def]
        connection = _factory()()
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
        instrument_id="plus500_t4_futures_v1:BTC/USD:1440m",
        horizon="1d",
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
            "system_version": "0.7.0-plus500-t4-v1",
            "mode": "V1_READ_ONLY",
            "execution_enabled": False,
            "not_financial_advice": True,
            "v1_gate_passed": False,
            "plus500_t4_source_attested": True,
            "external_delivery_eligible": True,
            "t4_bridge_schema_version": 4,
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


def bootstrap_and_test() -> None:
    _execute_file(PROJECT_ROOT / "db" / "schema.sql")
    migrations = discover_migrations(PROJECT_ROOT / "db" / "migrations")
    if apply_migrations(_factory(), migrations) != (
        "0011",
        "0012",
        "0013",
        "0014",
        "0015",
        "0016",
    ):
        raise AssertionError("clean PostgreSQL 16 did not apply migrations 0011-0016")
    apply_v1_seeds(_factory(), default_v1_seed_path())
    apply_v1_seeds(_factory(), default_v1_seed_path())
    _assert_ready()
    _assert_operational_ingest_and_replay()
    _assert_operational_alert_outbox()
    _assert_operational_alert_guards()

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

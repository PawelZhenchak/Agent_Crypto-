from __future__ import annotations

import hashlib
import importlib
import os
import re
import threading
from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, cast

from .deadline import (
    bounded_analysis_timeout,
    ensure_analysis_deadline,
    remaining_analysis_timeout,
)

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
        timeout_budget = bounded_analysis_timeout(
            float(self.settings.connect_timeout_seconds)
        )
        if timeout_budget < 1.0:
            raise TimeoutError("PostgreSQL connection deadline is too short")
        connect_timeout = max(1, int(timeout_budget))
        try:
            driver = importlib.import_module("psycopg")
            connection = driver.connect(
                self.settings.dsn,
                connect_timeout=connect_timeout,
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
    except (PostgresError, TimeoutError):
        raise
    except Exception:
        raise PostgresUnavailableError("PostgreSQL is unavailable") from None

    try:
        yield connection
        _commit_with_analysis_deadline(connection)
    except Exception:
        with suppress(Exception):
            connection.rollback()
        raise
    finally:
        with suppress(Exception):
            connection.close()


def _commit_with_analysis_deadline(connection: DBConnection) -> None:
    """Commit with cancellation support when a total analysis deadline is active."""

    remaining = remaining_analysis_timeout()
    if remaining is None:
        connection.commit()
        return

    cancel_method = getattr(connection, "cancel", None)
    if not callable(cancel_method):
        raise PostgresOperationError(
            "PostgreSQL connection cannot enforce the analysis deadline"
        )
    cancel = cast(Callable[[], object], cancel_method)
    deadline_reached = threading.Event()
    commit_finished = threading.Event()
    timer = threading.Timer(
        remaining,
        _cancel_commit_until_finished,
        args=(cancel, deadline_reached, commit_finished),
    )
    timer.daemon = True
    timer.start()
    try:
        ensure_analysis_deadline()
        if deadline_reached.is_set():
            ensure_analysis_deadline()
        connection.commit()
    except Exception:
        ensure_analysis_deadline()
        raise
    finally:
        commit_finished.set()
        timer.cancel()
    ensure_analysis_deadline()


def _cancel_commit_until_finished(
    cancel: Callable[[], object],
    deadline_reached: threading.Event,
    commit_finished: threading.Event,
) -> None:
    deadline_reached.set()
    # Cancellation can race just ahead of the driver's COMMIT I/O and become a
    # no-op. Retry until the caller confirms that COMMIT returned; ``cancel()`` is
    # Psycopg's thread-safe interruption boundary and uses a separate libpq request.
    while not commit_finished.is_set():
        with suppress(Exception):
            cancel()
        commit_finished.wait(0.001)


@contextmanager
def cursor(connection: DBConnection) -> Iterator[DBCursor]:
    database_cursor = connection.cursor()
    try:
        yield database_cursor
    finally:
        with suppress(Exception):
            database_cursor.close()


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


@dataclass(frozen=True, slots=True)
class _CatalogDefinitionRequirement:
    table: str
    name: str
    object_type: str
    definition: str


@dataclass(frozen=True, slots=True)
class _IndexRequirement:
    table: str
    name: str
    unique: bool
    definition: str


@dataclass(frozen=True, slots=True)
class _FunctionDefinitionRequirement:
    name: str
    language: str
    volatility: str
    security_definer: bool
    source_sha256: str


@dataclass(frozen=True, slots=True)
class _RoutineRequirement:
    name: str
    argument_count: int
    identity_arguments: str
    language: str
    volatility: str
    security_definer: bool
    owner: str
    fixed_search_path: bool
    source_sha256: str


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
    "t4_futures_snapshots",
    "t4_orderbook_levels",
    "t4_contract_transition_evidence",
    "alert_delivery_outbox",
    "alert_delivery_attempts",
    "t4_observation_campaigns",
    "t4_observation_research_inputs",
    "t4_observation_cycles",
    "t4_observation_session_events",
    "t4_observation_quality_reports",
    "t4_bridge_evidence_keys",
    "t4_bridge_observation_events",
    "t4_replay_verifier_attestations",
    "t4_observation_scenario_trial_requests",
    "t4_observation_scenario_trials",
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
    "enforce_alert_delivery_attempt",
    "enforce_t4_schema_v5_evidence",
    "enforce_t4_observation_campaign_start",
    "enforce_t4_observation_final_report",
    "enforce_t4_observation_research_input",
    "enforce_t4_observation_cycle_chain",
    "enforce_t4_observation_event_chain",
    "enforce_t4_bridge_observation_event",
    "enforce_t4_observation_scenario_trial",
    "enforce_t4_scenario_alert_finalization",
    "serialize_t4_observation_campaign_write",
)
_MIGRATED_ROUTINE_REQUIREMENTS = (
    _RoutineRequirement(
        "t4_evidence_verifier_session_is_safe", 0, "", "sql", "s", False,
        "crypto_agent_evidence_owner", True,
        "aa4f8a329e29f5252573bf1de0b32bb02a75973932ad65eaccb425ff6b4f4aa1",
    ),
    _RoutineRequirement(
        "t4_replay_attestations_are_verified", 2,
        "p_campaign_id uuid, p_attestation_ids bigint[]", "plpgsql", "s", True,
        "crypto_agent_evidence_owner", True,
        "53924f4fd74aead92ac3b07e98f40cd3860c594a4e68097adcb7292cac3e05b1",
    ),
    _RoutineRequirement(
        "forbid_append_only_change", 0, "", "plpgsql", "v", False,
        "crypto_agent_evidence_owner", True,
        "365608cac855e60c4fa0138ffaf654ee4cb66ef261e83086e47d9b71f60bc252",
    ),
    _RoutineRequirement(
        "canonical_jsonb_text", 1, "p_value jsonb", "plpgsql", "i", False,
        "crypto_agent_evidence_owner", True,
        "04429a83abe2acdc537329532cf7e3ec9206a3673d2757d9fe6a7714f4f4d9c1",
    ),
    _RoutineRequirement(
        "register_t4_bridge_evidence_key", 1,
        "p_public_key_spki_base64 text", "plpgsql", "v", True,
        "crypto_agent_evidence_owner", True,
        "10d90542e5a8cdd58c743a07ba5498b1eedcb7c228c70ad82ea557b3996dfa99",
    ),
    _RoutineRequirement(
        "record_verified_t4_bridge_observation_event", 17,
        "p_event_id uuid, p_campaign_id uuid, p_sequence_no bigint, "
        "p_event_at timestamp with time zone, p_event_type text, "
        "p_reason_code text, p_boot_id uuid, p_session_generation bigint, "
        "p_reconnect_count integer, p_scenario_code text, "
        "p_action_request_id uuid, p_previous_event_hash "
        "crypto_agent.sha256_hex, p_payload_hash_sha256 "
        "crypto_agent.sha256_hex, p_event_hash_sha256 "
        "crypto_agent.sha256_hex, p_canonical_payload_base64 text, "
        "p_evidence_key_fingerprint_sha256 crypto_agent.sha256_hex, "
        "p_signature_base64 text",
        "plpgsql", "v", True, "crypto_agent_evidence_owner", True,
        "79f4c4d7fe8058942003b374c9504db4a55e7bb56fd3d83beebba969056b08d9",
    ),
    _RoutineRequirement(
        "record_verified_t4_replay_attestation", 8,
        "p_attestation_id uuid, p_campaign_id uuid, p_scope_key text, "
        "p_replay_as_of timestamp with time zone, "
        "p_replay_fingerprint_sha256 crypto_agent.sha256_hex, "
        "p_source_batch_ids bigint[], p_source_batch_hashes "
        "crypto_agent.sha256_hex[], p_provenance_sha256 "
        "crypto_agent.sha256_hex",
        "plpgsql", "v", True, "crypto_agent_evidence_owner", True,
        "77d4d0e1dbf55a127ed22706fb6b5f96532275f8ff6b2a87b3100c0aec5a1370",
    ),
    _RoutineRequirement(
        "begin_t4_observation_scenario_trial", 4,
        "p_campaign_id uuid, p_scenario_code text, p_action_request_id uuid, "
        "p_scope_key text", "plpgsql", "v", True,
        "crypto_agent_evidence_owner", True,
        "b8544458b532eb9e1a9365569a91a6fcb722a6bf89b23f5f9c42c2b6b27953ea",
    ),
    _RoutineRequirement(
        "complete_t4_observation_scenario_trial", 3,
        "p_trial_id uuid, p_event_hashes crypto_agent.sha256_hex[], "
        "p_control_receipt_event_hash crypto_agent.sha256_hex",
        "plpgsql", "v", True, "crypto_agent_evidence_owner", True,
        "590a1f9ae99236e325d02f642f48ff457d4ebc5192333ca6bd4e44567434c334",
    ),
    _RoutineRequirement(
        "verify_passive_t4_observation_scenario", 2,
        "p_campaign_id uuid, p_scenario_code text", "plpgsql", "v", True,
        "crypto_agent_evidence_owner", True,
        "fcd08ad41f87a28a6034f9837180e67630def7f18b4c72846db468dc71557acd",
    ),
    _RoutineRequirement(
        "t4_observation_gate_is_verified", 1, "p_campaign_id uuid",
        "plpgsql", "v", True, "crypto_agent_evidence_owner", True,
        "dbe5d89b09c440440f60466aa22d04904e67065c13421e3add84abcb1d2b9023",
    ),
    _RoutineRequirement(
        "t4_observation_gate_is_verified", 2,
        "p_campaign_id uuid, p_checkpoint_event_id uuid", "plpgsql", "v",
        True, "crypto_agent_evidence_owner", True,
        "bf94252b1e006c8d84cf5b13528b579be476f91f51082c5175997a29b7e8a114",
    ),
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
    + (
        _ColumnRequirement(
            table="t4_ingestion_batches",
            name="rolled_from_contract_id",
            type_name="text",
            not_null=False,
        ),
        _ColumnRequirement(
            table="t4_ingestion_batches",
            name="environment",
            type_name="text",
            not_null=False,
        ),
        _ColumnRequirement(
            table="t4_ingestion_batches",
            name="exchange_id",
            type_name="text",
            not_null=False,
        ),
        _ColumnRequirement(
            table="t4_ingestion_batches",
            name="active_market_id",
            type_name="text",
            not_null=False,
        ),
        _ColumnRequirement(
            table="t4_ingestion_batches",
            name="rolled_from_market_id",
            type_name="text",
            not_null=False,
        ),
        _ColumnRequirement(
            table="t4_ingestion_batches",
            name="basis_exchange_id",
            type_name="text",
            not_null=False,
        ),
        _ColumnRequirement(
            table="t4_ingestion_batches",
            name="basis_contract_id",
            type_name="text",
            not_null=False,
        ),
        _ColumnRequirement(
            table="t4_ingestion_batches",
            name="basis_market_id",
            type_name="text",
            not_null=False,
        ),
    )
    + _column_requirements(
        "t4_canonical_candles",
        (
            ("t4_candle_id", "int8"),
            ("t4_batch_id", "int8"),
            ("source_id", "int8"),
            ("registry_market_id", "int8"),
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
    + (
        _ColumnRequirement(
            table="t4_canonical_candles",
            name="market_id",
            type_name="text",
            not_null=False,
        ),
    )
    + _column_requirements(
        "t4_futures_snapshots",
        (
            ("t4_snapshot_id", "int8"),
            ("t4_batch_id", "int8"),
            ("source_id", "int8"),
            ("market_id", "int8"),
            ("logical_symbol", "text"),
            ("contract_id", "text"),
            ("session_status", "text"),
            ("is_full_snapshot", "bool"),
            ("observed_at", "timestamptz"),
            ("available_at", "timestamptz"),
            ("provider_ingested_at", "timestamptz"),
            ("basis_reference_symbol", "text"),
            ("basis_reference_type", "text"),
            ("basis_reference_source", "text"),
            ("basis_reference_price", "numeric"),
            ("basis_observed_at", "timestamptz"),
            ("basis_available_at", "timestamptz"),
            ("basis_ingested_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + tuple(
        _ColumnRequirement(
            table="t4_futures_snapshots",
            name=name,
            type_name="text",
            not_null=False,
        )
        for name in (
            "exchange_id",
            "product_contract_id",
            "active_market_id",
            "basis_exchange_id",
            "basis_product_contract_id",
            "basis_market_id",
        )
    )
    + _column_requirements(
        "t4_orderbook_levels",
        (
            ("t4_snapshot_id", "int8"),
            ("side", "text"),
            ("level_no", "int4"),
            ("price", "numeric"),
            ("quantity", "numeric"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "t4_contract_transition_evidence",
        (
            ("t4_transition_id", "int8"),
            ("t4_batch_id", "int8"),
            ("source_id", "int8"),
            ("market_id", "int8"),
            ("logical_symbol", "text"),
            ("from_contract_id", "text"),
            ("to_contract_id", "text"),
            ("price_type", "text"),
            ("from_price", "numeric"),
            ("to_price", "numeric"),
            ("evidence_source", "text"),
            ("observed_at", "timestamptz"),
            ("available_at", "timestamptz"),
            ("provider_ingested_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + tuple(
        _ColumnRequirement(
            table="t4_contract_transition_evidence",
            name=name,
            type_name="text",
            not_null=False,
        )
        for name in (
            "exchange_id",
            "product_contract_id",
            "from_market_id",
            "to_market_id",
        )
    )
    + _column_requirements(
        "alert_delivery_outbox",
        (
            ("alert_delivery_outbox_id", "int8"),
            ("alert_id", "int8"),
            ("channel", "text"),
            ("destination", "text"),
            ("idempotency_key", "sha256_hex"),
            ("monitoring_policy_id", "text"),
            ("monitoring_policy_hash", "sha256_hex"),
            ("retention_days", "int2"),
            ("payload", "jsonb"),
            ("payload_hash", "sha256_hex"),
            ("available_at", "timestamptz"),
            ("expires_at", "timestamptz"),
            ("max_attempts", "int2"),
            ("created_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "alert_delivery_attempts",
        (
            ("alert_delivery_attempt_id", "int8"),
            ("alert_delivery_outbox_id", "int8"),
            ("attempt_no", "int2"),
            ("outcome", "text"),
            ("started_at", "timestamptz"),
            ("finished_at", "timestamptz"),
            ("request_payload_hash", "sha256_hex"),
            ("content_hash", "sha256_hex"),
            ("created_at", "timestamptz"),
        ),
    )
    + (
        _ColumnRequirement(
            table="alert_delivery_attempts",
            name="next_attempt_at",
            type_name="timestamptz",
            not_null=False,
        ),
        _ColumnRequirement(
            table="alert_delivery_attempts",
            name="error_code",
            type_name="text",
            not_null=False,
        ),
        _ColumnRequirement(
            table="alert_delivery_attempts",
            name="previous_attempt_hash",
            type_name="sha256_hex",
            not_null=False,
        ),
    )
    + _column_requirements(
        "t4_observation_campaigns",
        (
            ("campaign_id", "uuid"),
            ("environment", "text"),
            ("started_at", "timestamptz"),
            ("planned_ends_at", "timestamptz"),
            ("cycle_interval_seconds", "int4"),
            ("observation_policy_id", "text"),
            ("observation_policy_hash", "sha256_hex"),
            ("code_commit_hash", "sha256_hex"),
            ("t4_protocol_commit_hash", "sha256_hex"),
            ("runtime_config_hash", "sha256_hex"),
            ("scope_manifest", "jsonb"),
            ("scope_manifest_hash", "sha256_hex"),
            ("frozen_baseline_hash", "sha256_hex"),
            ("read_only", "bool"),
            ("execution_enabled", "bool"),
            ("content_hash", "sha256_hex"),
            ("created_at", "timestamptz"),
        ),
    )
    + _column_requirements(
        "t4_observation_research_inputs",
        (
            ("observation_research_input_id", "int8"),
            ("campaign_id", "uuid"),
            ("scope_key", "text"),
            ("sequence_no", "int4"),
            ("expected_at", "timestamptz"),
            ("t4_batch_id", "int8"),
            ("research_run_id", "int8"),
            ("raw_payload_hash", "sha256_hex"),
            ("analysis_input_hash", "sha256_hex"),
            ("trace_id", "uuid"),
            ("content_hash", "sha256_hex"),
            ("created_at", "timestamptz"),
        ),
    )
    + _column_requirements(
        "t4_observation_cycles",
        (
            ("observation_cycle_id", "int8"),
            ("campaign_id", "uuid"),
            ("scope_key", "text"),
            ("sequence_no", "int4"),
            ("expected_at", "timestamptz"),
            ("outcome", "text"),
            ("read_only", "bool"),
            ("execution_enabled", "bool"),
            ("content_hash", "sha256_hex"),
            ("created_at", "timestamptz"),
        ),
    )
    + tuple(
        _ColumnRequirement(
            table="t4_observation_cycles",
            name=name,
            type_name=type_name,
            not_null=False,
        )
        for name, type_name in (
            ("started_at", "timestamptz"),
            ("finished_at", "timestamptz"),
            ("error_code", "text"),
            ("gap_explanation_code", "text"),
            ("t4_batch_id", "int8"),
            ("research_run_id", "int8"),
            ("analysis_input_hash", "sha256_hex"),
            ("bridge_schema_version", "int2"),
            ("raw_payload_hash", "sha256_hex"),
            ("bridge_rtt_milliseconds", "int4"),
            ("trace_id", "uuid"),
            ("previous_cycle_hash", "sha256_hex"),
        )
    )
    + _column_requirements(
        "t4_observation_session_events",
        (
            ("observation_session_event_id", "int8"),
            ("campaign_id", "uuid"),
            ("sequence_no", "int4"),
            ("event_at", "timestamptz"),
            ("event_type", "text"),
            ("outcome", "text"),
            ("read_only", "bool"),
            ("execution_enabled", "bool"),
            ("content_hash", "sha256_hex"),
            ("created_at", "timestamptz"),
        ),
    )
    + tuple(
        _ColumnRequirement(
            table="t4_observation_session_events",
            name=name,
            type_name=type_name,
            not_null=False,
        )
        for name, type_name in (
            ("scenario_code", "text"),
            ("detail_code", "text"),
            ("previous_event_hash", "sha256_hex"),
        )
    )
    + _column_requirements(
        "t4_observation_quality_reports",
        (
            ("observation_quality_report_id", "int8"),
            ("campaign_id", "uuid"),
            ("generated_at", "timestamptz"),
            ("observed_until", "timestamptz"),
            ("overall_status", "text"),
            ("v1_gate_passed", "bool"),
            ("observation_policy_id", "text"),
            ("observation_policy_hash", "sha256_hex"),
            ("frozen_baseline_hash", "sha256_hex"),
            ("report", "jsonb"),
            ("report_hash", "sha256_hex"),
            ("content_hash", "sha256_hex"),
            ("created_at", "timestamptz"),
        ),
    )
    + tuple(
        _ColumnRequirement(
            table="t4_observation_quality_reports",
            name=name,
            type_name=type_name,
            not_null=False,
        )
        for name, type_name in (
            ("bridge_checkpoint_event_id", "uuid"),
            ("bridge_checkpoint_sequence_no", "int8"),
            ("bridge_checkpoint_event_hash", "sha256_hex"),
            ("bridge_checkpoint_action_request_id", "uuid"),
        )
    )
    + (
        _ColumnRequirement(
            table="t4_observation_campaigns",
            name="bridge_evidence_key_fingerprint",
            type_name="sha256_hex",
            not_null=False,
        ),
    )
    + _column_requirements(
        "t4_bridge_evidence_keys",
        (
            ("evidence_key_fingerprint", "sha256_hex"),
            ("public_key_spki_base64", "text"),
            ("signature_algorithm", "text"),
            ("registered_at", "timestamptz"),
        ),
    )
    + (
        _ColumnRequirement(
            table="t4_bridge_evidence_keys",
            name="retired_at",
            type_name="timestamptz",
            not_null=False,
        ),
    )
    + _column_requirements(
        "t4_bridge_observation_events",
        (
            ("bridge_observation_event_id", "int8"),
            ("event_id", "uuid"),
            ("sequence_no", "int8"),
            ("event_at", "timestamptz"),
            ("event_type", "text"),
            ("reason_code", "text"),
            ("boot_id", "uuid"),
            ("session_generation", "int8"),
            ("reconnect_count", "int4"),
            ("environment", "text"),
            ("bridge_schema_version", "int2"),
            ("read_only", "bool"),
            ("order_routes_exposed", "bool"),
            ("payload", "jsonb"),
            ("payload_hash_sha256", "sha256_hex"),
            ("event_hash_sha256", "sha256_hex"),
            ("canonical_payload_base64", "text"),
            ("signature_algorithm", "text"),
            ("evidence_key_fingerprint_sha256", "sha256_hex"),
            ("signature_base64", "text"),
            ("signature_verified", "bool"),
            ("signature_verified_at", "timestamptz"),
            ("signature_verified_by", "name"),
            ("received_at", "timestamptz"),
        ),
    )
    + tuple(
        _ColumnRequirement(
            table="t4_bridge_observation_events",
            name=name,
            type_name=type_name,
            not_null=False,
        )
        for name, type_name in (
            ("campaign_id", "uuid"),
            ("scenario_code", "text"),
            ("action_request_id", "uuid"),
            ("scope_key", "text"),
            ("control_action", "text"),
            ("control_step", "text"),
            ("roll_from_market_id", "text"),
            ("roll_to_market_id", "text"),
            ("previous_event_hash", "sha256_hex"),
        )
    )
    + _column_requirements(
        "t4_replay_verifier_attestations",
        (
            ("replay_attestation_id", "int8"),
            ("attestation_id", "uuid"),
            ("campaign_id", "uuid"),
            ("scope_key", "text"),
            ("replay_as_of", "timestamptz"),
            ("replay_limit", "int4"),
            ("replay_fingerprint_sha256", "sha256_hex"),
            ("source_batch_ids", "_int8"),
            ("source_batch_hashes", "_sha256_hex"),
            ("provenance_sha256", "sha256_hex"),
            ("read_only", "bool"),
            ("execution_enabled", "bool"),
            ("external_delivery_eligible", "bool"),
            ("attested_at", "timestamptz"),
            ("attested_by", "name"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "t4_observation_scenario_trial_requests",
        (
            ("trial_id", "uuid"),
            ("campaign_id", "uuid"),
            ("scenario_code", "text"),
            ("scope_key", "text"),
            ("action_request_id", "uuid"),
            ("started_at", "timestamptz"),
            ("content_hash", "sha256_hex"),
        ),
    )
    + _column_requirements(
        "t4_observation_scenario_trials",
        (
            ("scenario_trial_id", "int8"),
            ("trial_id", "uuid"),
            ("campaign_id", "uuid"),
            ("scenario_code", "text"),
            ("outcome", "text"),
            ("started_at", "timestamptz"),
            ("completed_at", "timestamptz"),
            ("evidence_event_ids", "_uuid"),
            ("evidence_event_hashes", "_sha256_hex"),
            ("replay_attestation_ids", "_int8"),
            ("detail_code", "text"),
            ("content_hash", "sha256_hex"),
            ("created_at", "timestamptz"),
        ),
    )
    + tuple(
        _ColumnRequirement(
            table="t4_observation_scenario_trials",
            name=name,
            type_name=type_name,
            not_null=False,
        )
        for name, type_name in (
            ("action_request_id", "uuid"),
            ("bridge_boot_id", "uuid"),
            ("first_event_id", "uuid"),
            ("last_event_id", "uuid"),
            ("pre_t4_batch_id", "int8"),
            ("post_t4_batch_id", "int8"),
            ("observation_cycle_id", "int8"),
            ("research_run_id", "int8"),
            ("replay_fingerprint_sha256", "sha256_hex"),
        )
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
    _TriggerRequirement(
        "alert_delivery_attempt_integrity_guard",
        "alert_delivery_attempts",
        "enforce_alert_delivery_attempt",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_canonical_candles_schema_v5_guard",
        "t4_canonical_candles",
        "enforce_t4_schema_v5_evidence",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_futures_snapshots_schema_v5_guard",
        "t4_futures_snapshots",
        "enforce_t4_schema_v5_evidence",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_contract_transitions_schema_v5_guard",
        "t4_contract_transition_evidence",
        "enforce_t4_schema_v5_evidence",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_observation_campaign_start_guard",
        "t4_observation_campaigns",
        "enforce_t4_observation_campaign_start",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_observation_research_input_guard",
        "t4_observation_research_inputs",
        "enforce_t4_observation_research_input",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_observation_cycle_chain_guard",
        "t4_observation_cycles",
        "enforce_t4_observation_cycle_chain",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_observation_event_chain_guard",
        "t4_observation_session_events",
        "enforce_t4_observation_event_chain",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_observation_final_report_guard",
        "t4_observation_quality_reports",
        "enforce_t4_observation_final_report",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_bridge_observation_event_integrity_guard",
        "t4_bridge_observation_events",
        "enforce_t4_bridge_observation_event",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_observation_scenario_trial_integrity_guard",
        "t4_observation_scenario_trials",
        "enforce_t4_observation_scenario_trial",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_observation_campaign_serialization_input_guard",
        "t4_observation_research_inputs",
        "serialize_t4_observation_campaign_write",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_observation_campaign_serialization_cycle_guard",
        "t4_observation_cycles",
        "serialize_t4_observation_campaign_write",
        1 | 2 | 4,
    ),
    _TriggerRequirement(
        "t4_scenario_alert_finalization_guard",
        "alerts",
        "enforce_t4_scenario_alert_finalization",
        1 | 2 | 4,
    ),
)
_OPERATIONAL_CONSTRAINT_REQUIREMENTS = (
    _CatalogDefinitionRequirement(
        "t4_ingestion_batches",
        "t4_ingestion_batches_bridge_schema_version_check",
        "c",
        "CHECK ((bridge_schema_version = ANY (ARRAY[2, 3, 4, 5])))",
    ),
    _CatalogDefinitionRequirement(
        "t4_contract_transition_evidence",
        "t4_contract_transition_batch_fk",
        "f",
        "FOREIGN KEY (t4_batch_id) REFERENCES "
        "crypto_agent.t4_ingestion_batches(t4_batch_id)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_pkey",
        "p",
        "PRIMARY KEY (alert_delivery_outbox_id)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_alert_fk",
        "f",
        "FOREIGN KEY (alert_id) REFERENCES crypto_agent.alerts(alert_id)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_idempotency_key_key",
        "u",
        "UNIQUE (idempotency_key)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_content_hash_key",
        "u",
        "UNIQUE (content_hash)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_alert_id_channel_destination_key",
        "u",
        "UNIQUE (alert_id, channel, destination)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_channel_check",
        "c",
        "CHECK ((channel = 'stdout_json'::text))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_destination_check",
        "c",
        "CHECK ((destination = 'process_stdout'::text))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_policy_id_check",
        "c",
        "CHECK (((btrim(monitoring_policy_id) <> ''::text) "
        "AND (length(monitoring_policy_id) <= 128)))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_retention_check",
        "c",
        "CHECK (((retention_days >= 35) AND (retention_days <= 365)))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_max_attempts_check",
        "c",
        "CHECK (((max_attempts >= 1) AND (max_attempts <= 5)))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check",
        "c",
        "CHECK ((jsonb_typeof(payload) = 'object'::text))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check1",
        "c",
        """
        CHECK (((payload ?& ARRAY['schema_version'::text,
            'alert_type'::text, 'decision'::text,
            'decision_id'::text, 'trace_id'::text, 'asset_id'::text,
            'instrument_id'::text, 'horizon'::text, 'as_of'::text,
            'expires_at'::text, 'reason_codes'::text, 'data_snapshot_id'::text,
            'policy_id'::text, 'policy_hash_sha256'::text,
            'futures_policy_id'::text, 'futures_policy_hash_sha256'::text,
            'environment'::text, 'external_delivery'::text, 'read_only'::text,
            'execution_enabled'::text, 'not_financial_advice'::text,
            'monitoring_policy_id'::text,
            'monitoring_policy_hash_sha256'::text, 'retention_days'::text])
        AND ((payload - ARRAY['schema_version'::text,
            'alert_type'::text, 'decision'::text,
            'decision_id'::text, 'trace_id'::text, 'asset_id'::text,
            'instrument_id'::text, 'horizon'::text, 'as_of'::text,
            'expires_at'::text, 'reason_codes'::text, 'data_snapshot_id'::text,
            'policy_id'::text, 'policy_hash_sha256'::text,
            'futures_policy_id'::text, 'futures_policy_hash_sha256'::text,
            'environment'::text, 'external_delivery'::text, 'read_only'::text,
            'execution_enabled'::text, 'not_financial_advice'::text,
            'monitoring_policy_id'::text,
            'monitoring_policy_hash_sha256'::text, 'retention_days'::text])
        = '{}'::jsonb)))
        """,
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check2",
        "c",
        "CHECK (((payload -> 'schema_version'::text) = '1'::jsonb))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check3",
        "c",
        "CHECK (((payload ->> 'alert_type'::text) = 'research_alert_v1'::text))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check4",
        "c",
        "CHECK (((payload ? 'decision'::text) "
        "AND ((payload ->> 'decision'::text) = 'ALERT'::text)))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check5",
        "c",
        "CHECK (((payload ->> 'environment'::text) = 'live_t4'::text))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check6",
        "c",
        "CHECK (((payload -> 'external_delivery'::text) = 'false'::jsonb))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check7",
        "c",
        "CHECK (((payload ? 'read_only'::text) "
        "AND (jsonb_typeof((payload -> 'read_only'::text)) = 'boolean'::text) "
        "AND ((payload -> 'read_only'::text) = 'true'::jsonb)))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check8",
        "c",
        "CHECK (((payload -> 'execution_enabled'::text) = 'false'::jsonb))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check9",
        "c",
        "CHECK (((payload -> 'not_financial_advice'::text) = 'true'::jsonb))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_payload_check10",
        "c",
        "CHECK ((octet_length((payload)::text) <= 16384))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_check",
        "c",
        "CHECK (((payload ->> 'monitoring_policy_id'::text) = monitoring_policy_id))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_check1",
        "c",
        "CHECK (((payload ->> 'monitoring_policy_hash_sha256'::text) "
        "= (monitoring_policy_hash)::text))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_check2",
        "c",
        "CHECK (((payload -> 'retention_days'::text) "
        "= to_jsonb((retention_days)::integer)))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_check3",
        "c",
        "CHECK ((available_at < expires_at))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_pkey",
        "p",
        "PRIMARY KEY (alert_delivery_attempt_id)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_outbox_fk",
        "f",
        "FOREIGN KEY (alert_delivery_outbox_id) REFERENCES "
        "crypto_agent.alert_delivery_outbox(alert_delivery_outbox_id)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_alert_delivery_outbox_id_attempt_no_key",
        "u",
        "UNIQUE (alert_delivery_outbox_id, attempt_no)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_content_hash_key",
        "u",
        "UNIQUE (content_hash)",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_attempt_no_check",
        "c",
        "CHECK ((attempt_no > 0))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_outcome_check",
        "c",
        "CHECK ((outcome = ANY (ARRAY['delivered'::text, "
        "'retryable_failure'::text, 'permanent_failure'::text, "
        "'expired'::text])))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_check",
        "c",
        "CHECK ((started_at <= finished_at))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_error_code_check",
        "c",
        "CHECK (((error_code IS NULL) OR "
        "(error_code ~ '^[A-Z][A-Z0-9_]{0,63}$'::text)))",
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_check1",
        "c",
        """
        CHECK ((((outcome = 'delivered'::text) AND (next_attempt_at IS NULL)
        AND (error_code IS NULL)) OR ((outcome = 'retryable_failure'::text)
        AND (next_attempt_at IS NOT NULL) AND (next_attempt_at > finished_at)
        AND (error_code IS NOT NULL)) OR ((outcome = 'permanent_failure'::text)
        AND (next_attempt_at IS NULL) AND (error_code IS NOT NULL))
        OR ((outcome = 'expired'::text) AND (next_attempt_at IS NULL))))
        """,
    ),
    _CatalogDefinitionRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_check2",
        "c",
        "CHECK ((((attempt_no = 1) AND (previous_attempt_hash IS NULL)) "
        "OR ((attempt_no > 1) AND (previous_attempt_hash IS NOT NULL))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_campaigns",
        "t4_observation_campaigns_pkey",
        "p",
        "PRIMARY KEY (campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_campaigns",
        "t4_observation_campaigns_frozen_baseline_hash_key",
        "u",
        "UNIQUE (frozen_baseline_hash)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_campaigns",
        "t4_observation_campaigns_content_hash_key",
        "u",
        "UNIQUE (content_hash)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_pkey",
        "p",
        "PRIMARY KEY (observation_research_input_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_campaign_id_fkey",
        "f",
        "FOREIGN KEY (campaign_id) REFERENCES "
        "crypto_agent.t4_observation_campaigns(campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_t4_batch_id_fkey",
        "f",
        "FOREIGN KEY (t4_batch_id) REFERENCES "
        "crypto_agent.t4_ingestion_batches(t4_batch_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_research_run_id_fkey",
        "f",
        "FOREIGN KEY (research_run_id) REFERENCES "
        "crypto_agent.research_runs(research_run_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_input_campaign_id_scope_key_sequenc_key",
        "u",
        "UNIQUE (campaign_id, scope_key, sequence_no)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_input_campaign_id_scope_key_sequen_key1",
        "u",
        "UNIQUE (campaign_id, scope_key, sequence_no, expected_at, t4_batch_id, "
        "research_run_id, raw_payload_hash, analysis_input_hash, trace_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_t4_batch_id_key",
        "u",
        "UNIQUE (t4_batch_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_research_run_id_key",
        "u",
        "UNIQUE (research_run_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_trace_id_key",
        "u",
        "UNIQUE (trace_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_content_hash_key",
        "u",
        "UNIQUE (content_hash)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_sequence_no_check",
        "c",
        "CHECK ((sequence_no > 0))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_scope_key_check",
        "c",
        "CHECK (((btrim(scope_key) <> ''::text) AND (length(scope_key) <= 256)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_cycles",
        "t4_observation_cycles_pkey",
        "p",
        "PRIMARY KEY (observation_cycle_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_cycles",
        "t4_observation_cycles_campaign_id_fkey",
        "f",
        "FOREIGN KEY (campaign_id) REFERENCES "
        "crypto_agent.t4_observation_campaigns(campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_cycles",
        "t4_observation_cycles_t4_batch_id_fkey",
        "f",
        "FOREIGN KEY (t4_batch_id) REFERENCES "
        "crypto_agent.t4_ingestion_batches(t4_batch_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_cycles",
        "t4_observation_cycles_research_run_id_fkey",
        "f",
        "FOREIGN KEY (research_run_id) REFERENCES "
        "crypto_agent.research_runs(research_run_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_cycles",
        "t4_observation_cycles_campaign_id_scope_key_sequence_no_ex_fkey",
        "f",
        "FOREIGN KEY (campaign_id, scope_key, sequence_no, expected_at, "
        "t4_batch_id, research_run_id, raw_payload_hash, analysis_input_hash, "
        "trace_id) REFERENCES crypto_agent.t4_observation_research_inputs"
        "(campaign_id, scope_key, sequence_no, expected_at, t4_batch_id, "
        "research_run_id, raw_payload_hash, analysis_input_hash, trace_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_cycles",
        "t4_observation_cycles_campaign_id_scope_key_sequence_no_key",
        "u",
        "UNIQUE (campaign_id, scope_key, sequence_no)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_cycles",
        "t4_observation_cycles_content_hash_key",
        "u",
        "UNIQUE (content_hash)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_session_events",
        "t4_observation_session_events_pkey",
        "p",
        "PRIMARY KEY (observation_session_event_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_session_events",
        "t4_observation_session_events_campaign_id_fkey",
        "f",
        "FOREIGN KEY (campaign_id) REFERENCES "
        "crypto_agent.t4_observation_campaigns(campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_session_events",
        "t4_observation_session_events_campaign_id_sequence_no_key",
        "u",
        "UNIQUE (campaign_id, sequence_no)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_session_events",
        "t4_observation_session_events_content_hash_key",
        "u",
        "UNIQUE (content_hash)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_quality_reports",
        "t4_observation_quality_reports_pkey",
        "p",
        "PRIMARY KEY (observation_quality_report_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_quality_reports",
        "t4_observation_quality_reports_campaign_id_key",
        "u",
        "UNIQUE (campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_quality_reports",
        "t4_observation_quality_reports_campaign_id_fkey",
        "f",
        "FOREIGN KEY (campaign_id) REFERENCES "
        "crypto_agent.t4_observation_campaigns(campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_quality_reports",
        "t4_observation_quality_reports_report_hash_key",
        "u",
        "UNIQUE (report_hash)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_quality_reports",
        "t4_observation_quality_reports_content_hash_key",
        "u",
        "UNIQUE (content_hash)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_quality_reports",
        "t4_observation_quality_reports_checkpoint_event_fk",
        "f",
        "FOREIGN KEY (bridge_checkpoint_event_id) REFERENCES "
        "crypto_agent.t4_bridge_observation_events(event_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_quality_reports",
        "t4_observation_quality_reports_checkpoint_event_key",
        "u",
        "UNIQUE (bridge_checkpoint_event_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_quality_reports",
        "t4_observation_quality_reports_checkpoint_shape_check",
        "c",
        "CHECK ((((bridge_checkpoint_event_id IS NULL) AND "
        "(bridge_checkpoint_sequence_no IS NULL) AND "
        "(bridge_checkpoint_event_hash IS NULL) AND "
        "(bridge_checkpoint_action_request_id IS NULL)) OR "
        "((bridge_checkpoint_event_id IS NOT NULL) AND "
        "(bridge_checkpoint_sequence_no > 0) AND "
        "(bridge_checkpoint_event_hash IS NOT NULL) AND "
        "(bridge_checkpoint_action_request_id IS NOT NULL))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_evidence_keys",
        "t4_bridge_evidence_keys_pkey",
        "p",
        "PRIMARY KEY (evidence_key_fingerprint)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_evidence_keys",
        "t4_bridge_evidence_keys_signature_algorithm_check",
        "c",
        "CHECK ((signature_algorithm = 'ecdsa-p256-sha256-der'::text))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_evidence_keys",
        "t4_bridge_evidence_keys_retirement_check",
        "c",
        "CHECK (((retired_at IS NULL) OR (retired_at > registered_at)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_evidence_keys",
        "t4_bridge_evidence_keys_spki_size_check",
        "c",
        "CHECK (((octet_length(decode(public_key_spki_base64, 'base64'::text)) "
        ">= 80) AND (octet_length(decode(public_key_spki_base64, "
        "'base64'::text)) <= 256)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_evidence_keys",
        "t4_bridge_evidence_keys_fingerprint_check",
        "c",
        "CHECK (((evidence_key_fingerprint)::text = encode(public.digest("
        "decode(public_key_spki_base64, 'base64'::text), 'sha256'::text), "
        "'hex'::text)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_campaigns",
        "t4_observation_campaigns_bridge_evidence_key_fk",
        "f",
        "FOREIGN KEY (bridge_evidence_key_fingerprint) REFERENCES "
        "crypto_agent.t4_bridge_evidence_keys(evidence_key_fingerprint)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_pkey",
        "p",
        "PRIMARY KEY (bridge_observation_event_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_event_id_key",
        "u",
        "UNIQUE (event_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_event_hash_sha256_key",
        "u",
        "UNIQUE (event_hash_sha256)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_evidence_key_fingerprint_sha25_key",
        "u",
        "UNIQUE (evidence_key_fingerprint_sha256, sequence_no)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_evidence_key_fingerprint_sha2_fkey",
        "f",
        "FOREIGN KEY (evidence_key_fingerprint_sha256) REFERENCES "
        "crypto_agent.t4_bridge_evidence_keys(evidence_key_fingerprint)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_sequence_positive_check",
        "c",
        "CHECK ((sequence_no > 0))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_event_type_check",
        "c",
        "CHECK ((event_type = ANY (ARRAY['bridge_started'::text, "
        "'session_connecting'::text, 'session_authenticated'::text, "
        "'session_ready'::text, 'session_disconnected'::text, "
        "'cache_cleared'::text, 'missing_data_detected'::text, "
        "'stale_data_detected'::text, 'rate_limited'::text, "
        "'contract_roll_observed'::text, 'control_requested'::text, "
        "'control_applied'::text, 'bridge_stopping'::text, "
        "'outbound_rejected'::text, 'campaign_checkpoint'::text])))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_live_environment_check",
        "c",
        "CHECK ((environment = 'live_t4'::text))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_schema_v5_check",
        "c",
        "CHECK ((bridge_schema_version = 5))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_read_only_check",
        "c",
        "CHECK (read_only)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_no_order_routes_check",
        "c",
        "CHECK ((NOT order_routes_exposed))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_signature_verified_check",
        "c",
        "CHECK (signature_verified)",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_signature_algorithm_check",
        "c",
        "CHECK ((signature_algorithm = 'ecdsa-p256-sha256-der'::text))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_chain_shape_check",
        "c",
        "CHECK ((((sequence_no = 1) AND (previous_event_hash IS NULL)) OR "
        "((sequence_no > 1) AND (previous_event_hash IS NOT NULL))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_roll_projection_check",
        "c",
        "CHECK ((((event_type = 'contract_roll_observed'::text) AND "
        "(roll_from_market_id IS NOT NULL) AND (roll_to_market_id IS NOT NULL) "
        "AND (roll_from_market_id <> roll_to_market_id)) OR "
        "((event_type <> 'contract_roll_observed'::text) AND "
        "(roll_from_market_id IS NULL) AND (roll_to_market_id IS NULL))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_hash_check",
        "c",
        "CHECK (((event_hash_sha256)::text = encode(public.digest("
        "decode(canonical_payload_base64, 'base64'::text), 'sha256'::text), "
        "'hex'::text)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trial_requests",
        "t4_observation_scenario_trial_requests_pkey",
        "p",
        "PRIMARY KEY (trial_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trial_requests",
        "t4_observation_scenario_trial_requests_campaign_id_fkey",
        "f",
        "FOREIGN KEY (campaign_id) REFERENCES "
        "crypto_agent.t4_observation_campaigns(campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trial_requests",
        "t4_observation_scenario_trial_campaign_id_scenario_code_act_key",
        "u",
        "UNIQUE (campaign_id, scenario_code, action_request_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trial_requests",
        "t4_observation_scenario_trial_requests_campaign_action_key",
        "u",
        "UNIQUE (campaign_id, action_request_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_pk",
        "p",
        "PRIMARY KEY (replay_attestation_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_attestation_key",
        "u",
        "UNIQUE (attestation_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_campaign_scope_key",
        "u",
        "UNIQUE (campaign_id, scope_key)",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_content_hash_key",
        "u",
        "UNIQUE (content_hash)",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_campaign_fk",
        "f",
        "FOREIGN KEY (campaign_id) REFERENCES "
        "crypto_agent.t4_observation_campaigns(campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_scope_key_check",
        "c",
        "CHECK ((scope_key ~ '^(BTC|ETH)/USD:(240|1440|10080)m$'::text))",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_limit_check",
        "c",
        "CHECK ((replay_limit = 120))",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_source_cardinality_check",
        "c",
        "CHECK (((cardinality(source_batch_ids) = 1) AND "
        "(cardinality(source_batch_hashes) = 1) AND "
        "(source_batch_ids[1] > 0)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_safety_check",
        "c",
        "CHECK ((read_only AND (NOT execution_enabled) AND "
        "(NOT external_delivery_eligible)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_replay_verifier_attestations",
        "t4_replay_verifier_attestations_time_check",
        "c",
        "CHECK ((replay_as_of <= attested_at))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_pkey",
        "p",
        "PRIMARY KEY (scenario_trial_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_trial_id_key",
        "u",
        "UNIQUE (trial_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_campaign_id_fkey",
        "f",
        "FOREIGN KEY (campaign_id) REFERENCES "
        "crypto_agent.t4_observation_campaigns(campaign_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_outcome_check",
        "c",
        "CHECK ((outcome = ANY (ARRAY['pass'::text, 'fail'::text])))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_first_event_id_fkey",
        "f",
        "FOREIGN KEY (first_event_id) REFERENCES "
        "crypto_agent.t4_bridge_observation_events(event_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_last_event_id_fkey",
        "f",
        "FOREIGN KEY (last_event_id) REFERENCES "
        "crypto_agent.t4_bridge_observation_events(event_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_pre_t4_batch_id_fkey",
        "f",
        "FOREIGN KEY (pre_t4_batch_id) REFERENCES "
        "crypto_agent.t4_ingestion_batches(t4_batch_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_post_t4_batch_id_fkey",
        "f",
        "FOREIGN KEY (post_t4_batch_id) REFERENCES "
        "crypto_agent.t4_ingestion_batches(t4_batch_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_observation_cycle_id_fkey",
        "f",
        "FOREIGN KEY (observation_cycle_id) REFERENCES "
        "crypto_agent.t4_observation_cycles(observation_cycle_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_research_run_id_fkey",
        "f",
        "FOREIGN KEY (research_run_id) REFERENCES "
        "crypto_agent.research_runs(research_run_id)",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_time_check",
        "c",
        "CHECK (((started_at <= completed_at) AND (completed_at <= created_at)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_event_cardinality_check",
        "c",
        "CHECK (((cardinality(evidence_event_ids) = "
        "cardinality(evidence_event_hashes)) AND "
        "(cardinality(evidence_event_ids) <= 64)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_replay_attestation_shape_check",
        "c",
        "CHECK ((((scenario_code = 'replay_blocked'::text) AND "
        "(research_run_id IS NULL) AND "
        "(replay_fingerprint_sha256 IS NULL) AND "
        "(cardinality(replay_attestation_ids) > 0)) OR "
        "((scenario_code <> 'replay_blocked'::text) AND "
        "(cardinality(replay_attestation_ids) = 0))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_shape_check",
        "c",
        "CHECK (((outcome = 'fail'::text) OR ((scenario_code = "
        "'replay_blocked'::text) AND (first_event_id IS NULL) AND "
        "(last_event_id IS NULL) AND (cardinality(evidence_event_ids) = 0)) OR "
        "((scenario_code <> 'replay_blocked'::text) AND "
        "(first_event_id IS NOT NULL) AND (last_event_id IS NOT NULL) AND "
        "(cardinality(evidence_event_ids) > 0) AND "
        "(evidence_event_ids[1] = first_event_id) AND "
        "(evidence_event_ids[cardinality(evidence_event_ids)] = "
        "last_event_id))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_attestation_time_check", "c",
        "CHECK (((event_at <= signature_verified_at) AND "
        "(signature_verified_at <= received_at)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_canonical_size_check", "c",
        "CHECK (((octet_length(decode(canonical_payload_base64, "
        "'base64'::text)) >= 32) AND (octet_length(decode("
        "canonical_payload_base64, 'base64'::text)) <= 65536)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_control_action_check", "c",
        "CHECK (((control_action IS NULL) OR (control_action = ANY "
        "(ARRAY['restart'::text, 'reconnect'::text, 'missing_data'::text, "
        "'stale_data'::text, 'rate_limit'::text, 'checkpoint'::text]))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_control_event_shape_check", "c",
        "CHECK ((((event_type = ANY (ARRAY['control_requested'::text, "
        "'control_applied'::text])) AND (action_request_id IS NOT NULL) AND "
        "(control_step IS NOT NULL)) OR (event_type <> ALL "
        "(ARRAY['control_requested'::text, 'control_applied'::text]))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_control_projection_check", "c",
        "CHECK (((control_action IS NULL) = (control_step IS NULL)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_control_step_check", "c",
        "CHECK (((control_step IS NULL) OR (control_step = ANY "
        "(ARRAY['requested'::text, 'applied'::text, 'consumed'::text]))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_payload_check", "c",
        "CHECK (((jsonb_typeof(payload) = 'object'::text) AND "
        "(octet_length((payload)::text) <= 16384)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_reason_code_check", "c",
        "CHECK ((reason_code ~ '^[A-Z][A-Z0-9_]{0,63}$'::text))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_received_at_check", "c",
        "CHECK ((received_at <= (clock_timestamp() + '00:05:00'::interval)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_reconnect_count_check", "c",
        "CHECK ((reconnect_count >= 0))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_scenario_code_check", "c",
        "CHECK (((scenario_code IS NULL) OR (scenario_code = ANY "
        "(ARRAY['bridge_restart'::text, 'missing_data'::text, "
        "'rate_limit'::text, 'reconnect'::text, 'replay_blocked'::text, "
        "'roll_transition'::text, 'stale_data'::text]))))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_scope_key_check", "c",
        "CHECK (((scope_key IS NULL) OR (scope_key ~ "
        "'^(BTC|ETH)/USD:(240|1440|10080)m$'::text)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_session_generation_check", "c",
        "CHECK ((session_generation >= 0))",
    ),
    _CatalogDefinitionRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_signature_size_check", "c",
        "CHECK (((octet_length(decode(signature_base64, 'base64'::text)) "
        ">= 8) AND (octet_length(decode(signature_base64, "
        "'base64'::text)) <= 256)))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trial_requests",
        "t4_observation_scenario_trial_requests_scenario_code_check", "c",
        "CHECK ((scenario_code = ANY (ARRAY['bridge_restart'::text, "
        "'missing_data'::text, 'rate_limit'::text, 'reconnect'::text, "
        "'stale_data'::text])))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trial_requests",
        "t4_observation_scenario_trial_requests_scope_key_check", "c",
        "CHECK ((scope_key ~ '^(BTC|ETH)/USD:(240|1440|10080)m$'::text))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_detail_code_check", "c",
        "CHECK ((detail_code ~ '^[A-Z][A-Z0-9_]{0,63}$'::text))",
    ),
    _CatalogDefinitionRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_scenario_code_check", "c",
        "CHECK ((scenario_code = ANY (ARRAY['bridge_restart'::text, "
        "'missing_data'::text, 'rate_limit'::text, 'reconnect'::text, "
        "'replay_blocked'::text, 'roll_transition'::text, "
        "'stale_data'::text])))",
    ),
)
_OPERATIONAL_INDEX_REQUIREMENTS = (
    _IndexRequirement(
        "t4_ingestion_batches",
        "t4_ingestion_batches_t4_identity_idx",
        False,
        "CREATE INDEX t4_ingestion_batches_t4_identity_idx ON "
        "crypto_agent.t4_ingestion_batches USING btree "
        "(environment, exchange_id, contract_id, active_market_id, available_at DESC) "
        "WHERE (bridge_schema_version = 5)",
    ),
    _IndexRequirement(
        "t4_canonical_candles",
        "t4_canonical_candles_market_id_idx",
        False,
        "CREATE INDEX t4_canonical_candles_market_id_idx ON "
        "crypto_agent.t4_canonical_candles USING btree (market_id, open_time DESC) "
        "WHERE (market_id IS NOT NULL)",
    ),
    _IndexRequirement(
        "alert_delivery_outbox",
        "alert_delivery_outbox_due_idx",
        False,
        "CREATE INDEX alert_delivery_outbox_due_idx ON "
        "crypto_agent.alert_delivery_outbox USING btree "
        "(available_at, expires_at, alert_delivery_outbox_id)",
    ),
    _IndexRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_outbox_idx",
        False,
        "CREATE INDEX alert_delivery_attempts_outbox_idx ON "
        "crypto_agent.alert_delivery_attempts USING btree "
        "(alert_delivery_outbox_id, attempt_no DESC)",
    ),
    _IndexRequirement(
        "alert_delivery_attempts",
        "alert_delivery_attempts_terminal_idx",
        False,
        "CREATE INDEX alert_delivery_attempts_terminal_idx ON "
        "crypto_agent.alert_delivery_attempts USING btree "
        "(outcome, finished_at DESC) WHERE (outcome = ANY "
        "(ARRAY['delivered'::text, 'permanent_failure'::text, 'expired'::text]))",
    ),
    _IndexRequirement(
        "t4_observation_cycles",
        "t4_observation_cycles_campaign_time_idx",
        False,
        "CREATE INDEX t4_observation_cycles_campaign_time_idx ON "
        "crypto_agent.t4_observation_cycles USING btree "
        "(campaign_id, expected_at, scope_key)",
    ),
    _IndexRequirement(
        "t4_observation_research_inputs",
        "t4_observation_research_inputs_campaign_time_idx",
        False,
        "CREATE INDEX t4_observation_research_inputs_campaign_time_idx ON "
        "crypto_agent.t4_observation_research_inputs USING btree "
        "(campaign_id, expected_at, scope_key)",
    ),
    _IndexRequirement(
        "t4_observation_session_events",
        "t4_observation_session_events_campaign_time_idx",
        False,
        "CREATE INDEX t4_observation_session_events_campaign_time_idx ON "
        "crypto_agent.t4_observation_session_events USING btree "
        "(campaign_id, event_at)",
    ),
    _IndexRequirement(
        "t4_observation_session_events",
        "t4_observation_session_events_scenario_idx",
        False,
        "CREATE INDEX t4_observation_session_events_scenario_idx ON "
        "crypto_agent.t4_observation_session_events USING btree "
        "(campaign_id, scenario_code, outcome) WHERE (scenario_code IS NOT NULL)",
    ),
    _IndexRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_campaign_time_idx",
        False,
        "CREATE INDEX t4_bridge_observation_events_campaign_time_idx ON "
        "crypto_agent.t4_bridge_observation_events USING btree "
        "(campaign_id, event_at, sequence_no)",
    ),
    _IndexRequirement(
        "t4_bridge_observation_events",
        "t4_bridge_observation_events_scenario_idx",
        False,
        "CREATE INDEX t4_bridge_observation_events_scenario_idx ON "
        "crypto_agent.t4_bridge_observation_events USING btree "
        "(campaign_id, scenario_code, action_request_id, sequence_no) "
        "WHERE (scenario_code IS NOT NULL)",
    ),
    _IndexRequirement(
        "t4_observation_scenario_trials",
        "t4_observation_scenario_trials_campaign_idx",
        False,
        "CREATE INDEX t4_observation_scenario_trials_campaign_idx ON "
        "crypto_agent.t4_observation_scenario_trials USING btree "
        "(campaign_id, scenario_code, completed_at)",
    ),
)
_OPERATIONAL_FUNCTION_REQUIREMENTS = (
    _FunctionDefinitionRequirement(
        "enforce_alert_delivery_attempt",
        "plpgsql",
        "v",
        False,
        "fa988543f76637d631a84a1548437802bb728c672364bb088aac37d8a65c1f29",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_schema_v5_evidence",
        "plpgsql",
        "v",
        False,
        "00d9d278d82c2284c003ccd8520d68427d27026ac726617206fbb239cd310027",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_observation_campaign_start",
        "plpgsql",
        "v",
        True,
        "d3c94b00a3e74822877f8ca8f149c8affe0180b5f0d1f41d15349f56254963c2",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_observation_final_report",
        "plpgsql",
        "v",
        True,
        "f08ab7f5df629aaf0f927e2ca5e43c6213e4504808c1e6d543469e3fa88859c3",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_observation_research_input",
        "plpgsql",
        "v",
        False,
        "e28de614028e10db3af05a06f8ce926a845e60677eef9f6d30e293f3a00fb337",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_observation_cycle_chain",
        "plpgsql",
        "v",
        False,
        "9d67015e96ecb2269b9d0043cd3ae687ad0fd1dc10935f87e29dd98afc308e77",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_observation_event_chain",
        "plpgsql",
        "v",
        False,
        "3a17765b11a411760b7a6ddc0524c9a60dcf6f362bc893f607de558e0b75daec",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_bridge_observation_event",
        "plpgsql",
        "v",
        False,
        "ee19cb134c47d62747bc7cabb842d9ced81c9ac72ab61e11631e083f6670fb2a",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_observation_scenario_trial",
        "plpgsql",
        "v",
        False,
        "db3fa6193cdd198bf47263db352256ab7e3694adafd8f42ce413fa2d5a2e2909",
    ),
    _FunctionDefinitionRequirement(
        "enforce_t4_scenario_alert_finalization",
        "plpgsql",
        "v",
        True,
        "360ade53de8e58c4439ac0d4cc91a99b067ca602acefbb8c7cee3d283c52d6a9",
    ),
    _FunctionDefinitionRequirement(
        "serialize_t4_observation_campaign_write",
        "plpgsql",
        "v",
        False,
        "5b8dd4b568aa1394df083b03d5711e1e3810e0e4d366101a7d6da440da13898d",
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
            # Catalog deparsers such as pg_get_constraintdef() use search_path
            # when deciding whether to qualify object names.  A CI role named
            # ``crypto_agent`` implicitly exposes the same-named schema through
            # ``\"$user\"`` and would otherwise make equivalent constraints look
            # different.  Keep health attestation deterministic across roles.
            db_cursor.execute("SET LOCAL search_path TO pg_catalog")
            db_cursor.execute(
                """
                SELECT current_setting('server_version_num'),
                       current_setting('server_version'),
                       current_setting('session_replication_role'),
                       current_user,
                       coalesce((SELECT role.rolsuper
                           FROM pg_catalog.pg_roles role
                           WHERE role.rolname = current_user), TRUE),
                       pg_catalog.pg_has_role(
                           current_user, 'crypto_agent_evidence_owner', 'MEMBER'
                       ),
                       pg_catalog.pg_has_role(
                           current_user, 'crypto_agent_evidence_verifier', 'MEMBER'
                       ),
                       pg_catalog.pg_has_role(
                           current_user, 'crypto_agent_evidence_reader', 'MEMBER'
                       ),
                       coalesce((SELECT database.datdba = role.oid
                           FROM pg_catalog.pg_database database
                           JOIN pg_catalog.pg_roles role
                             ON role.rolname = current_user
                           WHERE database.datname = current_database()), TRUE),
                       coalesce((SELECT namespace.nspowner = role.oid
                           FROM pg_catalog.pg_namespace namespace
                           JOIN pg_catalog.pg_roles role
                             ON role.rolname = current_user
                           WHERE namespace.nspname = 'crypto_agent'), TRUE),
                       EXISTS (
                           SELECT 1
                           FROM pg_catalog.pg_class relation
                           JOIN pg_catalog.pg_namespace namespace
                             ON namespace.oid = relation.relnamespace
                           JOIN pg_catalog.pg_roles role
                             ON role.oid = relation.relowner
                           WHERE namespace.nspname = 'crypto_agent'
                             AND role.rolname = current_user
                       ),
                       EXISTS (
                           SELECT 1
                           FROM pg_catalog.pg_proc routine
                           JOIN pg_catalog.pg_namespace namespace
                             ON namespace.oid = routine.pronamespace
                           JOIN pg_catalog.pg_roles role
                             ON role.oid = routine.proowner
                           WHERE namespace.nspname = 'crypto_agent'
                             AND role.rolname = current_user
                       ),
                       coalesce((SELECT
                           role.rolcanlogin AND role.rolinherit
                           AND NOT role.rolsuper AND NOT role.rolcreaterole
                           AND NOT role.rolcreatedb AND NOT role.rolreplication
                           AND NOT role.rolbypassrls
                           FROM pg_catalog.pg_roles role
                           WHERE role.rolname = current_user), FALSE)
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
            health_user = str(_row_value(version_row, "current_user", 3))
            health_user_super = _row_value(version_row, "rolsuper", 4) is True
            health_user_owner = _row_value(version_row, "pg_has_role", 5) is True
            health_user_verifier = _row_value(version_row, "pg_has_role", 6) is True
            health_user_reader = _row_value(version_row, "pg_has_role", 7) is True
            health_user_database_owner = _row_value(version_row, "coalesce", 8) is True
            health_user_schema_owner = _row_value(version_row, "coalesce", 9) is True
            health_user_table_owner = _row_value(version_row, "exists", 10) is True
            health_user_function_owner = _row_value(version_row, "exists", 11) is True
            health_user_role_safe = _row_value(version_row, "coalesce", 12) is True
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
            if (
                replication_role != "origin"
                or health_user_super
                or health_user_owner
                or health_user_verifier
                or not health_user_reader
                or health_user_database_owner
                or health_user_schema_owner
                or health_user_table_owner
                or health_user_function_owner
                or not health_user_role_safe
                or health_user in {
                    "crypto_agent_evidence_owner",
                    "crypto_agent_evidence_verifier",
                    "crypto_agent_evidence_reader",
                }
            ):
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
            migrated_missing.extend(
                _routine_differences(
                    _MIGRATED_ROUTINE_REQUIREMENTS,
                    _catalog_routines(db_cursor, _MIGRATED_ROUTINE_REQUIREMENTS),
                )
            )
            migrated_missing.extend(_catalog_evidence_boundary_differences(db_cursor))
            migrated_missing.extend(
                _catalog_definition_differences(
                    "constraint",
                    _OPERATIONAL_CONSTRAINT_REQUIREMENTS,
                    _catalog_constraints(
                        db_cursor,
                        _OPERATIONAL_CONSTRAINT_REQUIREMENTS,
                    ),
                )
            )
            migrated_missing.extend(
                _index_differences(
                    _OPERATIONAL_INDEX_REQUIREMENTS,
                    _catalog_indexes(db_cursor, _OPERATIONAL_INDEX_REQUIREMENTS),
                )
            )
            migrated_missing.extend(
                _function_definition_differences(
                    _OPERATIONAL_FUNCTION_REQUIREMENTS,
                    _catalog_function_definitions(
                        db_cursor,
                        _OPERATIONAL_FUNCTION_REQUIREMENTS,
                    ),
                )
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


def _catalog_routines(
    db_cursor: DBCursor,
    required: Sequence[_RoutineRequirement],
) -> dict[tuple[str, int], tuple[str, str, str, bool, str, bool, str]]:
    db_cursor.execute(
        """
        SELECT proc.proname, proc.pronargs,
               pg_catalog.pg_get_function_identity_arguments(proc.oid),
               language.lanname, proc.provolatile, proc.prosecdef, owner.rolname,
               coalesce(
                   'search_path=pg_catalog, crypto_agent' = ANY(proc.proconfig),
                   FALSE
               ) AS fixed_search_path, proc.prosrc
        FROM pg_catalog.pg_proc AS proc
        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = proc.pronamespace
        JOIN pg_catalog.pg_roles AS owner ON owner.oid = proc.proowner
        JOIN pg_catalog.pg_language AS language ON language.oid = proc.prolang
        WHERE ns.nspname = 'crypto_agent'
          AND proc.proname = ANY(%s)
          AND proc.prorettype <> 'pg_catalog.trigger'::pg_catalog.regtype
        """,
        ([item.name for item in required],),
    )
    return {
        (
            str(_row_value(row, "proname", 0)),
            _catalog_int(_row_value(row, "pronargs", 1)),
        ): (
            str(_row_value(row, "pg_get_function_identity_arguments", 2)),
            str(_row_value(row, "lanname", 3)),
            str(_row_value(row, "provolatile", 4)),
            _row_value(row, "prosecdef", 5) is True,
            str(_row_value(row, "rolname", 6)),
            _row_value(row, "fixed_search_path", 7) is True,
            _definition_sha256(_row_value(row, "prosrc", 8)),
        )
        for row in db_cursor.fetchall()
    }


def _routine_differences(
    required: Sequence[_RoutineRequirement],
    actual: Mapping[
        tuple[str, int], tuple[str, str, str, bool, str, bool, str]
    ],
) -> tuple[str, ...]:
    required_by_signature = {
        (item.name, item.argument_count): item for item in required
    }
    differences = {
        f"routine:{item.name}"
        for item in required
        if actual.get((item.name, item.argument_count))
        != (
            item.identity_arguments,
            item.language,
            item.volatility,
            item.security_definer,
            item.owner,
            item.fixed_search_path,
            item.source_sha256,
        )
    }
    differences.update(
        f"unexpected:routine:{name}/{argument_count}"
        for name, argument_count in actual.keys() - required_by_signature.keys()
    )
    return tuple(sorted(differences))


def _catalog_evidence_boundary_differences(
    db_cursor: DBCursor,
) -> tuple[str, ...]:
    db_cursor.execute(
        """
        WITH protected_tables(table_name) AS (VALUES
            ('t4_bridge_evidence_keys'),
            ('t4_bridge_observation_events'),
            ('t4_replay_verifier_attestations'),
            ('t4_observation_scenario_trial_requests'),
            ('t4_observation_scenario_trials')
        ), verifier_functions(function_name, argument_count) AS (VALUES
            ('register_t4_bridge_evidence_key', 1),
            ('record_verified_t4_bridge_observation_event', 17),
            ('record_verified_t4_replay_attestation', 8),
            ('begin_t4_observation_scenario_trial', 4),
            ('complete_t4_observation_scenario_trial', 3),
            ('verify_passive_t4_observation_scenario', 2)
        ), owner_only_functions(function_name, argument_count) AS (VALUES
            ('canonical_jsonb_text', 1),
            ('t4_evidence_verifier_session_is_safe', 0),
            ('t4_replay_attestations_are_verified', 2)
        ), owner_inputs(table_name) AS (VALUES
            ('t4_observation_campaigns'),
            ('t4_observation_quality_reports'),
            ('t4_ingestion_batches'),
            ('t4_observation_cycles'),
            ('t4_observation_session_events'),
            ('research_runs'), ('research_run_events'), ('research_run_inputs'),
            ('research_artifacts'),
            ('alerts'), ('alert_delivery_outbox'), ('alert_delivery_attempts'),
            ('t4_contract_transition_evidence'), ('t4_canonical_candles')
        ), reader_inputs(table_name) AS (VALUES
            ('t4_observation_campaigns'),
            ('t4_observation_quality_reports'),
            ('t4_ingestion_batches'), ('t4_canonical_candles'),
            ('t4_futures_snapshots'), ('t4_orderbook_levels'),
            ('t4_contract_transition_evidence')
        ), critical_definer_triggers(function_name) AS (VALUES
            ('enforce_t4_observation_campaign_start'),
            ('enforce_t4_observation_final_report'),
            ('enforce_t4_scenario_alert_finalization')
        )
        SELECT
            coalesce((
                SELECT ns.nspname = 'public'
                FROM pg_catalog.pg_extension extension
                JOIN pg_catalog.pg_namespace ns
                  ON ns.oid = extension.extnamespace
                WHERE extension.extname = 'pgcrypto'
            ), FALSE) AS pgcrypto_in_public,
            coalesce((SELECT
                NOT role.rolsuper AND NOT role.rolinherit
                AND NOT role.rolcreaterole AND NOT role.rolcreatedb
                AND NOT role.rolcanlogin AND NOT role.rolreplication
                AND NOT role.rolbypassrls
                FROM pg_catalog.pg_roles role
                WHERE role.rolname = 'crypto_agent_evidence_owner'
            ), FALSE) AS owner_role_safe,
            coalesce((SELECT
                NOT role.rolsuper AND NOT role.rolinherit
                AND NOT role.rolcreaterole AND NOT role.rolcreatedb
                AND NOT role.rolcanlogin AND NOT role.rolreplication
                AND NOT role.rolbypassrls
                FROM pg_catalog.pg_roles role
                WHERE role.rolname = 'crypto_agent_evidence_verifier'
            ), FALSE) AS verifier_role_safe,
            coalesce((SELECT
                NOT role.rolsuper AND NOT role.rolinherit
                AND NOT role.rolcreaterole AND NOT role.rolcreatedb
                AND NOT role.rolcanlogin AND NOT role.rolreplication
                AND NOT role.rolbypassrls
                FROM pg_catalog.pg_roles role
                WHERE role.rolname = 'crypto_agent_evidence_reader'
            ), FALSE) AS reader_role_safe,
            NOT pg_catalog.pg_has_role(
                'crypto_agent_evidence_verifier',
                'crypto_agent_evidence_owner', 'MEMBER'
            ) AND NOT pg_catalog.pg_has_role(
                'crypto_agent_evidence_owner',
                'crypto_agent_evidence_verifier', 'MEMBER'
            ) AND NOT pg_catalog.pg_has_role(
                'crypto_agent_evidence_reader',
                'crypto_agent_evidence_owner', 'MEMBER'
            ) AND NOT pg_catalog.pg_has_role(
                'crypto_agent_evidence_reader',
                'crypto_agent_evidence_verifier', 'MEMBER'
            ) AND NOT pg_catalog.pg_has_role(
                'crypto_agent_evidence_owner',
                'crypto_agent_evidence_reader', 'MEMBER'
            ) AND NOT pg_catalog.pg_has_role(
                'crypto_agent_evidence_verifier',
                'crypto_agent_evidence_reader', 'MEMBER'
            ) AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_auth_members owner_membership
                JOIN pg_catalog.pg_roles target
                  ON target.oid = owner_membership.roleid
                WHERE target.rolname = 'crypto_agent_evidence_owner'
            ) AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_auth_members membership
                JOIN pg_catalog.pg_roles evidence_role
                  ON evidence_role.oid = membership.member
                WHERE evidence_role.rolname IN (
                    'crypto_agent_evidence_owner',
                    'crypto_agent_evidence_verifier',
                    'crypto_agent_evidence_reader'
                )
            ) AND (
                SELECT count(*) = 1
                FROM pg_catalog.pg_auth_members membership
                JOIN pg_catalog.pg_roles target ON target.oid = membership.roleid
                JOIN pg_catalog.pg_roles member ON member.oid = membership.member
                WHERE target.rolname = 'crypto_agent_evidence_verifier'
                  AND member.rolcanlogin
            ) AND (
                SELECT count(*) = 2
                FROM pg_catalog.pg_auth_members membership
                JOIN pg_catalog.pg_roles target ON target.oid = membership.roleid
                JOIN pg_catalog.pg_roles member ON member.oid = membership.member
                WHERE target.rolname = 'crypto_agent_evidence_reader'
                  AND member.rolcanlogin
            ) AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_auth_members membership
                JOIN pg_catalog.pg_roles target ON target.oid = membership.roleid
                JOIN pg_catalog.pg_roles member ON member.oid = membership.member
                WHERE target.rolname IN (
                    'crypto_agent_evidence_verifier',
                    'crypto_agent_evidence_reader'
                )
                  AND (
                    NOT (
                        member.rolcanlogin AND member.rolinherit
                        AND NOT member.rolsuper
                        AND NOT member.rolcreaterole
                        AND NOT member.rolcreatedb
                        AND NOT member.rolreplication
                        AND NOT member.rolbypassrls
                    )
                    OR EXISTS (
                        SELECT 1 FROM pg_catalog.pg_database database
                        WHERE database.datname = current_database()
                          AND database.datdba = member.oid
                    )
                    OR EXISTS (
                        SELECT 1 FROM pg_catalog.pg_namespace namespace
                        WHERE namespace.nspname = 'crypto_agent'
                          AND namespace.nspowner = member.oid
                    )
                    OR EXISTS (
                        SELECT 1 FROM pg_catalog.pg_class relation
                        JOIN pg_catalog.pg_namespace namespace
                          ON namespace.oid = relation.relnamespace
                        WHERE namespace.nspname = 'crypto_agent'
                          AND relation.relowner = member.oid
                    )
                    OR EXISTS (
                        SELECT 1 FROM pg_catalog.pg_proc routine
                        JOIN pg_catalog.pg_namespace namespace
                          ON namespace.oid = routine.pronamespace
                        WHERE namespace.nspname = 'crypto_agent'
                          AND routine.proowner = member.oid
                    )
                  )
            ) AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles candidate
                WHERE NOT candidate.rolsuper
                  AND pg_catalog.pg_has_role(
                          candidate.rolname,
                          'crypto_agent_evidence_verifier', 'MEMBER'
                      )
                  AND pg_catalog.pg_has_role(
                          candidate.rolname,
                          'crypto_agent_evidence_reader', 'MEMBER'
                      )
            ) AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles candidate
                WHERE candidate.rolname NOT IN (
                          'crypto_agent_evidence_verifier',
                          'crypto_agent_evidence_reader'
                      )
                  AND NOT candidate.rolsuper
                  AND (
                      pg_catalog.pg_has_role(
                          candidate.rolname,
                          'crypto_agent_evidence_verifier', 'MEMBER'
                      ) OR pg_catalog.pg_has_role(
                          candidate.rolname,
                          'crypto_agent_evidence_reader', 'MEMBER'
                      )
                  )
                  AND NOT (
                      candidate.rolcanlogin AND candidate.rolinherit
                      AND NOT candidate.rolcreaterole
                      AND NOT candidate.rolcreatedb
                      AND NOT candidate.rolreplication
                      AND NOT candidate.rolbypassrls
                  )
            ) AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_roles candidate
                WHERE candidate.rolname NOT IN (
                    'crypto_agent_evidence_owner',
                    'crypto_agent_evidence_verifier',
                    'crypto_agent_evidence_reader'
                )
                  AND NOT candidate.rolsuper
                  AND (
                    pg_catalog.pg_has_role(
                        candidate.rolname,
                        'crypto_agent_evidence_verifier', 'MEMBER'
                    ) OR pg_catalog.pg_has_role(
                        candidate.rolname,
                        'crypto_agent_evidence_reader', 'MEMBER'
                    )
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_auth_members direct_membership
                    JOIN pg_catalog.pg_roles target
                      ON target.oid = direct_membership.roleid
                    WHERE direct_membership.member = candidate.oid
                      AND target.rolname IN (
                        'crypto_agent_evidence_verifier',
                        'crypto_agent_evidence_reader'
                      )
                  )
            ) AS roles_separated,
            (SELECT count(*) = 5 AND bool_and(
                        owner.rolname = 'crypto_agent_evidence_owner'
                    )
             FROM protected_tables expected
             JOIN pg_catalog.pg_class relation
               ON relation.relname = expected.table_name
             JOIN pg_catalog.pg_namespace ns
               ON ns.oid = relation.relnamespace
              AND ns.nspname = 'crypto_agent'
             JOIN pg_catalog.pg_roles owner ON owner.oid = relation.relowner
            ) AS protected_tables_owned,
            NOT EXISTS (
                SELECT 1
                FROM protected_tables expected
                JOIN pg_catalog.pg_class relation
                  ON relation.relname = expected.table_name
                JOIN pg_catalog.pg_namespace ns
                  ON ns.oid = relation.relnamespace
                 AND ns.nspname = 'crypto_agent'
                CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(
                    relation.relacl,
                    pg_catalog.acldefault('r', relation.relowner)
                )) acl
                LEFT JOIN pg_catalog.pg_roles grantee ON grantee.oid = acl.grantee
                WHERE NOT (
                    acl.grantee = (
                        SELECT role.oid FROM pg_catalog.pg_roles role
                        WHERE role.rolname = 'crypto_agent_evidence_owner'
                    )
                    OR (acl.grantee = (
                            SELECT role.oid FROM pg_catalog.pg_roles role
                            WHERE role.rolname = 'crypto_agent_evidence_reader'
                        )
                        AND acl.privilege_type = 'SELECT'
                        AND NOT acl.is_grantable)
                )
            ) AND (SELECT count(*) = 5
                    FROM protected_tables expected
                    JOIN pg_catalog.pg_class relation
                      ON relation.relname = expected.table_name
                    JOIN pg_catalog.pg_namespace ns
                      ON ns.oid = relation.relnamespace
                     AND ns.nspname = 'crypto_agent'
                    CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(
                        relation.relacl,
                        pg_catalog.acldefault('r', relation.relowner)
                    )) acl
                    WHERE acl.grantee = (
                            SELECT role.oid FROM pg_catalog.pg_roles role
                            WHERE role.rolname = 'crypto_agent_evidence_reader'
                        )
                      AND acl.privilege_type = 'SELECT'
                      AND NOT acl.is_grantable
                   ) AS protected_table_acl_exact,
            NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_namespace namespace
                CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(
                    namespace.nspacl,
                    pg_catalog.acldefault('n', namespace.nspowner)
                )) acl
                WHERE namespace.nspname = 'crypto_agent'
                  AND NOT (
                    acl.grantee = namespace.nspowner
                    OR (acl.grantee IN (
                            SELECT role.oid FROM pg_catalog.pg_roles role
                            WHERE role.rolname IN (
                                'crypto_agent_evidence_owner',
                                'crypto_agent_evidence_verifier',
                                'crypto_agent_evidence_reader'
                            )
                        )
                        AND acl.privilege_type = 'USAGE'
                        AND NOT acl.is_grantable)
                  )
            ) AND (SELECT count(*) = 3
                    FROM pg_catalog.pg_namespace namespace
                    CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(
                        namespace.nspacl,
                        pg_catalog.acldefault('n', namespace.nspowner)
                    )) acl
                    WHERE namespace.nspname = 'crypto_agent'
                      AND acl.grantee IN (
                          SELECT role.oid FROM pg_catalog.pg_roles role
                          WHERE role.rolname IN (
                              'crypto_agent_evidence_owner',
                              'crypto_agent_evidence_verifier',
                              'crypto_agent_evidence_reader'
                          )
                      )
                      AND acl.privilege_type = 'USAGE'
                      AND NOT acl.is_grantable
                   ) AS schema_privileges_safe,
            NOT EXISTS (
                SELECT 1
                FROM verifier_functions expected
                LEFT JOIN pg_catalog.pg_proc proc
                  ON proc.proname = expected.function_name
                 AND proc.pronargs = expected.argument_count
                LEFT JOIN pg_catalog.pg_namespace ns
                  ON ns.oid = proc.pronamespace
                 AND ns.nspname = 'crypto_agent'
                WHERE proc.oid IS NULL OR ns.oid IS NULL
                   OR NOT pg_catalog.has_function_privilege(
                        'crypto_agent_evidence_verifier', proc.oid, 'EXECUTE'
                   )
                   OR EXISTS (
                        SELECT 1
                        FROM pg_catalog.aclexplode(coalesce(
                            proc.proacl,
                            pg_catalog.acldefault('f', proc.proowner)
                        )) acl
                        WHERE acl.privilege_type = 'EXECUTE'
                          AND NOT (
                            acl.grantee = proc.proowner
                            OR (acl.grantee = (
                                    SELECT role.oid
                                    FROM pg_catalog.pg_roles role
                                    WHERE role.rolname =
                                        'crypto_agent_evidence_verifier'
                                )
                                AND NOT acl.is_grantable)
                          )
                   )
            ) AND (SELECT count(*) = 6
                    FROM verifier_functions expected
                    JOIN pg_catalog.pg_proc proc
                      ON proc.proname = expected.function_name
                     AND proc.pronargs = expected.argument_count
                    JOIN pg_catalog.pg_namespace ns
                      ON ns.oid = proc.pronamespace
                     AND ns.nspname = 'crypto_agent'
                    CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(
                        proc.proacl, pg_catalog.acldefault('f', proc.proowner)
                    )) acl
                    WHERE acl.grantee = (
                        SELECT role.oid FROM pg_catalog.pg_roles role
                        WHERE role.rolname = 'crypto_agent_evidence_verifier'
                    )
                      AND acl.privilege_type = 'EXECUTE'
                      AND NOT acl.is_grantable
                   ) AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_proc proc
                JOIN pg_catalog.pg_namespace ns ON ns.oid = proc.pronamespace
                CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(
                    proc.proacl, pg_catalog.acldefault('f', proc.proowner)
                )) acl
                WHERE ns.nspname = 'crypto_agent'
                  AND proc.proname = 't4_observation_gate_is_verified'
                  AND ((proc.pronargs = 1 AND NOT (
                        acl.grantee = proc.proowner
                        OR (acl.grantee = 0
                            AND acl.privilege_type = 'EXECUTE'
                            AND NOT acl.is_grantable)
                  )) OR (proc.pronargs = 2 AND
                        acl.grantee IS DISTINCT FROM proc.proowner))
            ) AND (SELECT count(*) = 1
                    FROM pg_catalog.pg_proc proc
                    JOIN pg_catalog.pg_namespace ns ON ns.oid = proc.pronamespace
                    CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(
                        proc.proacl, pg_catalog.acldefault('f', proc.proowner)
                    )) acl
                    WHERE ns.nspname = 'crypto_agent'
                      AND proc.proname = 't4_observation_gate_is_verified'
                      AND proc.pronargs = 1
                      AND acl.grantee = 0
                      AND acl.privilege_type = 'EXECUTE'
                      AND NOT acl.is_grantable
            ) AND NOT EXISTS (
                SELECT 1
                FROM owner_only_functions expected
                JOIN pg_catalog.pg_proc proc
                  ON proc.proname = expected.function_name
                 AND proc.pronargs = expected.argument_count
                JOIN pg_catalog.pg_namespace ns
                  ON ns.oid = proc.pronamespace
                 AND ns.nspname = 'crypto_agent'
                CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(
                    proc.proacl, pg_catalog.acldefault('f', proc.proowner)
                )) acl
                WHERE acl.grantee IS DISTINCT FROM proc.proowner
            ) AS verifier_functions_restricted,
            NOT EXISTS (
                SELECT 1 FROM owner_inputs expected
                LEFT JOIN pg_catalog.pg_class relation
                  ON relation.relname = expected.table_name
                LEFT JOIN pg_catalog.pg_namespace ns
                  ON ns.oid = relation.relnamespace
                 AND ns.nspname = 'crypto_agent'
                WHERE relation.oid IS NULL OR ns.oid IS NULL
                   OR NOT pg_catalog.has_table_privilege(
                        'crypto_agent_evidence_owner', relation.oid, 'SELECT'
                   )
            ) AND NOT EXISTS (
                SELECT 1 FROM reader_inputs expected
                LEFT JOIN pg_catalog.pg_class relation
                  ON relation.relname = expected.table_name
                LEFT JOIN pg_catalog.pg_namespace ns
                  ON ns.oid = relation.relnamespace
                 AND ns.nspname = 'crypto_agent'
                WHERE relation.oid IS NULL OR ns.oid IS NULL
                   OR NOT pg_catalog.has_table_privilege(
                        'crypto_agent_evidence_reader', relation.oid, 'SELECT'
                   )
                   OR pg_catalog.has_table_privilege(
                        'crypto_agent_evidence_reader', relation.oid,
                        'INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER'
                   )
            ) AS owner_inputs_readable,
            NOT EXISTS (
                SELECT 1
                FROM critical_definer_triggers expected
                LEFT JOIN pg_catalog.pg_proc proc
                  ON proc.proname = expected.function_name
                LEFT JOIN pg_catalog.pg_namespace ns
                  ON ns.oid = proc.pronamespace
                 AND ns.nspname = 'crypto_agent'
                LEFT JOIN pg_catalog.pg_roles owner ON owner.oid = proc.proowner
                WHERE proc.oid IS NULL OR ns.oid IS NULL
                   OR proc.prorettype <>
                        'pg_catalog.trigger'::pg_catalog.regtype
                   OR NOT proc.prosecdef
                   OR owner.rolname <> 'crypto_agent_evidence_owner'
                   OR NOT coalesce(
                        'search_path=pg_catalog, crypto_agent' =
                            ANY(proc.proconfig), FALSE
                   )
            ) AS critical_definer_triggers_safe
        """
    )
    row = db_cursor.fetchone()
    labels = (
        "pgcrypto_schema",
        "evidence_owner_role",
        "evidence_verifier_role",
        "evidence_reader_role",
        "evidence_roles_separated",
        "evidence_table_owners",
        "evidence_table_acl",
        "evidence_schema_acl",
        "evidence_function_acl",
        "evidence_owner_inputs",
        "critical_definer_triggers",
    )
    if row is None:
        return tuple(f"boundary:{label}" for label in labels)
    return tuple(
        f"boundary:{label}"
        for index, label in enumerate(labels)
        if _row_value(row, label, index) is not True
    )


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


def _catalog_constraints(
    db_cursor: DBCursor,
    required: Sequence[_CatalogDefinitionRequirement],
) -> dict[tuple[str, str], tuple[str, bool, str]]:
    db_cursor.execute(
        """
        SELECT rel.relname, con.conname, con.contype, con.convalidated,
               pg_catalog.pg_get_constraintdef(con.oid, false)
        FROM pg_catalog.pg_constraint AS con
        JOIN pg_catalog.pg_class AS rel ON rel.oid = con.conrelid
        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = rel.relnamespace
        WHERE ns.nspname = 'crypto_agent'
          AND rel.relname = ANY(%s)
          AND con.conname = ANY(%s)
        """,
        (
            sorted({item.table for item in required}),
            sorted({item.name for item in required}),
        ),
    )
    return {
        (
            str(_row_value(row, "relname", 0)),
            str(_row_value(row, "conname", 1)),
        ): (
            str(_row_value(row, "contype", 2)),
            _row_value(row, "convalidated", 3) is True,
            _normalize_catalog_definition(_row_value(row, "definition", 4)),
        )
        for row in db_cursor.fetchall()
    }


def _catalog_definition_differences(
    object_label: str,
    required: Sequence[_CatalogDefinitionRequirement],
    actual: Mapping[tuple[str, str], tuple[str, bool, str]],
) -> tuple[str, ...]:
    required_by_key = {(item.table, item.name): item for item in required}
    differences = {
        f"{object_label}:{item.table}.{item.name}"
        for item in required
        if actual.get((item.table, item.name))
        != (
            item.object_type,
            True,
            _normalize_catalog_definition(item.definition),
        )
    }
    differences.update(
        f"unexpected:{object_label}:{table}.{name}"
        for table, name in actual.keys() - required_by_key.keys()
    )
    return tuple(sorted(differences))


def _catalog_indexes(
    db_cursor: DBCursor,
    required: Sequence[_IndexRequirement],
) -> dict[tuple[str, str], tuple[bool, bool, bool, str]]:
    db_cursor.execute(
        """
        SELECT rel.relname, idx.relname, ind.indisunique, ind.indisvalid,
               ind.indisready,
               pg_catalog.pg_get_indexdef(ind.indexrelid, 0, false)
        FROM pg_catalog.pg_index AS ind
        JOIN pg_catalog.pg_class AS rel ON rel.oid = ind.indrelid
        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = rel.relnamespace
        JOIN pg_catalog.pg_class AS idx ON idx.oid = ind.indexrelid
        WHERE ns.nspname = 'crypto_agent'
          AND rel.relname = ANY(%s)
          AND idx.relname = ANY(%s)
        """,
        (
            sorted({item.table for item in required}),
            sorted({item.name for item in required}),
        ),
    )
    return {
        (
            str(_row_value(row, "relname", 0)),
            str(_row_value(row, "index_name", 1)),
        ): (
            _row_value(row, "indisunique", 2) is True,
            _row_value(row, "indisvalid", 3) is True,
            _row_value(row, "indisready", 4) is True,
            _normalize_catalog_definition(_row_value(row, "definition", 5)),
        )
        for row in db_cursor.fetchall()
    }


def _index_differences(
    required: Sequence[_IndexRequirement],
    actual: Mapping[tuple[str, str], tuple[bool, bool, bool, str]],
) -> tuple[str, ...]:
    required_by_key = {(item.table, item.name): item for item in required}
    differences = {
        f"index:{item.table}.{item.name}"
        for item in required
        if actual.get((item.table, item.name))
        != (
            item.unique,
            True,
            True,
            _normalize_catalog_definition(item.definition),
        )
    }
    differences.update(
        f"unexpected:index:{table}.{name}"
        for table, name in actual.keys() - required_by_key.keys()
    )
    return tuple(sorted(differences))


def _catalog_function_definitions(
    db_cursor: DBCursor,
    required: Sequence[_FunctionDefinitionRequirement],
) -> dict[str, tuple[str, str, bool, str]]:
    db_cursor.execute(
        """
        SELECT proc.proname, lang.lanname, proc.provolatile, proc.prosecdef,
               proc.prosrc
        FROM pg_catalog.pg_proc AS proc
        JOIN pg_catalog.pg_namespace AS ns ON ns.oid = proc.pronamespace
        JOIN pg_catalog.pg_language AS lang ON lang.oid = proc.prolang
        WHERE ns.nspname = 'crypto_agent'
          AND proc.proname = ANY(%s)
          AND proc.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype
          AND pg_catalog.pg_get_function_identity_arguments(proc.oid) = ''
        """,
        ([item.name for item in required],),
    )
    return {
        str(_row_value(row, "proname", 0)): (
            str(_row_value(row, "lanname", 1)),
            str(_row_value(row, "provolatile", 2)),
            _row_value(row, "prosecdef", 3) is True,
            _definition_sha256(_row_value(row, "prosrc", 4)),
        )
        for row in db_cursor.fetchall()
    }


def _function_definition_differences(
    required: Sequence[_FunctionDefinitionRequirement],
    actual: Mapping[str, tuple[str, str, bool, str]],
) -> tuple[str, ...]:
    required_by_name = {item.name: item for item in required}
    differences = {
        f"function_definition:{item.name}"
        for item in required
        if actual.get(item.name)
        != (
            item.language,
            item.volatility,
            item.security_definer,
            item.source_sha256,
        )
    }
    differences.update(
        f"unexpected:function_definition:{name}"
        for name in actual.keys() - required_by_name.keys()
    )
    return tuple(sorted(differences))


def _normalize_catalog_definition(value: object) -> str:
    return " ".join(str(value).split())


def _definition_sha256(value: object) -> str:
    normalized = _normalize_catalog_definition(value)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


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
                type_mask=_catalog_int(_row_value(row, "tgtype", 4)),
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
            _catalog_int(_row_value(row, "interval_seconds", 1)),
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


def _catalog_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, str, bytes, bytearray)):
        raise PostgresOperationError("PostgreSQL returned an invalid catalog integer")
    try:
        return int(value)
    except ValueError:
        raise PostgresOperationError(
            "PostgreSQL returned an invalid catalog integer"
        ) from None


def _row_value(row: object, key: str, index: int) -> object:
    if isinstance(row, Mapping):
        typed_row = cast(Mapping[object, object], row)
        if key in typed_row:
            return typed_row[key]
        return tuple(typed_row.values())[index]
    if isinstance(row, Sequence) and not isinstance(row, (str, bytes, bytearray)):
        return cast(Sequence[object], row)[index]
    raise PostgresOperationError("PostgreSQL returned an unsupported row shape")

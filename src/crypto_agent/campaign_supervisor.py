from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import FrameType
from typing import Any, Protocol
from uuid import UUID, uuid4


class CampaignSupervisorError(RuntimeError):
    """Safe operational failure exposed without secrets or raw subprocess output."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ScopeProgress:
    scope_key: str
    sequence_no: int
    next_expected_at: datetime
    last_expected_at: datetime | None
    last_finished_at: datetime | None
    last_outcome: str | None
    successful_cycles: int
    failed_cycles: int
    missed_cycles: int

    @property
    def attempted_cycles(self) -> int:
        return self.successful_cycles + self.failed_cycles


@dataclass(frozen=True)
class CampaignProgress:
    campaign_id: str
    database_now: datetime
    started_at: datetime
    planned_ends_at: datetime
    cycle_interval_seconds: int
    scopes: tuple[ScopeProgress, ...]
    final_report_stored: bool

    def due_scopes(self) -> tuple[ScopeProgress, ...]:
        if self.final_report_stored:
            return ()
        return tuple(
            scope
            for scope in self.scopes
            if scope.next_expected_at <= self.planned_ends_at
            and self.database_now >= scope.next_expected_at
        )

    @property
    def finalization_due(self) -> bool:
        return (
            not self.final_report_stored
            and self.database_now
            >= self.planned_ends_at
            + timedelta(seconds=self.cycle_interval_seconds)
        )


@dataclass(frozen=True)
class CycleCommandResult:
    scope_key: str
    exit_code: int
    status: str
    outcome: str | None = None
    error_code: str | None = None
    sequence_no: int | None = None
    missed_cycles_recorded: int = 0
    timed_out: bool = False

    @property
    def recorded(self) -> bool:
        return self.status == "recorded" and self.outcome in {
            "success",
            "failure",
            "missed",
        }

    @property
    def retryable(self) -> bool:
        return self.timed_out or (
            self.status == "error"
            and self.error_code
            in {
                "OBSERVATION_CYCLE_FAILED",
                "OBSERVATION_CYCLE_STORAGE_UNAVAILABLE",
                "OBSERVATION_DATABASE_UNAVAILABLE",
            }
        )


@dataclass(frozen=True)
class ReportCommandResult:
    exit_code: int
    status: str
    error_code: str | None = None
    v1_gate_passed: bool | None = None

    @property
    def stored(self) -> bool:
        return self.status == "stored"


@dataclass(frozen=True)
class ServiceStatus:
    name: str
    unit: str
    active: bool
    restart_attempted: bool = False
    recovered: bool = False


@dataclass(frozen=True)
class CampaignSupervisorConfig:
    campaign_id: str
    state_directory: Path
    poll_seconds: float = 15.0
    cycle_timeout_seconds: float = 120.0
    command_retries: int = 2
    retry_backoff_seconds: float = 2.0
    limit: int = 120
    process_manager: str = "systemd"
    bridge_unit: str = "crypto-agent-t4-bridge.service"
    verifier_unit: str = "crypto-agent-evidence-verifier.service"
    service_restart_cooldown_seconds: float = 30.0

    def validate(self) -> None:
        try:
            UUID(self.campaign_id)
        except (TypeError, ValueError, AttributeError):
            raise CampaignSupervisorError(
                "Campaign identifier is invalid",
                code="SUPERVISOR_CAMPAIGN_ID_INVALID",
            ) from None
        if not self.state_directory.is_absolute():
            raise CampaignSupervisorError(
                "Supervisor state directory must be absolute",
                code="SUPERVISOR_STATE_DIRECTORY_INVALID",
            )
        if not 1.0 <= float(self.poll_seconds) <= 300.0:
            raise CampaignSupervisorError(
                "Supervisor polling interval is invalid",
                code="SUPERVISOR_POLL_INTERVAL_INVALID",
            )
        if not 10.0 <= float(self.cycle_timeout_seconds) <= 300.0:
            raise CampaignSupervisorError(
                "Supervisor cycle timeout is invalid",
                code="SUPERVISOR_CYCLE_TIMEOUT_INVALID",
            )
        if type(self.command_retries) is not int or not 0 <= self.command_retries <= 5:
            raise CampaignSupervisorError(
                "Supervisor retry count is invalid",
                code="SUPERVISOR_RETRY_POLICY_INVALID",
            )
        if not 0.1 <= float(self.retry_backoff_seconds) <= 60.0:
            raise CampaignSupervisorError(
                "Supervisor retry backoff is invalid",
                code="SUPERVISOR_RETRY_POLICY_INVALID",
            )
        if type(self.limit) is not int or not 60 <= self.limit <= 720:
            raise CampaignSupervisorError(
                "Supervisor observation limit is invalid",
                code="SUPERVISOR_LIMIT_INVALID",
            )
        if self.process_manager not in {"none", "systemd"}:
            raise CampaignSupervisorError(
                "Supervisor process manager is invalid",
                code="SUPERVISOR_PROCESS_MANAGER_INVALID",
            )
        for unit in (self.bridge_unit, self.verifier_unit):
            if not _valid_systemd_unit(unit):
                raise CampaignSupervisorError(
                    "Supervisor systemd unit is invalid",
                    code="SUPERVISOR_SYSTEMD_UNIT_INVALID",
                )
        if not 1.0 <= float(self.service_restart_cooldown_seconds) <= 3600.0:
            raise CampaignSupervisorError(
                "Supervisor restart cooldown is invalid",
                code="SUPERVISOR_RESTART_POLICY_INVALID",
            )


class CampaignProgressReader(Protocol):
    def load_progress(self, campaign_id: str) -> CampaignProgress: ...


class CampaignCommandRunner(Protocol):
    def run_cycle(
        self,
        campaign_id: str,
        scope_key: str,
        *,
        limit: int,
        timeout_seconds: float,
    ) -> CycleCommandResult: ...

    def store_report(
        self,
        campaign_id: str,
        *,
        timeout_seconds: float,
    ) -> ReportCommandResult: ...


class ServiceManager(Protocol):
    def ensure_services(self, now: datetime) -> tuple[ServiceStatus, ...]: ...


class NullServiceManager:
    def ensure_services(self, now: datetime) -> tuple[ServiceStatus, ...]:
        del now
        return ()


class SystemdServiceManager:
    """Restart only two frozen systemd units; never accepts a shell command."""

    def __init__(
        self,
        units: Mapping[str, str],
        *,
        cooldown_seconds: float,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        if set(units) != {"bridge", "evidence_verifier"} or any(
            not _valid_systemd_unit(unit) for unit in units.values()
        ):
            raise CampaignSupervisorError(
                "Managed service allowlist is invalid",
                code="SUPERVISOR_SYSTEMD_UNIT_INVALID",
            )
        self._units = dict(units)
        self._cooldown_seconds = cooldown_seconds
        self._run = run
        self._last_restart: dict[str, datetime] = {}

    def ensure_services(self, now: datetime) -> tuple[ServiceStatus, ...]:
        statuses: list[ServiceStatus] = []
        for name, unit in sorted(self._units.items()):
            active = self._is_active(unit)
            attempted = False
            recovered = False
            last_restart = self._last_restart.get(name)
            cooldown_elapsed = (
                last_restart is None
                or (now - last_restart).total_seconds() >= self._cooldown_seconds
            )
            if not active and cooldown_elapsed:
                attempted = True
                self._last_restart[name] = now
                self._restart(unit)
                recovered = self._is_active(unit)
                active = recovered
            statuses.append(
                ServiceStatus(
                    name=name,
                    unit=unit,
                    active=active,
                    restart_attempted=attempted,
                    recovered=recovered,
                )
            )
        return tuple(statuses)

    def _is_active(self, unit: str) -> bool:
        try:
            result = self._run(
                ["systemctl", "is-active", "--quiet", "--", unit],
                capture_output=True,
                text=True,
                timeout=10.0,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return False
        return result.returncode == 0

    def _restart(self, unit: str) -> None:
        try:
            self._run(
                ["systemctl", "restart", "--", unit],
                capture_output=True,
                text=True,
                timeout=30.0,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            return


class SubprocessCampaignCommandRunner:
    """Run every cycle separately so a hung read can be terminated safely."""

    def __init__(
        self,
        *,
        executable: Sequence[str] | None = None,
        run: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    ) -> None:
        self._executable = tuple(
            executable or (sys.executable, "-m", "crypto_agent.cli")
        )
        if not self._executable or any(not item for item in self._executable):
            raise CampaignSupervisorError(
                "Supervisor command executable is invalid",
                code="SUPERVISOR_EXECUTABLE_INVALID",
            )
        self._run = run

    def run_cycle(
        self,
        campaign_id: str,
        scope_key: str,
        *,
        limit: int,
        timeout_seconds: float,
    ) -> CycleCommandResult:
        command = (
            *self._executable,
            "observe-run",
            "--campaign-id",
            campaign_id,
            "--scope",
            scope_key,
            "--limit",
            str(limit),
        )
        try:
            completed = self._run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return CycleCommandResult(
                scope_key=scope_key,
                exit_code=124,
                status="error",
                error_code="OBSERVATION_CYCLE_TIMEOUT",
                timed_out=True,
            )
        except OSError:
            return CycleCommandResult(
                scope_key=scope_key,
                exit_code=1,
                status="error",
                error_code="OBSERVATION_CYCLE_PROCESS_FAILED",
            )
        payload = _safe_json_object(completed.stdout)
        if payload is None:
            return CycleCommandResult(
                scope_key=scope_key,
                exit_code=completed.returncode,
                status="error",
                error_code="OBSERVATION_CYCLE_OUTPUT_INVALID",
            )
        return CycleCommandResult(
            scope_key=scope_key,
            exit_code=completed.returncode,
            status=_text(payload.get("status"), "error"),
            outcome=_optional_text(payload.get("outcome")),
            error_code=_optional_text(payload.get("error_code")),
            sequence_no=_optional_int(payload.get("sequence_no")),
            missed_cycles_recorded=_nonnegative_int(
                payload.get("missed_cycles_recorded")
            ),
        )

    def store_report(
        self,
        campaign_id: str,
        *,
        timeout_seconds: float,
    ) -> ReportCommandResult:
        command = (
            *self._executable,
            "observe-report",
            "--campaign-id",
            campaign_id,
        )
        try:
            completed = self._run(
                command,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return ReportCommandResult(
                exit_code=124,
                status="error",
                error_code="OBSERVATION_REPORT_TIMEOUT",
            )
        except OSError:
            return ReportCommandResult(
                exit_code=1,
                status="error",
                error_code="OBSERVATION_REPORT_PROCESS_FAILED",
            )
        payload = _safe_json_object(completed.stdout)
        if payload is None:
            return ReportCommandResult(
                exit_code=completed.returncode,
                status="error",
                error_code="OBSERVATION_REPORT_OUTPUT_INVALID",
            )
        gate_value = payload.get("v1_gate_passed")
        return ReportCommandResult(
            exit_code=completed.returncode,
            status=_text(payload.get("status"), "error"),
            error_code=_optional_text(payload.get("error_code")),
            v1_gate_passed=gate_value if isinstance(gate_value, bool) else None,
        )


class PostgresCampaignProgressReader:
    def __init__(self, connection_factory: Any) -> None:
        self.connection_factory = connection_factory

    def load_progress(self, campaign_id: str) -> CampaignProgress:
        from .postgres import cursor, transaction

        try:
            with (
                transaction(self.connection_factory) as connection,
                cursor(connection) as db_cursor,
            ):
                db_cursor.execute("SET TRANSACTION READ ONLY")
                db_cursor.execute(
                    """
                    SELECT started_at, planned_ends_at, cycle_interval_seconds,
                           scope_manifest, clock_timestamp(),
                           EXISTS (
                               SELECT 1
                               FROM crypto_agent.t4_observation_quality_reports report
                               WHERE report.campaign_id = campaign.campaign_id
                           )
                    FROM crypto_agent.t4_observation_campaigns campaign
                    WHERE campaign_id = %s
                    """,
                    (campaign_id,),
                )
                raw_row = db_cursor.fetchone()
                if raw_row is None:
                    raise CampaignSupervisorError(
                        "Observation campaign does not exist",
                        code="SUPERVISOR_CAMPAIGN_NOT_FOUND",
                    )
                row = _database_row(raw_row, 6)
                started_at = _utc_datetime(row[0], "started_at")
                planned_ends_at = _utc_datetime(row[1], "planned_ends_at")
                interval_seconds = _positive_int(row[2], "cycle_interval_seconds")
                scopes = _scope_manifest(row[3])
                database_now = _utc_datetime(row[4], "database_now")
                final_report_stored = row[5] is True
                db_cursor.execute(
                    """
                    WITH latest AS (
                        SELECT DISTINCT ON (scope_key)
                               scope_key, sequence_no, expected_at,
                               finished_at, outcome
                        FROM crypto_agent.t4_observation_cycles
                        WHERE campaign_id = %s
                        ORDER BY scope_key, sequence_no DESC
                    ), aggregate AS (
                        SELECT scope_key,
                               COUNT(*) FILTER (WHERE outcome = 'success') AS successes,
                               COUNT(*) FILTER (WHERE outcome = 'failure') AS failures,
                               COUNT(*) FILTER (WHERE outcome = 'missed') AS missed
                        FROM crypto_agent.t4_observation_cycles
                        WHERE campaign_id = %s
                        GROUP BY scope_key
                    )
                    SELECT latest.scope_key, latest.sequence_no,
                           latest.expected_at, latest.finished_at, latest.outcome,
                           aggregate.successes, aggregate.failures, aggregate.missed
                    FROM latest JOIN aggregate USING (scope_key)
                    ORDER BY latest.scope_key
                    """,
                    (campaign_id, campaign_id),
                )
                existing: dict[str, Sequence[object]] = {}
                for raw_item in db_cursor.fetchall():
                    item = _database_row(raw_item, 8)
                    existing[str(item[0])] = item
        except CampaignSupervisorError:
            raise
        except Exception:
            raise CampaignSupervisorError(
                "Campaign progress is unavailable",
                code="SUPERVISOR_DATABASE_UNAVAILABLE",
            ) from None
        progress: list[ScopeProgress] = []
        for scope_key in scopes:
            current = existing.get(scope_key)
            sequence_no = 0 if current is None else _database_count(current[1])
            progress.append(
                ScopeProgress(
                    scope_key=scope_key,
                    sequence_no=sequence_no,
                    next_expected_at=started_at
                    + timedelta(seconds=interval_seconds * (sequence_no + 1)),
                    last_expected_at=(
                        None
                        if current is None
                        else _utc_datetime(current[2], "expected_at")
                    ),
                    last_finished_at=(
                        None
                        if current is None
                        else _utc_datetime(current[3], "finished_at")
                    ),
                    last_outcome=None if current is None else str(current[4]),
                    successful_cycles=(
                        0 if current is None else _database_count(current[5])
                    ),
                    failed_cycles=(
                        0 if current is None else _database_count(current[6])
                    ),
                    missed_cycles=(
                        0 if current is None else _database_count(current[7])
                    ),
                )
            )
        return CampaignProgress(
            campaign_id=campaign_id,
            database_now=database_now,
            started_at=started_at,
            planned_ends_at=planned_ends_at,
            cycle_interval_seconds=interval_seconds,
            scopes=tuple(progress),
            final_report_stored=final_report_stored,
        )


class SupervisorStateStore:
    """Singleton lock, atomic live status and crash-recoverable hash-chained journal."""

    def __init__(self, directory: Path, campaign_id: str) -> None:
        self.directory = directory
        self.campaign_id = campaign_id
        self.status_path = directory / "status.json"
        self.daily_path = directory / "daily-snapshots.jsonl"
        self.events_path = directory / "events.jsonl"
        self.lock_path = directory / "supervisor.lock"
        self._lock_file: Any | None = None
        self._event_head: str | None = None
        self._daily_head: str | None = None
        self._recovered_daily_date: date | None = None

    def __enter__(self) -> SupervisorStateStore:
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.directory, 0o700)
        self._lock_file = self.lock_path.open("a+", encoding="utf-8")
        os.chmod(self.lock_path, 0o600)
        try:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self._lock_file.close()
            self._lock_file = None
            raise CampaignSupervisorError(
                "Another campaign supervisor already owns this state directory",
                code="SUPERVISOR_ALREADY_RUNNING",
            ) from None
        self._event_head = self._recover_journal(self.events_path, "event")
        self._daily_head = self._recover_journal(self.daily_path, "snapshot")
        return self

    def __exit__(self, *_args: object) -> None:
        if self._lock_file is not None:
            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)
            self._lock_file.close()
            self._lock_file = None

    def read_status(self) -> dict[str, object] | None:
        if not self.status_path.exists():
            return None
        try:
            payload = json.loads(self.status_path.read_text(encoding="utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(payload, dict) or payload.get("campaign_id") != self.campaign_id:
            return None
        return payload

    def write_status(self, payload: Mapping[str, object]) -> None:
        record = dict(payload)
        record["schema_version"] = 1
        record["campaign_id"] = self.campaign_id
        record["read_only"] = True
        record["execution_enabled"] = False
        data = _canonical_json(record) + b"\n"
        temporary = self.status_path.with_suffix(".json.tmp")
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.status_path)
            _fsync_directory(self.directory)
        finally:
            if temporary.exists():
                temporary.unlink()

    def append_event(self, payload: Mapping[str, object]) -> str:
        record, digest = _chain_record(
            kind="event",
            campaign_id=self.campaign_id,
            payload=payload,
            previous_hash=self._event_head,
        )
        _append_fsynced(self.events_path, _canonical_json(record) + b"\n")
        self._event_head = digest
        return digest

    def append_daily_snapshot(self, payload: Mapping[str, object]) -> str:
        record, digest = _chain_record(
            kind="snapshot",
            campaign_id=self.campaign_id,
            payload=payload,
            previous_hash=self._daily_head,
        )
        _append_fsynced(self.daily_path, _canonical_json(record) + b"\n")
        self._daily_head = digest
        return digest

    @property
    def last_daily_date(self) -> date | None:
        return self._recovered_daily_date

    def _recover_journal(self, path: Path, expected_kind: str) -> str | None:
        if not path.exists():
            return None
        os.chmod(path, 0o600)
        data = path.read_bytes()
        if data and not data.endswith(b"\n"):
            committed = data.rsplit(b"\n", 1)[0]
            data = committed + (b"\n" if committed else b"")
            descriptor = os.open(path, os.O_WRONLY | os.O_TRUNC)
            with os.fdopen(descriptor, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
        previous: str | None = None
        for line in data.splitlines():
            try:
                record = json.loads(line)
            except (TypeError, ValueError, json.JSONDecodeError):
                raise CampaignSupervisorError(
                    "Supervisor journal is corrupt",
                    code="SUPERVISOR_JOURNAL_INVALID",
                ) from None
            if not isinstance(record, dict):
                raise CampaignSupervisorError(
                    "Supervisor journal is corrupt",
                    code="SUPERVISOR_JOURNAL_INVALID",
                )
            stored_hash = record.pop("content_hash_sha256", None)
            if (
                record.get("kind") != expected_kind
                or record.get("campaign_id") != self.campaign_id
                or record.get("previous_hash_sha256") != previous
                or not isinstance(stored_hash, str)
                or hashlib.sha256(_canonical_json(record)).hexdigest() != stored_hash
            ):
                raise CampaignSupervisorError(
                    "Supervisor journal chain is invalid",
                    code="SUPERVISOR_JOURNAL_INVALID",
                )
            previous = stored_hash
            if expected_kind == "snapshot":
                payload = record.get("payload")
                snapshot_date = (
                    payload.get("snapshot_date")
                    if isinstance(payload, Mapping)
                    else None
                )
                if not isinstance(snapshot_date, str):
                    raise CampaignSupervisorError(
                        "Supervisor daily snapshot is invalid",
                        code="SUPERVISOR_JOURNAL_INVALID",
                    )
                try:
                    self._recovered_daily_date = date.fromisoformat(snapshot_date)
                except ValueError:
                    raise CampaignSupervisorError(
                        "Supervisor daily snapshot is invalid",
                        code="SUPERVISOR_JOURNAL_INVALID",
                    ) from None
        return previous


@dataclass
class CampaignSupervisor:
    config: CampaignSupervisorConfig
    progress_reader: CampaignProgressReader
    command_runner: CampaignCommandRunner
    service_manager: ServiceManager
    store: SupervisorStateStore
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    sleeper: Callable[[float], None] = time.sleep
    instance_id: str = field(default_factory=lambda: str(uuid4()))
    _last_daily_date: date | None = field(default=None, init=False)
    _last_missed_counts: dict[str, int] = field(default_factory=dict, init=False)
    _stuck_cycles: int = field(default=0, init=False)
    _restart_requests: int = field(default=0, init=False)

    def run(self, stop_event: threading.Event, *, once: bool = False) -> None:
        self.config.validate()
        with self.store:
            self._last_daily_date = self.store.last_daily_date
            previous = self.store.read_status()
            boot_count = _nonnegative_int(
                None if previous is None else previous.get("boot_count")
            ) + 1
            self.store.append_event(
                {
                    "event_type": "supervisor_started",
                    "event_at": self.clock().isoformat(),
                    "instance_id": self.instance_id,
                    "boot_count": boot_count,
                    "recovered_from_previous_boot": previous is not None,
                }
            )
            while not stop_event.is_set():
                complete = self._tick(boot_count)
                if once or complete:
                    break
                if stop_event.wait(self.config.poll_seconds):
                    break
            self.store.append_event(
                {
                    "event_type": "supervisor_stopped",
                    "event_at": self.clock().isoformat(),
                    "instance_id": self.instance_id,
                    "boot_count": boot_count,
                }
            )

    def _tick(self, boot_count: int) -> bool:
        local_now = self.clock()
        services = self.service_manager.ensure_services(local_now)
        for service in services:
            if service.restart_attempted:
                self._restart_requests += 1
                self.store.append_event(
                    {
                        "event_type": "service_restart_requested",
                        "event_at": local_now.isoformat(),
                        "instance_id": self.instance_id,
                        "service": service.name,
                        "unit": service.unit,
                        "recovered": service.recovered,
                    }
                )
        progress = self.progress_reader.load_progress(self.config.campaign_id)
        self._record_new_missed(progress)
        cycle_results: list[CycleCommandResult] = []
        if all(service.active for service in services):
            for scope in progress.due_scopes():
                cycle_results.append(self._run_scope(scope.scope_key))
            if cycle_results:
                progress = self.progress_reader.load_progress(self.config.campaign_id)
                self._record_new_missed(progress)
        report_result: ReportCommandResult | None = None
        if progress.finalization_due:
            report_result = self.command_runner.store_report(
                self.config.campaign_id,
                timeout_seconds=self.config.cycle_timeout_seconds,
            )
            self.store.append_event(
                {
                    "event_type": "final_report_attempted",
                    "event_at": self.clock().isoformat(),
                    "instance_id": self.instance_id,
                    "status": report_result.status,
                    "error_code": report_result.error_code,
                    "v1_gate_passed": report_result.v1_gate_passed,
                }
            )
            if report_result.stored:
                progress = self.progress_reader.load_progress(self.config.campaign_id)
        status = self._status_payload(
            progress,
            services,
            cycle_results,
            report_result,
            boot_count,
        )
        self.store.write_status(status)
        snapshot_date = progress.database_now.date()
        if self._last_daily_date != snapshot_date:
            self.store.append_daily_snapshot(
                {
                    "snapshot_date": snapshot_date.isoformat(),
                    "snapshot_at": progress.database_now.isoformat(),
                    "instance_id": self.instance_id,
                    "status": status,
                }
            )
            self._last_daily_date = snapshot_date
        return progress.final_report_stored

    def _run_scope(self, scope_key: str) -> CycleCommandResult:
        result: CycleCommandResult | None = None
        for attempt in range(self.config.command_retries + 1):
            result = self.command_runner.run_cycle(
                self.config.campaign_id,
                scope_key,
                limit=self.config.limit,
                timeout_seconds=self.config.cycle_timeout_seconds,
            )
            if result.timed_out:
                self._stuck_cycles += 1
                self.store.append_event(
                    {
                        "event_type": "cycle_timeout_detected",
                        "event_at": self.clock().isoformat(),
                        "instance_id": self.instance_id,
                        "scope": scope_key,
                        "attempt": attempt + 1,
                    }
                )
            if result.recorded or not result.retryable:
                break
            self.store.append_event(
                {
                    "event_type": "cycle_retry_scheduled",
                    "event_at": self.clock().isoformat(),
                    "instance_id": self.instance_id,
                    "scope": scope_key,
                    "attempt": attempt + 1,
                    "error_code": result.error_code,
                }
            )
            self.sleeper(self.config.retry_backoff_seconds * (2**attempt))
        assert result is not None
        self.store.append_event(
            {
                "event_type": "cycle_finished",
                "event_at": self.clock().isoformat(),
                "instance_id": self.instance_id,
                "scope": scope_key,
                "status": result.status,
                "outcome": result.outcome,
                "error_code": result.error_code,
                "sequence_no": result.sequence_no,
                "missed_cycles_recorded": result.missed_cycles_recorded,
            }
        )
        return result

    def _record_new_missed(self, progress: CampaignProgress) -> None:
        for scope in progress.scopes:
            previous = self._last_missed_counts.get(scope.scope_key, 0)
            if scope.missed_cycles > previous:
                self.store.append_event(
                    {
                        "event_type": "missed_cycles_detected",
                        "event_at": progress.database_now.isoformat(),
                        "instance_id": self.instance_id,
                        "scope": scope.scope_key,
                        "new_missed_cycles": scope.missed_cycles - previous,
                        "total_missed_cycles": scope.missed_cycles,
                    }
                )
            self._last_missed_counts[scope.scope_key] = scope.missed_cycles

    def _status_payload(
        self,
        progress: CampaignProgress,
        services: tuple[ServiceStatus, ...],
        results: list[CycleCommandResult],
        report: ReportCommandResult | None,
        boot_count: int,
    ) -> dict[str, object]:
        now = progress.database_now
        remaining = max(0, int((progress.planned_ends_at - now).total_seconds()))
        return {
            "status": "completed" if progress.final_report_stored else "running",
            "instance_id": self.instance_id,
            "boot_count": boot_count,
            "updated_at": now.isoformat(),
            "started_at": progress.started_at.isoformat(),
            "planned_ends_at": progress.planned_ends_at.isoformat(),
            "remaining_seconds": remaining,
            "cycle_interval_seconds": progress.cycle_interval_seconds,
            "final_report_stored": progress.final_report_stored,
            "stuck_cycles_detected": self._stuck_cycles,
            "service_restart_requests": self._restart_requests,
            "services": [
                {
                    "name": item.name,
                    "unit": item.unit,
                    "active": item.active,
                    "restart_attempted": item.restart_attempted,
                    "recovered": item.recovered,
                }
                for item in services
            ],
            "scopes": [
                {
                    "scope": item.scope_key,
                    "sequence_no": item.sequence_no,
                    "next_expected_at": item.next_expected_at.isoformat(),
                    "last_expected_at": (
                        None
                        if item.last_expected_at is None
                        else item.last_expected_at.isoformat()
                    ),
                    "last_finished_at": (
                        None
                        if item.last_finished_at is None
                        else item.last_finished_at.isoformat()
                    ),
                    "last_outcome": item.last_outcome,
                    "successful_cycles": item.successful_cycles,
                    "failed_cycles": item.failed_cycles,
                    "missed_cycles": item.missed_cycles,
                }
                for item in progress.scopes
            ],
            "last_cycle_results": [
                {
                    "scope": item.scope_key,
                    "status": item.status,
                    "outcome": item.outcome,
                    "error_code": item.error_code,
                    "sequence_no": item.sequence_no,
                    "missed_cycles_recorded": item.missed_cycles_recorded,
                }
                for item in results
            ],
            "last_report_result": (
                None
                if report is None
                else {
                    "status": report.status,
                    "error_code": report.error_code,
                    "v1_gate_passed": report.v1_gate_passed,
                }
            ),
        }


def install_signal_handlers(stop_event: threading.Event) -> None:
    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)


def _valid_systemd_unit(value: object) -> bool:
    return isinstance(value, str) and re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9_.@-]{0,126}\.service", value
    ) is not None


def _safe_json_object(value: object) -> dict[str, object] | None:
    if not isinstance(value, str) or len(value.encode("utf-8")) > 1_000_000:
        return None
    try:
        payload = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _text(value: object, default: str) -> str:
    return value if isinstance(value, str) and value else default


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _optional_int(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _nonnegative_int(value: object) -> int:
    return value if type(value) is int and value >= 0 else 0


def _positive_int(value: object, field_name: str) -> int:
    if type(value) is not int or value <= 0:
        raise CampaignSupervisorError(
            f"Campaign {field_name} is invalid",
            code="SUPERVISOR_CAMPAIGN_INVALID",
        )
    return value


def _database_row(value: object, minimum_length: int) -> Sequence[object]:
    if (
        not isinstance(value, Sequence)
        or isinstance(value, (str, bytes))
        or len(value) < minimum_length
    ):
        raise CampaignSupervisorError(
            "Campaign database row is invalid",
            code="SUPERVISOR_CAMPAIGN_INVALID",
        )
    return value


def _database_count(value: object) -> int:
    if type(value) is not int or value < 0:
        raise CampaignSupervisorError(
            "Campaign database count is invalid",
            code="SUPERVISOR_CAMPAIGN_INVALID",
        )
    return value


def _utc_datetime(value: object, field_name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise CampaignSupervisorError(
            f"Campaign {field_name} is invalid",
            code="SUPERVISOR_CAMPAIGN_INVALID",
        )
    return value.astimezone(UTC)


def _scope_manifest(value: object) -> tuple[str, ...]:
    if isinstance(value, Mapping) or (
        isinstance(value, Sequence) and not isinstance(value, (str, bytes))
    ):
        scopes = tuple(sorted(str(item) for item in value))
    else:
        scopes = ()
    if (
        not scopes
        or len(scopes) != len(set(scopes))
        or any(
            re.fullmatch(r"(?:BTC|ETH)/USD:(?:240|1440|10080)m", scope) is None
            for scope in scopes
        )
    ):
        raise CampaignSupervisorError(
            "Campaign scope manifest is invalid",
            code="SUPERVISOR_CAMPAIGN_INVALID",
        )
    return scopes


def _canonical_json(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _chain_record(
    *,
    kind: str,
    campaign_id: str,
    payload: Mapping[str, object],
    previous_hash: str | None,
) -> tuple[dict[str, object], str]:
    record: dict[str, object] = {
        "schema_version": 1,
        "kind": kind,
        "campaign_id": campaign_id,
        "previous_hash_sha256": previous_hash,
        "payload": dict(payload),
        "read_only": True,
        "execution_enabled": False,
    }
    digest = hashlib.sha256(_canonical_json(record)).hexdigest()
    record["content_hash_sha256"] = digest
    return record, digest


def _append_fsynced(path: Path, data: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    with os.fdopen(descriptor, "ab") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)

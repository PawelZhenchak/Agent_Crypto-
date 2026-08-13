from __future__ import annotations

import json
import subprocess
import tempfile
import threading
import unittest
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from crypto_agent.campaign_supervisor import (
    CampaignCommandRunner,
    CampaignProgress,
    CampaignSupervisor,
    CampaignSupervisorConfig,
    CampaignSupervisorError,
    CycleCommandResult,
    NullServiceManager,
    PostgresCampaignProgressReader,
    ReportCommandResult,
    ScopeProgress,
    ServiceStatus,
    SubprocessCampaignCommandRunner,
    SupervisorStateStore,
    SystemdServiceManager,
)
from tests.db_fakes import FakeConnection, SQLStep

CAMPAIGN_ID = "7bf3c831-ae63-4d32-b750-1d694c1de236"
NOW = datetime(2026, 8, 13, 12, 0, tzinfo=UTC)


def _scope(
    key: str,
    *,
    sequence_no: int = 0,
    next_expected_at: datetime | None = None,
    missed: int = 0,
) -> ScopeProgress:
    return ScopeProgress(
        scope_key=key,
        sequence_no=sequence_no,
        next_expected_at=next_expected_at or NOW,
        last_expected_at=None if sequence_no == 0 else NOW - timedelta(minutes=5),
        last_finished_at=None if sequence_no == 0 else NOW - timedelta(minutes=4),
        last_outcome=None if sequence_no == 0 else "success",
        successful_cycles=sequence_no - missed,
        failed_cycles=0,
        missed_cycles=missed,
    )


def _progress(
    *,
    now: datetime = NOW,
    scopes: Sequence[ScopeProgress] | None = None,
    report: bool = False,
    planned_ends_at: datetime | None = None,
) -> CampaignProgress:
    return CampaignProgress(
        campaign_id=CAMPAIGN_ID,
        database_now=now,
        started_at=NOW - timedelta(days=1),
        planned_ends_at=planned_ends_at or NOW + timedelta(days=27),
        cycle_interval_seconds=300,
        scopes=tuple(
            scopes
            or (
                _scope("BTC/USD:240m"),
                _scope("ETH/USD:240m"),
            )
        ),
        final_report_stored=report,
    )


class _ProgressReader:
    def __init__(self, values: Sequence[CampaignProgress]) -> None:
        self.values = list(values)
        self.calls = 0

    def load_progress(self, campaign_id: str) -> CampaignProgress:
        if campaign_id != CAMPAIGN_ID:
            raise AssertionError(campaign_id)
        value = self.values[min(self.calls, len(self.values) - 1)]
        self.calls += 1
        return value


class _Runner(CampaignCommandRunner):
    def __init__(
        self,
        cycles: Sequence[CycleCommandResult],
        report: ReportCommandResult | None = None,
    ) -> None:
        self.cycles = list(cycles)
        self.report = report or ReportCommandResult(1, "error", "NOT_DUE")
        self.cycle_calls: list[tuple[str, str, int, float]] = []
        self.report_calls: list[tuple[str, float]] = []

    def run_cycle(
        self,
        campaign_id: str,
        scope_key: str,
        *,
        limit: int,
        timeout_seconds: float,
    ) -> CycleCommandResult:
        self.cycle_calls.append((campaign_id, scope_key, limit, timeout_seconds))
        if not self.cycles:
            raise AssertionError("unexpected cycle")
        return self.cycles.pop(0)

    def store_report(
        self, campaign_id: str, *, timeout_seconds: float
    ) -> ReportCommandResult:
        self.report_calls.append((campaign_id, timeout_seconds))
        return self.report


class _ServiceManager:
    def __init__(self, statuses: Sequence[ServiceStatus]) -> None:
        self.statuses = tuple(statuses)

    def ensure_services(self, now: datetime) -> tuple[ServiceStatus, ...]:
        del now
        return self.statuses


class CampaignSupervisorConfigTests(unittest.TestCase):
    def test_config_rejects_relative_state_and_unapproved_unit(self) -> None:
        with self.assertRaisesRegex(
            CampaignSupervisorError, "state directory"
        ):
            CampaignSupervisorConfig(CAMPAIGN_ID, Path("state")).validate()
        with self.assertRaisesRegex(CampaignSupervisorError, "systemd unit"):
            CampaignSupervisorConfig(
                CAMPAIGN_ID,
                Path("/tmp/state"),
                bridge_unit="../../bad.service",
            ).validate()

    def test_due_scope_is_derived_from_database_time(self) -> None:
        future = _scope(
            "ETH/USD:240m", next_expected_at=NOW + timedelta(minutes=1)
        )
        progress = _progress(scopes=(_scope("BTC/USD:240m"), future))
        self.assertEqual(
            tuple(item.scope_key for item in progress.due_scopes()),
            ("BTC/USD:240m",),
        )


class SubprocessRunnerTests(unittest.TestCase):
    def test_cycle_uses_argument_vector_and_parses_recorded_result(self) -> None:
        run = Mock(
            return_value=subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout=json.dumps(
                    {
                        "status": "recorded",
                        "outcome": "success",
                        "sequence_no": 8,
                        "missed_cycles_recorded": 2,
                    }
                ),
                stderr="secret must stay ignored",
            )
        )
        result = SubprocessCampaignCommandRunner(
            executable=("crypto-agent",), run=run
        ).run_cycle(
            CAMPAIGN_ID,
            "BTC/USD:240m",
            limit=120,
            timeout_seconds=90,
        )
        self.assertTrue(result.recorded)
        self.assertEqual(result.sequence_no, 8)
        self.assertEqual(result.missed_cycles_recorded, 2)
        command = run.call_args.args[0]
        self.assertIsInstance(command, tuple)
        self.assertEqual(command[1], "observe-run")
        self.assertNotIn("secret", json.dumps(result.__dict__))

    def test_timeout_is_retryable_and_has_no_raw_exception(self) -> None:
        run = Mock(side_effect=subprocess.TimeoutExpired(["crypto-agent"], 10))
        result = SubprocessCampaignCommandRunner(
            executable=("crypto-agent",), run=run
        ).run_cycle(
            CAMPAIGN_ID,
            "BTC/USD:240m",
            limit=120,
            timeout_seconds=10,
        )
        self.assertTrue(result.timed_out)
        self.assertTrue(result.retryable)
        self.assertEqual(result.error_code, "OBSERVATION_CYCLE_TIMEOUT")

    def test_oversized_or_malformed_output_fails_closed(self) -> None:
        for output in ("not-json", "x" * 1_000_001):
            with self.subTest(length=len(output)):
                result = SubprocessCampaignCommandRunner(
                    run=Mock(
                        return_value=subprocess.CompletedProcess(
                            args=[], returncode=0, stdout=output, stderr=""
                        )
                    )
                ).run_cycle(
                    CAMPAIGN_ID,
                    "BTC/USD:240m",
                    limit=120,
                    timeout_seconds=10,
                )
                self.assertEqual(
                    result.error_code, "OBSERVATION_CYCLE_OUTPUT_INVALID"
                )


class SystemdServiceManagerTests(unittest.TestCase):
    def test_systemd_units_are_hardened_and_keep_read_only_role_boundaries(self) -> None:
        bridge = Path(
            "configs/systemd/crypto-agent-t4-bridge.service"
        ).read_text(encoding="utf-8")
        verifier = Path(
            "configs/systemd/crypto-agent-evidence-verifier.service"
        ).read_text(encoding="utf-8")
        supervisor = Path(
            "configs/systemd/crypto-agent-campaign-supervisor.service"
        ).read_text(encoding="utf-8")
        self.assertIn("User=crypto-bridge", bridge)
        self.assertIn("LoadCredential=t4-evidence-key.pem", bridge)
        self.assertIn("Restart=always", bridge)
        self.assertIn("User=crypto-verifier", verifier)
        self.assertIn("Restart=always", verifier)
        self.assertIn("User=crypto-runtime", supervisor)
        self.assertIn("Restart=on-failure", supervisor)
        for unit in (bridge, verifier, supervisor):
            self.assertIn("NoNewPrivileges=true", unit)
            self.assertIn("ProtectSystem=strict", unit)
            self.assertIn("UMask=0077", unit)
        self.assertNotIn("POSTGRES_EVIDENCE_DSN", supervisor)
        self.assertNotIn("T4_API_KEY", supervisor)

    def test_polkit_rule_is_exact_and_never_uses_wildcard_units(self) -> None:
        rule = Path(
            "configs/systemd/50-crypto-agent-supervisor.rules"
        ).read_text(encoding="utf-8")
        self.assertIn('subject.user !== "crypto-runtime"', rule)
        self.assertIn('unit === "crypto-agent-t4-bridge.service"', rule)
        self.assertIn(
            'unit === "crypto-agent-evidence-verifier.service"', rule
        )
        self.assertIn('verb === "restart"', rule)
        self.assertNotIn("startsWith", rule)
        self.assertNotIn("indexOf", rule)

    def test_inactive_service_is_restarted_without_shell(self) -> None:
        run = Mock(
            side_effect=(
                subprocess.CompletedProcess([], 3, "", ""),
                subprocess.CompletedProcess([], 0, "", ""),
                subprocess.CompletedProcess([], 0, "", ""),
                subprocess.CompletedProcess([], 0, "", ""),
            )
        )
        manager = SystemdServiceManager(
            {
                "bridge": "crypto-agent-t4-bridge.service",
                "evidence_verifier": "crypto-agent-evidence-verifier.service",
            },
            cooldown_seconds=30,
            run=run,
        )
        statuses = manager.ensure_services(NOW)
        self.assertTrue(all(item.active for item in statuses))
        bridge = next(item for item in statuses if item.name == "bridge")
        self.assertTrue(bridge.restart_attempted)
        restart_call = run.call_args_list[1]
        self.assertEqual(
            restart_call.args[0],
            [
                "systemctl",
                "restart",
                "--",
                "crypto-agent-t4-bridge.service",
            ],
        )
        self.assertNotIn("shell", restart_call.kwargs)

    def test_failed_restart_obeys_cooldown(self) -> None:
        run = Mock(return_value=subprocess.CompletedProcess([], 3, "", ""))
        manager = SystemdServiceManager(
            {
                "bridge": "crypto-agent-t4-bridge.service",
                "evidence_verifier": "crypto-agent-evidence-verifier.service",
            },
            cooldown_seconds=30,
            run=run,
        )
        first = manager.ensure_services(NOW)
        second = manager.ensure_services(NOW + timedelta(seconds=5))
        self.assertTrue(any(item.restart_attempted for item in first))
        self.assertFalse(any(item.restart_attempted for item in second))


class SupervisorStateStoreTests(unittest.TestCase):
    def test_status_is_atomic_private_and_journals_are_hash_chained(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "state"
            with SupervisorStateStore(directory, CAMPAIGN_ID) as store:
                store.write_status({"status": "running", "boot_count": 1})
                first = store.append_event({"event_type": "first"})
                second = store.append_event({"event_type": "second"})
                store.append_daily_snapshot(
                    {"snapshot_date": "2026-08-13", "status": {}}
                )
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual((directory / "status.json").stat().st_mode & 0o777, 0o600)
            lines = [
                json.loads(line)
                for line in (directory / "events.jsonl").read_text().splitlines()
            ]
            self.assertEqual(lines[0]["content_hash_sha256"], first)
            self.assertEqual(lines[1]["previous_hash_sha256"], first)
            self.assertEqual(lines[1]["content_hash_sha256"], second)

    def test_partial_tail_is_recovered_but_committed_tamper_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "state"
            with SupervisorStateStore(directory, CAMPAIGN_ID) as store:
                store.append_event({"event_type": "first"})
            with (directory / "events.jsonl").open("ab") as handle:
                handle.write(b'{"partial":')
            with SupervisorStateStore(directory, CAMPAIGN_ID):
                pass
            self.assertTrue((directory / "events.jsonl").read_bytes().endswith(b"\n"))
            record = json.loads((directory / "events.jsonl").read_text())
            record["payload"]["event_type"] = "tampered"
            (directory / "events.jsonl").write_text(json.dumps(record) + "\n")
            with (
                self.assertRaisesRegex(CampaignSupervisorError, "chain"),
                SupervisorStateStore(directory, CAMPAIGN_ID),
            ):
                pass

    def test_second_supervisor_cannot_own_the_same_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "state"
            with (
                SupervisorStateStore(directory, CAMPAIGN_ID),
                self.assertRaisesRegex(CampaignSupervisorError, "already owns"),
                SupervisorStateStore(directory, CAMPAIGN_ID),
            ):
                pass


class CampaignSupervisorTests(unittest.TestCase):
    def _config(self, directory: Path, **changes: object) -> CampaignSupervisorConfig:
        values: dict[str, object] = {
            "campaign_id": CAMPAIGN_ID,
            "state_directory": directory,
            "poll_seconds": 1.0,
            "cycle_timeout_seconds": 30.0,
            "command_retries": 2,
            "retry_backoff_seconds": 0.1,
            "limit": 120,
            "process_manager": "none",
        }
        values.update(changes)
        return CampaignSupervisorConfig(**values)  # type: ignore[arg-type]

    def test_runs_btc_and_eth_and_writes_one_daily_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            complete = _progress(
                scopes=(
                    _scope(
                        "BTC/USD:240m",
                        sequence_no=1,
                        next_expected_at=NOW + timedelta(minutes=5),
                    ),
                    _scope(
                        "ETH/USD:240m",
                        sequence_no=1,
                        next_expected_at=NOW + timedelta(minutes=5),
                    ),
                )
            )
            reader = _ProgressReader((_progress(), complete))
            runner = _Runner(
                (
                    CycleCommandResult(
                        "BTC/USD:240m", 0, "recorded", "success", sequence_no=1
                    ),
                    CycleCommandResult(
                        "ETH/USD:240m", 0, "recorded", "success", sequence_no=1
                    ),
                )
            )
            supervisor = CampaignSupervisor(
                self._config(directory),
                reader,
                runner,
                NullServiceManager(),
                SupervisorStateStore(directory, CAMPAIGN_ID),
                clock=lambda: NOW,
            )
            supervisor.run(threading.Event(), once=True)
            self.assertEqual(
                [item[1] for item in runner.cycle_calls],
                ["BTC/USD:240m", "ETH/USD:240m"],
            )
            status = json.loads((directory / "status.json").read_text())
            self.assertEqual(len(status["scopes"]), 2)
            self.assertEqual(
                len((directory / "daily-snapshots.jsonl").read_text().splitlines()),
                1,
            )

    def test_restart_increments_boot_without_duplicate_daily_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            progress = _progress(
                scopes=(
                    _scope(
                        "BTC/USD:240m", next_expected_at=NOW + timedelta(minutes=5)
                    ),
                )
            )
            for _ in range(2):
                CampaignSupervisor(
                    self._config(directory),
                    _ProgressReader((progress,)),
                    _Runner(()),
                    NullServiceManager(),
                    SupervisorStateStore(directory, CAMPAIGN_ID),
                    clock=lambda: NOW,
                ).run(threading.Event(), once=True)
            status = json.loads((directory / "status.json").read_text())
            self.assertEqual(status["boot_count"], 2)
            self.assertEqual(
                len((directory / "daily-snapshots.jsonl").read_text().splitlines()),
                1,
            )

    def test_twenty_eight_day_restart_simulation_keeps_one_snapshot_per_day(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            for day in range(28):
                current = NOW + timedelta(days=day)
                progress = CampaignProgress(
                    campaign_id=CAMPAIGN_ID,
                    database_now=current,
                    started_at=NOW,
                    planned_ends_at=NOW + timedelta(days=28),
                    cycle_interval_seconds=300,
                    scopes=(
                        _scope(
                            "BTC/USD:240m",
                            next_expected_at=current + timedelta(minutes=5),
                        ),
                    ),
                    final_report_stored=False,
                )
                CampaignSupervisor(
                    self._config(directory),
                    _ProgressReader((progress,)),
                    _Runner(()),
                    NullServiceManager(),
                    SupervisorStateStore(directory, CAMPAIGN_ID),
                    clock=lambda current=current: current,
                ).run(threading.Event(), once=True)
            snapshots = [
                json.loads(line)
                for line in (directory / "daily-snapshots.jsonl")
                .read_text()
                .splitlines()
            ]
            self.assertEqual(len(snapshots), 28)
            self.assertEqual(len({item["content_hash_sha256"] for item in snapshots}), 28)
            status = json.loads((directory / "status.json").read_text())
            self.assertEqual(status["boot_count"], 28)

    def test_hung_cycle_is_retried_without_duplicate_scope_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            complete = _progress(
                scopes=(
                    _scope(
                        "BTC/USD:240m",
                        sequence_no=1,
                        next_expected_at=NOW + timedelta(minutes=5),
                    ),
                )
            )
            runner = _Runner(
                (
                    CycleCommandResult(
                        "BTC/USD:240m",
                        124,
                        "error",
                        error_code="OBSERVATION_CYCLE_TIMEOUT",
                        timed_out=True,
                    ),
                    CycleCommandResult(
                        "BTC/USD:240m", 0, "recorded", "success", sequence_no=1
                    ),
                )
            )
            sleeps: list[float] = []
            CampaignSupervisor(
                self._config(directory),
                _ProgressReader(
                    (
                        _progress(scopes=(_scope("BTC/USD:240m"),)),
                        complete,
                    )
                ),
                runner,
                NullServiceManager(),
                SupervisorStateStore(directory, CAMPAIGN_ID),
                clock=lambda: NOW,
                sleeper=sleeps.append,
            ).run(threading.Event(), once=True)
            self.assertEqual(len(runner.cycle_calls), 2)
            self.assertEqual(sleeps, [0.1])
            status = json.loads((directory / "status.json").read_text())
            self.assertEqual(status["stuck_cycles_detected"], 1)

    def test_inactive_services_block_cycles_and_record_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            CampaignSupervisor(
                self._config(directory, process_manager="systemd"),
                _ProgressReader((_progress(),)),
                _Runner(()),
                _ServiceManager(
                    (
                        ServiceStatus(
                            "bridge",
                            "crypto-agent-t4-bridge.service",
                            False,
                            True,
                            False,
                        ),
                    )
                ),
                SupervisorStateStore(directory, CAMPAIGN_ID),
                clock=lambda: NOW,
            ).run(threading.Event(), once=True)
            status = json.loads((directory / "status.json").read_text())
            self.assertEqual(status["service_restart_requests"], 1)
            self.assertEqual(status["last_cycle_results"], [])

    def test_missed_cycles_are_detected_and_final_report_is_automatic(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            ended = _progress(
                now=NOW,
                planned_ends_at=NOW - timedelta(minutes=10),
                scopes=(
                    _scope(
                        "BTC/USD:240m",
                        sequence_no=9,
                        next_expected_at=NOW - timedelta(minutes=5),
                        missed=2,
                    ),
                ),
            )
            stored = _progress(
                now=NOW,
                planned_ends_at=NOW - timedelta(minutes=10),
                scopes=ended.scopes,
                report=True,
            )
            runner = _Runner(
                (), ReportCommandResult(3, "stored", v1_gate_passed=False)
            )
            CampaignSupervisor(
                self._config(directory),
                _ProgressReader((ended, stored)),
                runner,
                NullServiceManager(),
                SupervisorStateStore(directory, CAMPAIGN_ID),
                clock=lambda: NOW,
            ).run(threading.Event(), once=True)
            self.assertEqual(runner.report_calls, [(CAMPAIGN_ID, 30.0)])
            status = json.loads((directory / "status.json").read_text())
            self.assertTrue(status["final_report_stored"])
            events = (directory / "events.jsonl").read_text()
            self.assertIn("missed_cycles_detected", events)
            self.assertIn("final_report_attempted", events)


class PostgresProgressReaderTests(unittest.TestCase):
    def test_reads_frozen_schedule_and_cycle_counts(self) -> None:
        connection = FakeConnection(
            [
                SQLStep("SET TRANSACTION READ ONLY"),
                SQLStep(
                    "FROM crypto_agent.t4_observation_campaigns campaign",
                    [
                        (
                            NOW - timedelta(days=1),
                            NOW + timedelta(days=27),
                            300,
                            {"BTC/USD:240m": {}, "ETH/USD:240m": {}},
                            NOW,
                            False,
                        )
                    ],
                ),
                SQLStep(
                    "WITH latest AS",
                    [
                        (
                            "BTC/USD:240m",
                            4,
                            NOW - timedelta(minutes=5),
                            NOW - timedelta(minutes=4),
                            "success",
                            3,
                            0,
                            1,
                        )
                    ],
                ),
            ]
        )
        progress = PostgresCampaignProgressReader(lambda: connection).load_progress(
            CAMPAIGN_ID
        )
        self.assertEqual(len(progress.scopes), 2)
        btc = next(item for item in progress.scopes if item.scope_key.startswith("BTC"))
        self.assertEqual(btc.sequence_no, 4)
        self.assertEqual(btc.missed_cycles, 1)
        eth = next(item for item in progress.scopes if item.scope_key.startswith("ETH"))
        self.assertEqual(eth.sequence_no, 0)
        self.assertTrue(connection.committed)


if __name__ == "__main__":
    unittest.main()

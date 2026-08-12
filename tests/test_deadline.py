from __future__ import annotations

import os
import sqlite3
import threading
import time
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from crypto_agent.deadline import (
    AnalysisDeadlineExceeded,
    analysis_deadline_scope,
    bounded_analysis_timeout,
    ensure_analysis_deadline,
)
from crypto_agent.factory import build_orchestrator
from crypto_agent.orchestrator import ResearchOrchestrator, _canonical_input
from crypto_agent.policy import RiskPolicy
from crypto_agent.postgres import (
    PostgresOperationError,
    PostgresSettings,
    PostgresUnavailableError,
    PsycopgConnectionFactory,
    transaction,
)
from crypto_agent.providers.base import ProviderError
from crypto_agent.providers.synthetic import SyntheticProvider
from crypto_agent.providers.t4 import Plus500T4Provider
from crypto_agent.storage import ReportRepository


class _LateSyntheticProvider:
    source_id = SyntheticProvider.source_id

    def fetch_candles(self, **kwargs):  # type: ignore[no-untyped-def]
        time.sleep(0.02)
        return SyntheticProvider().fetch_candles(**kwargs)


class _RecordingRepository:
    def __init__(self) -> None:
        self.saved = False

    def save(self, report, *, input_snapshot):  # type: ignore[no-untyped-def]
        del report, input_snapshot
        self.saved = True


class _BlockingOpener:
    def __init__(self) -> None:
        self.timeout: float | None = None

    def open(self, request, timeout):  # type: ignore[no-untyped-def]
        del request
        self.timeout = float(timeout)
        raise OSError("bounded test socket")


class _RecordingDriver:
    def __init__(self) -> None:
        self.connect_timeout: int | None = None

    def connect(self, dsn, *, connect_timeout, autocommit):  # type: ignore[no-untyped-def]
        del dsn, autocommit
        self.connect_timeout = int(connect_timeout)
        raise OSError("bounded test database")


class _PlainCommitConnection:
    def __init__(self) -> None:
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


class _CancelableCommitConnection(_PlainCommitConnection):
    def __init__(self) -> None:
        super().__init__()
        self.cancel_called = False
        self._cancelled = threading.Event()

    def commit(self) -> None:
        if self._cancelled.wait(timeout=1.0):
            raise RuntimeError("commit cancelled")
        self.committed = True

    def cancel(self) -> None:
        self.cancel_called = True
        self._cancelled.set()


class _CancelBeforeCommitConnection(_PlainCommitConnection):
    """Simulate cancellation landing before the driver starts its COMMIT I/O."""

    def __init__(self) -> None:
        super().__init__()
        self.cancel_calls = 0
        self._first_cancel = threading.Event()
        self._commit_io_started = threading.Event()
        self._cancelled = threading.Event()

    def commit(self) -> None:
        if not self._first_cancel.wait(timeout=1.0):
            self.committed = True
            return
        self._commit_io_started.set()
        if self._cancelled.wait(timeout=1.0):
            raise RuntimeError("commit cancelled after retry")
        self.committed = True

    def cancel(self) -> None:
        self.cancel_calls += 1
        if self._commit_io_started.is_set():
            self._cancelled.set()
        else:
            self._first_cancel.set()


class AnalysisDeadlineTests(unittest.TestCase):
    def test_blocking_timeout_is_bounded_by_remaining_budget(self) -> None:
        with analysis_deadline_scope(time.monotonic() + 1.0):
            bounded = bounded_analysis_timeout(10.0)
        self.assertGreater(bounded, 0)
        self.assertLessEqual(bounded, 1.0)

    def test_expired_budget_raises_machine_specific_timeout(self) -> None:
        with (
            self.assertRaises(AnalysisDeadlineExceeded),
            analysis_deadline_scope(time.monotonic() - 1.0),
        ):
            ensure_analysis_deadline()

    def test_t4_does_not_open_socket_after_budget_expires(self) -> None:
        with (
            patch("crypto_agent.providers.t4.build_opener") as mocked_opener,
            self.assertRaises(AnalysisDeadlineExceeded),
            analysis_deadline_scope(time.monotonic() + 0.001),
        ):
            time.sleep(0.01)
            Plus500T4Provider(bridge_token="t" * 32).fetch_candles(
                symbol="BTC/USD",
                interval_minutes=1_440,
                as_of=datetime(2026, 8, 10, tzinfo=UTC),
                limit=120,
            )
        mocked_opener.assert_not_called()

    def test_t4_socket_timeout_is_bounded_by_remaining_budget(self) -> None:
        opener = _BlockingOpener()
        with (
            patch("crypto_agent.providers.t4.build_opener", return_value=opener),
            analysis_deadline_scope(time.monotonic() + 1.5),
            self.assertRaises(ProviderError),
        ):
            Plus500T4Provider(
                bridge_token="t" * 32,
                timeout_seconds=10,
            ).fetch_candles(
                symbol="BTC/USD",
                interval_minutes=1_440,
                as_of=datetime(2026, 8, 10, tzinfo=UTC),
                limit=120,
            )
        self.assertIsNotNone(opener.timeout)
        assert opener.timeout is not None
        self.assertGreater(opener.timeout, 0)
        self.assertLessEqual(opener.timeout, 1.5)

    def test_postgres_connect_timeout_is_bounded_by_remaining_budget(self) -> None:
        driver = _RecordingDriver()
        with (
            patch("crypto_agent.postgres.importlib.import_module", return_value=driver),
            analysis_deadline_scope(time.monotonic() + 1.5),
            self.assertRaises(PostgresUnavailableError),
        ):
            PsycopgConnectionFactory(PostgresSettings(dsn="opaque", connect_timeout_seconds=5))()
        self.assertEqual(driver.connect_timeout, 1)

    def test_postgres_does_not_start_a_connect_past_subsecond_budget(self) -> None:
        driver = _RecordingDriver()
        with (
            patch("crypto_agent.postgres.importlib.import_module", return_value=driver),
            analysis_deadline_scope(time.monotonic() + 0.5),
            self.assertRaises(TimeoutError),
        ):
            PsycopgConnectionFactory(PostgresSettings(dsn="opaque", connect_timeout_seconds=5))()
        self.assertIsNone(driver.connect_timeout)

    def test_postgres_commit_is_cancelled_at_the_analysis_deadline(self) -> None:
        connection = _CancelableCommitConnection()
        started = time.monotonic()

        with (
            self.assertRaises(AnalysisDeadlineExceeded),
            analysis_deadline_scope(time.monotonic() + 0.05),
            transaction(lambda: connection),  # type: ignore[arg-type]
        ):
            pass

        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 0.5)
        self.assertTrue(connection.cancel_called)
        self.assertFalse(connection.committed)
        self.assertTrue(connection.rolled_back)
        self.assertTrue(connection.closed)

    def test_postgres_commit_fails_closed_without_cancellation_support(self) -> None:
        connection = _PlainCommitConnection()

        with (
            self.assertRaises(PostgresOperationError),
            analysis_deadline_scope(time.monotonic() + 1.0),
            transaction(lambda: connection),  # type: ignore[arg-type]
        ):
            pass

        self.assertFalse(connection.committed)
        self.assertTrue(connection.rolled_back)
        self.assertTrue(connection.closed)

    def test_postgres_deadline_retries_cancellation_when_cancel_precedes_commit(
        self,
    ) -> None:
        connection = _CancelBeforeCommitConnection()
        started = time.monotonic()

        with (
            self.assertRaises(AnalysisDeadlineExceeded),
            analysis_deadline_scope(time.monotonic() + 0.05),
            transaction(lambda: connection),  # type: ignore[arg-type]
        ):
            pass

        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 0.5)
        self.assertGreaterEqual(connection.cancel_calls, 2)
        self.assertFalse(connection.committed)
        self.assertTrue(connection.closed)

    def test_postgres_commit_without_deadline_keeps_supporting_plain_connections(
        self,
    ) -> None:
        connection = _PlainCommitConnection()

        with transaction(lambda: connection):  # type: ignore[arg-type]
            pass

        self.assertTrue(connection.committed)
        self.assertFalse(connection.rolled_back)
        self.assertTrue(connection.closed)

    def test_expired_worker_cannot_persist_a_late_sqlite_report(self) -> None:
        repository = _RecordingRepository()
        orchestrator = ResearchOrchestrator(
            provider=_LateSyntheticProvider(),
            policy=RiskPolicy.load(Path("configs/risk_policy.v1.json")),
            repository=repository,  # type: ignore[arg-type]
        )
        with (
            self.assertRaises(AnalysisDeadlineExceeded),
            analysis_deadline_scope(time.monotonic() + 0.005),
        ):
            orchestrator.analyze(
                symbol="BTC/USD",
                interval_minutes=1_440,
                as_of=datetime(2026, 8, 10, tzinfo=UTC),
            )
        self.assertFalse(repository.saved)

    def test_sqlite_writer_lock_cannot_commit_after_deadline(self) -> None:
        selected_policy = RiskPolicy.load(Path("configs/risk_policy.v1.json"))
        provider = SyntheticProvider()
        as_of = datetime(2026, 8, 10, tzinfo=UTC)
        report = ResearchOrchestrator(
            provider=provider,
            policy=selected_policy,
        ).analyze(
            symbol="BTC/USD",
            interval_minutes=1_440,
            as_of=as_of,
        )
        snapshot = _canonical_input(
            provider.fetch_candles(
                symbol="BTC/USD",
                interval_minutes=1_440,
                as_of=as_of,
                limit=120,
            )
        )
        snapshot.append(
            {
                "record_type": "analysis_context",
                "as_of": report.as_of.isoformat(),
                "futures_policy_id": report.metadata["futures_policy_id"],
                "futures_policy_hash_sha256": report.metadata["futures_policy_hash_sha256"],
            }
        )

        with TemporaryDirectory() as directory:
            database_path = Path(directory) / "reports.db"
            repository = ReportRepository(database_path)
            blocker = sqlite3.connect(database_path, timeout=0)
            blocker.execute("BEGIN IMMEDIATE")
            try:
                with (
                    self.assertRaises(AnalysisDeadlineExceeded),
                    analysis_deadline_scope(time.monotonic() + 0.05),
                ):
                    repository.save(report, input_snapshot=snapshot)
            finally:
                blocker.rollback()
                blocker.close()

            with repository.connect() as connection:
                report_count = connection.execute(
                    "SELECT COUNT(*) FROM research_reports"
                ).fetchone()[0]
                snapshot_count = connection.execute(
                    "SELECT COUNT(*) FROM input_snapshots"
                ).fetchone()[0]
            self.assertEqual(report_count, 0)
            self.assertEqual(snapshot_count, 0)

    def test_orchestrator_build_is_bounded_by_the_same_deadline(self) -> None:
        with TemporaryDirectory() as directory:
            database_path = Path(directory) / "reports.db"
            blocker = sqlite3.connect(database_path, timeout=0)
            blocker.execute("BEGIN EXCLUSIVE")
            try:
                with patch.dict(
                    os.environ,
                    {
                        "CRYPTO_AGENT_ENV": "development",
                        "CRYPTO_AGENT_DATABASE_PATH": str(database_path),
                    },
                    clear=True,
                ):
                    started = time.monotonic()
                    with (
                        self.assertRaises(AnalysisDeadlineExceeded),
                        analysis_deadline_scope(time.monotonic() + 0.05),
                    ):
                        build_orchestrator("synthetic")
                    elapsed = time.monotonic() - started
            finally:
                blocker.rollback()
                blocker.close()

        self.assertLess(elapsed, 0.5)


if __name__ == "__main__":
    unittest.main()

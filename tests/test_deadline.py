from __future__ import annotations

import os
import sqlite3
import time
import unittest
from datetime import datetime, timezone
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
from crypto_agent.providers.t4 import Plus500T4Provider
from crypto_agent.providers.synthetic import SyntheticProvider
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


class AnalysisDeadlineTests(unittest.TestCase):
    def test_blocking_timeout_is_bounded_by_remaining_budget(self) -> None:
        with analysis_deadline_scope(time.monotonic() + 1.0):
            bounded = bounded_analysis_timeout(10.0)
        self.assertGreater(bounded, 0)
        self.assertLessEqual(bounded, 1.0)

    def test_expired_budget_raises_machine_specific_timeout(self) -> None:
        with self.assertRaises(AnalysisDeadlineExceeded):
            with analysis_deadline_scope(time.monotonic() - 1.0):
                ensure_analysis_deadline()

    def test_t4_does_not_open_socket_after_budget_expires(self) -> None:
        with patch("crypto_agent.providers.t4.build_opener") as mocked_opener:
            with self.assertRaises(AnalysisDeadlineExceeded):
                with analysis_deadline_scope(time.monotonic() + 0.001):
                    time.sleep(0.01)
                    Plus500T4Provider(
                        bridge_token="t" * 32
                    ).fetch_candles(
                        symbol="BTC/USD",
                        interval_minutes=1_440,
                        as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
                        limit=120,
                    )
        mocked_opener.assert_not_called()

    def test_expired_worker_cannot_persist_a_late_sqlite_report(self) -> None:
        repository = _RecordingRepository()
        orchestrator = ResearchOrchestrator(
            provider=_LateSyntheticProvider(),
            policy=RiskPolicy.load(Path("configs/risk_policy.v1.json")),
            repository=repository,  # type: ignore[arg-type]
        )
        with self.assertRaises(AnalysisDeadlineExceeded):
            with analysis_deadline_scope(time.monotonic() + 0.005):
                orchestrator.analyze(
                    symbol="BTC/USD",
                    interval_minutes=1_440,
                    as_of=datetime(2026, 8, 10, tzinfo=timezone.utc),
                )
        self.assertFalse(repository.saved)

    def test_sqlite_writer_lock_cannot_commit_after_deadline(self) -> None:
        selected_policy = RiskPolicy.load(Path("configs/risk_policy.v1.json"))
        provider = SyntheticProvider()
        as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
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

        with TemporaryDirectory() as directory:
            database_path = Path(directory) / "reports.db"
            repository = ReportRepository(database_path)
            blocker = sqlite3.connect(database_path, timeout=0)
            blocker.execute("BEGIN IMMEDIATE")
            try:
                with self.assertRaises(AnalysisDeadlineExceeded):
                    with analysis_deadline_scope(time.monotonic() + 0.05):
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
                    with self.assertRaises(AnalysisDeadlineExceeded):
                        with analysis_deadline_scope(time.monotonic() + 0.05):
                            build_orchestrator("synthetic")
                    elapsed = time.monotonic() - started
            finally:
                blocker.rollback()
                blocker.close()

        self.assertLess(elapsed, 0.5)


if __name__ == "__main__":
    unittest.main()

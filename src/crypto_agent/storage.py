from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from pathlib import Path
from typing import Any

from .domain import ResearchReport
from .deadline import (
    AnalysisDeadlineExceeded,
    bounded_analysis_timeout,
    ensure_analysis_deadline,
)


SQLITE_SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS input_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    fingerprint_sha256 TEXT NOT NULL UNIQUE,
    snapshot_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS research_reports (
    decision_id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    horizon TEXT NOT NULL,
    as_of TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('NO_SIGNAL', 'ALERT')),
    policy_id TEXT NOT NULL,
    data_quality REAL NOT NULL CHECK (data_quality >= 0 AND data_quality <= 1),
    input_snapshot_id TEXT NOT NULL REFERENCES input_snapshots(snapshot_id),
    report_hash_sha256 TEXT NOT NULL,
    report_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_research_reports_asset_horizon_asof
ON research_reports (asset_id, horizon, as_of DESC);

CREATE TRIGGER IF NOT EXISTS input_snapshots_no_update
BEFORE UPDATE ON input_snapshots BEGIN
    SELECT RAISE(ABORT, 'input_snapshots is append-only');
END;
CREATE TRIGGER IF NOT EXISTS input_snapshots_no_delete
BEFORE DELETE ON input_snapshots BEGIN
    SELECT RAISE(ABORT, 'input_snapshots is append-only');
END;
CREATE TRIGGER IF NOT EXISTS research_reports_no_update
BEFORE UPDATE ON research_reports BEGIN
    SELECT RAISE(ABORT, 'research_reports is append-only');
END;
CREATE TRIGGER IF NOT EXISTS research_reports_no_delete
BEFORE DELETE ON research_reports BEGIN
    SELECT RAISE(ABORT, 'research_reports is append-only');
END;
"""


class ReportRepository:
    _DEFAULT_SQLITE_TIMEOUT_SECONDS = 5.0

    def __init__(self, database_path: str | Path) -> None:
        self.path = Path(database_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def connect(
        self,
        *,
        timeout_seconds: float = _DEFAULT_SQLITE_TIMEOUT_SECONDS,
    ) -> sqlite3.Connection:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a positive finite number")
        connection = sqlite3.connect(self.path, timeout=float(timeout_seconds))
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        # sqlite3's busy timeout is the only cancellable boundary while waiting
        # for a writer lock. Bound it by the analysis deadline so an API 504
        # cannot be followed by a late append-only report commit.
        busy_timeout_ms = max(1, math.ceil(float(timeout_seconds) * 1_000))
        connection.execute(f"PRAGMA busy_timeout = {busy_timeout_ms}")
        return connection

    def initialize(self) -> None:
        ensure_analysis_deadline()
        timeout_seconds = bounded_analysis_timeout(
            self._DEFAULT_SQLITE_TIMEOUT_SECONDS
        )
        deadline_bounded = timeout_seconds < self._DEFAULT_SQLITE_TIMEOUT_SECONDS
        try:
            with self.connect(timeout_seconds=timeout_seconds) as connection:
                connection.executescript(SQLITE_SCHEMA)
                ensure_analysis_deadline()
        except sqlite3.OperationalError as exc:
            if deadline_bounded and _is_sqlite_lock_error(exc):
                raise AnalysisDeadlineExceeded(
                    "Read-only analysis exceeded its total deadline while "
                    "initializing durable report storage"
                ) from exc
            raise RuntimeError("Local report storage is unavailable") from None

    def save(
        self,
        report: ResearchReport,
        *,
        input_snapshot: list[dict[str, object]],
    ) -> None:
        ensure_analysis_deadline()
        snapshot_json = _canonical_json(input_snapshot)
        snapshot_hash = hashlib.sha256(snapshot_json.encode("utf-8")).hexdigest()
        expected_snapshot_id = f"sha256:{snapshot_hash}"
        if report.data_snapshot_id != expected_snapshot_id:
            raise ValueError("Report snapshot id does not match persisted input bytes")

        report_json = _canonical_json(report.to_dict())
        report_hash = hashlib.sha256(report_json.encode("utf-8")).hexdigest()
        timeout_seconds = bounded_analysis_timeout(
            self._DEFAULT_SQLITE_TIMEOUT_SECONDS
        )
        deadline_bounded = timeout_seconds < self._DEFAULT_SQLITE_TIMEOUT_SECONDS
        try:
            with self.connect(timeout_seconds=timeout_seconds) as connection:
                # Acquire the write lock before mutating either table. If the
                # lock arrives after the budget, the following check rolls the
                # transaction back without leaving a snapshot or report.
                connection.execute("BEGIN IMMEDIATE")
                ensure_analysis_deadline()
                connection.execute(
                    """
                    INSERT INTO input_snapshots (
                        snapshot_id, fingerprint_sha256, snapshot_json
                    ) VALUES (?, ?, ?)
                    ON CONFLICT(snapshot_id) DO NOTHING
                    """,
                    (expected_snapshot_id, snapshot_hash, snapshot_json),
                )
                connection.execute(
                    """
                    INSERT INTO research_reports (
                        decision_id, trace_id, asset_id, instrument_id, horizon, as_of,
                        expires_at, decision, policy_id, data_quality, input_snapshot_id,
                        report_hash_sha256, report_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report.decision_id,
                        report.trace_id,
                        report.asset_id,
                        report.instrument_id,
                        report.horizon,
                        report.as_of.isoformat(),
                        report.expires_at.isoformat(),
                        report.decision.value,
                        report.risk.policy_id,
                        report.data_quality.score,
                        report.data_snapshot_id,
                        report_hash,
                        report_json,
                    ),
                )
                ensure_analysis_deadline()
                connection.commit()
        except sqlite3.OperationalError as exc:
            if deadline_bounded and _is_sqlite_lock_error(exc):
                raise AnalysisDeadlineExceeded(
                    "Read-only analysis exceeded its total deadline while waiting "
                    "for durable report storage"
                ) from exc
            raise RuntimeError("Local report storage is unavailable") from None

    def latest(self, asset_id: str, horizon: str | None = None) -> dict[str, Any] | None:
        with self.connect() as connection:
            if horizon is None:
                row = connection.execute(
                    """
                    SELECT report_json FROM research_reports
                    WHERE asset_id = ?
                    ORDER BY as_of DESC, created_at DESC LIMIT 1
                    """,
                    (asset_id,),
                ).fetchone()
            else:
                row = connection.execute(
                    """
                    SELECT report_json FROM research_reports
                    WHERE asset_id = ? AND horizon = ?
                    ORDER BY as_of DESC, created_at DESC LIMIT 1
                    """,
                    (asset_id, horizon),
                ).fetchone()
        return None if row is None else json.loads(row["report_json"])

    def read_snapshot(self, snapshot_id: str) -> list[dict[str, object]] | None:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT snapshot_json FROM input_snapshots WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchone()
        return None if row is None else json.loads(row["snapshot_json"])


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def _is_sqlite_lock_error(exc: sqlite3.OperationalError) -> bool:
    return bool(
        getattr(exc, "sqlite_errorcode", None) == sqlite3.SQLITE_BUSY
        or "locked" in str(exc).lower()
    )

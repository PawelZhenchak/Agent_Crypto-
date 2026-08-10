from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field


@dataclass(slots=True)
class SQLStep:
    contains: str
    rows: list[object] = field(default_factory=list)
    error: Exception | None = None


class ScriptedCursor:
    def __init__(self, steps: Sequence[SQLStep]) -> None:
        self.steps = list(steps)
        self.current_rows: list[object] = []
        self.executions: list[tuple[str, object | None]] = []
        self.closed = False

    def execute(self, query: str, params: object | None = None) -> object:
        if not self.steps:
            raise AssertionError(f"Unexpected SQL: {query}")
        step = self.steps.pop(0)
        normalized = " ".join(query.split())
        if step.contains not in normalized:
            raise AssertionError(f"Expected SQL containing {step.contains!r}, got {normalized!r}")
        self.executions.append((normalized, params))
        self.current_rows = list(step.rows)
        if step.error is not None:
            raise step.error
        return self

    def fetchone(self) -> object | None:
        if not self.current_rows:
            return None
        return self.current_rows.pop(0)

    def fetchall(self) -> list[object]:
        rows = self.current_rows
        self.current_rows = []
        return rows

    def close(self) -> None:
        self.closed = True


class FakeConnection:
    def __init__(self, steps: Sequence[SQLStep]) -> None:
        self.scripted_cursor = ScriptedCursor(steps)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> ScriptedCursor:
        return self.scripted_cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        self.closed = True


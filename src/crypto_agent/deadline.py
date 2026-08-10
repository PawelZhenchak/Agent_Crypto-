from __future__ import annotations

import math
import time
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token


class AnalysisDeadlineExceeded(TimeoutError):
    """The read-only analysis exhausted its monotonic wall-clock budget."""


_analysis_deadline: ContextVar[float | None] = ContextVar(
    "crypto_agent_analysis_deadline",
    default=None,
)


@contextmanager
def analysis_deadline_scope(deadline_monotonic: float) -> Iterator[None]:
    if (
        isinstance(deadline_monotonic, bool)
        or not isinstance(deadline_monotonic, (int, float))
        or not math.isfinite(deadline_monotonic)
    ):
        raise ValueError("deadline_monotonic must be a finite monotonic timestamp")
    token: Token[float | None] = _analysis_deadline.set(float(deadline_monotonic))
    try:
        ensure_analysis_deadline()
        yield
    finally:
        _analysis_deadline.reset(token)


def ensure_analysis_deadline() -> None:
    deadline = _analysis_deadline.get()
    if deadline is not None and time.monotonic() >= deadline:
        raise AnalysisDeadlineExceeded(
            "Read-only analysis exceeded its total deadline"
        )


def bounded_analysis_timeout(configured_seconds: float) -> float:
    """Bound one blocking operation by the remaining total analysis budget."""

    if (
        isinstance(configured_seconds, bool)
        or not isinstance(configured_seconds, (int, float))
        or not math.isfinite(configured_seconds)
        or configured_seconds <= 0
    ):
        raise ValueError("configured_seconds must be a positive finite number")
    deadline = _analysis_deadline.get()
    if deadline is None:
        return float(configured_seconds)
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise AnalysisDeadlineExceeded(
            "Read-only analysis exceeded its total deadline"
        )
    return min(float(configured_seconds), remaining)

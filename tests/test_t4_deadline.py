from __future__ import annotations

import threading
import time
import unittest
from datetime import UTC, datetime
from email.message import Message
from unittest.mock import patch

from crypto_agent.deadline import AnalysisDeadlineExceeded, analysis_deadline_scope
from crypto_agent.providers.t4 import Plus500T4Provider


class _SlowResponse:
    def __init__(self) -> None:
        self.status = 200
        self.headers = Message()
        self.headers["Content-Type"] = "application/json"
        self.closed = threading.Event()
        self.read_finished = threading.Event()

    def read(self, size: int) -> bytes:
        del size
        try:
            if not self.closed.wait(timeout=2.0):
                raise AssertionError("response was not closed by the read deadline")
            raise OSError("response closed during read")
        finally:
            self.read_finished.set()

    def close(self) -> None:
        self.closed.set()


class _SlowOpener:
    def __init__(self, response: _SlowResponse) -> None:
        self.response = response

    def open(self, request, timeout):  # type: ignore[no-untyped-def]
        del request, timeout
        return self.response


class T4TotalReadDeadlineTests(unittest.TestCase):
    def test_total_deadline_closes_a_slow_response_and_preserves_timeout(self) -> None:
        response = _SlowResponse()
        started = time.monotonic()
        with (
            patch(
                "crypto_agent.providers.t4.build_opener",
                return_value=_SlowOpener(response),
            ),
            self.assertRaises(AnalysisDeadlineExceeded),
            analysis_deadline_scope(time.monotonic() + 0.05),
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

        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 0.5)
        self.assertTrue(response.closed.is_set())
        self.assertTrue(response.read_finished.is_set())


if __name__ == "__main__":
    unittest.main()

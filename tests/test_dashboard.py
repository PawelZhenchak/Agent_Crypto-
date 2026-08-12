from __future__ import annotations

import unittest

from crypto_agent.dashboard import render_dashboard


class DashboardTests(unittest.TestCase):
    def test_renders_static_read_only_sections(self) -> None:
        rendered = render_dashboard(
            {"status": "NOT_READY", "execution_enabled": False},
            [{"decision": "ALERT", "trace_id": "trace-1"}],
            [{"severity": "warning", "code": "T4_BRIDGE_UNAVAILABLE"}],
        )

        self.assertTrue(rendered.startswith("<!doctype html>"))
        self.assertIn("Podsumowanie", rendered)
        self.assertIn("Alerty", rendered)
        self.assertIn("Incydenty", rendered)
        self.assertIn("NOT_READY", rendered)
        self.assertIn("execution_enabled=false", rendered)
        self.assertIn("T4_BRIDGE_UNAVAILABLE", rendered)
        self.assertIn("Content-Security-Policy", rendered)

    def test_escapes_every_caller_provided_key_and_value(self) -> None:
        malicious_key = '<img src=x onerror="alert(1)">'
        malicious_value = '</style><script>alert("x")</script>'
        rendered = render_dashboard(
            {malicious_key: malicious_value},
            [{"title": malicious_value, "details": {"payload": malicious_key}}],
            [{malicious_key: malicious_value}],
        )

        self.assertNotIn(malicious_key, rendered)
        self.assertNotIn(malicious_value, rendered)
        self.assertNotIn("<script>", rendered.lower())
        self.assertNotIn("<img ", rendered.lower())
        self.assertIn("&lt;img src=x onerror=&quot;alert(1)&quot;&gt;", rendered)
        self.assertIn(
            "&lt;/style&gt;&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;",
            rendered,
        )

    def test_contains_no_active_or_remote_content(self) -> None:
        rendered = render_dashboard({}, [], []).lower()

        for forbidden in (
            "<script",
            "<a ",
            "<form",
            "<img",
            "<iframe",
            "<object",
            "href=",
            "src=",
            "action=",
            "innerhtml",
            "@import",
            "url(",
            "http://",
            "https://",
        ):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, rendered)

    def test_rejects_non_mapping_records(self) -> None:
        with self.assertRaises(TypeError):
            render_dashboard({}, ["not-a-record"], [])  # type: ignore[list-item]


if __name__ == "__main__":
    unittest.main()

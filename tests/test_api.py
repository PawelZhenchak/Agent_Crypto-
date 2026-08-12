from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi import HTTPException

from crypto_agent.api import (
    _dashboard_data,
    _security_headers,
    app,
    monitoring_trace,
)
from crypto_agent.api_config import analysis_timeout_seconds
from crypto_agent.postgres import PostgresUnavailableError


class ApiV11Tests(unittest.TestCase):
    def test_analysis_deadline_is_bounded(self) -> None:
        for value in ("nan", "inf", "0", "2", "121", "invalid"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"CRYPTO_AGENT_ANALYSIS_TIMEOUT_SECONDS": value},
                clear=False,
            ), self.assertRaises(ValueError):
                analysis_timeout_seconds()

        with patch.dict(
            os.environ,
            {"CRYPTO_AGENT_ANALYSIS_TIMEOUT_SECONDS": "30"},
            clear=False,
        ):
            self.assertEqual(analysis_timeout_seconds(), 30.0)

    def test_api_uses_threaded_deadline_and_postgres_readiness(self) -> None:
        source = Path("src/crypto_agent/api.py").read_text(encoding="utf-8")
        self.assertIn("asyncio.to_thread", source)
        self.assertIn("asyncio.wait_for", source)
        self.assertIn("analysis_deadline_scope", source)
        self.assertIn("deadline_monotonic", source)
        self.assertIn("_build_and_analyze_with_deadline", source)
        self.assertIn(
            "deadline_monotonic - ANALYSIS_TIMEOUT_RECORDING_RESERVE_SECONDS",
            source,
        )
        self.assertIn("failure_deadline_monotonic=deadline_monotonic", source)
        self.assertIn("timeout=timeout_seconds + 0.25", source)
        self.assertNotIn("orchestrator = build_orchestrator(provider)\n", source)
        self.assertIn("check_postgres_health", source)
        self.assertIn('"missing_seeds": list(database.missing_seeds)', source)
        self.assertIn('"missing_triggers": list(database.missing_triggers)', source)
        self.assertIn("MIGRATIONS_PENDING", Path("src/crypto_agent/postgres.py").read_text())

    def test_api_disables_interactive_docs_and_accepts_loopback_hosts_only(self) -> None:
        source = Path("src/crypto_agent/api.py").read_text(encoding="utf-8")

        self.assertIn("docs_url=None", source)
        self.assertIn("redoc_url=None", source)
        self.assertIn("openapi_url=None", source)
        self.assertIn("TrustedHostMiddleware", source)
        self.assertIn(
            'allowed_hosts=["127.0.0.1", "localhost", "[::1]", "testserver"]',
            source,
        )
        self.assertIn('client_host not in {"127.0.0.1", "::1", "testclient"}', source)
        self.assertIn('"detail": "LOOPBACK_REQUIRED"', source)

    def test_api_sets_no_store_and_browser_security_headers(self) -> None:
        headers = _security_headers()

        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Referrer-Policy"], "no-referrer")
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(headers["X-Frame-Options"], "DENY")
        csp = headers["Content-Security-Policy"]
        for directive in (
            "default-src 'none'",
            "script-src 'none'",
            "connect-src 'none'",
            "frame-ancestors 'none'",
            "base-uri 'none'",
            "form-action 'none'",
        ):
            with self.subTest(directive=directive):
                self.assertIn(directive, csp)

    def test_monitoring_endpoints_are_read_only_and_do_not_expose_exceptions(self) -> None:
        source = Path("src/crypto_agent/api.py").read_text(encoding="utf-8")

        for route in (
            '@app.get("/v1/monitoring/summary")',
            '@app.get("/v1/monitoring/alerts")',
            '@app.get("/v1/monitoring/incidents")',
            '@app.get("/v1/monitoring/traces/{trace_id}")',
            '@app.get("/dashboard", response_class=HTMLResponse)',
        ):
            with self.subTest(route=route):
                self.assertIn(route, source)
        monitoring_paths = (
            "/v1/monitoring/summary",
            "/v1/monitoring/alerts",
            "/v1/monitoring/incidents",
            "/v1/monitoring/traces/{trace_id}",
            "/dashboard",
        )
        for path in monitoring_paths:
            for method in ("post", "put", "patch", "delete"):
                with self.subTest(path=path, method=method):
                    self.assertNotIn(f'@app.{method}("{path}"', source)
        self.assertIn('headers={"Cache-Control": "no-store"}', source)
        self.assertIn("render_dashboard(data.summary, data.alerts, data.incidents)", source)
        self.assertNotIn("str(exc)", source)

    def test_postgres_factory_failures_map_to_stable_monitoring_503(self) -> None:
        secret = "opaque-api-postgres-secret-marker"
        for call in (
            lambda: _dashboard_data(limit=1),
            lambda: monitoring_trace("7bf3c831-ae63-4d32-b750-1d694c1de236"),
        ):
            with self.subTest(call=call):
                with (
                    patch(
                        "crypto_agent.api.build_monitoring_repository",
                        side_effect=PostgresUnavailableError(secret),
                    ),
                    self.assertRaises(HTTPException) as raised,
                ):
                    call()

                self.assertEqual(raised.exception.status_code, 503)
                self.assertEqual(raised.exception.detail, "MONITORING_UNAVAILABLE")
                self.assertNotIn(secret, str(raised.exception.detail))

    def test_analysis_is_post_only_and_requires_explicit_request_header(self) -> None:
        routes = [
            route
            for route in app.routes
            if getattr(route, "path", None) == "/v1/analyze"
        ]
        self.assertEqual(len(routes), 1)
        route = routes[0]
        self.assertEqual(getattr(route, "methods", set()), {"POST"})

        dependant = route.dependant
        request_headers = [
            field
            for field in dependant.header_params
            if field.alias.lower() == "x-crypto-agent-request"
        ]
        self.assertEqual(len(request_headers), 1)
        self.assertTrue(request_headers[0].field_info.is_required())

        # The explicit non-simple header is the browser same-origin guard.  With
        # no CORS middleware, a cross-origin browser cannot preflight it.
        middleware_names = {
            middleware.cls.__name__ for middleware in app.user_middleware
        }
        self.assertNotIn("CORSMiddleware", middleware_names)

        source = Path("src/crypto_agent/api.py").read_text(encoding="utf-8")
        self.assertIn('Literal["analyze-v1"]', source)
        self.assertIn('alias="X-Crypto-Agent-Request"', source)
        self.assertNotIn('@app.get("/v1/analyze")', source)

if __name__ == "__main__":
    unittest.main()

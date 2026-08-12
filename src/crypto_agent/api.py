from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Literal, cast
from uuid import uuid4

from fastapi import FastAPI, Header, HTTPException, Query, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from . import __version__
from .api_config import analysis_timeout_seconds
from .dashboard import render_dashboard
from .deadline import (
    ANALYSIS_TIMEOUT_RECORDING_RESERVE_SECONDS,
    analysis_deadline_scope,
)
from .domain import ResearchReport
from .factory import build_monitoring_repository, build_orchestrator
from .monitoring import DashboardData, MonitoringError, run_monitored_analysis
from .narrator import NarrativeUnavailable, OpenAINarrator
from .postgres import PostgresError
from .resource_paths import default_migration_directory

API_VERSION = f"{__version__}-plus500-t4-v1"


app = FastAPI(
    title="Plus500 Futures T4 Research Agent",
    version=API_VERSION,
    description="Development-only T4 market-data research system. No trading execution.",
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["127.0.0.1", "localhost", "[::1]", "testserver"],
)


@app.middleware("http")
async def enforce_loopback_and_security_headers(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    client_host = request.client.host if request.client is not None else ""
    if client_host not in {"127.0.0.1", "::1", "testclient"}:
        return JSONResponse(
            status_code=status.HTTP_403_FORBIDDEN,
            content={"detail": "LOOPBACK_REQUIRED"},
            headers=_security_headers(),
        )
    response = await call_next(request)
    for name, value in _security_headers().items():
        response.headers[name] = value
    return response


def _build_and_analyze_with_deadline(
    *,
    provider_name: str | None,
    symbol: str,
    interval_minutes: int,
    work_deadline_monotonic: float,
    failure_deadline_monotonic: float,
    trace_id: str,
) -> ResearchReport:
    with analysis_deadline_scope(work_deadline_monotonic):
        orchestrator = build_orchestrator(provider_name)
        operation = (
            "live_t4_analysis"
            if orchestrator.provider.source_id == "plus500_t4_futures_v1"
            else "analysis"
        )
        report, _ = run_monitored_analysis(
            orchestrator.analyze,
            build_monitoring_repository(),
            operation=operation,
            symbol=symbol,
            interval_minutes=interval_minutes,
            trace_id=trace_id,
            failure_deadline_monotonic=failure_deadline_monotonic,
        )
        return report


@app.get("/health")
def health(response: Response) -> dict[str, object]:
    try:
        build_orchestrator()
        from .postgres import (
            PostgresSettings,
            PsycopgConnectionFactory,
            check_postgres_health,
            discover_migrations,
        )

        migrations = discover_migrations(default_migration_directory())
        database = check_postgres_health(
            PsycopgConnectionFactory(PostgresSettings.from_env()),
            expected_migrations=migrations,
        )
    except (PostgresError, RuntimeError, ValueError):
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "not_ready",
            "version": API_VERSION,
            "reason": "READ_ONLY_RUNTIME_NOT_READY",
            "execution_enabled": False,
        }
    if not database.healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "not_ready",
            "version": API_VERSION,
            "reason": database.status_code,
            "postgres": {
                "database_reachable": database.database_reachable,
                "base_schema_ready": database.base_schema_ready,
                "migrations_current": database.migrations_current,
                "schema_ready": database.schema_ready,
                "triggers_ready": database.triggers_ready,
                "seeds_ready": database.seeds_ready,
                "missing_migrations": list(database.missing_migrations),
                "unexpected_migrations": list(database.unexpected_migrations),
                "missing_schema_objects": list(database.missing_schema_objects),
                "missing_triggers": list(database.missing_triggers),
                "missing_seeds": list(database.missing_seeds),
            },
            "execution_enabled": False,
        }
    return {
        "status": "development_ready",
        "version": API_VERSION,
        "mode": "V1_READ_ONLY",
        "v1_gate_passed": False,
        "execution_enabled": False,
    }


@app.post("/v1/analyze")
async def analyze(
    response: Response,
    x_crypto_agent_request: Literal["analyze-v1"] = Header(
        alias="X-Crypto-Agent-Request"
    ),
    symbol: str = Query(default="BTC/USD", pattern=r"^(BTC|ETH)/USD$"),
    interval_minutes: Literal[240, 1440, 10080] = Query(default=1440),
    provider: Literal["synthetic", "t4"] | None = Query(
        default=None
    ),
    narrate: bool = Query(default=False),
) -> dict[str, object]:
    trace_id = str(uuid4())
    response.headers["X-Trace-ID"] = trace_id
    del x_crypto_agent_request
    try:
        timeout_seconds = analysis_timeout_seconds()
        deadline_monotonic = time.monotonic() + timeout_seconds
        work_deadline_monotonic = (
            deadline_monotonic - ANALYSIS_TIMEOUT_RECORDING_RESERVE_SECONDS
        )
        report = await asyncio.wait_for(
            asyncio.to_thread(
                _build_and_analyze_with_deadline,
                provider_name=provider,
                symbol=symbol,
                interval_minutes=interval_minutes,
                work_deadline_monotonic=work_deadline_monotonic,
                failure_deadline_monotonic=deadline_monotonic,
                trace_id=trace_id,
            ),
            # The configured total includes a reserved failure-recording budget.
            # The small grace only lets the cooperative exception reach here.
            timeout=timeout_seconds + 0.25,
        )
        if narrate:
            remaining = deadline_monotonic - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Read-only analysis exceeded its total deadline")
            report = replace(
                report,
                narrative=await asyncio.wait_for(
                    OpenAINarrator().explain(report),
                    timeout=remaining,
                ),
            )
        return cast(dict[str, object], report.to_dict())
    except NarrativeUnavailable as exc:
        raise HTTPException(
            status_code=503,
            detail="NARRATIVE_UNAVAILABLE",
            headers={"X-Trace-ID": trace_id},
        ) from exc
    except TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="ANALYSIS_TIMEOUT",
            headers={"X-Trace-ID": trace_id},
        ) from exc
    except (MonitoringError, PostgresError) as exc:
        raise HTTPException(
            status_code=503,
            detail="MONITORING_UNAVAILABLE",
            headers={"X-Trace-ID": trace_id},
        ) from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(
            status_code=422,
            detail="READ_ONLY_ANALYSIS_REJECTED",
            headers={"X-Trace-ID": trace_id},
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="ANALYSIS_FAILED",
            headers={"X-Trace-ID": trace_id},
        ) from exc


@app.get("/v1/monitoring/summary")
def monitoring_summary() -> dict[str, object]:
    return dict(_dashboard_data().summary)


@app.get("/v1/monitoring/alerts")
def monitoring_alerts(
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, object]:
    return {"items": list(_dashboard_data(limit=limit).alerts), "read_only": True}


@app.get("/v1/monitoring/incidents")
def monitoring_incidents(
    limit: int = Query(default=50, ge=1, le=100),
) -> dict[str, object]:
    return {"items": list(_dashboard_data(limit=limit).incidents), "read_only": True}


@app.get("/v1/monitoring/traces/{trace_id}")
def monitoring_trace(trace_id: str) -> dict[str, object]:
    try:
        result = build_monitoring_repository().trace(trace_id)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="TRACE_ID_INVALID") from exc
    except (MonitoringError, PostgresError) as exc:
        raise HTTPException(status_code=503, detail="MONITORING_UNAVAILABLE") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="MONITORING_UNAVAILABLE") from exc
    if result is None:
        raise HTTPException(status_code=404, detail="TRACE_NOT_FOUND")
    return result


@app.get("/dashboard", response_class=HTMLResponse)
def monitoring_dashboard() -> HTMLResponse:
    data = _dashboard_data()
    return HTMLResponse(
        render_dashboard(data.summary, data.alerts, data.incidents),
        headers={"Cache-Control": "no-store"},
    )


def _dashboard_data(*, limit: int | None = None) -> DashboardData:
    try:
        return build_monitoring_repository().dashboard(limit=limit)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="MONITORING_LIMIT_INVALID") from exc
    except (MonitoringError, PostgresError) as exc:
        raise HTTPException(status_code=503, detail="MONITORING_UNAVAILABLE") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail="MONITORING_UNAVAILABLE") from exc


def _security_headers() -> dict[str, str]:
    return {
        "Cache-Control": "no-store",
        "Content-Security-Policy": (
            "default-src 'none'; style-src 'unsafe-inline'; img-src 'none'; "
            "script-src 'none'; connect-src 'none'; frame-ancestors 'none'; "
            "base-uri 'none'; form-action 'none'"
        ),
        "Referrer-Policy": "no-referrer",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
    }

from __future__ import annotations

import asyncio
import time
from dataclasses import replace
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Response, status

from . import __version__
from .api_config import analysis_timeout_seconds
from .deadline import analysis_deadline_scope
from .factory import build_orchestrator
from .narrator import NarrativeUnavailable, OpenAINarrator
from .resource_paths import default_migration_directory


API_VERSION = f"{__version__}-v1.1"


app = FastAPI(
    title="Crypto Research Agent V1.1",
    version=API_VERSION,
    description="Development-only dual-feed research system. No trading execution.",
)


def _build_and_analyze_with_deadline(
    *,
    provider_name: str | None,
    symbol: str,
    interval_minutes: int,
    deadline_monotonic: float,
) -> Any:
    with analysis_deadline_scope(deadline_monotonic):
        orchestrator = build_orchestrator(provider_name)
        return orchestrator.analyze(
            symbol=symbol,
            interval_minutes=interval_minutes,
        )


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
    except (RuntimeError, ValueError) as exc:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {
            "status": "not_ready",
            "version": API_VERSION,
            "reason": str(exc),
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
                "missing_migrations": list(database.missing_migrations),
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


@app.get("/v1/analyze")
async def analyze(
    symbol: str = Query(default="BTC/USD", pattern=r"^(BTC|ETH)/USD$"),
    interval_minutes: Literal[240, 1440, 10080] = Query(default=1440),
    provider: Literal["synthetic", "kraken", "coinbase", "consensus"] | None = Query(
        default=None
    ),
    narrate: bool = Query(default=False),
) -> dict[str, object]:
    try:
        timeout_seconds = analysis_timeout_seconds()
        deadline_monotonic = time.monotonic() + timeout_seconds
        report = await asyncio.wait_for(
            asyncio.to_thread(
                _build_and_analyze_with_deadline,
                provider_name=provider,
                symbol=symbol,
                interval_minutes=interval_minutes,
                deadline_monotonic=deadline_monotonic,
            ),
            # Approved adapters obey the stricter cooperative deadline. The
            # grace period only lets that exception reach this coroutine.
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
        return report.to_dict()
    except NarrativeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(
            status_code=504,
            detail="Read-only analysis exceeded its total deadline",
        ) from exc
    except (RuntimeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

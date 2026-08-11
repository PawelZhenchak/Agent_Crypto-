from __future__ import annotations

import os
from pathlib import Path

from .orchestrator import ResearchOrchestrator
from .policy import RiskPolicy
from .providers import (
    Plus500T4Provider,
    SyntheticProvider,
)
from .resource_paths import default_risk_policy_path
from .storage import ReportRepository


FORBIDDEN_EXCHANGE_SECRET_NAMES = {
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "T4_USERNAME",
    "T4_PASSWORD",
    "T4_API_KEY",
}


def build_orchestrator(provider_name: str | None = None) -> ResearchOrchestrator:
    try:
        from dotenv import load_dotenv

        load_dotenv()
    except ImportError:
        # Core and safety tests intentionally remain runnable with the standard library only.
        pass
    _assert_no_exchange_credentials()
    environment = os.getenv("CRYPTO_AGENT_ENV", "development").lower()
    if environment not in {"development", "test"}:
        raise RuntimeError(
            "Plus500 T4 V1 remains development/test-only until the complete gate passes"
        )
    selected = provider_name or os.getenv("CRYPTO_AGENT_DATA_PROVIDER")
    if selected is None:
        raise ValueError(
            "Choose an explicit read-only provider: synthetic or t4"
        )
    configured_policy_path = os.getenv("CRYPTO_AGENT_RISK_POLICY_PATH")
    policy_path = (
        Path(configured_policy_path)
        if configured_policy_path is not None
        else default_risk_policy_path()
    )
    database_path = Path(
        os.getenv("CRYPTO_AGENT_DATABASE_PATH", "var/crypto_agent_plus500_t4.db")
    )
    policy = RiskPolicy.load(policy_path)
    if selected == "synthetic":
        provider = SyntheticProvider()
    elif selected == "t4":
        provider = Plus500T4Provider(
            bridge_url=os.getenv(
                "CRYPTO_AGENT_T4_BRIDGE_URL", "http://127.0.0.1:8784"
            ),
            bridge_token=os.getenv("CRYPTO_AGENT_T4_BRIDGE_TOKEN", ""),
            timeout_seconds=float(os.getenv("CRYPTO_AGENT_T4_TIMEOUT_SECONDS", "10")),
        )
    else:
        raise ValueError(f"Unknown read-only provider: {selected}")
    return ResearchOrchestrator(
        provider=provider,
        policy=policy,
        repository=ReportRepository(database_path),
    )


def _assert_no_exchange_credentials() -> None:
    present = sorted(name for name in FORBIDDEN_EXCHANGE_SECRET_NAMES if os.getenv(name))
    if present:
        raise RuntimeError(
            "Exchange credentials are forbidden in the V1 process: " + ", ".join(present)
        )

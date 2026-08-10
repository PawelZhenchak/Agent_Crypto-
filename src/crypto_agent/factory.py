from __future__ import annotations

import os
from pathlib import Path

from .orchestrator import ResearchOrchestrator
from .policy import RiskPolicy
from .providers import (
    CoinbaseExchangePublicProvider,
    CrossExchangeConsensusProvider,
    KrakenPublicProvider,
    SyntheticProvider,
)
from .resource_paths import default_risk_policy_path
from .storage import ReportRepository


FORBIDDEN_EXCHANGE_SECRET_NAMES = {
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "COINBASE_API_KEY",
    "COINBASE_API_PASSPHRASE",
    "COINBASE_API_PRIVATE_KEY",
    "COINBASE_API_SECRET",
    "KRAKEN_API_KEY",
    "KRAKEN_API_SECRET",
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
            "V1.1 remains development/test-only until the complete Gate V1 is passed"
        )
    selected = provider_name or os.getenv("CRYPTO_AGENT_DATA_PROVIDER")
    if selected is None:
        raise ValueError(
            "Choose an explicit read-only provider: synthetic, kraken, coinbase or consensus"
        )
    configured_policy_path = os.getenv("CRYPTO_AGENT_RISK_POLICY_PATH")
    policy_path = (
        Path(configured_policy_path)
        if configured_policy_path is not None
        else default_risk_policy_path()
    )
    database_path = Path(
        os.getenv("CRYPTO_AGENT_DATABASE_PATH", "var/crypto_agent_v1_1.db")
    )
    policy = RiskPolicy.load(policy_path)
    if selected == "synthetic":
        provider = SyntheticProvider()
    elif selected == "kraken":
        provider = KrakenPublicProvider(
            base_url=os.getenv("CRYPTO_AGENT_KRAKEN_BASE_URL", "https://api.kraken.com")
        )
    elif selected == "coinbase":
        provider = CoinbaseExchangePublicProvider(
            base_url=os.getenv(
                "CRYPTO_AGENT_COINBASE_BASE_URL",
                "https://api.exchange.coinbase.com",
            )
        )
    elif selected == "consensus":
        provider = CrossExchangeConsensusProvider(
            (
                KrakenPublicProvider(
                    base_url=os.getenv(
                        "CRYPTO_AGENT_KRAKEN_BASE_URL", "https://api.kraken.com"
                    )
                ),
                CoinbaseExchangePublicProvider(
                    base_url=os.getenv(
                        "CRYPTO_AGENT_COINBASE_BASE_URL",
                        "https://api.exchange.coinbase.com",
                    )
                ),
            ),
            policy,
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

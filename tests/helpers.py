from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from crypto_agent.policy import RiskPolicy
from crypto_agent.providers.synthetic import SyntheticProvider


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def policy() -> RiskPolicy:
    return RiskPolicy.load(PROJECT_ROOT / "configs/risk_policy.v1.json")


def candles(count: int = 120):
    as_of = datetime(2026, 8, 10, tzinfo=timezone.utc)
    return SyntheticProvider().fetch_candles(
        symbol="BTC/USD",
        interval_minutes=1440,
        as_of=as_of,
        limit=count,
    )


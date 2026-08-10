from __future__ import annotations

import math
import os


def analysis_timeout_seconds() -> float:
    """Return the bounded end-to-end analysis deadline for the HTTP API."""

    raw = os.getenv("CRYPTO_AGENT_ANALYSIS_TIMEOUT_SECONDS", "30")
    try:
        value = float(raw)
    except ValueError:
        raise ValueError("CRYPTO_AGENT_ANALYSIS_TIMEOUT_SECONDS is invalid") from None
    if not math.isfinite(value) or not 1 <= value <= 120:
        raise ValueError(
            "CRYPTO_AGENT_ANALYSIS_TIMEOUT_SECONDS must be finite and in [1, 120]"
        )
    return value

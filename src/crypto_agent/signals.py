from __future__ import annotations

from dataclasses import dataclass

from .domain import Decision, MarketMetrics, Regime
from .futures_analytics import FuturesAnalysis


VOLUME_ALERT_ZSCORE = 2.0


@dataclass(frozen=True, slots=True)
class ResearchProposal:
    decision: Decision
    reason_codes: tuple[str, ...]
    reasons: tuple[str, ...]


def propose_research_alert(
    metrics: MarketMetrics | None,
    *,
    volume_anomaly_attested: bool = False,
) -> ResearchProposal:
    """Create a non-trading research proposal. Abstention is the default."""

    if metrics is None:
        return ResearchProposal(
            decision=Decision.NO_SIGNAL,
            reason_codes=("METRICS_UNAVAILABLE",),
            reasons=("Metryki są niedostępne.",),
        )

    triggers: list[str] = []
    reason_codes: list[str] = []
    if metrics.regime is Regime.HIGH_VOLATILITY:
        triggers.append("Wykryto reżim wysokiej zmienności.")
        reason_codes.append("HIGH_VOLATILITY_REGIME")
    if abs(metrics.period_return) >= 0.05:
        triggers.append("Bezwzględna zmiana ostatniego okresu przekracza 5%.")
        reason_codes.append("LARGE_PERIOD_MOVE")
    if abs(metrics.return_7_periods) >= 0.12:
        triggers.append("Bezwzględna zmiana siedmiu okresów przekracza 12%.")
        reason_codes.append("LARGE_SEVEN_PERIOD_MOVE")
    if volume_anomaly_attested and abs(metrics.volume_zscore) >= VOLUME_ALERT_ZSCORE:
        triggers.append("Wolumen odchyla się od ostatniego okna o co najmniej 2 sigma.")
        reason_codes.append("VOLUME_ANOMALY")

    if not triggers:
        return ResearchProposal(
            decision=Decision.NO_SIGNAL,
            reason_codes=("NO_ALERT_THRESHOLD",),
            reasons=("Żaden próg alertu badawczego nie został przekroczony.",),
        )
    return ResearchProposal(
        decision=Decision.ALERT,
        reason_codes=tuple(reason_codes),
        reasons=tuple(triggers),
    )


def apply_futures_gate(
    proposal: ResearchProposal,
    futures: FuturesAnalysis,
) -> ResearchProposal:
    """Require complete futures evidence before preserving any research alert."""

    reason_codes = tuple(
        dict.fromkeys((*proposal.reason_codes, *futures.reason_codes))
    )
    if futures.gate_passed:
        return ResearchProposal(
            decision=proposal.decision,
            reason_codes=reason_codes,
            reasons=proposal.reasons,
        )
    return ResearchProposal(
        decision=Decision.NO_SIGNAL,
        reason_codes=reason_codes,
        reasons=(
            *proposal.reasons,
            "Pełny zestaw atestowanych dowodów futures nie przeszedł bramki.",
        ),
    )

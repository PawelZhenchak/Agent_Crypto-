from __future__ import annotations

import unittest

from crypto_agent.domain import Decision, MarketMetrics, Regime
from crypto_agent.signals import propose_research_alert


class ResearchSignalTests(unittest.TestCase):
    def test_no_signal_is_default(self) -> None:
        metrics = MarketMetrics(
            last_price=100,
            period_return=0.01,
            return_7_periods=0.03,
            annualized_volatility=0.30,
            max_drawdown=-0.10,
            sma_20=101,
            sma_50=100,
            volume_zscore=0.5,
            regime=Regime.RANGE,
        )
        self.assertEqual(propose_research_alert(metrics).decision, Decision.NO_SIGNAL)

    def test_large_move_creates_research_alert_only(self) -> None:
        metrics = MarketMetrics(
            last_price=100,
            period_return=0.06,
            return_7_periods=0.08,
            annualized_volatility=0.50,
            max_drawdown=-0.10,
            sma_20=104,
            sma_50=100,
            volume_zscore=1.0,
            regime=Regime.TREND_UP,
        )
        proposal = propose_research_alert(metrics)
        self.assertEqual(proposal.decision, Decision.ALERT)
        self.assertTrue(proposal.reasons)


if __name__ == "__main__":
    unittest.main()

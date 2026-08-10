from __future__ import annotations

import math
import statistics

from .domain import Candle, MarketMetrics, Regime


def calculate_metrics(candles: list[Candle]) -> MarketMetrics:
    ordered = sorted(candles, key=lambda candle: candle.open_time)
    if len(ordered) < 50:
        raise ValueError("At least 50 candles are required")

    closes = [candle.close for candle in ordered]
    volumes = [candle.volume for candle in ordered]
    log_returns = [math.log(current / previous) for previous, current in zip(closes, closes[1:])]
    recent_returns = log_returns[-30:]
    periods_per_year = max(1.0, (365 * 24 * 60) / ordered[-1].interval_minutes)
    annualized_volatility = statistics.pstdev(recent_returns) * math.sqrt(periods_per_year)

    last_price = closes[-1]
    period_return = last_price / closes[-2] - 1
    return_7_periods = last_price / closes[-8] - 1
    sma_20 = statistics.fmean(closes[-20:])
    sma_50 = statistics.fmean(closes[-50:])
    max_drawdown = _max_drawdown(closes[-90:])
    volume_zscore = _zscore(volumes[-30:])
    regime = _classify_regime(
        annualized_volatility=annualized_volatility,
        sma_20=sma_20,
        sma_50=sma_50,
    )
    return MarketMetrics(
        last_price=round(last_price, 8),
        period_return=round(period_return, 8),
        return_7_periods=round(return_7_periods, 8),
        annualized_volatility=round(annualized_volatility, 8),
        max_drawdown=round(max_drawdown, 8),
        sma_20=round(sma_20, 8),
        sma_50=round(sma_50, 8),
        volume_zscore=round(volume_zscore, 8),
        regime=regime,
    )


def _max_drawdown(values: list[float]) -> float:
    peak = values[0]
    worst = 0.0
    for value in values:
        peak = max(peak, value)
        drawdown = value / peak - 1
        worst = min(worst, drawdown)
    return worst


def _zscore(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    deviation = statistics.pstdev(values[:-1])
    if deviation == 0:
        return 0.0
    return (values[-1] - statistics.fmean(values[:-1])) / deviation


def _classify_regime(*, annualized_volatility: float, sma_20: float, sma_50: float) -> Regime:
    if annualized_volatility > 0.80:
        return Regime.HIGH_VOLATILITY
    ratio = sma_20 / sma_50
    if ratio > 1.02:
        return Regime.TREND_UP
    if ratio < 0.98:
        return Regime.TREND_DOWN
    return Regime.RANGE


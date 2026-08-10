from __future__ import annotations

import math
from datetime import datetime, timedelta

from ..domain import Candle


class SyntheticProvider:
    """Deterministic fixture provider for safe local development and tests."""

    source_id = "synthetic_fixture_v1"

    def fetch_candles(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> list[Candle]:
        if as_of.tzinfo is None:
            raise ValueError("as_of must be timezone-aware")
        step = timedelta(minutes=interval_minutes)
        last_close = as_of.replace(second=0, microsecond=0)
        if interval_minutes >= 1440:
            last_close = last_close.replace(hour=0, minute=0)
        count = max(60, limit)
        base = 50_000.0 if symbol == "BTC/USD" else 2_500.0
        # The fixture represents a dataset already captured at the requested cutoff.
        ingested_at = as_of
        candles: list[Candle] = []
        previous_close = base
        for index in range(count):
            close_time = last_close - step * (count - index - 1)
            open_time = close_time - step
            trend = 0.0008
            wave = math.sin(index / 5.0) * 0.006
            shock = 0.0 if index % 37 else -0.012
            close = previous_close * (1 + trend + wave + shock)
            open_price = previous_close
            high = max(open_price, close) * 1.004
            low = min(open_price, close) * 0.996
            volume = 1000.0 + 80.0 * math.cos(index / 4.0) + index
            candles.append(
                Candle(
                    symbol=symbol,
                    interval_minutes=interval_minutes,
                    open_time=open_time,
                    close_time=close_time,
                    open=open_price,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                    source=self.source_id,
                    available_at=close_time,
                    ingested_at=ingested_at,
                )
            )
            previous_close = close
        return candles

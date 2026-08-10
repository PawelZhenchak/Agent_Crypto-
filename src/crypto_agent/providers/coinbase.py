from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .. import __version__
from ..deadline import bounded_analysis_timeout, ensure_analysis_deadline
from ..domain import Candle
from .base import ProviderError


class _NoRedirectHandler(HTTPRedirectHandler):
    """Reject redirects before urllib can issue a request to their target."""

    def redirect_request(
        self,
        req: Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> None:
        del req, fp, code, msg, headers, newurl
        return None


def urlopen(request: Request, *, timeout: float):  # type: ignore[no-untyped-def]
    """Open only the requested URL; every HTTP redirect remains an HTTPError."""

    return build_opener(_NoRedirectHandler()).open(request, timeout=timeout)


@dataclass(frozen=True, slots=True)
class _BaseCandle:
    open_time: datetime
    low: float
    high: float
    open: float
    close: float
    volume: float


class CoinbaseExchangePublicProvider:
    """Public Coinbase Exchange candle adapter with no private API surface.

    Coinbase Exchange has no native four-hour or weekly granularity. Four-hour
    candles are therefore built from four complete one-hour buckets, and weekly
    candles from seven complete UTC daily buckets aligned to the Unix epoch
    (Thursday 00:00 UTC), matching Kraken's native weekly interval.
    Missing source buckets are never filled or interpolated.
    """

    source_id = "coinbase_exchange_spot_rest_v1"
    _immutable_configuration_attributes = frozenset(
        {
            "source_id",
            "base_url",
            "_base_url",
            "timeout_seconds",
            "_timeout_seconds",
            "_configuration_sealed",
            "_immutable_configuration_attributes",
        }
    )
    _products = {"BTC/USD": "BTC-USD", "ETH/USD": "ETH-USD"}
    # target interval minutes: (Coinbase source granularity seconds, source bucket count)
    _interval_specs = {
        240: (3_600, 4),
        1_440: (86_400, 1),
        10_080: (86_400, 7),
    }
    # Use a 299-interval request window so the request remains <=300 data points
    # even if an API boundary is treated as inclusive. The documented response
    # maximum remains 300, including any rows preceding the requested start.
    _source_intervals_per_request = 299
    _max_source_candles_per_response = 300
    _max_target_candles = 300
    _max_response_bytes = 2_000_000
    _utc_epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)

    def __setattr__(self, name: str, value: object) -> None:
        if (
            getattr(self, "_configuration_sealed", False)
            and name in type(self)._immutable_configuration_attributes
        ):
            raise AttributeError(
                "Coinbase Exchange provider origin and configuration are immutable"
            )
        object.__setattr__(self, name, value)

    def __delattr__(self, name: str) -> None:
        if (
            getattr(self, "_configuration_sealed", False)
            and name in type(self)._immutable_configuration_attributes
        ):
            raise AttributeError(
                "Coinbase Exchange provider origin and configuration are immutable"
            )
        object.__delattr__(self, name)

    def __init__(
        self,
        base_url: str = "https://api.exchange.coinbase.com",
        timeout_seconds: float = 10,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "api.exchange.coinbase.com"
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Coinbase Exchange provider is pinned to "
                "https://api.exchange.coinbase.com"
            )
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a positive finite number")
        object.__setattr__(
            self,
            "_base_url",
            "https://api.exchange.coinbase.com",
        )
        object.__setattr__(self, "_timeout_seconds", float(timeout_seconds))
        object.__setattr__(self, "_configuration_sealed", True)

    @property
    def base_url(self) -> str:
        return self._base_url

    @property
    def timeout_seconds(self) -> float:
        return self._timeout_seconds

    def fetch_candles(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> list[Candle]:
        product = self._products.get(symbol)
        if product is None:
            raise ProviderError(f"Unsupported Coinbase Exchange V1 symbol: {symbol}")
        source_granularity, _ = self._validate_request(
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
        )

        target_end = self._floor_target_boundary(as_of, interval_minutes)
        target_start = target_end - timedelta(minutes=interval_minutes * limit)
        source_step = timedelta(seconds=source_granularity)
        page_span = source_step * self._source_intervals_per_request

        rows: list[Any] = []
        page_start = target_start
        while page_start < target_end:
            page_end = min(page_start + page_span, target_end)
            rows.extend(
                self._request_page(
                    product=product,
                    granularity_seconds=source_granularity,
                    start=page_start,
                    end=page_end,
                )
            )
            page_start = page_end

        return self.parse_payload(
            rows,
            symbol=symbol,
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
        )

    def _request_page(
        self,
        *,
        product: str,
        granularity_seconds: int,
        start: datetime,
        end: datetime,
    ) -> list[Any]:
        query = urlencode(
            {
                "granularity": granularity_seconds,
                "start": self._rfc3339(start),
                "end": self._rfc3339(end),
            }
        )
        request = Request(
            f"{self.base_url}/products/{product}/candles?{query}",
            headers={
                "Accept": "application/json",
                "User-Agent": f"crypto-research-agent/{__version__} read-only",
            },
        )
        try:
            with urlopen(
                request,
                timeout=bounded_analysis_timeout(self.timeout_seconds),
            ) as response:  # noqa: S310
                final_url = response.geturl() if hasattr(response, "geturl") else request.full_url
                if final_url != request.full_url:
                    raise ProviderError(
                        "Coinbase Exchange response was redirected away from the pinned "
                        "public endpoint",
                        code="SOURCE_ORIGIN_MISMATCH",
                    )
                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) > self._max_response_bytes:
                    raise ProviderError("Coinbase Exchange response exceeds the size limit")
                raw = response.read(self._max_response_bytes + 1)
                if len(raw) > self._max_response_bytes:
                    raise ProviderError("Coinbase Exchange response exceeds the size limit")
                payload = json.loads(raw.decode("utf-8"))
        except ProviderError:
            raise
        except json.JSONDecodeError as exc:
            raise ProviderError("Coinbase Exchange returned invalid JSON") from exc
        except (HTTPError, URLError, TimeoutError, ValueError, UnicodeDecodeError) as exc:
            ensure_analysis_deadline()
            raise ProviderError(f"Coinbase Exchange public data request failed: {exc}") from exc
        if not isinstance(payload, list):
            raise ProviderError("Coinbase Exchange candle response root must be an array")
        if len(payload) > self._max_source_candles_per_response:
            raise ProviderError("Coinbase Exchange returned more than 300 candles")
        return payload

    def parse_payload(
        self,
        payload: list[Any],
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> list[Candle]:
        if symbol not in self._products:
            raise ProviderError(f"Unsupported Coinbase Exchange V1 symbol: {symbol}")
        source_granularity_seconds, source_bucket_count = self._validate_request(
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
        )
        if not isinstance(payload, list):
            raise ProviderError("Coinbase Exchange candle response root must be an array")

        base_by_open_time: dict[datetime, _BaseCandle] = {}
        source_step = timedelta(seconds=source_granularity_seconds)
        for row in payload:
            candle = self._parse_row(row, source_granularity_seconds)
            existing = base_by_open_time.get(candle.open_time)
            if existing is not None and existing != candle:
                raise ProviderError("Conflicting duplicate Coinbase Exchange candle")
            base_by_open_time.setdefault(candle.open_time, candle)

        target_end = self._floor_target_boundary(as_of, interval_minutes)
        target_step = timedelta(minutes=interval_minutes)
        target_start = target_end - target_step * limit
        ingested_at = datetime.now(timezone.utc)
        candles: list[Candle] = []

        bucket_start = target_start
        while bucket_start < target_end:
            expected_open_times = [
                bucket_start + source_step * index
                for index in range(source_bucket_count)
            ]
            components = [base_by_open_time.get(item) for item in expected_open_times]
            # Coinbase documents that empty trade intervals may be absent. Preserve
            # that gap instead of fabricating a target candle from partial data.
            if all(component is not None for component in components):
                complete = [component for component in components if component is not None]
                candles.append(
                    Candle(
                        symbol=symbol,
                        interval_minutes=interval_minutes,
                        open_time=bucket_start,
                        close_time=bucket_start + target_step,
                        open=complete[0].open,
                        high=max(component.high for component in complete),
                        low=min(component.low for component in complete),
                        close=complete[-1].close,
                        volume=sum(component.volume for component in complete),
                        source=self.source_id,
                        # The complete multi-page payload only becomes available to
                        # this process after the final page has been collected.
                        available_at=ingested_at,
                        ingested_at=ingested_at,
                    )
                )
            bucket_start += target_step
        return candles

    def _parse_row(self, row: Any, source_granularity_seconds: int) -> _BaseCandle:
        # Coinbase documents [time, low, high, open, close, volume].
        if not isinstance(row, list) or len(row) != 6:
            raise ProviderError("Malformed Coinbase Exchange candle row")
        timestamp = row[0]
        if isinstance(timestamp, bool) or not isinstance(timestamp, int):
            raise ProviderError("Coinbase Exchange candle time must be integer Unix seconds")
        try:
            open_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise ProviderError("Malformed Coinbase Exchange candle time") from exc
        epoch_seconds = int((open_time - self._utc_epoch).total_seconds())
        if epoch_seconds % source_granularity_seconds != 0:
            raise ProviderError("Coinbase Exchange candle is not aligned to its granularity")

        numeric_values = tuple(self._finite_float(value) for value in row[1:])
        low, high, open_price, close_price, volume = numeric_values
        if min(low, high, open_price, close_price) <= 0 or volume < 0:
            raise ProviderError("Coinbase Exchange candle contains an invalid price or volume")
        if not (low <= open_price <= high and low <= close_price <= high):
            raise ProviderError("Coinbase Exchange candle has inconsistent OHLC values")
        return _BaseCandle(
            open_time=open_time,
            low=low,
            high=high,
            open=open_price,
            close=close_price,
            volume=volume,
        )

    def _validate_request(
        self,
        *,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> tuple[int, int]:
        spec = self._interval_specs.get(interval_minutes)
        if spec is None:
            raise ProviderError(f"Unsupported Coinbase Exchange V1 interval: {interval_minutes}")
        if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
            raise ProviderError("as_of must be timezone-aware and normalized to UTC")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or limit <= 0
            or limit > self._max_target_candles
        ):
            raise ProviderError(
                f"Coinbase Exchange limit must be between 1 and {self._max_target_candles}"
            )
        return spec

    def _floor_target_boundary(self, value: datetime, interval_minutes: int) -> datetime:
        step_seconds = interval_minutes * 60
        elapsed_seconds = int((value - self._utc_epoch).total_seconds())
        return self._utc_epoch + timedelta(
            seconds=(elapsed_seconds // step_seconds) * step_seconds
        )

    @staticmethod
    def _finite_float(value: Any) -> float:
        if isinstance(value, bool):
            raise ProviderError("Coinbase Exchange candle values must be finite numbers")
        try:
            converted = float(value)
        except (TypeError, ValueError) as exc:
            raise ProviderError("Malformed Coinbase Exchange candle value") from exc
        if not math.isfinite(converted):
            raise ProviderError("Coinbase Exchange candle values must be finite numbers")
        return converted

    @staticmethod
    def _rfc3339(value: datetime) -> str:
        return value.isoformat(timespec="seconds").replace("+00:00", "Z")

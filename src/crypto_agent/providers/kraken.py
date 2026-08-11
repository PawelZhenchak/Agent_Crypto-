from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .. import __version__
from ..deadline import bounded_analysis_timeout, ensure_analysis_deadline
from ..domain import Candle, ReferencePriceObservation
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


class KrakenPublicProvider:
    """Public OHLC adapter. It has no code path for private endpoints or credentials."""

    __slots__ = ("_base_url", "_timeout_seconds", "_configuration_sealed")

    source_id = "kraken_spot_rest_v1"
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
    _pairs = {"BTC/USD": "XBTUSD", "ETH/USD": "ETHUSD"}
    _result_keys = {
        "BTC/USD": {"XXBTZUSD", "XBTUSD"},
        "ETH/USD": {"XETHZUSD", "ETHUSD"},
    }
    _max_response_bytes = 2_000_000
    _interval_specs = {
        240: 240,
        1440: 1440,
        10080: 10080,
    }

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_configuration_sealed", False):
            raise AttributeError("Kraken provider origin and configuration are immutable")
        object.__setattr__(self, name, value)

    def __delattr__(self, name: str) -> None:
        if getattr(self, "_configuration_sealed", False):
            raise AttributeError("Kraken provider origin and configuration are immutable")
        object.__delattr__(self, name)

    def __init__(
        self,
        base_url: str = "https://api.kraken.com",
        timeout_seconds: float = 10,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "api.kraken.com"
            or parsed.port is not None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("Kraken provider is pinned to https://api.kraken.com")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be a positive finite number")
        object.__setattr__(self, "_base_url", "https://api.kraken.com")
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
        pair = self._pairs.get(symbol)
        if pair is None:
            raise ProviderError(f"Unsupported Kraken V1 symbol: {symbol}")
        source_interval_minutes = self._validate_request(
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
        )
        payload = self._request_ohlc_payload(
            pair=pair,
            source_interval_minutes=source_interval_minutes,
        )
        return self.parse_payload(
            payload,
            symbol=symbol,
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
            source_interval_minutes=source_interval_minutes,
        )

    def fetch_reference_price(
        self,
        *,
        symbol: str,
        as_of: datetime,
    ) -> ReferencePriceObservation:
        pair = self._pairs.get(symbol)
        if pair is None:
            raise ProviderError(f"Unsupported Kraken V1 symbol: {symbol}")
        self._validate_reference_request(as_of=as_of)
        payload = self._request_ohlc_payload(
            pair=pair,
            source_interval_minutes=1,
        )
        return self.parse_reference_price_payload(
            payload,
            symbol=symbol,
            as_of=as_of,
        )

    def _request_ohlc_payload(
        self,
        *,
        pair: str,
        source_interval_minutes: int,
    ) -> dict[str, Any]:
        query = urlencode({"pair": pair, "interval": source_interval_minutes})
        request = Request(
            f"https://api.kraken.com/0/public/OHLC?{query}",
            headers={"User-Agent": f"crypto-research-agent/{__version__} read-only"},
        )
        try:
            with urlopen(
                request,
                timeout=bounded_analysis_timeout(self.timeout_seconds),
            ) as response:  # noqa: S310
                final_url = response.geturl() if hasattr(response, "geturl") else request.full_url
                if final_url != request.full_url:
                    raise ProviderError(
                        "Kraken response was redirected away from the pinned public endpoint",
                        code="SOURCE_ORIGIN_MISMATCH",
                    )
                content_length = response.headers.get("Content-Length")
                if content_length and int(content_length) > self._max_response_bytes:
                    raise ProviderError("Kraken response exceeds the size limit")
                raw = response.read(self._max_response_bytes + 1)
                if len(raw) > self._max_response_bytes:
                    raise ProviderError("Kraken response exceeds the size limit")
                payload = json.loads(raw.decode("utf-8"))
        except ProviderError:
            raise
        except (HTTPError, URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            ensure_analysis_deadline()
            raise ProviderError(f"Kraken public data request failed: {exc}") from exc
        if not isinstance(payload, dict):
            raise ProviderError("Kraken response root must be an object")
        return payload

    def parse_reference_price_payload(
        self,
        payload: dict[str, Any],
        *,
        symbol: str,
        as_of: datetime,
    ) -> ReferencePriceObservation:
        self._validate_reference_request(as_of=as_of)
        series = self._extract_ohlc_series(payload, symbol=symbol)
        expected_close = as_of.replace(second=0, microsecond=0)
        matches: list[float] = []
        # Kraken documents the last row as the current, not-yet-committed candle.
        for row in series[:-1]:
            if not isinstance(row, list) or len(row) < 7:
                raise ProviderError("Malformed Kraken OHLC row")
            try:
                open_time = datetime.fromtimestamp(int(row[0]), tz=timezone.utc)
                close_time = open_time + timedelta(minutes=1)
                price = float(row[4])
            except (TypeError, ValueError, OverflowError, OSError) as exc:
                raise ProviderError("Malformed Kraken OHLC value") from exc
            if not math.isfinite(price) or price <= 0:
                raise ProviderError("Kraken reference price must be positive and finite")
            if close_time == expected_close:
                matches.append(price)
        if not matches:
            raise ProviderError(
                "Kraken returned no completed one-minute reference candle",
                code="REFERENCE_PRICE_UNAVAILABLE",
            )
        if any(item != matches[0] for item in matches[1:]):
            raise ProviderError("Conflicting duplicate Kraken reference candle")
        ingested_at = datetime.now(timezone.utc)
        return ReferencePriceObservation(
            symbol=symbol,
            price=matches[0],
            event_time=expected_close,
            available_at=ingested_at,
            ingested_at=ingested_at,
            source=self.source_id,
        )

    def parse_payload(
        self,
        payload: dict[str, Any],
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
        source_interval_minutes: int | None = None,
    ) -> list[Candle]:
        expected_source_interval = self._validate_request(
            interval_minutes=interval_minutes,
            as_of=as_of,
            limit=limit,
        )
        if source_interval_minutes is None:
            source_interval_minutes = expected_source_interval
        if source_interval_minutes != expected_source_interval:
            raise ProviderError("Kraken source interval does not match the target interval")
        series = self._extract_ohlc_series(payload, symbol=symbol)

        ingested_at = datetime.now(timezone.utc)
        source_step = timedelta(minutes=source_interval_minutes)
        source_candles: list[Candle] = []
        # Kraken documents the last item as the current, not-yet-committed candle.
        for row in series[:-1]:
            if not isinstance(row, list) or len(row) < 7:
                raise ProviderError("Malformed Kraken OHLC row")
            try:
                open_time = datetime.fromtimestamp(int(row[0]), tz=timezone.utc)
                close_time = open_time + source_step
                numeric_values = tuple(float(row[index]) for index in (1, 2, 3, 4, 6))
            except (TypeError, ValueError, OverflowError, OSError) as exc:
                raise ProviderError("Malformed Kraken OHLC value") from exc
            if not all(math.isfinite(value) for value in numeric_values):
                raise ProviderError("Kraken OHLC values must be finite")
            candle = Candle(
                symbol=symbol,
                interval_minutes=source_interval_minutes,
                open_time=open_time,
                close_time=close_time,
                open=numeric_values[0],
                high=numeric_values[1],
                low=numeric_values[2],
                close=numeric_values[3],
                volume=numeric_values[4],
                source=self.source_id,
                # Without a captured historical arrival timestamp, V1 may only claim that
                # this payload became available when it entered our process.
                available_at=ingested_at,
                ingested_at=ingested_at,
            )
            if candle.close_time <= as_of:
                source_candles.append(candle)
        if interval_minutes == source_interval_minutes:
            return source_candles[-limit:]
        raise ProviderError("Kraken source interval does not match the target interval")

    def _extract_ohlc_series(
        self,
        payload: dict[str, Any],
        *,
        symbol: str,
    ) -> list[Any]:
        if not isinstance(payload, dict):
            raise ProviderError("Kraken response root must be an object")
        errors = payload.get("error") or []
        if not isinstance(errors, list):
            raise ProviderError("Kraken error field must be an array")
        if errors:
            raise ProviderError(f"Kraken returned errors: {errors}")
        result = payload.get("result")
        if not isinstance(result, dict):
            raise ProviderError("Kraken response has no result object")
        data_keys = [key for key in result if key != "last"]
        if len(data_keys) != 1 or data_keys[0] not in self._result_keys.get(symbol, set()):
            raise ProviderError("Kraken response pair does not match the requested symbol")
        series = result[data_keys[0]]
        if not isinstance(series, list):
            raise ProviderError("Kraken response has no OHLC series")
        return series

    def _validate_request(
        self,
        *,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> int:
        source_interval = self._interval_specs.get(interval_minutes)
        if source_interval is None:
            raise ProviderError(f"Unsupported Kraken V1 interval: {interval_minutes}")
        if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
            raise ProviderError("as_of must be timezone-aware and normalized to UTC")
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 720:
            raise ProviderError("Kraken limit must be an integer between 1 and 720")
        return source_interval

    @staticmethod
    def _validate_reference_request(*, as_of: datetime) -> None:
        if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
            raise ProviderError("as_of must be timezone-aware and normalized to UTC")

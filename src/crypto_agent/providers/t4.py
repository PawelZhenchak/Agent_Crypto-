from __future__ import annotations

import json
from datetime import datetime, timedelta
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from ..deadline import ensure_analysis_deadline
from ..domain import Candle, ReferencePriceObservation, ReferencePriceSnapshot
from .base import ProviderBatch, ProviderError


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


class Plus500T4Provider:
    """Read-only adapter for a local Plus500 Futures T4 .NET bridge.

    T4 credentials and the proprietary .NET session remain in the isolated bridge.
    The Python analysis process accepts market-data snapshots only.
    """

    source_id = "plus500_t4_futures_v1"
    venue_id = "plus500_t4"
    _allowed_hosts = frozenset({"127.0.0.1", "localhost", "::1"})
    _allowed_intervals = frozenset({240, 1440, 10080})
    _allowed_symbols = frozenset({"BTC/USD", "ETH/USD"})
    _max_response_bytes = 4_000_000

    def __init__(
        self,
        *,
        bridge_url: str = "http://127.0.0.1:8784",
        timeout_seconds: float = 10.0,
    ) -> None:
        parsed = urlsplit(bridge_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in self._allowed_hosts
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
            or parsed.port is None
        ):
            raise ValueError("T4 bridge must be an explicit loopback HTTP origin with a port")
        if not isinstance(timeout_seconds, (int, float)) or isinstance(
            timeout_seconds, bool
        ):
            raise ValueError("T4 bridge timeout is invalid")
        if not 1 <= float(timeout_seconds) <= 30:
            raise ValueError("T4 bridge timeout must be in [1, 30] seconds")
        self._bridge_url = bridge_url.rstrip("/")
        self._timeout_seconds = float(timeout_seconds)

    def fetch_candles(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> list[Candle]:
        return list(
            self.fetch_batch(
                symbol=symbol,
                interval_minutes=interval_minutes,
                as_of=as_of,
                limit=limit,
            ).candles
        )

    def fetch_batch(
        self,
        *,
        symbol: str,
        interval_minutes: int,
        as_of: datetime,
        limit: int,
    ) -> ProviderBatch:
        if symbol not in self._allowed_symbols:
            raise ProviderError("T4 instrument is not approved", code="T4_SYMBOL_NOT_ALLOWED")
        if interval_minutes not in self._allowed_intervals:
            raise ProviderError("T4 interval is not approved", code="T4_INTERVAL_NOT_ALLOWED")
        if as_of.tzinfo is None or as_of.utcoffset() != timedelta(0):
            raise ProviderError("T4 cutoff must be UTC", code="T4_TIME_INVALID")
        if type(limit) is not int or not 60 <= limit <= 720:
            raise ProviderError("T4 candle limit is invalid", code="T4_LIMIT_INVALID")

        query = urlencode(
            {
                "symbol": symbol,
                "interval_minutes": interval_minutes,
                "as_of": as_of.isoformat(),
                "limit": limit,
            }
        )
        request = Request(
            f"{self._bridge_url}/v1/market-data?{query}",
            headers={"Accept": "application/json"},
            method="GET",
        )
        ensure_analysis_deadline()
        try:
            response = build_opener(_NoRedirectHandler()).open(
                request, timeout=self._timeout_seconds
            )
            payload_bytes = response.read(self._max_response_bytes + 1)
        except Exception as exc:
            raise ProviderError(
                "Plus500 T4 bridge is unavailable", code="T4_BRIDGE_UNAVAILABLE"
            ) from exc
        if len(payload_bytes) > self._max_response_bytes:
            raise ProviderError("T4 response is too large", code="T4_RESPONSE_TOO_LARGE")
        try:
            payload = json.loads(payload_bytes)
            candles = tuple(
                self._parse_candle(item, symbol, interval_minutes)
                for item in payload["candles"]
            )
            reference = self._parse_reference(payload["reference_price"], symbol)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ProviderError("T4 payload is invalid", code="T4_PAYLOAD_INVALID") from exc
        if len(candles) < limit:
            raise ProviderError("T4 history is incomplete", code="T4_HISTORY_INCOMPLETE")
        return ProviderBatch(
            candles=candles[-limit:],
            input_candles=candles[-limit:],
            sources=(
                {
                    "id": self.source_id,
                    "kind": "futures_market_data",
                    "trust": "authenticated_external_data",
                },
            ),
            metadata={
                "t4_read_only_attested": payload.get("read_only") is True,
                "t4_source_id": payload.get("source_id"),
                "t4_venue_id": payload.get("venue_id"),
                "t4_order_routes_exposed": payload.get("order_routes_exposed"),
                "t4_volume_zscore": payload.get("volume_zscore"),
            },
            reference_price=ReferencePriceSnapshot(
                symbol=symbol, observations=(reference,)
            ),
        )

    def _parse_candle(
        self, item: object, symbol: str, interval_minutes: int
    ) -> Candle:
        if not isinstance(item, dict):
            raise ValueError("candle is not an object")
        if item.get("symbol") != symbol or item.get("interval_minutes") != interval_minutes:
            raise ValueError("candle scope mismatch")
        if item.get("source") != self.source_id:
            raise ValueError("candle source mismatch")
        return Candle(
            symbol=symbol,
            interval_minutes=interval_minutes,
            open_time=_utc(item["open_time"]),
            close_time=_utc(item["close_time"]),
            open=_number(item["open"]),
            high=_number(item["high"]),
            low=_number(item["low"]),
            close=_number(item["close"]),
            volume=_number(item["volume"]),
            source=self.source_id,
            available_at=_utc(item["available_at"]),
            ingested_at=_utc(item["ingested_at"]),
        )

    def _parse_reference(
        self, item: object, symbol: str
    ) -> ReferencePriceObservation:
        if not isinstance(item, dict) or item.get("source") != self.source_id:
            raise ValueError("reference-price source mismatch")
        return ReferencePriceObservation(
            symbol=symbol,
            price=_number(item["price"]),
            event_time=_utc(item["event_time"]),
            available_at=_utc(item["available_at"]),
            ingested_at=_utc(item["ingested_at"]),
            source=self.source_id,
        )


def _utc(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return parsed


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("numeric value is invalid")
    result = float(value)
    if not result == result or result in {float("inf"), float("-inf")}:
        raise ValueError("numeric value must be finite")
    return result

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from ..deadline import ensure_analysis_deadline
from ..domain import (
    BasisReference,
    Candle,
    ContractTransitionEvidence,
    FuturesEvidence,
    OrderBookLevel,
    ReferencePriceObservation,
    ReferencePriceSnapshot,
    SessionStatus,
)
from ..futures_policy import (
    APPROVED_BASIS_REFERENCE_TYPE,
    APPROVED_BASIS_SOURCE_ID,
)
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
    _bridge_schema_version = 3
    _contract_id_pattern = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}$")

    def __init__(
        self,
        *,
        bridge_url: str = "http://127.0.0.1:8784",
        bridge_token: str,
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
        if (
            not isinstance(bridge_token, str)
            or not 32 <= len(bridge_token) <= 256
            or any(character.isspace() for character in bridge_token)
        ):
            raise ValueError("T4 bridge token must contain 32-256 non-whitespace characters")
        self._bridge_url = bridge_url.rstrip("/")
        self._bridge_token = bridge_token
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
            headers={
                "Accept": "application/json",
                "X-Crypto-Agent-Bridge-Token": self._bridge_token,
            },
            method="GET",
        )
        ensure_analysis_deadline()
        try:
            response = build_opener(_NoRedirectHandler()).open(
                request, timeout=self._timeout_seconds
            )
            status = getattr(response, "status", 200)
            content_type = response.headers.get_content_type()
            if status != 200 or content_type != "application/json":
                raise ValueError("unexpected bridge response")
            payload_bytes = response.read(self._max_response_bytes + 1)
        except Exception as exc:
            raise ProviderError(
                "Plus500 T4 bridge is unavailable", code="T4_BRIDGE_UNAVAILABLE"
            ) from exc
        if len(payload_bytes) > self._max_response_bytes:
            raise ProviderError("T4 response is too large", code="T4_RESPONSE_TOO_LARGE")
        try:
            payload = json.loads(payload_bytes)
            if not isinstance(payload, dict):
                raise ValueError("payload is not an object")
            contract_expires_at = _utc(payload["contract_expires_at"])
            contract_roll_at = _utc(payload["contract_roll_at"])
            contract_id = payload.get("contract_id")
            contract_selection = payload.get("contract_selection")
            rolled_from_contract_id = payload.get("rolled_from_contract_id")
            if (
                type(payload.get("schema_version")) is not int
                or payload.get("schema_version") != self._bridge_schema_version
                or payload.get("source_id") != self.source_id
                or payload.get("venue_id") != self.venue_id
                or payload.get("read_only") is not True
                or payload.get("order_routes_exposed") is not False
                or payload.get("logical_symbol") != symbol
                or type(payload.get("interval_minutes")) is not int
                or payload.get("interval_minutes") != interval_minutes
                or not isinstance(contract_id, str)
                or self._contract_id_pattern.fullmatch(contract_id) is None
                or contract_selection not in {"front_month", "rolled"}
                or (
                    contract_selection == "front_month"
                    and rolled_from_contract_id is not None
                )
                or (
                    contract_selection == "rolled"
                    and (
                        not isinstance(rolled_from_contract_id, str)
                        or self._contract_id_pattern.fullmatch(rolled_from_contract_id)
                        is None
                        or rolled_from_contract_id == contract_id
                    )
                )
                or not as_of < contract_roll_at < contract_expires_at
            ):
                raise ValueError("bridge attestation mismatch")
            candles = tuple(
                self._parse_candle(item, symbol, interval_minutes)
                for item in payload["candles"]
            )
            reference = self._parse_reference(payload["reference_price"], symbol)
            futures_evidence = self._parse_futures_evidence(
                payload["futures_evidence"],
                symbol=symbol,
                contract_id=contract_id,
                contract_selection=contract_selection,
                rolled_from_contract_id=rolled_from_contract_id,
                as_of=as_of,
            )
        except (AttributeError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ProviderError("T4 payload is invalid", code="T4_PAYLOAD_INVALID") from exc
        if len(candles) < limit:
            raise ProviderError("T4 history is incomplete", code="T4_HISTORY_INCOMPLETE")
        if any(candle.close_time > as_of for candle in candles):
            raise ProviderError("T4 returned future data", code="T4_FUTURE_DATA")
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
                "t4_bridge_schema_version": payload.get("schema_version"),
                "t4_contract_id": payload.get("contract_id"),
                "t4_contract_expires_at": contract_expires_at.isoformat(),
                "t4_contract_roll_at": contract_roll_at.isoformat(),
                "t4_contract_selection": contract_selection,
                "t4_rolled_from_contract_id": rolled_from_contract_id,
                "t4_futures_evidence_attested": True,
            },
            reference_price=ReferencePriceSnapshot(
                symbol=symbol, observations=(reference,)
            ),
            raw_payload=payload_bytes,
            raw_payload_sha256=hashlib.sha256(payload_bytes).hexdigest(),
            futures_evidence=futures_evidence,
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
        if item.get("symbol") != symbol:
            raise ValueError("reference-price symbol mismatch")
        return ReferencePriceObservation(
            symbol=symbol,
            price=_number(item["price"]),
            event_time=_utc(item["event_time"]),
            available_at=_utc(item["available_at"]),
            ingested_at=_utc(item["ingested_at"]),
            source=self.source_id,
        )

    def _parse_futures_evidence(
        self,
        item: object,
        *,
        symbol: str,
        contract_id: str,
        contract_selection: str,
        rolled_from_contract_id: object,
        as_of: datetime,
    ) -> FuturesEvidence:
        if not isinstance(item, dict):
            raise ValueError("futures evidence is not an object")
        if (
            item.get("contract_id") != contract_id
            or item.get("source_id") != self.source_id
            or item.get("is_full_snapshot") is not True
        ):
            raise ValueError("futures evidence scope mismatch")
        try:
            session_status = SessionStatus(item["session_status"])
        except (KeyError, ValueError):
            raise ValueError("futures session status is invalid") from None
        observed_at = _utc(item["observed_at"])
        available_at = _utc(item["available_at"])
        ingested_at = _utc(item["ingested_at"])
        if not observed_at <= available_at <= ingested_at <= as_of:
            raise ValueError("futures evidence time order is invalid")

        bids = _parse_levels(item.get("bids"), side="bid")
        asks = _parse_levels(item.get("asks"), side="ask")
        if asks[0].price <= bids[0].price:
            raise ValueError("futures order book is crossed or locked")
        current_mid = bids[0].price + (asks[0].price - bids[0].price) / 2.0

        basis_item = item.get("basis_reference")
        if not isinstance(basis_item, dict):
            raise ValueError("basis reference is missing")
        basis_source = basis_item.get("source")
        if (
            basis_item.get("symbol") != symbol
            or basis_item.get("reference_type") != APPROVED_BASIS_REFERENCE_TYPE
            or basis_source != APPROVED_BASIS_SOURCE_ID
        ):
            raise ValueError("basis reference scope is invalid")
        basis_observed_at = _utc(basis_item["observed_at"])
        basis_available_at = _utc(basis_item["available_at"])
        basis_ingested_at = _utc(basis_item["ingested_at"])
        if not basis_observed_at <= basis_available_at <= basis_ingested_at <= as_of:
            raise ValueError("basis reference time order is invalid")
        basis = BasisReference(
            symbol=symbol,
            reference_type=str(basis_item["reference_type"]),
            source=basis_source,
            price=_positive_number(basis_item["price"]),
            observed_at=basis_observed_at,
            available_at=basis_available_at,
            ingested_at=basis_ingested_at,
        )

        transition_item = item.get("contract_transition")
        transition: ContractTransitionEvidence | None
        if contract_selection == "front_month":
            if transition_item is not None:
                raise ValueError("front-month evidence cannot contain a roll transition")
            transition = None
        else:
            if not isinstance(transition_item, dict):
                raise ValueError("rolled contract requires transition evidence")
            transition_observed_at = _utc(transition_item["observed_at"])
            transition_available_at = _utc(transition_item["available_at"])
            transition_ingested_at = _utc(transition_item["ingested_at"])
            if not (
                transition_item.get("from_contract_id") == rolled_from_contract_id
                and transition_item.get("to_contract_id") == contract_id
                and transition_item.get("price_type") == "mid"
                and transition_item.get("source") == self.source_id
                and math.isclose(
                    _positive_number(transition_item["to_price"]),
                    current_mid,
                    rel_tol=1e-12,
                    abs_tol=1e-12,
                )
                and transition_observed_at
                <= transition_available_at
                <= transition_ingested_at
                <= as_of
            ):
                raise ValueError("roll transition evidence is invalid")
            transition = ContractTransitionEvidence(
                from_contract_id=str(transition_item["from_contract_id"]),
                to_contract_id=contract_id,
                price_type="mid",
                from_price=_positive_number(transition_item["from_price"]),
                to_price=_positive_number(transition_item["to_price"]),
                source=self.source_id,
                observed_at=transition_observed_at,
                available_at=transition_available_at,
                ingested_at=transition_ingested_at,
            )

        return FuturesEvidence(
            contract_id=contract_id,
            source=self.source_id,
            session_status=session_status,
            is_full_snapshot=True,
            observed_at=observed_at,
            available_at=available_at,
            ingested_at=ingested_at,
            bids=bids,
            asks=asks,
            basis_reference=basis,
            contract_transition=transition,
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


def _positive_number(value: object) -> float:
    result = _number(value)
    if result <= 0:
        raise ValueError("numeric value must be positive")
    return result


def _parse_levels(value: object, *, side: str) -> tuple[OrderBookLevel, ...]:
    if not isinstance(value, list) or not 1 <= len(value) <= 50:
        raise ValueError("order-book side is incomplete")
    levels: list[OrderBookLevel] = []
    for expected_level, item in enumerate(value, start=1):
        if not isinstance(item, dict) or type(item.get("level")) is not int:
            raise ValueError("order-book level is invalid")
        if item["level"] != expected_level:
            raise ValueError("order-book levels are not contiguous")
        quantity = _number(item.get("quantity"))
        if quantity < 0:
            raise ValueError("order-book quantity is negative")
        levels.append(
            OrderBookLevel(
                level=expected_level,
                price=_positive_number(item.get("price")),
                quantity=quantity,
            )
        )
    prices = [item.price for item in levels]
    if side == "bid" and any(
        current >= previous
        for previous, current in zip(prices, prices[1:], strict=False)
    ):
        raise ValueError("bid levels are not strictly descending")
    if side == "ask" and any(
        current <= previous
        for previous, current in zip(prices, prices[1:], strict=False)
    ):
        raise ValueError("ask levels are not strictly ascending")
    return tuple(levels)

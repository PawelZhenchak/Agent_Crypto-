from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from . import __version__
from .analytics import calculate_metrics
from .deadline import ensure_analysis_deadline
from .domain import (
    Candle,
    DataQualityReport,
    Decision,
    MarketMetrics,
    ReferencePriceObservation,
    ReferencePriceSnapshot,
    ResearchReport,
    RiskAssessment,
)
from .futures_analytics import (
    FuturesAnalysis,
    analyze_futures,
    futures_evidence_to_dict,
)
from .futures_policy import FuturesAnalysisPolicy
from .policy import RiskGate, RiskPolicy
from .providers.base import CandleProvider, ProviderBatch, ProviderError, fetch_provider_batch
from .providers.t4 import Plus500T4Provider
from .quality import assess_data_quality, deduplicate_exact_candles
from .resource_paths import default_futures_policy_path
from .signals import VOLUME_ALERT_ZSCORE, apply_futures_gate, propose_research_alert
from .storage import ReportRepository
from .t4_ingest import T4ReplayProvider


SYSTEM_VERSION = f"{__version__}-plus500-t4-v1"
MODEL_VERSION = "deterministic-futures-research-v2"


class ResearchOrchestrator:
    def __init__(
        self,
        *,
        provider: CandleProvider,
        policy: RiskPolicy,
        repository: ReportRepository | None = None,
        futures_policy: FuturesAnalysisPolicy | None = None,
    ) -> None:
        policy.validate_v1_safety()
        self.provider = provider
        self.policy = policy
        self.risk_gate = RiskGate(policy)
        self.repository = repository
        self.futures_policy = futures_policy or FuturesAnalysisPolicy.load(
            default_futures_policy_path()
        )

    def analyze(
        self,
        *,
        symbol: str,
        interval_minutes: int = 1440,
        as_of: datetime | None = None,
        limit: int = 120,
    ) -> ResearchReport:
        ensure_analysis_deadline()
        started_at = datetime.now(timezone.utc)
        requested_cutoff = as_of or started_at
        if requested_cutoff.tzinfo is None or requested_cutoff.utcoffset() != timedelta(0):
            raise ValueError("as_of must be timezone-aware and normalized to UTC")

        candles: list[Candle] = []
        provider_error: str | None = None
        provider_error_code: str | None = None
        provider_batch: ProviderBatch | None = None
        preflight_flags: list[str] = []
        if requested_cutoff > started_at + timedelta(seconds=self.policy.max_clock_skew_seconds):
            preflight_flags.append("FUTURE_AS_OF")
        if symbol not in self.policy.allowed_assets:
            preflight_flags.append("REQUEST_OUTSIDE_POLICY")
        if interval_minutes not in self.policy.allowed_intervals_minutes:
            preflight_flags.append("REQUEST_OUTSIDE_POLICY")

        if preflight_flags:
            quality = _failure_quality(*preflight_flags)
            analysis_time = started_at
        else:
            try:
                provider_batch = fetch_provider_batch(
                    self.provider,
                    symbol=symbol,
                    interval_minutes=interval_minutes,
                    as_of=requested_cutoff,
                    limit=max(limit, self.policy.min_samples),
                )
                candles = list(provider_batch.candles)
            except ProviderError as exc:
                provider_error = str(exc)
                provider_error_code = exc.code
                if exc.evidence is not None:
                    provider_batch = ProviderBatch(
                        candles=(),
                        input_candles=exc.evidence.input_candles,
                        sources=exc.evidence.sources,
                        metadata={
                            **exc.evidence.metadata,
                            "provider_failure_code": exc.code,
                        },
                        reference_price=exc.evidence.reference_price,
                        futures_evidence=exc.evidence.futures_evidence,
                    )
                quality = _failure_quality(exc.code)
                analysis_time = as_of or datetime.now(timezone.utc)
            else:
                # Live knowledge is sealed after ingestion. An explicit replay cutoff stays fixed.
                analysis_time = as_of or datetime.now(timezone.utc)
                quality = assess_data_quality(
                    candles,
                    as_of=analysis_time,
                    interval_minutes=interval_minutes,
                    expected_symbol=symbol,
                    expected_source=self.provider.source_id,
                    policy=self.policy,
                )

        ensure_analysis_deadline()
        input_candles = (
            list(provider_batch.input_candles) if provider_batch is not None else candles
        )
        reference_price = (
            provider_batch.reference_price if provider_batch is not None else None
        )
        candle_snapshot = _canonical_input(input_candles)
        snapshot = _canonical_evidence(candle_snapshot, reference_price, provider_batch)
        snapshot.append(
            {
                "record_type": "analysis_context",
                "as_of": analysis_time.isoformat(),
                "futures_policy_id": self.futures_policy.policy_id,
                "futures_policy_hash_sha256": self.futures_policy.policy_hash_sha256,
            }
        )
        input_fingerprint = _input_fingerprint(snapshot)
        metrics_allowed = quality.passed and quality.score >= self.policy.min_data_quality
        analytics_candles = deduplicate_exact_candles(candles) if metrics_allowed else []
        metrics = self._safe_metrics(analytics_candles, metrics_allowed)
        source_attested = _source_attested(
            provider=self.provider,
            batch=provider_batch,
            required_sources=self.policy.required_source_count,
        )
        provider_metadata = provider_batch.metadata if provider_batch is not None else {}
        futures_analysis = analyze_futures(
            analytics_candles,
            provider_batch.futures_evidence if provider_batch is not None else None,
            as_of=analysis_time,
            symbol=symbol,
            contract_id=provider_metadata.get("t4_contract_id"),
            contract_expires_at=provider_metadata.get("t4_contract_expires_at"),
            contract_roll_at=provider_metadata.get("t4_contract_roll_at"),
            contract_selection=provider_metadata.get("t4_contract_selection"),
            rolled_from_contract_id=provider_metadata.get("t4_rolled_from_contract_id"),
            policy=self.futures_policy,
        )
        volume_anomaly_attested = _volume_anomaly_attested(
            futures_analysis,
            source_attested=source_attested,
        )
        proposal = apply_futures_gate(
            propose_research_alert(
                metrics,
                volume_anomaly_attested=volume_anomaly_attested,
            ),
            futures_analysis,
        )
        expires_at = analysis_time + timedelta(seconds=self.policy.report_ttl_seconds)
        risk = self.risk_gate.evaluate(
            symbol=symbol,
            interval_minutes=interval_minutes,
            quality=quality,
            metrics=metrics,
            reference_price=reference_price,
            as_of=analysis_time,
            expires_at=expires_at,
            input_fingerprint_sha256=input_fingerprint,
            source_attested=source_attested,
            proposed_decision=proposal.decision,
            proposal_reasons=proposal.reasons,
            additional_veto_flags=futures_analysis.veto_flags,
            additional_veto_reasons=(
                "The deterministic futures-evidence gate did not pass.",
            )
            if futures_analysis.veto_flags
            else (),
        )
        reason_codes = tuple(
            dict.fromkeys(
                (*risk.flags, *proposal.reason_codes, *futures_analysis.reason_codes)
            )
        )
        instrument_id = f"{self.provider.source_id}:{symbol}:{interval_minutes}m"
        report = ResearchReport(
            decision_id=str(uuid4()),
            trace_id=str(uuid4()),
            as_of=analysis_time,
            expires_at=expires_at,
            asset_id=_canonical_asset_id(symbol),
            instrument_id=instrument_id,
            horizon=_horizon(interval_minutes),
            decision=risk.decision,
            reason_codes=tuple(reason_codes),
            regime_probabilities=(),
            model_version=MODEL_VERSION,
            policy_version=self.policy.policy_id,
            data_snapshot_id=f"sha256:{input_fingerprint}",
            thesis=_thesis(metrics, risk),
            counter_evidence=(
                *_counter_evidence(metrics, quality.flags),
                *futures_analysis.counter_evidence,
            ),
            scenarios=_scenarios(metrics, futures_analysis),
            invalidation_conditions=futures_analysis.invalidation_conditions,
            data_quality=quality,
            risk=risk,
            sources=(
                provider_batch.sources
                if provider_batch is not None
                else (
                    {
                        "id": self.provider.source_id,
                        "kind": "market_data",
                        "trust": "untrusted_external_data",
                    },
                )
            ),
            metrics=metrics,
            futures_metrics=futures_analysis.metrics,
            metadata={
                "system_version": SYSTEM_VERSION,
                "mode": self.policy.mode,
                "execution_enabled": False,
                "not_financial_advice": True,
                "v1_gate_passed": False,
                "plus500_t4_source_attested": source_attested,
                "plus500_t4_volume_anomaly_attested": volume_anomaly_attested,
                "futures_gate_passed": futures_analysis.gate_passed,
                "futures_policy_id": self.futures_policy.policy_id,
                "futures_policy_hash_sha256": self.futures_policy.policy_hash_sha256,
                "market": instrument_id,
                "requested_as_of": requested_cutoff.isoformat(),
                "input_fingerprint_sha256": input_fingerprint,
                "input_candle_count": len(input_candles),
                "analysis_candle_count": len(candles),
                "input_first_available_at": _timestamp_bound(input_candles, minimum=True),
                "input_last_available_at": _timestamp_bound(input_candles, minimum=False),
                "reference_price_present": reference_price is not None,
                "reference_price_evidence": _canonical_reference_price(reference_price),
                "provider_error": provider_error,
                "provider_error_code": provider_error_code,
                "provider_diagnostics": (
                    provider_batch.metadata if provider_batch is not None else {}
                ),
            },
        )
        ensure_analysis_deadline()
        if self.repository is not None:
            self.repository.save(report, input_snapshot=snapshot)
        return report

    @staticmethod
    def _safe_metrics(candles: list[Candle], quality_passed: bool) -> MarketMetrics | None:
        if not quality_passed or len(candles) < 50:
            return None
        try:
            metrics = calculate_metrics(candles)
        except (ValueError, ArithmeticError, OverflowError, ZeroDivisionError):
            return None
        numeric_values = (
            metrics.last_price,
            metrics.period_return,
            metrics.return_7_periods,
            metrics.annualized_volatility,
            metrics.max_drawdown,
            metrics.sma_20,
            metrics.sma_50,
            metrics.volume_zscore,
        )
        return metrics if all(math.isfinite(value) for value in numeric_values) else None


def _horizon(interval_minutes: int) -> str:
    return {240: "4h", 1440: "1d", 10080: "1w"}.get(
        interval_minutes, f"{interval_minutes}m"
    )


def _thesis(metrics: MarketMetrics | None, risk: RiskAssessment) -> str:
    if risk.vetoed:
        return "Brak wiarygodnego sygnału: risk gate zatrzymał interpretację rynku."
    if metrics is None or risk.decision is Decision.NO_SIGNAL:
        return "Brak kwalifikowanego alertu badawczego; system pozostaje w NO_SIGNAL."
    return (
        f"Wykryty reżim {metrics.regime.value}; raport ma status ALERT wyłącznie jako "
        "alert badawczy, nie rekomendacja kupna ani sprzedaży."
    )


def _canonical_asset_id(symbol: str) -> str:
    return {
        "BTC/USD": "bip122:000000000019d6689c085ae165831e93:native",
        "ETH/USD": "eip155:1:native",
    }.get(symbol, f"unresolved:{symbol}")


def _canonical_input(candles: list[Candle]) -> list[dict[str, object]]:
    return [
        {
            "symbol": candle.symbol,
            "interval_minutes": candle.interval_minutes,
            "open_time": _safe_timestamp(candle.open_time),
            "close_time": _safe_timestamp(candle.close_time),
            "open": _safe_number(candle.open),
            "high": _safe_number(candle.high),
            "low": _safe_number(candle.low),
            "close": _safe_number(candle.close),
            "volume": _safe_number(candle.volume),
            "source": candle.source,
            "available_at": _safe_timestamp(candle.available_at),
            "ingested_at": _safe_timestamp(candle.ingested_at),
        }
        for candle in sorted(
            candles,
            key=lambda item: (
                _safe_timestamp(item.open_time),
                _safe_timestamp(item.close_time),
                str(item.source),
                str(item.symbol),
            ),
        )
    ]


def _canonical_evidence(
    candle_snapshot: list[dict[str, object]],
    reference_price: ReferencePriceSnapshot | None,
    batch: ProviderBatch | None,
) -> list[dict[str, object]]:
    reference = _canonical_reference_price(reference_price)
    records: list[dict[str, object]] = list(candle_snapshot)
    if reference is not None:
        records.append(
            {
                "record_type": "reference_price_snapshot",
                **reference,
            }
        )
    if batch is not None:
        metadata = batch.metadata
        lifecycle_keys = (
            "t4_bridge_schema_version",
            "t4_source_id",
            "t4_venue_id",
            "t4_contract_id",
            "t4_contract_expires_at",
            "t4_contract_roll_at",
            "t4_contract_selection",
            "t4_rolled_from_contract_id",
        )
        if any(key in metadata for key in lifecycle_keys):
            records.append(
                {
                    "record_type": "t4_contract_lifecycle",
                    **{key: metadata.get(key) for key in lifecycle_keys},
                }
            )
        futures_evidence = futures_evidence_to_dict(batch.futures_evidence)
        if futures_evidence is not None:
            records.append(
                {
                    "record_type": "t4_futures_evidence",
                    **futures_evidence,
                }
            )
    return records


def _canonical_reference_price(
    snapshot: ReferencePriceSnapshot | None,
) -> dict[str, object] | None:
    if not isinstance(snapshot, ReferencePriceSnapshot):
        return None
    return {
        "symbol": snapshot.symbol,
        "observations": [
            {
                "symbol": item.symbol,
                "price": _safe_number(item.price),
                "event_time": _safe_timestamp(item.event_time),
                "available_at": _safe_timestamp(item.available_at),
                "ingested_at": _safe_timestamp(item.ingested_at),
                "source": item.source,
            }
            for item in sorted(
                snapshot.observations,
                key=lambda observation: (
                    str(observation.source),
                    _safe_timestamp(observation.event_time),
                ),
            )
        ],
    }


def _source_attested(
    *,
    provider: CandleProvider,
    batch: ProviderBatch | None,
    required_sources: int,
) -> bool:
    if type(provider) not in {Plus500T4Provider, T4ReplayProvider} or batch is None:
        return False
    try:
        source_ids = tuple(item.get("id") for item in batch.sources)
        reference = batch.reference_price
        schema_version = batch.metadata.get("t4_bridge_schema_version")
        futures_evidence_valid = bool(
            (
                schema_version == 2
                and type(provider) is T4ReplayProvider
                and batch.futures_evidence is None
            )
            or (
                schema_version == 3
                and batch.metadata.get("t4_futures_evidence_attested") is True
                and batch.futures_evidence is not None
            )
        )
        return bool(
            type(required_sources) is int
            and required_sources == 1
            and source_ids == (Plus500T4Provider.source_id,)
            and len(batch.candles) >= 120
            and batch.candles == batch.input_candles
            and all(
                type(item) is Candle and item.source == Plus500T4Provider.source_id
                for item in batch.candles
            )
            and type(reference) is ReferencePriceSnapshot
            and len(reference.observations) == 1
            and type(reference.observations[0]) is ReferencePriceObservation
            and reference.observations[0].source == Plus500T4Provider.source_id
            and batch.metadata.get("t4_read_only_attested") is True
            and batch.metadata.get("t4_source_id") == Plus500T4Provider.source_id
            and batch.metadata.get("t4_venue_id") == Plus500T4Provider.venue_id
            and batch.metadata.get("t4_order_routes_exposed") is False
            and schema_version in {2, 3}
            and futures_evidence_valid
            and isinstance(batch.metadata.get("t4_contract_id"), str)
            and bool(batch.metadata.get("t4_contract_id"))
            and isinstance(batch.metadata.get("t4_contract_roll_at"), str)
            and isinstance(batch.metadata.get("t4_contract_expires_at"), str)
            and batch.metadata.get("t4_contract_selection") in {"front_month", "rolled"}
            and (
                (
                    batch.metadata.get("t4_contract_selection") == "front_month"
                    and batch.metadata.get("t4_rolled_from_contract_id") is None
                )
                or (
                    batch.metadata.get("t4_contract_selection") == "rolled"
                    and isinstance(
                        batch.metadata.get("t4_rolled_from_contract_id"), str
                    )
                    and bool(batch.metadata.get("t4_rolled_from_contract_id"))
                    and batch.metadata.get("t4_rolled_from_contract_id")
                    != batch.metadata.get("t4_contract_id")
                )
            )
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def _input_fingerprint(snapshot: list[dict[str, object]]) -> str:
    encoded = json.dumps(
        snapshot,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _volume_anomaly_attested(
    futures: FuturesAnalysis,
    *,
    source_attested: bool,
) -> bool:
    if source_attested is not True:
        return False
    value = futures.metrics.volume_zscore
    return bool(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        and abs(float(value)) >= VOLUME_ALERT_ZSCORE
    )


def _safe_number(value: object) -> float | int | str:
    if (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
    ):
        return value
    return str(value)


def _safe_timestamp(value: object) -> str:
    return value.isoformat() if isinstance(value, datetime) else str(value)


def _timestamp_bound(candles: list[Candle], *, minimum: bool) -> str | None:
    values = [_safe_timestamp(candle.available_at) for candle in candles]
    if not values:
        return None
    return (min(values) if minimum else max(values))


def _failure_quality(*flags: str) -> DataQualityReport:
    deduplicated = tuple(dict.fromkeys(flags))
    return DataQualityReport(
        score=0.0,
        sample_count=0,
        flags=deduplicated,
        critical_flags=deduplicated,
        newest_observed_at=None,
        newest_available_at=None,
    )


def _counter_evidence(
    metrics: MarketMetrics | None, quality_flags: tuple[str, ...]
) -> tuple[str, ...]:
    evidence = [f"Flagi jakości danych: {', '.join(quality_flags) or 'brak' }."]
    if metrics is not None:
        evidence.extend(
            (
                f"Maksymalne obsunięcie w oknie: {metrics.max_drawdown:.2%}.",
                f"Annualizowana zmienność: {metrics.annualized_volatility:.2%}.",
                "Klasyfikacja reżimu jest opisowa i może zmienić się po kolejnej świecy.",
            )
        )
    return tuple(evidence)


def _scenarios(
    metrics: MarketMetrics | None,
    futures: FuturesAnalysis,
) -> tuple[dict[str, object], ...]:
    if metrics is None:
        return (
            {
                "name": "data_failure",
                "condition": "Dane nie przechodzą kontroli jakości lub bramki futures.",
                "probability": None,
                "active": True,
            },
        )
    trend_ratio = metrics.sma_20 / metrics.sma_50
    bull_active = bool(
        futures.gate_passed
        and trend_ratio > 1.02
        and metrics.last_price > metrics.sma_20
        and metrics.return_7_periods > 0
    )
    bear_active = bool(
        futures.gate_passed
        and trend_ratio < 0.98
        and metrics.last_price < metrics.sma_20
        and metrics.return_7_periods < 0
    )
    base_active = bool(futures.gate_passed and 0.98 <= trend_ratio <= 1.02)
    return (
        {
            "name": "bull",
            "condition": "SMA20/SMA50 > 1.02, close > SMA20 i zwrot 7 okresów > 0.",
            "probability": None,
            "active": bull_active,
        },
        {
            "name": "base",
            "condition": "SMA20/SMA50 pozostaje w przedziale [0.98, 1.02].",
            "probability": None,
            "active": base_active,
        },
        {
            "name": "bear",
            "condition": "SMA20/SMA50 < 0.98, close < SMA20 i zwrot 7 okresów < 0.",
            "probability": None,
            "active": bear_active,
        },
        {
            "name": "data_failure",
            "condition": "Dowody są opóźnione, sprzeczne, niepełne albo niedostępne.",
            "probability": None,
            "active": not futures.gate_passed,
        },
    )

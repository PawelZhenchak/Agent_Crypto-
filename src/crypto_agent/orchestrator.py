from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from . import __version__
from .analytics import calculate_metrics
from .consensus_math import CONSENSUS_ALGORITHM_VERSION, CONSENSUS_WINDOW_SIZE
from .deadline import ensure_analysis_deadline
from .domain import (
    Candle,
    DataQualityReport,
    Decision,
    MarketMetrics,
    ResearchReport,
    RiskAssessment,
)
from .policy import RiskGate, RiskPolicy
from .providers.base import CandleProvider, ProviderBatch, ProviderError, fetch_provider_batch
from .providers.consensus import CrossExchangeConsensusProvider
from .quality import assess_data_quality, deduplicate_exact_candles
from .signals import VOLUME_ALERT_ZSCORE, propose_research_alert
from .storage import ReportRepository


SYSTEM_VERSION = f"{__version__}-v1.1"
MODEL_VERSION = "deterministic-research-v1"


class ResearchOrchestrator:
    def __init__(
        self,
        *,
        provider: CandleProvider,
        policy: RiskPolicy,
        repository: ReportRepository | None = None,
    ) -> None:
        policy.validate_v1_safety()
        if (
            isinstance(provider, CrossExchangeConsensusProvider)
            and provider.policy.fingerprint() != policy.fingerprint()
        ):
            raise ValueError(
                "Consensus provider and orchestrator must use the same risk policy"
            )
        self.provider = provider
        self.policy = policy
        self.risk_gate = RiskGate(policy)
        self.repository = repository

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
                            "consensus_failure_code": exc.code,
                        },
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
        snapshot = _canonical_input(input_candles)
        input_fingerprint = _input_fingerprint(snapshot)
        metrics_allowed = quality.passed and quality.score >= self.policy.min_data_quality
        analytics_candles = deduplicate_exact_candles(candles) if metrics_allowed else []
        metrics = self._safe_metrics(analytics_candles, metrics_allowed)
        consensus_passed = _consensus_attested(
            provider=self.provider,
            batch=provider_batch,
            required_sources=self.policy.min_consensus_sources,
            required_policy_hash=self.policy.fingerprint(),
        )
        volume_anomaly_attested = _volume_anomaly_attested(
            provider_batch,
            required_sources=self.policy.min_consensus_sources,
            consensus_attested=consensus_passed,
        )
        proposal = propose_research_alert(
            metrics,
            volume_anomaly_attested=volume_anomaly_attested,
        )
        expires_at = analysis_time + timedelta(seconds=self.policy.report_ttl_seconds)
        risk = self.risk_gate.evaluate(
            symbol=symbol,
            interval_minutes=interval_minutes,
            quality=quality,
            metrics=metrics,
            as_of=analysis_time,
            expires_at=expires_at,
            input_fingerprint_sha256=input_fingerprint,
            consensus_passed=consensus_passed,
            proposed_decision=proposal.decision,
            proposal_reasons=proposal.reasons,
        )
        reason_codes = tuple(dict.fromkeys((*risk.flags, *proposal.reason_codes)))
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
            counter_evidence=_counter_evidence(metrics, quality.flags),
            scenarios=_scenarios(metrics),
            invalidation_conditions=(
                "Pojawienie się nowszych danych niż as_of.",
                "Spadek jakości danych poniżej progu polityki.",
                "Zmiana sklasyfikowanego reżimu lub aktywacja risk veto.",
            ),
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
            metadata={
                "system_version": SYSTEM_VERSION,
                "mode": self.policy.mode,
                "execution_enabled": False,
                "not_financial_advice": True,
                "v1_gate_passed": False,
                "v1_1_consensus_passed": consensus_passed,
                "v1_1_volume_anomaly_attested": volume_anomaly_attested,
                "market": instrument_id,
                "requested_as_of": requested_cutoff.isoformat(),
                "input_fingerprint_sha256": input_fingerprint,
                "input_candle_count": len(input_candles),
                "analysis_candle_count": len(candles),
                "input_first_available_at": _timestamp_bound(input_candles, minimum=True),
                "input_last_available_at": _timestamp_bound(input_candles, minimum=False),
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


def _consensus_attested(
    *,
    provider: CandleProvider,
    batch: ProviderBatch | None,
    required_sources: int,
    required_policy_hash: str,
) -> bool:
    if type(provider) is not CrossExchangeConsensusProvider or batch is None:
        return False
    try:
        sealed_source_ids = tuple(item.source_id for item in provider.providers)
        if not all(
            isinstance(item, dict) and isinstance(item.get("id"), str)
            for item in batch.sources
        ):
            return False
        source_ids = tuple(item["id"] for item in batch.sources)
        diagnostic_ids = batch.metadata.get("consensus_source_ids")
        source_counts = batch.metadata.get("consensus_source_counts")
        overlap = batch.metadata.get("consensus_overlap_count")
        window_size = batch.metadata.get("consensus_window_size")

        if not (
            provider.approved_venue_pair is True
            and provider.source_id == CONSENSUS_ALGORITHM_VERSION
            and batch.metadata.get("consensus_passed") is True
            and batch.metadata.get("consensus_version")
            == CONSENSUS_ALGORITHM_VERSION
            and batch.metadata.get("consensus_policy_hash") == required_policy_hash
            and type(required_sources) is int
            and required_sources == 2
            and len(source_ids) == required_sources
            and source_ids == sealed_source_ids
            and len(set(source_ids)) == required_sources
            and all(source_ids)
            and isinstance(diagnostic_ids, list)
            and tuple(diagnostic_ids) == sealed_source_ids
            and type(overlap) is int
            and overlap == CONSENSUS_WINDOW_SIZE
            and type(window_size) is int
            and window_size == CONSENSUS_WINDOW_SIZE
            and isinstance(source_counts, dict)
            and set(source_counts) == set(sealed_source_ids)
            and all(
                type(source_counts[source_id]) is int
                and source_counts[source_id] >= CONSENSUS_WINDOW_SIZE
                for source_id in sealed_source_ids
            )
        ):
            return False

        canonical_candles = batch.candles
        raw_candles = batch.input_candles
        if (
            len(canonical_candles) != CONSENSUS_WINDOW_SIZE
            or len(raw_candles) != required_sources * CONSENSUS_WINDOW_SIZE
            or not all(isinstance(item, Candle) for item in canonical_candles)
            or not all(isinstance(item, Candle) for item in raw_candles)
        ):
            return False

        canonical_windows = {
            (item.open_time, item.close_time) for item in canonical_candles
        }
        canonical_scopes = {
            (item.symbol, item.interval_minutes) for item in canonical_candles
        }
        if (
            len(canonical_windows) != CONSENSUS_WINDOW_SIZE
            or len(canonical_scopes) != 1
            or any(
                item.source != CONSENSUS_ALGORITHM_VERSION
                for item in canonical_candles
            )
        ):
            return False

        expected_scope = next(iter(canonical_scopes))
        raw_by_source: dict[str, list[Candle]] = {
            source_id: [] for source_id in sealed_source_ids
        }
        for item in raw_candles:
            if (
                item.source not in raw_by_source
                or (item.symbol, item.interval_minutes) != expected_scope
            ):
                return False
            raw_by_source[item.source].append(item)
        return all(
            len(items) == CONSENSUS_WINDOW_SIZE
            and {(item.open_time, item.close_time) for item in items}
            == canonical_windows
            for items in raw_by_source.values()
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
    batch: ProviderBatch | None,
    *,
    required_sources: int,
    consensus_attested: bool,
) -> bool:
    if (
        consensus_attested is not True
        or batch is None
        or batch.metadata.get("consensus_passed") is not True
    ):
        return False
    raw = batch.metadata.get("consensus_volume_zscores")
    if not isinstance(raw, dict) or len(raw) != required_sources:
        return False
    source_ids = {
        item.get("id") for item in batch.sources if isinstance(item, dict)
    }
    if set(raw) != source_ids:
        return False
    values = tuple(raw.values())
    if not all(
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(float(value))
        for value in values
    ):
        return False
    numeric = tuple(float(value) for value in values)
    return bool(
        all(value >= VOLUME_ALERT_ZSCORE for value in numeric)
        or all(value <= -VOLUME_ALERT_ZSCORE for value in numeric)
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


def _scenarios(metrics: MarketMetrics | None) -> tuple[dict[str, object], ...]:
    if metrics is None:
        return (
            {
                "name": "no_signal",
                "condition": "Dane nie przechodzą kontroli jakości lub ryzyka.",
                "probability": None,
            },
        )
    return (
        {
            "name": "base",
            "condition": f"Reżim {metrics.regime.value} pozostaje aktywny.",
            "probability": None,
        },
        {
            "name": "adverse",
            "condition": "Zmienność rośnie lub cena narusza warunki unieważnienia.",
            "probability": None,
        },
        {
            "name": "data_failure",
            "condition": "Źródła stają się opóźnione, sprzeczne albo niedostępne.",
            "probability": None,
        },
    )

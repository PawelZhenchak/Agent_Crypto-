using System.Text.Json.Serialization;

namespace CryptoAgent.T4Bridge;

public sealed record MarketDataRequest(
    string LogicalSymbol,
    string ContractId,
    DateTimeOffset ContractExpiresAt,
    DateTimeOffset ContractRollAt,
    string? RolledFromContractId,
    int IntervalMinutes,
    DateTimeOffset AsOf,
    int Limit);

public sealed record MarketDataEnvelope(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("source_id")] string SourceId,
    [property: JsonPropertyName("venue_id")] string VenueId,
    [property: JsonPropertyName("read_only")] bool ReadOnly,
    [property: JsonPropertyName("order_routes_exposed")] bool OrderRoutesExposed,
    [property: JsonPropertyName("environment")] string Environment,
    [property: JsonPropertyName("logical_symbol")] string LogicalSymbol,
    [property: JsonPropertyName("interval_minutes")] int IntervalMinutes,
    [property: JsonPropertyName("contract_id")] string ContractId,
    [property: JsonPropertyName("contract_expires_at")] DateTimeOffset ContractExpiresAt,
    [property: JsonPropertyName("contract_roll_at")] DateTimeOffset ContractRollAt,
    [property: JsonPropertyName("contract_selection")] string ContractSelection,
    [property: JsonPropertyName("rolled_from_contract_id")] string? RolledFromContractId,
    [property: JsonPropertyName("volume_zscore")] double? VolumeZScore,
    [property: JsonPropertyName("candles")] IReadOnlyList<CandleEnvelope> Candles,
    [property: JsonPropertyName("reference_price")] ReferencePriceEnvelope ReferencePrice,
    [property: JsonPropertyName("futures_evidence")] FuturesEvidenceEnvelope FuturesEvidence);

public sealed record CandleEnvelope(
    [property: JsonPropertyName("symbol")] string Symbol,
    [property: JsonPropertyName("interval_minutes")] int IntervalMinutes,
    [property: JsonPropertyName("open_time")] DateTimeOffset OpenTime,
    [property: JsonPropertyName("close_time")] DateTimeOffset CloseTime,
    [property: JsonPropertyName("open")] double Open,
    [property: JsonPropertyName("high")] double High,
    [property: JsonPropertyName("low")] double Low,
    [property: JsonPropertyName("close")] double Close,
    [property: JsonPropertyName("volume")] double Volume,
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("available_at")] DateTimeOffset AvailableAt,
    [property: JsonPropertyName("ingested_at")] DateTimeOffset IngestedAt);

public sealed record ReferencePriceEnvelope(
    [property: JsonPropertyName("symbol")] string Symbol,
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("price")] double Price,
    [property: JsonPropertyName("event_time")] DateTimeOffset EventTime,
    [property: JsonPropertyName("available_at")] DateTimeOffset AvailableAt,
    [property: JsonPropertyName("ingested_at")] DateTimeOffset IngestedAt);

public sealed record FuturesEvidenceEnvelope(
    [property: JsonPropertyName("contract_id")] string ContractId,
    [property: JsonPropertyName("source_id")] string SourceId,
    [property: JsonPropertyName("session_status")] string SessionStatus,
    [property: JsonPropertyName("is_full_snapshot")] bool IsFullSnapshot,
    [property: JsonPropertyName("observed_at")] DateTimeOffset ObservedAt,
    [property: JsonPropertyName("available_at")] DateTimeOffset AvailableAt,
    [property: JsonPropertyName("ingested_at")] DateTimeOffset IngestedAt,
    [property: JsonPropertyName("bids")] IReadOnlyList<OrderBookLevelEnvelope> Bids,
    [property: JsonPropertyName("asks")] IReadOnlyList<OrderBookLevelEnvelope> Asks,
    [property: JsonPropertyName("basis_reference")] BasisReferenceEnvelope BasisReference,
    [property: JsonPropertyName("contract_transition")] ContractTransitionEnvelope? ContractTransition);

public sealed record OrderBookLevelEnvelope(
    [property: JsonPropertyName("level")] int Level,
    [property: JsonPropertyName("price")] double Price,
    [property: JsonPropertyName("quantity")] double Quantity);

public sealed record BasisReferenceEnvelope(
    [property: JsonPropertyName("symbol")] string Symbol,
    [property: JsonPropertyName("reference_type")] string ReferenceType,
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("price")] double Price,
    [property: JsonPropertyName("observed_at")] DateTimeOffset ObservedAt,
    [property: JsonPropertyName("available_at")] DateTimeOffset AvailableAt,
    [property: JsonPropertyName("ingested_at")] DateTimeOffset IngestedAt);

public sealed record ContractTransitionEnvelope(
    [property: JsonPropertyName("from_contract_id")] string FromContractId,
    [property: JsonPropertyName("to_contract_id")] string ToContractId,
    [property: JsonPropertyName("price_type")] string PriceType,
    [property: JsonPropertyName("from_price")] double FromPrice,
    [property: JsonPropertyName("to_price")] double ToPrice,
    [property: JsonPropertyName("source")] string Source,
    [property: JsonPropertyName("observed_at")] DateTimeOffset ObservedAt,
    [property: JsonPropertyName("available_at")] DateTimeOffset AvailableAt,
    [property: JsonPropertyName("ingested_at")] DateTimeOffset IngestedAt);

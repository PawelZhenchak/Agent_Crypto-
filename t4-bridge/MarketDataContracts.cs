using System.Text.Json.Serialization;

namespace CryptoAgent.T4Bridge;

public sealed record MarketDataRequest(
    string LogicalSymbol,
    string ContractId,
    DateTimeOffset ContractExpiresAt,
    int IntervalMinutes,
    DateTimeOffset AsOf,
    int Limit);

public sealed record MarketDataEnvelope(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("source_id")] string SourceId,
    [property: JsonPropertyName("venue_id")] string VenueId,
    [property: JsonPropertyName("read_only")] bool ReadOnly,
    [property: JsonPropertyName("order_routes_exposed")] bool OrderRoutesExposed,
    [property: JsonPropertyName("logical_symbol")] string LogicalSymbol,
    [property: JsonPropertyName("interval_minutes")] int IntervalMinutes,
    [property: JsonPropertyName("contract_id")] string ContractId,
    [property: JsonPropertyName("contract_expires_at")] DateTimeOffset ContractExpiresAt,
    [property: JsonPropertyName("volume_zscore")] double? VolumeZScore,
    [property: JsonPropertyName("candles")] IReadOnlyList<CandleEnvelope> Candles,
    [property: JsonPropertyName("reference_price")] ReferencePriceEnvelope ReferencePrice);

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

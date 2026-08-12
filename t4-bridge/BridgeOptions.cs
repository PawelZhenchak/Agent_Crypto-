using System.Text.Json;
using System.Text.Json.Serialization;

namespace CryptoAgent.T4Bridge;

public sealed class BridgeOptions
{
    public const string TokenEnvironmentName = "T4_BRIDGE_TOKEN";
    public const string CatalogPathEnvironmentName = "T4_CONTRACT_CATALOG_PATH";
    private const int MaximumCatalogBytes = 1_000_000;
    private const int LegacyRollDays = 5;

    private BridgeOptions(int port, string token, FuturesContractCatalog contractCatalog)
    {
        Port = port;
        Token = token;
        ContractCatalog = contractCatalog;
    }

    public int Port { get; }

    public string Token { get; }

    public FuturesContractCatalog ContractCatalog { get; }

    public static BridgeOptions FromEnvironment()
    {
        var token = Environment.GetEnvironmentVariable(TokenEnvironmentName) ?? string.Empty;
        if (token.Length is < 32 or > 256 || token.Any(char.IsWhiteSpace))
        {
            throw new InvalidOperationException(
                $"{TokenEnvironmentName} must contain 32-256 non-whitespace characters.");
        }

        var rawPort = Environment.GetEnvironmentVariable("T4_BRIDGE_PORT") ?? "8784";
        if (!int.TryParse(rawPort, out var port) || port is < 1024 or > 65535)
        {
            throw new InvalidOperationException("T4_BRIDGE_PORT must be between 1024 and 65535.");
        }

        return new BridgeOptions(port, token, LoadContractCatalog());
    }

    private static FuturesContractCatalog LoadContractCatalog()
    {
        var catalogPath = Environment.GetEnvironmentVariable(CatalogPathEnvironmentName);
        if (string.IsNullOrWhiteSpace(catalogPath))
        {
            return new FuturesContractCatalog(
                new[] { ReadLegacyContract("BTC", "BTC/USD"), ReadLegacyContract("ETH", "ETH/USD") }
                    .OfType<FuturesContract>());
        }

        var file = new FileInfo(Path.GetFullPath(catalogPath));
        if (!file.Exists || file.Length is <= 0 or > MaximumCatalogBytes)
        {
            throw new InvalidOperationException(
                "The T4 contract catalog file is missing or invalid.");
        }

        ContractCatalogDocument? document;
        try
        {
            using var stream = file.OpenRead();
            document = JsonSerializer.Deserialize<ContractCatalogDocument>(stream);
        }
        catch (JsonException exception)
        {
            throw new InvalidOperationException(
                "The T4 contract catalog is not valid JSON.", exception);
        }

        if (document is null || document.SchemaVersion != 1 ||
            document.DefaultRollDays is < 1 or > 30 || document.Contracts is null)
        {
            throw new InvalidOperationException("The T4 contract catalog schema is invalid.");
        }

        return new FuturesContractCatalog(document.Contracts.Select(item =>
        {
            var rollAt = item.RollAt ?? item.ExpiresAt.AddDays(-document.DefaultRollDays);
            return new FuturesContract(item.LogicalSymbol, item.ContractId, item.ExpiresAt, rollAt);
        }));
    }

    private static FuturesContract? ReadLegacyContract(string asset, string logicalSymbol)
    {
        var contractId = Environment.GetEnvironmentVariable(
            $"T4_{asset}_CONTRACT_ID") ?? string.Empty;
        var rawExpiry = Environment.GetEnvironmentVariable(
            $"T4_{asset}_CONTRACT_EXPIRES_AT") ?? string.Empty;
        if (string.IsNullOrWhiteSpace(contractId) ||
            !DateTimeOffset.TryParse(rawExpiry, out var expiresAt) ||
            expiresAt.Offset != TimeSpan.Zero)
        {
            return null;
        }

        return new FuturesContract(
            logicalSymbol,
            contractId,
            expiresAt,
            expiresAt.AddDays(-LegacyRollDays));
    }
}

public sealed record ContractCatalogDocument(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("default_roll_days")] int DefaultRollDays,
    [property: JsonPropertyName("contracts")] IReadOnlyList<ContractCatalogItem>? Contracts);

public sealed record ContractCatalogItem(
    [property: JsonPropertyName("logical_symbol")] string LogicalSymbol,
    [property: JsonPropertyName("contract_id")] string ContractId,
    [property: JsonPropertyName("expires_at")] DateTimeOffset ExpiresAt,
    [property: JsonPropertyName("roll_at")] DateTimeOffset? RollAt);

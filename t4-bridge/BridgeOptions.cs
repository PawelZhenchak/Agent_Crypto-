using System.Text.Json;
using System.Text.Json.Serialization;

namespace CryptoAgent.T4Bridge;

public sealed class BridgeOptions
{
    public const string TokenEnvironmentName = "T4_BRIDGE_TOKEN";
    public const string CatalogPathEnvironmentName = "T4_CONTRACT_CATALOG_PATH";
    private const int MaximumCatalogBytes = 1_000_000;
    private const int LegacyRollDays = 5;

    private BridgeOptions(
        int port,
        string token,
        FuturesContractCatalog contractCatalog,
        T4ApiOptions api)
    {
        Port = port;
        Token = token;
        ContractCatalog = contractCatalog;
        Api = api;
    }

    public int Port { get; }

    public string Token { get; }

    public FuturesContractCatalog ContractCatalog { get; }

    public T4ApiOptions Api { get; }

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

        var catalog = LoadContractCatalog();
        return new BridgeOptions(port, token, catalog, T4ApiOptions.FromEnvironment(catalog));
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

        if (document is null || document.SchemaVersion is not (1 or 2) ||
            document.DefaultRollDays is < 1 or > 30 || document.Contracts is null)
        {
            throw new InvalidOperationException("The T4 contract catalog schema is invalid.");
        }

        return new FuturesContractCatalog(document.Contracts.Select(item =>
        {
            var rollAt = item.RollAt ?? item.ExpiresAt.AddDays(-document.DefaultRollDays);
            return document.SchemaVersion == 1
                ? new FuturesContract(item.LogicalSymbol, item.ContractId, item.ExpiresAt, rollAt)
                : new FuturesContract(
                    item.LogicalSymbol,
                    item.ExchangeId ?? string.Empty,
                    item.ContractId,
                    item.MarketId ?? string.Empty,
                    item.ExpiresAt,
                    rollAt,
                    item.BasisExchangeId ?? string.Empty,
                    item.BasisContractId ?? string.Empty,
                    item.BasisMarketId ?? string.Empty);
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
    [property: JsonPropertyName("exchange_id")] string? ExchangeId,
    [property: JsonPropertyName("contract_id")] string ContractId,
    [property: JsonPropertyName("market_id")] string? MarketId,
    [property: JsonPropertyName("expires_at")] DateTimeOffset ExpiresAt,
    [property: JsonPropertyName("roll_at")] DateTimeOffset? RollAt,
    [property: JsonPropertyName("basis_exchange_id")] string? BasisExchangeId,
    [property: JsonPropertyName("basis_contract_id")] string? BasisContractId,
    [property: JsonPropertyName("basis_market_id")] string? BasisMarketId);

public enum T4ApiEnvironment
{
    Pending,
    Simulator,
    Live,
}

public sealed record T4ApiOptions(
    T4ApiEnvironment Environment,
    string ApiKey,
    Uri? WebSocketUri,
    Uri? RestUri)
{
    public const string EnvironmentVariableName = "T4_API_ENVIRONMENT";
    public const string ApiKeyEnvironmentName = "T4_API_KEY";
    public const string OfficialProtocolCommit =
        "1a68b674482194f1cf3b9d7f129ce5fbed8bcb51";

    public bool IsConfigured => Environment is not T4ApiEnvironment.Pending;

    public string AttestedEnvironment => Environment switch
    {
        T4ApiEnvironment.Simulator => "t4_simulator",
        T4ApiEnvironment.Live => "live_t4",
        _ => "not_configured",
    };

    public static T4ApiOptions FromEnvironment(FuturesContractCatalog catalog)
    {
        var rawEnvironment = System.Environment.GetEnvironmentVariable(EnvironmentVariableName) ??
            "pending";
        var environment = rawEnvironment switch
        {
            "pending" => T4ApiEnvironment.Pending,
            "simulator" => T4ApiEnvironment.Simulator,
            "live" => T4ApiEnvironment.Live,
            _ => throw new InvalidOperationException(
                $"{EnvironmentVariableName} must be pending, simulator, or live."),
        };
        var apiKey = System.Environment.GetEnvironmentVariable(ApiKeyEnvironmentName) ??
            string.Empty;
        if (environment is T4ApiEnvironment.Pending)
        {
            if (apiKey.Length > 0)
            {
                throw new InvalidOperationException(
                    $"{ApiKeyEnvironmentName} must not be set while T4 is pending.");
            }
            return new T4ApiOptions(environment, string.Empty, null, null);
        }
        if (apiKey.Length is < 16 or > 512 || apiKey.Any(char.IsWhiteSpace) ||
            apiKey.Any(char.IsControl))
        {
            throw new InvalidOperationException(
                $"{ApiKeyEnvironmentName} must contain 16-512 non-whitespace characters.");
        }
        if (!catalog.IsOfficiallyAddressable)
        {
            throw new InvalidOperationException(
                "The schema v2 T4 contract catalog is required for an official session.");
        }

        return environment switch
        {
            T4ApiEnvironment.Simulator => new T4ApiOptions(
                environment,
                apiKey,
                new Uri("wss://wss-sim.t4login.com/v1"),
                new Uri("https://api-sim.t4login.com/")),
            T4ApiEnvironment.Live => new T4ApiOptions(
                environment,
                apiKey,
                new Uri("wss://wss.t4login.com/v1"),
                new Uri("https://api.t4login.com/")),
            _ => throw new InvalidOperationException("Unsupported T4 API environment."),
        };
    }
}

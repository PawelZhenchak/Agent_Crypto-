using System.Collections.ObjectModel;

namespace CryptoAgent.T4Bridge;

public sealed class BridgeOptions
{
    public const string TokenEnvironmentName = "T4_BRIDGE_TOKEN";

    private BridgeOptions(int port, string token, IReadOnlyDictionary<string, ContractBinding> contracts)
    {
        Port = port;
        Token = token;
        Contracts = contracts;
    }

    public int Port { get; }

    public string Token { get; }

    public IReadOnlyDictionary<string, ContractBinding> Contracts { get; }

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

        var contracts = new Dictionary<string, ContractBinding>(StringComparer.Ordinal)
        {
            ["BTC/USD"] = ReadContract("BTC"),
            ["ETH/USD"] = ReadContract("ETH"),
        };
        return new BridgeOptions(
            port,
            token,
            new ReadOnlyDictionary<string, ContractBinding>(contracts));
    }

    private static ContractBinding ReadContract(string asset)
    {
        var contractId = Environment.GetEnvironmentVariable($"T4_{asset}_CONTRACT_ID") ?? string.Empty;
        var rawExpiry = Environment.GetEnvironmentVariable($"T4_{asset}_CONTRACT_EXPIRES_AT") ?? string.Empty;
        if (string.IsNullOrWhiteSpace(contractId) ||
            !DateTimeOffset.TryParse(rawExpiry, out var expiresAt) ||
            expiresAt.Offset != TimeSpan.Zero)
        {
            return ContractBinding.Unconfigured;
        }

        return new ContractBinding(contractId, expiresAt);
    }
}

public sealed record ContractBinding(string ContractId, DateTimeOffset ExpiresAt)
{
    public static ContractBinding Unconfigured { get; } = new(string.Empty, DateTimeOffset.MinValue);

    public bool IsConfigured => !string.IsNullOrWhiteSpace(ContractId) && ExpiresAt > DateTimeOffset.UtcNow;
}

namespace CryptoAgent.T4Bridge;

public static class RequestValidator
{
    private static readonly HashSet<int> LegacyIntervals = [240, 1440, 10080];

    public static MarketDataRequest Validate(
        BridgeOptions options,
        string symbol,
        int intervalMinutes,
        DateTimeOffset asOf,
        int limit)
    {
        var intervalAllowed = options.Api.IsConfigured
            ? intervalMinutes == 240
            : LegacyIntervals.Contains(intervalMinutes);
        if (!intervalAllowed || limit is < 60 or > 720 ||
            asOf.Offset != TimeSpan.Zero || asOf > DateTimeOffset.UtcNow.AddSeconds(30))
        {
            throw new ArgumentException("The market-data request is outside the approved policy.");
        }

        var selection = options.ContractCatalog.Resolve(symbol, asOf);
        var contract = selection.Contract;

        return new MarketDataRequest(
            symbol,
            contract.ExchangeId,
            contract.ContractId,
            contract.MarketId,
            contract.ExpiresAt,
            contract.RollAt,
            selection.RolledFromMarketId,
            contract.BasisExchangeId,
            contract.BasisContractId,
            contract.BasisMarketId,
            intervalMinutes,
            asOf,
            limit);
    }
}

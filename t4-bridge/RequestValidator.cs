namespace CryptoAgent.T4Bridge;

public static class RequestValidator
{
    private static readonly HashSet<int> AllowedIntervals = [240, 1440, 10080];

    public static MarketDataRequest Validate(
        BridgeOptions options,
        string symbol,
        int intervalMinutes,
        DateTimeOffset asOf,
        int limit)
    {
        if (!AllowedIntervals.Contains(intervalMinutes) || limit is < 60 or > 720 ||
            asOf.Offset != TimeSpan.Zero || asOf > DateTimeOffset.UtcNow.AddSeconds(30))
        {
            throw new ArgumentException("The market-data request is outside the approved policy.");
        }

        var selection = options.ContractCatalog.Resolve(symbol, asOf);
        var contract = selection.Contract;

        return new MarketDataRequest(
            symbol,
            contract.ContractId,
            contract.ExpiresAt,
            contract.RollAt,
            selection.RolledFromContractId,
            intervalMinutes,
            asOf,
            limit);
    }
}

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
        if (!options.Contracts.TryGetValue(symbol, out var contract) || !contract.IsConfigured)
        {
            throw new T4SessionUnavailableException("The requested T4 contract is not configured.");
        }

        if (!AllowedIntervals.Contains(intervalMinutes) || limit is < 60 or > 720 ||
            asOf.Offset != TimeSpan.Zero || asOf > DateTimeOffset.UtcNow.AddSeconds(30) ||
            contract.ExpiresAt <= asOf)
        {
            throw new ArgumentException("The market-data request is outside the approved policy.");
        }

        return new MarketDataRequest(
            symbol,
            contract.ContractId,
            contract.ExpiresAt,
            intervalMinutes,
            asOf,
            limit);
    }
}

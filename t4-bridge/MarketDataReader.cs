namespace CryptoAgent.T4Bridge;

public interface IT4MarketDataReader
{
    Task<MarketDataEnvelope> ReadAsync(MarketDataRequest request, CancellationToken cancellationToken);
}

public sealed class T4ApplicationRegistrationPendingReader : IT4MarketDataReader
{
    public Task<MarketDataEnvelope> ReadAsync(
        MarketDataRequest request,
        CancellationToken cancellationToken)
    {
        _ = request;
        cancellationToken.ThrowIfCancellationRequested();
        throw new T4SessionUnavailableException(
            "The T4 application and official market-data client are not configured.");
    }
}

public sealed class T4SessionUnavailableException : Exception
{
    public T4SessionUnavailableException(string message)
        : base(message)
    {
    }
}

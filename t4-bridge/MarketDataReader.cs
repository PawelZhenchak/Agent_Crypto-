namespace CryptoAgent.T4Bridge;

public interface IT4MarketDataReader
{
    T4ReaderHealth Health { get; }

    Task<MarketDataEnvelope> ReadAsync(
        MarketDataRequest request,
        CancellationToken cancellationToken);
}

public sealed class T4ApplicationRegistrationPendingReader : IT4MarketDataReader
{
    public T4ReaderHealth Health => new(
        false,
        "not_configured",
        "T4_APPLICATION_REGISTRATION_REQUIRED",
        null,
        0);

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

public sealed record T4ReaderHealth(
    bool Ready,
    string Environment,
    string ReasonCode,
    DateTimeOffset? LastMessageAt,
    int ReconnectCount);

public sealed class T4SessionUnavailableException : Exception
{
    public T4SessionUnavailableException(string message)
        : base(message)
    {
    }
}

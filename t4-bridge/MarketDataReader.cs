namespace CryptoAgent.T4Bridge;

public interface IT4MarketDataReader
{
    T4ReaderHealth Health { get; }

    Task<MarketDataEnvelope> ReadAsync(
        MarketDataRequest request,
        CancellationToken cancellationToken);
}

public interface IT4ObservationController
{
    SignedObservationEvent ApplyControl(
        ObservationControlAction action,
        Guid campaignId,
        Guid actionRequestId,
        string scopeKey);
}

public sealed class T4ApplicationRegistrationPendingReader : IT4MarketDataReader
{
    private readonly Guid _bootId;
    private readonly string? _evidenceKeyFingerprintSha256;

    public T4ApplicationRegistrationPendingReader()
    {
        _bootId = Guid.NewGuid();
    }

    public T4ApplicationRegistrationPendingReader(ObservationEventJournal journal)
    {
        _bootId = journal.BootId;
        _evidenceKeyFingerprintSha256 = journal.EvidenceKeyFingerprintSha256;
    }

    public T4ReaderHealth Health => new(
        false,
        "not_configured",
        "T4_APPLICATION_REGISTRATION_REQUIRED",
        null,
        0,
        _bootId,
        0,
        _evidenceKeyFingerprintSha256);

    public Task<MarketDataEnvelope> ReadAsync(
        MarketDataRequest request,
        CancellationToken cancellationToken)
    {
        _ = request;
        cancellationToken.ThrowIfCancellationRequested();
        throw new T4SessionUnavailableException(
            "The T4 application and official market-data client are not configured.",
            "T4_APPLICATION_REGISTRATION_REQUIRED");
    }
}

public sealed record T4ReaderHealth(
    bool Ready,
    string Environment,
    string ReasonCode,
    DateTimeOffset? LastMessageAt,
    int ReconnectCount,
    Guid BootId,
    long SessionGeneration,
    string? EvidenceKeyFingerprintSha256);

public sealed class T4SessionUnavailableException : Exception
{
    public T4SessionUnavailableException(
        string message,
        string reasonCode = "T4_SESSION_UNAVAILABLE")
        : base(message)
    {
        ReasonCode = T4ReasonCodes.Safe(reasonCode);
    }

    public string ReasonCode { get; }
}

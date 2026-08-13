using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace CryptoAgent.T4Bridge;

public static class ObservationEvidenceContract
{
    public const int SchemaVersion = 1;
    public const int BridgeSchemaVersion = 5;
    public const string SignatureAlgorithm = "ecdsa-p256-sha256-der";
    public const string ControlTokenHeaderName =
        "X-Crypto-Agent-Observation-Control-Token";
    public const string ReasonCodeHeaderName = "X-Crypto-Agent-T4-Reason-Code";

    public static bool IsScopeKey(string value) => value is
        "BTC/USD:240m" or "BTC/USD:1440m" or "BTC/USD:10080m" or
        "ETH/USD:240m" or "ETH/USD:1440m" or "ETH/USD:10080m";
}

public static class T4ReasonCodes
{
    private static readonly HashSet<string> Allowed = new(StringComparer.Ordinal)
    {
        "BRIDGE_STARTED",
        "BRIDGE_STOPPING",
        "OBSERVATION_CAMPAIGN_CHECKPOINT",
        "OBSERVATION_CONTROL_APPLIED",
        "OBSERVATION_CONTROL_REQUESTED",
        "READY",
        "T4_APPLICATION_REGISTRATION_REQUIRED",
        "T4_AUTHENTICATION_FAILED",
        "T4_CACHE_CLEARED",
        "T4_CONTRACT_ROLL_OBSERVED",
        "T4_CONTRACT_UNAVAILABLE",
        "T4_DELAYED_MARKET_DATA",
        "T4_DEPTH_SUBSCRIPTION_REJECTED",
        "T4_HISTORY_UNAVAILABLE",
        "T4_MISSING_DATA",
        "T4_OUTBOUND_REJECTED",
        "T4_PREWARM_IN_PROGRESS",
        "T4_RATE_LIMITED",
        "T4_SESSION_AUTHENTICATED",
        "T4_SESSION_CONNECTING",
        "T4_SESSION_DISCONNECTED",
        "T4_SESSION_STARTING",
        "T4_SESSION_UNAVAILABLE",
        "T4_STALE_DATA",
    };

    public static bool IsAllowed(string value) => Allowed.Contains(value);

    public static string Safe(string? value) =>
        value is not null && IsAllowed(value) ? value : "T4_SESSION_UNAVAILABLE";
}

public static class ObservationEventTypes
{
    private static readonly HashSet<string> Allowed = new(StringComparer.Ordinal)
    {
        "bridge_started",
        "bridge_stopping",
        "campaign_checkpoint",
        "cache_cleared",
        "contract_roll_observed",
        "control_applied",
        "control_requested",
        "missing_data_detected",
        "outbound_rejected",
        "rate_limited",
        "session_authenticated",
        "session_connecting",
        "session_disconnected",
        "session_ready",
        "stale_data_detected",
    };

    public static bool IsAllowed(string value) => Allowed.Contains(value);
}

public enum ObservationControlAction
{
    Reconnect,
    MissingData,
    StaleData,
    RateLimit,
    Restart,
}

public static class ObservationControlActions
{
    public static bool TryParse(string value, out ObservationControlAction action)
    {
        action = value switch
        {
            "reconnect" => ObservationControlAction.Reconnect,
            "missing_data" => ObservationControlAction.MissingData,
            "stale_data" => ObservationControlAction.StaleData,
            "rate_limit" => ObservationControlAction.RateLimit,
            "restart" => ObservationControlAction.Restart,
            _ => default,
        };
        return value is "reconnect" or "missing_data" or "stale_data" or
            "rate_limit" or "restart";
    }

    public static string Code(ObservationControlAction action) => action switch
    {
        ObservationControlAction.Reconnect => "reconnect",
        ObservationControlAction.MissingData => "missing_data",
        ObservationControlAction.StaleData => "stale_data",
        ObservationControlAction.RateLimit => "rate_limit",
        ObservationControlAction.Restart => "restart",
        _ => throw new ArgumentOutOfRangeException(nameof(action)),
    };
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record ObservationControlRequest(
    [property: JsonPropertyName("campaign_id")] string CampaignId,
    [property: JsonPropertyName("action_request_id")] string ActionRequestId,
    [property: JsonPropertyName("scope_key")] string ScopeKey)
{
    public (Guid CampaignId, Guid ActionRequestId, string ScopeKey) Validate(
        T4ObservationOptions options)
    {
        if (!Guid.TryParseExact(CampaignId, "D", out var campaignId) ||
            campaignId == Guid.Empty || CampaignId != campaignId.ToString("D") ||
            !Guid.TryParseExact(ActionRequestId, "D", out var actionRequestId) ||
            actionRequestId == Guid.Empty ||
            ActionRequestId != actionRequestId.ToString("D") ||
            options.CampaignId is null || options.CampaignId.Value != campaignId ||
            !ObservationEvidenceContract.IsScopeKey(ScopeKey))
        {
            throw new ArgumentException("The observation control request is invalid.");
        }
        return (campaignId, actionRequestId, ScopeKey);
    }
}

[JsonUnmappedMemberHandling(JsonUnmappedMemberHandling.Disallow)]
public sealed record ObservationCheckpointRequest(
    [property: JsonPropertyName("campaign_id")] string CampaignId,
    [property: JsonPropertyName("action_request_id")] string ActionRequestId)
{
    public (Guid CampaignId, Guid ActionRequestId) Validate(
        T4ObservationOptions options)
    {
        if (!Guid.TryParseExact(CampaignId, "D", out var campaignId) ||
            campaignId == Guid.Empty || CampaignId != campaignId.ToString("D") ||
            !Guid.TryParseExact(ActionRequestId, "D", out var actionRequestId) ||
            actionRequestId == Guid.Empty ||
            ActionRequestId != actionRequestId.ToString("D") ||
            options.CampaignId is null || options.CampaignId.Value != campaignId)
        {
            throw new ArgumentException("The observation checkpoint request is invalid.");
        }
        return (campaignId, actionRequestId);
    }
}

public sealed record ObservationEventPayload(
    [property: JsonPropertyName("action")]
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? Action = null,
    [property: JsonPropertyName("control_step")]
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? ControlStep = null,
    [property: JsonPropertyName("from_market_id")]
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? FromMarketId = null,
    [property: JsonPropertyName("to_market_id")]
    [property: JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)] string? ToMarketId = null);

public sealed record SignedObservationEvent(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("event_id")] string EventId,
    [property: JsonPropertyName("sequence_no")] long SequenceNo,
    [property: JsonPropertyName("event_at")] string EventAt,
    [property: JsonPropertyName("event_type")] string EventType,
    [property: JsonPropertyName("reason_code")] string ReasonCode,
    [property: JsonPropertyName("campaign_id")] string? CampaignId,
    [property: JsonPropertyName("boot_id")] string BootId,
    [property: JsonPropertyName("session_generation")] long SessionGeneration,
    [property: JsonPropertyName("reconnect_count")] int ReconnectCount,
    [property: JsonPropertyName("environment")] string Environment,
    [property: JsonPropertyName("bridge_schema_version")] int BridgeSchemaVersion,
    [property: JsonPropertyName("read_only")] bool ReadOnly,
    [property: JsonPropertyName("order_routes_exposed")] bool OrderRoutesExposed,
    [property: JsonPropertyName("scope_key")] string? ScopeKey,
    [property: JsonPropertyName("scenario_code")] string? ScenarioCode,
    [property: JsonPropertyName("action_request_id")] string? ActionRequestId,
    [property: JsonPropertyName("previous_event_hash")] string? PreviousEventHash,
    [property: JsonPropertyName("payload")] ObservationEventPayload Payload,
    [property: JsonPropertyName("payload_hash_sha256")] string PayloadHashSha256,
    [property: JsonPropertyName("event_hash_sha256")] string EventHashSha256,
    [property: JsonPropertyName("canonical_payload_base64")] string CanonicalPayloadBase64,
    [property: JsonPropertyName("signature_algorithm")] string SignatureAlgorithm,
    [property: JsonPropertyName("evidence_key_fingerprint_sha256")]
        string EvidenceKeyFingerprintSha256,
    [property: JsonPropertyName("signature_base64")] string SignatureBase64);

public sealed record ObservationEventPage(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("boot_id")] string BootId,
    [property: JsonPropertyName("environment")] string Environment,
    [property: JsonPropertyName("read_only")] bool ReadOnly,
    [property: JsonPropertyName("order_routes_exposed")] bool OrderRoutesExposed,
    [property: JsonPropertyName("signature_algorithm")] string SignatureAlgorithm,
    [property: JsonPropertyName("evidence_key_fingerprint_sha256")]
        string EvidenceKeyFingerprintSha256,
    [property: JsonPropertyName("public_key_spki_base64")] string PublicKeySpkiBase64,
    [property: JsonPropertyName("events")] IReadOnlyList<SignedObservationEvent> Events);

internal sealed record ObservationEventClaims(
    [property: JsonPropertyName("schema_version")] int SchemaVersion,
    [property: JsonPropertyName("event_id")] string EventId,
    [property: JsonPropertyName("sequence_no")] long SequenceNo,
    [property: JsonPropertyName("event_at")] string EventAt,
    [property: JsonPropertyName("event_type")] string EventType,
    [property: JsonPropertyName("reason_code")] string ReasonCode,
    [property: JsonPropertyName("campaign_id")] string? CampaignId,
    [property: JsonPropertyName("boot_id")] string BootId,
    [property: JsonPropertyName("session_generation")] long SessionGeneration,
    [property: JsonPropertyName("reconnect_count")] int ReconnectCount,
    [property: JsonPropertyName("environment")] string Environment,
    [property: JsonPropertyName("bridge_schema_version")] int BridgeSchemaVersion,
    [property: JsonPropertyName("read_only")] bool ReadOnly,
    [property: JsonPropertyName("order_routes_exposed")] bool OrderRoutesExposed,
    [property: JsonPropertyName("scope_key")] string? ScopeKey,
    [property: JsonPropertyName("scenario_code")] string? ScenarioCode,
    [property: JsonPropertyName("action_request_id")] string? ActionRequestId,
    [property: JsonPropertyName("previous_event_hash")] string? PreviousEventHash,
    [property: JsonPropertyName("payload")] ObservationEventPayload Payload,
    [property: JsonPropertyName("payload_hash_sha256")] string PayloadHashSha256);

public sealed class ObservationEventJournal : IDisposable
{
    private const long MaximumJournalBytes = 64L * 1024 * 1024;
    private const int MaximumEventBytes = 64 * 1024;
    private const int MaximumEvents = 100_000;
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
        UnmappedMemberHandling = JsonUnmappedMemberHandling.Disallow,
    };

    private readonly object _gate = new();
    private readonly List<SignedObservationEvent> _events = [];
    private readonly T4ObservationOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly string _environment;
    private readonly ECDsa? _signingKey;
    private readonly string? _journalPath;
    private readonly FileStream? _journalProcessLock;
    private string? _previousEventHash;
    private long _sequenceNo;

    public ObservationEventJournal(BridgeOptions options, TimeProvider timeProvider)
    {
        _options = options.Observation;
        _timeProvider = timeProvider;
        _environment = options.Api.AttestedEnvironment;
        BootId = Guid.NewGuid();
        if (!_options.EvidenceEnabled)
        {
            return;
        }

        _journalPath = _options.JournalPath!;
        _signingKey = LoadSigningKey(_options.SigningKeyPath!);
        try
        {
            _journalProcessLock = AcquireJournalProcessLock(_journalPath);
            var publicKey = _signingKey.ExportSubjectPublicKeyInfo();
            PublicKeySpkiBase64 = Convert.ToBase64String(publicKey);
            EvidenceKeyFingerprintSha256 = Convert.ToHexString(
                SHA256.HashData(publicKey)).ToLowerInvariant();
            LoadAndVerifyExistingEvents();
            Append(
                "bridge_started",
                "BRIDGE_STARTED",
                sessionGeneration: 0,
                reconnectCount: 0);
        }
        catch
        {
            _journalProcessLock?.Dispose();
            _signingKey.Dispose();
            throw;
        }
    }

    public Guid BootId { get; }

    public bool IsConfigured => _signingKey is not null;

    public string? EvidenceKeyFingerprintSha256 { get; }

    public string? PublicKeySpkiBase64 { get; }

    public SignedObservationEvent Append(
        string eventType,
        string reasonCode,
        long sessionGeneration,
        int reconnectCount,
        string? scopeKey = null,
        string? scenarioCode = null,
        Guid? actionRequestId = null,
        ObservationEventPayload? payload = null,
        Guid? campaignId = null)
    {
        if (_signingKey is null || _journalPath is null)
        {
            throw new InvalidOperationException("Signed T4 observation evidence is not configured.");
        }
        if (!ObservationEventTypes.IsAllowed(eventType) ||
            !T4ReasonCodes.IsAllowed(reasonCode) || sessionGeneration < 0 ||
            reconnectCount < 0 || !SafeOptionalText(scopeKey, 256) ||
            !SafeOptionalCode(scenarioCode))
        {
            throw new ArgumentException("The T4 observation event is invalid.");
        }
        var effectiveCampaignId = campaignId ?? _options.CampaignId;
        if (_options.CampaignId is not null && effectiveCampaignId != _options.CampaignId)
        {
            throw new ArgumentException("The T4 observation campaign does not match.");
        }

        lock (_gate)
        {
            if (_events.Count >= MaximumEvents)
            {
                throw new InvalidOperationException("The T4 observation journal is full.");
            }
            if (_events.Count > 0 &&
                _events[^1].BootId == BootId.ToString("D") &&
                (sessionGeneration < _events[^1].SessionGeneration ||
                 reconnectCount < _events[^1].ReconnectCount))
            {
                throw new InvalidOperationException(
                    "The T4 observation event counters cannot move backwards.");
            }
            var eventPayload = payload ?? new ObservationEventPayload();
            ValidatePayload(eventPayload);
            var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(eventPayload, JsonOptions);
            var payloadHash = HexHash(payloadBytes);
            var sequenceNo = checked(_sequenceNo + 1);
            var claims = new ObservationEventClaims(
                ObservationEvidenceContract.SchemaVersion,
                Guid.NewGuid().ToString("D"),
                sequenceNo,
                // PostgreSQL and Python both persist microseconds.  Never sign
                // DateTimeOffset's seven-digit (100 ns) round-trip form because
                // its final digit can be rounded differently across runtimes.
                _timeProvider.GetUtcNow().ToUniversalTime().ToString(
                    "yyyy-MM-dd'T'HH:mm:ss.ffffff'Z'", CultureInfo.InvariantCulture),
                eventType,
                reasonCode,
                effectiveCampaignId?.ToString("D"),
                BootId.ToString("D"),
                sessionGeneration,
                reconnectCount,
                _environment,
                ObservationEvidenceContract.BridgeSchemaVersion,
                true,
                false,
                scopeKey,
                scenarioCode,
                actionRequestId?.ToString("D"),
                _previousEventHash,
                eventPayload,
                payloadHash);
            var canonicalBytes = JsonSerializer.SerializeToUtf8Bytes(claims, JsonOptions);
            var eventHashBytes = SHA256.HashData(canonicalBytes);
            var eventHash = Convert.ToHexString(eventHashBytes).ToLowerInvariant();
            var signature = _signingKey.SignHash(
                eventHashBytes,
                DSASignatureFormat.Rfc3279DerSequence);
            var signed = new SignedObservationEvent(
                claims.SchemaVersion,
                claims.EventId,
                claims.SequenceNo,
                claims.EventAt,
                claims.EventType,
                claims.ReasonCode,
                claims.CampaignId,
                claims.BootId,
                claims.SessionGeneration,
                claims.ReconnectCount,
                claims.Environment,
                claims.BridgeSchemaVersion,
                claims.ReadOnly,
                claims.OrderRoutesExposed,
                claims.ScopeKey,
                claims.ScenarioCode,
                claims.ActionRequestId,
                claims.PreviousEventHash,
                claims.Payload,
                claims.PayloadHashSha256,
                eventHash,
                Convert.ToBase64String(canonicalBytes),
                ObservationEvidenceContract.SignatureAlgorithm,
                EvidenceKeyFingerprintSha256!,
                Convert.ToBase64String(signature));
            Persist(signed);
            _events.Add(signed);
            _sequenceNo = sequenceNo;
            _previousEventHash = eventHash;
            return signed;
        }
    }

    public ObservationEventPage Read(long afterSequence, int limit)
    {
        if (!IsConfigured || EvidenceKeyFingerprintSha256 is null ||
            PublicKeySpkiBase64 is null)
        {
            throw new InvalidOperationException("Signed T4 observation evidence is not configured.");
        }
        if (afterSequence < 0 || limit is < 1 or > 1000)
        {
            throw new ArgumentException("The T4 observation event query is invalid.");
        }
        lock (_gate)
        {
            return new ObservationEventPage(
                ObservationEvidenceContract.SchemaVersion,
                BootId.ToString("D"),
                _environment,
                true,
                false,
                ObservationEvidenceContract.SignatureAlgorithm,
                EvidenceKeyFingerprintSha256,
                PublicKeySpkiBase64,
                _events.Where(item => item.SequenceNo > afterSequence)
                    .Take(limit)
                    .ToArray());
        }
    }

    public SignedObservationEvent RecordCampaignCheckpoint(
        Guid campaignId,
        Guid actionRequestId,
        long sessionGeneration,
        int reconnectCount)
    {
        if (!IsConfigured || campaignId == Guid.Empty || actionRequestId == Guid.Empty ||
            sessionGeneration < 0 || reconnectCount < 0 ||
            _options.CampaignId is null || _options.CampaignId.Value != campaignId)
        {
            throw new InvalidOperationException(
                "The T4 observation checkpoint is outside the configured campaign.");
        }
        lock (_gate)
        {
            // Health is sampled before this method is entered.  A concurrent
            // session transition may already have appended a newer counter.
            // Inherit the journal head under the same lock so the signed
            // checkpoint can never regress and be rejected by PostgreSQL.
            if (_events.Count > 0 && _events[^1].BootId == BootId.ToString("D"))
            {
                sessionGeneration = Math.Max(
                    sessionGeneration, _events[^1].SessionGeneration);
                reconnectCount = Math.Max(
                    reconnectCount, _events[^1].ReconnectCount);
            }
            return Append(
                "campaign_checkpoint",
                "OBSERVATION_CAMPAIGN_CHECKPOINT",
                sessionGeneration: sessionGeneration,
                reconnectCount: reconnectCount,
                campaignId: campaignId,
                actionRequestId: actionRequestId,
                payload: new ObservationEventPayload("checkpoint", "consumed"));
        }
    }

    public void Dispose()
    {
        _signingKey?.Dispose();
        _journalProcessLock?.Dispose();
    }

    private static FileStream AcquireJournalProcessLock(string journalPath)
    {
        var journal = new FileInfo(journalPath);
        if (journal.Directory is null)
        {
            throw new InvalidOperationException("The T4 observation journal path is invalid.");
        }
        Directory.CreateDirectory(journal.Directory.FullName);
        try
        {
            return new FileStream(
                journalPath + ".lock",
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None,
                1,
                FileOptions.WriteThrough);
        }
        catch (Exception exception) when (
            exception is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException(
                "The T4 observation journal is already in use or unavailable.");
        }
    }

    private static ECDsa LoadSigningKey(string path)
    {
        var file = new FileInfo(path);
        if (!file.Exists || file.Length is <= 0 or > 32_768)
        {
            throw new InvalidOperationException("The T4 evidence signing key is unavailable.");
        }
        if (!OperatingSystem.IsWindows())
        {
            UnixFileMode unsafeBits =
                UnixFileMode.GroupRead | UnixFileMode.GroupWrite |
                UnixFileMode.GroupExecute | UnixFileMode.OtherRead |
                UnixFileMode.OtherWrite | UnixFileMode.OtherExecute;
            if ((File.GetUnixFileMode(path) & unsafeBits) != 0)
            {
                throw new InvalidOperationException(
                    "The T4 evidence signing key permissions are unsafe.");
            }
        }
        string pem;
        try
        {
            pem = File.ReadAllText(path, Encoding.ASCII);
        }
        catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
        {
            throw new InvalidOperationException(
                "The T4 evidence signing key is unavailable.");
        }
        var normalizedPem = pem.Trim();
        if (!normalizedPem.StartsWith(
                "-----BEGIN PRIVATE KEY-----", StringComparison.Ordinal) ||
            !normalizedPem.EndsWith(
                "-----END PRIVATE KEY-----", StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                "The T4 evidence signing key must be a private PKCS#8 P-256 PEM key.");
        }
        var key = ECDsa.Create();
        try
        {
            key.ImportFromPem(pem);
            var parameters = key.ExportParameters(includePrivateParameters: true);
            if (key.KeySize != 256 ||
                parameters.Curve.Oid.Value != "1.2.840.10045.3.1.7" ||
                parameters.D is null || parameters.D.Length != 32)
            {
                throw new CryptographicException();
            }
            return key;
        }
        catch (Exception exception) when (exception is ArgumentException or CryptographicException)
        {
            key.Dispose();
            throw new InvalidOperationException(
                "The T4 evidence signing key must be a private PKCS#8 P-256 PEM key.");
        }
    }

    private void LoadAndVerifyExistingEvents()
    {
        var path = _journalPath!;
        var file = new FileInfo(path);
        if (!file.Exists)
        {
            var directory = file.Directory;
            if (directory is null)
            {
                throw new InvalidOperationException("The T4 observation journal path is invalid.");
            }
            Directory.CreateDirectory(directory.FullName);
            return;
        }
        if (file.Length > MaximumJournalBytes)
        {
            throw new InvalidOperationException("The T4 observation journal is too large.");
        }

        // A record is committed only once its trailing newline has reached the
        // journal.  A crash or disk-full error may leave one unterminated tail;
        // discard only that tail while retaining every signed, newline-framed
        // record before it.
        if (file.Length > 0)
        {
            var existing = File.ReadAllBytes(path);
            var lastNewline = Array.LastIndexOf(existing, (byte)'\n');
            var committedLength = lastNewline + 1;
            if (existing.Length - committedLength > MaximumEventBytes)
            {
                throw new InvalidOperationException(
                    "The T4 observation journal tail is invalid.");
            }
            if (committedLength != existing.Length)
            {
                using var recovery = new FileStream(
                    path,
                    FileMode.Open,
                    FileAccess.Write,
                    FileShare.Read,
                    4096,
                    FileOptions.WriteThrough);
                recovery.SetLength(committedLength);
                recovery.Flush(flushToDisk: true);
            }
        }

        string? expectedPrevious = null;
        long expectedSequence = 1;
        SignedObservationEvent? previous = null;
        foreach (var line in File.ReadLines(path, Encoding.UTF8))
        {
            if (Encoding.UTF8.GetByteCount(line) > MaximumEventBytes ||
                _events.Count >= MaximumEvents)
            {
                throw new InvalidOperationException("The T4 observation journal is invalid.");
            }
            SignedObservationEvent? item;
            try
            {
                item = JsonSerializer.Deserialize<SignedObservationEvent>(line, JsonOptions);
            }
            catch (JsonException)
            {
                throw new InvalidOperationException("The T4 observation journal is invalid.");
            }
            if (item is null || item.SequenceNo != expectedSequence ||
                item.PreviousEventHash != expectedPrevious || !Verify(item) ||
                (previous is not null && previous.BootId == item.BootId &&
                 (item.SessionGeneration < previous.SessionGeneration ||
                  item.ReconnectCount < previous.ReconnectCount)))
            {
                throw new InvalidOperationException("The T4 observation journal is invalid.");
            }
            _events.Add(item);
            previous = item;
            expectedPrevious = item.EventHashSha256;
            expectedSequence++;
        }
        _sequenceNo = expectedSequence - 1;
        _previousEventHash = expectedPrevious;
    }

    private bool Verify(SignedObservationEvent item)
    {
        if (item.SchemaVersion != ObservationEvidenceContract.SchemaVersion ||
            item.BridgeSchemaVersion != ObservationEvidenceContract.BridgeSchemaVersion ||
            item.SignatureAlgorithm != ObservationEvidenceContract.SignatureAlgorithm ||
            item.EvidenceKeyFingerprintSha256 != EvidenceKeyFingerprintSha256 ||
            item.Environment != _environment || !item.ReadOnly || item.OrderRoutesExposed ||
            item.Payload is null || item.CanonicalPayloadBase64 is null ||
            item.SignatureBase64 is null ||
            item.CampaignId != _options.CampaignId?.ToString("D") ||
            item.SequenceNo < 1 || item.SessionGeneration < 0 || item.ReconnectCount < 0 ||
            !CanonicalGuid(item.EventId) || !CanonicalGuid(item.BootId) ||
            !CanonicalOptionalGuid(item.ActionRequestId) ||
            !LowerSha256(item.PayloadHashSha256) || !LowerSha256(item.EventHashSha256) ||
            (item.PreviousEventHash is not null && !LowerSha256(item.PreviousEventHash)) ||
            !SafeOptionalText(item.ScopeKey, 256) || !SafeOptionalCode(item.ScenarioCode) ||
            !ObservationEventTypes.IsAllowed(item.EventType) ||
            !T4ReasonCodes.IsAllowed(item.ReasonCode))
        {
            return false;
        }
        try
        {
            ValidatePayload(item.Payload);
        }
        catch (ArgumentException)
        {
            return false;
        }
        byte[] canonicalBytes;
        byte[] signature;
        try
        {
            canonicalBytes = Convert.FromBase64String(item.CanonicalPayloadBase64);
            signature = Convert.FromBase64String(item.SignatureBase64);
        }
        catch (FormatException)
        {
            return false;
        }
        if (canonicalBytes.Length > MaximumEventBytes || signature.Length is < 64 or > 80)
        {
            return false;
        }
        var hash = SHA256.HashData(canonicalBytes);
        if (item.EventHashSha256 != Convert.ToHexString(hash).ToLowerInvariant() ||
            !_signingKey!.VerifyHash(
                hash,
                signature,
                DSASignatureFormat.Rfc3279DerSequence))
        {
            return false;
        }
        ObservationEventClaims? claims;
        try
        {
            claims = JsonSerializer.Deserialize<ObservationEventClaims>(
                canonicalBytes, JsonOptions);
        }
        catch (JsonException)
        {
            return false;
        }
        if (claims is null ||
            JsonSerializer.SerializeToUtf8Bytes(claims, JsonOptions)
                .AsSpan().SequenceEqual(canonicalBytes) is not true ||
            item.EventId != claims.EventId || item.SequenceNo != claims.SequenceNo ||
            item.EventAt != claims.EventAt || item.EventType != claims.EventType ||
            item.ReasonCode != claims.ReasonCode || item.CampaignId != claims.CampaignId ||
            item.BootId != claims.BootId ||
            item.SessionGeneration != claims.SessionGeneration ||
            item.ReconnectCount != claims.ReconnectCount ||
            item.Environment != claims.Environment ||
            item.BridgeSchemaVersion != claims.BridgeSchemaVersion ||
            item.ReadOnly != claims.ReadOnly ||
            item.OrderRoutesExposed != claims.OrderRoutesExposed ||
            item.ScopeKey != claims.ScopeKey || item.ScenarioCode != claims.ScenarioCode ||
            item.ActionRequestId != claims.ActionRequestId ||
            item.PreviousEventHash != claims.PreviousEventHash ||
            item.Payload != claims.Payload ||
            item.PayloadHashSha256 != claims.PayloadHashSha256)
        {
            return false;
        }
        var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(claims.Payload, JsonOptions);
        return claims.PayloadHashSha256 == HexHash(payloadBytes);
    }

    private void Persist(SignedObservationEvent item)
    {
        var bytes = JsonSerializer.SerializeToUtf8Bytes(item, JsonOptions);
        if (bytes.Length > MaximumEventBytes)
        {
            throw new InvalidOperationException("The T4 observation event is too large.");
        }
        var file = new FileInfo(_journalPath!);
        if (file.Exists && file.Length + bytes.Length + 1 > MaximumJournalBytes)
        {
            throw new InvalidOperationException("The T4 observation journal is full.");
        }
        using var stream = new FileStream(
            _journalPath!,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read,
            4096,
            FileOptions.WriteThrough);
        stream.Write(bytes);
        stream.WriteByte((byte)'\n');
        stream.Flush(flushToDisk: true);
    }

    private static void ValidatePayload(ObservationEventPayload payload)
    {
        if (!SafeOptionalCode(payload.Action) || !SafeOptionalCode(payload.ControlStep) ||
            !SafeOptionalText(payload.FromMarketId, 256) ||
            !SafeOptionalText(payload.ToMarketId, 256))
        {
            throw new ArgumentException("The T4 observation event payload is invalid.");
        }
    }

    private static bool SafeOptionalCode(string? value) => value is null ||
        (value.Length is > 0 and <= 64 &&
         value.All(character => char.IsAsciiLetterOrDigit(character) || character == '_'));

    private static bool SafeOptionalText(string? value, int maximumLength) => value is null ||
        (value.Length is > 0 && value.Length <= maximumLength && value == value.Trim() &&
         value.All(character => character is >= ' ' and <= '~'));

    private static string HexHash(ReadOnlySpan<byte> value) =>
        Convert.ToHexString(SHA256.HashData(value)).ToLowerInvariant();

    private static bool CanonicalGuid(string? value) =>
        Guid.TryParseExact(value, "D", out var parsed) && parsed != Guid.Empty &&
        value == parsed.ToString("D");

    private static bool CanonicalOptionalGuid(string? value) =>
        value is null || CanonicalGuid(value);

    private static bool LowerSha256(string? value) => value is not null && value.Length == 64 &&
        value.All(character => character is >= '0' and <= '9' or >= 'a' and <= 'f');
}

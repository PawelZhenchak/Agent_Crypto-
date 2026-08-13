using CryptoAgent.T4Bridge;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using System.Security.Cryptography;
using System.Text.Json;
using System.Text.RegularExpressions;
using T4Proto.V1.Auth;
using T4Proto.V1.Common;
using T4Proto.V1.Service;

var token = new string('t', 32);
Environment.SetEnvironmentVariable("T4_BRIDGE_TOKEN", token);
Environment.SetEnvironmentVariable("T4_BRIDGE_PORT", "8784");
Environment.SetEnvironmentVariable("T4_API_ENVIRONMENT", null);
Environment.SetEnvironmentVariable("T4_API_KEY", null);
Environment.SetEnvironmentVariable("T4_CONTRACT_CATALOG_PATH", null);
Environment.SetEnvironmentVariable("T4_BTC_CONTRACT_ID", "SIM:MBT:TEST");
Environment.SetEnvironmentVariable(
    "T4_BTC_CONTRACT_EXPIRES_AT",
    DateTimeOffset.UtcNow.AddDays(30).ToString("O"));
Environment.SetEnvironmentVariable("T4_ETH_CONTRACT_ID", null);
Environment.SetEnvironmentVariable("T4_ETH_CONTRACT_EXPIRES_AT", null);
Environment.SetEnvironmentVariable("T4_EVIDENCE_SIGNING_KEY_PATH", null);
Environment.SetEnvironmentVariable("T4_OBSERVATION_EVENT_JOURNAL_PATH", null);
Environment.SetEnvironmentVariable("T4_OBSERVATION_CAMPAIGN_ID", null);
Environment.SetEnvironmentVariable("T4_OBSERVATION_CONTROL_ENABLED", null);
Environment.SetEnvironmentVariable("T4_OBSERVATION_CONTROL_TOKEN", null);

var options = BridgeOptions.FromEnvironment();
Assert(options.Port == 8784, "port was not parsed");
Assert(!options.Observation.ControlEnabled && !options.Observation.EvidenceEnabled,
    "observation control was not default-disabled");
Assert(BridgeSecurity.TokenMatches(token, options.Token), "valid token was rejected");
Assert(!BridgeSecurity.TokenMatches("wrong-token", options.Token), "wrong token was accepted");

var request = RequestValidator.Validate(
    options,
    "BTC/USD",
    1440,
    DateTimeOffset.UtcNow.AddSeconds(-1),
    120);
Assert(request.ContractId == "SIM:MBT:TEST", "actual contract ID was not retained");
Assert(!(request.RolledFromMarketId?.Any() ?? false), "legacy contract unexpectedly rolled");

var march = new FuturesContract(
    "BTC/USD",
    "SIM:MBT:202703",
    new DateTimeOffset(2027, 3, 26, 0, 0, 0, TimeSpan.Zero),
    new DateTimeOffset(2027, 3, 19, 0, 0, 0, TimeSpan.Zero));
var june = new FuturesContract(
    "BTC/USD",
    "SIM:MBT:202706",
    new DateTimeOffset(2027, 6, 25, 0, 0, 0, TimeSpan.Zero),
    new DateTimeOffset(2027, 6, 18, 0, 0, 0, TimeSpan.Zero));
var catalog = new FuturesContractCatalog([march, june]);

var beforeRoll = catalog.Resolve(
    "BTC/USD",
    new DateTimeOffset(2027, 3, 18, 23, 59, 59, TimeSpan.Zero));
Assert(beforeRoll.Contract.ContractId == march.ContractId, "front month was not selected");
Assert(!beforeRoll.IsRolled, "front month was incorrectly marked as rolled");

var atRoll = catalog.Resolve(
    "BTC/USD",
    new DateTimeOffset(2027, 3, 19, 0, 0, 0, TimeSpan.Zero));
Assert(atRoll.Contract.ContractId == june.ContractId, "next contract was not selected at roll");
Assert(atRoll.RolledFromMarketId == march.ContractId, "roll provenance was not retained");
var afterTransitionWindow = catalog.Resolve(
    "BTC/USD",
    new DateTimeOffset(2027, 3, 20, 0, 0, 1, TimeSpan.Zero));
Assert(!afterTransitionWindow.IsRolled, "roll evidence outlived its 24-hour window");

Expect<T4SessionUnavailableException>(() => catalog.Resolve(
    "BTC/USD",
    new DateTimeOffset(2027, 6, 18, 0, 0, 0, TimeSpan.Zero)));
Expect<InvalidOperationException>(() => new FuturesContractCatalog([march, march]));
Expect<InvalidOperationException>(() => new FuturesContractCatalog([
    march with { ContractId = " invalid contract id" },
]));
Expect<InvalidOperationException>(() => new FuturesContractCatalog([
    march with { RollAt = march.ExpiresAt.AddMinutes(-30) },
]));
Expect<InvalidOperationException>(() => new FuturesContractCatalog([
    march,
    june with { RollAt = march.RollAt },
]));

var catalogPath = Path.GetTempFileName();
try
{
    File.WriteAllText(catalogPath, """
        {
          "schema_version": 1,
          "default_roll_days": 7,
          "contracts": [
            {
              "logical_symbol": "BTC/USD",
              "contract_id": "SIM:MBT:202703",
              "expires_at": "2027-03-26T00:00:00Z"
            },
            {
              "logical_symbol": "BTC/USD",
              "contract_id": "SIM:MBT:202706",
              "expires_at": "2027-06-25T00:00:00Z"
            }
          ]
        }
        """);
    Environment.SetEnvironmentVariable("T4_CONTRACT_CATALOG_PATH", catalogPath);
    var catalogOptions = BridgeOptions.FromEnvironment();
    var fromFile = catalogOptions.ContractCatalog.Resolve(
        "BTC/USD",
        new DateTimeOffset(2027, 3, 19, 0, 0, 0, TimeSpan.Zero));
    Assert(fromFile.Contract.ContractId == "SIM:MBT:202706", "catalog roll failed");
    Assert(fromFile.RolledFromMarketId == "SIM:MBT:202703", "catalog roll lost provenance");
}
finally
{
    Environment.SetEnvironmentVariable("T4_CONTRACT_CATALOG_PATH", null);
    File.Delete(catalogPath);
}

Expect<InvalidOperationException>(() => new FuturesContractCatalog(
    [
        new FuturesContract(
            "BTC/USD",
            "CME_E",
            "BTC",
            "XCME_E BTC \u2028 (Z99)",
            DateTimeOffset.UtcNow.AddDays(30),
            DateTimeOffset.UtcNow.AddDays(25),
            "CME_E",
            "BTCIDX",
            "XCME_E BTC Index (Z99)"),
    ]));

Expect<ArgumentException>(() => RequestValidator.Validate(
    options,
    "BTC/USD",
    1,
    DateTimeOffset.UtcNow,
    120));
Expect<T4SessionUnavailableException>(() => RequestValidator.Validate(
    options,
    "ETH/USD",
    1440,
    DateTimeOffset.UtcNow,
    120));

var reader = new T4ApplicationRegistrationPendingReader();
Assert(!reader.Health.Ready, "pending reader unexpectedly reported ready");
Assert(T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.LoginRequest),
    "login was not allowed");
Assert(T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.AuthenticationTokenRequest),
    "read-only bearer token refresh was not allowed");
Assert(T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.Heartbeat),
    "heartbeat was not allowed");
Assert(T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.MarketDepthSubscribe),
    "market depth subscription was not allowed");
Assert(!T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.OrderSubmit),
    "order submission was not blocked");
Assert(!T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.OrderRevise),
    "order revision was not blocked");
Assert(!T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.OrderPull),
    "order pull was not blocked");
Assert(!T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.OrderBatch),
    "order batch was not blocked");
Assert(!T4OutboundPolicy.IsAllowed(ClientMessage.PayloadOneofCase.CreateUds),
    "UDS creation was not blocked");
var allowedOutboundPayloads = System.Enum.GetValues<ClientMessage.PayloadOneofCase>()
    .Where(T4OutboundPolicy.IsAllowed)
    .ToHashSet();
Assert(allowedOutboundPayloads.SetEquals(new[]
    {
        ClientMessage.PayloadOneofCase.LoginRequest,
        ClientMessage.PayloadOneofCase.AuthenticationTokenRequest,
        ClientMessage.PayloadOneofCase.Heartbeat,
        ClientMessage.PayloadOneofCase.MarketDepthSubscribe,
    }),
    "the exact read-only outbound allowlist changed");
var safetyNow = new DateTimeOffset(2027, 1, 2, 3, 4, 5, TimeSpan.Zero);
Assert(T4ChartSafetyPolicy.IsFresh(
        safetyNow.AddSeconds(-60), safetyNow, TimeSpan.FromSeconds(60)),
    "a boundary-fresh snapshot was rejected");
Assert(!T4ChartSafetyPolicy.IsFresh(
        safetyNow.AddSeconds(-61), safetyNow, TimeSpan.FromSeconds(60)),
    "a stale snapshot was accepted");
Assert(!T4ChartSafetyPolicy.IsFresh(
        safetyNow.AddSeconds(1), safetyNow, TimeSpan.FromSeconds(60)),
    "a future snapshot was accepted");
Assert(T4ChartSafetyPolicy.IsFresh(
        safetyNow - T4ChartSafetyPolicy.MaximumHistoryCacheAge,
        safetyNow,
        T4ChartSafetyPolicy.MaximumHistoryCacheAge),
    "a boundary-fresh Chart cache was rejected");
Assert(!T4ChartSafetyPolicy.IsFresh(
        safetyNow - T4ChartSafetyPolicy.MaximumHistoryCacheAge - TimeSpan.FromTicks(1),
        safetyNow,
        T4ChartSafetyPolicy.MaximumHistoryCacheAge),
    "a stale Chart cache was accepted");
Assert(T4ChartSafetyPolicy.HasRealDepthPermission(MarketDataType.Depth),
    "real T4 depth permission was rejected");
Assert(!T4ChartSafetyPolicy.HasRealDepthPermission(
        MarketDataType.Depth | MarketDataType.Delayed),
    "delayed T4 depth permission was accepted");
Assert(!T4ChartSafetyPolicy.HasRealDepthPermission(MarketDataType.NotSet),
    "unknown T4 market-data permission was accepted");
var chartToken = new AuthenticationToken
{
    Token = new string('b', 32),
    ExpireTime = Timestamp.FromDateTime(safetyNow.AddMinutes(5).UtcDateTime),
};
Assert(T4ChartSafetyPolicy.TryReadBearerToken(
        chartToken, safetyNow, out var bearerToken, out var tokenExpiresAt) &&
    bearerToken == chartToken.Token && tokenExpiresAt == safetyNow.AddMinutes(5),
    "a valid T4 Chart bearer token was rejected");
var expiringChartToken = chartToken.Clone();
expiringChartToken.ExpireTime = Timestamp.FromDateTime(
    safetyNow.AddSeconds(30).UtcDateTime);
Assert(!T4ChartSafetyPolicy.TryReadBearerToken(
        expiringChartToken,
        safetyNow,
        out _,
        out _),
    "a bearer token at the refresh boundary was accepted");
Assert(Math.Abs(T4ChartSafetyPolicy.PriceToDouble(new T4.Price(4735.75m)) - 4735.75) < 1e-9,
    "the official Chart decoder price was not preserved");
// Raw T4BinAggr payload from the official t4-api-tools decoder fixture.
var officialChartPayload = Convert.FromBase64String(
    "BQEBAAAAHgIFRVNNMjUBBAQwLjI1oYCA+JwFu6iclAWAgEgAAAoEgIDMteqzqO0IBwMFRVNNMjVSC4DQp/netqjtCICMjZ4CVYCA5JcEiqWigwGPAoCASFWAgJiOB7mjkcoFjwKAgEhlgIDgUoqRxHaOAoCASFWAgNxm/6PshwSPAoCASGQyMgoFBQsUgKCDvdO5qO0IAhsWgPDegMi8qO0IpYCA4OcBn/ChwgaPAoCASAENFYDAusS8v6jtCJDIAg==");
var officialChartMessage = new T4.Messages.MsgChartAggregatedData
{
    Data = officialChartPayload,
};
await using var chartFixtureStream = new MemoryStream();
using (var chartFixtureWriter = new BinaryWriter(
           chartFixtureStream, System.Text.Encoding.UTF8, leaveOpen: true))
{
    chartFixtureWriter.Write((short)officialChartMessage.MessageType);
    chartFixtureWriter.Write(officialChartMessage.MessageVersion);
    chartFixtureWriter.Write(officialChartMessage.Bytes.Length);
    chartFixtureWriter.Write(officialChartMessage.Bytes);
}
chartFixtureStream.Position = 0;
Assert(T4ChartSafetyPolicy.HasValidBinaryEnvelope(chartFixtureStream.ToArray()),
    "the official Chart message envelope was rejected");
var decoded = await new T4ChartDecoder.T4BinaryDecoder().DecodeAll(
    chartFixtureStream, CancellationToken.None);
Assert(decoded.Bars.Count == 1, "the official binary Chart fixture did not decode");
var decodedBar = decoded.Bars[0];
Assert(decodedBar.MarketId == "ESM25", "the decoded Chart market ID was lost");
Assert(decodedBar.Open.DecimalValue == 5000.25m &&
    decodedBar.High.DecimalValue == 5005.50m &&
    decodedBar.Low.DecimalValue == 4998.00m &&
    decodedBar.Close.DecimalValue == 5003.75m,
    "the official binary Chart price scaling was not preserved");
Assert(decodedBar.Volume == 100, "the official binary Chart volume was not preserved");
await using var truncatedChartStream = new MemoryStream(
    chartFixtureStream.ToArray()[..^1], writable: false);
Assert(!T4ChartSafetyPolicy.HasValidBinaryEnvelope(truncatedChartStream.ToArray()),
    "a truncated Chart message envelope was accepted");
await ExpectAsync<T4ChartDecoder.DecodingException>(() =>
    new T4ChartDecoder.T4BinaryDecoder().DecodeAll(
        truncatedChartStream, CancellationToken.None));
await ExpectAsync<T4SessionUnavailableException>(() => reader.ReadAsync(
    request,
    CancellationToken.None));

var evidenceObservedAt = request.AsOf.AddSeconds(-2);
var evidenceAvailableAt = request.AsOf.AddSeconds(-1);
var validEvidence = new FuturesEvidenceEnvelope(
    "CME_E",
    request.ContractId,
    "XCME_E BTC (U26)",
    "plus500_t4_futures_v1",
    "OPEN",
    true,
    evidenceObservedAt,
    evidenceAvailableAt,
    request.AsOf,
    Enumerable.Range(1, 5)
        .Select(level => new OrderBookLevelEnvelope(level, 100.0 - level / 10.0, level))
        .ToArray(),
    Enumerable.Range(1, 5)
        .Select(level => new OrderBookLevelEnvelope(level, 100.0 + level / 10.0, level + 1))
        .ToArray(),
    new BasisReferenceEnvelope(
        request.LogicalSymbol,
        "index",
        "plus500_t4_index_v1",
        "CME_E",
        "BTCIDX",
        "XCME_E BTCIDX (U26)",
        99.0,
        evidenceObservedAt,
        evidenceAvailableAt,
        request.AsOf),
    null);
var candles = Enumerable.Range(0, request.Limit)
    .Select(index =>
    {
        var closeAt = request.AsOf.AddMinutes(-request.IntervalMinutes *
            (request.Limit - index));
        return new CandleEnvelope(
            request.LogicalSymbol,
            request.IntervalMinutes,
            closeAt.AddMinutes(-request.IntervalMinutes),
            closeAt,
            99.0,
            101.0,
            98.0,
            100.0,
            10.0,
            "XCME_E BTC (U26)",
            "plus500_t4_futures_v1",
            closeAt,
            closeAt);
    })
    .ToArray();
var officialRequest = request with
{
    ExchangeId = "CME_E",
    MarketId = "XCME_E BTC (U26)",
    BasisExchangeId = "CME_E",
    BasisContractId = "BTCIDX",
    BasisMarketId = "XCME_E BTCIDX (U26)",
};
var validEnvelope = new MarketDataEnvelope(
    5,
    "plus500_t4_futures_v1",
    "plus500_t4",
    true,
    false,
    "live_t4",
    request.LogicalSymbol,
    request.IntervalMinutes,
    officialRequest.ExchangeId,
    request.ContractId,
    officialRequest.MarketId,
    request.ContractExpiresAt,
    request.ContractRollAt,
    "front_month",
    null,
    null,
    candles,
    new ReferencePriceEnvelope(
        request.LogicalSymbol,
        "plus500_t4_futures_v1",
        officialRequest.ExchangeId,
        request.ContractId,
        officialRequest.MarketId,
        100.0,
        evidenceObservedAt,
        evidenceAvailableAt,
        request.AsOf),
    validEvidence);
FuturesEvidenceValidator.Validate(officialRequest, validEnvelope);
Expect<T4SessionUnavailableException>(() => FuturesEvidenceValidator.Validate(
    officialRequest,
    validEnvelope with { Environment = "fixture" }));
Expect<T4SessionUnavailableException>(() => FuturesEvidenceValidator.Validate(
    officialRequest,
    validEnvelope with { OrderRoutesExposed = true }));
Expect<T4SessionUnavailableException>(() => FuturesEvidenceValidator.Validate(
    officialRequest,
    validEnvelope with
    {
        FuturesEvidence = validEvidence with
        {
            BasisReference = validEvidence.BasisReference with { Source = "unapproved" },
        },
    }));
FuturesEvidenceValidator.Validate(
    officialRequest,
    validEnvelope with { Environment = "t4_simulator" });

var officialCatalog = new FuturesContractCatalog([
    new FuturesContract(
        "BTC/USD",
        "CME_E",
        "BTC",
        "XCME_E BTC (U26)",
        new DateTimeOffset(2027, 9, 25, 0, 0, 0, TimeSpan.Zero),
        new DateTimeOffset(2027, 9, 18, 0, 0, 0, TimeSpan.Zero),
        "CME_E",
        "BTCIDX",
        "XCME_E BTC Index (U26)"),
]);
Environment.SetEnvironmentVariable("T4_API_KEY", new string('k', 32));
try
{
    Environment.SetEnvironmentVariable("T4_API_ENVIRONMENT", "simulator");
    var simulatorApi = T4ApiOptions.FromEnvironment(officialCatalog);
    Assert(simulatorApi.WebSocketUri?.Host == "wss-sim.t4login.com",
        "simulator endpoint is not pinned");
    Assert(simulatorApi.AttestedEnvironment == "t4_simulator",
        "simulator environment attestation failed");

    Environment.SetEnvironmentVariable("T4_API_ENVIRONMENT", "live");
    var liveApi = T4ApiOptions.FromEnvironment(officialCatalog);
    Assert(liveApi.WebSocketUri?.Host == "wss.t4login.com",
        "live endpoint is not pinned");
    Assert(liveApi.AttestedEnvironment == "live_t4",
        "live environment attestation failed");
}
finally
{
    Environment.SetEnvironmentVariable("T4_API_ENVIRONMENT", null);
    Environment.SetEnvironmentVariable("T4_API_KEY", null);
}

await RunObservationEvidenceContractTests();

Console.WriteLine("T4 bridge contract tests passed.");

static async Task RunObservationEvidenceContractTests()
{
    Assert(ObservationEvidenceContract.ReasonCodeHeaderName ==
            "X-Crypto-Agent-T4-Reason-Code" &&
            T4ReasonCodes.Safe("provider secret") == "T4_SESSION_UNAVAILABLE",
        "market-data failures do not expose the safe reason-code contract");
    Assert(!ObservationControlActions.TryParse("missing-data", out _),
        "a non-canonical control action was accepted");
    Assert(!ObservationControlActions.TryParse("order_submit", out _),
        "an order action was accepted by observation control");
    Expect<JsonException>(() => JsonSerializer.Deserialize<ObservationControlRequest>("""
        {"campaign_id":"11111111-1111-1111-1111-111111111111",
         "action_request_id":"22222222-2222-2222-2222-222222222222",
         "scope_key":"BTC/USD:240m",
         "unexpected":true}
        """));
    Expect<JsonException>(() => JsonSerializer.Deserialize<ObservationCheckpointRequest>("""
        {"campaign_id":"11111111-1111-1111-1111-111111111111",
         "action_request_id":"22222222-2222-2222-2222-222222222222",
         "unexpected":true}
        """));

    var testDirectory = Path.Combine(
        Path.GetTempPath(), $"crypto-agent-t4-evidence-{Guid.NewGuid():N}");
    Directory.CreateDirectory(testDirectory);
    var keyPath = Path.Combine(testDirectory, "evidence-key.pem");
    var journalPath = Path.Combine(testDirectory, "events.jsonl");
    var catalogPath = Path.Combine(testDirectory, "catalog.json");
    var campaignId = Guid.NewGuid();
    var environmentNames = new[]
    {
        "T4_CONTRACT_CATALOG_PATH",
        "T4_API_ENVIRONMENT",
        "T4_API_KEY",
        "T4_EVIDENCE_SIGNING_KEY_PATH",
        "T4_OBSERVATION_EVENT_JOURNAL_PATH",
        "T4_OBSERVATION_CAMPAIGN_ID",
        "T4_OBSERVATION_CONTROL_ENABLED",
        "T4_OBSERVATION_CONTROL_TOKEN",
    };
    try
    {
        using (var key = ECDsa.Create(ECCurve.NamedCurves.nistP256))
        {
            File.WriteAllText(keyPath, key.ExportPkcs8PrivateKeyPem());
        }
        if (!OperatingSystem.IsWindows())
        {
            File.SetUnixFileMode(
                keyPath,
                UnixFileMode.UserRead | UnixFileMode.UserWrite);
        }
        File.WriteAllText(catalogPath, """
            {
              "schema_version": 2,
              "default_roll_days": 5,
              "contracts": [
                {
                  "logical_symbol": "BTC/USD",
                  "exchange_id": "CME_E",
                  "contract_id": "BTC",
                  "market_id": "XCME_E BTC (Z99)",
                  "expires_at": "2099-12-25T00:00:00Z",
                  "roll_at": "2099-12-20T00:00:00Z",
                  "basis_exchange_id": "CME_E",
                  "basis_contract_id": "BTCIDX",
                  "basis_market_id": "XCME_E BTC Index (Z99)"
                }
              ]
            }
            """);
        Environment.SetEnvironmentVariable("T4_CONTRACT_CATALOG_PATH", catalogPath);
        Environment.SetEnvironmentVariable("T4_API_ENVIRONMENT", "simulator");
        Environment.SetEnvironmentVariable("T4_API_KEY", new string('k', 32));
        Environment.SetEnvironmentVariable("T4_EVIDENCE_SIGNING_KEY_PATH", keyPath);
        Environment.SetEnvironmentVariable("T4_OBSERVATION_EVENT_JOURNAL_PATH", journalPath);
        Environment.SetEnvironmentVariable(
            "T4_OBSERVATION_CAMPAIGN_ID", campaignId.ToString("D"));
        Environment.SetEnvironmentVariable("T4_OBSERVATION_CONTROL_ENABLED", "true");
        Environment.SetEnvironmentVariable(
            "T4_OBSERVATION_CONTROL_TOKEN", new string('c', 32));

        var options = BridgeOptions.FromEnvironment();
        using var journal = new ObservationEventJournal(options, TimeProvider.System);
        Expect<InvalidOperationException>(() =>
            new ObservationEventJournal(options, TimeProvider.System));
        var firstBootId = journal.BootId;
        var reader = new OfficialT4MarketDataReader(
            options,
            new NoNetworkHttpClientFactory(),
            NullLogger<OfficialT4MarketDataReader>.Instance,
            journal,
            TimeProvider.System);
        var request = new MarketDataRequest(
            "BTC/USD",
            "CME_E",
            "BTC",
            "XCME_E BTC (Z99)",
            new DateTimeOffset(2099, 12, 25, 0, 0, 0, TimeSpan.Zero),
            new DateTimeOffset(2099, 12, 20, 0, 0, 0, TimeSpan.Zero),
            null,
            "CME_E",
            "BTCIDX",
            "XCME_E BTC Index (Z99)",
            240,
            DateTimeOffset.UtcNow,
            60);

        var actionReceipts = new Dictionary<ObservationControlAction, SignedObservationEvent>();
        foreach (var action in new[]
                 {
                     ObservationControlAction.MissingData,
                     ObservationControlAction.StaleData,
                     ObservationControlAction.RateLimit,
                 })
        {
            if (action is ObservationControlAction.MissingData or
                ObservationControlAction.RateLimit)
            {
                reader.SeedCacheEntriesForContractTest(request);
                var seeded = reader.CacheEntryCountsForContractTest;
                Assert(seeded.SnapshotKeys == 2 && seeded.HistoryKeys == 1,
                    "contract-test cache setup failed");
            }
            var actionRequestId = Guid.NewGuid();
            actionReceipts[action] = reader.ApplyControl(
                action, campaignId, actionRequestId, "BTC/USD:240m");
            Expect<InvalidOperationException>(() => reader.ApplyControl(
                ObservationControlAction.StaleData,
                campaignId,
                Guid.NewGuid(),
                "ETH/USD:240m"));
            if (action is ObservationControlAction.MissingData or
                ObservationControlAction.RateLimit)
            {
                var cleared = reader.CacheEntryCountsForContractTest;
                Assert(cleared.SnapshotKeys == 0 && cleared.HistoryKeys == 0,
                    $"{action} did not clear both T4 caches");
            }
            var expectedReason = action switch
            {
                ObservationControlAction.MissingData => "T4_MISSING_DATA",
                ObservationControlAction.StaleData => "T4_STALE_DATA",
                ObservationControlAction.RateLimit => "T4_RATE_LIMITED",
                _ => throw new InvalidOperationException(),
            };
            await ExpectSessionReasonAsync(
                () => reader.ReadAsync(
                    request with { LogicalSymbol = "ETH/USD" },
                    CancellationToken.None),
                expectedReason);
            Assert(!journal.Read(0, 1000).Events.Any(item =>
                    item.ActionRequestId == actionRequestId.ToString("D") &&
                    item.Payload.ControlStep == "consumed"),
                $"{action} was consumed by a mismatched market-data scope");
            await ExpectSessionReasonAsync(
                () => reader.ReadAsync(request, CancellationToken.None), expectedReason);
            if (action is ObservationControlAction.RateLimit)
            {
                reader.RefreshRateLimitForContractTest();
            }
            Assert(reader.Health.ReasonCode == expectedReason,
                $"{action} did not persist its fail-closed reason");
        }

        var reconnectRequestId = Guid.NewGuid();
        reader.SeedCacheEntriesForContractTest(request);
        actionReceipts[ObservationControlAction.Reconnect] = reader.ApplyControl(
            ObservationControlAction.Reconnect, campaignId, reconnectRequestId,
            "BTC/USD:240m");
        Expect<InvalidOperationException>(() => reader.ApplyControl(
            ObservationControlAction.MissingData,
            campaignId,
            Guid.NewGuid(),
            "ETH/USD:240m"));
        var reconnectCleared = reader.CacheEntryCountsForContractTest;
        Assert(reconnectCleared.SnapshotKeys == 0 && reconnectCleared.HistoryKeys == 0,
            "controlled reconnect did not clear both T4 caches");
        Assert(reader.Health.ReasonCode == "T4_SESSION_DISCONNECTED",
            "controlled reconnect did not fail closed");
        reader.RecordReadyTransition();
        Assert(reader.Health.Ready && reader.Health.ReasonCode == "READY",
            "controlled reconnect recovery was not recorded");
        reader.RecordRollEvidence(
            request with { RolledFromMarketId = "XCME_E BTC (U99)" },
            new ContractTransitionEnvelope(
                "XCME_E BTC (U99)",
                request.MarketId,
                "mid",
                100.0,
                101.0,
                "plus500_t4_futures_v1",
                request.AsOf,
                request.AsOf,
                request.AsOf));

        var checkpointRequestId = Guid.NewGuid();
        var preCheckpointHead = journal.Read(0, 1000).Events[^1];
        var checkpoint = journal.RecordCampaignCheckpoint(
            campaignId,
            checkpointRequestId,
            sessionGeneration: 0,
            reconnectCount: 0);
        Assert(checkpoint.EventType == "campaign_checkpoint" &&
                checkpoint.ReasonCode == "OBSERVATION_CAMPAIGN_CHECKPOINT" &&
                checkpoint.CampaignId == campaignId.ToString("D") &&
                checkpoint.ActionRequestId == checkpointRequestId.ToString("D") &&
                checkpoint.ScopeKey is null && checkpoint.ScenarioCode is null &&
                checkpoint.Payload.Action == "checkpoint" &&
                checkpoint.Payload.ControlStep == "consumed" &&
                checkpoint.SessionGeneration >= preCheckpointHead.SessionGeneration &&
                checkpoint.ReconnectCount >= preCheckpointHead.ReconnectCount,
            "campaign closing checkpoint is not an exact signed tail anchor");

        var restartRequestId = Guid.NewGuid();
        actionReceipts[ObservationControlAction.Restart] = reader.ApplyControl(
            ObservationControlAction.Restart, campaignId, restartRequestId,
            "BTC/USD:240m");
        var persistedBeforeStop = File.ReadAllText(journalPath);
        Assert(persistedBeforeStop.Contains(
                actionReceipts[ObservationControlAction.Restart].EventHashSha256,
                StringComparison.Ordinal),
            "restart receipt was not persisted before shutdown scheduling");
        await reader.StopAsync(CancellationToken.None);
        reader.Dispose();

        var beforeRestart = journal.Read(0, 1000);
        Assert(beforeRestart.Events.Count(item => item.EventType == "control_applied") == 5,
            "not all strict controlled actions produced receipts");
        foreach (var (action, receipt) in actionReceipts)
        {
            Assert(receipt.EventType == "control_applied" &&
                    receipt.ReasonCode == "OBSERVATION_CONTROL_APPLIED" &&
                    receipt.Payload.Action == ObservationControlActions.Code(action) &&
                    receipt.Payload.ControlStep == "applied" &&
                    receipt.ScopeKey == "BTC/USD:240m",
                $"{action} returned an invalid signed control receipt");
        }
        Assert(beforeRestart.Events.Any(item => item.EventType == "cache_cleared" &&
                item.ScenarioCode == "reconnect" &&
                item.ActionRequestId == reconnectRequestId.ToString("D")),
            "controlled reconnect did not prove cache clearing");
        Assert(beforeRestart.Events.Any(item => item.EventType == "session_ready" &&
                item.Payload.Action == "reconnect" && item.Payload.ControlStep == "consumed"),
            "controlled reconnect recovery event is absent");
        Assert(beforeRestart.Events
                .Where(item => item.EventType is "missing_data_detected" or
                    "stale_data_detected" or "rate_limited")
                .All(item => item.ScopeKey == "BTC/USD:240m"),
            "controlled data faults did not bind their signed scope");
        Assert(beforeRestart.Events.Any(item => item.EventType == "bridge_stopping" &&
                item.Payload.Action == "restart" && item.Payload.ControlStep == "consumed"),
            "restart shutdown event is absent");
        Assert(beforeRestart.Events.Any(item => item.EventType == "contract_roll_observed" &&
                item.ReasonCode == "T4_CONTRACT_ROLL_OBSERVED" &&
                item.Payload.FromMarketId == "XCME_E BTC (U99)" &&
                item.Payload.ToMarketId == request.MarketId),
            "signed contract-roll evidence is absent");
        VerifyEvidencePage(beforeRestart, campaignId);

        var tailBeforeRestart = beforeRestart.Events[^1];
        journal.Dispose();
        File.AppendAllText(journalPath, "{\"partial_uncommitted_tail\"");
        using var restartedJournal = new ObservationEventJournal(options, TimeProvider.System);
        var afterRestart = restartedJournal.Read(0, 1000);
        var restarted = afterRestart.Events[^1];
        Assert(afterRestart.BootId == restartedJournal.BootId.ToString("D") &&
                restartedJournal.BootId != firstBootId,
            "GET page did not expose the current boot ID after restart");
        Assert(afterRestart.Events.Any(item => item.BootId == firstBootId.ToString("D")),
            "events from the previous boot were not retained");
        Assert(restarted.SequenceNo == tailBeforeRestart.SequenceNo + 1 &&
                restarted.PreviousEventHash == tailBeforeRestart.EventHashSha256,
            "global journal sequence/hash continuity was lost across restart");
        VerifyEvidencePage(afterRestart, campaignId);

        Assert(T4RateLimitPolicy.Clamp(TimeSpan.Zero) == TimeSpan.FromSeconds(1) &&
                T4RateLimitPolicy.Clamp(TimeSpan.FromHours(1)) == TimeSpan.FromMinutes(5) &&
                T4RateLimitPolicy.ResolveRetryAfter(null, DateTimeOffset.UtcNow) ==
                    TimeSpan.FromSeconds(30),
            "rate-limit backoff is not bounded and persistent");
    }
    finally
    {
        foreach (var name in environmentNames)
        {
            Environment.SetEnvironmentVariable(name, null);
        }
        Directory.Delete(testDirectory, recursive: true);
    }
}

static void VerifyEvidencePage(ObservationEventPage page, Guid campaignId)
{
    Assert(page.ReadOnly && !page.OrderRoutesExposed,
        "evidence page weakened the read-only attestation");
    var publicKeyBytes = Convert.FromBase64String(page.PublicKeySpkiBase64);
    Assert(Convert.ToHexString(SHA256.HashData(publicKeyBytes)).ToLowerInvariant() ==
            page.EvidenceKeyFingerprintSha256,
        "public evidence-key fingerprint is invalid");
    using var verifier = ECDsa.Create();
    verifier.ImportSubjectPublicKeyInfo(publicKeyBytes, out var consumed);
    Assert(consumed == publicKeyBytes.Length, "public evidence key has trailing data");
    string? previousHash = null;
    long expectedSequence = 1;
    var expectedClaimNames = new[]
    {
        "schema_version", "event_id", "sequence_no", "event_at", "event_type",
        "reason_code", "campaign_id", "boot_id", "session_generation",
        "reconnect_count", "environment", "bridge_schema_version", "read_only",
        "order_routes_exposed", "scope_key", "scenario_code", "action_request_id",
        "previous_event_hash", "payload", "payload_hash_sha256",
    };
    foreach (var item in page.Events)
    {
        Assert(Regex.IsMatch(
                item.EventAt,
                @"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$",
                RegexOptions.CultureInvariant),
            "evidence timestamp is not canonical microsecond UTC");
        var canonical = Convert.FromBase64String(item.CanonicalPayloadBase64);
        var hash = SHA256.HashData(canonical);
        Assert(item.SequenceNo == expectedSequence++ &&
                item.PreviousEventHash == previousHash,
            "evidence sequence/hash link is invalid");
        Assert(Convert.ToHexString(hash).ToLowerInvariant() == item.EventHashSha256 &&
                verifier.VerifyHash(
                    hash,
                    Convert.FromBase64String(item.SignatureBase64),
                    DSASignatureFormat.Rfc3279DerSequence),
            "evidence signature is invalid");
        using var claims = JsonDocument.Parse(canonical);
        Assert(claims.RootElement.EnumerateObject().Select(property => property.Name)
                .SequenceEqual(expectedClaimNames),
            "canonical signed claim list or order changed");
        Assert(item.CampaignId == campaignId.ToString("D") && item.ReadOnly &&
                !item.OrderRoutesExposed,
            "signed campaign/read-only claims are invalid");
        previousHash = item.EventHashSha256;
    }
}

static async Task ExpectSessionReasonAsync(
    Func<Task<MarketDataEnvelope>> action,
    string expectedReason)
{
    try
    {
        await action();
    }
    catch (T4SessionUnavailableException exception)
    {
        Assert(exception.ReasonCode == expectedReason,
            $"expected {expectedReason}, received {exception.ReasonCode}");
        return;
    }
    throw new InvalidOperationException("Expected a fail-closed T4 read.");
}

static void Assert(bool condition, string message)
{
    if (!condition)
    {
        throw new InvalidOperationException(message);
    }
}

static void Expect<TException>(Action action)
    where TException : Exception
{
    try
    {
        action();
    }
    catch (TException)
    {
        return;
    }

    throw new InvalidOperationException($"Expected {typeof(TException).Name}.");
}

static async Task ExpectAsync<TException>(Func<Task> action)
    where TException : Exception
{
    try
    {
        await action();
    }
    catch (TException)
    {
        return;
    }

    throw new InvalidOperationException($"Expected {typeof(TException).Name}.");
}

sealed class NoNetworkHttpClientFactory : IHttpClientFactory
{
    public HttpClient CreateClient(string name)
    {
        _ = name;
        return new HttpClient(new NoNetworkHandler());
    }

    private sealed class NoNetworkHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(
            HttpRequestMessage request,
            CancellationToken cancellationToken)
        {
            _ = request;
            cancellationToken.ThrowIfCancellationRequested();
            throw new InvalidOperationException("A unit contract test attempted network access.");
        }
    }
}

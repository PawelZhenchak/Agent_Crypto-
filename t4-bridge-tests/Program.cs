using CryptoAgent.T4Bridge;
using Google.Protobuf.WellKnownTypes;
using T4Proto.V1.Auth;
using T4Proto.V1.Common;
using T4Proto.V1.Service;

var token = new string('t', 32);
Environment.SetEnvironmentVariable("T4_BRIDGE_TOKEN", token);
Environment.SetEnvironmentVariable("T4_BRIDGE_PORT", "8784");
Environment.SetEnvironmentVariable("T4_API_ENVIRONMENT", null);
Environment.SetEnvironmentVariable("T4_API_KEY", null);
Environment.SetEnvironmentVariable("T4_BTC_CONTRACT_ID", "SIM:MBT:TEST");
Environment.SetEnvironmentVariable(
    "T4_BTC_CONTRACT_EXPIRES_AT",
    DateTimeOffset.UtcNow.AddDays(30).ToString("O"));
Environment.SetEnvironmentVariable("T4_ETH_CONTRACT_ID", null);
Environment.SetEnvironmentVariable("T4_ETH_CONTRACT_EXPIRES_AT", null);

var options = BridgeOptions.FromEnvironment();
Assert(options.Port == 8784, "port was not parsed");
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

Console.WriteLine("T4 bridge contract tests passed.");

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

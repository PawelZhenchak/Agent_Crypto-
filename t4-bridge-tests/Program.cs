using CryptoAgent.T4Bridge;

var token = new string('t', 32);
Environment.SetEnvironmentVariable("T4_BRIDGE_TOKEN", token);
Environment.SetEnvironmentVariable("T4_BRIDGE_PORT", "8784");
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
Assert(!(request.RolledFromContractId?.Any() ?? false), "legacy contract unexpectedly rolled");

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
Assert(atRoll.RolledFromContractId == march.ContractId, "roll provenance was not retained");

Expect<T4SessionUnavailableException>(() => catalog.Resolve(
    "BTC/USD",
    new DateTimeOffset(2027, 6, 18, 0, 0, 0, TimeSpan.Zero)));
Expect<InvalidOperationException>(() => new FuturesContractCatalog([march, march]));
Expect<InvalidOperationException>(() => new FuturesContractCatalog([
    march with { ContractId = "invalid contract id" },
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
    Assert(fromFile.RolledFromContractId == "SIM:MBT:202703", "catalog roll lost provenance");
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
await ExpectAsync<T4SessionUnavailableException>(() => reader.ReadAsync(
    request,
    CancellationToken.None));

var evidenceObservedAt = request.AsOf.AddSeconds(-2);
var evidenceAvailableAt = request.AsOf.AddSeconds(-1);
var validEvidence = new FuturesEvidenceEnvelope(
    request.ContractId,
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
        99.0,
        evidenceObservedAt,
        evidenceAvailableAt,
        request.AsOf),
    null);
var validEnvelope = new MarketDataEnvelope(
    4,
    "plus500_t4_futures_v1",
    "plus500_t4",
    true,
    false,
    "live_t4",
    request.LogicalSymbol,
    request.IntervalMinutes,
    request.ContractId,
    request.ContractExpiresAt,
    request.ContractRollAt,
    "front_month",
    null,
    null,
    Array.Empty<CandleEnvelope>(),
    new ReferencePriceEnvelope(
        request.LogicalSymbol,
        "plus500_t4_futures_v1",
        100.0,
        evidenceObservedAt,
        evidenceAvailableAt,
        request.AsOf),
    validEvidence);
FuturesEvidenceValidator.Validate(request, validEnvelope);
Expect<T4SessionUnavailableException>(() => FuturesEvidenceValidator.Validate(
    request,
    validEnvelope with { Environment = "fixture" }));
Expect<T4SessionUnavailableException>(() => FuturesEvidenceValidator.Validate(
    request,
    validEnvelope with { OrderRoutesExposed = true }));
Expect<T4SessionUnavailableException>(() => FuturesEvidenceValidator.Validate(
    request,
    validEnvelope with
    {
        FuturesEvidence = validEvidence with
        {
            BasisReference = validEvidence.BasisReference with { Source = "unapproved" },
        },
    }));

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

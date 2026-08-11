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

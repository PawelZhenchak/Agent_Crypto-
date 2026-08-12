namespace CryptoAgent.T4Bridge;

public sealed record FuturesContract(
    string LogicalSymbol,
    string ExchangeId,
    string ContractId,
    string MarketId,
    DateTimeOffset ExpiresAt,
    DateTimeOffset RollAt,
    string BasisExchangeId,
    string BasisContractId,
    string BasisMarketId)
{
    public FuturesContract(
        string logicalSymbol,
        string contractId,
        DateTimeOffset expiresAt,
        DateTimeOffset rollAt)
        : this(logicalSymbol, string.Empty, contractId, string.Empty, expiresAt, rollAt,
            string.Empty, string.Empty, string.Empty)
    {
    }

    public bool IsOfficiallyAddressable =>
        ExchangeId.Length > 0 && MarketId.Length > 0 &&
        BasisExchangeId.Length > 0 && BasisContractId.Length > 0 && BasisMarketId.Length > 0;
}

public sealed record ContractSelection(
    FuturesContract Contract,
    string? RolledFromMarketId)
{
    public bool IsRolled => RolledFromMarketId is not null;
}

public sealed class FuturesContractCatalog
{
    private static readonly TimeSpan MinimumRollLead = TimeSpan.FromHours(1);
    private static readonly TimeSpan MaximumRollLead = TimeSpan.FromDays(30);
    private static readonly TimeSpan TransitionEvidenceWindow = TimeSpan.FromHours(24);

    private readonly IReadOnlyDictionary<string, IReadOnlyList<FuturesContract>> _contracts;

    public IReadOnlyList<FuturesContract> Contracts { get; }

    public bool IsOfficiallyAddressable =>
        Contracts.Count > 0 && Contracts.All(item => item.IsOfficiallyAddressable);

    public FuturesContractCatalog(IEnumerable<FuturesContract> contracts)
    {
        ArgumentNullException.ThrowIfNull(contracts);
        var materialized = contracts.ToArray();
        Contracts = materialized;
        foreach (var contract in materialized)
        {
            Validate(contract);
        }

        var duplicateId = materialized
            .GroupBy(item => item.IsOfficiallyAddressable
                ? $"{item.ExchangeId}\n{item.MarketId}"
                : $"legacy\n{item.ContractId}", StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicateId is not null)
        {
            throw new InvalidOperationException("Duplicate T4 exchange/market identity.");
        }

        var duplicateExpiry = materialized
            .GroupBy(item => (item.LogicalSymbol, item.ExpiresAt))
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicateExpiry is not null)
        {
            throw new InvalidOperationException(
                $"Duplicate expiry for {duplicateExpiry.Key.LogicalSymbol}.");
        }

        _contracts = materialized
            .GroupBy(item => item.LogicalSymbol, StringComparer.Ordinal)
            .ToDictionary(
                group => group.Key,
                group => (IReadOnlyList<FuturesContract>)group
                    .OrderBy(item => item.ExpiresAt)
                    .ToArray(),
                StringComparer.Ordinal);

        foreach (var series in _contracts.Values)
        {
            if (series.All(item => item.IsOfficiallyAddressable) &&
                series.Select(item => (item.ExchangeId, item.ContractId,
                    item.BasisExchangeId, item.BasisContractId))
                .Distinct()
                .Skip(1)
                .Any())
            {
                throw new InvalidOperationException(
                    "T4 product and basis identities must remain stable across expiries.");
            }
            for (var index = 1; index < series.Count; index++)
            {
                if (series[index].RollAt <= series[index - 1].RollAt)
                {
                    throw new InvalidOperationException(
                        "T4 contract roll timestamps must increase with expiry.");
                }
            }
        }
    }

    public ContractSelection Resolve(string logicalSymbol, DateTimeOffset asOf)
    {
        if (asOf.Offset != TimeSpan.Zero)
        {
            throw new ArgumentException("Contract resolution time must be UTC.", nameof(asOf));
        }

        if (!_contracts.TryGetValue(logicalSymbol, out var contracts) || contracts.Count == 0)
        {
            throw new T4SessionUnavailableException(
                "The requested T4 contract catalog is not configured.");
        }

        var selectedIndex = -1;
        for (var index = 0; index < contracts.Count; index++)
        {
            var candidate = contracts[index];
            if (candidate.ExpiresAt > asOf && candidate.RollAt > asOf)
            {
                selectedIndex = index;
                break;
            }
        }

        if (selectedIndex < 0)
        {
            throw new T4SessionUnavailableException(
                "No safe front-month T4 contract is available for the requested time.");
        }

        string? previous = null;
        if (selectedIndex > 0)
        {
            var transitionAt = contracts[selectedIndex - 1].RollAt;
            if (asOf >= transitionAt && asOf - transitionAt <= TransitionEvidenceWindow)
            {
                var previousContract = contracts[selectedIndex - 1];
                previous = previousContract.MarketId.Length > 0
                    ? previousContract.MarketId
                    : previousContract.ContractId;
            }
        }
        return new ContractSelection(contracts[selectedIndex], previous);
    }

    private static void Validate(FuturesContract contract)
    {
        if (contract.LogicalSymbol is not ("BTC/USD" or "ETH/USD"))
        {
            throw new InvalidOperationException("The T4 logical symbol is not approved.");
        }

        if (!ValidOpaqueId(contract.ContractId, 128) ||
            !ValidOptionalOpaqueId(contract.ExchangeId, 64) ||
            !ValidOptionalOpaqueId(contract.MarketId, 256) ||
            !ValidOptionalOpaqueId(contract.BasisExchangeId, 64) ||
            !ValidOptionalOpaqueId(contract.BasisContractId, 128) ||
            !ValidOptionalOpaqueId(contract.BasisMarketId, 256))
        {
            throw new InvalidOperationException("A T4 catalog identifier has an invalid format.");
        }
        if ((contract.ExchangeId.Length == 0) != (contract.MarketId.Length == 0) ||
            (contract.BasisExchangeId.Length == 0) != (contract.BasisContractId.Length == 0) ||
            (contract.BasisContractId.Length == 0) != (contract.BasisMarketId.Length == 0))
        {
            throw new InvalidOperationException("The T4 official market identity is incomplete.");
        }
        if (contract.IsOfficiallyAddressable &&
            contract.ExchangeId == contract.BasisExchangeId &&
            contract.ContractId == contract.BasisContractId &&
            contract.MarketId == contract.BasisMarketId)
        {
            throw new InvalidOperationException(
                "The T4 basis market must be independent from the futures market.");
        }

        var rollLead = contract.ExpiresAt - contract.RollAt;
        if (contract.ExpiresAt.Offset != TimeSpan.Zero ||
            contract.RollAt.Offset != TimeSpan.Zero ||
            rollLead < MinimumRollLead || rollLead > MaximumRollLead)
        {
            throw new InvalidOperationException(
                "T4 expiry and roll timestamps must be ordered UTC values.");
        }
    }

    private static bool ValidOptionalOpaqueId(string value, int maximumLength) =>
        value.Length == 0 || ValidOpaqueId(value, maximumLength);

    private static bool ValidOpaqueId(string value, int maximumLength) =>
        value.Length is > 0 && value.Length <= maximumLength &&
        value == value.Trim() && !value.Any(char.IsControl);
}

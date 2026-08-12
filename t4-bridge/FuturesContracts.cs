using System.Text.RegularExpressions;

namespace CryptoAgent.T4Bridge;

public sealed record FuturesContract(
    string LogicalSymbol,
    string ContractId,
    DateTimeOffset ExpiresAt,
    DateTimeOffset RollAt);

public sealed record ContractSelection(
    FuturesContract Contract,
    string? RolledFromContractId)
{
    public bool IsRolled => RolledFromContractId is not null;
}

public sealed class FuturesContractCatalog
{
    private static readonly TimeSpan MinimumRollLead = TimeSpan.FromHours(1);
    private static readonly TimeSpan MaximumRollLead = TimeSpan.FromDays(30);
    private static readonly Regex ContractIdPattern = new(
        "^[A-Za-z0-9][A-Za-z0-9._:/-]{2,127}$",
        RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);

    private readonly IReadOnlyDictionary<string, IReadOnlyList<FuturesContract>> _contracts;

    public FuturesContractCatalog(IEnumerable<FuturesContract> contracts)
    {
        ArgumentNullException.ThrowIfNull(contracts);
        var materialized = contracts.ToArray();
        foreach (var contract in materialized)
        {
            Validate(contract);
        }

        var duplicateId = materialized
            .GroupBy(item => item.ContractId, StringComparer.Ordinal)
            .FirstOrDefault(group => group.Count() > 1);
        if (duplicateId is not null)
        {
            throw new InvalidOperationException($"Duplicate T4 contract ID: {duplicateId.Key}.");
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

        var previous = selectedIndex > 0 ? contracts[selectedIndex - 1].ContractId : null;
        return new ContractSelection(contracts[selectedIndex], previous);
    }

    private static void Validate(FuturesContract contract)
    {
        if (contract.LogicalSymbol is not ("BTC/USD" or "ETH/USD"))
        {
            throw new InvalidOperationException("The T4 logical symbol is not approved.");
        }

        if (!ContractIdPattern.IsMatch(contract.ContractId))
        {
            throw new InvalidOperationException("The T4 contract ID has an invalid format.");
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
}

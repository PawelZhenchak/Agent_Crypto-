namespace CryptoAgent.T4Bridge;

public static class FuturesEvidenceValidator
{
    private const string SourceId = "plus500_t4_futures_v1";
    private const string VenueId = "plus500_t4";
    private const string BasisSourceId = "plus500_t4_index_v1";
    private const string BasisReferenceType = "index";
    private const string LiveEnvironment = "live_t4";
    private static readonly HashSet<string> SessionStatuses = ["OPEN", "CLOSED", "HALTED"];

    public static void Validate(MarketDataRequest request, MarketDataEnvelope envelope)
    {
        if (envelope is null)
        {
            throw new T4SessionUnavailableException("T4 futures envelope is unavailable.");
        }
        var evidence = envelope.FuturesEvidence;
        var expectedSelection = request.RolledFromContractId is null ? "front_month" : "rolled";
        if (evidence is null || evidence.Bids is null || evidence.Asks is null ||
            evidence.BasisReference is null ||
            envelope.SchemaVersion != 4 || envelope.SourceId != SourceId ||
            envelope.VenueId != VenueId || envelope.ReadOnly is not true ||
            envelope.OrderRoutesExposed is not false ||
            envelope.Environment != LiveEnvironment ||
            envelope.LogicalSymbol != request.LogicalSymbol ||
            envelope.IntervalMinutes != request.IntervalMinutes ||
            envelope.ContractId != request.ContractId ||
            envelope.ContractExpiresAt != request.ContractExpiresAt ||
            envelope.ContractRollAt != request.ContractRollAt ||
            envelope.ContractSelection != expectedSelection ||
            envelope.RolledFromContractId != request.RolledFromContractId ||
            evidence.ContractId != request.ContractId || evidence.SourceId != SourceId ||
            evidence.IsFullSnapshot is not true ||
            !SessionStatuses.Contains(evidence.SessionStatus) ||
            !OrderedUtc(evidence.ObservedAt, evidence.AvailableAt, evidence.IngestedAt, request.AsOf))
        {
            throw new T4SessionUnavailableException("T4 futures evidence attestation failed.");
        }

        ValidateSide(evidence.Bids, descending: true);
        ValidateSide(evidence.Asks, descending: false);
        if (evidence.Asks[0].Price <= evidence.Bids[0].Price)
        {
            throw new T4SessionUnavailableException("T4 order book is crossed or locked.");
        }

        var basis = evidence.BasisReference;
        if (basis.Symbol != request.LogicalSymbol ||
            basis.ReferenceType != BasisReferenceType || basis.Source != BasisSourceId ||
            !FinitePositive(basis.Price) ||
            Math.Abs((basis.ObservedAt - evidence.ObservedAt).TotalSeconds) > 5 ||
            !OrderedUtc(basis.ObservedAt, basis.AvailableAt, basis.IngestedAt, request.AsOf))
        {
            throw new T4SessionUnavailableException("T4 basis reference attestation failed.");
        }

        if (request.RolledFromContractId is null)
        {
            if (evidence.ContractTransition is not null)
            {
                throw new T4SessionUnavailableException("Unexpected T4 roll evidence.");
            }
            return;
        }

        var transition = evidence.ContractTransition;
        var currentMid = evidence.Bids[0].Price +
            (evidence.Asks[0].Price - evidence.Bids[0].Price) / 2.0;
        if (transition is null || transition.FromContractId != request.RolledFromContractId ||
            transition.ToContractId != request.ContractId || transition.PriceType != "mid" ||
            transition.Source != SourceId || !FinitePositive(transition.FromPrice) ||
            !FinitePositive(transition.ToPrice) ||
            !NearlyEqual(transition.ToPrice, currentMid) ||
            Math.Abs((transition.ObservedAt - evidence.ObservedAt).TotalSeconds) > 5 ||
            !OrderedUtc(
                transition.ObservedAt,
                transition.AvailableAt,
                transition.IngestedAt,
                request.AsOf))
        {
            throw new T4SessionUnavailableException("T4 roll evidence attestation failed.");
        }
    }

    private static void ValidateSide(
        IReadOnlyList<OrderBookLevelEnvelope>? levels,
        bool descending)
    {
        if (levels is null || levels.Count is < 1 or > 50)
        {
            throw new T4SessionUnavailableException("T4 order-book depth is invalid.");
        }
        for (var index = 0; index < levels.Count; index++)
        {
            var level = levels[index];
            if (level.Level != index + 1 || !FinitePositive(level.Price) ||
                !double.IsFinite(level.Quantity) || level.Quantity < 0)
            {
                throw new T4SessionUnavailableException("T4 order-book level is invalid.");
            }
            if (index == 0)
            {
                continue;
            }
            var previous = levels[index - 1].Price;
            if ((descending && level.Price >= previous) || (!descending && level.Price <= previous))
            {
                throw new T4SessionUnavailableException("T4 order-book levels are unsorted.");
            }
        }
    }

    private static bool OrderedUtc(
        DateTimeOffset observedAt,
        DateTimeOffset availableAt,
        DateTimeOffset ingestedAt,
        DateTimeOffset asOf) =>
        observedAt.Offset == TimeSpan.Zero && availableAt.Offset == TimeSpan.Zero &&
        ingestedAt.Offset == TimeSpan.Zero && asOf.Offset == TimeSpan.Zero &&
        observedAt <= availableAt && availableAt <= ingestedAt && ingestedAt <= asOf;

    private static bool FinitePositive(double value) => double.IsFinite(value) && value > 0;

    private static bool NearlyEqual(double left, double right) =>
        Math.Abs(left - right) <= Math.Max(1e-12, 1e-12 * Math.Max(Math.Abs(left), Math.Abs(right)));
}

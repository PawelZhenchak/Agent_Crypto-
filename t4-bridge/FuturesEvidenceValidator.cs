namespace CryptoAgent.T4Bridge;

public static class FuturesEvidenceValidator
{
    private const string SourceId = "plus500_t4_futures_v1";
    private const string VenueId = "plus500_t4";
    private const string BasisSourceId = "plus500_t4_index_v1";
    private const string BasisReferenceType = "index";
    private static readonly HashSet<string> Environments = ["live_t4", "t4_simulator"];
    private static readonly HashSet<string> SessionStatuses = ["OPEN", "CLOSED", "HALTED"];

    public static void Validate(MarketDataRequest request, MarketDataEnvelope envelope)
    {
        if (envelope is null)
        {
            throw new T4SessionUnavailableException("T4 futures envelope is unavailable.");
        }
        var evidence = envelope.FuturesEvidence;
        var expectedSelection = request.RolledFromMarketId is null ? "front_month" : "rolled";
        if (evidence is null || evidence.Bids is null || evidence.Asks is null ||
            evidence.BasisReference is null ||
            envelope.SchemaVersion != 5 || envelope.SourceId != SourceId ||
            envelope.VenueId != VenueId || envelope.ReadOnly is not true ||
            envelope.OrderRoutesExposed is not false ||
            !Environments.Contains(envelope.Environment) ||
            envelope.LogicalSymbol != request.LogicalSymbol ||
            envelope.IntervalMinutes != request.IntervalMinutes ||
            envelope.ExchangeId != request.ExchangeId ||
            envelope.ContractId != request.ContractId ||
            envelope.MarketId != request.MarketId ||
            envelope.ContractExpiresAt != request.ContractExpiresAt ||
            envelope.ContractRollAt != request.ContractRollAt ||
            envelope.ContractSelection != expectedSelection ||
            envelope.RolledFromMarketId != request.RolledFromMarketId ||
            evidence.ExchangeId != request.ExchangeId ||
            evidence.ContractId != request.ContractId ||
            evidence.MarketId != request.MarketId || evidence.SourceId != SourceId ||
            evidence.IsFullSnapshot is not true ||
            !SessionStatuses.Contains(evidence.SessionStatus) ||
            !OrderedUtc(evidence.ObservedAt, evidence.AvailableAt, evidence.IngestedAt,
                request.AsOf) || request.AsOf - evidence.IngestedAt > TimeSpan.FromSeconds(60))
        {
            throw new T4SessionUnavailableException("T4 futures evidence attestation failed.");
        }

        ValidateHistory(request, envelope);

        ValidateSide(evidence.Bids, descending: true);
        ValidateSide(evidence.Asks, descending: false);
        if (evidence.Asks[0].Price <= evidence.Bids[0].Price)
        {
            throw new T4SessionUnavailableException("T4 order book is crossed or locked.");
        }

        var basis = evidence.BasisReference;
        if (basis.Symbol != request.LogicalSymbol ||
            basis.ReferenceType != BasisReferenceType || basis.Source != BasisSourceId ||
            basis.ExchangeId != request.BasisExchangeId ||
            basis.ContractId != request.BasisContractId ||
            basis.MarketId != request.BasisMarketId ||
            (basis.ExchangeId == request.ExchangeId &&
             basis.ContractId == request.ContractId &&
             basis.MarketId == request.MarketId) ||
            !FinitePositive(basis.Price) ||
            Math.Abs((basis.ObservedAt - evidence.ObservedAt).TotalSeconds) > 5 ||
            !OrderedUtc(basis.ObservedAt, basis.AvailableAt, basis.IngestedAt, request.AsOf))
        {
            throw new T4SessionUnavailableException("T4 basis reference attestation failed.");
        }

        if (request.RolledFromMarketId is null)
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
        if (transition is null || transition.FromMarketId != request.RolledFromMarketId ||
            transition.ToMarketId != request.MarketId || transition.PriceType != "mid" ||
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

    private static void ValidateHistory(MarketDataRequest request, MarketDataEnvelope envelope)
    {
        if (envelope.Candles is null || envelope.Candles.Count != request.Limit ||
            envelope.ReferencePrice is null)
        {
            throw new T4SessionUnavailableException("T4 history is incomplete.");
        }
        for (var index = 0; index < envelope.Candles.Count; index++)
        {
            var candle = envelope.Candles[index];
            if (candle.Symbol != request.LogicalSymbol ||
                candle.IntervalMinutes != request.IntervalMinutes ||
                candle.Source != SourceId ||
                !ValidOpaqueId(candle.MarketId, 256) ||
                !FinitePositive(candle.Open) || !FinitePositive(candle.High) ||
                !FinitePositive(candle.Low) || !FinitePositive(candle.Close) ||
                !double.IsFinite(candle.Volume) || candle.Volume < 0 ||
                candle.Low > Math.Min(candle.Open, candle.Close) ||
                candle.High < Math.Max(candle.Open, candle.Close) ||
                !OrderedUtc(candle.OpenTime, candle.CloseTime, candle.AvailableAt,
                    candle.IngestedAt, request.AsOf))
            {
                throw new T4SessionUnavailableException("T4 candle attestation failed.");
            }
            if (index > 0 && candle.OpenTime < envelope.Candles[index - 1].CloseTime)
            {
                throw new T4SessionUnavailableException("T4 candle history overlaps.");
            }
        }
        if (envelope.Candles[^1].MarketId != request.MarketId)
        {
            throw new T4SessionUnavailableException(
                "The latest T4 candle does not identify the active MarketID.");
        }
        var reference = envelope.ReferencePrice;
        if (reference.Symbol != request.LogicalSymbol || reference.Source != SourceId ||
            reference.ExchangeId != request.ExchangeId ||
            reference.ContractId != request.ContractId ||
            reference.MarketId != request.MarketId || !FinitePositive(reference.Price) ||
            !OrderedUtc(reference.EventTime, reference.AvailableAt, reference.IngestedAt,
                request.AsOf) ||
            request.AsOf - reference.IngestedAt > TimeSpan.FromSeconds(60))
        {
            throw new T4SessionUnavailableException("T4 reference price attestation failed.");
        }
    }

    private static void ValidateSide(
        IReadOnlyList<OrderBookLevelEnvelope>? levels,
        bool descending)
    {
        if (levels is null || levels.Count is < 5 or > 50)
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

    private static bool OrderedUtc(
        DateTimeOffset openAt,
        DateTimeOffset closeAt,
        DateTimeOffset availableAt,
        DateTimeOffset ingestedAt,
        DateTimeOffset asOf) =>
        openAt.Offset == TimeSpan.Zero && closeAt.Offset == TimeSpan.Zero &&
        availableAt.Offset == TimeSpan.Zero && ingestedAt.Offset == TimeSpan.Zero &&
        asOf.Offset == TimeSpan.Zero && openAt < closeAt && closeAt <= availableAt &&
        availableAt <= ingestedAt && ingestedAt <= asOf;

    private static bool FinitePositive(double value) => double.IsFinite(value) && value > 0;

    private static bool NearlyEqual(double left, double right) =>
        Math.Abs(left - right) <= Math.Max(1e-12, 1e-12 * Math.Max(Math.Abs(left), Math.Abs(right)));

    private static bool ValidOpaqueId(string value, int maximumLength) =>
        value.Length is > 0 && value.Length <= maximumLength &&
        value == value.Trim() && !value.Any(char.IsControl);
}

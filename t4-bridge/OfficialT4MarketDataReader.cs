using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.WebSockets;
using Google.Protobuf;
using T4Proto.V1.Auth;
using T4Proto.V1.Common;
using T4Proto.V1.Market;
using T4Proto.V1.Service;

namespace CryptoAgent.T4Bridge;

public sealed class OfficialT4MarketDataReader : BackgroundService, IT4MarketDataReader
{
    private const string SourceId = "plus500_t4_futures_v1";
    private const string BasisSourceId = "plus500_t4_index_v1";
    private const int MaximumWireMessageBytes = 2_000_000;
    private const int MaximumChartBytes = 16_000_000;
    private const int MaximumSnapshotsPerMarket = 4096;
    private const int MaximumHistorySnapshots = 8;
    private static readonly TimeSpan MessageTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan MaximumSnapshotAge = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan HistoryRefreshInterval = TimeSpan.FromMinutes(5);
    // V1 live acceptance is intentionally limited to native 4-hour Chart bars.
    // Longer stitched histories remain replay-compatible but are not attested live.
    private static readonly int[] SupportedIntervals = [240];

    private readonly BridgeOptions _options;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<OfficialT4MarketDataReader> _logger;
    private readonly HashSet<string> _basisMarketIds;
    private readonly ConcurrentDictionary<string, SnapshotBuffer> _snapshots = new();
    private readonly ConcurrentDictionary<string, HistoryBuffer> _history = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly object _chartTokenGate = new();
    private ClientWebSocket? _socket;
    private T4ReaderHealth _health;
    private ChartBearerToken? _chartBearerToken;
    private TaskCompletionSource<ChartBearerToken>? _pendingChartToken;
    private string? _pendingChartTokenRequestId;
    private int _reconnectCount;
    private bool _connected;

    public OfficialT4MarketDataReader(
        BridgeOptions options,
        IHttpClientFactory httpClientFactory,
        ILogger<OfficialT4MarketDataReader> logger)
    {
        _options = options;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
        _basisMarketIds = options.ContractCatalog.Contracts
            .Select(item => item.BasisMarketId)
            .Where(item => item.Length > 0)
            .ToHashSet(StringComparer.Ordinal);
        _health = new T4ReaderHealth(
            false,
            options.Api.AttestedEnvironment,
            "T4_SESSION_STARTING",
            null,
            0);
    }

    public T4ReaderHealth Health => Volatile.Read(ref _health);

    public Task<MarketDataEnvelope> ReadAsync(
        MarketDataRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_connected || !Health.Ready)
        {
            throw new T4SessionUnavailableException("The official T4 session is not ready.");
        }

        var current = SnapshotBefore(request.MarketId, request.AsOf);
        var basis = SnapshotBefore(request.BasisMarketId, request.AsOf);
        if (current is null || basis is null ||
            !T4ChartSafetyPolicy.IsFresh(
                current.IngestedAt, request.AsOf, MaximumSnapshotAge) ||
            !T4ChartSafetyPolicy.IsFresh(
                basis.IngestedAt, request.AsOf, MaximumSnapshotAge) ||
            Math.Abs((current.ObservedAt - basis.ObservedAt).TotalSeconds) > 5)
        {
            throw new T4SessionUnavailableException(
                "A synchronized T4 futures/index snapshot is unavailable.");
        }
        var history = HistoryBefore(
            HistoryKey(request.LogicalSymbol, request.IntervalMinutes), request.AsOf);
        if (history is null ||
            !T4ChartSafetyPolicy.IsFresh(
                history.IngestedAt,
                request.AsOf,
                T4ChartSafetyPolicy.MaximumHistoryCacheAge))
        {
            throw new T4SessionUnavailableException(
                "Fresh T4 chart history was not cached before the requested cutoff.");
        }
        var bars = history.Bars
            .Where(item => item.CloseTime <= request.AsOf)
            .OrderBy(item => item.CloseTime)
            .TakeLast(request.Limit)
            .ToArray();
        if (bars.Length < request.Limit)
        {
            throw new T4SessionUnavailableException("T4 chart history is incomplete.");
        }

        var currentMid = Mid(current);
        ContractTransitionEnvelope? transition = null;
        if (request.RolledFromMarketId is not null)
        {
            var previous = SnapshotBefore(request.RolledFromMarketId, request.AsOf);
            if (previous is null ||
                !T4ChartSafetyPolicy.IsFresh(
                    previous.IngestedAt, request.AsOf, MaximumSnapshotAge) ||
                Math.Abs((previous.ObservedAt - current.ObservedAt).TotalSeconds) > 5)
            {
                throw new T4SessionUnavailableException(
                    "Synchronized T4 roll evidence is unavailable.");
            }
            var observedAt = Later(previous.ObservedAt, current.ObservedAt);
            var availableAt = Later(previous.AvailableAt, current.AvailableAt);
            var ingestedAt = Later(previous.IngestedAt, current.IngestedAt);
            transition = new ContractTransitionEnvelope(
                request.RolledFromMarketId,
                request.MarketId,
                "mid",
                Mid(previous),
                currentMid,
                SourceId,
                observedAt,
                availableAt,
                ingestedAt);
        }

        var candles = bars.Select(item => new CandleEnvelope(
            request.LogicalSymbol,
            request.IntervalMinutes,
            item.OpenTime,
            item.CloseTime,
            item.Open,
            item.High,
            item.Low,
            item.Close,
            item.Volume,
            item.MarketId,
            SourceId,
            history.IngestedAt,
            history.IngestedAt)).ToArray();
        var result = new MarketDataEnvelope(
            5,
            SourceId,
            "plus500_t4",
            true,
            false,
            _options.Api.AttestedEnvironment,
            request.LogicalSymbol,
            request.IntervalMinutes,
            request.ExchangeId,
            request.ContractId,
            request.MarketId,
            request.ContractExpiresAt,
            request.ContractRollAt,
            request.RolledFromMarketId is null ? "front_month" : "rolled",
            request.RolledFromMarketId,
            null,
            candles,
            new ReferencePriceEnvelope(
                request.LogicalSymbol,
                SourceId,
                request.ExchangeId,
                request.ContractId,
                request.MarketId,
                currentMid,
                current.ObservedAt,
                current.AvailableAt,
                current.IngestedAt),
            new FuturesEvidenceEnvelope(
                request.ExchangeId,
                request.ContractId,
                request.MarketId,
                SourceId,
                current.SessionStatus,
                true,
                current.ObservedAt,
                current.AvailableAt,
                current.IngestedAt,
                Levels(current.Bids),
                Levels(current.Asks),
                new BasisReferenceEnvelope(
                    request.LogicalSymbol,
                    "index",
                    BasisSourceId,
                    request.BasisExchangeId,
                    request.BasisContractId,
                    request.BasisMarketId,
                    Mid(basis),
                    basis.ObservedAt,
                    basis.AvailableAt,
                    basis.IngestedAt),
                transition));
        return Task.FromResult(result);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var attempt = 0;
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunSessionAsync(stoppingToken);
                attempt = 0;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (AuthenticationException)
            {
                SetUnavailable("T4_AUTHENTICATION_FAILED");
                attempt = Math.Min(attempt + 1, 8);
            }
            catch (Exception)
            {
                SetUnavailable("T4_SESSION_DISCONNECTED");
                attempt = Math.Min(attempt + 1, 8);
            }
            finally
            {
                _connected = false;
                _snapshots.Clear();
                _history.Clear();
                ClearChartToken();
                var socket = Interlocked.Exchange(ref _socket, null);
                if (socket is not null)
                {
                    socket.Abort();
                    socket.Dispose();
                }
            }
            if (!stoppingToken.IsCancellationRequested)
            {
                Interlocked.Increment(ref _reconnectCount);
                var delay = TimeSpan.FromSeconds(Math.Min(30, Math.Pow(2, attempt)));
                await Task.Delay(delay, stoppingToken);
            }
        }
    }

    private async Task RunSessionAsync(CancellationToken stoppingToken)
    {
        var socket = new ClientWebSocket();
        _socket = socket;
        using var connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        connectTimeout.CancelAfter(TimeSpan.FromSeconds(10));
        await socket.ConnectAsync(
            _options.Api.WebSocketUri ?? throw new InvalidOperationException(
                "The T4 WebSocket endpoint is not configured."),
            connectTimeout.Token);
        await AuthenticateAsync(socket, stoppingToken);
        await SubscribeAsync(socket, stoppingToken);
        _connected = true;
        SetUnavailable("T4_PREWARM_IN_PROGRESS");

        using var session = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        var receive = ReceiveLoopAsync(socket, session.Token);
        var heartbeat = HeartbeatLoopAsync(socket, session.Token);
        var history = HistoryLoopAsync(session.Token);
        var completed = await Task.WhenAny(receive, heartbeat, history);
        try
        {
            await completed;
        }
        finally
        {
            session.Cancel();
            foreach (var task in new[] { receive, heartbeat, history })
            {
                if (!ReferenceEquals(task, completed))
                {
                    await IgnoreCancellationAsync(task);
                }
            }
        }
    }

    private async Task AuthenticateAsync(
        ClientWebSocket socket,
        CancellationToken cancellationToken)
    {
        var login = new LoginRequest
        {
            ApiKey = _options.Api.ApiKey,
            PriceFormat = PriceFormat.Real,
        };
        await SendAsync(socket, new ClientMessage { LoginRequest = login }, cancellationToken);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        var response = await ReceiveAsync(socket, timeout.Token);
        if (response.PayloadCase != ServerMessage.PayloadOneofCase.LoginResponse ||
            response.LoginResponse.Result != LoginResult.Success)
        {
            throw new AuthenticationException("Official T4 API authentication failed.");
        }
        var requiredExchanges = _options.ContractCatalog.Contracts
            .SelectMany(item => new[] { item.ExchangeId, item.BasisExchangeId })
            .Distinct(StringComparer.Ordinal);
        foreach (var exchange in requiredExchanges)
        {
            var permission = response.LoginResponse.Exchanges.FirstOrDefault(item =>
                item.ExchangeId == exchange);
            if (permission is null ||
                !T4ChartSafetyPolicy.HasRealDepthPermission(permission.MarketDataType))
            {
                throw new AuthenticationException(
                    "Official T4 depth permission is missing.");
            }
        }
        if (!StoreChartToken(response.LoginResponse.AuthenticationToken))
        {
            await RequestInitialChartTokenAsync(socket, cancellationToken);
        }
    }

    private async Task RequestInitialChartTokenAsync(
        ClientWebSocket socket,
        CancellationToken cancellationToken)
    {
        var requestId = Guid.NewGuid().ToString("N", CultureInfo.InvariantCulture);
        await SendAsync(
            socket,
            new ClientMessage
            {
                AuthenticationTokenRequest = new AuthenticationTokenRequest
                {
                    RequestId = requestId,
                },
            },
            cancellationToken);
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        while (true)
        {
            var response = await ReceiveAsync(socket, timeout.Token);
            if (response.PayloadCase == ServerMessage.PayloadOneofCase.Heartbeat)
            {
                continue;
            }
            if (response.PayloadCase != ServerMessage.PayloadOneofCase.AuthenticationToken ||
                response.AuthenticationToken.RequestId != requestId ||
                !StoreChartToken(response.AuthenticationToken))
            {
                throw new AuthenticationException(
                    "The official T4 Chart bearer token was not issued.");
            }
            return;
        }
    }

    private async Task SubscribeAsync(
        ClientWebSocket socket,
        CancellationToken cancellationToken)
    {
        var subscriptions = _options.ContractCatalog.Contracts
            .SelectMany(item => new[]
            {
                (item.ExchangeId, item.ContractId, item.MarketId),
                (item.BasisExchangeId, item.BasisContractId, item.BasisMarketId),
            })
            .Distinct();
        foreach (var (exchangeId, contractId, marketId) in subscriptions)
        {
            var subscribe = new MarketDepthSubscribe
            {
                ExchangeId = exchangeId,
                ContractId = contractId,
                MarketId = marketId,
                Buffer = DepthBuffer.SmartTrade,
                DepthLevels = DepthLevels.Normal,
            };
            await SendAsync(
                socket,
                new ClientMessage { MarketDepthSubscribe = subscribe },
                cancellationToken);
        }
    }

    private async Task ReceiveLoopAsync(
        ClientWebSocket socket,
        CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(MessageTimeout);
            var message = await ReceiveAsync(socket, timeout.Token);
            var receivedAt = DateTimeOffset.UtcNow;
            Volatile.Write(ref _health, Health with { LastMessageAt = receivedAt });
            switch (message.PayloadCase)
            {
                case ServerMessage.PayloadOneofCase.Heartbeat:
                    break;
                case ServerMessage.PayloadOneofCase.AuthenticationToken:
                    CompleteChartTokenRequest(message.AuthenticationToken);
                    break;
                case ServerMessage.PayloadOneofCase.MarketDepth:
                    StoreDepth(message.MarketDepth, receivedAt);
                    break;
                case ServerMessage.PayloadOneofCase.MarketDepthTrade:
                    if (message.MarketDepthTrade.Delayed)
                    {
                        _snapshots.Clear();
                        SetUnavailable("T4_DELAYED_MARKET_DATA");
                    }
                    break;
                case ServerMessage.PayloadOneofCase.MarketSnapshot:
                    if (message.MarketSnapshot.Delayed)
                    {
                        _snapshots.Clear();
                        SetUnavailable("T4_DELAYED_MARKET_DATA");
                        break;
                    }
                    foreach (var item in message.MarketSnapshot.Messages)
                    {
                        if (item.PayloadCase ==
                            MarketSnapshotMessage.PayloadOneofCase.MarketDepth)
                        {
                            if (item.MarketDepth.Mode == MarketMode.Undefined)
                            {
                                item.MarketDepth.Mode = message.MarketSnapshot.Mode;
                            }
                            StoreDepth(item.MarketDepth, receivedAt);
                        }
                    }
                    break;
                case ServerMessage.PayloadOneofCase.MarketDepthSubscribeReject:
                    _snapshots.Clear();
                    SetUnavailable("T4_DEPTH_SUBSCRIPTION_REJECTED");
                    break;
            }
            RefreshHealth();
        }
    }

    private async Task HeartbeatLoopAsync(
        ClientWebSocket socket,
        CancellationToken cancellationToken)
    {
        using var timer = new PeriodicTimer(HeartbeatInterval);
        while (await timer.WaitForNextTickAsync(cancellationToken))
        {
            await SendAsync(
                socket,
                new ClientMessage
                {
                    Heartbeat = new Heartbeat
                    {
                        Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    },
                },
                cancellationToken);
        }
    }

    private async Task HistoryLoopAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            foreach (var logicalSymbol in _options.ContractCatalog.Contracts
                         .Select(item => item.LogicalSymbol)
                         .Distinct(StringComparer.Ordinal))
            {
                FuturesContract contract;
                try
                {
                    contract = _options.ContractCatalog.Resolve(
                        logicalSymbol, DateTimeOffset.UtcNow).Contract;
                }
                catch (T4SessionUnavailableException)
                {
                    SetUnavailable("T4_CONTRACT_UNAVAILABLE");
                    continue;
                }
                foreach (var intervalMinutes in SupportedIntervals)
                {
                    try
                    {
                        var snapshot = await RequestHistoryAsync(
                            contract, intervalMinutes, cancellationToken);
                        _history.GetOrAdd(
                                HistoryKey(contract.LogicalSymbol, intervalMinutes),
                                _ => new HistoryBuffer())
                            .Add(snapshot);
                    }
                    catch (OperationCanceledException)
                        when (cancellationToken.IsCancellationRequested)
                    {
                        throw;
                    }
                    catch (HttpRequestException exception)
                        when (exception.StatusCode == HttpStatusCode.TooManyRequests)
                    {
                        _history.Clear();
                        SetUnavailable("T4_RATE_LIMITED");
                    }
                    catch (Exception)
                    {
                        _history.Clear();
                        SetUnavailable("T4_HISTORY_UNAVAILABLE");
                    }
                }
            }
            RefreshHealth();
            await Task.Delay(HistoryRefreshInterval, cancellationToken);
        }
    }

    private async Task<ChartHistory> RequestHistoryAsync(
        FuturesContract contract,
        int intervalMinutes,
        CancellationToken cancellationToken)
    {
        const int requestedLimit = 720;
        var (barInterval, barPeriod) = intervalMinutes switch
        {
            240 => ("Hour", 4),
            1440 => ("Day", 1),
            10080 => ("Week", 1),
            _ => throw new InvalidOperationException("Unsupported T4 chart interval."),
        };
        var end = DateTimeOffset.UtcNow;
        var start = end.AddMinutes(-intervalMinutes * requestedLimit * 1.5);
        var query = string.Join("&", new Dictionary<string, string>
        {
            ["exchangeId"] = contract.ExchangeId,
            ["contractId"] = contract.ContractId,
            ["chartType"] = "Bar",
            ["barInterval"] = barInterval,
            ["barPeriod"] = barPeriod.ToString(CultureInfo.InvariantCulture),
            ["tradeDateStart"] = start.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            ["tradeDateEnd"] = end.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            ["continuationType"] = "Volume",
        }.Select(item => $"{Uri.EscapeDataString(item.Key)}={Uri.EscapeDataString(item.Value)}"));
        var endpoint = new Uri(
            _options.Api.RestUri ?? throw new InvalidOperationException(
                "The T4 REST endpoint is not configured."),
            $"chart/barchart?{query}");
        using var request = new HttpRequestMessage(HttpMethod.Get, endpoint);
        var bearerToken = await GetChartBearerTokenAsync(cancellationToken);
        request.Headers.Authorization = new AuthenticationHeaderValue(
            "Bearer", bearerToken);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/t4"));
        var client = _httpClientFactory.CreateClient("T4Chart");
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        using var response = await client.SendAsync(
            request,
            HttpCompletionOption.ResponseHeadersRead,
            timeout.Token);
        if (response.StatusCode == HttpStatusCode.TooManyRequests)
        {
            throw new HttpRequestException(
                "The T4 Chart API rate limit was reached.",
                null,
                response.StatusCode);
        }
        response.EnsureSuccessStatusCode();
        var mediaType = response.Content.Headers.ContentType?.MediaType;
        if (mediaType is not ("application/t4" or "application/octet-stream"))
        {
            throw new InvalidDataException("The T4 Chart API content type is invalid.");
        }
        var contentLength = response.Content.Headers.ContentLength;
        if (contentLength is > MaximumChartBytes)
        {
            throw new InvalidDataException("The T4 Chart API response is too large.");
        }
        await using var responseStream = await response.Content.ReadAsStreamAsync(timeout.Token);
        var payload = await ReadBoundedAsync(
            responseStream, MaximumChartBytes, timeout.Token);
        if (!T4ChartSafetyPolicy.HasValidBinaryEnvelope(payload))
        {
            throw new InvalidDataException("The T4 Chart binary envelope is invalid.");
        }
        await using var chartStream = new MemoryStream(payload, writable: false);
        var decoder = new T4ChartDecoder.T4BinaryDecoder();
        var collection = await decoder.DecodeAll(chartStream, timeout.Token);
        var bars = DecodeBars(collection.Bars);
        var ingestedAt = DateTimeOffset.UtcNow;
        if (bars.Count < 60 || bars.Any(item => item.CloseTime > ingestedAt))
        {
            throw new InvalidDataException("The decoded T4 chart history is incomplete.");
        }
        return new ChartHistory(
            bars.OrderBy(item => item.CloseTime).TakeLast(requestedLimit).ToArray(),
            ingestedAt);
    }

    private static IReadOnlyList<ChartBar> DecodeBars(
        IReadOnlyList<T4ChartDecoder.Bar> items)
    {
        if (items.Count > 5_000)
        {
            throw new InvalidDataException("The T4 Chart API returned too many bars.");
        }
        var result = new List<ChartBar>();
        foreach (var record in items)
        {
            var marketId = record.MarketId;
            var openTime = ToUtc(record.Time);
            var closeTime = ToUtc(record.CloseTime);
            var open = T4ChartSafetyPolicy.PriceToDouble(record.Open);
            var high = T4ChartSafetyPolicy.PriceToDouble(record.High);
            var low = T4ChartSafetyPolicy.PriceToDouble(record.Low);
            var close = T4ChartSafetyPolicy.PriceToDouble(record.Close);
            var volume = (double)record.Volume;
            if (!ValidOpaqueId(marketId, 256) || openTime >= closeTime ||
                !FinitePositive(open) || !FinitePositive(high) || !FinitePositive(low) ||
                !FinitePositive(close) || !double.IsFinite(volume) || volume < 0 ||
                low > Math.Min(open, close) || high < Math.Max(open, close))
            {
                throw new InvalidDataException("A decoded T4 chart bar is invalid.");
            }
            result.Add(new ChartBar(
                marketId,
                openTime,
                closeTime,
                open,
                high,
                low,
                close,
                volume));
        }
        if (result.GroupBy(item => item.CloseTime).Any(group => group.Count() > 1))
        {
            throw new InvalidDataException("The T4 Chart API returned duplicate bars.");
        }
        return result;
    }

    private void StoreDepth(MarketDepth depth, DateTimeOffset receivedAt)
    {
        if (depth is null)
        {
            return;
        }
        if (depth.Delayed)
        {
            _snapshots.Clear();
            SetUnavailable("T4_DELAYED_MARKET_DATA");
            return;
        }
        var minimumDepth = _basisMarketIds.Contains(depth.MarketId)
            ? 1
            : 5;
        if (depth.Time is null ||
            !ValidOpaqueId(depth.MarketId, 256) ||
            !TrySessionStatus(depth.Mode, out var sessionStatus) ||
            depth.Bids.Count < minimumDepth || depth.Offers.Count < minimumDepth)
        {
            return;
        }
        var observedAt = new DateTimeOffset(
            DateTime.SpecifyKind(depth.Time.ToDateTime(), DateTimeKind.Utc));
        if (observedAt > receivedAt.AddSeconds(2))
        {
            return;
        }
        var bids = ParseLevels(
            depth.Bids, descending: true, minimumDepth: minimumDepth);
        var asks = ParseLevels(
            depth.Offers, descending: false, minimumDepth: minimumDepth);
        if (bids is null || asks is null || asks[0].Price <= bids[0].Price)
        {
            return;
        }
        var snapshot = new DepthSnapshot(
            depth.MarketId,
            sessionStatus,
            observedAt,
            receivedAt,
            receivedAt,
            bids,
            asks);
        _snapshots.GetOrAdd(depth.MarketId, _ => new SnapshotBuffer()).Add(snapshot);
    }

    private static IReadOnlyList<DepthLevel>? ParseLevels(
        IEnumerable<MarketDepth.Types.DepthLine> source,
        bool descending,
        int minimumDepth)
    {
        var result = new List<DepthLevel>();
        foreach (var line in source.Take(50))
        {
            if (line.Price is null ||
                !double.TryParse(line.Price.Value, NumberStyles.Float,
                    CultureInfo.InvariantCulture, out var price) ||
                !FinitePositive(price) || line.Volume < 0)
            {
                return null;
            }
            if (result.Count > 0 &&
                ((descending && price >= result[^1].Price) ||
                 (!descending && price <= result[^1].Price)))
            {
                return null;
            }
            result.Add(new DepthLevel(price, line.Volume));
        }
        return result.Count >= minimumDepth ? result : null;
    }

    private async Task<ServerMessage> ReceiveAsync(
        ClientWebSocket socket,
        CancellationToken cancellationToken)
    {
        await using var stream = new MemoryStream();
        var buffer = new byte[8192];
        while (true)
        {
            var result = await socket.ReceiveAsync(
                new ArraySegment<byte>(buffer), cancellationToken);
            if (result.MessageType == WebSocketMessageType.Close)
            {
                throw new WebSocketException("The T4 server closed the WebSocket session.");
            }
            if (result.MessageType != WebSocketMessageType.Binary ||
                stream.Length + result.Count > MaximumWireMessageBytes)
            {
                throw new InvalidDataException("The T4 WebSocket message is invalid.");
            }
            await stream.WriteAsync(buffer.AsMemory(0, result.Count), cancellationToken);
            if (result.EndOfMessage)
            {
                return ServerMessage.Parser.ParseFrom(stream.ToArray());
            }
        }
    }

    private async Task SendAsync(
        ClientWebSocket socket,
        ClientMessage message,
        CancellationToken cancellationToken)
    {
        if (!T4OutboundPolicy.IsAllowed(message.PayloadCase))
        {
            throw new InvalidOperationException(
                "The read-only T4 bridge rejected a non-market-data outbound message.");
        }
        var payload = message.ToByteArray();
        await _sendLock.WaitAsync(cancellationToken);
        try
        {
            await socket.SendAsync(
                new ArraySegment<byte>(payload),
                WebSocketMessageType.Binary,
                true,
                cancellationToken);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private async Task<string> GetChartBearerTokenAsync(
        CancellationToken cancellationToken)
    {
        Task<ChartBearerToken> pending;
        string? requestId = null;
        lock (_chartTokenGate)
        {
            if (_chartBearerToken is not null &&
                _chartBearerToken.ExpiresAt >
                    DateTimeOffset.UtcNow + T4ChartSafetyPolicy.TokenRefreshLead)
            {
                return _chartBearerToken.Value;
            }
            if (_pendingChartToken is null)
            {
                _pendingChartToken = new TaskCompletionSource<ChartBearerToken>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                _pendingChartTokenRequestId = Guid.NewGuid().ToString("N");
                requestId = _pendingChartTokenRequestId;
            }
            pending = _pendingChartToken.Task;
        }

        if (requestId is not null)
        {
            var socket = _socket;
            if (socket is null || socket.State != WebSocketState.Open)
            {
                FailChartTokenRequest(
                    new T4SessionUnavailableException("The T4 session is unavailable."));
            }
            else
            {
                try
                {
                    await SendAsync(
                        socket,
                        new ClientMessage
                        {
                            AuthenticationTokenRequest = new AuthenticationTokenRequest
                            {
                                RequestId = requestId,
                            },
                        },
                        cancellationToken);
                }
                catch (Exception exception)
                {
                    FailChartTokenRequest(exception);
                }
            }
        }

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        ChartBearerToken token;
        try
        {
            token = await pending.WaitAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            var exception = new T4SessionUnavailableException(
                "The T4 Chart bearer token request timed out.");
            FailChartTokenRequest(exception);
            throw exception;
        }
        if (token.ExpiresAt <= DateTimeOffset.UtcNow + T4ChartSafetyPolicy.TokenRefreshLead)
        {
            throw new T4SessionUnavailableException("The T4 Chart bearer token is expired.");
        }
        return token.Value;
    }

    private bool StoreChartToken(AuthenticationToken? token)
    {
        if (!T4ChartSafetyPolicy.TryReadBearerToken(
                token,
                DateTimeOffset.UtcNow,
                out var value,
                out var expiresAt))
        {
            return false;
        }
        lock (_chartTokenGate)
        {
            _chartBearerToken = new ChartBearerToken(value, expiresAt);
        }
        return true;
    }

    private void CompleteChartTokenRequest(AuthenticationToken token)
    {
        TaskCompletionSource<ChartBearerToken>? completion = null;
        ChartBearerToken? result = null;
        lock (_chartTokenGate)
        {
            if (_pendingChartToken is null ||
                token.RequestId != _pendingChartTokenRequestId ||
                !T4ChartSafetyPolicy.TryReadBearerToken(
                    token,
                    DateTimeOffset.UtcNow,
                    out var value,
                    out var expiresAt))
            {
                return;
            }
            result = new ChartBearerToken(value, expiresAt);
            _chartBearerToken = result;
            completion = _pendingChartToken;
            _pendingChartToken = null;
            _pendingChartTokenRequestId = null;
        }
        completion!.TrySetResult(result!);
    }

    private void FailChartTokenRequest(Exception exception)
    {
        TaskCompletionSource<ChartBearerToken>? completion;
        lock (_chartTokenGate)
        {
            completion = _pendingChartToken;
            _pendingChartToken = null;
            _pendingChartTokenRequestId = null;
        }
        completion?.TrySetException(exception);
    }

    private void ClearChartToken()
    {
        TaskCompletionSource<ChartBearerToken>? completion;
        lock (_chartTokenGate)
        {
            _chartBearerToken = null;
            completion = _pendingChartToken;
            _pendingChartToken = null;
            _pendingChartTokenRequestId = null;
        }
        completion?.TrySetCanceled();
    }

    private DepthSnapshot? SnapshotBefore(string marketId, DateTimeOffset asOf) =>
        _snapshots.TryGetValue(marketId, out var buffer) ? buffer.Before(asOf) : null;

    private ChartHistory? HistoryBefore(string logicalSymbol, DateTimeOffset asOf) =>
        _history.TryGetValue(logicalSymbol, out var buffer) ? buffer.Before(asOf) : null;

    private void RefreshHealth()
    {
        if (!_connected)
        {
            return;
        }
        var now = DateTimeOffset.UtcNow;
        foreach (var symbol in _options.ContractCatalog.Contracts
                     .Select(item => item.LogicalSymbol)
                     .Distinct(StringComparer.Ordinal))
        {
            ContractSelection selection;
            try
            {
                selection = _options.ContractCatalog.Resolve(symbol, now);
            }
            catch (T4SessionUnavailableException)
            {
                SetUnavailable("T4_CONTRACT_UNAVAILABLE");
                return;
            }
            var current = SnapshotBefore(selection.Contract.MarketId, now);
            var basis = SnapshotBefore(selection.Contract.BasisMarketId, now);
            if (current is null || basis is null ||
                !T4ChartSafetyPolicy.IsFresh(
                    current.IngestedAt, now, MaximumSnapshotAge) ||
                !T4ChartSafetyPolicy.IsFresh(
                    basis.IngestedAt, now, MaximumSnapshotAge) ||
                Math.Abs((current.ObservedAt - basis.ObservedAt).TotalSeconds) > 5)
            {
                SetUnavailable("T4_PREWARM_IN_PROGRESS");
                return;
            }
            if (SupportedIntervals.Any(interval =>
                {
                    var cached = HistoryBefore(HistoryKey(symbol, interval), now);
                    return cached is null || !T4ChartSafetyPolicy.IsFresh(
                        cached.IngestedAt,
                        now,
                        T4ChartSafetyPolicy.MaximumHistoryCacheAge);
                }))
            {
                SetUnavailable("T4_HISTORY_UNAVAILABLE");
                return;
            }
        }
        Volatile.Write(ref _health, Health with
        {
            Ready = true,
            ReasonCode = "READY",
            ReconnectCount = Volatile.Read(ref _reconnectCount),
        });
    }

    private void SetUnavailable(string reasonCode)
    {
        if (Health.ReasonCode != reasonCode)
        {
            _logger.LogInformation(
                "Official T4 reader state changed to {ReasonCode}.", reasonCode);
        }
        Volatile.Write(ref _health, Health with
        {
            Ready = false,
            ReasonCode = reasonCode,
            ReconnectCount = Volatile.Read(ref _reconnectCount),
        });
    }

    private static IReadOnlyList<OrderBookLevelEnvelope> Levels(
        IReadOnlyList<DepthLevel> levels) => levels
        .Select((item, index) => new OrderBookLevelEnvelope(
            index + 1, item.Price, item.Quantity))
        .ToArray();

    private static double Mid(DepthSnapshot snapshot) =>
        snapshot.Bids[0].Price +
        (snapshot.Asks[0].Price - snapshot.Bids[0].Price) / 2.0;

    private static DateTimeOffset Later(DateTimeOffset left, DateTimeOffset right) =>
        left >= right ? left : right;

    private static bool TrySessionStatus(MarketMode mode, out string status)
    {
        status = mode switch
        {
            MarketMode.Open => "OPEN",
            MarketMode.Closed => "CLOSED",
            MarketMode.Halted => "HALTED",
            _ => string.Empty,
        };
        return status.Length > 0;
    }

    private static DateTimeOffset ToUtc(DateTime value)
    {
        if (value.Kind == DateTimeKind.Utc)
        {
            return new DateTimeOffset(value);
        }
        var unspecified = DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
        var zone = CentralTimeZone.Value;
        if (zone.IsInvalidTime(unspecified) || zone.IsAmbiguousTime(unspecified))
        {
            throw new InvalidDataException("An ambiguous T4 Central timestamp was rejected.");
        }
        return new DateTimeOffset(TimeZoneInfo.ConvertTimeToUtc(unspecified, zone));
    }

    private static readonly Lazy<TimeZoneInfo> CentralTimeZone = new(() =>
    {
        foreach (var id in new[] { "America/Chicago", "Central Standard Time" })
        {
            try
            {
                return TimeZoneInfo.FindSystemTimeZoneById(id);
            }
            catch (TimeZoneNotFoundException)
            {
            }
        }
        throw new InvalidOperationException("The T4 Central time zone is unavailable.");
    });

    private static bool ValidOpaqueId(string value, int maximumLength) =>
        value.Length is > 0 && value.Length <= maximumLength &&
        value == value.Trim() && !value.Any(char.IsControl);

    private static bool FinitePositive(double value) => double.IsFinite(value) && value > 0;

    private static string HistoryKey(string logicalSymbol, int intervalMinutes) =>
        $"{logicalSymbol}:{intervalMinutes}";

    private static async Task<byte[]> ReadBoundedAsync(
        Stream stream,
        int maximumBytes,
        CancellationToken cancellationToken)
    {
        await using var bounded = new MemoryStream();
        var buffer = new byte[81920];
        while (true)
        {
            var read = await stream.ReadAsync(buffer, cancellationToken);
            if (read == 0)
            {
                return bounded.ToArray();
            }
            if (bounded.Length + read > maximumBytes)
            {
                throw new InvalidDataException("The T4 Chart API response is too large.");
            }
            await bounded.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
        }
    }

    private static async Task IgnoreCancellationAsync(Task task)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
        }
    }

    private sealed class SnapshotBuffer
    {
        private readonly object _gate = new();
        private readonly List<DepthSnapshot> _items = [];

        public void Add(DepthSnapshot snapshot)
        {
            lock (_gate)
            {
                _items.Add(snapshot);
                _items.Sort((left, right) => left.IngestedAt.CompareTo(right.IngestedAt));
                if (_items.Count > MaximumSnapshotsPerMarket)
                {
                    _items.RemoveRange(0, _items.Count - MaximumSnapshotsPerMarket);
                }
            }
        }

        public DepthSnapshot? Before(DateTimeOffset asOf)
        {
            lock (_gate)
            {
                return _items.LastOrDefault(item => item.IngestedAt <= asOf);
            }
        }
    }

    private sealed class HistoryBuffer
    {
        private readonly object _gate = new();
        private readonly List<ChartHistory> _items = [];

        public void Add(ChartHistory snapshot)
        {
            lock (_gate)
            {
                _items.Add(snapshot);
                _items.Sort((left, right) => left.IngestedAt.CompareTo(right.IngestedAt));
                if (_items.Count > MaximumHistorySnapshots)
                {
                    _items.RemoveRange(0, _items.Count - MaximumHistorySnapshots);
                }
            }
        }

        public ChartHistory? Before(DateTimeOffset asOf)
        {
            lock (_gate)
            {
                return _items.LastOrDefault(item => item.IngestedAt <= asOf);
            }
        }
    }

    private sealed record DepthLevel(double Price, double Quantity);

    private sealed record DepthSnapshot(
        string MarketId,
        string SessionStatus,
        DateTimeOffset ObservedAt,
        DateTimeOffset AvailableAt,
        DateTimeOffset IngestedAt,
        IReadOnlyList<DepthLevel> Bids,
        IReadOnlyList<DepthLevel> Asks);

    private sealed record ChartBar(
        string MarketId,
        DateTimeOffset OpenTime,
        DateTimeOffset CloseTime,
        double Open,
        double High,
        double Low,
        double Close,
        double Volume);

    private sealed record ChartHistory(
        IReadOnlyList<ChartBar> Bars,
        DateTimeOffset IngestedAt);

    private sealed record ChartBearerToken(
        string Value,
        DateTimeOffset ExpiresAt);
}

public static class T4OutboundPolicy
{
    public static bool IsAllowed(ClientMessage.PayloadOneofCase payload) => payload is
        ClientMessage.PayloadOneofCase.LoginRequest or
        ClientMessage.PayloadOneofCase.AuthenticationTokenRequest or
        ClientMessage.PayloadOneofCase.Heartbeat or
        ClientMessage.PayloadOneofCase.MarketDepthSubscribe;
}

public static class T4ChartSafetyPolicy
{
    public static readonly TimeSpan TokenRefreshLead = TimeSpan.FromSeconds(30);
    public static readonly TimeSpan MaximumHistoryCacheAge = TimeSpan.FromMinutes(10);

    public static bool IsFresh(
        DateTimeOffset ingestedAt,
        DateTimeOffset cutoff,
        TimeSpan maximumAge) =>
        ingestedAt.Offset == TimeSpan.Zero && cutoff.Offset == TimeSpan.Zero &&
        maximumAge > TimeSpan.Zero && ingestedAt <= cutoff &&
        cutoff - ingestedAt <= maximumAge;

    public static bool HasValidBinaryEnvelope(ReadOnlySpan<byte> payload) =>
        payload.Length >= 8 &&
        System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(payload[4..8]) ==
            payload.Length - 8;

    public static bool HasRealDepthPermission(MarketDataType marketDataType)
    {
        var value = (int)marketDataType;
        return value >= 0 && (value & (int)MarketDataType.Depth) != 0 &&
            (value & (int)MarketDataType.Delayed) == 0;
    }

    public static bool TryReadBearerToken(
        AuthenticationToken? token,
        DateTimeOffset now,
        out string value,
        out DateTimeOffset expiresAt)
    {
        value = string.Empty;
        expiresAt = default;
        if (token is null || token.ExpireTime is null ||
            token.FailMessage.Length > 0 ||
            token.Token.Length is < 16 or > 8_192 ||
            token.Token.Any(char.IsWhiteSpace) || token.Token.Any(char.IsControl) ||
            now.Offset != TimeSpan.Zero)
        {
            return false;
        }
        try
        {
            expiresAt = new DateTimeOffset(token.ExpireTime.ToDateTime());
        }
        catch (InvalidOperationException)
        {
            return false;
        }
        catch (ArgumentOutOfRangeException)
        {
            return false;
        }
        if (expiresAt.Offset != TimeSpan.Zero ||
            expiresAt <= now + TokenRefreshLead)
        {
            expiresAt = default;
            return false;
        }
        value = token.Token;
        return true;
    }

    public static double PriceToDouble(T4.Price price)
    {
        var value = decimal.ToDouble(price.DecimalValue);
        if (!double.IsFinite(value))
        {
            throw new InvalidDataException("A decoded T4 Chart price is invalid.");
        }
        return value;
    }
}

public sealed class AuthenticationException : Exception
{
    public AuthenticationException(string message)
        : base(message)
    {
    }
}

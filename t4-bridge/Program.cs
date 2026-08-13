using CryptoAgent.T4Bridge;
using Microsoft.AspNetCore.Http.Json;

var options = BridgeOptions.FromEnvironment();
var builder = WebApplication.CreateBuilder(args);
builder.WebHost.ConfigureKestrel(server => server.ListenLocalhost(options.Port));
builder.Services.Configure<JsonOptions>(json =>
{
    json.SerializerOptions.PropertyNamingPolicy = null;
});
builder.Services.AddSingleton(options);
builder.Services.AddSingleton<TimeProvider>(TimeProvider.System);
builder.Services.AddSingleton<ObservationEventJournal>();
if (options.Api.IsConfigured)
{
    builder.Services.AddHttpClient("T4Chart")
        .ConfigurePrimaryHttpMessageHandler(() => new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            AutomaticDecompression = System.Net.DecompressionMethods.None,
            ConnectTimeout = TimeSpan.FromSeconds(10),
        });
    builder.Services.AddSingleton<OfficialT4MarketDataReader>();
    builder.Services.AddSingleton<IT4MarketDataReader>(services =>
        services.GetRequiredService<OfficialT4MarketDataReader>());
    builder.Services.AddSingleton<IHostedService>(services =>
        services.GetRequiredService<OfficialT4MarketDataReader>());
}
else
{
    builder.Services.AddSingleton<IT4MarketDataReader,
        T4ApplicationRegistrationPendingReader>();
}

var app = builder.Build();

app.Use(async (context, next) =>
{
    var supplied = context.Request.Headers[BridgeSecurity.TokenHeaderName].ToString();
    if (!BridgeSecurity.TokenMatches(supplied, options.Token))
    {
        context.Response.StatusCode = StatusCodes.Status401Unauthorized;
        return;
    }

    context.Response.Headers["Cache-Control"] = "no-store";
    context.Response.Headers["X-Content-Type-Options"] = "nosniff";
    await next(context);
});

app.MapGet("/healthz", (IT4MarketDataReader reader) =>
{
    var health = reader.Health;
    return Results.Json(new
    {
        status = health.Ready ? "READY" : "NOT_READY",
        source_id = "plus500_t4_futures_v1",
        venue_id = "plus500_t4",
        environment = health.Environment,
        read_only = true,
        order_routes_exposed = false,
        official_protocol_commit = T4ApiOptions.OfficialProtocolCommit,
        reason_code = health.ReasonCode,
        last_message_at = health.LastMessageAt,
        reconnect_count = health.ReconnectCount,
        boot_id = health.BootId,
        session_generation = health.SessionGeneration,
        evidence_key_fingerprint_sha256 = health.EvidenceKeyFingerprintSha256,
    }, statusCode: health.Ready
        ? StatusCodes.Status200OK
        : StatusCodes.Status503ServiceUnavailable);
});

app.MapGet("/v1/market-data", async (
    string symbol,
    int interval_minutes,
    DateTimeOffset as_of,
    int limit,
    HttpContext context,
    IT4MarketDataReader reader,
    CancellationToken cancellationToken) =>
{
    try
    {
        var request = RequestValidator.Validate(options, symbol, interval_minutes, as_of, limit);
        var result = await reader.ReadAsync(request, cancellationToken);
        FuturesEvidenceValidator.Validate(request, result);
        return Results.Json(result);
    }
    catch (ArgumentException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid market-data request");
    }
    catch (T4SessionUnavailableException exception)
    {
        context.Response.Headers[ObservationEvidenceContract.ReasonCodeHeaderName] =
            T4ReasonCodes.Safe(exception.ReasonCode);
        return Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "T4 market data is unavailable");
    }
});

app.MapGet("/v1/observation/events", (
    ObservationEventJournal journal,
    long after_sequence = 0,
    int limit = 100) =>
{
    try
    {
        return Results.Json(journal.Read(after_sequence, limit));
    }
    catch (ArgumentException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid observation event query");
    }
    catch (InvalidOperationException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Signed observation evidence is unavailable");
    }
});

app.MapPost("/v1/observation/checkpoint", (
    ObservationCheckpointRequest request,
    ObservationEventJournal journal,
    IT4MarketDataReader reader,
    BridgeOptions bridgeOptions) =>
{
    try
    {
        var (campaignId, actionRequestId) =
            request.Validate(bridgeOptions.Observation);
        var health = reader.Health;
        var checkpoint = journal.RecordCampaignCheckpoint(
            campaignId,
            actionRequestId,
            health.SessionGeneration,
            health.ReconnectCount);
        return Results.Json(new
        {
            schema_version = ObservationEvidenceContract.SchemaVersion,
            status = "recorded",
            campaign_id = campaignId,
            action_request_id = actionRequestId,
            event_sequence_no = checkpoint.SequenceNo,
            event_hash_sha256 = checkpoint.EventHashSha256,
            read_only = true,
            execution_enabled = false,
        }, statusCode: StatusCodes.Status201Created);
    }
    catch (ArgumentException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid observation checkpoint request");
    }
    catch (InvalidOperationException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Signed observation checkpoint is unavailable");
    }
});

app.MapPost("/v1/observation/control/{action}", (
    string action,
    ObservationControlRequest request,
    HttpContext context,
    IT4MarketDataReader reader,
    BridgeOptions bridgeOptions,
    IHostApplicationLifetime applicationLifetime) =>
{
    if (!bridgeOptions.Observation.ControlEnabled)
    {
        return Results.NotFound();
    }
    var supplied = context.Request.Headers[
        ObservationEvidenceContract.ControlTokenHeaderName].ToString();
    if (!BridgeSecurity.TokenMatches(supplied, bridgeOptions.Observation.ControlToken))
    {
        return Results.Unauthorized();
    }
    if (!ObservationControlActions.TryParse(action, out var parsedAction))
    {
        return Results.NotFound();
    }
    if (reader is not IT4ObservationController controller)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Official T4 observation control is unavailable");
    }
    try
    {
        var (campaignId, actionRequestId, scopeKey) =
            request.Validate(bridgeOptions.Observation);
        var receipt = controller.ApplyControl(
            parsedAction,
            campaignId,
            actionRequestId,
            scopeKey);
        if (parsedAction is ObservationControlAction.Restart)
        {
            context.Response.OnCompleted(() =>
            {
                applicationLifetime.StopApplication();
                return Task.CompletedTask;
            });
        }
        return Results.Json(new
        {
            schema_version = ObservationEvidenceContract.SchemaVersion,
            status = "accepted",
            action = ObservationControlActions.Code(parsedAction),
            campaign_id = campaignId,
            action_request_id = actionRequestId,
            scope_key = scopeKey,
            event_sequence_no = receipt.SequenceNo,
            event_hash_sha256 = receipt.EventHashSha256,
            read_only = true,
            execution_enabled = false,
        }, statusCode: StatusCodes.Status202Accepted);
    }
    catch (ArgumentException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid observation control request");
    }
    catch (InvalidOperationException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "Observation control could not be applied");
    }
});

app.Run();

public partial class Program
{
}

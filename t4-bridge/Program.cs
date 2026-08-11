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
builder.Services.AddSingleton<IT4MarketDataReader, T4ApplicationRegistrationPendingReader>();

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

app.MapGet("/healthz", () => Results.Json(new
{
    status = "NOT_READY",
    source_id = "plus500_t4_futures_v1",
    venue_id = "plus500_t4",
    read_only = true,
    order_routes_exposed = false,
    reason = "T4_APPLICATION_REGISTRATION_REQUIRED",
}, statusCode: StatusCodes.Status503ServiceUnavailable));

app.MapGet("/v1/market-data", async (
    string symbol,
    int interval_minutes,
    DateTimeOffset as_of,
    int limit,
    IT4MarketDataReader reader,
    CancellationToken cancellationToken) =>
{
    try
    {
        var request = RequestValidator.Validate(options, symbol, interval_minutes, as_of, limit);
        var result = await reader.ReadAsync(request, cancellationToken);
        return Results.Json(result);
    }
    catch (ArgumentException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status400BadRequest,
            title: "Invalid market-data request");
    }
    catch (T4SessionUnavailableException)
    {
        return Results.Problem(
            statusCode: StatusCodes.Status503ServiceUnavailable,
            title: "T4 market data is unavailable");
    }
});

app.Run();

public partial class Program
{
}

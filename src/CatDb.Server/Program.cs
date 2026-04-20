using System.Text.Json;
using CatDb.Server;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

// ── Background service ────────────────────────────────────────────────────────
builder.Services.AddSingleton<ServerState>();
builder.Services.AddHostedService<CatDbServerService>();

// ── Health checks ─────────────────────────────────────────────────────────────
builder.Services.AddHealthChecks()
    .AddCheck<CatDbHealthCheck>("catdb");

// ── OpenTelemetry ─────────────────────────────────────────────────────────────
builder.Logging.AddOpenTelemetry(otel =>
{
    otel.IncludeScopes = true;
    otel.IncludeFormattedMessage = true;
    otel.AddConsoleExporter();
    // Uncomment to export to an OTLP endpoint (e.g. Grafana, Jaeger):
    otel.AddOtlpExporter();
});

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing => tracing
        .AddSource("CatDb.Server")
        .AddAspNetCoreInstrumentation()
        .AddConsoleExporter())
    .WithMetrics(metrics => metrics
        .AddRuntimeInstrumentation()
        .AddAspNetCoreInstrumentation()
        .AddConsoleExporter());

// ── Build ─────────────────────────────────────────────────────────────────────
var app = builder.Build();

app.MapHealthChecks("/health",       new HealthCheckOptions { ResponseWriter = HealthResponse.WriteJson });
app.MapHealthChecks("/health/catdb", new HealthCheckOptions { Predicate = r => r.Name == "catdb", ResponseWriter = HealthResponse.WriteJson });
app.MapGet("/", () => Results.Ok(new { service = "CatDb Server", version = "1.0" }));

await app.RunAsync();


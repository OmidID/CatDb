using CatDb.Server.Apis.Admin;
using CatDb.Server;
using CatDb.Server.Services;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

// ── CLI overrides: --catdb-port 7183  --catdb-file ~/newdb.catdb ──────────────
builder.Configuration.AddCommandLine(args, new Dictionary<string, string>
{
    ["--catdb-port"] = "CatDb:Port",
    ["--catdb-dir"] = "CatDb:Directory",
    ["--catdb-default-db"] = "CatDb:DefaultDatabase",
    ["--catdb-admin-user"] = "CatDb:Admin:UserName",
    ["--catdb-admin-password"] = "CatDb:Admin:Password",
});

// ── Background service ────────────────────────────────────────────────────────
builder.Services.AddSingleton<ServerState>();
builder.Services.AddSingleton<SystemCatalogService>(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var logger = sp.GetRequiredService<ILogger<SystemCatalogService>>();
    var directory = config["CatDb:Directory"] ?? AppContext.BaseDirectory;
    Directory.CreateDirectory(directory);
    var systemPath = Path.Combine(directory, SystemCatalogService.SystemDatabaseName);
    return new SystemCatalogService(systemPath, logger);
});
builder.Services.AddSingleton<DatabaseHostService>(sp =>
{
    var config = sp.GetRequiredService<IConfiguration>();
    var logger = sp.GetRequiredService<ILogger<DatabaseHostService>>();
    var catalog = sp.GetRequiredService<SystemCatalogService>();
    var directory = config["CatDb:Directory"] ?? AppContext.BaseDirectory;
    return new DatabaseHostService(directory, logger, catalog);
});
builder.Services.AddSingleton<EngineAccessPolicy>();
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
app.MapAdminDatabaseEndpoints();
app.MapAdminUserEndpoints();

await app.RunAsync();


// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

﻿using CatDb.Server;
using CatDb.Server.Apis.Admin;
using CatDb.Server.Apis.Data;
using CatDb.Server.Auth;
using CatDb.Server.Models;
using CatDb.Server.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

// ── CLI overrides: --catdb-port 7183 ──────────────
builder.Configuration.AddCommandLine(args, new Dictionary<string, string>
{
    ["--catdb-port"] = "CatDb:Port",
    ["--catdb-dir"] = "CatDb:Directory",
    ["--catdb-default-db"] = "CatDb:DefaultDatabase"
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
builder.Services.AddSingleton<DataExplorerService>();
builder.Services.AddHostedService<CatDbServerService>();

// ── Authentication & Authorization ────────────────────────────────────────────
builder.Services.AddAuthentication(BasicAuthenticationHandler.SchemeName)
    .AddScheme<AuthenticationSchemeOptions, BasicAuthenticationHandler>(
        BasicAuthenticationHandler.SchemeName, null);

builder.Services.AddAuthorizationBuilder()
    .AddPolicy(PolicyNames.ManageUsers, policy =>
        policy.AddRequirements(new GlobalPermissionRequirement(GlobalPermission.ManageUsers)))
    .AddPolicy(PolicyNames.ManageDatabases, policy =>
        policy.AddRequirements(new GlobalPermissionRequirement(GlobalPermission.ManageDatabases)))
    .AddPolicy(PolicyNames.ListDatabases, policy =>
        policy.AddRequirements(new GlobalPermissionRequirement(GlobalPermission.ListDatabases)))
    .AddPolicy(PolicyNames.DatabaseRead, policy =>
        policy.AddRequirements(new DatabaseReadRequirement()));

builder.Services.AddSingleton<IAuthorizationHandler, GlobalPermissionHandler>();
builder.Services.AddSingleton<IAuthorizationHandler, DatabaseReadHandler>();

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

app.UseAuthentication();
app.UseAuthorization();

app.MapHealthChecks("/health",       new HealthCheckOptions { ResponseWriter = HealthResponse.WriteJson });
app.MapHealthChecks("/health/catdb", new HealthCheckOptions { Predicate = r => r.Name == "catdb", ResponseWriter = HealthResponse.WriteJson });
app.MapGet("/", () => Results.Ok(new
{
    service = "CatDb Server",
    version = typeof(Program).Assembly.GetName().Version?.ToString() ?? "0.0.0",
}));
app.MapAdminDatabaseEndpoints();
app.MapAdminUserEndpoints();
app.MapDataDatabaseEndpoints();
app.MapDataTableEndpoints();

await app.RunAsync();


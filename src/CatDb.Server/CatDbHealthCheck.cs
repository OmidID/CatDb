// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CatDb.Server;

public sealed class CatDbHealthCheck(ServerState state) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (!state.IsRunning)
            return Task.FromResult(HealthCheckResult.Unhealthy("CatDb server is not running"));

        var data = new Dictionary<string, object>
        {
            ["port"] = state.Port,
            ["databaseDirectory"] = state.DatabaseDirectory,
            ["defaultDatabase"] = state.DefaultDatabaseName,
        };

        return Task.FromResult(HealthCheckResult.Healthy("CatDb server is running", data));
    }
}

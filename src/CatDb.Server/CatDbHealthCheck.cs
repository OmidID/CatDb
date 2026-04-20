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
            ["port"]     = state.Port,
            ["fileName"] = state.FileName,
        };

        return Task.FromResult(HealthCheckResult.Healthy("CatDb server is running", data));
    }
}

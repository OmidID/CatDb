// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text.Json;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace CatDb.Server;

internal static class HealthResponse
{
    private static readonly JsonSerializerOptions _json = new() { WriteIndented = true };

    public static Task WriteJson(HttpContext ctx, HealthReport report)
    {
        ctx.Response.ContentType = "application/json";

        var result = new
        {
            status = report.Status.ToString(),
            duration = report.TotalDuration,
            checks = report.Entries.Select(e => new
            {
                name     = e.Key,
                status   = e.Value.Status.ToString(),
                duration = e.Value.Duration,
                description = e.Value.Description,
                data     = e.Value.Data,
                exception = e.Value.Exception?.Message,
            }),
        };

        return ctx.Response.WriteAsync(JsonSerializer.Serialize(result, _json));
    }
}

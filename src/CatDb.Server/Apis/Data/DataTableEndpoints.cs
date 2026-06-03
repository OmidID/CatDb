// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Server.Auth;
using CatDb.Server.Services;

namespace CatDb.Server.Apis.Data;

public static class DataTableEndpoints
{
    public static IEndpointRouteBuilder MapDataTableEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/v1/data/{databaseName}/{tableName}", (
            DataExplorerService  explorer,
            string               databaseName,
            string               tableName,
            int                  take      = 50,
            string?              fromKey   = null,
            string?              toKey     = null,
            string               direction = "forward") =>
        {
            try
            {
                var result = explorer.BrowseTable(databaseName, tableName, take, fromKey, toKey, direction);
                return Results.Ok(result);
            }
            catch (KeyNotFoundException ex)
            {
                return Results.NotFound(new { error = ex.Message });
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireAuthorization(PolicyNames.DatabaseRead);

        return app;
    }
}

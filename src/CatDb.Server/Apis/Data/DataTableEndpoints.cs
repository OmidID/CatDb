// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using CatDb.Server.Auth;
using CatDb.Server.Services;

namespace CatDb.Server.Apis.Data;

public static class DataTableEndpoints
{
    public static IEndpointRouteBuilder MapDataTableEndpoints(this IEndpointRouteBuilder app)
    {
        // GET /api/v1/data/{db}/{table} — browse / read rows
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
            catch (KeyNotFoundException ex)  { return Results.NotFound(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.BadRequest(new { error = ex.Message }); }
        }).RequireAuthorization(PolicyNames.DatabaseRead);

        // POST /api/v1/data/{db}/{table} — insert (InsertOrIgnore)
        app.MapPost("/api/v1/data/{databaseName}/{tableName}", (
            DataExplorerService  explorer,
            string               databaseName,
            string               tableName,
            [FromBody] JsonElement body) =>
        {
            try
            {
                if (!body.TryGetProperty("key", out var keyEl) ||
                    !body.TryGetProperty("value", out var valueEl))
                    return Results.BadRequest(new { error = "Body must have 'key' and 'value' properties." });
                var result = explorer.InsertRecord(databaseName, tableName, keyEl, valueEl);
                return Results.Ok(result);
            }
            catch (KeyNotFoundException ex)  { return Results.NotFound(new { error = ex.Message }); }
            catch (ArgumentException ex)     { return Results.BadRequest(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.BadRequest(new { error = ex.Message }); }
        }).RequireAuthorization(PolicyNames.DatabaseWrite);

        // PUT /api/v1/data/{db}/{table} — replace (upsert)
        app.MapPut("/api/v1/data/{databaseName}/{tableName}", (
            DataExplorerService  explorer,
            string               databaseName,
            string               tableName,
            [FromBody] JsonElement body) =>
        {
            try
            {
                if (!body.TryGetProperty("key", out var keyEl) ||
                    !body.TryGetProperty("value", out var valueEl))
                    return Results.BadRequest(new { error = "Body must have 'key' and 'value' properties." });
                var result = explorer.ReplaceRecord(databaseName, tableName, keyEl, valueEl);
                return Results.Ok(result);
            }
            catch (KeyNotFoundException ex)  { return Results.NotFound(new { error = ex.Message }); }
            catch (ArgumentException ex)     { return Results.BadRequest(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.BadRequest(new { error = ex.Message }); }
        }).RequireAuthorization(PolicyNames.DatabaseWrite);

        // DELETE /api/v1/data/{db}/{table} — delete by key (key in request body)
        app.MapDelete("/api/v1/data/{databaseName}/{tableName}", (
            DataExplorerService  explorer,
            string               databaseName,
            string               tableName,
            [FromBody] JsonElement body) =>
        {
            try
            {
                if (!body.TryGetProperty("key", out var keyEl))
                    return Results.BadRequest(new { error = "Body must have a 'key' property." });
                var result = explorer.DeleteRecord(databaseName, tableName, keyEl);
                return Results.Ok(result);
            }
            catch (KeyNotFoundException ex)  { return Results.NotFound(new { error = ex.Message }); }
            catch (ArgumentException ex)     { return Results.BadRequest(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.BadRequest(new { error = ex.Message }); }
        }).RequireAuthorization(PolicyNames.DatabaseWrite);

        return app;
    }
}

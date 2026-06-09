// Copyright (c) 2024-2026 CatDb (https://github.com/OmidID/CatDb)
// Licensed under the MIT License. See LICENSE in the project root for license information.

using CatDb.Database.Indexing;
using CatDb.Server.Auth;
using CatDb.Server.Services;

namespace CatDb.Server.Apis.Data;

/// <summary>
/// Table schema management (schemas are JSON Schema draft-07 objects):
///   GET    /api/v1/tables/{db}                         — list tables
///   POST   /api/v1/tables/{db}                         — create table
///   GET    /api/v1/tables/{db}/{table}                  — describe table (schema + indexes)
///   DELETE /api/v1/tables/{db}/{table}                  — delete table
///
/// Index management:
///   GET    /api/v1/tables/{db}/{table}/indexes          — list indexes
///   POST   /api/v1/tables/{db}/{table}/indexes          — create index
///   DELETE /api/v1/tables/{db}/{table}/indexes/{name}   — drop index
///   POST   /api/v1/tables/{db}/{table}/indexes/{name}/rebuild  — rebuild one index
///   POST   /api/v1/tables/{db}/{table}/indexes/rebuild         — rebuild all indexes
/// </summary>
public static class DataTableManagementEndpoints
{
    public static IEndpointRouteBuilder MapDataTableManagementEndpoints(this IEndpointRouteBuilder app)
    {
        var tables = app.MapGroup("/api/v1/tables/{databaseName}")
            .RequireAuthorization(PolicyNames.DatabaseTableAdmin);

        // ── Table CRUD ────────────────────────────────────────────────────────

        tables.MapGet("/", (
            TableManagementService svc,
            string databaseName) =>
        {
            try
            {
                var list = svc.ListTables(databaseName);
                return Results.Ok(new { database = databaseName, count = list.Count, tables = list });
            }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        }).RequireAuthorization(PolicyNames.DatabaseRead);

        tables.MapPost("/", (
            TableManagementService svc,
            string databaseName,
            CreateTableRequest body) =>
        {
            try
            {
                var info = svc.CreateTable(databaseName, body.Name, body.KeySchema, body.ValueSchema);
                return Results.Created($"/api/v1/tables/{databaseName}/{body.Name}", info);
            }
            catch (InvalidOperationException ex) { return Results.Conflict(new { error = ex.Message }); }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        tables.MapGet("/{tableName}", (
            TableManagementService svc,
            string databaseName,
            string tableName) =>
        {
            try
            {
                return Results.Ok(svc.GetTable(databaseName, tableName));
            }
            catch (KeyNotFoundException ex)      { return Results.NotFound(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.BadRequest(new { error = ex.Message }); }
        }).RequireAuthorization(PolicyNames.DatabaseRead);

        tables.MapDelete("/{tableName}", (
            TableManagementService svc,
            string databaseName,
            string tableName) =>
        {
            try
            {
                svc.DeleteTable(databaseName, tableName);
                return Results.Ok(new { database = databaseName, table = tableName, deleted = true });
            }
            catch (KeyNotFoundException ex)    { return Results.NotFound(new { error = ex.Message }); }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        // ── Index management ──────────────────────────────────────────────────

        tables.MapGet("/{tableName}/indexes", (
            TableManagementService svc,
            string databaseName,
            string tableName) =>
        {
            try
            {
                var indexes = svc.ListIndexes(databaseName, tableName);
                return Results.Ok(new { database = databaseName, table = tableName, indexes });
            }
            catch (KeyNotFoundException ex)    { return Results.NotFound(new { error = ex.Message }); }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        }).RequireAuthorization(PolicyNames.DatabaseRead);

        tables.MapPost("/{tableName}/indexes", (
            TableManagementService svc,
            string databaseName,
            string tableName,
            CreateIndexRequest body) =>
        {
            try
            {
                if (!Enum.TryParse<IndexType>(body.Type, ignoreCase: true, out var indexType))
                    return Results.BadRequest(new
                    {
                        error = $"Unknown index type '{body.Type}'. Use 'Unique' or 'NonUnique'."
                    });

                var info = svc.CreateIndex(databaseName, tableName, body.IndexName, body.Members, indexType);
                return Results.Created(
                    $"/api/v1/tables/{databaseName}/{tableName}/indexes/{body.IndexName}", info);
            }
            catch (KeyNotFoundException ex)    { return Results.NotFound(new { error = ex.Message }); }
            catch (InvalidOperationException ex) { return Results.Conflict(new { error = ex.Message }); }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        tables.MapDelete("/{tableName}/indexes/{indexName}", (
            TableManagementService svc,
            string databaseName,
            string tableName,
            string indexName) =>
        {
            try
            {
                svc.DropIndex(databaseName, tableName, indexName);
                return Results.Ok(new
                {
                    database = databaseName,
                    table    = tableName,
                    index    = indexName,
                    dropped  = true,
                });
            }
            catch (KeyNotFoundException ex)    { return Results.NotFound(new { error = ex.Message }); }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        tables.MapPost("/{tableName}/indexes/{indexName}/rebuild", (
            TableManagementService svc,
            string databaseName,
            string tableName,
            string indexName) =>
        {
            try
            {
                svc.RebuildIndex(databaseName, tableName, indexName);
                return Results.Ok(new
                {
                    database = databaseName,
                    table    = tableName,
                    index    = indexName,
                    rebuilt  = true,
                });
            }
            catch (KeyNotFoundException ex)    { return Results.NotFound(new { error = ex.Message }); }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        tables.MapPost("/{tableName}/indexes/rebuild", (
            TableManagementService svc,
            string databaseName,
            string tableName) =>
        {
            try
            {
                svc.RebuildIndex(databaseName, tableName, indexName: null);
                return Results.Ok(new
                {
                    database = databaseName,
                    table    = tableName,
                    rebuilt  = "all",
                });
            }
            catch (KeyNotFoundException ex)    { return Results.NotFound(new { error = ex.Message }); }
            catch (Exception ex) { return Results.BadRequest(new { error = ex.Message }); }
        });

        return app;
    }
}

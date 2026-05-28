using CatDb.Data;
using CatDb.Database;
using CatDb.Server.Auth;
using CatDb.Server.Services;

namespace CatDb.Server.Apis.Data;

public static class DataDatabaseEndpoints
{
    public static IEndpointRouteBuilder MapDataDatabaseEndpoints(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/v1/data/{databaseName}", (
            DatabaseHostService  host,
            string               databaseName) =>
        {
            IStorageEngine engine;
            try   { engine = host.GetOrOpenDatabase(databaseName); }
            catch (Exception ex) { return Results.NotFound(new { error = ex.Message }); }

            var tables = engine
                .Where(d => d.StructureType == StructureType.XTABLE)
                .Select(d => new
                {
                    name        = d.Name,
                    keySchema   = DataSchemaHelper.Describe(d.KeyDataType, d.KeyMembers),
                    valueSchema = DataSchemaHelper.Describe(d.RecordDataType, d.RecordMembers),
                    createdAt   = d.CreateTime,
                    modifiedAt  = d.ModifiedTime,
                    accessedAt  = d.AccessTime,
                })
                .ToList();

            return Results.Ok(new
            {
                database = databaseName,
                count    = tables.Count,
                tables,
            });
        }).RequireAuthorization(PolicyNames.DatabaseRead);

        return app;
    }
}

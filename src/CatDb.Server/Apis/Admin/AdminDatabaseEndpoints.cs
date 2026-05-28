using CatDb.Server.Auth;
using CatDb.Server.Services;

namespace CatDb.Server.Apis.Admin;

public static class AdminDatabaseEndpoints
{
    public static IEndpointRouteBuilder MapAdminDatabaseEndpoints(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/v1/admin/databases")
            .RequireAuthorization(PolicyNames.ListDatabases);

        group.MapGet("/", (
            DatabaseHostService host,
            int page = 1,
            int pageSize = 20) =>
        {
            page = Math.Max(1, page);
            pageSize = Math.Clamp(pageSize, 1, 200);

            var (items, total) = host.ListDatabases(page, pageSize);
            return Results.Ok(new
            {
                page,
                pageSize,
                total,
                items,
            });
        });

        group.MapPost("/{databaseName}", (
            DatabaseHostService host,
            string databaseName) =>
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                return Results.BadRequest(new { error = "databaseName is required." });

            try
            {
                host.CreateDatabase(databaseName);
                return Results.Ok(new { name = databaseName, created = true });
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireAuthorization(PolicyNames.ManageDatabases);

        group.MapDelete("/{databaseName}", (
            DatabaseHostService host,
            string databaseName) =>
        {
            if (string.IsNullOrWhiteSpace(databaseName))
                return Results.BadRequest(new { error = "databaseName is required." });

            try
            {
                host.DeleteDatabase(databaseName);
                return Results.Ok(new { name = databaseName, deleted = true });
            }
            catch (Exception ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        }).RequireAuthorization(PolicyNames.ManageDatabases);

        return app;
    }
}
